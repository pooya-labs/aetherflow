//! Standalone durable worker process for the full Rust AetherFlow engine.
//!
//! The process runs the PostgreSQL-backed task worker, transactional outbox
//! dispatcher, and JetStream inbox consumer. PostgreSQL migrations are owned by
//! the control-plane server; this process uses only the restricted runtime role.

use std::error::Error;
use std::fs;
use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aetherflow_core::domain::{NodeId, NodeIncarnationId, TaskId, WorkerId};
use aetherflow_runtime::repository::TaskOutput;
use aetherflow_runtime::scheduler::{TaskContext, TaskExecutionError, TaskRegistry};
use aetherflow_runtime::wasm::WasmTaskHandler;
use aetherflow_runtime::{CapabilitySet, ExecutionLimits, WasmPluginRuntime};
use aetherflow_storage_postgres::{
    OutboxDispatcherConfig, PgInboxRepository, PgMembershipRepository, PgStore, PgTaskRepository,
    PostgresOutboxDispatcher, PostgresTaskWorker, PostgresWorkerConfig, WorkerRegistration,
};
use aetherflow_transport_nats::{
    EventHandlerFuture, JetStreamEvent, JetStreamEventHandler, NatsConnectionConfig,
    NatsJetStreamConsumer, NatsJetStreamPublisher,
};
use async_nats::ServerAddr;
use async_nats::jetstream::{self, consumer::pull::Config as PullConsumerConfig};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde::Deserialize;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_postgres::{Config as PostgresConfig, NoTls};

const DEFAULT_NATS_URLS: &str = "nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224";
const WORKER_LEASE: Duration = Duration::from_secs(30);
const WORKER_HEARTBEAT: Duration = Duration::from_secs(10);
const INBOX_LEASE: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AddInput {
    a: i64,
    b: i64,
}

struct WorkflowEventLogger;

impl JetStreamEventHandler for WorkflowEventLogger {
    fn handle<'a>(&'a self, event: &'a JetStreamEvent) -> EventHandlerFuture<'a> {
        Box::pin(async move {
            println!(
                "event received: type={} aggregate={} id={} event_id={}",
                event.event_type(),
                event.aggregate_type(),
                event.aggregate_id(),
                event.event_id()
            );
            Ok(())
        })
    }
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn required_env(name: &str) -> Result<String, io::Error> {
    std::env::var(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} is required by aetherflow-worker"),
        )
    })
}

fn positive_u32(name: &str, default: u32, maximum: u32) -> Result<u32, io::Error> {
    let value = match std::env::var(name) {
        Ok(value) => value.parse::<u32>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must be a positive integer"),
            )
        })?,
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unable to read {name}: {error}"),
            ));
        }
    };
    if value == 0 || value > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} must be between 1 and {maximum}"),
        ));
    }
    Ok(value)
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}-{nanos}", std::process::id())
}

fn parse_nats_seeds(urls: &str) -> Result<Vec<ServerAddr>, Box<dyn Error>> {
    let seeds = urls
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::parse::<ServerAddr>)
        .collect::<Result<Vec<_>, _>>()?;
    if seeds.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "AETHERFLOW_NATS_URLS must contain at least one NATS server URL",
        )
        .into());
    }
    Ok(seeds)
}

fn uppercase_payload(input: &[u8]) -> Result<Vec<u8>, TaskExecutionError> {
    let text = std::str::from_utf8(input).map_err(|error| {
        TaskExecutionError::permanent(format!("uppercase expects UTF-8: {error}"))
    })?;
    Ok(text.to_uppercase().into_bytes())
}

fn json_add_payload(input: &[u8]) -> Result<Vec<u8>, TaskExecutionError> {
    let values: AddInput = serde_json::from_slice(input).map_err(|error| {
        TaskExecutionError::permanent(format!(
            "json_add expects a JSON object with integer fields a and b: {error}"
        ))
    })?;
    let result = values
        .a
        .checked_add(values.b)
        .ok_or_else(|| TaskExecutionError::permanent("json_add integer overflow"))?;
    serde_json::to_vec(&result)
        .map_err(|error| TaskExecutionError::permanent(format!("encode json_add output: {error}")))
}

fn fail_once_output(attempt: u32) -> Result<TaskOutput, TaskExecutionError> {
    if attempt == 1 {
        return Err(TaskExecutionError::retryable(
            "demonstration failure: fail_once succeeds on the second attempt",
        ));
    }
    TaskOutput::new(b"retry-succeeded".to_vec())
        .map_err(|error| TaskExecutionError::permanent(error.to_string()))
}

fn build_task_registry(plugin_path: Option<PathBuf>) -> Result<TaskRegistry, Box<dyn Error>> {
    let mut registry = TaskRegistry::new();

    registry.register(TaskId::parse("echo")?, |context: TaskContext| async move {
        TaskOutput::new(context.input().as_bytes().to_vec())
            .map_err(|error| TaskExecutionError::permanent(error.to_string()))
    })?;

    registry.register(
        TaskId::parse("uppercase")?,
        |context: TaskContext| async move {
            let output = uppercase_payload(context.input().as_bytes())?;
            TaskOutput::new(output)
                .map_err(|error| TaskExecutionError::permanent(error.to_string()))
        },
    )?;

    registry.register(
        TaskId::parse("json_add")?,
        |context: TaskContext| async move {
            let output = json_add_payload(context.input().as_bytes())?;
            TaskOutput::new(output)
                .map_err(|error| TaskExecutionError::permanent(error.to_string()))
        },
    )?;

    registry.register(
        TaskId::parse("fail_once")?,
        |context: TaskContext| async move {
            fail_once_output(context.claim().attempt_number().get())
        },
    )?;

    if let Some(path) = plugin_path {
        let artifact = fs::read(&path)?;
        let runtime = Arc::new(WasmPluginRuntime::new()?);
        let plugin = Arc::new(runtime.compile_component(&artifact)?);
        let handler = WasmTaskHandler::new(
            runtime,
            plugin,
            CapabilitySet::default(),
            ExecutionLimits::default(),
        )?;
        registry.register(TaskId::parse("wasm")?, handler)?;
        println!(
            "loaded ABI v1 WebAssembly task plugin from {}",
            path.display()
        );
    }

    Ok(registry)
}

async fn heartbeat_transport_registration(
    membership: Arc<PgMembershipRepository>,
    registration: WorkerRegistration,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let mut interval = tokio::time::interval(WORKER_HEARTBEAT);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow_and_update() {
                    return Ok(());
                }
            }
            _ = interval.tick() => {
                membership
                    .heartbeat_worker(&registration, WORKER_LEASE)
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
    }
}

fn worker_join_failure(
    component: &str,
    result: Result<Result<(), String>, tokio::task::JoinError>,
    unexpected_success: bool,
) -> Option<String> {
    match result {
        Ok(Ok(())) if unexpected_success => Some(format!("{component} stopped unexpectedly")),
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(format!("{component} failed: {error}")),
        Err(error) => Some(format!("{component} task failed to join: {error}")),
    }
}

type NamedTransportResult = (&'static str, Result<(), String>);
type NamedTransportJoin = Result<NamedTransportResult, tokio::task::JoinError>;

fn transport_join_failure(
    result: Option<NamedTransportJoin>,
    unexpected_success: bool,
) -> Option<String> {
    match result {
        Some(Ok((component, Ok(())))) if unexpected_success => {
            Some(format!("{component} stopped unexpectedly"))
        }
        Some(Ok((_, Ok(())))) => None,
        Some(Ok((component, Err(error)))) => Some(format!("{component} failed: {error}")),
        Some(Err(error)) => Some(format!("transport task failed to join: {error}")),
        None if unexpected_success => Some("all transport tasks stopped unexpectedly".to_owned()),
        None => None,
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let database_url = required_env("DATABASE_URL")?;
    let worker_concurrency = positive_u32("AETHERFLOW_WORKER_CONCURRENCY", 4, 256)?;
    let worker_concurrency_usize = usize::try_from(worker_concurrency)?;
    let pool_size = worker_concurrency_usize.saturating_add(8).max(12);

    // The worker deliberately does not run DDL. The control-plane server owns
    // migrations; the worker starts only after the schema has been provisioned.
    let postgres_config: PostgresConfig = database_url.parse()?;
    let manager = Manager::from_config(
        postgres_config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(manager).max_size(pool_size).build()?;
    PgStore::from_pool(pool.clone()).check_connection().await?;
    let client = pool.get().await?;
    let migration = client
        .query_one("SELECT MAX(version) FROM aetherflow_schema_migrations", &[])
        .await?;
    let schema_version: Option<i64> = migration.get(0);
    if schema_version.is_none() {
        return Err(io::Error::other(
            "AetherFlow schema is not initialized; start aetherflow-server once with its migration-owner connection first",
        )
        .into());
    }
    drop(client);

    let suffix = unique_suffix();
    let node_id = NodeId::parse(env_or(
        "AETHERFLOW_NODE_ID",
        &format!("aetherflow-node-{}", std::process::id()),
    ))?;
    let incarnation_id = NodeIncarnationId::parse(format!("incarnation-{suffix}"))?;
    let task_worker_id = WorkerId::parse(env_or(
        "AETHERFLOW_TASK_WORKER_ID",
        &format!("task-worker-{suffix}"),
    ))?;
    let transport_worker_id = WorkerId::parse(env_or(
        "AETHERFLOW_TRANSPORT_WORKER_ID",
        &format!("transport-worker-{suffix}"),
    ))?;
    if task_worker_id == transport_worker_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "AETHERFLOW_TASK_WORKER_ID and AETHERFLOW_TRANSPORT_WORKER_ID must be different",
        )
        .into());
    }

    let membership = Arc::new(PgMembershipRepository::from_pool(pool.clone()));
    let node = membership
        .join_node(&node_id, &incarnation_id, WORKER_LEASE)
        .await?;
    let task_registration = membership
        .register_worker(
            &node,
            &task_worker_id,
            NonZeroU32::new(worker_concurrency)
                .ok_or_else(|| io::Error::other("worker concurrency must be positive"))?,
            WORKER_LEASE,
        )
        .await?;
    let transport_registration = membership
        .register_worker(&node, &transport_worker_id, NonZeroU32::MIN, WORKER_LEASE)
        .await?;

    let repository = Arc::new(PgTaskRepository::from_pool(pool.clone()));
    let registry =
        build_task_registry(std::env::var_os("AETHERFLOW_WASM_PLUGIN_PATH").map(PathBuf::from))?;
    let task_worker = PostgresTaskWorker::new(
        Arc::clone(&repository),
        Arc::clone(&membership),
        task_registration,
        registry,
        PostgresWorkerConfig::new(
            NonZeroUsize::new(worker_concurrency_usize)
                .ok_or_else(|| io::Error::other("worker concurrency must be positive"))?,
            Duration::from_millis(25),
            WORKER_LEASE,
            WORKER_HEARTBEAT,
            None,
        )?,
    );

    let nats_urls = env_or("AETHERFLOW_NATS_URLS", DEFAULT_NATS_URLS);
    let nats_client = NatsConnectionConfig::from_env()?
        .connect(parse_nats_seeds(&nats_urls)?)
        .await?;
    let jetstream = jetstream::new(nats_client);
    let stream_name = env_or("AETHERFLOW_NATS_STREAM", "AETHERFLOW_EVENTS");
    let subject = env_or("AETHERFLOW_NATS_SUBJECT", "aetherflow.events");
    let durable_name = env_or("AETHERFLOW_NATS_DURABLE", "aetherflow_worker_events");

    if subject.is_empty() || subject.contains('*') || subject.contains('>') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "AETHERFLOW_NATS_SUBJECT must be one concrete publish subject, not a wildcard",
        )
        .into());
    }

    let stream = jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![subject.clone()],
            num_replicas: 3,
            ..Default::default()
        })
        .await?;
    let pull_consumer = stream
        .get_or_create_consumer(
            durable_name.as_str(),
            PullConsumerConfig {
                durable_name: Some(durable_name.clone()),
                filter_subject: subject.clone(),
                ack_wait: Duration::from_secs(60),
                max_ack_pending: 1024,
                ..Default::default()
            },
        )
        .await?;

    let publisher = Arc::new(NatsJetStreamPublisher::new(jetstream, &subject));
    let publisher_metrics = publisher.metrics();
    let dispatcher = PostgresOutboxDispatcher::new_registered(
        Arc::clone(&repository),
        transport_registration.clone(),
        publisher,
        OutboxDispatcherConfig::new(
            128,
            Duration::from_millis(25),
            WORKER_LEASE,
            Duration::from_millis(250),
            Duration::from_secs(10),
        )?,
    );

    let consumer = NatsJetStreamConsumer::new_registered(
        pull_consumer,
        PgInboxRepository::from_pool(pool),
        durable_name,
        transport_registration.clone(),
        INBOX_LEASE,
        Arc::new(WorkflowEventLogger),
    )?;
    let consumer_metrics = consumer.metrics();

    let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);
    let mut worker_handle = tokio::spawn(async move {
        task_worker
            .run(worker_shutdown_rx)
            .await
            .map_err(|error| error.to_string())
    });

    let (transport_shutdown_tx, transport_shutdown_rx) = watch::channel(false);
    let mut transport_tasks: JoinSet<NamedTransportResult> = JoinSet::new();

    let dispatcher_shutdown = transport_shutdown_rx.clone();
    transport_tasks.spawn(async move {
        (
            "PostgreSQL outbox dispatcher",
            dispatcher
                .run(dispatcher_shutdown)
                .await
                .map_err(|error| error.to_string()),
        )
    });

    let consumer_shutdown = transport_shutdown_rx.clone();
    transport_tasks.spawn(async move {
        (
            "NATS inbox consumer",
            consumer
                .run(consumer_shutdown)
                .await
                .map_err(|error| error.to_string()),
        )
    });

    let heartbeat_shutdown = transport_shutdown_rx.clone();
    let heartbeat_membership = Arc::clone(&membership);
    transport_tasks.spawn(async move {
        (
            "transport worker membership heartbeat",
            heartbeat_transport_registration(
                heartbeat_membership,
                transport_registration,
                heartbeat_shutdown,
            )
            .await,
        )
    });

    println!("AetherFlow Rust worker is running.");
    println!("node_id={}", node_id.as_str());
    println!("task_worker_id={}", task_worker_id.as_str());
    println!("transport_worker_id={}", transport_worker_id.as_str());
    println!("task_concurrency={worker_concurrency}");
    println!("nats_stream={stream_name} subject={subject}");
    println!(
        "supported task IDs: echo, uppercase, json_add, fail_once{}",
        if std::env::var_os("AETHERFLOW_WASM_PLUGIN_PATH").is_some() {
            ", wasm"
        } else {
            ""
        }
    );
    println!("Press Ctrl+C to drain and stop the worker.");

    let mut metrics_interval = tokio::time::interval(Duration::from_secs(30));
    let mut worker_completed = false;
    let mut failure: Option<String> = None;

    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal?;
                break;
            }
            result = &mut worker_handle => {
                worker_completed = true;
                failure = worker_join_failure("PostgreSQL task worker", result, true);
                break;
            }
            joined = transport_tasks.join_next() => {
                failure = transport_join_failure(joined, true);
                break;
            }
            _ = metrics_interval.tick() => {
                println!(
                    "metrics: publisher={:?} consumer={:?}",
                    publisher_metrics.snapshot(),
                    consumer_metrics.snapshot()
                );
            }
        }
    }

    // Stop task admission first. The transport registration is separate, so
    // dispatcher and consumer can continue draining while active task handlers
    // finish and the task worker marks its own registration as draining.
    if !worker_completed {
        let _ = worker_shutdown_tx.send(true);
        if let Some(error) =
            worker_join_failure("PostgreSQL task worker", worker_handle.await, false)
        {
            failure.get_or_insert(error);
        }
    }

    let _ = transport_shutdown_tx.send(true);
    while let Some(joined) = transport_tasks.join_next().await {
        if let Some(error) = transport_join_failure(Some(joined), false) {
            failure.get_or_insert(error);
        }
    }

    if let Some(error) = failure {
        return Err(io::Error::other(error).into());
    }

    println!("AetherFlow Rust worker stopped cleanly.");
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("aetherflow-worker failed: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{fail_once_output, json_add_payload, uppercase_payload};
    use aetherflow_runtime::scheduler::TaskExecutionError;

    #[test]
    fn uppercase_handler_supports_utf8_and_rejects_invalid_bytes() {
        assert_eq!(
            uppercase_payload("سلام Rust".as_bytes()).expect("valid UTF-8"),
            "سلام RUST".as_bytes()
        );
        assert!(matches!(
            uppercase_payload(&[0xff]),
            Err(TaskExecutionError::Permanent(_))
        ));
    }

    #[test]
    fn json_add_handler_accepts_integer_input_and_rejects_overflow() {
        assert_eq!(
            json_add_payload(br#"{"a":19,"b":23}"#).expect("valid addition"),
            b"42"
        );
        assert!(matches!(
            json_add_payload(br#"{"a":9223372036854775807,"b":1}"#),
            Err(TaskExecutionError::Permanent(_))
        ));
        assert!(matches!(
            json_add_payload(br#"{"a":1}"#),
            Err(TaskExecutionError::Permanent(_))
        ));
    }

    #[test]
    fn fail_once_handler_uses_the_durable_attempt_number() {
        assert!(matches!(
            fail_once_output(1),
            Err(TaskExecutionError::Retryable(_))
        ));
        assert_eq!(
            fail_once_output(2)
                .expect("second attempt succeeds")
                .as_bytes(),
            b"retry-succeeded"
        );
    }
}
