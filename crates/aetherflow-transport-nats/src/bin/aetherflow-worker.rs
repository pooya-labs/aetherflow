//! Standalone durable worker process for the full Rust AetherFlow engine.
//!
//! The process runs the PostgreSQL-backed task worker, transactional outbox
//! dispatcher, and JetStream inbox consumer. PostgreSQL migrations are owned by
//! the control-plane server; this process uses only the restricted runtime role.

use std::error::Error;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

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
    ConsumerMetrics, EventHandlerFuture, JetStreamEvent, JetStreamEventHandler,
    NatsConnectionConfig, NatsJetStreamConsumer, NatsJetStreamPublisher, PublisherMetrics,
};
use async_nats::ServerAddr;
use async_nats::jetstream::{self, consumer::pull::Config as PullConsumerConfig};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_postgres::Config as PostgresConfig;
use tokio_postgres::config::SslMode;
use tokio_postgres_rustls::MakeRustlsConnect;

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

fn ensure_rustls_crypto_provider() -> Result<(), io::Error> {
    static INITIALIZATION: OnceLock<()> = OnceLock::new();

    INITIALIZATION.get_or_init(|| {
        if rustls::crypto::CryptoProvider::get_default().is_none() {
            let _ = rustls::crypto::ring::default_provider().install_default();
        }
    });

    if rustls::crypto::CryptoProvider::get_default().is_some() {
        Ok(())
    } else {
        Err(io::Error::other(
            "unable to install the rustls ring crypto provider",
        ))
    }
}

fn parse_database_ssl_mode(value: Option<&str>) -> Result<SslMode, io::Error> {
    match value.unwrap_or("require") {
        "disable" => Ok(SslMode::Disable),
        "prefer" => Ok(SslMode::Prefer),
        "require" => Ok(SslMode::Require),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "DATABASE_SSL_MODE must be disable, prefer or require",
        )),
    }
}

fn database_ssl_mode() -> Result<SslMode, io::Error> {
    parse_database_ssl_mode(std::env::var("DATABASE_SSL_MODE").ok().as_deref())
}

fn optional_env(name: &str) -> Result<Option<String>, io::Error> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unable to read {name}: {error}"),
        )),
    }
}

struct MetricsServerConfig {
    bind: SocketAddr,
    token_hash: [u8; 32],
}

fn hash_metrics_token(token: &str) -> Result<[u8; 32], io::Error> {
    let valid_alphabet = token
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
    if !(32..=256).contains(&token.len()) || !valid_alphabet {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "AETHERFLOW_METRICS_TOKEN must be 32-256 ASCII letters, digits, '-' or '_' characters",
        ));
    }
    Ok(Sha256::digest(token.as_bytes()).into())
}

fn parse_metrics_server_config(
    bind: Option<&str>,
    token: Option<&str>,
) -> Result<Option<MetricsServerConfig>, io::Error> {
    match (bind, token) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "AETHERFLOW_METRICS_TOKEN requires AETHERFLOW_METRICS_BIND",
        )),
        (Some(_), None) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "AETHERFLOW_METRICS_TOKEN is required when the metrics listener is enabled",
        )),
        (Some(bind), Some(token)) => {
            let bind = bind.parse::<SocketAddr>().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("AETHERFLOW_METRICS_BIND must be a socket address: {error}"),
                )
            })?;
            if !bind.ip().is_loopback() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "AETHERFLOW_METRICS_BIND must use a loopback address; expose it remotely only through a TLS-terminating, authenticated proxy",
                ));
            }
            Ok(Some(MetricsServerConfig {
                bind,
                token_hash: hash_metrics_token(token)?,
            }))
        }
    }
}

fn metrics_server_config_from_env() -> Result<Option<MetricsServerConfig>, io::Error> {
    let bind = optional_env("AETHERFLOW_METRICS_BIND")?;
    let token = optional_env("AETHERFLOW_METRICS_TOKEN")?;
    parse_metrics_server_config(bind.as_deref(), token.as_deref())
}

#[derive(Clone)]
struct WorkerMetricsState {
    publisher: PublisherMetrics,
    consumer: ConsumerMetrics,
    token_hash: [u8; 32],
}

fn worker_metrics_authorization_matches(
    expected: &[u8; 32],
    headers: &HeaderMap,
) -> bool {
    let Some(value) = headers
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
    else {
        return false;
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return false;
    };
    if token.is_empty() {
        return false;
    }

    let supplied: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    bool::from(expected.as_slice().ct_eq(supplied.as_slice()))
}

fn render_worker_metrics(state: &WorkerMetricsState) -> String {
    let mut output = state.publisher.render_prometheus();
    output.push_str(&state.consumer.render_prometheus());
    output
}

async fn export_worker_metrics(
    State(state): State<WorkerMetricsState>,
    headers: HeaderMap,
) -> Response {
    if !worker_metrics_authorization_matches(&state.token_hash, &headers) {
        let mut response = StatusCode::UNAUTHORIZED.into_response();
        response.headers_mut().insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer"),
        );
        response.headers_mut().insert(
            CACHE_CONTROL,
            HeaderValue::from_static("no-store"),
        );
        return response;
    }

    let mut response = (
        StatusCode::OK,
        [
            (CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8"),
            (CACHE_CONTROL, "no-store"),
        ],
        render_worker_metrics(&state),
    )
        .into_response();
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    response
}

fn worker_metrics_router(state: WorkerMetricsState) -> Router {
    Router::new()
        .route("/metrics", get(export_worker_metrics))
        .with_state(state)
}

async fn run_metrics_server(
    listener: TcpListener,
    state: WorkerMetricsState,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    axum::serve(listener, worker_metrics_router(state))
        .with_graceful_shutdown(async move {
            if !*shutdown.borrow() {
                let _ = shutdown.changed().await;
            }
        })
        .await
        .map_err(|error| format!("Prometheus metrics listener: {error}"))
}


fn postgres_manager(database_url: &str, ssl_mode: SslMode) -> Result<Manager, Box<dyn Error>> {
    ensure_rustls_crypto_provider()?;

    let tls = if ssl_mode == SslMode::Disable {
        MakeRustlsConnect::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth(),
        )
    } else {
        let (tls, certificate_warnings) =
            MakeRustlsConnect::with_native_certs().map_err(|errors| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "unable to load a trusted PostgreSQL CA certificate store ({} errors)",
                        errors.len()
                    ),
                )
            })?;
        if !certificate_warnings.is_empty() {
            eprintln!(
                "PostgreSQL native certificate loader reported {} warning(s); valid certificates were retained.",
                certificate_warnings.len()
            );
        }
        tls
    };

    let mut config: PostgresConfig = database_url.parse()?;
    config.ssl_mode(ssl_mode);
    Ok(Manager::from_config(
        config,
        tls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    ))
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
    let metrics_config = metrics_server_config_from_env()?;
    let metrics_listener = match metrics_config {
        Some(config) => Some((
            TcpListener::bind(config.bind).await?,
            config.token_hash,
        )),
        None => None,
    };
    let database_url = required_env("DATABASE_URL")?;
    let worker_concurrency = positive_u32("AETHERFLOW_WORKER_CONCURRENCY", 4, 256)?;
    let worker_concurrency_usize = usize::try_from(worker_concurrency)?;
    let pool_size = worker_concurrency_usize.saturating_add(8).max(12);

    // The worker deliberately does not run DDL. The control-plane server owns
    // migrations; the worker starts only after the schema has been provisioned.
    let manager = postgres_manager(&database_url, database_ssl_mode()?)?;
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

    if let Some((listener, token_hash)) = metrics_listener {
        let bind = listener.local_addr()?;
        let metrics_state = WorkerMetricsState {
            publisher: publisher_metrics.clone(),
            consumer: consumer_metrics.clone(),
            token_hash,
        };
        let metrics_shutdown = transport_shutdown_rx.clone();
        transport_tasks.spawn(async move {
            (
                "Prometheus metrics listener",
                run_metrics_server(listener, metrics_state, metrics_shutdown).await,
            )
        });
        println!(
            "metrics_endpoint=http://{bind}/metrics (loopback-only; Bearer token required)"
        );
    } else {
        println!(
            "metrics_endpoint=disabled (set AETHERFLOW_METRICS_BIND and AETHERFLOW_METRICS_TOKEN to enable)"
        );
    }

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
    use super::{
        fail_once_output, hash_metrics_token, json_add_payload, parse_database_ssl_mode,
        parse_metrics_server_config, postgres_manager, worker_metrics_router, ConsumerMetrics,
        PublisherMetrics, WorkerMetricsState, uppercase_payload,
    };
    use axum::body::{to_bytes, Body};
    use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;
    use aetherflow_runtime::scheduler::TaskExecutionError;
    use deadpool_postgres::Pool;
    use tokio_postgres::config::SslMode;

    const TEST_METRICS_TOKEN: &str = "test-worker-metrics-token-32-characters-min";

    #[test]
    fn worker_metrics_listener_is_opt_in_loopback_only_and_requires_a_strong_token() {
        assert!(
            parse_metrics_server_config(None, None)
                .expect("disabled metrics listener")
                .is_none()
        );
        assert!(parse_metrics_server_config(None, Some(TEST_METRICS_TOKEN)).is_err());
        assert!(parse_metrics_server_config(Some("127.0.0.1:9464"), None).is_err());
        assert!(
            parse_metrics_server_config(
                Some("0.0.0.0:9464"),
                Some(TEST_METRICS_TOKEN)
            )
            .is_err()
        );
        assert!(
            parse_metrics_server_config(
                Some("127.0.0.1:9464"),
                Some("too-short")
            )
            .is_err()
        );

        let config = parse_metrics_server_config(
            Some("127.0.0.1:9464"),
            Some(TEST_METRICS_TOKEN),
        )
        .expect("valid loopback metrics configuration")
        .expect("metrics listener enabled");
        assert_eq!(config.bind, "127.0.0.1:9464".parse().expect("socket address"));
        assert_eq!(
            config.token_hash,
            hash_metrics_token(TEST_METRICS_TOKEN).expect("hash valid token")
        );
    }

    #[tokio::test]
    async fn worker_metrics_endpoint_authenticates_and_exports_bounded_nats_metrics() {
        let state = WorkerMetricsState {
            publisher: PublisherMetrics::default(),
            consumer: ConsumerMetrics::default(),
            token_hash: hash_metrics_token(TEST_METRICS_TOKEN).expect("hash valid token"),
        };
        let app = worker_metrics_router(state);

        let unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("valid unauthenticated metrics request"),
            )
            .await
            .expect("unauthorized metrics response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            unauthorized
                .headers()
                .get(WWW_AUTHENTICATE)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer")
        );
        assert_eq!(
            unauthorized
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store")
        );

        let invalid_token = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header(AUTHORIZATION, "Bearer definitely-not-the-configured-token")
                    .body(Body::empty())
                    .expect("valid wrong-token metrics request"),
            )
            .await
            .expect("wrong-token metrics response");
        assert_eq!(invalid_token.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header(AUTHORIZATION, format!("Bearer {TEST_METRICS_TOKEN}"))
                    .body(Body::empty())
                    .expect("valid authenticated metrics request"),
            )
            .await
            .expect("authenticated metrics response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).and_then(|value| value.to_str().ok()),
            Some("text/plain; version=0.0.4; charset=utf-8")
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store")
        );
        assert_eq!(
            response
                .headers()
                .get("x-content-type-options")
                .and_then(|value| value.to_str().ok()),
            Some("nosniff")
        );

        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("read metrics response body");
        let exposition = std::str::from_utf8(&body).expect("Prometheus exposition is UTF-8");
        assert!(exposition.contains("aetherflow_nats_publisher_confirmed_total 0"));
        assert!(exposition.contains("aetherflow_nats_consumer_processed_total 0"));
        assert!(!exposition.contains('{'), "metrics must not include labels");
    }

    #[test]
    fn database_tls_defaults_to_required_and_accepts_only_supported_modes() {
        assert_eq!(
            parse_database_ssl_mode(None).expect("default TLS mode"),
            SslMode::Require
        );
        assert_eq!(
            parse_database_ssl_mode(Some("disable")).expect("disabled TLS mode"),
            SslMode::Disable
        );
        assert_eq!(
            parse_database_ssl_mode(Some("prefer")).expect("preferred TLS mode"),
            SslMode::Prefer
        );
        assert_eq!(
            parse_database_ssl_mode(Some("require")).expect("required TLS mode"),
            SslMode::Require
        );
        assert!(parse_database_ssl_mode(Some("verify-full")).is_err());
        assert!(parse_database_ssl_mode(Some("unknown")).is_err());
    }

    #[tokio::test]
    async fn worker_postgres_rustls_accepts_trusted_ca_and_rejects_wrong_hostname() {
        let Ok(database_url) = std::env::var("AETHERFLOW_TLS_TEST_DATABASE_URL") else {
            eprintln!("AETHERFLOW_TLS_TEST_DATABASE_URL is not set; skipping PostgreSQL TLS test");
            return;
        };
        let Ok(hostname_mismatch_url) =
            std::env::var("AETHERFLOW_TLS_HOSTNAME_MISMATCH_DATABASE_URL")
        else {
            eprintln!(
                "AETHERFLOW_TLS_HOSTNAME_MISMATCH_DATABASE_URL is not set; skipping PostgreSQL TLS test"
            );
            return;
        };

        let trusted_manager =
            postgres_manager(&database_url, SslMode::Require).expect("build trusted TLS manager");
        let trusted_pool = Pool::builder(trusted_manager)
            .max_size(1)
            .build()
            .expect("build trusted TLS pool");
        let trusted_client = trusted_pool
            .get()
            .await
            .expect("connect through trusted PostgreSQL TLS");
        let row = trusted_client
            .query_one(
                "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
                &[],
            )
            .await
            .expect("inspect PostgreSQL TLS session");
        assert!(
            row.get::<_, bool>(0),
            "worker PostgreSQL connection must use TLS"
        );
        drop(trusted_client);
        drop(trusted_pool);

        let mismatch_manager = postgres_manager(&hostname_mismatch_url, SslMode::Require)
            .expect("build hostname-mismatch TLS manager");
        let mismatch_pool = Pool::builder(mismatch_manager)
            .max_size(1)
            .build()
            .expect("build hostname-mismatch TLS pool");
        assert!(
            mismatch_pool.get().await.is_err(),
            "worker PostgreSQL TLS must reject a trusted certificate with the wrong hostname"
        );
    }

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
