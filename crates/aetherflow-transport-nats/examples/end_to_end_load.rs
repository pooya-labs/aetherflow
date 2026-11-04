//! Reproducible end-to-end PostgreSQL and JetStream load profile.
//!
//! This executable drives workflow submissions through PostgreSQL persistence,
//! durable workers, the transactional outbox, JetStream publication, and the
//! PostgreSQL-backed inbox consumer. It is an operational load profile, not a
//! production capacity benchmark or an HTTP/API benchmark.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aetherflow_core::domain::{
    NodeId, NodeIncarnationId, RetryPolicy, TaskId, WorkerId, WorkflowId, WorkflowRunId,
};
use aetherflow_runtime::repository::{TaskInput, TaskOutput};
use aetherflow_runtime::scheduler::{TaskContext, TaskExecutionError, TaskRegistry};
use aetherflow_runtime::workflow::{WorkflowDefinition, WorkflowTask};
use aetherflow_storage_postgres::{
    OutboxDispatcherConfig, PgInboxRepository, PgMembershipRepository, PgStore, PgTaskRepository,
    PgWorkflowCoordinator, PostgresOutboxDispatcher, PostgresTaskWorker, PostgresWorkerConfig,
};
use aetherflow_transport_nats::{
    EventHandlerFuture, JetStreamEvent, JetStreamEventHandler, NatsJetStreamConsumer,
    NatsJetStreamPublisher,
};
use async_nats::jetstream::{self, consumer::pull::Config as PullConsumerConfig};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio::sync::watch;
use tokio::time::{sleep, timeout};
use tokio_postgres::{Config as PostgresConfig, NoTls};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkflowShape {
    FanOut,
    Chain,
}

impl WorkflowShape {
    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        match value {
            "fan_out" => Ok(Self::FanOut),
            "chain" => Ok(Self::Chain),
            other => Err(invalid(format!(
                "AETHERFLOW_LOAD_SHAPE must be fan_out or chain, found {other:?}"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::FanOut => "fan_out",
            Self::Chain => "chain",
        }
    }
}

struct LoadConfig {
    database_url: String,
    nats_urls: String,
    shape: WorkflowShape,
    workflows: usize,
    tasks_per_workflow: usize,
    payload_bytes: usize,
    worker_concurrency: usize,
    producer_rate: u64,
    handler_delay: Duration,
    timeout: Duration,
    output: String,
}

impl LoadConfig {
    fn from_env() -> Result<Self, Box<dyn Error>> {
        let database_url = required_env("AETHERFLOW_LOAD_DATABASE_URL")?;
        let nats_urls = required_env("AETHERFLOW_LOAD_NATS_URL")?;
        let shape = WorkflowShape::parse(&required_env("AETHERFLOW_LOAD_SHAPE")?)?;
        let workflows = env_number("AETHERFLOW_LOAD_WORKFLOWS", 16_usize)?;
        let tasks_per_workflow = env_number("AETHERFLOW_LOAD_TASKS", 4_usize)?;
        let payload_bytes = env_number("AETHERFLOW_LOAD_PAYLOAD_BYTES", 256_usize)?;
        let worker_concurrency = env_number("AETHERFLOW_LOAD_WORKERS", 4_usize)?;
        let producer_rate = env_number("AETHERFLOW_LOAD_PRODUCER_RATE", 8_u64)?;
        let handler_delay_ms = env_number("AETHERFLOW_LOAD_HANDLER_DELAY_MS", 1_u64)?;
        let timeout_secs = env_number("AETHERFLOW_LOAD_TIMEOUT_SECS", 120_u64)?;
        let output = std::env::var("AETHERFLOW_LOAD_OUTPUT")
            .unwrap_or_else(|_| "target/aetherflow-end-to-end-load.csv".to_owned());

        if workflows == 0 || workflows > 10_000 {
            return Err(invalid("workflows must be between 1 and 10000"));
        }
        if tasks_per_workflow == 0 || tasks_per_workflow > 256 {
            return Err(invalid("tasks per workflow must be between 1 and 256"));
        }
        if workflows.saturating_mul(tasks_per_workflow) > 100_000 {
            return Err(invalid("total tasks per profile must not exceed 100000"));
        }
        if payload_bytes > 1_048_576 {
            return Err(invalid(
                "payload bytes must not exceed the 1 MiB task limit",
            ));
        }
        if worker_concurrency == 0 || worker_concurrency > 128 {
            return Err(invalid("workers must be between 1 and 128"));
        }
        if producer_rate == 0 || producer_rate > 10_000 {
            return Err(invalid(
                "producer rate must be between 1 and 10000 workflows/second",
            ));
        }
        if timeout_secs == 0 || timeout_secs > 3600 {
            return Err(invalid("timeout seconds must be between 1 and 3600"));
        }
        if handler_delay_ms > 10_000 {
            return Err(invalid("handler delay must not exceed 10000 ms"));
        }

        Ok(Self {
            database_url,
            nats_urls,
            shape,
            workflows,
            tasks_per_workflow,
            payload_bytes,
            worker_concurrency,
            producer_rate,
            handler_delay: Duration::from_millis(handler_delay_ms),
            timeout: Duration::from_secs(timeout_secs),
            output,
        })
    }
}

#[derive(Clone)]
struct CountingEventHandler {
    processed: Arc<AtomicUsize>,
}

impl JetStreamEventHandler for CountingEventHandler {
    fn handle<'a>(&'a self, event: &'a JetStreamEvent) -> EventHandlerFuture<'a> {
        let processed = Arc::clone(&self.processed);
        Box::pin(async move {
            std::hint::black_box(event.payload());
            processed.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    }
}

fn required_env(name: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(name).map_err(|_| invalid(format!("{name} is required")))
}

fn env_number<T>(name: &str, default: T) -> Result<T, Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(value) => value
            .parse::<T>()
            .map_err(|error| invalid(format!("{name}={value:?} is invalid: {error}"))),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(invalid(format!("cannot read {name}: {error}"))),
    }
}

fn invalid(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.into(),
    ))
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}_{nanos}", std::process::id())
}

fn nats_seeds(urls: &str) -> Result<Vec<async_nats::ServerAddr>, Box<dyn Error>> {
    let seeds = urls
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::parse::<async_nats::ServerAddr>)
        .collect::<Result<Vec<_>, _>>()?;
    if seeds.is_empty() {
        return Err(invalid(
            "AETHERFLOW_LOAD_NATS_URL must contain at least one NATS URL",
        ));
    }
    Ok(seeds)
}

fn workflow_definition(
    config: &LoadConfig,
    handler_registry: &mut TaskRegistry,
) -> Result<WorkflowDefinition, Box<dyn Error>> {
    let workflow_id = WorkflowId::parse("load_workflow_v1")?;
    let input = TaskInput::new(vec![b'x'; config.payload_bytes])?;
    let mut tasks = Vec::with_capacity(config.tasks_per_workflow);

    for index in 0..config.tasks_per_workflow {
        let task_id = TaskId::parse(format!("load_task_{index:03}"))?;
        let dependencies = match config.shape {
            WorkflowShape::FanOut => Vec::new(),
            WorkflowShape::Chain if index > 0 => {
                vec![TaskId::parse(format!("load_task_{:03}", index - 1))?]
            }
            WorkflowShape::Chain => Vec::new(),
        };

        let handler_delay = config.handler_delay;
        handler_registry.register(task_id.clone(), move |context: TaskContext| async move {
            std::hint::black_box(context.input().len());
            if !handler_delay.is_zero() {
                sleep(handler_delay).await;
            }
            Ok::<TaskOutput, TaskExecutionError>(TaskOutput::empty())
        })?;
        tasks.push(WorkflowTask::new(task_id, dependencies).with_input(input.clone()));
    }

    Ok(WorkflowDefinition::new(workflow_id, tasks)?)
}

async fn run() -> Result<(), Box<dyn Error>> {
    let config = LoadConfig::from_env()?;
    let suffix = unique_suffix();
    let started_at = Instant::now();
    let wall_started_at = SystemTime::now();

    let postgres_config: PostgresConfig = config.database_url.parse()?;
    let manager = Manager::from_config(
        postgres_config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(manager)
        .max_size(32)
        .build()
        .map_err(|error| invalid(format!("create PostgreSQL pool: {error}")))?;

    let migration = PgStore::from_pool(pool.clone()).migrate().await?;
    println!("schema: {migration:?}");

    let nats_client = async_nats::connect(nats_seeds(&config.nats_urls)?).await?;
    let jetstream = jetstream::new(nats_client);
    let stream_name = format!("AFLOW_LOAD_{}", suffix.to_ascii_uppercase());
    let subject = format!("aetherflow.load.{suffix}.events");
    let durable_name = format!("aflow_load_{}", suffix.replace('_', ""));
    let stream = jetstream
        .create_stream(jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![subject.clone()],
            num_replicas: 3,
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        })
        .await?;
    let pull_consumer = stream
        .create_consumer(PullConsumerConfig {
            durable_name: Some(durable_name.clone()),
            filter_subject: subject.clone(),
            ack_wait: Duration::from_secs(2),
            max_ack_pending: 1024,
            ..Default::default()
        })
        .await?;

    let repository = Arc::new(PgTaskRepository::from_pool(pool.clone()));
    let coordinator = PgWorkflowCoordinator::new(Arc::clone(&repository));
    let membership = Arc::new(PgMembershipRepository::from_pool(pool.clone()));
    let node_id = NodeId::parse(format!("load-node-{suffix}"))?;
    let incarnation_id = NodeIncarnationId::parse(format!("load-incarnation-{suffix}"))?;
    let worker_id = WorkerId::parse(format!("load-worker-{suffix}"))?;
    let node = membership
        .join_node(&node_id, &incarnation_id, Duration::from_secs(90))
        .await?;
    let worker_capacity = u32::try_from(config.worker_concurrency)?;
    let registration = membership
        .register_worker(
            &node,
            &worker_id,
            NonZeroU32::new(worker_capacity)
                .ok_or_else(|| invalid("worker count must be positive"))?,
            Duration::from_secs(60),
        )
        .await?;

    let mut registry = TaskRegistry::new();
    let definition = workflow_definition(&config, &mut registry)?;
    let worker_config = PostgresWorkerConfig::new(
        NonZeroUsize::new(config.worker_concurrency)
            .ok_or_else(|| invalid("worker concurrency must be positive"))?,
        Duration::from_millis(3),
        Duration::from_secs(30),
        Duration::from_secs(10),
        None,
    )?;
    let worker = PostgresTaskWorker::new(
        Arc::clone(&repository),
        Arc::clone(&membership),
        registration,
        registry,
        worker_config,
    );

    let publisher = Arc::new(NatsJetStreamPublisher::new(jetstream, subject));
    let dispatcher = PostgresOutboxDispatcher::new(
        Arc::clone(&repository),
        WorkerId::parse(format!("load-publisher-{suffix}"))?,
        publisher,
        OutboxDispatcherConfig::new(
            128,
            Duration::from_millis(3),
            Duration::from_secs(30),
            Duration::from_millis(50),
            Duration::from_secs(2),
        )?,
    );

    let processed_events = Arc::new(AtomicUsize::new(0));
    let consumer = NatsJetStreamConsumer::new(
        pull_consumer,
        PgInboxRepository::from_pool(pool.clone()),
        durable_name.clone(),
        WorkerId::parse(format!("load-consumer-{suffix}"))?,
        Duration::from_secs(30),
        Arc::new(CountingEventHandler {
            processed: Arc::clone(&processed_events),
        }),
    )?;
    let consumer_metrics = consumer.metrics();

    let (worker_shutdown, worker_shutdown_rx) = watch::channel(false);
    let worker_task = tokio::spawn(async move {
        worker
            .run(worker_shutdown_rx)
            .await
            .map_err(|error| error.to_string())
    });
    let (dispatcher_shutdown, mut dispatcher_shutdown_rx) = watch::channel(false);
    let dispatcher_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = dispatcher_shutdown_rx.changed() => {
                    if changed.is_err() || *dispatcher_shutdown_rx.borrow_and_update() {
                        return Ok::<(), String>(());
                    }
                }
                result = dispatcher.dispatch_once() => {
                    result.map_err(|error| error.to_string())?;
                    sleep(Duration::from_millis(3)).await;
                }
            }
        }
    });
    let (consumer_shutdown, consumer_shutdown_rx) = watch::channel(false);
    let consumer_task = tokio::spawn(async move {
        consumer
            .run(consumer_shutdown_rx)
            .await
            .map_err(|error| error.to_string())
    });

    // Wait for the pull consumer to subscribe before creating the first outbox row.
    sleep(Duration::from_millis(100)).await;

    let retry_policy = RetryPolicy::new(
        NonZeroU32::new(1).ok_or_else(|| invalid("invalid retry policy"))?,
        0,
        0,
    )?;
    let mut submissions = Vec::with_capacity(config.workflows);
    let producer_interval = Duration::from_secs_f64(1.0 / config.producer_rate as f64);

    for index in 0..config.workflows {
        let workflow_run_id = WorkflowRunId::parse(format!("load-{suffix}-{index:06}"))?;
        let submitted_at = Instant::now();
        coordinator
            .submit_workflow(&workflow_run_id, &definition, retry_policy, None)
            .await?;
        submissions.push((workflow_run_id.as_str().to_owned(), submitted_at));
        if index + 1 < config.workflows {
            sleep(producer_interval).await;
        }
    }

    let workflow_ids = submissions
        .iter()
        .map(|(workflow_id, _)| workflow_id.clone())
        .collect::<Vec<_>>();
    let submitted_at = submissions
        .iter()
        .map(|(workflow_id, instant)| (workflow_id.clone(), *instant))
        .collect::<HashMap<_, _>>();
    let deadline = started_at + config.timeout;
    let mut completed = HashSet::with_capacity(config.workflows);
    let mut completion_latencies_ms = Vec::with_capacity(config.workflows);
    let client = pool.get().await?;

    while completed.len() < config.workflows {
        if Instant::now() >= deadline {
            return Err(invalid(format!(
                "workflow completion timed out: {}/{} succeeded",
                completed.len(),
                config.workflows
            )));
        }
        if worker_task.is_finished() {
            return Err(invalid(
                "PostgreSQL task worker exited before the load profile completed",
            ));
        }
        if consumer_task.is_finished() {
            return Err(invalid(
                "JetStream inbox consumer exited before the load profile completed",
            ));
        }
        if dispatcher_task.is_finished() {
            return Err(invalid(
                "PostgreSQL outbox dispatcher exited before the load profile completed",
            ));
        }

        let rows = client
            .query(
                "SELECT workflow_run_id, status
                 FROM workflow_runs
                 WHERE workflow_run_id = ANY($1::TEXT[])",
                &[&workflow_ids],
            )
            .await?;
        let observed_at = Instant::now();

        for row in rows {
            let workflow_id: String = row.get(0);
            let status: String = row.get(1);
            match status.as_str() {
                "succeeded" => {
                    if completed.insert(workflow_id.clone()) {
                        if let Some(submission) = submitted_at.get(&workflow_id) {
                            completion_latencies_ms.push(
                                observed_at.duration_since(*submission).as_secs_f64() * 1000.0,
                            );
                        }
                    }
                }
                "failed" | "cancelled" | "timed_out" => {
                    return Err(invalid(format!(
                        "workflow {workflow_id} reached terminal failure status {status}"
                    )));
                }
                _ => {}
            }
        }
        if completed.len() < config.workflows {
            sleep(Duration::from_millis(5)).await;
        }
    }

    // Workflow success is not enough: wait until every transactional outbox row
    // is published and every published event has a committed inbox outcome.
    let (published_events, pending_events, processed_inbox_rows) = loop {
        if Instant::now() >= deadline {
            return Err(invalid("outbox or inbox drain timed out"));
        }
        if consumer_task.is_finished() || dispatcher_task.is_finished() {
            return Err(invalid(
                "transport loop stopped before all events were processed",
            ));
        }

        let outbox = client
            .query_one(
                "SELECT count(*) FILTER (WHERE published_at IS NOT NULL),
                        count(*) FILTER (WHERE published_at IS NULL)
                 FROM outbox_events",
                &[],
            )
            .await?;
        let published: i64 = outbox.get(0);
        let pending: i64 = outbox.get(1);
        let inbox: i64 = client
            .query_one(
                "SELECT count(*) FROM consumer_inbox
                 WHERE consumer_name = $1 AND status = 'processed'",
                &[&durable_name],
            )
            .await?
            .get(0);
        let in_process = i64::try_from(consumer_metrics.snapshot().processed)?;
        if pending == 0 && published > 0 && inbox == published && in_process == published {
            break (published, pending, inbox);
        }
        sleep(Duration::from_millis(10)).await;
    };

    drop(client);
    let elapsed = started_at.elapsed();
    let task_count = config.workflows * config.tasks_per_workflow;
    let events_per_second = published_events as f64 / elapsed.as_secs_f64().max(f64::EPSILON);
    let workflow_per_second = config.workflows as f64 / elapsed.as_secs_f64().max(f64::EPSILON);
    let tasks_per_second = task_count as f64 / elapsed.as_secs_f64().max(f64::EPSILON);
    let (p50, p95, mean) = latency_summary(&mut completion_latencies_ms);

    let row = format!(
        "{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{},{},{},{}\n",
        config.shape.as_str(),
        config.workflows,
        config.tasks_per_workflow,
        task_count,
        config.payload_bytes,
        config.worker_concurrency,
        config.producer_rate,
        elapsed.as_secs_f64() * 1000.0,
        workflow_per_second,
        tasks_per_second,
        events_per_second,
        p50,
        p95,
        mean,
        published_events,
        processed_inbox_rows,
        consumer_metrics.snapshot().duplicates,
        0
    );
    let csv = concat!(
        "shape,workflows,tasks_per_workflow,total_tasks,payload_bytes,worker_concurrency,",
        "producer_rate_workflows_per_second,elapsed_ms,workflows_per_second,tasks_per_second,",
        "events_per_second,p50_workflow_ms,p95_workflow_ms,mean_workflow_ms,",
        "outbox_events_published,inbox_events_processed,duplicate_events,errors\n"
    );
    fs::write(&config.output, format!("{csv}{row}"))?;

    let _ = worker_shutdown.send(true);
    let _ = dispatcher_shutdown.send(true);
    let _ = consumer_shutdown.send(true);
    timeout(Duration::from_secs(20), worker_task)
        .await??
        .map_err(invalid)?;
    timeout(Duration::from_secs(10), dispatcher_task)
        .await??
        .map_err(invalid)?;
    timeout(Duration::from_secs(10), consumer_task)
        .await??
        .map_err(invalid)?;

    println!(
        "PASS: shape={}, workflows={}, tasks={}, events={}, {:.2} workflows/s, {:.2} tasks/s, p95={:.2} ms; CSV={}",
        config.shape.as_str(),
        config.workflows,
        task_count,
        published_events,
        workflow_per_second,
        tasks_per_second,
        p95,
        config.output
    );
    let _ = (pending_events, wall_started_at);
    Ok(())
}

fn latency_summary(samples: &mut [f64]) -> (f64, f64, f64) {
    samples.sort_by(f64::total_cmp);
    let count = samples.len();
    let p50_index = count.saturating_mul(50).div_ceil(100).saturating_sub(1);
    let p95_index = count.saturating_mul(95).div_ceil(100).saturating_sub(1);
    let mean = samples.iter().sum::<f64>() / count.max(1) as f64;
    (
        samples[p50_index.min(count.saturating_sub(1))],
        samples[p95_index.min(count.saturating_sub(1))],
        mean,
    )
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("end-to-end load profile failed: {error}");
        std::process::exit(1);
    }
}
