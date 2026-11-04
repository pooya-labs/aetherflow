//! Exercises a restored PostgreSQL database and JetStream target through the
//! live control-plane API, durable worker, transactional outbox, and inbox.
use std::error::Error;
use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use aetherflow_core::domain::{
    NodeId, NodeIncarnationId, TaskId, TaskRunId, TaskStatus, WorkerId, WorkflowRunId,
    WorkflowStatus,
};
use aetherflow_runtime::repository::TaskOutput;
use aetherflow_runtime::scheduler::{TaskContext, TaskExecutionError, TaskRegistry};
use aetherflow_storage_postgres::{
    OutboxDispatcherConfig, PgInboxRepository, PgMembershipRepository, PgTaskRepository,
    PostgresOutboxDispatcher, PostgresTaskWorker, PostgresWorkerConfig,
};
use aetherflow_transport_nats::{
    EventHandlerFuture, JetStreamEvent, JetStreamEventHandler, NatsJetStreamConsumer,
    NatsJetStreamPublisher,
};
use async_nats::jetstream::{
    self,
    consumer::{DeliverPolicy, pull::Config as PullConsumerConfig},
};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio::sync::watch;
use tokio::time::{Instant, sleep, timeout};
use tokio_postgres::{Config as PostgresConfig, NoTls};

const WORKER_ID: &str = "restore_smoke_worker";
const NODE_ID: &str = "restore_smoke_node";
const INCARNATION_ID: &str = "restore_smoke_incarnation";
const CONSUMER_NAME: &str = "restore_smoke_consumer";
const STREAM_NAME: &str = "AETHERFLOW_RECOVERY_EVENTS";
const SUBJECT: &str = "aetherflow.recovery.events";
const EXPECTED_INPUT: &[u8] = b"restore-smoke-input";
const EXPECTED_OUTPUT: &[u8] = b"restore-smoke-output";

#[derive(Clone, Default)]
struct RecoveryEventHandler {
    processed: Arc<AtomicUsize>,
}

impl JetStreamEventHandler for RecoveryEventHandler {
    fn handle<'a>(&'a self, event: &'a JetStreamEvent) -> EventHandlerFuture<'a> {
        let processed = Arc::clone(&self.processed);
        Box::pin(async move {
            if event.schema_version() != 1 {
                return Err(format!(
                    "unsupported event schema version {}",
                    event.schema_version()
                ));
            }
            processed.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    }
}

fn failure(message: impl Into<String>) -> Box<dyn Error> {
    io::Error::other(message.into()).into()
}

fn required_env(name: &str) -> Result<String, std::io::Error> {
    std::env::var(name).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{name} is required"),
        )
    })
}

fn postgres_pool(database_url: &str, max_size: usize) -> Result<Pool, Box<dyn Error>> {
    let config: PostgresConfig = database_url.parse()?;
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Ok(Pool::builder(manager).max_size(max_size).build()?)
}

async fn join_task(
    handle: tokio::task::JoinHandle<Result<(), String>>,
    name: &str,
) -> Result<(), Box<dyn Error>> {
    let joined = timeout(Duration::from_secs(15), handle)
        .await
        .map_err(|_| failure(format!("{name} did not stop within 15 seconds")))?;
    let result = joined.map_err(|error| failure(format!("{name} task panicked: {error}")))?;
    result.map_err(failure)?;
    Ok(())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let runtime_database_url = required_env("AETHERFLOW_RESTORE_DATABASE_URL")?;
    let admin_database_url = required_env("AETHERFLOW_RESTORE_ADMIN_DATABASE_URL")?;
    let nats_url = required_env("AETHERFLOW_RESTORE_NATS_URL")?;
    let workflow_run_id =
        WorkflowRunId::parse(required_env("AETHERFLOW_RESTORE_WORKFLOW_RUN_ID")?)?;
    let task_run_id = TaskRunId::parse(required_env("AETHERFLOW_RESTORE_TASK_RUN_ID")?)?;

    let runtime_pool = postgres_pool(&runtime_database_url, 16)?;
    let admin_pool = postgres_pool(&admin_database_url, 4)?;
    let repository = Arc::new(PgTaskRepository::from_pool(runtime_pool.clone()));
    let membership = Arc::new(PgMembershipRepository::from_pool(runtime_pool.clone()));

    let workflow = repository.get_workflow_run(&workflow_run_id).await?;
    if workflow.status() != WorkflowStatus::Running {
        return Err(failure(format!(
            "expected newly submitted workflow to be running, found {:?}",
            workflow.status()
        )));
    }
    if workflow.task_runs().len() != 1
        || workflow.task_runs()[0].task_run_id() != &task_run_id
        || workflow.task_runs()[0].status() != TaskStatus::Pending
    {
        return Err(failure(
            "restored API submission did not materialize the expected pending task",
        ));
    }

    let task_id = TaskId::parse("restore_smoke_task")?;
    let expected_input = EXPECTED_INPUT.to_vec();
    let expected_output = TaskOutput::new(EXPECTED_OUTPUT.to_vec())?;
    let mut registry = TaskRegistry::new();
    registry.register(task_id, move |context: TaskContext| {
        let expected_input = expected_input.clone();
        let output = expected_output.clone();
        async move {
            if context.input().as_bytes() != expected_input.as_slice() {
                return Err(TaskExecutionError::permanent(
                    "restored task input did not match the API submission",
                ));
            }
            Ok(output)
        }
    })?;

    let node = membership
        .join_node(
            &NodeId::parse(NODE_ID)?,
            &NodeIncarnationId::parse(INCARNATION_ID)?,
            Duration::from_secs(90),
        )
        .await?;
    let registration = membership
        .register_worker(
            &node,
            &WorkerId::parse(WORKER_ID)?,
            NonZeroU32::new(1).expect("one is non-zero"),
            Duration::from_secs(60),
        )
        .await?;

    // The HTTP operator endpoint creates legacy-default work. Registered
    // workers have no tenant GUC by default, so RLS requires an explicit
    // worker grant before this worker can promote or claim that work.
    // This admin-side grant is scoped to the disposable restore fixture.
    let admin_client = admin_pool.get().await?;
    admin_client
        .execute(
            "INSERT INTO worker_tenant_grants (tenant_id, worker_id, status)
             VALUES ('legacy-default', $1, 'active')
             ON CONFLICT (tenant_id, worker_id) DO UPDATE
             SET status = 'active',
                 granted_at = clock_timestamp(),
                 expires_at = NULL,
                 revoked_at = NULL",
            &[&WORKER_ID],
        )
        .await?;
    let worker_config = PostgresWorkerConfig::new(
        NonZeroUsize::new(1).expect("one is non-zero"),
        Duration::from_millis(5),
        Duration::from_secs(30),
        Duration::from_secs(5),
        Some(Duration::from_secs(10)),
    )?;
    let worker = PostgresTaskWorker::new(
        Arc::clone(&repository),
        Arc::clone(&membership),
        registration,
        registry,
        worker_config,
    );

    let nats_client = async_nats::connect(nats_url).await?;
    let jetstream = jetstream::new(nats_client);
    let stream = jetstream.get_stream(STREAM_NAME).await?;
    let pull_consumer = stream
        .create_consumer(PullConsumerConfig {
            durable_name: Some(CONSUMER_NAME.to_owned()),
            // The restored stream already contains retained fixture messages.
            // This drill must count only events produced by the new workflow.
            deliver_policy: DeliverPolicy::New,
            filter_subject: SUBJECT.to_owned(),
            ack_wait: Duration::from_secs(5),
            max_ack_pending: 128,
            ..Default::default()
        })
        .await?;
    let event_handler = Arc::new(RecoveryEventHandler::default());
    let consumer = NatsJetStreamConsumer::new(
        pull_consumer,
        PgInboxRepository::from_pool(runtime_pool.clone()),
        CONSUMER_NAME,
        WorkerId::parse("restore_smoke_consumer_worker")?,
        Duration::from_secs(30),
        Arc::clone(&event_handler),
    )?;
    let consumer_metrics = consumer.metrics();

    let publisher = Arc::new(NatsJetStreamPublisher::new(jetstream, SUBJECT));
    let dispatcher = PostgresOutboxDispatcher::new(
        Arc::clone(&repository),
        WorkerId::parse("restore_smoke_publisher")?,
        publisher,
        OutboxDispatcherConfig::new(
            32,
            Duration::from_millis(5),
            Duration::from_secs(30),
            Duration::from_millis(50),
            Duration::from_secs(2),
        )?,
    );

    let (worker_shutdown, worker_shutdown_rx) = watch::channel(false);
    let worker_task = tokio::spawn(async move {
        worker
            .run(worker_shutdown_rx)
            .await
            .map_err(|error| error.to_string())
    });
    let (consumer_shutdown, consumer_shutdown_rx) = watch::channel(false);
    let consumer_task = tokio::spawn(async move {
        consumer
            .run(consumer_shutdown_rx)
            .await
            .map_err(|error| error.to_string())
    });

    let aggregate_ids = vec![
        workflow_run_id.as_str().to_owned(),
        task_run_id.as_str().to_owned(),
    ];
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut final_counts = (0_i64, 0_i64, 0_i64);
    let mut final_metrics = (0_i64, 0_i64, 0_i64, 0_i64);

    loop {
        if Instant::now() >= deadline {
            let workflow_state = admin_client
                .query_one(
                    "SELECT status, version FROM workflow_runs
                     WHERE tenant_id = 'legacy-default' AND workflow_run_id = $1",
                    &[&workflow_run_id.as_str()],
                )
                .await?;
            let task_state = admin_client
                .query_one(
                    "SELECT status, version, attempts_started, octet_length(output_payload),
                            last_error_kind, last_error_message, claimed_by, lease_expires_at
                     FROM task_runs
                     WHERE tenant_id = 'legacy-default' AND task_run_id = $1",
                    &[&task_run_id.as_str()],
                )
                .await?;
            let event_rows = admin_client
                .query(
                    "SELECT event_type, count(*)::BIGINT,
                            count(*) FILTER (WHERE published_at IS NOT NULL)::BIGINT
                     FROM outbox_events
                     WHERE tenant_id = 'legacy-default' AND aggregate_id = ANY($1::TEXT[])
                     GROUP BY event_type ORDER BY event_type",
                    &[&aggregate_ids],
                )
                .await?;
            let inbox_rows = admin_client
                .query(
                    "SELECT status, count(*)::BIGINT
                     FROM consumer_inbox
                     WHERE tenant_id = 'legacy-default' AND consumer_name = $1
                       AND event_id IN (
                           SELECT event_id::TEXT FROM outbox_events
                           WHERE tenant_id = 'legacy-default' AND aggregate_id = ANY($2::TEXT[])
                       )
                     GROUP BY status ORDER BY status",
                    &[&CONSUMER_NAME, &aggregate_ids],
                )
                .await?;
            let attempt_rows = admin_client
                .query(
                    "SELECT outcome, count(*)::BIGINT
                     FROM task_attempts
                     WHERE tenant_id = 'legacy-default' AND task_run_id = $1
                     GROUP BY outcome ORDER BY outcome",
                    &[&task_run_id.as_str()],
                )
                .await?;
            let events = event_rows
                .iter()
                .map(|row| {
                    format!(
                        "{}={}/{}",
                        row.get::<_, String>(0),
                        row.get::<_, i64>(2),
                        row.get::<_, i64>(1)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let inbox_states = inbox_rows
                .iter()
                .map(|row| format!("{}={}", row.get::<_, String>(0), row.get::<_, i64>(1)))
                .collect::<Vec<_>>()
                .join(",");
            let attempts = attempt_rows
                .iter()
                .map(|row| format!("{}={}", row.get::<_, String>(0), row.get::<_, i64>(1)))
                .collect::<Vec<_>>()
                .join(",");
            return Err(failure(format!(
                "restore execution did not drain before the deadline; published={}, pending={}, inbox={}, processed={}, duplicates={}, busy={}, handler_failures={}; workflow={:?}/v{}; task={:?}/v{}, attempts_started={}, output_bytes={:?}, error={:?}:{:?}, claimed_by={:?}, lease={:?}; events=[{}]; inbox_states=[{}]; attempts=[{}]",
                final_counts.0,
                final_counts.1,
                final_counts.2,
                final_metrics.0,
                final_metrics.1,
                final_metrics.2,
                final_metrics.3,
                workflow_state.get::<_, String>(0),
                workflow_state.get::<_, i64>(1),
                task_state.get::<_, String>(0),
                task_state.get::<_, i64>(1),
                task_state.get::<_, i32>(2),
                task_state.get::<_, Option<i32>>(3),
                task_state.get::<_, Option<String>>(4),
                task_state.get::<_, Option<String>>(5),
                task_state.get::<_, Option<String>>(6),
                task_state.get::<_, Option<std::time::SystemTime>>(7),
                events,
                inbox_states,
                attempts
            )));
        }
        if worker_task.is_finished() {
            return Err(failure(
                "PostgreSQL worker exited before recovery verification completed",
            ));
        }
        if consumer_task.is_finished() {
            return Err(failure(
                "JetStream inbox consumer exited before recovery verification completed",
            ));
        }

        dispatcher.dispatch_once().await?;

        let workflow = repository.get_workflow_run(&workflow_run_id).await?;
        match workflow.status() {
            WorkflowStatus::Succeeded => {
                if workflow.task_runs().len() != 1 {
                    return Err(failure(
                        "successful restore workflow has an unexpected task count",
                    ));
                }
                let task = &workflow.task_runs()[0];
                if task.task_run_id() != &task_run_id
                    || task.status() != TaskStatus::Succeeded
                    || task.output().map(|output| output.as_bytes()) != Some(EXPECTED_OUTPUT)
                {
                    return Err(failure(
                        "restored task did not persist its expected successful output",
                    ));
                }

                let outbox = admin_client
                    .query_one(
                        "SELECT count(*) FILTER (WHERE published_at IS NOT NULL)::BIGINT,
                                count(*) FILTER (WHERE published_at IS NULL)::BIGINT
                         FROM outbox_events
                         WHERE tenant_id = 'legacy-default'
                           AND aggregate_id = ANY($1::TEXT[])",
                        &[&aggregate_ids],
                    )
                    .await?;
                let published: i64 = outbox.get(0);
                let pending: i64 = outbox.get(1);
                let inbox: i64 = admin_client
                    .query_one(
                        "SELECT count(*)::BIGINT
                         FROM consumer_inbox AS inbox
                         JOIN outbox_events AS event
                           ON event.tenant_id = inbox.tenant_id
                          AND event.event_id::TEXT = inbox.event_id
                         WHERE inbox.tenant_id = 'legacy-default'
                           AND inbox.consumer_name = $2
                           AND inbox.status = 'processed'
                           AND event.aggregate_id = ANY($1::TEXT[])",
                        &[&aggregate_ids, &CONSUMER_NAME],
                    )
                    .await?
                    .get(0);
                final_counts = (published, pending, inbox);
                let metrics = consumer_metrics.snapshot();
                let in_process = i64::try_from(metrics.processed)?;
                let duplicates = i64::try_from(metrics.duplicates)?;
                final_metrics = (
                    in_process,
                    duplicates,
                    i64::try_from(metrics.busy)?,
                    i64::try_from(metrics.handler_failures)?,
                );

                if published > 0
                    && pending == 0
                    && inbox == published
                    && in_process == published
                    && duplicates == 0
                {
                    break;
                }
            }
            WorkflowStatus::Failed | WorkflowStatus::Cancelled | WorkflowStatus::TimedOut => {
                return Err(failure(format!(
                    "restored workflow reached terminal failure status {:?}",
                    workflow.status()
                )));
            }
            WorkflowStatus::Pending | WorkflowStatus::Running => {}
        }

        sleep(Duration::from_millis(10)).await;
    }

    worker_shutdown
        .send(true)
        .map_err(|_| failure("worker shutdown channel closed"))?;
    consumer_shutdown
        .send(true)
        .map_err(|_| failure("consumer shutdown channel closed"))?;
    join_task(worker_task, "PostgreSQL worker").await?;
    join_task(consumer_task, "JetStream consumer").await?;
    membership.drain_node(&node).await?;

    println!(
        "PASS: restored API submission executed through PostgreSQL worker; task output persisted; {} outbox events were published and committed to the PostgreSQL inbox with zero duplicates.",
        final_counts.0
    );
    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("restore recovery verification failed: {error:#}");
        std::process::exit(1);
    }
}
