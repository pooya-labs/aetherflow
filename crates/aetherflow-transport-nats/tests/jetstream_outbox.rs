use std::io::Write;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aetherflow_core::domain::{
    AttemptId, NodeId, NodeIncarnationId, RetryPolicy, TaskId, TaskRunId, TaskStatus, WorkerId,
    WorkflowId, WorkflowRunId, WorkflowStatus,
};
use aetherflow_runtime::repository::{TaskInput, TaskOutput};
use aetherflow_runtime::scheduler::{TaskContext, TaskRegistry};
use aetherflow_storage_postgres::{
    InboxClaimResult, MigrationOutcome, NewTaskRun, OutboxClaim, OutboxDispatchReport,
    OutboxDispatcherConfig, OutboxPublishError, OutboxPublishFuture, OutboxPublisher,
    PgInboxRepository, PgMembershipRepository, PgStore, PgTaskRepository, PostgresOutboxDispatcher,
    PostgresTaskWorker, PostgresWorkerConfig,
};
use aetherflow_transport_nats::{
    EventHandlerFuture, JetStreamEvent, JetStreamEventHandler, NatsJetStreamConsumer,
    NatsJetStreamPublisher,
};
use async_nats::jetstream::{
    self, consumer::PullConsumer, consumer::pull::Config as PullConsumerConfig,
};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures_util::StreamExt;
use tokio::sync::{Mutex as TokioMutex, watch};
use tokio::time::timeout;
use tokio_postgres::{Config, NoTls};

static NATS_CLUSTER_FAULT_LOCK: TokioMutex<()> = TokioMutex::const_new(());

struct CrashWindowPublisher {
    inner: NatsJetStreamPublisher,
    last_claim: Mutex<Option<OutboxClaim>>,
    attempted_event_ids: Mutex<Vec<i64>>,
    fail_after_first_storage: AtomicBool,
}

impl OutboxPublisher for CrashWindowPublisher {
    fn publish<'a>(&'a self, event: &'a OutboxClaim) -> OutboxPublishFuture<'a> {
        *self
            .last_claim
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(event.clone());
        self.attempted_event_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.event_id());

        let fail_after_storage = self.fail_after_first_storage.swap(false, Ordering::SeqCst);
        let publish = self.inner.publish(event);
        Box::pin(async move {
            publish.await?;
            if fail_after_storage {
                Err(OutboxPublishError::new(
                    "simulated process loss after JetStream storage acknowledgement",
                ))
            } else {
                Ok(())
            }
        })
    }
}

struct BrokerUnavailablePublisher {
    inner: NatsJetStreamPublisher,
    broker_available: Arc<AtomicBool>,
    attempted_event_ids: Mutex<Vec<i64>>,
}

impl OutboxPublisher for BrokerUnavailablePublisher {
    fn publish<'a>(&'a self, event: &'a OutboxClaim) -> OutboxPublishFuture<'a> {
        self.attempted_event_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.event_id());

        if !self.broker_available.load(Ordering::SeqCst) {
            return Box::pin(async {
                Err(OutboxPublishError::new(
                    "simulated NATS broker/network partition before publish",
                ))
            });
        }

        self.inner.publish(event)
    }
}

struct CountingHandler {
    calls: Arc<AtomicUsize>,
    delay: Duration,
}

impl JetStreamEventHandler for CountingHandler {
    fn handle<'a>(&'a self, event: &'a JetStreamEvent) -> EventHandlerFuture<'a> {
        let calls = Arc::clone(&self.calls);
        let event_id = event.event_id().to_owned();
        let delay = self.delay;
        Box::pin(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            if event_id.is_empty() {
                return Err("event identity must not be empty".to_owned());
            }
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

struct FailOnceHandler {
    calls: Arc<AtomicUsize>,
}

impl JetStreamEventHandler for FailOnceHandler {
    fn handle<'a>(&'a self, event: &'a JetStreamEvent) -> EventHandlerFuture<'a> {
        let calls = Arc::clone(&self.calls);
        let event_id = event.event_id().to_owned();
        Box::pin(async move {
            if event_id.is_empty() {
                return Err("event identity must not be empty".to_owned());
            }
            let attempt = calls.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                Err("simulated transient consumer failure".to_owned())
            } else {
                Ok(())
            }
        })
    }
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    format!("{}_{nanos}", std::process::id())
}

fn parse_nats_seed_urls(urls: &str) -> Vec<async_nats::ServerAddr> {
    let servers = urls
        .split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(|url| {
            url.parse::<async_nats::ServerAddr>()
                .expect("valid NATS seed server URL")
        })
        .collect::<Vec<_>>();
    assert!(
        !servers.is_empty(),
        "at least one NATS seed URL is required"
    );
    servers
}

async fn connect_to_nats(urls: &str) -> Result<async_nats::Client, async_nats::ConnectError> {
    async_nats::connect(parse_nats_seed_urls(urls)).await
}

#[test]
fn nats_seed_url_list_parses_all_cluster_members() {
    let servers =
        parse_nats_seed_urls("nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224");
    assert_eq!(servers.len(), 3);
}

#[tokio::test]
async fn jetstream_publisher_confirms_storage_and_deduplicates_stable_event_ids() {
    let (Ok(database_url), Ok(nats_url)) = (
        std::env::var("AETHERFLOW_TEST_NATS_DATABASE_URL"),
        std::env::var("AETHERFLOW_TEST_NATS_URL"),
    ) else {
        eprintln!(
            "AETHERFLOW_TEST_NATS_DATABASE_URL or AETHERFLOW_TEST_NATS_URL is not set; skipping JetStream integration test"
        );
        return;
    };

    let _cluster_fault_guard = NATS_CLUSTER_FAULT_LOCK.lock().await;
    let suffix = unique_suffix();
    let subject = format!("aetherflow.integration.{suffix}.events");
    let stream_name = format!("AFLOW_TEST_{}", suffix.replace('_', ""));
    let consumer_name = format!("aflow_test_{}", suffix.replace('_', ""));

    let client = connect_to_nats(&nats_url)
        .await
        .expect("connect to NATS seed server pool");
    let jetstream = jetstream::new(client);
    let stream = jetstream
        .create_stream(jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: vec![subject.clone()],
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        })
        .await
        .expect("create isolated JetStream stream");
    let consumer = stream
        .create_consumer(PullConsumerConfig {
            durable_name: Some(consumer_name),
            filter_subject: subject.clone(),
            ack_wait: Duration::from_millis(250),
            max_ack_pending: 1,
            ..Default::default()
        })
        .await
        .expect("create pull consumer");

    let config: Config = database_url.parse().expect("valid PostgreSQL URL");
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("build isolated PostgreSQL pool");
    let store = PgStore::from_pool(pool.clone());
    assert!(matches!(
        store.migrate().await.expect("apply PostgreSQL schema"),
        MigrationOutcome::Applied { version: 3, .. }
            | MigrationOutcome::AlreadyCurrent { version: 3, .. }
    ));

    let repository = Arc::new(PgTaskRepository::from_pool(pool.clone()));
    let workflow_run_id =
        WorkflowRunId::parse(format!("nats-workflow-run-{suffix}")).expect("valid workflow-run ID");
    let workflow_id =
        WorkflowId::parse(format!("nats-workflow-{suffix}")).expect("valid workflow ID");
    let task_run_id =
        TaskRunId::parse(format!("nats-task-run-{suffix}")).expect("valid task-run ID");
    let task_id = TaskId::parse(format!("nats-task-{suffix}")).expect("valid task ID");
    let retry_policy = RetryPolicy::new(NonZeroU32::new(1).expect("non-zero attempts"), 0, 0)
        .expect("valid retry policy");

    repository
        .create_workflow(
            &workflow_run_id,
            &workflow_id,
            None,
            &[NewTaskRun::new(
                task_run_id,
                task_id,
                TaskInput::new(Vec::new()).expect("valid empty task input"),
                Vec::new(),
                retry_policy,
                SystemTime::now() + Duration::from_secs(300),
            )],
        )
        .await
        .expect("create workflow and transactional outbox event");

    let nats_publisher = NatsJetStreamPublisher::new(jetstream.clone(), subject);
    let capturing_publisher = Arc::new(CrashWindowPublisher {
        inner: nats_publisher,
        last_claim: Mutex::new(None),
        attempted_event_ids: Mutex::new(Vec::new()),
        fail_after_first_storage: AtomicBool::new(true),
    });
    let dispatcher = PostgresOutboxDispatcher::new(
        Arc::clone(&repository),
        WorkerId::parse(format!("nats-publisher-{suffix}")).expect("valid publisher ID"),
        Arc::clone(&capturing_publisher),
        OutboxDispatcherConfig::new(
            1,
            Duration::from_millis(10),
            Duration::from_secs(10),
            Duration::from_millis(50),
            Duration::from_secs(1),
        )
        .expect("valid dispatcher config"),
    );

    let first_report: OutboxDispatchReport = dispatcher
        .dispatch_once()
        .await
        .expect("first publication reaches JetStream before simulated process loss");
    assert_eq!(first_report.claimed(), 1);
    assert_eq!(first_report.acknowledged(), 0);
    assert_eq!(first_report.rescheduled(), 1);

    let first_claim = capturing_publisher
        .last_claim
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .expect("capture first outbox claim");
    let client = pool.get().await.expect("get PostgreSQL test connection");
    let first_state = client
        .query_one(
            "SELECT published_at IS NOT NULL, publish_attempts
             FROM outbox_events WHERE event_id = $1",
            &[&first_claim.event_id()],
        )
        .await
        .expect("inspect outbox after simulated process loss");
    assert!(!first_state.get::<_, bool>(0));
    assert_eq!(first_state.get::<_, i32>(1), 1);

    // The retry must reclaim the same event. JetStream recognizes the stable
    // Nats-Msg-Id and acknowledges it without storing a second copy.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second_report = dispatcher
        .dispatch_once()
        .await
        .expect("retry outbox event after simulated process loss");
    assert_eq!(second_report.claimed(), 1);
    assert_eq!(second_report.acknowledged(), 1);
    assert_eq!(second_report.rescheduled(), 0);

    let attempted_ids = capturing_publisher
        .attempted_event_ids
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(
        attempted_ids,
        vec![first_claim.event_id(), first_claim.event_id()]
    );

    let final_state = client
        .query_one(
            "SELECT published_at IS NOT NULL, publish_attempts
             FROM outbox_events WHERE event_id = $1",
            &[&first_claim.event_id()],
        )
        .await
        .expect("inspect successfully acknowledged outbox event");
    assert!(final_state.get::<_, bool>(0));
    assert_eq!(final_state.get::<_, i32>(1), 2);
    drop(client);

    let inbox = PgInboxRepository::from_pool(pool.clone());
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let handler = Arc::new(CountingHandler {
        calls: Arc::clone(&handler_calls),
        delay: Duration::from_millis(700),
    });
    let logical_consumer = format!("billing_{}", suffix.replace('_', ""));
    let first_consumer = NatsJetStreamConsumer::new(
        consumer.clone(),
        inbox.clone(),
        logical_consumer.clone(),
        WorkerId::parse(format!("nats-consumer-a-{suffix}")).expect("valid consumer worker ID"),
        Duration::from_millis(300),
        Arc::clone(&handler),
    )
    .expect("create inbox-backed JetStream consumer");
    let first_metrics = first_consumer.metrics();
    let (first_shutdown_tx, first_shutdown_rx) = watch::channel(false);
    let first_task = tokio::spawn(async move { first_consumer.run(first_shutdown_rx).await });

    timeout(Duration::from_secs(5), async {
        while first_metrics.snapshot().processed == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("first JetStream delivery should be processed");
    first_shutdown_tx
        .send(true)
        .expect("signal first consumer shutdown");
    first_task
        .await
        .expect("join first consumer")
        .expect("first consumer should stop cleanly");
    // Keep the durable consumer open beyond AckWait. Progress acknowledgements
    // must prevent JetStream from redelivering the event while the handler is
    // still working, and the PostgreSQL inbox lease must stay owned as well.
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    assert_eq!(first_metrics.snapshot().processed, 1);
    assert_eq!(first_metrics.snapshot().duplicates, 0);
    assert_eq!(first_metrics.snapshot().busy, 0);

    // Republish the same logical event ID under a different NATS message ID.
    // This simulates a broker redelivery after the inbox commit: the inbox must
    // suppress the handler while the consumer still acknowledges the delivery.
    let duplicate_envelope = serde_json::json!({
        "schema_version": 1,
        "event_id": first_claim.event_id().to_string(),
        "aggregate_type": first_claim.aggregate_type(),
        "aggregate_id": first_claim.aggregate_id(),
        "aggregate_version": first_claim.aggregate_version(),
        "event_type": first_claim.event_type(),
        "payload": first_claim.payload(),
    });
    let mut duplicate_headers = async_nats::HeaderMap::new();
    duplicate_headers.append("Nats-Msg-Id", format!("redelivery-{suffix}"));
    let duplicate_ack = jetstream
        .publish_with_headers(
            format!("aetherflow.integration.{suffix}.events"),
            duplicate_headers,
            serde_json::to_vec(&duplicate_envelope)
                .expect("encode duplicate logical event")
                .into(),
        )
        .await
        .expect("publish duplicate logical event to JetStream");
    duplicate_ack
        .await
        .expect("JetStream should confirm duplicate logical event");

    let second_consumer = NatsJetStreamConsumer::new(
        consumer.clone(),
        inbox.clone(),
        logical_consumer.clone(),
        WorkerId::parse(format!("nats-consumer-b-{suffix}")).expect("valid consumer worker ID"),
        Duration::from_millis(300),
        Arc::clone(&handler),
    )
    .expect("reopen inbox-backed JetStream consumer");
    let second_metrics = second_consumer.metrics();
    let (second_shutdown_tx, second_shutdown_rx) = watch::channel(false);
    let second_task = tokio::spawn(async move { second_consumer.run(second_shutdown_rx).await });
    timeout(Duration::from_secs(5), async {
        while second_metrics.snapshot().duplicates == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("duplicate delivery should be suppressed by the durable inbox");
    second_shutdown_tx
        .send(true)
        .expect("signal second consumer shutdown");
    second_task
        .await
        .expect("join second consumer")
        .expect("second consumer should stop cleanly");

    assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_metrics.snapshot().duplicates, 1);
    let inbox_state = pool
        .get()
        .await
        .expect("get inbox verification connection")
        .query_one(
            "SELECT status, attempts, claim_token
             FROM consumer_inbox WHERE consumer_name = $1 AND event_id = $2",
            &[&logical_consumer, &first_claim.event_id().to_string()],
        )
        .await
        .expect("inspect processed inbox event");
    let inbox_status: String = inbox_state.get(0);
    let inbox_attempts: i64 = inbox_state.get(1);
    let inbox_token: i64 = inbox_state.get(2);
    assert_eq!(inbox_status, "processed");
    assert_eq!(inbox_attempts, 1);
    assert_eq!(inbox_token, 1);

    // A transient application error releases the inbox row and leaves the
    // JetStream delivery unacknowledged. Its redelivery must be claimable again.
    let retry_event_id = format!("consumer-retry-{suffix}");
    let retry_envelope = serde_json::json!({
        "schema_version": 1,
        "event_id": retry_event_id,
        "aggregate_type": "test",
        "aggregate_id": format!("aggregate-{suffix}"),
        "aggregate_version": 1,
        "event_type": "test.retry",
        "payload": {"retry": true},
    });
    let mut retry_headers = async_nats::HeaderMap::new();
    retry_headers.append("Nats-Msg-Id", format!("handler-retry-{suffix}"));
    let retry_ack = jetstream
        .publish_with_headers(
            format!("aetherflow.integration.{suffix}.events"),
            retry_headers,
            serde_json::to_vec(&retry_envelope)
                .expect("encode retry event")
                .into(),
        )
        .await
        .expect("publish retry test event");
    retry_ack
        .await
        .expect("JetStream should store retry test event");

    let retry_calls = Arc::new(AtomicUsize::new(0));
    let retry_handler = Arc::new(FailOnceHandler {
        calls: Arc::clone(&retry_calls),
    });
    let retry_consumer_name = format!("retry_{}", suffix.replace('_', ""));
    let retry_consumer = NatsJetStreamConsumer::new(
        consumer,
        inbox,
        retry_consumer_name.clone(),
        WorkerId::parse(format!("nats-retry-consumer-{suffix}"))
            .expect("valid retry consumer worker ID"),
        Duration::from_millis(300),
        retry_handler,
    )
    .expect("create retrying JetStream consumer");
    let retry_metrics = retry_consumer.metrics();
    let (retry_shutdown_tx, retry_shutdown_rx) = watch::channel(false);
    let retry_task = tokio::spawn(async move { retry_consumer.run(retry_shutdown_rx).await });
    timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = retry_metrics.snapshot();
            if snapshot.handler_failures == 1 && snapshot.processed == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("handler failure should be retried and eventually processed");
    retry_shutdown_tx
        .send(true)
        .expect("signal retry consumer shutdown");
    retry_task
        .await
        .expect("join retry consumer")
        .expect("retry consumer should stop cleanly");
    assert_eq!(retry_calls.load(Ordering::SeqCst), 2);
    assert_eq!(retry_metrics.snapshot().handler_failures, 1);
    assert_eq!(retry_metrics.snapshot().processed, 1);

    let retry_state = pool
        .get()
        .await
        .expect("get retry inbox verification connection")
        .query_one(
            "SELECT status, attempts, claim_token
             FROM consumer_inbox WHERE consumer_name = $1 AND event_id = $2",
            &[&retry_consumer_name, &format!("consumer-retry-{suffix}")],
        )
        .await
        .expect("inspect retried inbox event");
    let retry_status: String = retry_state.get(0);
    let retry_attempts: i64 = retry_state.get(1);
    let retry_token: i64 = retry_state.get(2);
    assert_eq!(retry_status, "processed");
    assert_eq!(retry_attempts, 2);
    assert_eq!(retry_token, 2);

    // --- Real publisher process death after broker acceptance, before DB ack. ---
    let publisher_crash_subject = format!("aetherflow.integration.{suffix}.publisher-crash");
    let publisher_crash_stream_name = format!("AFLOW_PUB_CRASH_{}", suffix.replace('_', ""));
    let publisher_crash_durable = format!("aflow_pub_crash_{}", suffix.replace('_', ""));
    let publisher_crash_stream = jetstream
        .create_stream(jetstream::stream::Config {
            name: publisher_crash_stream_name.clone(),
            subjects: vec![publisher_crash_subject.clone()],
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        })
        .await
        .expect("create publisher crash-window stream");
    let publisher_crash_consumer = publisher_crash_stream
        .create_consumer(PullConsumerConfig {
            durable_name: Some(publisher_crash_durable),
            filter_subject: publisher_crash_subject.clone(),
            ..Default::default()
        })
        .await
        .expect("create publisher crash-window pull consumer");

    let crash_workflow_run_id = WorkflowRunId::parse(format!("nats-publisher-crash-run-{suffix}"))
        .expect("valid publisher crash workflow-run ID");
    let crash_workflow_id = WorkflowId::parse(format!("nats-publisher-crash-workflow-{suffix}"))
        .expect("valid publisher crash workflow ID");
    let crash_task_run_id = TaskRunId::parse(format!("nats-publisher-crash-task-run-{suffix}"))
        .expect("valid publisher crash task-run ID");
    let crash_retry_policy = RetryPolicy::new(NonZeroU32::new(1).expect("non-zero attempts"), 0, 0)
        .expect("valid publisher crash retry policy");
    repository
        .create_workflow(
            &crash_workflow_run_id,
            &crash_workflow_id,
            None,
            &[NewTaskRun::new(
                crash_task_run_id,
                TaskId::parse(format!("nats-publisher-crash-task-{suffix}"))
                    .expect("valid publisher crash task ID"),
                TaskInput::new(Vec::new()).expect("valid empty publisher crash input"),
                Vec::new(),
                crash_retry_policy,
                SystemTime::now() + Duration::from_secs(300),
            )],
        )
        .await
        .expect("create workflow for publisher process-crash test");

    let crash_event_id: i64 = pool
        .get()
        .await
        .expect("get publisher crash test connection")
        .query_one(
            "SELECT event_id FROM outbox_events
             WHERE aggregate_type = 'workflow_run'
               AND aggregate_id = $1
               AND event_type = 'workflow.started'
               AND published_at IS NULL",
            &[&crash_workflow_run_id.as_str()],
        )
        .await
        .expect("find publisher crash-window event")
        .get(0);

    let publisher_crash_lease_ms = 450_u64;
    let publisher_child = Command::new(
        std::env::current_exe().expect("current JetStream integration test executable"),
    )
    .args([
        "--exact",
        "jetstream_child_process_exits_after_broker_storage_before_outbox_ack",
        "--nocapture",
    ])
    .env("AETHERFLOW_CRASH_PUBLISH_DATABASE_URL", &database_url)
    .env("AETHERFLOW_CRASH_PUBLISH_NATS_URL", &nats_url)
    .env("AETHERFLOW_CRASH_PUBLISH_SUBJECT", &publisher_crash_subject)
    .env(
        "AETHERFLOW_CRASH_PUBLISH_WORKER_ID",
        format!("crash-publisher-{suffix}"),
    )
    .env(
        "AETHERFLOW_CRASH_PUBLISH_EVENT_ID",
        crash_event_id.to_string(),
    )
    .env(
        "AETHERFLOW_CRASH_PUBLISH_LEASE_MS",
        publisher_crash_lease_ms.to_string(),
    )
    .output()
    .expect("spawn publisher process-crash child");
    assert_eq!(
        publisher_child.status.code(),
        Some(86),
        "publisher child must terminate after JetStream storage and before DB ack; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&publisher_child.stdout),
        String::from_utf8_lossy(&publisher_child.stderr),
    );

    let publisher_crash_state = pool
        .get()
        .await
        .expect("get post-crash outbox connection")
        .query_one(
            "SELECT published_at IS NOT NULL, publish_attempts, claim_token,
                    claim_owner IS NOT NULL, claim_expires_at > clock_timestamp()
             FROM outbox_events WHERE event_id = $1",
            &[&crash_event_id],
        )
        .await
        .expect("inspect outbox after publisher process exit");
    assert!(!publisher_crash_state.get::<_, bool>(0));
    assert_eq!(publisher_crash_state.get::<_, i32>(1), 1);
    assert_eq!(publisher_crash_state.get::<_, i64>(2), 1);
    assert!(publisher_crash_state.get::<_, bool>(3));
    assert!(publisher_crash_state.get::<_, bool>(4));

    // The OS process is gone. Wait for its database-owned lease, then let a
    // fresh dispatcher retry the same stable event ID through real JetStream.
    tokio::time::sleep(Duration::from_millis(publisher_crash_lease_ms + 180)).await;
    let publisher_crash_dispatcher = PostgresOutboxDispatcher::new(
        Arc::clone(&repository),
        WorkerId::parse(format!("crash-retry-publisher-{suffix}"))
            .expect("valid crash retry publisher ID"),
        Arc::new(NatsJetStreamPublisher::new(
            jetstream.clone(),
            publisher_crash_subject.clone(),
        )),
        OutboxDispatcherConfig::new(
            1,
            Duration::from_millis(10),
            Duration::from_secs(5),
            Duration::from_millis(20),
            Duration::from_secs(1),
        )
        .expect("valid publisher crash retry config"),
    );
    let publisher_crash_retry = publisher_crash_dispatcher
        .dispatch_once()
        .await
        .expect("reclaim and republish event after publisher process exit");
    assert_eq!(publisher_crash_retry.claimed(), 1);
    assert_eq!(publisher_crash_retry.acknowledged(), 1);
    assert_eq!(publisher_crash_retry.rescheduled(), 0);

    let publisher_crash_final = pool
        .get()
        .await
        .expect("get final outbox verification connection")
        .query_one(
            "SELECT published_at IS NOT NULL, publish_attempts, claim_token,
                    claim_owner IS NULL
             FROM outbox_events WHERE event_id = $1",
            &[&crash_event_id],
        )
        .await
        .expect("verify fresh publisher completed the event");
    assert!(publisher_crash_final.get::<_, bool>(0));
    assert_eq!(publisher_crash_final.get::<_, i32>(1), 2);
    assert_eq!(publisher_crash_final.get::<_, i64>(2), 2);
    assert!(publisher_crash_final.get::<_, bool>(3));

    let mut publisher_crash_messages = publisher_crash_consumer
        .messages()
        .await
        .expect("open publisher crash-window stream messages");
    let stored_crash_event = timeout(Duration::from_secs(5), publisher_crash_messages.next())
        .await
        .expect("JetStream should deliver publisher crash-window event")
        .expect("publisher crash-window consumer stream should remain open")
        .expect("read publisher crash-window message");
    let stored_crash_envelope: serde_json::Value =
        serde_json::from_slice(&stored_crash_event.payload)
            .expect("decode publisher crash-window envelope");
    assert_eq!(
        stored_crash_envelope["event_id"],
        crash_event_id.to_string()
    );
    stored_crash_event
        .double_ack()
        .await
        .expect("acknowledge single stored publisher crash-window message");
    match timeout(Duration::from_millis(250), publisher_crash_messages.next()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(Err(error))) => panic!("publisher crash-window stream failed: {error}"),
        Ok(Some(Ok(message))) => {
            panic!(
                "JetStream stored a duplicate after real publisher process death: {:?}",
                message.subject
            )
        }
    }
    drop(publisher_crash_messages);
    let _ = jetstream.delete_stream(publisher_crash_stream_name).await;

    // --- Real consumer process death after inbox commit, before broker ack. ---
    let consumer_crash_subject = format!("aetherflow.integration.{suffix}.consumer-crash");
    let consumer_crash_stream_name = format!("AFLOW_CON_CRASH_{}", suffix.replace('_', ""));
    let consumer_crash_durable = format!("aflow_con_crash_{}", suffix.replace('_', ""));
    let consumer_crash_logical_name = format!("crash_consumer_{}", suffix.replace('_', ""));
    let consumer_crash_stream = jetstream
        .create_stream(jetstream::stream::Config {
            name: consumer_crash_stream_name.clone(),
            subjects: vec![consumer_crash_subject.clone()],
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        })
        .await
        .expect("create consumer crash-window stream");
    let _consumer_crash_pull = consumer_crash_stream
        .create_consumer(PullConsumerConfig {
            durable_name: Some(consumer_crash_durable.clone()),
            filter_subject: consumer_crash_subject.clone(),
            ack_wait: Duration::from_millis(300),
            max_ack_pending: 1,
            ..Default::default()
        })
        .await
        .expect("create consumer crash-window pull consumer");

    pool.get()
        .await
        .expect("get consumer crash setup connection")
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS aetherflow_test_consumer_effects (
                 event_id TEXT PRIMARY KEY,
                 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
             )",
        )
        .await
        .expect("create consumer crash side-effect ledger");

    let consumer_crash_event_id = format!("consumer-process-crash-{suffix}");
    let consumer_crash_envelope = serde_json::json!({
        "schema_version": 1,
        "event_id": consumer_crash_event_id,
        "aggregate_type": "test",
        "aggregate_id": format!("consumer-crash-aggregate-{suffix}"),
        "aggregate_version": 1,
        "event_type": "test.consumer_process_crash",
        "payload": {"crash_window": "inbox-commit-before-broker-ack"},
    });
    let mut consumer_crash_headers = async_nats::HeaderMap::new();
    consumer_crash_headers.append("Nats-Msg-Id", format!("consumer-process-crash-{suffix}"));
    let consumer_crash_publish_ack = jetstream
        .publish_with_headers(
            consumer_crash_subject.clone(),
            consumer_crash_headers,
            serde_json::to_vec(&consumer_crash_envelope)
                .expect("encode consumer crash-window envelope")
                .into(),
        )
        .await
        .expect("publish consumer crash-window event");
    consumer_crash_publish_ack
        .await
        .expect("JetStream should store consumer crash-window event");

    let consumer_child = Command::new(
        std::env::current_exe().expect("current JetStream integration test executable"),
    )
    .args([
        "--exact",
        "jetstream_child_process_exits_after_inbox_commit_before_broker_ack",
        "--nocapture",
    ])
    .env("AETHERFLOW_CRASH_CONSUMER_DATABASE_URL", &database_url)
    .env("AETHERFLOW_CRASH_CONSUMER_NATS_URL", &nats_url)
    .env(
        "AETHERFLOW_CRASH_CONSUMER_STREAM",
        &consumer_crash_stream_name,
    )
    .env("AETHERFLOW_CRASH_CONSUMER_DURABLE", &consumer_crash_durable)
    .env(
        "AETHERFLOW_CRASH_CONSUMER_LOGICAL_NAME",
        &consumer_crash_logical_name,
    )
    .env(
        "AETHERFLOW_CRASH_CONSUMER_EVENT_ID",
        &consumer_crash_event_id,
    )
    .env(
        "AETHERFLOW_CRASH_CONSUMER_WORKER_ID",
        format!("crash-consumer-worker-{suffix}"),
    )
    .output()
    .expect("spawn consumer process-crash child");
    assert_eq!(
        consumer_child.status.code(),
        Some(87),
        "consumer child must terminate after inbox commit and before broker ack; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&consumer_child.stdout),
        String::from_utf8_lossy(&consumer_child.stderr),
    );

    let consumer_crash_inbox_state = pool
        .get()
        .await
        .expect("get consumer crash inbox verification connection")
        .query_one(
            "SELECT status, attempts, claim_token FROM consumer_inbox
             WHERE consumer_name = $1 AND event_id = $2",
            &[&consumer_crash_logical_name, &consumer_crash_event_id],
        )
        .await
        .expect("inspect inbox after consumer process exit");
    assert_eq!(consumer_crash_inbox_state.get::<_, String>(0), "processed");
    assert_eq!(consumer_crash_inbox_state.get::<_, i64>(1), 1);
    assert_eq!(consumer_crash_inbox_state.get::<_, i64>(2), 1);
    let side_effect_count: i64 = pool
        .get()
        .await
        .expect("get consumer crash side-effect connection")
        .query_one(
            "SELECT count(*) FROM aetherflow_test_consumer_effects WHERE event_id = $1",
            &[&consumer_crash_event_id],
        )
        .await
        .expect("count side effects completed before inbox commit")
        .get(0);
    assert_eq!(side_effect_count, 1);

    let consumer_crash_handler_calls = Arc::new(AtomicUsize::new(0));
    let consumer_crash_handler = Arc::new(CountingHandler {
        calls: Arc::clone(&consumer_crash_handler_calls),
        delay: Duration::ZERO,
    });
    let existing_crash_pull: PullConsumer = consumer_crash_stream
        .get_consumer(&consumer_crash_durable)
        .await
        .expect("reattach to durable consumer after process restart");
    let restarted_consumer = NatsJetStreamConsumer::new(
        existing_crash_pull,
        PgInboxRepository::from_pool(pool.clone()),
        consumer_crash_logical_name.clone(),
        WorkerId::parse(format!("crash-consumer-restart-{suffix}"))
            .expect("valid restarted consumer worker ID"),
        Duration::from_millis(300),
        consumer_crash_handler,
    )
    .expect("create restarted consumer after inbox commit");
    let restarted_metrics = restarted_consumer.metrics();
    let (restarted_shutdown_tx, restarted_shutdown_rx) = watch::channel(false);
    let restarted_task =
        tokio::spawn(async move { restarted_consumer.run(restarted_shutdown_rx).await });
    timeout(Duration::from_secs(5), async {
        while restarted_metrics.snapshot().duplicates == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("broker redelivery after child exit should be suppressed by inbox");
    restarted_shutdown_tx
        .send(true)
        .expect("signal restarted consumer shutdown");
    restarted_task
        .await
        .expect("join restarted consumer")
        .expect("restarted consumer should acknowledge processed redelivery");
    assert_eq!(restarted_metrics.snapshot().duplicates, 1);
    assert_eq!(consumer_crash_handler_calls.load(Ordering::SeqCst), 0);
    let final_side_effect_count: i64 = pool
        .get()
        .await
        .expect("get final consumer side-effect connection")
        .query_one(
            "SELECT count(*) FROM aetherflow_test_consumer_effects WHERE event_id = $1",
            &[&consumer_crash_event_id],
        )
        .await
        .expect("verify no duplicate side effect after process restart")
        .get(0);
    assert_eq!(final_side_effect_count, 1);
    pool.get()
        .await
        .expect("get consumer crash cleanup connection")
        .execute("DROP TABLE aetherflow_test_consumer_effects", &[])
        .await
        .expect("drop consumer crash side-effect table");
    let _ = jetstream.delete_stream(consumer_crash_stream_name).await;

    // Broker partition/recovery: PostgreSQL remains healthy while the
    // transport adapter reports NATS unavailable. The outbox must retain the
    // same durable event identity and publish it after the broker returns.
    let partition_subject = format!("aetherflow.integration.{suffix}.partition-recovery");
    let partition_stream_name = format!("AFLOW_PARTITION_{}", suffix.replace('_', ""));
    let partition_consumer_name = format!("aflow_partition_{}", suffix.replace('_', ""));
    let partition_stream = jetstream
        .create_stream(jetstream::stream::Config {
            name: partition_stream_name.clone(),
            subjects: vec![partition_subject.clone()],
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        })
        .await
        .expect("create broker-partition recovery stream");
    let partition_consumer = partition_stream
        .create_consumer(PullConsumerConfig {
            durable_name: Some(partition_consumer_name),
            filter_subject: partition_subject.clone(),
            ack_wait: Duration::from_millis(500),
            max_ack_pending: 1,
            ..Default::default()
        })
        .await
        .expect("create broker-partition recovery consumer");

    let partition_workflow_run_id = WorkflowRunId::parse(format!("nats-partition-run-{suffix}"))
        .expect("valid broker-partition workflow-run ID");
    let partition_workflow_id = WorkflowId::parse(format!("nats-partition-workflow-{suffix}"))
        .expect("valid broker-partition workflow ID");
    let partition_task_run_id = TaskRunId::parse(format!("nats-partition-task-run-{suffix}"))
        .expect("valid broker-partition task-run ID");
    let partition_task_id = TaskId::parse(format!("nats-partition-task-{suffix}"))
        .expect("valid broker-partition task ID");
    repository
        .create_workflow(
            &partition_workflow_run_id,
            &partition_workflow_id,
            None,
            &[NewTaskRun::new(
                partition_task_run_id,
                partition_task_id,
                TaskInput::empty(),
                Vec::new(),
                RetryPolicy::new(NonZeroU32::new(1).expect("non-zero attempts"), 0, 0)
                    .expect("valid broker-partition retry policy"),
                SystemTime::now() + Duration::from_secs(300),
            )],
        )
        .await
        .expect("create workflow during simulated broker partition");

    let partition_event_id: i64 = pool
        .get()
        .await
        .expect("get broker-partition outbox connection")
        .query_one(
            "SELECT event_id FROM outbox_events
             WHERE aggregate_type = 'workflow_run'
               AND aggregate_id = $1
               AND event_type = 'workflow.started'
               AND published_at IS NULL",
            &[&partition_workflow_run_id.as_str()],
        )
        .await
        .expect("find broker-partition outbox event")
        .get(0);

    let broker_available = Arc::new(AtomicBool::new(false));
    let partition_publisher = Arc::new(BrokerUnavailablePublisher {
        inner: NatsJetStreamPublisher::new(jetstream.clone(), partition_subject),
        broker_available: Arc::clone(&broker_available),
        attempted_event_ids: Mutex::new(Vec::new()),
    });
    let partition_dispatcher = PostgresOutboxDispatcher::new(
        Arc::clone(&repository),
        WorkerId::parse(format!("nats-partition-publisher-{suffix}"))
            .expect("valid broker-partition publisher ID"),
        Arc::clone(&partition_publisher),
        OutboxDispatcherConfig::new(
            1,
            Duration::from_millis(10),
            Duration::from_secs(10),
            Duration::from_millis(50),
            Duration::from_secs(1),
        )
        .expect("valid broker-partition dispatcher config"),
    );

    let unavailable_report = partition_dispatcher
        .dispatch_once()
        .await
        .expect("dispatcher reschedules unavailable broker publication");
    assert_eq!(unavailable_report.claimed(), 1);
    assert_eq!(unavailable_report.acknowledged(), 0);
    assert_eq!(unavailable_report.rescheduled(), 1);
    let unavailable_state = pool
        .get()
        .await
        .expect("get unavailable outbox verification connection")
        .query_one(
            "SELECT published_at IS NOT NULL, publish_attempts
             FROM outbox_events WHERE event_id = $1",
            &[&partition_event_id],
        )
        .await
        .expect("inspect outbox after broker partition");
    assert!(!unavailable_state.get::<_, bool>(0));
    assert_eq!(unavailable_state.get::<_, i32>(1), 1);

    broker_available.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(120)).await;
    let recovered_report = partition_dispatcher
        .dispatch_once()
        .await
        .expect("dispatcher publishes after broker connectivity returns");
    assert_eq!(recovered_report.claimed(), 1);
    assert_eq!(recovered_report.acknowledged(), 1);
    assert_eq!(recovered_report.rescheduled(), 0);

    let attempted_partition_ids = partition_publisher
        .attempted_event_ids
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(
        attempted_partition_ids,
        vec![partition_event_id, partition_event_id]
    );

    let recovered_outbox_state = pool
        .get()
        .await
        .expect("get recovered outbox verification connection")
        .query_one(
            "SELECT published_at IS NOT NULL, publish_attempts, claim_token,
                    claim_owner IS NULL
             FROM outbox_events WHERE event_id = $1",
            &[&partition_event_id],
        )
        .await
        .expect("inspect recovered outbox event");
    assert!(recovered_outbox_state.get::<_, bool>(0));
    assert_eq!(recovered_outbox_state.get::<_, i32>(1), 2);
    assert_eq!(recovered_outbox_state.get::<_, i64>(2), 2);
    assert!(recovered_outbox_state.get::<_, bool>(3));

    let mut partition_messages = partition_consumer
        .messages()
        .await
        .expect("open broker-partition recovery messages");
    let delivered = timeout(Duration::from_secs(5), partition_messages.next())
        .await
        .expect("recovered broker event should be delivered")
        .expect("broker-partition consumer stream should remain open")
        .expect("read recovered broker event");
    let delivered_envelope: serde_json::Value =
        serde_json::from_slice(&delivered.payload).expect("decode recovered event envelope");
    assert_eq!(
        delivered_envelope["event_id"],
        partition_event_id.to_string()
    );
    delivered
        .double_ack()
        .await
        .expect("acknowledge recovered broker event");
    match timeout(Duration::from_millis(250), partition_messages.next()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(Err(error))) => panic!("broker-partition consumer failed: {error}"),
        Ok(Some(Ok(message))) => {
            panic!(
                "broker recovery produced a duplicate event: {:?}",
                message.subject
            )
        }
    }
    drop(partition_messages);
    let _ = jetstream.delete_stream(partition_stream_name).await;

    // Exercise an actual NATS server stop/start in CI. This is gated so a
    // developer running the integration test against a remote/local NATS
    // server does not need Docker access or permission to stop that server.
    if let Ok(container_name) = std::env::var("AETHERFLOW_TEST_NATS_CONTAINER") {
        let restart_subject = format!("aetherflow.integration.{suffix}.server-restart");
        let restart_stream_name = format!("AFLOW_RESTART_{}", suffix.replace('_', ""));
        let restart_durable_name = format!("aflow_restart_{}", suffix.replace('_', ""));
        let restart_stream = jetstream
            .create_stream(jetstream::stream::Config {
                name: restart_stream_name.clone(),
                subjects: vec![restart_subject.clone()],
                duplicate_window: Duration::from_secs(120),
                num_replicas: 3,
                ..Default::default()
            })
            .await
            .expect("create three-replica NATS server-restart stream");
        let restart_consumer = restart_stream
            .create_consumer(PullConsumerConfig {
                durable_name: Some(restart_durable_name),
                filter_subject: restart_subject.clone(),
                ack_wait: Duration::from_millis(500),
                max_ack_pending: 1,
                ..Default::default()
            })
            .await
            .expect("create NATS server-restart consumer");

        let restart_workflow_run_id = WorkflowRunId::parse(format!("nats-restart-run-{suffix}"))
            .expect("valid server-restart workflow-run ID");
        let restart_workflow_id = WorkflowId::parse(format!("nats-restart-workflow-{suffix}"))
            .expect("valid server-restart workflow ID");
        let restart_task_run_id = TaskRunId::parse(format!("nats-restart-task-run-{suffix}"))
            .expect("valid server-restart task-run ID");
        let restart_task_id = TaskId::parse(format!("nats-restart-task-{suffix}"))
            .expect("valid server-restart task ID");
        repository
            .create_workflow(
                &restart_workflow_run_id,
                &restart_workflow_id,
                None,
                &[NewTaskRun::new(
                    restart_task_run_id,
                    restart_task_id,
                    TaskInput::empty(),
                    Vec::new(),
                    RetryPolicy::new(NonZeroU32::new(1).expect("non-zero attempts"), 0, 0)
                        .expect("valid server-restart retry policy"),
                    SystemTime::now() + Duration::from_secs(300),
                )],
            )
            .await
            .expect("create workflow before stopping NATS");

        let restart_event_id: i64 = pool
            .get()
            .await
            .expect("get server-restart outbox connection")
            .query_one(
                "SELECT event_id FROM outbox_events
                 WHERE aggregate_type = 'workflow_run'
                   AND aggregate_id = $1
                   AND event_type = 'workflow.started'
                   AND published_at IS NULL",
                &[&restart_workflow_run_id.as_str()],
            )
            .await
            .expect("find server-restart outbox event")
            .get(0);

        let restart_dispatcher = PostgresOutboxDispatcher::new(
            Arc::clone(&repository),
            WorkerId::parse(format!("nats-restart-publisher-{suffix}"))
                .expect("valid server-restart publisher ID"),
            Arc::new(NatsJetStreamPublisher::new(
                jetstream.clone(),
                restart_subject,
            )),
            OutboxDispatcherConfig::new(
                1,
                Duration::from_millis(10),
                Duration::from_millis(700),
                Duration::from_millis(50),
                Duration::from_millis(250),
            )
            .expect("valid server-restart dispatcher config"),
        );

        // Stop the first JetStream member while PostgreSQL remains healthy.
        // The stream has three replicas, so the two surviving members must
        // retain quorum and accept a publication without node 1.
        let stop = Command::new("docker")
            .args(["stop", &container_name])
            .output()
            .expect("stop the first CI NATS cluster member");
        assert!(
            stop.status.success(),
            "docker stop failed: {}",
            String::from_utf8_lossy(&stop.stderr)
        );

        let first_attempt =
            timeout(Duration::from_secs(2), restart_dispatcher.dispatch_once()).await;
        let acknowledged_on_first_attempt =
            matches!(&first_attempt, Ok(Ok(report)) if report.acknowledged() == 1);

        // If the first attempt was interrupted, wait for its durable outbox
        // lease to expire and retry while node 1 remains down. This proves
        // publication through the remaining JetStream quorum.
        let failover_recovered = if acknowledged_on_first_attempt {
            true
        } else {
            tokio::time::sleep(Duration::from_millis(850)).await;
            timeout(Duration::from_secs(10), async {
                loop {
                    if let Ok(report) = restart_dispatcher.dispatch_once().await {
                        if report.acknowledged() == 1 {
                            break true;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .unwrap_or(false)
        };

        // Restore node 1 before assertions and cleanup, even if quorum failed.
        let start = Command::new("docker")
            .args(["start", &container_name])
            .output()
            .expect("restart the first CI NATS cluster member");
        assert!(
            start.status.success(),
            "docker start failed: {}",
            String::from_utf8_lossy(&start.stderr)
        );

        let ready = timeout(Duration::from_secs(15), async {
            loop {
                if let Ok(Ok(client)) =
                    timeout(Duration::from_millis(300), connect_to_nats(&nats_url)).await
                {
                    drop(client);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        assert!(
            ready.is_ok(),
            "NATS cluster did not become reachable after node restart"
        );
        assert!(
            failover_recovered,
            "two surviving JetStream members did not recover the outbox event while node 1 was down"
        );

        let restart_outbox_state = pool
            .get()
            .await
            .expect("get server-restart outbox verification connection")
            .query_one(
                "SELECT published_at IS NOT NULL, publish_attempts, claim_token
                 FROM outbox_events WHERE event_id = $1",
                &[&restart_event_id],
            )
            .await
            .expect("inspect server-restart outbox event");
        assert!(restart_outbox_state.get::<_, bool>(0));
        assert!(restart_outbox_state.get::<_, i32>(1) >= 1);
        assert!(restart_outbox_state.get::<_, i64>(2) >= 1);

        let mut restart_messages = restart_consumer
            .messages()
            .await
            .expect("open messages after NATS server restart");
        let delivered = timeout(Duration::from_secs(5), restart_messages.next())
            .await
            .expect("event should be delivered after NATS restart")
            .expect("server-restart consumer stream should remain open")
            .expect("read event after NATS restart");
        let delivered_envelope: serde_json::Value = serde_json::from_slice(&delivered.payload)
            .expect("decode event delivered after NATS restart");
        assert_eq!(delivered_envelope["event_id"], restart_event_id.to_string());
        delivered
            .double_ack()
            .await
            .expect("acknowledge event delivered after NATS restart");
        match timeout(Duration::from_millis(250), restart_messages.next()).await {
            Err(_) | Ok(None) => {}
            Ok(Some(Err(error))) => panic!("NATS restart consumer failed: {error}"),
            Ok(Some(Ok(message))) => {
                panic!(
                    "NATS restart created a duplicate message: {:?}",
                    message.subject
                )
            }
        }
        drop(restart_messages);
        let _ = jetstream.delete_stream(restart_stream_name).await;
    }

    let _ = jetstream.delete_stream(stream_name).await;
}

/// Child process for the publisher crash-window test. It commits an outbox
/// claim, stores the event in JetStream, and exits without acknowledging the
/// outbox row. The parent expects exit code 86.
#[tokio::test]
async fn jetstream_child_process_exits_after_broker_storage_before_outbox_ack() {
    let (
        Ok(database_url),
        Ok(nats_url),
        Ok(subject),
        Ok(worker_id),
        Ok(expected_event_id),
        Ok(lease_ms),
    ) = (
        std::env::var("AETHERFLOW_CRASH_PUBLISH_DATABASE_URL"),
        std::env::var("AETHERFLOW_CRASH_PUBLISH_NATS_URL"),
        std::env::var("AETHERFLOW_CRASH_PUBLISH_SUBJECT"),
        std::env::var("AETHERFLOW_CRASH_PUBLISH_WORKER_ID"),
        std::env::var("AETHERFLOW_CRASH_PUBLISH_EVENT_ID"),
        std::env::var("AETHERFLOW_CRASH_PUBLISH_LEASE_MS"),
    )
    else {
        return;
    };

    let config: Config = database_url
        .parse()
        .expect("valid publisher child database URL");
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(manager)
        .max_size(2)
        .build()
        .expect("build publisher child database pool");
    let repository = PgTaskRepository::from_pool(pool);
    let worker_id = WorkerId::parse(worker_id).expect("valid publisher child worker ID");
    let expected_event_id = expected_event_id
        .parse::<i64>()
        .expect("valid expected outbox event ID");
    let lease = Duration::from_millis(lease_ms.parse().expect("valid child lease milliseconds"));
    let claims = repository
        .claim_outbox_events(&worker_id, 1, lease)
        .await
        .expect("claim outbox event in child process");
    assert_eq!(
        claims.len(),
        1,
        "child should claim exactly the crash test event"
    );
    let claim = &claims[0];
    assert_eq!(claim.event_id(), expected_event_id);

    let client = connect_to_nats(&nats_url)
        .await
        .expect("connect publisher child to NATS seed server pool");
    let publisher = NatsJetStreamPublisher::new(jetstream::new(client), subject);
    publisher
        .publish(claim)
        .await
        .expect("JetStream must confirm storage before child termination");

    // Exit without running destructors or calling acknowledge_outbox_event.
    let _ = std::io::stdout().flush();
    std::process::exit(86);
}

/// Child process for the consumer crash-window test. It writes a test side
/// effect and commits the inbox claim, then exits before acknowledging the
/// JetStream message. The parent expects exit code 87.
#[tokio::test]
async fn jetstream_child_process_exits_after_inbox_commit_before_broker_ack() {
    let (
        Ok(database_url),
        Ok(nats_url),
        Ok(stream_name),
        Ok(durable_name),
        Ok(logical_consumer),
        Ok(expected_event_id),
        Ok(worker_id),
    ) = (
        std::env::var("AETHERFLOW_CRASH_CONSUMER_DATABASE_URL"),
        std::env::var("AETHERFLOW_CRASH_CONSUMER_NATS_URL"),
        std::env::var("AETHERFLOW_CRASH_CONSUMER_STREAM"),
        std::env::var("AETHERFLOW_CRASH_CONSUMER_DURABLE"),
        std::env::var("AETHERFLOW_CRASH_CONSUMER_LOGICAL_NAME"),
        std::env::var("AETHERFLOW_CRASH_CONSUMER_EVENT_ID"),
        std::env::var("AETHERFLOW_CRASH_CONSUMER_WORKER_ID"),
    )
    else {
        return;
    };

    let config: Config = database_url
        .parse()
        .expect("valid consumer child database URL");
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(manager)
        .max_size(2)
        .build()
        .expect("build consumer child database pool");
    let inbox = PgInboxRepository::from_pool(pool.clone());
    let client = connect_to_nats(&nats_url)
        .await
        .expect("connect consumer child to NATS seed server pool");
    let jetstream = jetstream::new(client);
    let stream = jetstream
        .get_stream(stream_name)
        .await
        .expect("get consumer crash-window stream in child");
    let consumer: PullConsumer = stream
        .get_consumer(&durable_name)
        .await
        .expect("reattach to durable consumer in child");
    let mut messages = consumer.messages().await.expect("open child pull consumer");
    let message = timeout(Duration::from_secs(5), messages.next())
        .await
        .expect("child should receive pending broker delivery")
        .expect("child consumer stream should remain open")
        .expect("read pending child broker delivery");
    let event: JetStreamEvent =
        serde_json::from_slice(&message.payload).expect("decode child consumer event");
    assert_eq!(event.event_id(), expected_event_id);

    let worker_id = WorkerId::parse(worker_id).expect("valid consumer child worker ID");
    let claim = match inbox
        .try_claim(
            &logical_consumer,
            event.event_id(),
            &worker_id,
            Duration::from_secs(3),
        )
        .await
        .expect("claim inbox event in child process")
    {
        InboxClaimResult::Acquired(claim) => claim,
        other => panic!("child expected a fresh inbox claim, got {other:?}"),
    };

    // Simulate a handler side effect that completed before inbox acknowledgement.
    pool.get()
        .await
        .expect("get child side-effect connection")
        .execute(
            "INSERT INTO aetherflow_test_consumer_effects (event_id) VALUES ($1)",
            &[&event.event_id()],
        )
        .await
        .expect("record single side effect before inbox completion");
    inbox
        .acknowledge(&claim)
        .await
        .expect("commit inbox before child process termination");

    // Exit without calling message.double_ack(). JetStream must redeliver, and
    // a new consumer process must suppress the handler using the inbox row.
    let _ = std::io::stdout().flush();
    std::process::exit(87);
}

/// UID-scoped host-firewall rule for a single worker process's PostgreSQL path.
struct CombinedPacketFilter {
    uid: String,
    port: u16,
    installed: bool,
}

impl CombinedPacketFilter {
    fn new(uid: String, port: u16) -> Self {
        Self {
            uid,
            port,
            installed: false,
        }
    }

    fn command(&self, operation: &str) -> std::process::Output {
        let port = self.port.to_string();
        Command::new("sudo")
            .args([
                "-n",
                "iptables",
                operation,
                "OUTPUT",
                "-m",
                "owner",
                "--uid-owner",
                self.uid.as_str(),
                "-p",
                "tcp",
                "--dport",
                port.as_str(),
                "-j",
                "REJECT",
                "--reject-with",
                "tcp-reset",
            ])
            .output()
            .expect("run combined-chaos iptables rule")
    }

    fn install(&mut self) {
        assert!(!self.installed);
        let result = self.command("-I");
        assert!(
            result.status.success(),
            "install combined-chaos firewall rule: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        self.installed = true;
    }

    fn remove(&mut self) {
        if !self.installed {
            return;
        }
        let result = self.command("-D");
        assert!(
            result.status.success(),
            "remove combined-chaos firewall rule: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        self.installed = false;
    }
}

impl Drop for CombinedPacketFilter {
    fn drop(&mut self) {
        if self.installed {
            let _ = self.command("-D");
            self.installed = false;
        }
    }
}

fn assert_handler_heartbeat_is_fresh(marker: &Path) {
    let observed: u128 = std::fs::read_to_string(marker)
        .expect("read active handler heartbeat marker")
        .trim()
        .parse()
        .expect("handler marker should contain Unix nanoseconds");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    let age = now.saturating_sub(observed);
    assert!(
        age < 500_000_000,
        "handler future stopped making progress {age}ns before worker kill"
    );
}

struct CombinedChaosDirectory(PathBuf);

impl CombinedChaosDirectory {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("aetherflow-combined-chaos-{name}"));
        std::fs::create_dir(&path).expect("create combined-chaos directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o777))
                .expect("make combined-chaos directory accessible");
        }
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for CombinedChaosDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct CombinedChaosChild(Option<Child>);

impl CombinedChaosChild {
    fn try_wait(&mut self) -> Option<ExitStatus> {
        self.0
            .as_mut()
            .expect("combined-chaos child exists")
            .try_wait()
            .expect("poll combined-chaos child")
    }

    fn kill_process_group(child: &mut Child) {
        // The direct child is sudo, which may supervise a separate target
        // process. Kill its dedicated process group so the unprivileged worker
        // cannot survive, renew its lease, or outlive the marker directory.
        let process_group = format!("-{}", child.id());
        let result = Command::new("sudo")
            .args(["-n", "kill", "-KILL", "--", process_group.as_str()])
            .output()
            .expect("kill combined-chaos worker process group");
        if !result.status.success() {
            let _ = child.kill();
        }
    }

    fn kill_and_wait(&mut self) -> ExitStatus {
        let mut child = self.0.take().expect("combined-chaos child exists");
        Self::kill_process_group(&mut child);
        child
            .wait()
            .expect("wait for killed combined-chaos process group")
    }
}

impl Drop for CombinedChaosChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            if child.try_wait().ok().flatten().is_none() {
                Self::kill_process_group(&mut child);
                let _ = child.wait();
            }
        }
    }
}

struct RestartNatsContainer {
    name: String,
    stopped: bool,
}

impl RestartNatsContainer {
    fn new(name: String) -> Self {
        Self {
            name,
            stopped: false,
        }
    }

    fn stop(&mut self) {
        assert!(!self.stopped);
        self.stopped = true;
        let result = Command::new("docker")
            .args(["stop", self.name.as_str()])
            .output()
            .expect("stop NATS member for combined chaos");
        assert!(
            result.status.success(),
            "stop NATS member: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    fn restore(&mut self) {
        if !self.stopped {
            return;
        }
        let result = Command::new("docker")
            .args(["start", self.name.as_str()])
            .output()
            .expect("restart NATS member after combined chaos");
        assert!(
            result.status.success(),
            "restart NATS member: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        self.stopped = false;
    }
}

impl Drop for RestartNatsContainer {
    fn drop(&mut self) {
        if self.stopped {
            let _ = Command::new("docker")
                .args(["start", self.name.as_str()])
                .output();
            self.stopped = false;
        }
    }
}

/// Runs two bounded end-to-end M05 iterations. Each iteration overlaps a
/// worker-only PostgreSQL packet partition, one JetStream member outage, and
/// an abrupt worker-process kill; then checks durable recovery and inbox dedup.
#[tokio::test]
async fn jetstream_combined_chaos_recovers_partitioned_worker_and_member_loss() {
    let (Ok(database_url), Ok(nats_url), Ok(container_name), Ok(packet_mode)) = (
        std::env::var("AETHERFLOW_TEST_NATS_DATABASE_URL"),
        std::env::var("AETHERFLOW_TEST_NATS_URL"),
        std::env::var("AETHERFLOW_TEST_NATS_CONTAINER"),
        std::env::var("AETHERFLOW_TEST_PACKET_PARTITION"),
    ) else {
        eprintln!("combined chaos requires the PostgreSQL/NATS CI fixture; skipping");
        return;
    };
    if packet_mode != "1" {
        eprintln!("combined chaos requires CI packet-partition mode; skipping");
        return;
    }

    let _cluster_fault_guard = NATS_CLUSTER_FAULT_LOCK.lock().await;
    for round in 0..2 {
        let suffix = format!("{}_{}", unique_suffix(), round);
        let subject = format!("aetherflow.integration.{suffix}.combined-chaos");
        let stream_name = format!("AFLOW_CHAOS_{}", suffix.replace('_', ""));
        let durable_name = format!("aflow_chaos_{}", suffix.replace('_', ""));
        let logical_consumer = format!("combined_chaos_{}", suffix.replace('_', ""));

        let pg_config: Config = database_url.parse().expect("valid PostgreSQL URL");
        let pg_port = pg_config.get_ports().first().copied().unwrap_or(5432);
        assert!(
            !pg_config.get_hosts().is_empty(),
            "combined chaos needs a TCP database"
        );
        let manager = Manager::from_config(
            pg_config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(8)
            .build()
            .expect("build combined-chaos PostgreSQL pool");
        let store = PgStore::from_pool(pool.clone());
        store.migrate().await.expect("ensure combined-chaos schema");
        let repository = Arc::new(PgTaskRepository::from_pool(pool.clone()));
        let membership = PgMembershipRepository::from_pool(pool.clone());

        let workflow_id = WorkflowId::parse(format!("combined-chaos-workflow-{suffix}"))
            .expect("valid workflow ID");
        let workflow_run_id = WorkflowRunId::parse(format!("combined-chaos-run-{suffix}"))
            .expect("valid workflow-run ID");
        let task_id =
            TaskId::parse(format!("combined-chaos-task-{suffix}")).expect("valid task ID");
        let task_run_id = TaskRunId::parse(format!("combined-chaos-task-run-{suffix}"))
            .expect("valid task-run ID");
        let node_id =
            NodeId::parse(format!("combined-chaos-node-{suffix}")).expect("valid node ID");
        let old_incarnation = NodeIncarnationId::parse(format!("combined-chaos-old-{suffix}"))
            .expect("valid old incarnation");
        let new_incarnation = NodeIncarnationId::parse(format!("combined-chaos-new-{suffix}"))
            .expect("valid replacement incarnation");
        let old_worker_id = WorkerId::parse(format!("combined-chaos-worker-old-{suffix}"))
            .expect("valid old worker ID");
        let new_worker_id = WorkerId::parse(format!("combined-chaos-worker-new-{suffix}"))
            .expect("valid replacement worker ID");
        let retry_policy = RetryPolicy::new(NonZeroU32::new(2).expect("two attempts"), 0, 0)
            .expect("valid retry policy");

        repository
            .create_workflow(
                &workflow_run_id,
                &workflow_id,
                None,
                &[NewTaskRun::new(
                    task_run_id.clone(),
                    task_id.clone(),
                    TaskInput::empty(),
                    Vec::new(),
                    retry_policy,
                    SystemTime::now() - Duration::from_secs(1),
                )],
            )
            .await
            .expect("create workflow and durable started event");
        repository.promote_due_tasks(8).await.expect("promote task");

        let client = connect_to_nats(&nats_url)
            .await
            .expect("connect NATS seed pool");
        let jetstream = jetstream::new(client);
        let stream = jetstream
            .create_stream(jetstream::stream::Config {
                name: stream_name.clone(),
                subjects: vec![subject.clone()],
                duplicate_window: Duration::from_secs(120),
                num_replicas: 3,
                ..Default::default()
            })
            .await
            .expect("create three-replica chaos stream");
        stream
            .create_consumer(PullConsumerConfig {
                durable_name: Some(durable_name.clone()),
                filter_subject: subject.clone(),
                ack_wait: Duration::from_millis(500),
                max_ack_pending: 16,
                ..Default::default()
            })
            .await
            .expect("create durable chaos consumer");

        // Declare these guards before activating faults. Rust drops them in
        // reverse order: child, firewall, NATS node, then marker directory.
        let directory = CombinedChaosDirectory::new(&suffix);
        let mut nats_node = RestartNatsContainer::new(container_name.clone());
        let marker = directory.join("handler-started");
        let child_executable = directory.join("jetstream-outbox-child");
        std::fs::copy(
            std::env::current_exe().expect("current NATS integration executable"),
            &child_executable,
        )
        .expect("copy child test binary into shared directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&child_executable, std::fs::Permissions::from_mode(0o755))
                .expect("make copied test executable accessible to nobody");
        }

        let uid_output = Command::new("id")
            .args(["-u", "nobody"])
            .output()
            .expect("read nobody UID");
        assert!(uid_output.status.success(), "nobody user is required");
        let uid = String::from_utf8_lossy(&uid_output.stdout)
            .trim()
            .to_owned();
        let sudo_check = Command::new("sudo")
            .args(["-n", "-u", "nobody", "--", "id", "-u"])
            .output()
            .expect("check passwordless sudo");
        assert!(
            sudo_check.status.success(),
            "passwordless sudo required: {}",
            String::from_utf8_lossy(&sudo_check.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&sudo_check.stdout).trim(), uid);
        // Declared before the child guard: unwind kills/reaps the child first,
        // then Drop removes its packet filter, then NATS is restarted.
        let mut packet_filter = CombinedPacketFilter::new(uid.clone(), pg_port);

        let envs = [
            (
                "AETHERFLOW_COMBINED_CHAOS_DATABASE_URL",
                database_url.clone(),
            ),
            (
                "AETHERFLOW_COMBINED_CHAOS_NODE_ID",
                node_id.as_str().to_owned(),
            ),
            (
                "AETHERFLOW_COMBINED_CHAOS_INCARNATION_ID",
                old_incarnation.as_str().to_owned(),
            ),
            (
                "AETHERFLOW_COMBINED_CHAOS_WORKER_ID",
                old_worker_id.as_str().to_owned(),
            ),
            (
                "AETHERFLOW_COMBINED_CHAOS_TASK_ID",
                task_id.as_str().to_owned(),
            ),
            (
                "AETHERFLOW_COMBINED_CHAOS_MARKER",
                marker.to_string_lossy().into_owned(),
            ),
        ];
        let mut command = Command::new("sudo");
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        command.args(["-n", "-u", "nobody", "--", "/usr/bin/env"]);
        for (key, value) in &envs {
            command.arg(format!("{key}={value}"));
        }
        command
            .arg(&child_executable)
            .args([
                "--exact",
                "jetstream_combined_chaos_worker_child_process",
                "--nocapture",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit());
        let mut old_worker =
            CombinedChaosChild(Some(command.spawn().expect("spawn worker child as nobody")));

        timeout(Duration::from_secs(15), async {
            loop {
                if marker.exists() {
                    break;
                }
                if let Some(status) = old_worker.try_wait() {
                    panic!("worker child exited before handler start: {status}");
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("child worker should enter its active handler");

        let running = pool.get().await.expect("get task diagnostic connection")
            .query_one(
                "SELECT status, fencing_token, attempts_started FROM task_runs WHERE task_run_id = $1",
                &[&task_run_id.as_str()],
            ).await.expect("read active task state");
        assert_eq!(running.get::<_, String>(0), "running");
        let old_fencing_token: i64 = running.get(1);
        assert_eq!(running.get::<_, i32>(2), 1);
        let node_lease_before: f64 = pool
            .get()
            .await
            .expect("get pre-partition membership connection")
            .query_one(
                "SELECT extract(epoch FROM lease_expires_at)::double precision
                 FROM cluster_nodes WHERE node_id = $1",
                &[&node_id.as_str()],
            )
            .await
            .expect("read node lease before packet partition")
            .get(0);

        let workflow_event_id: i64 = pool
            .get()
            .await
            .expect("get workflow event connection")
            .query_one(
                "SELECT event_id FROM outbox_events
                 WHERE aggregate_id = $1 AND event_type = 'workflow.started'
                   AND published_at IS NULL",
                &[&workflow_run_id.as_str()],
            )
            .await
            .expect("find workflow.started event")
            .get(0);
        let dispatcher = PostgresOutboxDispatcher::new(
            Arc::clone(&repository),
            WorkerId::parse(format!("combined-chaos-publisher-{suffix}"))
                .expect("valid publisher ID"),
            Arc::new(NatsJetStreamPublisher::new(
                jetstream.clone(),
                subject.clone(),
            )),
            OutboxDispatcherConfig::new(
                1,
                Duration::from_millis(10),
                Duration::from_millis(700),
                Duration::from_millis(50),
                Duration::from_millis(250),
            )
            .expect("valid outbox dispatcher config"),
        );

        // Stop the broker member while the worker still has database access.
        // Once the slow Docker stop has completed, prove the handler is live;
        // then install the worker-only DB packet filter and kill immediately.
        // This keeps all three faults overlapping without allowing a failed
        // membership heartbeat to cooperatively abort the handler first.
        nats_node.stop();
        assert_handler_heartbeat_is_fresh(&marker);
        assert!(
            old_worker.try_wait().is_none(),
            "worker process should still be alive before packet partition"
        );
        packet_filter.install();

        let node_lease_after: f64 = pool
            .get()
            .await
            .expect("get post-partition membership connection")
            .query_one(
                "SELECT extract(epoch FROM lease_expires_at)::double precision
                 FROM cluster_nodes WHERE node_id = $1",
                &[&node_id.as_str()],
            )
            .await
            .expect("read node lease after packet partition")
            .get(0);
        assert!(
            node_lease_after <= node_lease_before + 0.01,
            "partitioned worker renewed its PostgreSQL membership lease"
        );

        assert!(
            old_worker.try_wait().is_none(),
            "worker process should remain alive immediately before abrupt kill"
        );

        // All three failures overlap at the kill boundary: DB egress is
        // packet-filtered, one JetStream member is down, and the active
        // worker process is terminated without graceful drain.
        let child_status = old_worker.kill_and_wait();
        assert!(
            !child_status.success(),
            "worker process must be abruptly killed while handler is active"
        );
        packet_filter.remove();

        let quorum_ack = timeout(Duration::from_secs(12), async {
            loop {
                if let Ok(report) = dispatcher.dispatch_once().await {
                    if report.acknowledged() == 1 {
                        break true;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or(false);
        assert!(
            quorum_ack,
            "JetStream quorum did not acknowledge while one member was down"
        );
        let stopped = Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{.State.Running}}",
                nats_node.name.as_str(),
            ])
            .output()
            .expect("inspect stopped JetStream member");
        assert!(stopped.status.success());
        assert_eq!(String::from_utf8_lossy(&stopped.stdout).trim(), "false");
        assert!(
            pool.get()
                .await
                .expect("get event status connection")
                .query_one(
                    "SELECT published_at IS NOT NULL FROM outbox_events WHERE event_id = $1",
                    &[&workflow_event_id],
                )
                .await
                .expect("verify started event acknowledgement")
                .get::<_, bool>(0)
        );

        nats_node.restore();

        timeout(Duration::from_secs(20), async {
            loop {
                if let Ok(Ok(nats)) =
                    timeout(Duration::from_millis(500), connect_to_nats(&nats_url)).await
                {
                    drop(nats);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("NATS cluster should recover after member restart");

        // Node lease (16 s), worker lease (12 s), and task lease (12 s)
        // are separate durable clocks. Wait for PostgreSQL's authoritative
        // node lease expiry instead of relying on a wall-clock sleep margin.
        timeout(Duration::from_secs(20), async {
            loop {
                let expired: bool = pool
                    .get()
                    .await
                    .expect("get node-lease expiry connection")
                    .query_one(
                        "SELECT lease_expires_at <= clock_timestamp()
                         FROM cluster_nodes WHERE node_id = $1",
                        &[&node_id.as_str()],
                    )
                    .await
                    .expect("read authoritative node lease expiry")
                    .get(0);
                if expired {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("old node membership lease should expire after worker process-group kill");
        let replacement_node = membership
            .join_node(&node_id, &new_incarnation, Duration::from_secs(30))
            .await
            .expect("replacement takes over expired node");
        let replacement_registration = membership
            .register_worker(
                &replacement_node,
                &new_worker_id,
                NonZeroU32::new(1).expect("one worker slot"),
                Duration::from_secs(20),
            )
            .await
            .expect("register replacement worker");
        let recovered = repository
            .recover_expired_leases_registered(&replacement_registration, 8)
            .await
            .expect("recover abandoned task");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].task_run().status(), TaskStatus::RetryWaiting);
        repository
            .promote_due_tasks_registered(&replacement_registration, 8)
            .await
            .expect("promote recovered task");
        let replacement_attempt = AttemptId::parse(format!("combined-chaos-attempt-{suffix}"))
            .expect("valid replacement attempt ID");
        let claim = repository
            .claim_next_registered_for_tasks(
                &replacement_registration,
                std::slice::from_ref(&task_id),
                &replacement_attempt,
                Duration::from_secs(3),
            )
            .await
            .expect("claim recovered task")
            .expect("recovered task is ready");
        assert_eq!(claim.attempt_number().get(), 2);
        assert!(claim.fencing_token() > old_fencing_token as u64);
        let completed = repository
            .complete_registered(
                &task_run_id,
                &replacement_registration,
                claim.task_run().version(),
                claim.fencing_token(),
                TaskOutput::new(b"combined-chaos-replacement-complete".to_vec())
                    .expect("valid output"),
            )
            .await
            .expect("complete recovered task");
        assert_eq!(completed.status(), TaskStatus::Succeeded);
        assert_eq!(
            repository
                .aggregate_workflow_run_registered(&workflow_run_id, &replacement_registration)
                .await
                .expect("aggregate recovered workflow"),
            WorkflowStatus::Succeeded
        );
        membership
            .drain_node(&replacement_node)
            .await
            .expect("drain replacement node");

        let recovered_nats = connect_to_nats(&nats_url).await.expect("reconnect to NATS");
        let recovered_jetstream = jetstream::new(recovered_nats);
        let recovery_dispatcher = PostgresOutboxDispatcher::new(
            Arc::clone(&repository),
            WorkerId::parse(format!("combined-chaos-recovery-{suffix}")).expect("publisher ID"),
            Arc::new(NatsJetStreamPublisher::new(
                recovered_jetstream.clone(),
                subject.clone(),
            )),
            OutboxDispatcherConfig::new(
                16,
                Duration::from_millis(10),
                Duration::from_secs(2),
                Duration::from_millis(50),
                Duration::from_millis(250),
            )
            .expect("valid recovery dispatcher config"),
        );
        timeout(Duration::from_secs(15), async {
            loop {
                let pending: i64 = pool
                    .get()
                    .await
                    .expect("outbox diagnostic connection")
                    .query_one(
                        "SELECT count(*) FROM outbox_events
                         WHERE aggregate_id IN ($1, $2) AND published_at IS NULL",
                        &[&workflow_run_id.as_str(), &task_run_id.as_str()],
                    )
                    .await
                    .expect("count pending outbox rows")
                    .get(0);
                if pending == 0 {
                    break;
                }
                let _ = recovery_dispatcher
                    .dispatch_once()
                    .await
                    .expect("dispatch pending outbox rows after recovery");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("outbox rows should all converge");

        let diagnostic = pool.get().await.expect("get final verification connection");
        let task_state = diagnostic.query_one(
            "SELECT status, attempts_started, fencing_token FROM task_runs WHERE task_run_id = $1",
            &[&task_run_id.as_str()],
        ).await.expect("read final task state");
        assert_eq!(task_state.get::<_, String>(0), "succeeded");
        assert_eq!(task_state.get::<_, i32>(1), 2);
        assert!(task_state.get::<_, i64>(2) > old_fencing_token);
        let workflow_status: String = diagnostic
            .query_one(
                "SELECT status FROM workflow_runs WHERE workflow_run_id = $1",
                &[&workflow_run_id.as_str()],
            )
            .await
            .expect("read final workflow status")
            .get(0);
        assert_eq!(workflow_status, "succeeded");
        let attempt_count: i64 = diagnostic
            .query_one(
                "SELECT count(*) FROM task_attempts WHERE task_run_id = $1",
                &[&task_run_id.as_str()],
            )
            .await
            .expect("count task attempt history")
            .get(0);
        assert_eq!(attempt_count, 2);

        let events = diagnostic
            .query(
                "SELECT event_id, published_at IS NOT NULL FROM outbox_events
             WHERE aggregate_id IN ($1, $2) ORDER BY event_id",
                &[&workflow_run_id.as_str(), &task_run_id.as_str()],
            )
            .await
            .expect("read workflow outbox history");
        let event_ids = events
            .iter()
            .map(|row| {
                assert!(row.get::<_, bool>(1), "outbox event was not published");
                row.get::<_, i64>(0)
            })
            .collect::<Vec<_>>();
        assert!(event_ids.len() >= 2);

        let source_event = diagnostic
            .query_one(
                "SELECT aggregate_type, aggregate_id, aggregate_version, event_type, payload
             FROM outbox_events WHERE event_id = $1",
                &[&event_ids[0]],
            )
            .await
            .expect("read event for intentional duplicate");
        let duplicate_envelope = serde_json::json!({
            "schema_version": 1,
            "event_id": event_ids[0].to_string(),
            "aggregate_type": source_event.get::<_, String>(0),
            "aggregate_id": source_event.get::<_, String>(1),
            "aggregate_version": source_event.get::<_, i64>(2),
            "event_type": source_event.get::<_, String>(3),
            "payload": source_event.get::<_, serde_json::Value>(4),
        });
        let mut duplicate_headers = async_nats::HeaderMap::new();
        duplicate_headers.append("Nats-Msg-Id", format!("combined-chaos-redelivery-{suffix}"));
        let ack = recovered_jetstream
            .publish_with_headers(
                subject.clone(),
                duplicate_headers,
                serde_json::to_vec(&duplicate_envelope)
                    .expect("encode replay event")
                    .into(),
            )
            .await
            .expect("publish logical duplicate with a new broker ID");
        ack.await.expect("store logical duplicate in JetStream");

        let durable_stream = recovered_jetstream
            .get_stream(stream_name.clone())
            .await
            .expect("reopen combined-chaos stream");
        let durable_consumer: PullConsumer = durable_stream
            .get_consumer(&durable_name)
            .await
            .expect("reopen combined-chaos durable consumer");
        let inbox = PgInboxRepository::from_pool(pool.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = Arc::new(CountingHandler {
            calls: Arc::clone(&calls),
            delay: Duration::ZERO,
        });
        let consumer = NatsJetStreamConsumer::new(
            durable_consumer,
            inbox,
            logical_consumer.clone(),
            WorkerId::parse(format!("combined-chaos-consumer-{suffix}")).expect("consumer ID"),
            Duration::from_millis(300),
            handler,
        )
        .expect("create inbox-backed recovery consumer");
        let metrics = consumer.metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let consumer_task = tokio::spawn(async move { consumer.run(shutdown_rx).await });
        let unique_events = event_ids.len();
        timeout(Duration::from_secs(15), async {
            loop {
                let snapshot = metrics.snapshot();
                if snapshot.processed == unique_events && snapshot.duplicates >= 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("consume each unique event and suppress duplicate");
        shutdown_tx.send(true).expect("signal consumer shutdown");
        consumer_task
            .await
            .expect("join recovery consumer")
            .expect("recovery consumer should stop cleanly");
        assert_eq!(calls.load(Ordering::SeqCst), unique_events);
        assert_eq!(metrics.snapshot().duplicates, 1);

        let expected_ids = event_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let inbox_count: i64 = diagnostic
            .query_one(
                "SELECT count(*) FROM consumer_inbox
             WHERE consumer_name = $1 AND status = 'processed' AND event_id = ANY($2)",
                &[&logical_consumer, &expected_ids],
            )
            .await
            .expect("count processed inbox events")
            .get(0);
        assert_eq!(inbox_count, unique_events as i64);
        let pending: i64 = diagnostic
            .query_one(
                "SELECT count(*) FROM outbox_events
             WHERE aggregate_id IN ($1, $2) AND published_at IS NULL",
                &[&workflow_run_id.as_str(), &task_run_id.as_str()],
            )
            .await
            .expect("count pending outbox events")
            .get(0);
        assert_eq!(pending, 0);

        let _ = recovered_jetstream.delete_stream(stream_name).await;
        nats_node.restore();
    }
}

/// Real worker child for the combined-chaos campaign. The parent terminates
/// this process while its registered task handler is still running.
#[tokio::test]
async fn jetstream_combined_chaos_worker_child_process() {
    let (
        Ok(database_url),
        Ok(node_id),
        Ok(incarnation_id),
        Ok(worker_id),
        Ok(task_id),
        Ok(marker_path),
    ) = (
        std::env::var("AETHERFLOW_COMBINED_CHAOS_DATABASE_URL"),
        std::env::var("AETHERFLOW_COMBINED_CHAOS_NODE_ID"),
        std::env::var("AETHERFLOW_COMBINED_CHAOS_INCARNATION_ID"),
        std::env::var("AETHERFLOW_COMBINED_CHAOS_WORKER_ID"),
        std::env::var("AETHERFLOW_COMBINED_CHAOS_TASK_ID"),
        std::env::var("AETHERFLOW_COMBINED_CHAOS_MARKER"),
    )
    else {
        return;
    };

    let config: Config = database_url.parse().expect("valid child PostgreSQL URL");
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(manager)
        .max_size(6)
        .build()
        .expect("build child pool");
    let repository = Arc::new(PgTaskRepository::from_pool(pool.clone()));
    let membership = Arc::new(PgMembershipRepository::from_pool(pool));
    let node_id = NodeId::parse(node_id).expect("valid node ID");
    let incarnation_id = NodeIncarnationId::parse(incarnation_id).expect("valid incarnation ID");
    let worker_id = WorkerId::parse(worker_id).expect("valid worker ID");
    let task_id = TaskId::parse(task_id).expect("valid task ID");
    let marker = PathBuf::from(marker_path);
    let handler_marker = marker.clone();
    let mut registry = TaskRegistry::new();
    registry
        .register(task_id, move |_context: TaskContext| {
            let marker = handler_marker.clone();
            async move {
                let _ = tokio::time::timeout(Duration::from_secs(3_600), async {
                    loop {
                        let timestamp = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("system clock after Unix epoch")
                            .as_nanos()
                            .to_string();
                        std::fs::write(&marker, timestamp)
                            .expect("write live handler heartbeat marker");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                })
                .await;
                Ok(TaskOutput::empty())
            }
        })
        .expect("register long-running handler");

    let node = membership
        .join_node(&node_id, &incarnation_id, Duration::from_secs(16))
        .await
        .expect("join old node incarnation");
    let registration = membership
        .register_worker(
            &node,
            &worker_id,
            NonZeroU32::new(1).expect("one worker"),
            Duration::from_secs(12),
        )
        .await
        .expect("register old worker");
    let config = PostgresWorkerConfig::new(
        NonZeroUsize::new(1).expect("one worker slot"),
        Duration::from_millis(10),
        Duration::from_secs(12),
        Duration::from_secs(8),
        None,
    )
    .expect("valid worker configuration");
    let worker = PostgresTaskWorker::new(
        Arc::clone(&repository),
        Arc::clone(&membership),
        registration,
        registry,
        config,
    );
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn(async move { worker.run(shutdown_rx).await });
    timeout(Duration::from_secs(15), async {
        loop {
            if marker.exists() {
                break;
            }
            if handle.is_finished() {
                panic!("worker terminated before task handler started");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("child worker enters active task handler");
    std::future::pending::<()>().await;
}
