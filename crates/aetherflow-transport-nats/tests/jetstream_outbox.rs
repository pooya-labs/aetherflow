use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aetherflow_core::domain::{
    RetryPolicy, TaskId, TaskRunId, WorkerId, WorkflowId, WorkflowRunId,
};
use aetherflow_runtime::repository::TaskInput;
use aetherflow_storage_postgres::{
    MigrationOutcome, NewTaskRun, OutboxClaim, OutboxDispatchReport, OutboxDispatcherConfig,
    OutboxPublishError, OutboxPublishFuture, OutboxPublisher, PgStore, PgTaskRepository,
    PostgresOutboxDispatcher,
};
use aetherflow_transport_nats::NatsJetStreamPublisher;
use async_nats::jetstream::{self, consumer::pull::Config as PullConsumerConfig};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures_util::StreamExt;
use tokio::time::timeout;
use tokio_postgres::{Config, NoTls};

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

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    format!("{}_{nanos}", std::process::id())
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

    let suffix = unique_suffix();
    let subject = format!("aetherflow.integration.{suffix}.events");
    let stream_name = format!("AFLOW_TEST_{}", suffix.replace('_', ""));
    let consumer_name = format!("aflow_test_{}", suffix.replace('_', ""));

    let client = async_nats::connect(nats_url)
        .await
        .expect("connect to NATS");
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
        MigrationOutcome::Applied { version: 2, .. }
            | MigrationOutcome::AlreadyCurrent { version: 2, .. }
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
    assert_eq!(first_state.get::<_, i64>(1), 1);

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
    assert_eq!(final_state.get::<_, i64>(1), 2);
    drop(client);

    let mut messages = consumer.messages().await.expect("open pull message stream");
    let first = timeout(Duration::from_secs(5), messages.next())
        .await
        .expect("JetStream should deliver the stored event")
        .expect("consumer stream should remain open")
        .expect("read stored JetStream message");
    let envelope: serde_json::Value =
        serde_json::from_slice(&first.payload).expect("decode published event envelope");
    assert_eq!(envelope["schema_version"], 1);
    assert_eq!(envelope["event_id"], first_claim.event_id().to_string());
    assert_eq!(envelope["event_type"], first_claim.event_type());
    assert_eq!(envelope["aggregate_id"], first_claim.aggregate_id());
    first.ack().await.expect("acknowledge test message");

    // The duplicate publication must not create a second stored message within
    // the stream's configured duplicate window.
    match timeout(Duration::from_millis(250), messages.next()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(Err(error))) => panic!("JetStream consumer failed: {error}"),
        Ok(Some(Ok(message))) => {
            panic!("JetStream stored a duplicate event: {:?}", message.subject)
        }
    }

    drop(messages);
    let _ = jetstream.delete_stream(stream_name).await;
}
