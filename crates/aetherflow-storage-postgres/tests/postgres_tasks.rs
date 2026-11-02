use std::num::{NonZeroU32, NonZeroUsize};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use aetherflow_core::domain::{
    AttemptId, IdempotencyKey, NodeId, NodeIncarnationId, RetryPolicy, TaskId, TaskRunId,
    TaskStatus, WorkerId, WorkflowId, WorkflowRunId, WorkflowStatus,
};
use aetherflow_runtime::repository::{TaskInput, TaskOutput};
use aetherflow_runtime::scheduler::{TaskContext, TaskExecutionError, TaskRegistry};
use aetherflow_runtime::workflow::{WorkflowDefinition, WorkflowTask};
use aetherflow_storage_postgres::{
    InboxClaimResult, MigrationOutcome, NewTaskRun, OutboxClaim, OutboxDispatcherConfig,
    OutboxPublishError, OutboxPublishFuture, OutboxPublisher, PgInboxRepository,
    PgMembershipRepository, PgStore, PgTaskRepository, PgWorkflowCoordinator,
    PostgresOutboxDispatcher, PostgresTaskWorker, PostgresWorkerConfig, StorageError, TaskFailure,
};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio::sync::watch;
use tokio_postgres::{Config, NoTls};

#[derive(Default)]
struct RecordingPublisher {
    failures_remaining: std::sync::atomic::AtomicUsize,
    attempted_event_ids: Mutex<Vec<i64>>,
    published_event_ids: Mutex<Vec<i64>>,
}

impl RecordingPublisher {
    fn failing_once() -> Self {
        Self {
            failures_remaining: std::sync::atomic::AtomicUsize::new(1),
            attempted_event_ids: Mutex::new(Vec::new()),
            published_event_ids: Mutex::new(Vec::new()),
        }
    }

    fn attempts(&self) -> Vec<i64> {
        self.attempted_event_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn published(&self) -> Vec<i64> {
        self.published_event_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl OutboxPublisher for RecordingPublisher {
    fn publish<'a>(&'a self, event: &'a OutboxClaim) -> OutboxPublishFuture<'a> {
        let event_id = event.event_id();
        self.attempted_event_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event_id);
        let should_fail = self.failures_remaining.swap(0, Ordering::SeqCst) > 0;
        Box::pin(async move {
            if should_fail {
                Err(OutboxPublishError::new("simulated broker unavailable"))
            } else {
                self.published_event_ids
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(event_id);
                Ok(())
            }
        })
    }
}

#[tokio::test]
async fn postgres_task_claim_heartbeat_completion_and_lease_recovery_are_fenced() {
    let Ok(database_url) = std::env::var("AETHERFLOW_TEST_DATABASE_URL") else {
        eprintln!(
            "AETHERFLOW_TEST_DATABASE_URL is not set; skipping PostgreSQL task-repository test"
        );
        return;
    };

    let config: Config = database_url
        .parse()
        .expect("valid PostgreSQL connection URL");
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
        .expect("build PostgreSQL pool");
    let store = PgStore::from_pool(pool.clone());
    assert!(matches!(
        store.migrate().await.expect("schema is current"),
        MigrationOutcome::AlreadyCurrent { version: 3, .. }
    ));
    let repository = PgTaskRepository::from_pool(pool.clone());

    let workflow_run_id = WorkflowRunId::parse("durable-run-001").expect("valid workflow-run ID");
    let workflow_id = WorkflowId::parse("durable-workflow-001").expect("valid workflow ID");
    let parent_id = TaskRunId::parse("durable-task-run-parent").expect("valid task-run ID");
    let child_id = TaskRunId::parse("durable-task-run-child").expect("valid task-run ID");
    let retry_policy =
        RetryPolicy::new(NonZeroU32::new(2).unwrap(), 0, 0).expect("valid retry policy");
    let due_time = SystemTime::now() - Duration::from_secs(1);

    // Exercise the real durable coordinator and worker against PostgreSQL before
    // the lower-level repository fixtures create any other ready tasks.
    let durable_worker_workflow_run =
        WorkflowRunId::parse("durable-worker-workflow-run").expect("valid workflow-run ID");
    let durable_worker_workflow_id =
        WorkflowId::parse("durable-worker-workflow").expect("valid workflow ID");
    let durable_root_task_id =
        TaskId::parse("durable-worker-root-task").expect("valid root task ID");
    let durable_child_task_id =
        TaskId::parse("durable-worker-child-task").expect("valid child task ID");
    let durable_definition = WorkflowDefinition::new(
        durable_worker_workflow_id.clone(),
        vec![
            WorkflowTask::new(durable_root_task_id.clone(), Vec::new())
                .with_input(TaskInput::new(b"durable-root-input".to_vec()).expect("valid input")),
            WorkflowTask::new(
                durable_child_task_id.clone(),
                vec![durable_root_task_id.clone()],
            ),
        ],
    )
    .expect("valid durable workflow definition");
    let durable_repository = Arc::new(PgTaskRepository::from_pool(pool.clone()));
    let durable_coordinator = PgWorkflowCoordinator::new(Arc::clone(&durable_repository));
    let durable_submission = durable_coordinator
        .submit_workflow_at(
            &durable_worker_workflow_run,
            &durable_definition,
            retry_policy,
            Some(&IdempotencyKey::parse("durable-worker-idempotency").expect("valid key")),
            due_time,
        )
        .await
        .expect("materialize durable workflow");
    let child_execution_count = Arc::new(AtomicUsize::new(0));
    let mut durable_registry = TaskRegistry::new();
    durable_registry
        .register(
            durable_root_task_id.clone(),
            |_context: TaskContext| async {
                Ok(TaskOutput::new(b"durable-root-output".to_vec()).expect("valid output"))
            },
        )
        .expect("register durable root handler");
    let expected_dependency_task_id = durable_root_task_id.clone();
    let child_execution_count_for_handler = Arc::clone(&child_execution_count);
    durable_registry
        .register(
            durable_child_task_id.clone(),
            move |context: TaskContext| {
                let dependency_task_id = expected_dependency_task_id.clone();
                let executions = Arc::clone(&child_execution_count_for_handler);
                async move {
                    let dependency =
                        context
                            .dependency_output(&dependency_task_id)
                            .ok_or_else(|| {
                                TaskExecutionError::permanent("root dependency output missing")
                            })?;
                    if dependency.output().as_bytes() != b"durable-root-output" {
                        return Err(TaskExecutionError::permanent(
                            "root dependency output did not match",
                        ));
                    }
                    executions.fetch_add(1, Ordering::SeqCst);
                    Ok(TaskOutput::new(b"durable-child-output".to_vec()).expect("valid output"))
                }
            },
        )
        .expect("register durable child handler");
    let durable_membership = Arc::new(PgMembershipRepository::from_pool(pool.clone()));
    let durable_node = durable_membership
        .join_node(
            &NodeId::parse("durable-worker-node").expect("valid node ID"),
            &NodeIncarnationId::parse("durable-worker-incarnation")
                .expect("valid process incarnation ID"),
            Duration::from_secs(10),
        )
        .await
        .expect("register durable worker node incarnation");
    let durable_worker_id = WorkerId::parse("durable-worker-integration").expect("valid worker ID");
    let durable_registration = durable_membership
        .register_worker(
            &durable_node,
            &durable_worker_id,
            NonZeroU32::new(2).expect("non-zero worker capacity"),
            Duration::from_secs(5),
        )
        .await
        .expect("register durable worker under its node epoch");
    let durable_worker_config = PostgresWorkerConfig::new(
        NonZeroUsize::new(2).expect("non-zero concurrency"),
        Duration::from_millis(5),
        Duration::from_secs(5),
        Duration::from_secs(1),
        Some(Duration::from_secs(3)),
    )
    .expect("valid durable worker configuration");
    let durable_worker = PostgresTaskWorker::new(
        Arc::clone(&durable_repository),
        Arc::clone(&durable_membership),
        durable_registration,
        durable_registry,
        durable_worker_config,
    );
    let (durable_shutdown_tx, durable_shutdown_rx) = watch::channel(false);
    let durable_worker_handle =
        tokio::spawn(async move { durable_worker.run(durable_shutdown_rx).await });
    let durable_workflow_status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = durable_repository
                .aggregate_workflow_run(&durable_worker_workflow_run)
                .await
                .expect("read durable workflow state");
            if matches!(
                status,
                WorkflowStatus::Succeeded
                    | WorkflowStatus::Failed
                    | WorkflowStatus::Cancelled
                    | WorkflowStatus::TimedOut
            ) {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("durable worker finishes the workflow before the deadline");
    durable_shutdown_tx
        .send(true)
        .expect("request durable worker shutdown");
    durable_worker_handle
        .await
        .expect("join durable worker")
        .expect("durable worker exits cleanly");
    assert!(
        durable_membership
            .active_workers()
            .await
            .expect("list active workers after shutdown")
            .iter()
            .all(|worker| worker.worker_id() != &durable_worker_id),
        "worker shutdown must persist draining state and stop new admissions"
    );
    durable_membership
        .drain_node(&durable_node)
        .await
        .expect("drain durable integration-test node");
    assert_eq!(durable_workflow_status, WorkflowStatus::Succeeded);
    assert_eq!(child_execution_count.load(Ordering::SeqCst), 1);
    let durable_root_run_id = durable_submission
        .task_run_id(&durable_root_task_id)
        .expect("root task-run ID");
    let durable_child_run_id = durable_submission
        .task_run_id(&durable_child_task_id)
        .expect("child task-run ID");
    assert_eq!(
        durable_repository
            .get_task_run(durable_root_run_id)
            .await
            .expect("read durable root task")
            .status(),
        TaskStatus::Succeeded
    );
    assert_eq!(
        durable_repository
            .get_task_run(durable_child_run_id)
            .await
            .expect("read durable child task")
            .output()
            .map(TaskOutput::as_bytes),
        Some(&b"durable-child-output"[..])
    );

    repository
        .create_workflow(
            &workflow_run_id,
            &workflow_id,
            Some(&IdempotencyKey::parse("durable-idem-001").expect("valid key")),
            &[
                NewTaskRun::new(
                    parent_id.clone(),
                    TaskId::parse("durable-task-parent").expect("valid task ID"),
                    TaskInput::new(b"root-input".to_vec()).expect("valid input"),
                    Vec::new(),
                    retry_policy,
                    due_time,
                ),
                NewTaskRun::new(
                    child_id.clone(),
                    TaskId::parse("durable-task-child").expect("valid task ID"),
                    TaskInput::new(b"child-input".to_vec()).expect("valid input"),
                    vec![parent_id.clone()],
                    retry_policy,
                    due_time,
                ),
            ],
        )
        .await
        .expect("create workflow and dependency graph atomically");

    assert_eq!(
        repository
            .promote_due_tasks(10)
            .await
            .expect("promote root"),
        vec![parent_id.clone()]
    );

    let worker_a = WorkerId::parse("durable-worker-a").expect("valid worker ID");
    let worker_b = WorkerId::parse("durable-worker-b").expect("valid worker ID");
    let parent_attempt = AttemptId::parse("durable-attempt-parent").expect("valid attempt ID");
    let parent_claim = repository
        .claim_next(&worker_a, &parent_attempt, Duration::from_secs(30))
        .await
        .expect("claim parent")
        .expect("parent is ready");
    assert_eq!(parent_claim.attempt_number().get(), 1);
    assert_eq!(parent_claim.fencing_token(), 1);
    assert_eq!(parent_claim.task_run().status(), TaskStatus::Running);
    assert!(
        repository
            .heartbeat(
                &parent_id,
                &worker_a,
                parent_claim.task_run().version(),
                parent_claim.fencing_token(),
                Duration::from_secs(30),
            )
            .await
            .expect("heartbeat")
            .duration_since(SystemTime::now())
            .is_ok()
    );
    assert!(matches!(
        repository
            .heartbeat(
                &parent_id,
                &worker_b,
                parent_claim.task_run().version(),
                parent_claim.fencing_token(),
                Duration::from_secs(30),
            )
            .await,
        Err(StorageError::ClaimLost(_))
    ));

    let parent_completed = repository
        .complete(
            &parent_id,
            &worker_a,
            parent_claim.task_run().version(),
            parent_claim.fencing_token(),
            TaskOutput::new(b"parent-output".to_vec()).expect("valid output"),
        )
        .await
        .expect("complete parent atomically");
    assert_eq!(parent_completed.status(), TaskStatus::Succeeded);
    assert_eq!(
        parent_completed.output().map(TaskOutput::as_bytes),
        Some(&b"parent-output"[..])
    );
    assert_eq!(
        repository
            .aggregate_workflow_run(&workflow_run_id)
            .await
            .expect("aggregate incomplete workflow"),
        WorkflowStatus::Running
    );

    assert_eq!(
        repository
            .promote_due_tasks(10)
            .await
            .expect("promote dependency"),
        vec![child_id.clone()]
    );
    let child_claim = repository
        .claim_next(
            &worker_a,
            &AttemptId::parse("durable-attempt-child").expect("valid attempt ID"),
            Duration::from_secs(30),
        )
        .await
        .expect("claim child")
        .expect("child is ready");
    assert_eq!(
        child_claim.task_run().dependencies(),
        std::slice::from_ref(&parent_id)
    );
    repository
        .complete(
            &child_id,
            &worker_a,
            child_claim.task_run().version(),
            child_claim.fencing_token(),
            TaskOutput::new(b"child-output".to_vec()).expect("valid output"),
        )
        .await
        .expect("complete child");
    assert_eq!(
        repository
            .aggregate_workflow_run(&workflow_run_id)
            .await
            .expect("aggregate successful workflow"),
        WorkflowStatus::Succeeded
    );

    let retry_workflow_run =
        WorkflowRunId::parse("durable-retry-run-001").expect("valid workflow-run ID");
    let retry_workflow_id =
        WorkflowId::parse("durable-retry-workflow-001").expect("valid workflow ID");
    let retry_task_run = TaskRunId::parse("durable-retry-task-run-001").expect("valid task-run ID");
    repository
        .create_workflow(
            &retry_workflow_run,
            &retry_workflow_id,
            None,
            &[NewTaskRun::new(
                retry_task_run.clone(),
                TaskId::parse("durable-retry-task-001").expect("valid task ID"),
                TaskInput::empty(),
                Vec::new(),
                retry_policy,
                due_time,
            )],
        )
        .await
        .expect("create retry workflow");
    repository
        .promote_due_tasks(10)
        .await
        .expect("promote retry task");
    let attempt_one = AttemptId::parse("durable-attempt-retry-one").expect("valid attempt ID");
    let expired_claim = repository
        .claim_next(&worker_a, &attempt_one, Duration::from_secs(30))
        .await
        .expect("claim first attempt")
        .expect("retry task ready");
    assert_eq!(expired_claim.fencing_token(), 1);

    let client = pool.get().await.expect("get test connection");
    client.execute(
        "UPDATE task_runs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE task_run_id = $1",
        &[&retry_task_run.as_str()],
    ).await.expect("force lease expiry");
    let recovered = repository
        .recover_expired_leases(10)
        .await
        .expect("recover expired lease");
    assert_eq!(recovered.len(), 1);
    assert!(recovered[0].was_retried());
    assert_eq!(recovered[0].task_run().status(), TaskStatus::RetryWaiting);

    repository
        .promote_due_tasks(10)
        .await
        .expect("promote retry");
    let attempt_two = AttemptId::parse("durable-attempt-retry-two").expect("valid attempt ID");
    let reclaimed = repository
        .claim_next(&worker_b, &attempt_two, Duration::from_secs(30))
        .await
        .expect("reclaim task")
        .expect("retry task ready");
    assert_eq!(reclaimed.attempt_number().get(), 2);
    assert_eq!(reclaimed.fencing_token(), 2);
    assert!(matches!(
        repository
            .heartbeat(
                &retry_task_run,
                &worker_a,
                expired_claim.task_run().version(),
                expired_claim.fencing_token(),
                Duration::from_secs(30),
            )
            .await,
        Err(StorageError::ClaimLost(_))
    ));
    assert!(matches!(
        repository
            .complete(
                &retry_task_run,
                &worker_a,
                expired_claim.task_run().version(),
                expired_claim.fencing_token(),
                TaskOutput::empty(),
            )
            .await,
        Err(StorageError::ClaimLost(_))
    ));

    repository
        .complete(
            &retry_task_run,
            &worker_b,
            reclaimed.task_run().version(),
            reclaimed.fencing_token(),
            TaskOutput::new(b"recovered".to_vec()).expect("valid output"),
        )
        .await
        .expect("new owner completes task");

    let outbox_count: i64 = client
        .query_one(
            "SELECT count(*) FROM outbox_events WHERE aggregate_id IN ($1, $2, $3, $4, $5)",
            &[
                &workflow_run_id.as_str(),
                &parent_id.as_str(),
                &child_id.as_str(),
                &retry_workflow_run.as_str(),
                &retry_task_run.as_str(),
            ],
        )
        .await
        .expect("count transition events")
        .get(0);
    assert!(outbox_count >= 14);

    let publisher_a = WorkerId::parse("durable-publisher-a").expect("valid publisher ID");
    let publisher_b = WorkerId::parse("durable-publisher-b").expect("valid publisher ID");
    let first_batch = repository
        .claim_outbox_events(&publisher_a, 100, Duration::from_secs(30))
        .await
        .expect("claim pending outbox events");
    assert!(first_batch.len() >= 14);
    let first_event = first_batch
        .first()
        .expect("at least one outbox event")
        .clone();
    repository
        .retry_outbox_event(
            first_event.event_id(),
            &publisher_a,
            first_event.claim_token(),
            Duration::ZERO,
            "simulated broker unavailable",
        )
        .await
        .expect("reschedule failed publication");

    let reclaimed_events = repository
        .claim_outbox_events(&publisher_b, 100, Duration::from_secs(30))
        .await
        .expect("reclaim pending outbox events");
    let reclaimed_event = reclaimed_events
        .iter()
        .find(|event| event.event_id() == first_event.event_id())
        .expect("retried event is available to another publisher");
    assert!(reclaimed_event.claim_token() > first_event.claim_token());
    assert_eq!(
        reclaimed_event.publish_attempts(),
        first_event.publish_attempts() + 1
    );

    assert!(matches!(
        repository
            .acknowledge_outbox_event(
                first_event.event_id(),
                &publisher_a,
                first_event.claim_token(),
            )
            .await,
        Err(StorageError::ClaimLost(_))
    ));
    repository
        .acknowledge_outbox_event(
            reclaimed_event.event_id(),
            &publisher_b,
            reclaimed_event.claim_token(),
        )
        .await
        .expect("current publisher acknowledges event");

    assert!(matches!(
        repository
            .acknowledge_outbox_event(
                reclaimed_event.event_id(),
                &publisher_b,
                reclaimed_event.claim_token(),
            )
            .await,
        Err(StorageError::ClaimLost(_))
    ));

    // Exercise handler-reported retryable and permanent failures separately
    // from lease expiry recovery.
    let failure_workflow_run =
        WorkflowRunId::parse("durable-failure-run-001").expect("valid workflow-run ID");
    let failure_workflow_id =
        WorkflowId::parse("durable-failure-workflow-001").expect("valid workflow ID");
    let failure_task_run =
        TaskRunId::parse("durable-failure-task-run-001").expect("valid task-run ID");
    let failure_child_run =
        TaskRunId::parse("durable-failure-child-run-001").expect("valid task-run ID");
    let failure_sibling_run =
        TaskRunId::parse("durable-failure-sibling-run-001").expect("valid task-run ID");
    repository
        .create_workflow(
            &failure_workflow_run,
            &failure_workflow_id,
            None,
            &[
                NewTaskRun::new(
                    failure_task_run.clone(),
                    TaskId::parse("durable-failure-task-001").expect("valid task ID"),
                    TaskInput::empty(),
                    Vec::new(),
                    retry_policy,
                    due_time,
                ),
                NewTaskRun::new(
                    failure_child_run.clone(),
                    TaskId::parse("durable-failure-child-001").expect("valid task ID"),
                    TaskInput::empty(),
                    vec![failure_task_run.clone()],
                    retry_policy,
                    due_time,
                ),
                NewTaskRun::new(
                    failure_sibling_run.clone(),
                    TaskId::parse("durable-failure-sibling-001").expect("valid task ID"),
                    TaskInput::empty(),
                    Vec::new(),
                    retry_policy,
                    due_time,
                ),
            ],
        )
        .await
        .expect("create failure workflow");
    repository
        .promote_due_tasks(10)
        .await
        .expect("promote failure task");
    let failure_attempt_a =
        AttemptId::parse("durable-failure-attempt-a").expect("valid attempt ID");
    let failure_claim_a = repository
        .claim_next(&worker_a, &failure_attempt_a, Duration::from_secs(30))
        .await
        .expect("claim first failure attempt")
        .expect("task is ready");
    let failure_sibling_attempt =
        AttemptId::parse("durable-failure-sibling-attempt").expect("valid attempt ID");
    let failure_sibling_claim = repository
        .claim_next(&worker_b, &failure_sibling_attempt, Duration::from_secs(30))
        .await
        .expect("claim independent sibling")
        .expect("sibling is ready");
    let retrying_failure = repository
        .record_failure(
            &failure_task_run,
            &worker_a,
            failure_claim_a.task_run().version(),
            failure_claim_a.fencing_token(),
            TaskFailure::retryable("temporary", "transient downstream error"),
        )
        .await
        .expect("persist retryable failure");
    assert_eq!(retrying_failure.status(), TaskStatus::RetryWaiting);

    repository
        .promote_due_tasks(10)
        .await
        .expect("promote retry after zero delay");
    let failure_attempt_b =
        AttemptId::parse("durable-failure-attempt-b").expect("valid attempt ID");
    let failure_claim_b = repository
        .claim_next(&worker_b, &failure_attempt_b, Duration::from_secs(30))
        .await
        .expect("claim second failure attempt")
        .expect("retry is ready");
    let failed = repository
        .record_failure(
            &failure_task_run,
            &worker_b,
            failure_claim_b.task_run().version(),
            failure_claim_b.fencing_token(),
            TaskFailure::permanent("permanent", "non-retryable business error"),
        )
        .await
        .expect("persist permanent failure");
    assert_eq!(failed.status(), TaskStatus::Failed);
    assert_eq!(
        repository
            .aggregate_workflow_run(&failure_workflow_run)
            .await
            .expect("aggregate failed workflow"),
        WorkflowStatus::Failed
    );
    assert_eq!(
        repository
            .get_task_run(&failure_child_run)
            .await
            .expect("read cancelled dependency")
            .status(),
        TaskStatus::Cancelled
    );
    assert_eq!(
        repository
            .get_task_run(&failure_sibling_run)
            .await
            .expect("read cancelled running sibling")
            .status(),
        TaskStatus::Cancelled
    );
    assert!(matches!(
        repository
            .complete(
                &failure_sibling_run,
                &worker_b,
                failure_sibling_claim.task_run().version(),
                failure_sibling_claim.fencing_token(),
                TaskOutput::empty(),
            )
            .await,
        Err(StorageError::ClaimLost(_))
    ));
    let sibling_attempt_outcome: String = pool
        .get()
        .await
        .expect("get PostgreSQL connection")
        .query_one(
            "SELECT outcome FROM task_attempts WHERE task_run_id = $1",
            &[&failure_sibling_run.as_str()],
        )
        .await
        .expect("read cancelled sibling attempt")
        .get(0);
    assert_eq!(sibling_attempt_outcome, "cancelled");

    // Durable cancellation of a running task closes its attempt and fences late
    // worker writes in the same transaction as the task.cancelled event.
    let cancel_workflow_run =
        WorkflowRunId::parse("durable-cancel-workflow-run-001").expect("valid workflow-run ID");
    let cancel_workflow =
        WorkflowId::parse("durable-cancel-workflow-001").expect("valid workflow ID");
    let cancel_task_run =
        TaskRunId::parse("durable-cancel-task-run-001").expect("valid task-run ID");
    repository
        .create_workflow(
            &cancel_workflow_run,
            &cancel_workflow,
            None,
            &[NewTaskRun::new(
                cancel_task_run.clone(),
                TaskId::parse("durable-cancel-task-001").expect("valid task ID"),
                TaskInput::empty(),
                Vec::new(),
                retry_policy,
                due_time,
            )],
        )
        .await
        .expect("create cancellation workflow");
    assert_eq!(
        repository
            .promote_due_tasks(10)
            .await
            .expect("promote cancellation task"),
        vec![cancel_task_run.clone()]
    );
    let cancel_attempt = AttemptId::parse("durable-cancel-attempt-001").expect("valid attempt ID");
    let cancel_claim = repository
        .claim_next(&worker_a, &cancel_attempt, Duration::from_secs(30))
        .await
        .expect("claim cancellation task")
        .expect("cancellation task is ready");
    let cancelled = repository
        .cancel_task_run(&cancel_task_run, cancel_claim.task_run().version())
        .await
        .expect("commit durable cancellation");
    assert_eq!(cancelled.status(), TaskStatus::Cancelled);
    assert_eq!(
        repository
            .aggregate_workflow_run(&cancel_workflow_run)
            .await
            .expect("aggregate cancelled workflow"),
        WorkflowStatus::Cancelled
    );
    assert!(matches!(
        repository
            .complete(
                &cancel_task_run,
                &worker_a,
                cancel_claim.task_run().version(),
                cancel_claim.fencing_token(),
                TaskOutput::empty(),
            )
            .await,
        Err(StorageError::ClaimLost(_))
    ));
    let cancel_attempt_outcome: String = pool
        .get()
        .await
        .expect("get PostgreSQL connection")
        .query_one(
            "SELECT outcome FROM task_attempts WHERE task_run_id = $1",
            &[&cancel_task_run.as_str()],
        )
        .await
        .expect("read cancelled attempt outcome")
        .get(0);
    assert_eq!(cancel_attempt_outcome, "cancelled");

    // Timeout has the same first-committed terminal-writer rule, but records
    // the timed_out attempt outcome rather than a retryable handler failure.
    let timeout_workflow_run =
        WorkflowRunId::parse("durable-timeout-workflow-run-001").expect("valid workflow-run ID");
    let timeout_workflow =
        WorkflowId::parse("durable-timeout-workflow-001").expect("valid workflow ID");
    let timeout_task_run =
        TaskRunId::parse("durable-timeout-task-run-001").expect("valid task-run ID");
    repository
        .create_workflow(
            &timeout_workflow_run,
            &timeout_workflow,
            None,
            &[NewTaskRun::new(
                timeout_task_run.clone(),
                TaskId::parse("durable-timeout-task-001").expect("valid task ID"),
                TaskInput::empty(),
                Vec::new(),
                retry_policy,
                due_time,
            )],
        )
        .await
        .expect("create timeout workflow");
    assert_eq!(
        repository
            .promote_due_tasks(10)
            .await
            .expect("promote timeout task"),
        vec![timeout_task_run.clone()]
    );
    let timeout_attempt =
        AttemptId::parse("durable-timeout-attempt-001").expect("valid attempt ID");
    let timeout_claim = repository
        .claim_next(&worker_b, &timeout_attempt, Duration::from_secs(30))
        .await
        .expect("claim timeout task")
        .expect("timeout task is ready");
    let timed_out = repository
        .timeout_task_run(&timeout_task_run, timeout_claim.task_run().version())
        .await
        .expect("commit durable timeout");
    assert_eq!(timed_out.status(), TaskStatus::TimedOut);
    assert_eq!(
        repository
            .aggregate_workflow_run(&timeout_workflow_run)
            .await
            .expect("aggregate timed-out workflow"),
        WorkflowStatus::TimedOut
    );
    assert!(matches!(
        repository
            .complete(
                &timeout_task_run,
                &worker_b,
                timeout_claim.task_run().version(),
                timeout_claim.fencing_token(),
                TaskOutput::empty(),
            )
            .await,
        Err(StorageError::ClaimLost(_))
    ));
    let timeout_attempt_outcome: String = pool
        .get()
        .await
        .expect("get PostgreSQL connection")
        .query_one(
            "SELECT outcome FROM task_attempts WHERE task_run_id = $1",
            &[&timeout_task_run.as_str()],
        )
        .await
        .expect("read timed-out attempt outcome")
        .get(0);
    assert_eq!(timeout_attempt_outcome, "timed_out");

    // Queued work can also be cancelled without fabricating an attempt.
    let queued_workflow_run =
        WorkflowRunId::parse("durable-queued-cancel-run-001").expect("valid workflow-run ID");
    let queued_workflow =
        WorkflowId::parse("durable-queued-cancel-workflow-001").expect("valid workflow ID");
    let queued_task_run =
        TaskRunId::parse("durable-queued-cancel-task-run-001").expect("valid task-run ID");
    repository
        .create_workflow(
            &queued_workflow_run,
            &queued_workflow,
            None,
            &[NewTaskRun::new(
                queued_task_run.clone(),
                TaskId::parse("durable-queued-cancel-task-001").expect("valid task ID"),
                TaskInput::empty(),
                Vec::new(),
                retry_policy,
                due_time,
            )],
        )
        .await
        .expect("create queued cancellation workflow");
    let queued_cancelled = repository
        .cancel_task_run(&queued_task_run, 0)
        .await
        .expect("cancel pending task");
    assert_eq!(queued_cancelled.status(), TaskStatus::Cancelled);
    assert_eq!(
        repository
            .aggregate_workflow_run(&queued_workflow_run)
            .await
            .expect("aggregate queued cancellation"),
        WorkflowStatus::Cancelled
    );
    let queued_attempt_count: i64 = pool
        .get()
        .await
        .expect("get PostgreSQL connection")
        .query_one(
            "SELECT count(*) FROM task_attempts WHERE task_run_id = $1",
            &[&queued_task_run.as_str()],
        )
        .await
        .expect("count queued task attempts")
        .get(0);
    assert_eq!(queued_attempt_count, 0);

    let terminal_events = repository
        .claim_outbox_events(
            &WorkerId::parse("durable-terminal-publisher").expect("valid publisher ID"),
            100,
            Duration::from_secs(30),
        )
        .await
        .expect("claim terminal transition events");
    assert!(terminal_events.iter().any(|event| {
        event.aggregate_id() == cancel_task_run.as_str() && event.event_type() == "task.cancelled"
    }));
    assert!(terminal_events.iter().any(|event| {
        event.aggregate_id() == timeout_task_run.as_str() && event.event_type() == "task.timed_out"
    }));
    assert!(terminal_events.iter().any(|event| {
        event.aggregate_id() == queued_task_run.as_str() && event.event_type() == "task.cancelled"
    }));
    for (aggregate_id, event_type) in [
        (workflow_run_id.as_str(), "workflow.succeeded"),
        (failure_workflow_run.as_str(), "workflow.failed"),
        (cancel_workflow_run.as_str(), "workflow.cancelled"),
        (timeout_workflow_run.as_str(), "workflow.timed_out"),
    ] {
        let event_count: i64 = client
            .query_one(
                "SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = $2",
                &[&aggregate_id, &event_type],
            )
            .await
            .expect("read workflow transition outbox event")
            .get(0);
        assert_eq!(event_count, 1, "expected one {event_type} event");
    }

    // Exercise a real PostgreSQL claim -> failed publish -> retry -> publish ->
    // acknowledgement cycle using a transport adapter that fails exactly once.
    let dispatch_workflow_run =
        WorkflowRunId::parse("durable-outbox-dispatch-workflow-run").expect("valid workflow ID");
    let dispatch_workflow =
        WorkflowId::parse("durable-outbox-dispatch-workflow").expect("valid workflow ID");
    let dispatch_task_run =
        TaskRunId::parse("durable-outbox-dispatch-task-run").expect("valid task-run ID");
    let dispatch_task_id = TaskId::parse("durable-outbox-dispatch-task").expect("valid task ID");
    repository
        .create_workflow(
            &dispatch_workflow_run,
            &dispatch_workflow,
            None,
            &[NewTaskRun::new(
                dispatch_task_run,
                dispatch_task_id,
                TaskInput::empty(),
                Vec::new(),
                retry_policy,
                SystemTime::now() + Duration::from_secs(300),
            )],
        )
        .await
        .expect("create outbox dispatch workflow");
    let dispatch_event_id: i64 = client
        .query_one(
            "SELECT event_id FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'workflow.started'",
            &[&dispatch_workflow_run.as_str()],
        )
        .await
        .expect("read outbox dispatch event ID")
        .get(0);
    let recording_publisher = Arc::new(RecordingPublisher::failing_once());
    let dispatcher = PostgresOutboxDispatcher::new(
        Arc::new(PgTaskRepository::from_pool(pool.clone())),
        WorkerId::parse("durable-outbox-dispatcher").expect("valid dispatcher ID"),
        Arc::clone(&recording_publisher),
        OutboxDispatcherConfig::new(
            10,
            Duration::from_millis(5),
            Duration::from_secs(5),
            Duration::from_millis(10),
            Duration::from_millis(50),
        )
        .expect("valid outbox dispatcher configuration"),
    );
    let first_dispatch = dispatcher
        .dispatch_once()
        .await
        .expect("record failed outbox publication");
    assert_eq!(first_dispatch.claimed(), 1);
    assert_eq!(first_dispatch.rescheduled(), 1);
    assert_eq!(first_dispatch.acknowledged(), 0);
    assert_eq!(recording_publisher.attempts(), vec![dispatch_event_id]);

    tokio::time::sleep(Duration::from_millis(20)).await;
    let second_dispatch = dispatcher
        .dispatch_once()
        .await
        .expect("publish retried outbox event");
    assert_eq!(second_dispatch.claimed(), 1);
    assert_eq!(second_dispatch.acknowledged(), 1);
    assert_eq!(
        recording_publisher.attempts(),
        vec![dispatch_event_id, dispatch_event_id]
    );
    assert_eq!(recording_publisher.published(), vec![dispatch_event_id]);
    let published_event_count: i64 = client
        .query_one(
            "SELECT count(*) FROM outbox_events WHERE event_id = $1 AND published_at IS NOT NULL AND claim_owner IS NULL",
            &[&dispatch_event_id],
        )
        .await
        .expect("verify outbox event acknowledged after publish")
        .get(0);
    assert_eq!(published_event_count, 1);
    let dispatch_metadata = client
        .query_one(
            "SELECT publish_attempts, claim_token FROM outbox_events WHERE event_id = $1",
            &[&dispatch_event_id],
        )
        .await
        .expect("read persisted dispatcher retry metadata");
    let dispatch_attempts: i32 = dispatch_metadata.get(0);
    let dispatch_claim_token: i64 = dispatch_metadata.get(1);
    assert_eq!(dispatch_attempts, 2);
    assert_eq!(dispatch_claim_token, 2);

    // The durable inbox suppresses duplicate delivery, reports concurrent
    // ownership, and fences a previous owner after release or lease expiry.
    let inbox = PgInboxRepository::from_pool(pool.clone());
    let inbox_worker_a = WorkerId::parse("durable-inbox-worker-a").expect("valid worker ID");
    let inbox_worker_b = WorkerId::parse("durable-inbox-worker-b").expect("valid worker ID");

    let completed_claim = match inbox
        .try_claim(
            "billing-consumer",
            "event-inbox-completed-001",
            &inbox_worker_a,
            Duration::from_secs(5),
        )
        .await
        .expect("claim inbox event")
    {
        InboxClaimResult::Acquired(claim) => claim,
        other => panic!("expected a new inbox claim, got {other:?}"),
    };
    assert_eq!(completed_claim.attempts(), 1);
    assert_eq!(completed_claim.claim_token(), 1);
    assert!(matches!(
        inbox
            .try_claim(
                "billing-consumer",
                "event-inbox-completed-001",
                &inbox_worker_b,
                Duration::from_secs(5),
            )
            .await
            .expect("observe the active inbox claim"),
        InboxClaimResult::Busy
    ));
    inbox
        .acknowledge(&completed_claim)
        .await
        .expect("acknowledge completed inbox event");
    inbox
        .acknowledge(&completed_claim)
        .await
        .expect("make inbox acknowledgement idempotent");
    assert!(matches!(
        inbox
            .try_claim(
                "billing-consumer",
                "event-inbox-completed-001",
                &inbox_worker_b,
                Duration::from_secs(5),
            )
            .await
            .expect("deduplicate processed event"),
        InboxClaimResult::AlreadyProcessed
    ));

    let released_claim = match inbox
        .try_claim(
            "billing-consumer",
            "event-inbox-release-001",
            &inbox_worker_a,
            Duration::from_secs(5),
        )
        .await
        .expect("claim event before releasing it")
    {
        InboxClaimResult::Acquired(claim) => claim,
        other => panic!("expected a releasable inbox claim, got {other:?}"),
    };
    inbox
        .release(&released_claim, Some("simulated consumer failure"))
        .await
        .expect("release failed inbox event");
    let reclaimed_claim = match inbox
        .try_claim(
            "billing-consumer",
            "event-inbox-release-001",
            &inbox_worker_b,
            Duration::from_secs(5),
        )
        .await
        .expect("reclaim released inbox event")
    {
        InboxClaimResult::Acquired(claim) => claim,
        other => panic!("expected released inbox event to be claimable, got {other:?}"),
    };
    assert_eq!(reclaimed_claim.attempts(), 2);
    assert_eq!(reclaimed_claim.claim_token(), 2);
    assert!(matches!(
        inbox.acknowledge(&released_claim).await,
        Err(StorageError::InboxClaimLost { .. })
    ));
    inbox
        .acknowledge(&reclaimed_claim)
        .await
        .expect("acknowledge reclaimed inbox event");

    let renewable_claim = match inbox
        .try_claim(
            "billing-consumer",
            "event-inbox-renew-001",
            &inbox_worker_a,
            Duration::from_millis(200),
        )
        .await
        .expect("claim inbox event before renewal")
    {
        InboxClaimResult::Acquired(claim) => claim,
        other => panic!("expected renewable inbox claim, got {other:?}"),
    };
    tokio::time::sleep(Duration::from_millis(25)).await;
    inbox
        .renew(&renewable_claim, Duration::from_secs(5))
        .await
        .expect("renew active inbox lease");
    tokio::time::sleep(Duration::from_millis(225)).await;
    assert!(matches!(
        inbox
            .try_claim(
                "billing-consumer",
                "event-inbox-renew-001",
                &inbox_worker_b,
                Duration::from_secs(5),
            )
            .await
            .expect("check renewed inbox lease"),
        InboxClaimResult::Busy
    ));
    inbox
        .acknowledge(&renewable_claim)
        .await
        .expect("acknowledge event with renewed lease");

    let expiring_claim = match inbox
        .try_claim(
            "billing-consumer",
            "event-inbox-expiry-001",
            &inbox_worker_a,
            Duration::from_millis(20),
        )
        .await
        .expect("claim inbox event with a short lease")
    {
        InboxClaimResult::Acquired(claim) => claim,
        other => panic!("expected an expiring inbox claim, got {other:?}"),
    };
    tokio::time::sleep(Duration::from_millis(35)).await;
    let after_expiry_claim = match inbox
        .try_claim(
            "billing-consumer",
            "event-inbox-expiry-001",
            &inbox_worker_b,
            Duration::from_secs(5),
        )
        .await
        .expect("reclaim inbox event after lease expiry")
    {
        InboxClaimResult::Acquired(claim) => claim,
        other => panic!("expected expired inbox event to be reclaimable, got {other:?}"),
    };
    assert_eq!(after_expiry_claim.attempts(), 2);
    assert_eq!(after_expiry_claim.claim_token(), 2);
    assert!(matches!(
        inbox.renew(&expiring_claim, Duration::from_secs(5)).await,
        Err(StorageError::InboxClaimLost { .. })
    ));
    assert!(matches!(
        inbox.acknowledge(&expiring_claim).await,
        Err(StorageError::InboxClaimLost { .. })
    ));
    inbox
        .acknowledge(&after_expiry_claim)
        .await
        .expect("acknowledge inbox event after lease recovery");

    // Force the outbox insert to fail after the task and attempt UPDATEs have
    // executed. PostgreSQL must roll the whole completion transaction back.
    let rollback_workflow_run =
        WorkflowRunId::parse("durable-outbox-rollback-workflow").expect("valid workflow ID");
    let rollback_workflow =
        WorkflowId::parse("durable-outbox-rollback-definition").expect("valid workflow ID");
    let rollback_task_run =
        TaskRunId::parse("durable-outbox-rollback-task-run").expect("valid task-run ID");
    repository
        .create_workflow(
            &rollback_workflow_run,
            &rollback_workflow,
            None,
            &[NewTaskRun::new(
                rollback_task_run.clone(),
                TaskId::parse("durable-outbox-rollback-task").expect("valid task ID"),
                TaskInput::empty(),
                Vec::new(),
                retry_policy,
                due_time,
            )],
        )
        .await
        .expect("create outbox rollback workflow");
    repository
        .promote_due_tasks(10)
        .await
        .expect("promote outbox rollback task");
    let rollback_worker =
        WorkerId::parse("durable-outbox-rollback-worker").expect("valid worker ID");
    let rollback_attempt =
        AttemptId::parse("durable-outbox-rollback-attempt").expect("valid attempt ID");
    let rollback_claim = repository
        .claim_next(&rollback_worker, &rollback_attempt, Duration::from_secs(30))
        .await
        .expect("claim outbox rollback task")
        .expect("rollback task is ready");
    client
        .batch_execute(
            "ALTER TABLE outbox_events ADD CONSTRAINT aetherflow_test_reject_outbox_aggregate CHECK (aggregate_id <> 'durable-outbox-rollback-task-run') NOT VALID",
        )
        .await
        .expect("install temporary outbox failure constraint");
    let failed_commit = repository
        .complete(
            &rollback_task_run,
            &rollback_worker,
            rollback_claim.task_run().version(),
            rollback_claim.fencing_token(),
            TaskOutput::new(b"must-rollback".to_vec()).expect("valid output"),
        )
        .await;
    assert!(
        matches!(failed_commit, Err(StorageError::Database(_))),
        "outbox constraint should reject the completion event; got {failed_commit:?}"
    );
    let rolled_back_task = repository
        .get_task_run(&rollback_task_run)
        .await
        .expect("read task after failed completion transaction");
    assert_eq!(rolled_back_task.status(), TaskStatus::Running);
    assert_eq!(
        rolled_back_task.version(),
        rollback_claim.task_run().version()
    );
    let rolled_back_attempt: String = client
        .query_one(
            "SELECT outcome FROM task_attempts WHERE task_run_id = $1",
            &[&rollback_task_run.as_str()],
        )
        .await
        .expect("read attempt after failed completion transaction")
        .get(0);
    assert_eq!(rolled_back_attempt, "running");
    let rolled_back_event_count: i64 = client
        .query_one(
            "SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'task.succeeded'",
            &[&rollback_task_run.as_str()],
        )
        .await
        .expect("count events after failed completion transaction")
        .get(0);
    assert_eq!(rolled_back_event_count, 0);
    client
        .batch_execute(
            "ALTER TABLE outbox_events DROP CONSTRAINT aetherflow_test_reject_outbox_aggregate",
        )
        .await
        .expect("remove temporary outbox failure constraint");
    let committed_after_repair = repository
        .complete(
            &rollback_task_run,
            &rollback_worker,
            rollback_claim.task_run().version(),
            rollback_claim.fencing_token(),
            TaskOutput::new(b"committed-after-outbox-repair".to_vec()).expect("valid output"),
        )
        .await
        .expect("complete task after restoring outbox insert");
    assert_eq!(committed_after_repair.status(), TaskStatus::Succeeded);
    assert_eq!(
        repository
            .aggregate_workflow_run(&rollback_workflow_run)
            .await
            .expect("aggregate workflow after repaired outbox"),
        WorkflowStatus::Succeeded
    );

    // Persist one more running attempt, expire its lease, then discard every
    // pool/repository handle before starting a separate test process. The child
    // receives only identifiers and the database URL; all state is reloaded
    // from PostgreSQL after the simulated process crash.
    let restart_workflow_run =
        WorkflowRunId::parse("durable-process-restart-workflow-run").expect("valid workflow ID");
    let restart_workflow =
        WorkflowId::parse("durable-process-restart-workflow").expect("valid workflow ID");
    let restart_task_run =
        TaskRunId::parse("durable-process-restart-task-run").expect("valid task-run ID");
    repository
        .create_workflow(
            &restart_workflow_run,
            &restart_workflow,
            None,
            &[NewTaskRun::new(
                restart_task_run.clone(),
                TaskId::parse("durable-process-restart-task").expect("valid task ID"),
                TaskInput::new(b"survive-process-restart".to_vec()).expect("valid input"),
                Vec::new(),
                retry_policy,
                due_time,
            )],
        )
        .await
        .expect("create process-restart workflow");
    assert_eq!(
        repository
            .promote_due_tasks(10)
            .await
            .expect("promote process-restart task"),
        vec![restart_task_run.clone()]
    );
    let restart_attempt_one =
        AttemptId::parse("durable-process-restart-attempt-one").expect("valid attempt ID");
    let restart_claim = repository
        .claim_next(&worker_a, &restart_attempt_one, Duration::from_secs(30))
        .await
        .expect("claim process-restart task")
        .expect("process-restart task is ready");
    assert_eq!(restart_claim.fencing_token(), 1);
    client
        .execute(
            "UPDATE task_runs SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE task_run_id = $1",
            &[&restart_task_run.as_str()],
        )
        .await
        .expect("expire lease before process restart");

    drop(client);
    drop(repository);
    drop(store);
    drop(pool);

    let database_url = std::env::var("AETHERFLOW_TEST_DATABASE_URL")
        .expect("database URL was checked at test start");
    let child_status = Command::new(std::env::current_exe().expect("current test executable"))
        .args([
            "--exact",
            "postgres_restart_child_process_recovers_expired_task",
            "--nocapture",
        ])
        .env("AETHERFLOW_RESTART_TEST_DATABASE_URL", database_url)
        .env("AETHERFLOW_RESTART_TASK_RUN_ID", restart_task_run.as_str())
        .env(
            "AETHERFLOW_RESTART_WORKFLOW_RUN_ID",
            restart_workflow_run.as_str(),
        )
        .status()
        .expect("spawn a fresh recovery test process");
    assert!(
        child_status.success(),
        "fresh process failed to recover the persisted task (exit: {child_status})"
    );
}

/// Runs in a separate OS process when launched by the durable task integration
/// test. During the normal test pass it is a no-op because the restart-specific
/// environment is not set.
#[tokio::test]
async fn postgres_restart_child_process_recovers_expired_task() {
    let (Ok(database_url), Ok(task_run_id), Ok(workflow_run_id)) = (
        std::env::var("AETHERFLOW_RESTART_TEST_DATABASE_URL"),
        std::env::var("AETHERFLOW_RESTART_TASK_RUN_ID"),
        std::env::var("AETHERFLOW_RESTART_WORKFLOW_RUN_ID"),
    ) else {
        return;
    };

    let config: Config = database_url
        .parse()
        .expect("valid PostgreSQL connection URL");
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
        .expect("build fresh-process PostgreSQL pool");
    let store = PgStore::from_pool(pool.clone());
    assert!(matches!(
        store.migrate().await.expect("verify schema after restart"),
        MigrationOutcome::AlreadyCurrent { version: 3, .. }
    ));
    let repository = PgTaskRepository::from_pool(pool);

    let task_run_id = TaskRunId::parse(task_run_id).expect("valid persisted task-run ID");
    let workflow_run_id =
        WorkflowRunId::parse(workflow_run_id).expect("valid persisted workflow-run ID");
    let recovered = repository
        .recover_expired_leases(100)
        .await
        .expect("recover leases after process restart");
    let recovered_task = recovered
        .iter()
        .find(|lease| lease.task_run().task_run_id() == &task_run_id)
        .expect("expired task is recovered from PostgreSQL");
    assert!(recovered_task.was_retried());
    assert_eq!(recovered_task.task_run().status(), TaskStatus::RetryWaiting);

    assert_eq!(
        repository
            .promote_due_tasks(10)
            .await
            .expect("promote recovered task"),
        vec![task_run_id.clone()]
    );
    let worker = WorkerId::parse("durable-process-restart-worker-b").expect("valid worker ID");
    let attempt =
        AttemptId::parse("durable-process-restart-attempt-two").expect("valid attempt ID");
    let claim = repository
        .claim_next(&worker, &attempt, Duration::from_secs(30))
        .await
        .expect("claim recovered task in fresh process")
        .expect("recovered task is ready");
    assert_eq!(claim.attempt_number().get(), 2);
    assert_eq!(claim.fencing_token(), 2);
    assert_eq!(
        claim.task_run().input().as_bytes(),
        b"survive-process-restart"
    );

    let completed = repository
        .complete(
            &task_run_id,
            &worker,
            claim.task_run().version(),
            claim.fencing_token(),
            TaskOutput::new(b"completed-after-restart".to_vec()).expect("valid output"),
        )
        .await
        .expect("complete recovered task in fresh process");
    assert_eq!(completed.status(), TaskStatus::Succeeded);
    assert_eq!(
        repository
            .aggregate_workflow_run(&workflow_run_id)
            .await
            .expect("aggregate workflow in fresh process"),
        WorkflowStatus::Succeeded
    );
}
#[tokio::test]
async fn registered_worker_keeps_membership_alive_while_draining_inflight_task() {
    let Ok(database_url) = std::env::var("AETHERFLOW_TEST_DATABASE_URL") else {
        eprintln!(
            "AETHERFLOW_TEST_DATABASE_URL is not set; skipping registered worker drain integration test"
        );
        return;
    };

    let config: Config = database_url.parse().expect("valid PostgreSQL URL");
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    let pool = Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("build PostgreSQL pool");
    let store = PgStore::from_pool(pool.clone());
    store.migrate().await.expect("apply current PostgreSQL schema");

    let repository = Arc::new(PgTaskRepository::from_pool(pool.clone()));
    let membership = Arc::new(PgMembershipRepository::from_pool(pool));
    let node_id = NodeId::parse("drain-worker-node").expect("valid node ID");
    let incarnation_id =
        NodeIncarnationId::parse("drain-worker-incarnation").expect("valid incarnation ID");
    let worker_id = WorkerId::parse("drain-worker-instance").expect("valid worker ID");
    let node = membership
        .join_node(&node_id, &incarnation_id, Duration::from_millis(600))
        .await
        .expect("join node before worker startup");
    let registration = membership
        .register_worker(
            &node,
            &worker_id,
            NonZeroU32::new(1).expect("positive worker capacity"),
            Duration::from_millis(600),
        )
        .await
        .expect("register worker before startup");

    let workflow_id = WorkflowId::parse("drain-worker-workflow").expect("valid workflow ID");
    let workflow_run_id =
        WorkflowRunId::parse("drain-worker-workflow-run").expect("valid workflow-run ID");
    let task_id = TaskId::parse("drain-worker-task").expect("valid task ID");
    let task_run_id = TaskRunId::parse("drain-worker-task-run").expect("valid task-run ID");
    let retry_policy = RetryPolicy::new(
        NonZeroU32::new(1).expect("positive attempt budget"),
        0,
        0,
    )
    .expect("valid retry policy");
    repository
        .create_workflow(
            &workflow_run_id,
            &workflow_id,
            None,
            &[NewTaskRun::new(
                task_run_id.clone(),
                task_id.clone(),
                TaskInput::new(b"draining-worker-input".to_vec()).expect("valid task input"),
                Vec::new(),
                retry_policy,
                SystemTime::now() - Duration::from_secs(1),
            )],
        )
        .await
        .expect("create task for draining worker");

    let handler_started = Arc::new(AtomicBool::new(false));
    let handler_started_flag = Arc::clone(&handler_started);
    let mut registry = TaskRegistry::new();
    registry
        .register(task_id, move |_context: TaskContext| {
            let started = Arc::clone(&handler_started_flag);
            async move {
                started.store(true, Ordering::SeqCst);
                // Longer than the initial node/worker membership lease. The
                // worker must continue renewing membership after it starts draining.
                tokio::time::sleep(Duration::from_millis(900)).await;
                Ok(TaskOutput::new(b"finished-while-draining".to_vec())
                    .expect("valid task output"))
            }
        })
        .expect("register slow task handler");

    let worker_config = PostgresWorkerConfig::new(
        NonZeroUsize::new(1).expect("positive worker concurrency"),
        Duration::from_millis(5),
        Duration::from_millis(600),
        Duration::from_millis(100),
        Some(Duration::from_secs(3)),
    )
    .expect("valid short-lease worker config")
    .with_shutdown_timeouts(Duration::from_secs(2), Duration::from_secs(1))
    .expect("valid graceful drain policy");
    let worker = PostgresTaskWorker::new(
        Arc::clone(&repository),
        Arc::clone(&membership),
        registration,
        registry,
        worker_config,
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_handle = tokio::spawn(async move { worker.run(shutdown_rx).await });

    tokio::time::timeout(Duration::from_secs(3), async {
        while !handler_started.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("registered worker starts the in-flight handler");

    shutdown_tx
        .send(true)
        .expect("request graceful worker shutdown while handler is running");
    tokio::time::timeout(Duration::from_secs(5), worker_handle)
        .await
        .expect("worker drains the active handler before exiting")
        .expect("join registered worker task")
        .expect("registered worker exits cleanly");

    let completed = repository
        .get_task_run(&task_run_id)
        .await
        .expect("read task after graceful worker drain");
    assert_eq!(completed.status(), TaskStatus::Succeeded);
    assert_eq!(
        completed.output().map(TaskOutput::as_bytes),
        Some(&b"finished-while-draining"[..])
    );
    assert!(
        membership
            .active_workers()
            .await
            .expect("list active workers after drain")
            .iter()
            .all(|worker| worker.worker_id() != &worker_id),
        "draining worker must not admit more tasks"
    );
    membership
        .drain_node(&node)
        .await
        .expect("drain integration-test node");
}

