use std::num::NonZeroU32;
use std::process::Command;
use std::time::{Duration, SystemTime};

use aetherflow_core::domain::{
    AttemptId, IdempotencyKey, RetryPolicy, TaskId, TaskRunId, TaskStatus, WorkerId, WorkflowId,
    WorkflowRunId, WorkflowStatus,
};
use aetherflow_runtime::repository::{TaskInput, TaskOutput};
use aetherflow_storage_postgres::{
    MigrationOutcome, NewTaskRun, PgStore, PgTaskRepository, StorageError, TaskFailure,
};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{Config, NoTls};

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
        MigrationOutcome::AlreadyCurrent { version: 1, .. }
    ));
    let repository = PgTaskRepository::from_pool(pool.clone());

    let workflow_run_id = WorkflowRunId::parse("durable-run-001").expect("valid workflow-run ID");
    let workflow_id = WorkflowId::parse("durable-workflow-001").expect("valid workflow ID");
    let parent_id = TaskRunId::parse("durable-task-run-parent").expect("valid task-run ID");
    let child_id = TaskRunId::parse("durable-task-run-child").expect("valid task-run ID");
    let retry_policy =
        RetryPolicy::new(NonZeroU32::new(2).unwrap(), 0, 0).expect("valid retry policy");
    let due_time = SystemTime::now() - Duration::from_secs(1);

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
        "outbox constraint should reject the completion event"
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
        MigrationOutcome::AlreadyCurrent { version: 1, .. }
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
