use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};

use aetherflow_core::domain::{
    AttemptId, IdempotencyKey, RetryPolicy, TaskId, TaskRunId, TaskStatus, WorkerId, WorkflowId,
    WorkflowRunId,
};
use aetherflow_runtime::repository::{TaskInput, TaskOutput};
use aetherflow_storage_postgres::{
    MigrationOutcome, NewTaskRun, PgStore, PgTaskRepository, StorageError,
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
    assert_eq!(child_claim.task_run().dependencies(), std::slice::from_ref(&parent_id));
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
}
