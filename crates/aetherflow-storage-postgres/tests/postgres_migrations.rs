use aetherflow_core::domain::{
    AttemptId, NodeId, NodeIncarnationId, RetryPolicy, TaskId, TaskRunId, TaskStatus,
    TenantId, WorkerId, WorkflowId, WorkflowRunId, WorkflowStatus,
};
use aetherflow_runtime::repository::{TaskInput, TaskOutput};
use aetherflow_runtime::workflow::{WorkflowDefinition, WorkflowTask};
use aetherflow_storage_postgres::{
    InboxClaimResult, MigrationOutcome, NewTaskRun, PgInboxRepository,
    PgMembershipRepository, PgStore, PgTaskRepository, PgTenantCredentialRepository,
    PgWorkflowCoordinator, StorageError, TaskFailure,
    TenantPermission, TenantRole, initial_migration_checksum,
};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use sha2::Digest;
use tokio_postgres::{Config, NoTls};

#[tokio::test]
async fn postgres_migration_is_atomic_locked_checksummed_and_fail_closed() {
    let Ok(database_url) = std::env::var("AETHERFLOW_MIGRATION_TEST_DATABASE_URL") else {
        eprintln!(
            "AETHERFLOW_MIGRATION_TEST_DATABASE_URL is not set; skipping isolated PostgreSQL migration integration test"
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
        .max_size(4)
        .build()
        .expect("build PostgreSQL pool");
    let store = PgStore::from_pool(pool.clone());

    store
        .check_connection()
        .await
        .expect("PostgreSQL is reachable");

    // Simultaneous first-start calls must serialize: one applies, the other
    // observes the committed ledger and returns AlreadyCurrent.
    let (first_result, second_result) = tokio::join!(store.migrate(), store.migrate());
    let outcomes = [
        first_result.expect("first migration call succeeds"),
        second_result.expect("second migration call succeeds"),
    ];
    let applied = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, MigrationOutcome::Applied { version: 6, .. }))
        .count();
    let already_current = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, MigrationOutcome::AlreadyCurrent { version: 6, .. }))
        .count();
    assert_eq!(applied, 1);
    assert_eq!(already_current, 1);

    // The additive ownership migration creates one reserved legacy tenant.
    let legacy_tenant: bool = pool
        .get()
        .await
        .expect("get legacy tenant verification connection")
        .query_one(
            "SELECT is_system FROM tenants WHERE tenant_id = 'legacy-default'",
            &[],
        )
        .await
        .expect("legacy tenant was bootstrapped")
        .get(0);
    assert!(legacy_tenant);

    let client = pool
        .get()
        .await
        .expect("get ownership verification connection");
    let tenant_columns = client
        .query_one(
            "SELECT count(*) FROM information_schema.columns
             WHERE table_schema = 'public' AND column_name = 'tenant_id'
               AND table_name IN (
                 'workflow_runs', 'task_runs', 'task_run_dependencies',
                 'task_attempts', 'outbox_events', 'consumer_inbox'
               )",
            &[],
        )
        .await
        .expect("read tenant ownership columns");
    let tenant_column_count: i64 = tenant_columns.get(0);
    assert_eq!(tenant_column_count, 6);

    // Create two independent tenant trees and a tenant-bound API credential.
    client
        .batch_execute(
            "INSERT INTO tenants (tenant_id, display_name, status) VALUES
               ('tenant-a', 'Tenant A', 'active'),
               ('tenant-b', 'Tenant B', 'active');
             INSERT INTO tenant_principals
               (principal_id, tenant_id, display_name, status)
             VALUES
               ('principal-a', 'tenant-a', 'Tenant A operator', 'active'),
               ('principal-b', 'tenant-b', 'Tenant B operator', 'active');
             INSERT INTO workflow_runs (workflow_run_id, workflow_id, tenant_id, status)
             VALUES
               ('workflow-a', 'workflow-definition-a', 'tenant-a', 'pending'),
               ('workflow-b', 'workflow-definition-b', 'tenant-b', 'pending');
             INSERT INTO task_runs (task_run_id, workflow_run_id, task_id, tenant_id, status)
             VALUES
               ('task-a-parent', 'workflow-a', 'task-parent', 'tenant-a', 'pending'),
               ('task-a-another-parent', 'workflow-a', 'task-parent-2', 'tenant-a', 'pending'),
               ('task-a-child', 'workflow-a', 'task-child', 'tenant-a', 'pending'),
               ('task-b-child', 'workflow-b', 'task-child', 'tenant-b', 'pending');
             INSERT INTO task_run_dependencies
               (tenant_id, workflow_run_id, child_task_run_id, parent_task_run_id, dependency_position)
             VALUES ('tenant-a', 'workflow-a', 'task-a-child', 'task-a-parent', 0);"
        )
        .await
        .expect("tenant-owned rows and same-tenant dependency are accepted");

    let tenant_token = "AetherFlowTenantCredentialTest0123456789";
    let tenant_digest = format!("{:x}", sha2::Sha256::digest(tenant_token.as_bytes()));
    let token_prefix = &tenant_token[..12];
    client
        .execute(
            "INSERT INTO tenant_credentials
               (credential_id, tenant_id, principal_id, token_sha256, token_prefix, role, status)
             VALUES ('credential-a', 'tenant-a', 'principal-a', $1, $2, 'tenant_operator', 'active')",
            &[&tenant_digest, &token_prefix],
        )
        .await
        .expect("seed active tenant credential");

    let credentials = PgTenantCredentialRepository::from_pool(pool.clone());
    let context = credentials
        .authenticate_token(tenant_token)
        .await
        .expect("active tenant credential authenticates");
    assert_eq!(context.tenant_id().as_str(), "tenant-a");
    assert_eq!(context.principal_id().as_str(), "principal-a");
    assert_eq!(context.role(), TenantRole::Operator);
    assert!(context.allows(TenantPermission::Read));
    assert!(context.allows(TenantPermission::Submit));
    assert!(context.allows(TenantPermission::Cancel));
    assert!(!context.allows(TenantPermission::ManageCredentials));

    // Tenant-scoped reads must hide another tenant's resources exactly as
    // they hide nonexistent identifiers, even before forced RLS is enabled.
    let tasks = PgTaskRepository::from_pool(pool.clone());
    let workflow_a = WorkflowRunId::parse("workflow-a").expect("valid workflow A ID");
    let workflow_b = WorkflowRunId::parse("workflow-b").expect("valid workflow B ID");
    let own_workflow = tasks
        .get_workflow_run_for_tenant(&context, &workflow_a)
        .await
        .expect("tenant A can read its workflow");
    assert_eq!(own_workflow.workflow_run_id().as_str(), "workflow-a");
    assert_eq!(own_workflow.task_runs().len(), 3);

    let foreign_workflow = tasks
        .get_workflow_run_for_tenant(&context, &workflow_b)
        .await;
    let missing_workflow = tasks
        .get_workflow_run_for_tenant(
            &context,
            &WorkflowRunId::parse("workflow-missing").expect("valid missing workflow ID"),
        )
        .await;
    assert!(matches!(
        foreign_workflow,
        Err(StorageError::WorkflowRunNotFound(_))
    ));
    assert!(matches!(
        missing_workflow,
        Err(StorageError::WorkflowRunNotFound(_))
    ));

    let own_task = tasks
        .get_task_run_for_tenant(
            &context,
            &TaskRunId::parse("task-a-child").expect("valid tenant A task ID"),
        )
        .await
        .expect("tenant A can read its task");
    assert_eq!(own_task.dependencies().len(), 1);

    let foreign_task = tasks
        .get_task_run_for_tenant(
            &context,
            &TaskRunId::parse("task-b-child").expect("valid tenant B task ID"),
        )
        .await;
    let missing_task = tasks
        .get_task_run_for_tenant(
            &context,
            &TaskRunId::parse("task-missing").expect("valid missing task ID"),
        )
        .await;
    assert!(matches!(
        foreign_task,
        Err(StorageError::TaskRunNotFound(_))
    ));
    assert!(matches!(
        missing_task,
        Err(StorageError::TaskRunNotFound(_))
    ));

    let now = std::time::SystemTime::now();
    let policy = RetryPolicy::new(
        std::num::NonZeroU32::new(1).expect("non-zero attempts"),
        0,
        0,
    )
    .expect("valid retry policy");
    let parent_run_id =
        TaskRunId::parse("tenant-created-parent-run").expect("valid parent task-run ID");
    let child_run_id =
        TaskRunId::parse("tenant-created-child-run").expect("valid child task-run ID");
    let new_tasks = vec![
        NewTaskRun::new(
            parent_run_id.clone(),
            TaskId::parse("tenant-created-parent").expect("valid parent task ID"),
            TaskInput::new(Vec::new()).expect("valid empty input"),
            Vec::new(),
            policy,
            now,
        ),
        NewTaskRun::new(
            child_run_id,
            TaskId::parse("tenant-created-child").expect("valid child task ID"),
            TaskInput::new(Vec::new()).expect("valid empty input"),
            vec![parent_run_id],
            policy,
            now,
        ),
    ];
    let created_workflow_run_id =
        WorkflowRunId::parse("tenant-a-created-workflow").expect("valid created workflow-run ID");
    let created_workflow_id =
        WorkflowId::parse("tenant-a-created-definition").expect("valid created workflow ID");
    tasks
        .create_workflow_for_tenant(
            &context,
            &created_workflow_run_id,
            &created_workflow_id,
            None,
            &new_tasks,
        )
        .await
        .expect("tenant A workflow creation is atomic");

    let created_tenant_id: String = client
        .query_one(
            "SELECT tenant_id FROM workflow_runs WHERE workflow_run_id = $1",
            &[&created_workflow_run_id.as_str()],
        )
        .await
        .expect("read created workflow tenant")
        .get(0);
    assert_eq!(created_tenant_id, "tenant-a");

    let created_task_tenants = client
        .query(
            "SELECT tenant_id FROM task_runs WHERE workflow_run_id = $1 ORDER BY task_run_id",
            &[&created_workflow_run_id.as_str()],
        )
        .await
        .expect("read created task ownership");
    assert_eq!(created_task_tenants.len(), 2);
    assert!(
        created_task_tenants
            .iter()
            .all(|row| row.get::<_, String>(0) == "tenant-a")
    );

    let created_dependency_tenant: String = client
        .query_one(
            "SELECT tenant_id FROM task_run_dependencies WHERE workflow_run_id = $1",
            &[&created_workflow_run_id.as_str()],
        )
        .await
        .expect("read created dependency ownership")
        .get(0);
    assert_eq!(created_dependency_tenant, "tenant-a");

    let created_outbox_tenant: String = client
        .query_one(
            "SELECT tenant_id FROM outbox_events
             WHERE aggregate_type = 'workflow_run' AND aggregate_id = $1
               AND event_type = 'workflow.started'",
            &[&created_workflow_run_id.as_str()],
        )
        .await
        .expect("read created workflow event ownership")
        .get(0);
    assert_eq!(created_outbox_tenant, "tenant-a");

    let own_created_workflow = tasks
        .get_workflow_run_for_tenant(&context, &created_workflow_run_id)
        .await
        .expect("tenant A can read its newly created workflow");
    assert_eq!(own_created_workflow.task_runs().len(), 2);

    let coordinator = PgWorkflowCoordinator::new(std::sync::Arc::new(tasks.clone()));
    let coordinated_run_id = WorkflowRunId::parse("tenant-a-coordinated-workflow")
        .expect("valid coordinated workflow-run ID");
    let coordinated_workflow_id = WorkflowId::parse("tenant-a-coordinated-definition")
        .expect("valid coordinated workflow ID");
    let coordinated_task_id =
        TaskId::parse("tenant-a-coordinated-task").expect("valid coordinated task ID");
    let definition = WorkflowDefinition::new(
        coordinated_workflow_id,
        vec![
            WorkflowTask::new(coordinated_task_id, Vec::new())
                .with_input(TaskInput::new(Vec::new()).expect("valid empty input")),
        ],
    )
    .expect("valid coordinated workflow definition");
    let coordinated_submission = coordinator
        .submit_workflow_for_tenant(&context, &coordinated_run_id, &definition, policy, None)
        .await
        .expect("coordinator persists tenant-owned workflow");
    assert_eq!(coordinated_submission.task_runs().count(), 1);
    let coordinated_tenant: String = client
        .query_one(
            "SELECT tenant_id FROM workflow_runs WHERE workflow_run_id = $1",
            &[&coordinated_run_id.as_str()],
        )
        .await
        .expect("read coordinator-owned workflow tenant")
        .get(0);
    assert_eq!(coordinated_tenant, "tenant-a");

    // A registered worker can claim only a tenant with a live explicit grant.
    // Make tenant B's task older so an unfiltered claim would pick it first.
    let coordinated_task_run_id = coordinated_submission
        .task_runs()
        .next()
        .expect("one coordinated task")
        .1
        .clone();
    client
        .execute(
            "UPDATE task_runs
             SET status = 'ready', available_at = clock_timestamp() - INTERVAL '2 minutes'
             WHERE tenant_id = 'tenant-b' AND task_run_id = 'task-b-child'",
            &[],
        )
        .await
        .expect("prepare an earlier ungranted tenant B task");
    client
        .execute(
            "UPDATE task_runs
             SET status = 'ready', available_at = clock_timestamp()
             WHERE tenant_id = 'tenant-a' AND task_run_id = $1",
            &[&coordinated_task_run_id.as_str()],
        )
        .await
        .expect("prepare the granted tenant A task");

    let membership = PgMembershipRepository::from_pool(pool.clone());
    let node_id = NodeId::parse("node-grant-claim-test").expect("valid claim-test node ID");
    let incarnation_id = NodeIncarnationId::parse("incarnation-grant-claim-test")
        .expect("valid claim-test incarnation ID");
    let node = membership
        .join_node(
            &node_id,
            &incarnation_id,
            std::time::Duration::from_secs(60),
        )
        .await
        .expect("join claim-test node");
    let granted_worker_id =
        WorkerId::parse("worker-grant-claim-test").expect("valid granted worker ID");
    let granted_registration = membership
        .register_worker(
            &node,
            &granted_worker_id,
            std::num::NonZeroU32::new(2).expect("non-zero concurrency"),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("register granted worker");
    client
        .execute(
            "INSERT INTO worker_tenant_grants
               (tenant_id, worker_id, status, granted_by_principal_id)
             VALUES ('tenant-a', $1, 'active', 'principal-a')",
            &[&granted_worker_id.as_str()],
        )
        .await
        .expect("grant worker access to tenant A");

    // The same logical consumer/event pair is independently claimable in
    // two tenants after migration v5 moved tenant_id into the primary key.
    let inbox = PgInboxRepository::from_pool(pool.clone());
    let tenant_a_id = TenantId::parse("tenant-a").expect("valid tenant A ID");
    let tenant_b_id = TenantId::parse("tenant-b").expect("valid tenant B ID");
    let inbox_claim_a = match inbox
        .try_claim_for_tenant(
            &tenant_a_id,
            &granted_registration,
            "shared-consumer",
            "shared-event-id",
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("tenant A inbox claim succeeds")
    {
        InboxClaimResult::Acquired(claim) => claim,
        other => panic!("expected acquired tenant A inbox claim, got {other:?}"),
    };
    assert_eq!(inbox_claim_a.tenant_id(), "tenant-a");

    let worker_b_id =
        WorkerId::parse("worker-tenant-b-inbox").expect("valid tenant B worker ID");
    let registration_b = membership
        .register_worker(
            &node,
            &worker_b_id,
            std::num::NonZeroU32::new(2).expect("non-zero concurrency"),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("register tenant B inbox worker");
    client
        .execute(
            "INSERT INTO worker_tenant_grants
               (tenant_id, worker_id, status, granted_by_principal_id)
             VALUES ('tenant-b', $1, 'active', 'principal-b')",
            &[&worker_b_id.as_str()],
        )
        .await
        .expect("grant worker access to tenant B");

    let tenant_b_outbox_event_id: i64 = client
        .query_one(
            "INSERT INTO outbox_events
               (tenant_id, aggregate_type, aggregate_id, aggregate_version, event_type, payload)
             VALUES ('tenant-b', 'workflow_run', 'workflow-b', 1,
                     'workflow.started', '{}'::jsonb)
             RETURNING event_id",
            &[],
        )
        .await
        .expect("seed tenant B outbox event")
        .get(0);

    let tenant_a_outbox_claims = tasks
        .claim_outbox_events_registered(
            &granted_registration,
            &granted_worker_id,
            64,
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("tenant A publisher can claim granted outbox events");
    assert!(!tenant_a_outbox_claims.is_empty());
    assert!(
        tenant_a_outbox_claims
            .iter()
            .all(|claim| claim.tenant_id() == "tenant-a")
    );
    assert!(
        tenant_a_outbox_claims
            .iter()
            .all(|claim| claim.event_id() != tenant_b_outbox_event_id)
    );
    let guarded_outbox_claim = tenant_a_outbox_claims
        .first()
        .expect("tenant A has at least one outbox event")
        .clone();

    let tenant_b_outbox_claims = tasks
        .claim_outbox_events_registered(
            &registration_b,
            &worker_b_id,
            64,
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("tenant B publisher can claim its own events");
    assert!(!tenant_b_outbox_claims.is_empty());
    assert!(
        tenant_b_outbox_claims
            .iter()
            .all(|claim| claim.tenant_id() == "tenant-b")
    );
    assert!(
        tenant_b_outbox_claims
            .iter()
            .any(|claim| claim.event_id() == tenant_b_outbox_event_id)
    );
    for claim in &tenant_b_outbox_claims {
        tasks
            .acknowledge_outbox_event_registered(
                claim.event_id(),
                &registration_b,
                &worker_b_id,
                claim.claim_token(),
            )
            .await
            .expect("tenant B publisher can acknowledge its events");
    }

    let inbox_claim_b = match inbox
        .try_claim_for_tenant(
            &tenant_b_id,
            &registration_b,
            "shared-consumer",
            "shared-event-id",
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("tenant B inbox claim succeeds independently")
    {
        InboxClaimResult::Acquired(claim) => claim,
        other => panic!("expected acquired tenant B inbox claim, got {other:?}"),
    };
    assert_eq!(inbox_claim_b.tenant_id(), "tenant-b");
    assert_eq!(inbox_claim_b.event_id(), inbox_claim_a.event_id());

    let duplicate_tenant_a = inbox
        .try_claim_for_tenant(
            &tenant_a_id,
            &granted_registration,
            "shared-consumer",
            "shared-event-id",
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("same-tenant duplicate claim is a normal inbox outcome");
    assert_eq!(duplicate_tenant_a, InboxClaimResult::Busy);
    let independent_inbox_rows: i64 = client
        .query_one(
            "SELECT count(*) FROM consumer_inbox
             WHERE consumer_name = 'shared-consumer' AND event_id = 'shared-event-id'",
            &[],
        )
        .await
        .expect("count tenant-specific inbox identities")
        .get(0);
    assert_eq!(independent_inbox_rows, 2);

    let granted_claim = tasks
        .claim_next_registered(
            &granted_registration,
            &AttemptId::parse("tenant-grant-claim-attempt").expect("valid grant claim attempt ID"),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("registered worker claim query succeeds")
        .expect("worker with tenant A grant can claim one task");
    assert_eq!(
        granted_claim.task_run().task_run_id(),
        &coordinated_task_run_id
    );
    assert_eq!(
        granted_claim.task_run().workflow_run_id(),
        &coordinated_run_id
    );

    // Revoking the tenant grant after claim fences every subsequent worker write.
    let claimed_task_id = granted_claim.task_run().task_run_id().clone();
    let claimed_version = granted_claim.task_run().version();
    let claimed_fencing_token = granted_claim.fencing_token();
    client
        .execute(
            "UPDATE worker_tenant_grants
             SET status = 'revoked', revoked_at = clock_timestamp()
             WHERE tenant_id = 'tenant-a' AND worker_id = $1",
            &[&granted_worker_id.as_str()],
        )
        .await
        .expect("revoke tenant A worker grant");

    let rejected_outbox_ack = tasks
        .acknowledge_outbox_event_registered(
            guarded_outbox_claim.event_id(),
            &granted_registration,
            &granted_worker_id,
            guarded_outbox_claim.claim_token(),
        )
        .await;
    assert!(matches!(
        rejected_outbox_ack,
        Err(StorageError::WorkerRegistrationLost(_))
    ));
    let rejected_outbox_retry = tasks
        .retry_outbox_event_registered(
            guarded_outbox_claim.event_id(),
            &granted_registration,
            &granted_worker_id,
            guarded_outbox_claim.claim_token(),
            std::time::Duration::from_secs(1),
            "grant revoked",
        )
        .await;
    assert!(matches!(
        rejected_outbox_retry,
        Err(StorageError::WorkerRegistrationLost(_))
    ));

    let rejected_inbox_renew = inbox
        .renew(&inbox_claim_a, std::time::Duration::from_secs(30))
        .await;
    assert!(matches!(
        rejected_inbox_renew,
        Err(StorageError::WorkerRegistrationLost(_))
    ));
    let rejected_inbox_ack = inbox.acknowledge(&inbox_claim_a).await;
    assert!(matches!(
        rejected_inbox_ack,
        Err(StorageError::WorkerRegistrationLost(_))
    ));
    let rejected_inbox_release = inbox
        .release(&inbox_claim_a, Some("grant revoked"))
        .await;
    assert!(matches!(
        rejected_inbox_release,
        Err(StorageError::WorkerRegistrationLost(_))
    ));

    let rejected_heartbeat = tasks
        .heartbeat_registered(
            &claimed_task_id,
            &granted_registration,
            claimed_version,
            claimed_fencing_token,
            std::time::Duration::from_secs(30),
        )
        .await;
    assert!(matches!(
        rejected_heartbeat,
        Err(StorageError::WorkerRegistrationLost(_))
    ));

    let rejected_failure = tasks
        .record_failure_registered(
            &claimed_task_id,
            &granted_registration,
            claimed_version,
            claimed_fencing_token,
            TaskFailure::permanent("grant_revoked", "tenant grant was revoked"),
        )
        .await;
    assert!(matches!(
        rejected_failure,
        Err(StorageError::WorkerRegistrationLost(_))
    ));

    let rejected_completion = tasks
        .complete_registered(
            &claimed_task_id,
            &granted_registration,
            claimed_version,
            claimed_fencing_token,
            TaskOutput::new(Vec::new()).expect("valid empty task output"),
        )
        .await;
    assert!(matches!(
        rejected_completion,
        Err(StorageError::WorkerRegistrationLost(_))
    ));

    let rejected_aggregation = tasks
        .aggregate_workflow_run_registered(&coordinated_run_id, &granted_registration)
        .await;
    assert!(matches!(
        rejected_aggregation,
        Err(StorageError::WorkerRegistrationLost(_))
    ));

    client
        .execute(
            "UPDATE worker_tenant_grants
             SET status = 'active', revoked_at = NULL
             WHERE tenant_id = 'tenant-a' AND worker_id = $1",
            &[&granted_worker_id.as_str()],
        )
        .await
        .expect("restore tenant A worker grant");

    for claim in &tenant_a_outbox_claims {
        tasks
            .acknowledge_outbox_event_registered(
                claim.event_id(),
                &granted_registration,
                &granted_worker_id,
                claim.claim_token(),
            )
            .await
            .expect("restored tenant A grant permits outbox acknowledgement");
    }

    inbox
        .acknowledge(&inbox_claim_a)
        .await
        .expect("restored tenant A grant permits inbox acknowledgement");
    inbox
        .acknowledge(&inbox_claim_b)
        .await
        .expect("tenant B grant independently permits inbox acknowledgement");

    let completed = tasks
        .complete_registered(
            &claimed_task_id,
            &granted_registration,
            claimed_version,
            claimed_fencing_token,
            TaskOutput::new(b"tenant-a-completed".to_vec()).expect("valid task output"),
        )
        .await
        .expect("active grant permits task completion");
    assert_eq!(completed.status(), TaskStatus::Succeeded);

    let aggregated = tasks
        .aggregate_workflow_run_registered(&coordinated_run_id, &granted_registration)
        .await
        .expect("active grant permits workflow aggregation");
    assert_eq!(aggregated, WorkflowStatus::Succeeded);

    let ungranted_worker_id =
        WorkerId::parse("worker-without-tenant-grant").expect("valid ungranted worker ID");
    let ungranted_registration = membership
        .register_worker(
            &node,
            &ungranted_worker_id,
            std::num::NonZeroU32::new(1).expect("non-zero concurrency"),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("register worker without tenant grant");
    let denied_inbox_claim = inbox
        .try_claim_for_tenant(
            &tenant_a_id,
            &ungranted_registration,
            "shared-consumer",
            "ungranted-event",
            std::time::Duration::from_secs(30),
        )
        .await;
    assert!(matches!(
        denied_inbox_claim,
        Err(StorageError::WorkerRegistrationLost(_))
    ));
    let ungranted_claim = tasks
        .claim_next_registered(
            &ungranted_registration,
            &AttemptId::parse("tenant-ungranted-claim-attempt")
                .expect("valid ungranted attempt ID"),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("ungranted claim check succeeds");
    assert!(ungranted_claim.is_none());

    let legacy_claim = tasks
        .claim_next(
            &WorkerId::parse("legacy-low-level-worker").expect("valid legacy worker ID"),
            &AttemptId::parse("legacy-low-level-claim-attempt").expect("valid legacy attempt ID"),
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("legacy low-level claim check succeeds");
    assert!(legacy_claim.is_none());

    // Tenant-scoped cancellation must update the owned task, its running
    // attempt, and its outbox event in the same tenant.
    let running_task_id =
        TaskRunId::parse("tenant-created-parent-run").expect("valid tenant-owned running task ID");
    client
        .execute(
            "UPDATE task_runs
             SET status = 'running', attempts_started = 1,
                 claimed_by = 'tenant-a-worker', fencing_token = 1,
                 lease_expires_at = clock_timestamp() + INTERVAL '1 minute',
                 started_at = clock_timestamp()
             WHERE tenant_id = 'tenant-a' AND task_run_id = $1",
            &[&running_task_id.as_str()],
        )
        .await
        .expect("prepare a tenant-owned running task");
    client
        .execute(
            "INSERT INTO task_attempts
               (attempt_id, tenant_id, task_run_id, attempt_number, worker_id,
                fencing_token, outcome)
             VALUES ('tenant-created-attempt', 'tenant-a', $1, 1,
                     'tenant-a-worker', 1, 'running')",
            &[&running_task_id.as_str()],
        )
        .await
        .expect("seed tenant-owned running attempt");

    let cancelled_task = tasks
        .cancel_task_run_for_tenant(&context, &running_task_id, 0)
        .await
        .expect("tenant A can cancel its task");
    assert_eq!(cancelled_task.status(), TaskStatus::Cancelled);
    assert_eq!(cancelled_task.version(), 1);

    let foreign_cancel = tasks
        .cancel_task_run_for_tenant(
            &context,
            &TaskRunId::parse("task-b-child").expect("valid tenant B task ID"),
            0,
        )
        .await;
    let missing_cancel = tasks
        .cancel_task_run_for_tenant(
            &context,
            &TaskRunId::parse("tenant-a-missing-task").expect("valid missing task ID"),
            0,
        )
        .await;
    assert!(matches!(
        foreign_cancel,
        Err(StorageError::TaskRunNotFound(_))
    ));
    assert!(matches!(
        missing_cancel,
        Err(StorageError::TaskRunNotFound(_))
    ));

    let attempt_outcome: String = client
        .query_one(
            "SELECT outcome FROM task_attempts
             WHERE tenant_id = 'tenant-a' AND task_run_id = $1 AND attempt_number = 1",
            &[&running_task_id.as_str()],
        )
        .await
        .expect("read cancelled attempt")
        .get(0);
    assert_eq!(attempt_outcome, "cancelled");

    let task_cancel_events: i64 = client
        .query_one(
            "SELECT count(*) FROM outbox_events
             WHERE tenant_id = 'tenant-a' AND aggregate_type = 'task_run'
               AND aggregate_id = $1 AND event_type = 'task.cancelled'",
            &[&running_task_id.as_str()],
        )
        .await
        .expect("read tenant task cancellation event")
        .get(0);
    assert_eq!(task_cancel_events, 1);

    // A tenant-owned terminal task drives atomic parent aggregation. The
    // pending sibling is cancelled and both task/workflow events stay scoped.
    let aggregate_status = tasks
        .aggregate_workflow_run_for_tenant(&context, &created_workflow_run_id)
        .await
        .expect("tenant A can aggregate its workflow");
    assert_eq!(aggregate_status, WorkflowStatus::Cancelled);

    let child_status: String = client
        .query_one(
            "SELECT status FROM task_runs
             WHERE tenant_id = 'tenant-a' AND task_run_id = 'tenant-created-child-run'",
            &[],
        )
        .await
        .expect("read sibling task after aggregation")
        .get(0);
    assert_eq!(child_status, "cancelled");

    let workflow_status: String = client
        .query_one(
            "SELECT status FROM workflow_runs
             WHERE tenant_id = 'tenant-a' AND workflow_run_id = $1",
            &[&created_workflow_run_id.as_str()],
        )
        .await
        .expect("read tenant workflow terminal state")
        .get(0);
    assert_eq!(workflow_status, "cancelled");

    let owned_terminal_events: i64 = client
        .query_one(
            "SELECT count(*) FROM outbox_events
             WHERE tenant_id = 'tenant-a'
               AND ((aggregate_type = 'task_run' AND aggregate_id = 'tenant-created-child-run'
                     AND event_type = 'task.cancelled')
                 OR (aggregate_type = 'workflow_run' AND aggregate_id = $1
                     AND event_type = 'workflow.cancelled'))",
            &[&created_workflow_run_id.as_str()],
        )
        .await
        .expect("read tenant-owned aggregation events")
        .get(0);
    assert_eq!(owned_terminal_events, 2);

    let foreign_aggregate = tasks
        .aggregate_workflow_run_for_tenant(
            &context,
            &WorkflowRunId::parse("workflow-b").expect("valid tenant B workflow ID"),
        )
        .await;
    let missing_aggregate = tasks
        .aggregate_workflow_run_for_tenant(
            &context,
            &WorkflowRunId::parse("tenant-a-missing-workflow").expect("valid missing workflow ID"),
        )
        .await;
    assert!(matches!(
        foreign_aggregate,
        Err(StorageError::WorkflowRunNotFound(_))
    ));
    assert!(matches!(
        missing_aggregate,
        Err(StorageError::WorkflowRunNotFound(_))
    ));

    let last_used: Option<std::time::SystemTime> = client
        .query_one(
            "SELECT last_used_at FROM tenant_credentials WHERE credential_id = 'credential-a'",
            &[],
        )
        .await
        .expect("read credential usage timestamp")
        .get(0);
    assert!(last_used.is_some());

    let bad_token = credentials
        .authenticate_token("AetherFlowTenantCredentialTest0123456780")
        .await;
    assert!(matches!(
        bad_token,
        Err(StorageError::InvalidTenantCredential)
    ));

    client
        .execute(
            "UPDATE tenant_credentials
             SET status = 'revoked', revoked_at = clock_timestamp()
             WHERE credential_id = 'credential-a'",
            &[],
        )
        .await
        .expect("revoke tenant credential");
    let revoked_token = credentials.authenticate_token(tenant_token).await;
    assert!(matches!(
        revoked_token,
        Err(StorageError::InvalidTenantCredential)
    ));
    client
        .execute(
            "UPDATE tenant_credentials
             SET status = 'active', revoked_at = NULL, expires_at = clock_timestamp() - INTERVAL '1 second'
             WHERE credential_id = 'credential-a'",
            &[],
        )
        .await
        .expect("expire tenant credential");
    let expired_token = credentials.authenticate_token(tenant_token).await;
    assert!(matches!(
        expired_token,
        Err(StorageError::InvalidTenantCredential)
    ));
    client
        .execute(
            "UPDATE tenant_credentials
             SET expires_at = NULL
             WHERE credential_id = 'credential-a'",
            &[],
        )
        .await
        .expect("restore tenant credential for remaining migration assertions");

    let cross_tenant_task = client
        .execute(
            "INSERT INTO task_runs (task_run_id, workflow_run_id, task_id, tenant_id, status)
             VALUES ('task-cross-tenant', 'workflow-a', 'task-cross', 'tenant-b', 'pending')",
            &[],
        )
        .await;
    assert!(
        cross_tenant_task.is_err(),
        "a task cannot join another tenant's workflow"
    );

    let cross_tenant_dependency = client
        .execute(
            "INSERT INTO task_run_dependencies
               (tenant_id, workflow_run_id, child_task_run_id, parent_task_run_id, dependency_position)
             VALUES ('tenant-b', 'workflow-a', 'task-a-child', 'task-a-another-parent', 1)",
            &[],
        )
        .await;
    assert!(
        cross_tenant_dependency.is_err(),
        "a dependency cannot use a different tenant from its task runs"
    );

    let cross_tenant_attempt = client
        .execute(
            "INSERT INTO task_attempts
               (attempt_id, tenant_id, task_run_id, attempt_number, worker_id,
                fencing_token, outcome)
             VALUES ('attempt-cross-tenant', 'tenant-b', 'task-a-parent', 1,
                     'worker-a', 1, 'running')",
            &[],
        )
        .await;
    assert!(
        cross_tenant_attempt.is_err(),
        "an attempt cannot attach to another tenant's task"
    );

    let cross_tenant_credential = client
        .execute(
            "INSERT INTO tenant_credentials
               (credential_id, tenant_id, principal_id, token_sha256, token_prefix, role, status)
             VALUES ('credential-cross-tenant', 'tenant-b', 'principal-a',
                     $1, 'btoken12', 'tenant_reader', 'active')",
            &[&"b".repeat(64)],
        )
        .await;
    assert!(
        cross_tenant_credential.is_err(),
        "a credential cannot attach to another tenant's principal"
    );

    client
        .batch_execute(
            "INSERT INTO cluster_nodes
               (node_id, incarnation_id, membership_epoch, status, lease_expires_at)
             VALUES
               ('node-tenant-test', 'incarnation-tenant-test', 1, 'active',
                clock_timestamp() + INTERVAL '1 minute');
             INSERT INTO worker_registrations
               (worker_id, node_id, membership_epoch, status, concurrency_limit, lease_expires_at)
             VALUES
               ('worker-tenant-test', 'node-tenant-test', 1, 'active', 2,
                clock_timestamp() + INTERVAL '1 minute');
             INSERT INTO worker_tenant_grants
               (tenant_id, worker_id, status, granted_by_principal_id)
             VALUES ('tenant-a', 'worker-tenant-test', 'active', 'principal-a');",
        )
        .await
        .expect("tenant-scoped worker grant is accepted");

    let explicit_ownership: bool = client
        .query_one(
            "SELECT tenant_id = 'tenant-a'
             FROM workflow_runs WHERE workflow_run_id = 'workflow-a'",
            &[],
        )
        .await
        .expect("read explicit tenant fixture")
        .get(0);
    assert!(explicit_ownership);

    let expected_checksum = initial_migration_checksum();

    // Checksum drift must fail closed.
    let invalid_checksum = if expected_checksum.starts_with('0') {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    };
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 1",
            &[&invalid_checksum],
        )
        .await
        .expect("corrupt migration checksum for negative test");
    assert!(matches!(
        store.migrate().await,
        Err(StorageError::MigrationChecksumMismatch { version: 1, .. })
    ));
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 1",
            &[&expected_checksum],
        )
        .await
        .expect("restore migration checksum");

    // The additive inbox migration has its own immutable checksum as well.
    let inbox_checksum: String = client
        .query_one(
            "SELECT checksum_sha256 FROM aetherflow_schema_migrations WHERE version = 2",
            &[],
        )
        .await
        .expect("read consumer inbox migration checksum")
        .get(0);
    let invalid_inbox_checksum = if inbox_checksum.starts_with('0') {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    };
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 2",
            &[&invalid_inbox_checksum],
        )
        .await
        .expect("corrupt inbox migration checksum for negative test");
    assert!(matches!(
        store.migrate().await,
        Err(StorageError::MigrationChecksumMismatch { version: 2, .. })
    ));
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 2",
            &[&inbox_checksum],
        )
        .await
        .expect("restore inbox migration checksum");

    // The node membership migration is independently checksummed.
    let membership_checksum: String = client
        .query_one(
            "SELECT checksum_sha256 FROM aetherflow_schema_migrations WHERE version = 3",
            &[],
        )
        .await
        .expect("read node membership migration checksum")
        .get(0);
    let invalid_membership_checksum = if membership_checksum.starts_with('0') {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    };
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 3",
            &[&invalid_membership_checksum],
        )
        .await
        .expect("corrupt node membership migration checksum for negative test");
    assert!(matches!(
        store.migrate().await,
        Err(StorageError::MigrationChecksumMismatch { version: 3, .. })
    ));
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 3",
            &[&membership_checksum],
        )
        .await
        .expect("restore node membership migration checksum");

    // The tenant-ownership migration has an independent immutable checksum.
    let tenant_checksum: String = client
        .query_one(
            "SELECT checksum_sha256 FROM aetherflow_schema_migrations WHERE version = 4",
            &[],
        )
        .await
        .expect("read tenant migration checksum")
        .get(0);
    let invalid_tenant_checksum = if tenant_checksum.starts_with('0') {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    };
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 4",
            &[&invalid_tenant_checksum],
        )
        .await
        .expect("corrupt tenant migration checksum for negative test");
    assert!(matches!(
        store.migrate().await,
        Err(StorageError::MigrationChecksumMismatch { version: 4, .. })
    ));
    client
        .execute(
            "UPDATE aetherflow_schema_migrations SET checksum_sha256 = $1 WHERE version = 4",
            &[&tenant_checksum],
        )
        .await
        .expect("restore tenant migration checksum");

    // Existing schema objects without a tracked migration row are not adopted.
    client
        .execute(
            "DELETE FROM aetherflow_schema_migrations WHERE version = 1",
            &[],
        )
        .await
        .expect("remove migration ledger row for negative test");
    assert!(matches!(
        store.migrate().await,
        Err(StorageError::UntrackedSchema)
    ));
    client
        .execute(
            "INSERT INTO aetherflow_schema_migrations \
             (version, migration_name, checksum_sha256) VALUES (1, $1, $2)",
            &[&"0001_initial_schema.sql", &expected_checksum],
        )
        .await
        .expect("restore migration ledger row");

    // A database schema newer than the binary is never downgraded or ignored.
    let future_version = 5_i64;
    let future_name = "0005_future_schema.sql";
    let future_checksum = "a".repeat(64);
    client
        .execute(
            "INSERT INTO aetherflow_schema_migrations \
             (version, migration_name, checksum_sha256) VALUES ($1, $2, $3)",
            &[&future_version, &future_name, &future_checksum],
        )
        .await
        .expect("insert future migration marker");
    assert!(matches!(
        store.migrate().await,
        Err(StorageError::NewerSchema {
            found: 5,
            supported: 4
        })
    ));
    client
        .execute(
            "DELETE FROM aetherflow_schema_migrations WHERE version = 5",
            &[],
        )
        .await
        .expect("remove future migration marker");
}

#[tokio::test]
async fn tenant_migration_backfills_existing_v1_to_v3_rows() {
    let Ok(database_url) = std::env::var("AETHERFLOW_MIGRATION_BACKFILL_TEST_DATABASE_URL") else {
        eprintln!(
            "AETHERFLOW_MIGRATION_BACKFILL_TEST_DATABASE_URL is not set; skipping legacy tenant backfill integration test"
        );
        return;
    };

    let config: Config = database_url
        .parse()
        .expect("valid PostgreSQL backfill connection URL");
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
        .expect("build PostgreSQL backfill pool");
    let client = pool.get().await.expect("get pre-v4 schema connection");

    // Build a genuine v3 database and record the immutable v1-v3 checksums.
    let migrations = [
        (
            1_i64,
            "0001_initial_schema.sql",
            include_str!("../../aetherflow-runtime/migrations/postgres/0001_initial_schema.sql"),
        ),
        (
            2_i64,
            "0002_consumer_inbox.sql",
            include_str!("../../aetherflow-runtime/migrations/postgres/0002_consumer_inbox.sql"),
        ),
        (
            3_i64,
            "0003_node_membership.sql",
            include_str!("../../aetherflow-runtime/migrations/postgres/0003_node_membership.sql"),
        ),
    ];
    for (version, name, sql) in migrations {
        client
            .batch_execute(sql)
            .await
            .unwrap_or_else(|error| panic!("apply pre-v4 migration {version}: {error}"));
        let checksum = format!("{:x}", sha2::Sha256::digest(sql.as_bytes()));
        client
            .execute(
                "INSERT INTO aetherflow_schema_migrations
                   (version, migration_name, checksum_sha256)
                 VALUES ($1, $2, $3)",
                &[&version, &name, &checksum],
            )
            .await
            .expect("record pre-v4 migration checksum");
    }

    client
        .batch_execute(
            "INSERT INTO workflow_runs (workflow_run_id, workflow_id, status)
             VALUES ('legacy-workflow', 'legacy-definition', 'pending');
             INSERT INTO task_runs
               (task_run_id, workflow_run_id, task_id, status)
             VALUES
               ('legacy-parent', 'legacy-workflow', 'legacy-parent-task', 'pending'),
               ('legacy-child', 'legacy-workflow', 'legacy-child-task', 'pending');
             INSERT INTO task_run_dependencies
               (workflow_run_id, child_task_run_id, parent_task_run_id, dependency_position)
             VALUES ('legacy-workflow', 'legacy-child', 'legacy-parent', 0);
             INSERT INTO task_attempts
               (attempt_id, task_run_id, attempt_number, worker_id, fencing_token, outcome)
             VALUES
               ('legacy-attempt', 'legacy-parent', 1, 'legacy-worker', 1, 'running');
             INSERT INTO outbox_events
               (aggregate_type, aggregate_id, aggregate_version, event_type, payload)
             VALUES
               ('workflow_run', 'legacy-workflow', 1, 'workflow.created', '{}'::jsonb);
             INSERT INTO consumer_inbox
               (consumer_name, event_id, status, attempts)
             VALUES ('legacy-consumer', 'legacy-event', 'pending', 0);",
        )
        .await
        .expect("seed rows using the exact pre-v4 table shape");

    let store = PgStore::from_pool(pool.clone());
    let outcome = store
        .migrate()
        .await
        .expect("apply tenant ownership and tenant inbox identity migrations to existing v3 schema");
    assert!(matches!(
        outcome,
        MigrationOutcome::Applied { version: 6, .. }
    ));

    let inbox_primary_key: String = client
        .query_one(
            "SELECT string_agg(column_name, ',' ORDER BY ordinal_position)
             FROM information_schema.key_column_usage
             WHERE table_schema = 'public' AND table_name = 'consumer_inbox'
               AND constraint_name = 'consumer_inbox_pkey'",
            &[],
        )
        .await
        .expect("read tenant-aware inbox primary key")
        .get(0);
    assert_eq!(inbox_primary_key, "tenant_id,consumer_name,event_id");

    for (table, expected) in [
        ("workflow_runs", 1_i64),
        ("task_runs", 2_i64),
        ("task_run_dependencies", 1_i64),
        ("task_attempts", 1_i64),
        ("outbox_events", 1_i64),
        ("consumer_inbox", 1_i64),
    ] {
        let query = format!("SELECT count(*) FROM {table} WHERE tenant_id = 'legacy-default'");
        let actual: i64 = client
            .query_one(&query, &[])
            .await
            .unwrap_or_else(|error| panic!("verify {table} legacy backfill: {error}"))
            .get(0);
        assert_eq!(actual, expected, "all old {table} rows must be backfilled");
    }
}
