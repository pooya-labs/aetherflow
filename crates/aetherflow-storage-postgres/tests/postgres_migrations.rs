use aetherflow_storage_postgres::{
    MigrationOutcome, PgStore, StorageError, initial_migration_checksum,
};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
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
        .filter(|outcome| matches!(outcome, MigrationOutcome::Applied { version: 4, .. }))
        .count();
    let already_current = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, MigrationOutcome::AlreadyCurrent { version: 4, .. }))
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
            "INSERT INTO tenants (tenant_id, display_name) VALUES
               ('tenant-a', 'Tenant A'), ('tenant-b', 'Tenant B');
             INSERT INTO tenant_principals
               (principal_id, tenant_id, display_name, status)
             VALUES ('principal-a', 'tenant-a', 'Tenant A operator', 'active');
             INSERT INTO tenant_credentials
               (credential_id, tenant_id, principal_id, token_sha256, token_prefix, role, status)
             VALUES
               ('credential-a', 'tenant-a', 'principal-a', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                'aether123', 'tenant_operator', 'active');
             INSERT INTO workflow_runs (workflow_run_id, workflow_id, tenant_id, status)
             VALUES
               ('workflow-a', 'workflow-definition-a', 'tenant-a', 'pending'),
               ('workflow-b', 'workflow-definition-b', 'tenant-b', 'pending');
             INSERT INTO task_runs (task_run_id, workflow_run_id, task_id, tenant_id, status)
             VALUES
               ('task-a-parent', 'workflow-a', 'task-parent', 'tenant-a', 'pending'),
               ('task-a-child', 'workflow-a', 'task-child', 'tenant-a', 'pending'),
               ('task-b-child', 'workflow-b', 'task-child', 'tenant-b', 'pending');
             INSERT INTO task_run_dependencies
               (tenant_id, workflow_run_id, child_task_run_id, parent_task_run_id, dependency_position)
             VALUES ('tenant-a', 'workflow-a', 'task-a-child', 'task-a-parent', 0);"
        )
        .await
        .expect("tenant-owned rows and same-tenant dependency are accepted");

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
             VALUES ('tenant-b', 'workflow-a', 'task-a-child', 'task-a-parent', 1)",
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

    let legacy_ownership_default: bool = client
        .query_one(
            "SELECT tenant_id = 'legacy-default'
             FROM workflow_runs WHERE workflow_run_id = 'workflow-a'",
            &[],
        )
        .await
        .expect("read explicit tenant fixture")
        .get(0);
    assert!(legacy_ownership_default == false);

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
