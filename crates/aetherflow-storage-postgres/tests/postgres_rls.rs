use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use sha2::{Digest, Sha256};
use tokio_postgres::{Config, NoTls};

use aetherflow_storage_postgres::PgStore;

#[tokio::test]
async fn forced_rls_isolates_tenants_workers_and_credential_authentication() {
    let Ok(database_url) = std::env::var("AETHERFLOW_TEST_DATABASE_URL") else {
        eprintln!(
            "AETHERFLOW_TEST_DATABASE_URL is not set; skipping forced-RLS PostgreSQL integration test"
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
        .max_size(1)
        .build()
        .expect("build single-connection RLS test pool");
    PgStore::from_pool(pool.clone())
        .migrate()
        .await
        .expect("apply the forced-RLS migration");

    let process_id = std::process::id();
    let runtime_role = format!("aetherflow_rls_test_{process_id}");
    let tenant_a = format!("rls_tenant_a_{process_id}");
    let tenant_b = format!("rls_tenant_b_{process_id}");
    let principal_a = format!("rls_principal_a_{process_id}");
    let principal_b = format!("rls_principal_b_{process_id}");
    let credential_a = format!("rls_credential_a_{process_id}");
    let credential_b = format!("rls_credential_b_{process_id}");
    let worker_id = format!("rls_worker_{process_id}");
    let node_id = format!("rls_node_{process_id}");
    let workflow_a = format!("rls_workflow_a_{process_id}");
    let workflow_b = format!("rls_workflow_b_{process_id}");
    let digest_a = format!(
        "{:x}",
        Sha256::digest(format!("tenant-a-secret-{process_id}").as_bytes())
    );
    let digest_b = format!(
        "{:x}",
        Sha256::digest(format!("tenant-b-secret-{process_id}").as_bytes())
    );

    let mut client = pool.get().await.expect("get RLS fixture connection");
    let transaction = client
        .transaction()
        .await
        .expect("begin isolated RLS fixture transaction");

    let secured_tables: i64 = transaction
        .query_one(
            "SELECT count(*)::BIGINT
             FROM pg_class AS relation
             JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = 'public'
               AND relation.relname = ANY($1)
               AND relation.relrowsecurity
               AND relation.relforcerowsecurity",
            &[&[
                "workflow_runs",
                "task_runs",
                "task_run_dependencies",
                "task_attempts",
                "outbox_events",
                "consumer_inbox",
                "tenants",
                "tenant_principals",
                "tenant_credentials",
                "worker_tenant_grants",
            ][..]],
        )
        .await
        .expect("inspect forced-RLS table flags")
        .get(0);
    assert_eq!(secured_tables, 10, "all tenant-owned tables use FORCE RLS");

    transaction
        .batch_execute(&format!(
            "CREATE ROLE {runtime_role}
                NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE;
             GRANT USAGE ON SCHEMA public TO {runtime_role};
             GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {runtime_role};
             GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {runtime_role};"
        ))
        .await
        .expect("create restricted runtime test role and grant runtime DML");

    transaction
        .execute(
            "INSERT INTO tenants (tenant_id, display_name, status)
             VALUES ($1, 'RLS Tenant A', 'active'), ($2, 'RLS Tenant B', 'active')",
            &[&tenant_a, &tenant_b],
        )
        .await
        .expect("create two tenant fixtures");
    transaction
        .execute(
            "INSERT INTO tenant_principals (principal_id, tenant_id, display_name, status)
             VALUES ($1, $2, 'RLS Principal A', 'active'),
                    ($3, $4, 'RLS Principal B', 'active')",
            &[&principal_a, &tenant_a, &principal_b, &tenant_b],
        )
        .await
        .expect("create tenant principal fixtures");
    transaction
        .execute(
            "INSERT INTO tenant_credentials
               (credential_id, tenant_id, principal_id, token_sha256, token_prefix, role, status)
             VALUES ($1, $2, $3, $4, 'rlscredA', 'tenant_operator', 'active'),
                    ($5, $6, $7, $8, 'rlscredB', 'tenant_operator', 'active')",
            &[
                &credential_a,
                &tenant_a,
                &principal_a,
                &digest_a,
                &credential_b,
                &tenant_b,
                &principal_b,
                &digest_b,
            ],
        )
        .await
        .expect("create tenant credential fixtures");
    transaction
        .execute(
            "INSERT INTO workflow_runs (workflow_run_id, workflow_id, status, tenant_id)
             VALUES ($1, 'rls_definition_a', 'pending', $2),
                    ($3, 'rls_definition_b', 'pending', $4)",
            &[&workflow_a, &tenant_a, &workflow_b, &tenant_b],
        )
        .await
        .expect("create tenant workflow fixtures");
    transaction
        .execute(
            "INSERT INTO cluster_nodes
               (node_id, incarnation_id, membership_epoch, status, lease_expires_at)
             VALUES ($1, 'rls_incarnation', 1, 'active', clock_timestamp() + INTERVAL '2 minutes')",
            &[&node_id],
        )
        .await
        .expect("create cluster node fixture");
    transaction
        .execute(
            "INSERT INTO worker_registrations
               (worker_id, node_id, membership_epoch, status, concurrency_limit, lease_expires_at)
             VALUES ($1, $2, 1, 'active', 2, clock_timestamp() + INTERVAL '1 minute')",
            &[&worker_id, &node_id],
        )
        .await
        .expect("create registered worker fixture");
    transaction
        .execute(
            "INSERT INTO worker_tenant_grants
               (tenant_id, worker_id, status, granted_by_principal_id, revoked_at)
             VALUES
               ($1, $2, 'active', $3, NULL),
               ($4, $2, 'revoked', $5, clock_timestamp())",
            &[&tenant_a, &worker_id, &principal_a, &tenant_b, &principal_b],
        )
        .await
        .expect("grant worker access to tenant A and add a revoked tenant B grant");

    transaction
        .batch_execute(&format!("SET LOCAL ROLE {runtime_role}"))
        .await
        .expect("switch to restricted runtime role");

    let current_role: String = transaction
        .query_one("SELECT current_user", &[])
        .await
        .expect("read active database role")
        .get(0);
    assert_eq!(current_role, runtime_role);

    // Tenant context exposes tenant A, hides tenant B, and blocks writes that
    // try to place a row in a different tenant.
    transaction
        .query_one(
            "SELECT set_config('aetherflow.tenant_id', $1, true)",
            &[&tenant_a],
        )
        .await
        .expect("set tenant A context");
    let own_count: i64 = transaction
        .query_one("SELECT count(*) FROM workflow_runs", &[])
        .await
        .expect("read tenant-scoped workflows")
        .get(0);
    assert_eq!(own_count, 1);
    let foreign_count: i64 = transaction
        .query_one(
            "SELECT count(*) FROM workflow_runs WHERE workflow_run_id = $1",
            &[&workflow_b],
        )
        .await
        .expect("foreign workflow is filtered by RLS")
        .get(0);
    assert_eq!(foreign_count, 0);
    let foreign_update = transaction
        .execute(
            "UPDATE workflow_runs SET updated_at = clock_timestamp()
             WHERE workflow_run_id = $1",
            &[&workflow_b],
        )
        .await
        .expect("foreign update is filtered rather than disclosed");
    assert_eq!(foreign_update, 0);

    transaction
        .batch_execute("SAVEPOINT reject_cross_tenant_insert")
        .await
        .expect("create RLS rejection savepoint");
    let cross_tenant_insert = transaction
        .execute(
            "INSERT INTO workflow_runs (workflow_run_id, workflow_id, status, tenant_id)
             VALUES ('rls_forbidden_insert', 'rls_forbidden_definition', 'pending', $1)",
            &[&tenant_b],
        )
        .await;
    assert!(
        cross_tenant_insert.is_err(),
        "RLS must reject INSERT into a foreign tenant"
    );
    transaction
        .batch_execute("ROLLBACK TO SAVEPOINT reject_cross_tenant_insert")
        .await
        .expect("recover after expected RLS rejection");

    // A worker identity sees only the tenant with an active, unexpired grant.
    transaction
        .query_one(
            "SELECT
                set_config('aetherflow.tenant_id', '', true),
                set_config('aetherflow.worker_id', $1, true),
                set_config('aetherflow.tenant_role', 'worker', true)",
            &[&worker_id],
        )
        .await
        .expect("set worker identity");
    let worker_visible: i64 = transaction
        .query_one("SELECT count(*) FROM workflow_runs", &[])
        .await
        .expect("scan worker-granted workflows")
        .get(0);
    assert_eq!(worker_visible, 1);
    let own_worker_tenant: String = transaction
        .query_one(
            "SELECT tenant_id FROM workflow_runs",
            &[],
        )
        .await
        .expect("read the one granted tenant")
        .get(0);
    assert_eq!(own_worker_tenant, tenant_a);
    let locked_grant_status: String = transaction
        .query_one(
            "SELECT status FROM worker_tenant_grants
             WHERE tenant_id = $1 AND worker_id = $2
             FOR SHARE",
            &[&tenant_a, &worker_id],
        )
        .await
        .expect("worker can lock its own grant row for a registered operation")
        .get(0);
    assert_eq!(locked_grant_status, "active");
    let revoked_grant_rows: i64 = transaction
        .query_one(
            "SELECT count(*) FROM worker_tenant_grants
             WHERE tenant_id = $1 AND worker_id = $2 AND status = 'revoked'",
            &[&tenant_b, &worker_id],
        )
        .await
        .expect("worker may inspect its own revoked grant")
        .get(0);
    assert_eq!(revoked_grant_rows, 1);
    transaction
        .batch_execute("SAVEPOINT reject_worker_grant_update")
        .await
        .expect("create worker grant write rejection savepoint");
    let worker_grant_update = transaction
        .execute(
            "UPDATE worker_tenant_grants SET status = 'revoked'
             WHERE tenant_id = $1 AND worker_id = $2",
            &[&tenant_a, &worker_id],
        )
        .await;
    assert!(
        worker_grant_update.is_err(),
        "worker lock policy must not authorize grant mutation"
    );
    transaction
        .batch_execute("ROLLBACK TO SAVEPOINT reject_worker_grant_update")
        .await
        .expect("recover after expected worker grant write rejection");

    transaction
        .query_one(
            "SELECT
                set_config('aetherflow.worker_id', '', true),
                set_config('aetherflow.tenant_id', '', true)",
            &[],
        )
        .await
        .expect("clear worker identity");
    let unscoped_count: i64 = transaction
        .query_one("SELECT count(*) FROM workflow_runs", &[])
        .await
        .expect("unscoped access returns no tenant rows")
        .get(0);
    assert_eq!(unscoped_count, 0);

    // During authentication, a transaction-local SHA-256 digest exposes only
    // that credential and its principal/tenant join, including last-used write.
    transaction
        .query_one(
            "SELECT set_config('aetherflow.credential_sha256', $1, true)",
            &[&digest_a],
        )
        .await
        .expect("set credential digest");
    let authenticated_rows: i64 = transaction
        .query_one(
            "SELECT count(*)
             FROM tenant_credentials AS credential
             JOIN tenant_principals AS principal
               ON principal.tenant_id = credential.tenant_id
              AND principal.principal_id = credential.principal_id
             JOIN tenants AS tenant ON tenant.tenant_id = credential.tenant_id
             WHERE credential.status = 'active'
               AND principal.status = 'active'
               AND tenant.status = 'active'",
            &[],
        )
        .await
        .expect("credential digest filters the identity join")
        .get(0);
    assert_eq!(authenticated_rows, 1);
    let updated_credentials = transaction
        .execute(
            "UPDATE tenant_credentials
             SET last_used_at = clock_timestamp()
             WHERE token_sha256 = $1",
            &[&digest_a],
        )
        .await
        .expect("credential authentication can update its own last-used timestamp");
    assert_eq!(updated_credentials, 1);

    transaction
        .rollback()
        .await
        .expect("rollback all temporary roles and fixture rows");
}
