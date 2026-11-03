-- AetherFlow PostgreSQL schema, version 6.
--
-- Forced row-level security is the final tenant boundary for every tenant-owned
-- table. The runtime role must not own these tables, inherit their owner role,
-- be a superuser, or have BYPASSRLS. Cluster membership remains global.

DO $$
DECLARE
    protected_table TEXT;
BEGIN
    FOREACH protected_table IN ARRAY ARRAY[
        'workflow_runs',
        'task_runs',
        'task_run_dependencies',
        'task_attempts',
        'outbox_events',
        'consumer_inbox',
        'tenants',
        'tenant_principals',
        'tenant_credentials',
        'worker_tenant_grants'
    ]
    LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', protected_table);
        EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', protected_table);
    END LOOP;
END
$$;

-- Tenant-facing identity. During credential authentication the transaction
-- has only credential_sha256; after authentication it receives tenant_id.
CREATE POLICY tenants_identity_scope ON tenants
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1
            FROM tenant_credentials AS credential
            WHERE credential.tenant_id = tenants.tenant_id
              AND credential.token_sha256 =
                  NULLIF(current_setting('aetherflow.credential_sha256', TRUE), '')
        )
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
    );

CREATE POLICY tenant_principals_identity_scope ON tenant_principals
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1
            FROM tenant_credentials AS credential
            WHERE credential.tenant_id = tenant_principals.tenant_id
              AND credential.token_sha256 =
                  NULLIF(current_setting('aetherflow.credential_sha256', TRUE), '')
        )
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
    );

CREATE POLICY tenant_credentials_identity_scope ON tenant_credentials
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR token_sha256 =
            NULLIF(current_setting('aetherflow.credential_sha256', TRUE), '')
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR token_sha256 =
            NULLIF(current_setting('aetherflow.credential_sha256', TRUE), '')
    );

-- A tenant may inspect and administer its own grants. A worker may inspect
-- only its own grants; it cannot mutate the grant registry through worker GUCs.
CREATE POLICY worker_tenant_grants_tenant_scope ON worker_tenant_grants
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
    );

CREATE POLICY worker_tenant_grants_worker_read ON worker_tenant_grants
    FOR SELECT
    USING (
        worker_id = NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
    );

-- Worker-scoped queries can scan only tenants for which the registered worker
-- has a currently active, unexpired grant. TenantContext operations use the
-- tenant GUC only after the application has authenticated that tenant.
CREATE POLICY workflow_runs_tenant_scope ON workflow_runs
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1 FROM worker_tenant_grants AS grant_row
            WHERE grant_row.tenant_id = workflow_runs.tenant_id
              AND grant_row.worker_id =
                  NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
              AND grant_row.status = 'active'
              AND (grant_row.expires_at IS NULL OR grant_row.expires_at > clock_timestamp())
        )
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1 FROM worker_tenant_grants AS grant_row
            WHERE grant_row.tenant_id = workflow_runs.tenant_id
              AND grant_row.worker_id =
                  NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
              AND grant_row.status = 'active'
              AND (grant_row.expires_at IS NULL OR grant_row.expires_at > clock_timestamp())
        )
    );

CREATE POLICY task_runs_tenant_scope ON task_runs
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1 FROM worker_tenant_grants AS grant_row
            WHERE grant_row.tenant_id = task_runs.tenant_id
              AND grant_row.worker_id =
                  NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
              AND grant_row.status = 'active'
              AND (grant_row.expires_at IS NULL OR grant_row.expires_at > clock_timestamp())
        )
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1 FROM worker_tenant_grants AS grant_row
            WHERE grant_row.tenant_id = task_runs.tenant_id
              AND grant_row.worker_id =
                  NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
              AND grant_row.status = 'active'
              AND (grant_row.expires_at IS NULL OR grant_row.expires_at > clock_timestamp())
        )
    );

CREATE POLICY task_run_dependencies_tenant_scope ON task_run_dependencies
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1 FROM worker_tenant_grants AS grant_row
            WHERE grant_row.tenant_id = task_run_dependencies.tenant_id
              AND grant_row.worker_id =
                  NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
              AND grant_row.status = 'active'
              AND (grant_row.expires_at IS NULL OR grant_row.expires_at > clock_timestamp())
        )
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1 FROM worker_tenant_grants AS grant_row
            WHERE grant_row.tenant_id = task_run_dependencies.tenant_id
              AND grant_row.worker_id =
                  NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
              AND grant_row.status = 'active'
              AND (grant_row.expires_at IS NULL OR grant_row.expires_at > clock_timestamp())
        )
    );

CREATE POLICY task_attempts_tenant_scope ON task_attempts
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1 FROM worker_tenant_grants AS grant_row
            WHERE grant_row.tenant_id = task_attempts.tenant_id
              AND grant_row.worker_id =
                  NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
              AND grant_row.status = 'active'
              AND (grant_row.expires_at IS NULL OR grant_row.expires_at > clock_timestamp())
        )
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1 FROM worker_tenant_grants AS grant_row
            WHERE grant_row.tenant_id = task_attempts.tenant_id
              AND grant_row.worker_id =
                  NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
              AND grant_row.status = 'active'
              AND (grant_row.expires_at IS NULL OR grant_row.expires_at > clock_timestamp())
        )
    );

CREATE POLICY outbox_events_tenant_scope ON outbox_events
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1 FROM worker_tenant_grants AS grant_row
            WHERE grant_row.tenant_id = outbox_events.tenant_id
              AND grant_row.worker_id =
                  NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
              AND grant_row.status = 'active'
              AND (grant_row.expires_at IS NULL OR grant_row.expires_at > clock_timestamp())
        )
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1 FROM worker_tenant_grants AS grant_row
            WHERE grant_row.tenant_id = outbox_events.tenant_id
              AND grant_row.worker_id =
                  NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
              AND grant_row.status = 'active'
              AND (grant_row.expires_at IS NULL OR grant_row.expires_at > clock_timestamp())
        )
    );

CREATE POLICY consumer_inbox_tenant_scope ON consumer_inbox
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1 FROM worker_tenant_grants AS grant_row
            WHERE grant_row.tenant_id = consumer_inbox.tenant_id
              AND grant_row.worker_id =
                  NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
              AND grant_row.status = 'active'
              AND (grant_row.expires_at IS NULL OR grant_row.expires_at > clock_timestamp())
        )
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        OR EXISTS (
            SELECT 1 FROM worker_tenant_grants AS grant_row
            WHERE grant_row.tenant_id = consumer_inbox.tenant_id
              AND grant_row.worker_id =
                  NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
              AND grant_row.status = 'active'
              AND (grant_row.expires_at IS NULL OR grant_row.expires_at > clock_timestamp())
        )
    );
