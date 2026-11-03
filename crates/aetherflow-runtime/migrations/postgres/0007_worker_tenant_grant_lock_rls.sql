-- AetherFlow PostgreSQL schema, version 7.
--
-- SELECT ... FOR SHARE requires a matching UPDATE USING policy in PostgreSQL.
-- Registered workers must be able to lock their own grant while authorizing an
-- operation, but must never be able to modify the grant registry themselves.

DROP POLICY worker_tenant_grants_tenant_scope ON worker_tenant_grants;

CREATE POLICY worker_tenant_grants_tenant_scope ON worker_tenant_grants
    FOR ALL
    USING (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        AND NULLIF(current_setting('aetherflow.tenant_role', TRUE), '') <> 'worker'
    )
    WITH CHECK (
        tenant_id = NULLIF(current_setting('aetherflow.tenant_id', TRUE), '')
        AND NULLIF(current_setting('aetherflow.tenant_role', TRUE), '') <> 'worker'
    );

-- PostgreSQL adds UPDATE USING policies to SELECT ... FOR SHARE/UPDATE
-- statements. The explicit false write check allows row locking but rejects
-- every attempted mutation through a worker identity, even after the worker
-- operation has selected a tenant context.
CREATE POLICY worker_tenant_grants_worker_lock ON worker_tenant_grants
    FOR UPDATE
    USING (
        worker_id = NULLIF(current_setting('aetherflow.worker_id', TRUE), '')
        AND NULLIF(current_setting('aetherflow.tenant_role', TRUE), '') = 'worker'
    )
    WITH CHECK (FALSE);
