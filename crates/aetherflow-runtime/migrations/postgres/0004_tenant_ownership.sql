-- AetherFlow PostgreSQL schema, version 4.
--
-- Tenant ownership is introduced without changing the globally unique resource
-- identifiers or rewriting migrations v1-v3. Existing rows receive the
-- reserved legacy tenant through the NOT NULL DEFAULT backfill.
--
-- Compatibility boundary: this migration does not enable RLS or authorize
-- tenant requests. Existing repositories continue to create/use legacy-default
-- until every API, worker, outbox and inbox operation has a TenantContext.
-- Do not provision customer-owned records or expose the listener externally
-- until transaction-local tenant context and forced RLS are deployed.

CREATE TABLE tenants (
    tenant_id TEXT PRIMARY KEY
        CHECK (tenant_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    display_name TEXT NOT NULL
        CHECK (length(display_name) BETWEEN 1 AND 128),
    status TEXT NOT NULL CHECK (status IN ('active', 'suspended')),
    is_system BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT tenants_legacy_identity_check
        CHECK ((tenant_id = 'legacy-default') = is_system)
);

INSERT INTO tenants (tenant_id, display_name, status, is_system)
VALUES ('legacy-default', 'Legacy data (pre-tenant)', 'active', TRUE);

CREATE TABLE tenant_principals (
    principal_id TEXT PRIMARY KEY
        CHECK (principal_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    tenant_id TEXT NOT NULL
        REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    display_name TEXT NOT NULL
        CHECK (length(display_name) BETWEEN 1 AND 128),
    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT tenant_principals_tenant_identity_uq
        UNIQUE (tenant_id, principal_id)
);

CREATE TABLE tenant_credentials (
    credential_id TEXT PRIMARY KEY
        CHECK (credential_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    tenant_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    token_sha256 CHAR(64) NOT NULL
        CHECK (token_sha256 ~ '^[0-9a-f]{64}$'),
    token_prefix TEXT NOT NULL
        CHECK (token_prefix ~ '^[A-Za-z0-9_-]{8,24}$'),
    role TEXT NOT NULL
        CHECK (role IN ('tenant_admin', 'tenant_operator', 'tenant_reader')),
    status TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT tenant_credentials_principal_fk
        FOREIGN KEY (tenant_id, principal_id)
        REFERENCES tenant_principals (tenant_id, principal_id) ON DELETE RESTRICT,
    CONSTRAINT tenant_credentials_revocation_check
        CHECK ((status = 'revoked') = (revoked_at IS NOT NULL))
);

CREATE UNIQUE INDEX tenant_credentials_token_sha256_uq
    ON tenant_credentials (token_sha256);

CREATE INDEX tenant_principals_tenant_status_idx
    ON tenant_principals (tenant_id, status, principal_id);

CREATE INDEX tenant_credentials_principal_status_idx
    ON tenant_credentials (tenant_id, principal_id, status);

-- The defaults backfill every pre-v4 row and preserve existing writers during
-- the staged repository migration. Composite foreign keys prevent a row from
-- mixing ownership identities even while the runtime still uses one tenant.

ALTER TABLE workflow_runs
    ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'legacy-default';
ALTER TABLE workflow_runs
    ADD CONSTRAINT workflow_runs_tenant_fk
        FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE RESTRICT;
ALTER TABLE workflow_runs
    ADD CONSTRAINT workflow_runs_tenant_run_uq UNIQUE (tenant_id, workflow_run_id);

CREATE INDEX workflow_runs_tenant_status_created_idx
    ON workflow_runs (tenant_id, status, created_at, workflow_run_id);

ALTER TABLE task_runs
    ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'legacy-default';
ALTER TABLE task_runs
    ADD CONSTRAINT task_runs_tenant_fk
        FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE RESTRICT;
ALTER TABLE task_runs
    ADD CONSTRAINT task_runs_tenant_task_uq UNIQUE (tenant_id, task_run_id);
ALTER TABLE task_runs
    ADD CONSTRAINT task_runs_tenant_workflow_task_uq
        UNIQUE (tenant_id, workflow_run_id, task_run_id);
ALTER TABLE task_runs
    ADD CONSTRAINT task_runs_tenant_workflow_fk
        FOREIGN KEY (tenant_id, workflow_run_id)
        REFERENCES workflow_runs (tenant_id, workflow_run_id) ON DELETE RESTRICT;

CREATE INDEX task_runs_tenant_ready_claim_idx
    ON task_runs (tenant_id, available_at, created_at, task_run_id)
    WHERE status = 'ready';

CREATE INDEX task_runs_tenant_expired_lease_idx
    ON task_runs (tenant_id, lease_expires_at, task_run_id)
    WHERE status = 'running';

ALTER TABLE task_run_dependencies
    ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'legacy-default';
ALTER TABLE task_run_dependencies
    ADD CONSTRAINT task_run_dependencies_tenant_fk
        FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE RESTRICT;
ALTER TABLE task_run_dependencies
    ADD CONSTRAINT task_run_dependencies_tenant_child_fk
        FOREIGN KEY (tenant_id, workflow_run_id, child_task_run_id)
        REFERENCES task_runs (tenant_id, workflow_run_id, task_run_id) ON DELETE RESTRICT;
ALTER TABLE task_run_dependencies
    ADD CONSTRAINT task_run_dependencies_tenant_parent_fk
        FOREIGN KEY (tenant_id, workflow_run_id, parent_task_run_id)
        REFERENCES task_runs (tenant_id, workflow_run_id, task_run_id) ON DELETE RESTRICT;

CREATE INDEX task_run_dependencies_tenant_parent_idx
    ON task_run_dependencies (tenant_id, workflow_run_id, parent_task_run_id);

ALTER TABLE task_attempts
    ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'legacy-default';
ALTER TABLE task_attempts
    ADD CONSTRAINT task_attempts_tenant_fk
        FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE RESTRICT;
ALTER TABLE task_attempts
    ADD CONSTRAINT task_attempts_tenant_task_fk
        FOREIGN KEY (tenant_id, task_run_id)
        REFERENCES task_runs (tenant_id, task_run_id) ON DELETE RESTRICT;
ALTER TABLE task_attempts
    ADD CONSTRAINT task_attempts_tenant_number_uq
        UNIQUE (tenant_id, task_run_id, attempt_number);

CREATE INDEX task_attempts_tenant_history_idx
    ON task_attempts (tenant_id, task_run_id, attempt_number DESC);

ALTER TABLE outbox_events
    ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'legacy-default';
ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_events_tenant_fk
        FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE RESTRICT;
ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_events_tenant_aggregate_uq
        UNIQUE (tenant_id, aggregate_type, aggregate_id, aggregate_version, event_type);

CREATE INDEX outbox_events_tenant_pending_idx
    ON outbox_events (tenant_id, available_at, event_id)
    WHERE published_at IS NULL;

CREATE INDEX outbox_events_tenant_expired_claim_idx
    ON outbox_events (tenant_id, claim_expires_at, event_id)
    WHERE published_at IS NULL AND claim_owner IS NOT NULL;

ALTER TABLE consumer_inbox
    ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'legacy-default';
ALTER TABLE consumer_inbox
    ADD CONSTRAINT consumer_inbox_tenant_fk
        FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE RESTRICT;
ALTER TABLE consumer_inbox
    ADD CONSTRAINT consumer_inbox_tenant_identity_uq
        UNIQUE (tenant_id, consumer_name, event_id);

CREATE INDEX consumer_inbox_tenant_expired_lease_idx
    ON consumer_inbox (tenant_id, lease_expires_at, consumer_name, event_id)
    WHERE status = 'processing';

CREATE INDEX consumer_inbox_tenant_processed_retention_idx
    ON consumer_inbox (tenant_id, processed_at, consumer_name, event_id)
    WHERE status = 'processed';

-- Node membership remains cluster-scoped. A worker receives tenant access only
-- through an explicit grant; the worker runtime starts enforcing these rows in
-- a later migration of its repository contract.

CREATE TABLE worker_tenant_grants (
    tenant_id TEXT NOT NULL
        REFERENCES tenants (tenant_id) ON DELETE RESTRICT,
    worker_id TEXT NOT NULL
        REFERENCES worker_registrations (worker_id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
    granted_by_principal_id TEXT,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, worker_id),
    CONSTRAINT worker_tenant_grants_principal_fk
        FOREIGN KEY (tenant_id, granted_by_principal_id)
        REFERENCES tenant_principals (tenant_id, principal_id) ON DELETE RESTRICT,
    CONSTRAINT worker_tenant_grants_revocation_check
        CHECK ((status = 'revoked') = (revoked_at IS NOT NULL)),
    CONSTRAINT worker_tenant_grants_expiry_check
        CHECK (expires_at IS NULL OR expires_at > granted_at)
);

CREATE INDEX worker_tenant_grants_worker_status_idx
    ON worker_tenant_grants (worker_id, status, tenant_id);
