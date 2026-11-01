-- AetherFlow PostgreSQL schema, version 1.
--
-- Apply this file as one transaction. The migration runner records the
-- version, name, and SHA-256 checksum in aetherflow_schema_migrations in the
-- same transaction after all statements succeed.
--
-- IDs use the ASCII token syntax enforced by the domain EntityId type.
-- Persist absolute timestamptz deadlines; never persist process-local
-- monotonic clock ticks as durable deadlines.

CREATE TABLE aetherflow_schema_migrations (
    version BIGINT PRIMARY KEY CHECK (version > 0),
    migration_name TEXT NOT NULL UNIQUE,
    checksum_sha256 CHAR(64) NOT NULL
        CHECK (checksum_sha256 ~ '^[0-9a-f]{64}$'),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE workflow_runs (
    workflow_run_id TEXT PRIMARY KEY
        CHECK (workflow_run_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    workflow_id TEXT NOT NULL
        CHECK (workflow_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    status TEXT NOT NULL
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'cancelled', 'timed_out')),
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    idempotency_key TEXT
        CHECK (idempotency_key IS NULL OR idempotency_key ~ '^[A-Za-z0-9_-]{1,128}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    CONSTRAINT workflow_runs_terminal_timestamp_check
        CHECK (
            (status IN ('succeeded', 'failed', 'cancelled', 'timed_out'))
            = (finished_at IS NOT NULL)
        )
);

CREATE UNIQUE INDEX workflow_runs_idempotency_key_uq
    ON workflow_runs (workflow_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX workflow_runs_status_created_idx
    ON workflow_runs (status, created_at, workflow_run_id);

CREATE TABLE task_runs (
    task_run_id TEXT PRIMARY KEY
        CHECK (task_run_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    workflow_run_id TEXT NOT NULL
        REFERENCES workflow_runs (workflow_run_id) ON DELETE RESTRICT,
    task_id TEXT NOT NULL
        CHECK (task_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    status TEXT NOT NULL
        CHECK (
            status IN (
                'pending', 'ready', 'running', 'retry_waiting',
                'succeeded', 'failed', 'cancelled', 'timed_out'
            )
        ),
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    attempts_started INTEGER NOT NULL DEFAULT 0 CHECK (attempts_started >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 1 CHECK (max_attempts > 0),
    initial_backoff_ms BIGINT NOT NULL DEFAULT 0 CHECK (initial_backoff_ms >= 0),
    max_backoff_ms BIGINT NOT NULL DEFAULT 0 CHECK (max_backoff_ms >= initial_backoff_ms),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    input_payload BYTEA NOT NULL DEFAULT decode('', 'hex')
        CHECK (octet_length(input_payload) <= 1048576),
    output_payload BYTEA
        CHECK (output_payload IS NULL OR octet_length(output_payload) <= 1048576),
    claimed_by TEXT
        CHECK (claimed_by IS NULL OR claimed_by ~ '^[A-Za-z0-9_-]{1,128}$'),
    fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    lease_expires_at TIMESTAMPTZ,
    last_error_kind TEXT,
    last_error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    CONSTRAINT task_runs_workflow_task_uq UNIQUE (workflow_run_id, task_id),
    CONSTRAINT task_runs_workflow_id_uq UNIQUE (workflow_run_id, task_run_id),
    CONSTRAINT task_runs_attempt_budget_check CHECK (attempts_started <= max_attempts),
    CONSTRAINT task_runs_running_lease_check
        CHECK ((status = 'running') = (claimed_by IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CONSTRAINT task_runs_started_timestamp_check
        CHECK (attempts_started = 0 OR started_at IS NOT NULL),
    CONSTRAINT task_runs_output_status_check
        CHECK ((status = 'succeeded') = (output_payload IS NOT NULL)),
    CONSTRAINT task_runs_terminal_timestamp_check
        CHECK (
            (status IN ('succeeded', 'failed', 'cancelled', 'timed_out'))
            = (finished_at IS NOT NULL)
        )
);

CREATE INDEX task_runs_ready_claim_idx
    ON task_runs (available_at, created_at, task_run_id)
    WHERE status = 'ready';

CREATE INDEX task_runs_expired_lease_idx
    ON task_runs (lease_expires_at, task_run_id)
    WHERE status = 'running';

CREATE INDEX task_runs_workflow_status_idx
    ON task_runs (workflow_run_id, status, task_id);

CREATE TABLE task_run_dependencies (
    workflow_run_id TEXT NOT NULL,
    child_task_run_id TEXT NOT NULL,
    parent_task_run_id TEXT NOT NULL,
    dependency_position INTEGER NOT NULL CHECK (dependency_position >= 0),
    PRIMARY KEY (workflow_run_id, child_task_run_id, dependency_position),
    UNIQUE (workflow_run_id, child_task_run_id, parent_task_run_id),
    CONSTRAINT task_run_dependencies_not_self_check
        CHECK (child_task_run_id <> parent_task_run_id),
    CONSTRAINT task_run_dependencies_child_fk
        FOREIGN KEY (workflow_run_id, child_task_run_id)
        REFERENCES task_runs (workflow_run_id, task_run_id) ON DELETE RESTRICT,
    CONSTRAINT task_run_dependencies_parent_fk
        FOREIGN KEY (workflow_run_id, parent_task_run_id)
        REFERENCES task_runs (workflow_run_id, task_run_id) ON DELETE RESTRICT
);

CREATE INDEX task_run_dependencies_parent_idx
    ON task_run_dependencies (workflow_run_id, parent_task_run_id);

CREATE TABLE task_attempts (
    attempt_id TEXT PRIMARY KEY
        CHECK (attempt_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    task_run_id TEXT NOT NULL
        REFERENCES task_runs (task_run_id) ON DELETE RESTRICT,
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    worker_id TEXT NOT NULL
        CHECK (worker_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
    outcome TEXT NOT NULL
        CHECK (
            outcome IN (
                'running', 'succeeded', 'retryable_failure', 'permanent_failure',
                'timed_out', 'cancelled', 'lease_expired'
            )
        ),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    error_kind TEXT,
    error_message TEXT,
    UNIQUE (task_run_id, attempt_number),
    CONSTRAINT task_attempts_finished_check
        CHECK ((outcome = 'running') = (finished_at IS NULL))
);

CREATE UNIQUE INDEX task_attempts_one_running_uq
    ON task_attempts (task_run_id)
    WHERE outcome = 'running';

CREATE INDEX task_attempts_history_idx
    ON task_attempts (task_run_id, attempt_number DESC);

CREATE TABLE outbox_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_type TEXT NOT NULL
        CHECK (aggregate_type IN ('workflow_run', 'task_run')),
    aggregate_id TEXT NOT NULL
        CHECK (aggregate_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    claim_owner TEXT
        CHECK (claim_owner IS NULL OR claim_owner ~ '^[A-Za-z0-9_-]{1,128}$'),
    claim_token BIGINT NOT NULL DEFAULT 0 CHECK (claim_token >= 0),
    claim_expires_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    last_error TEXT,
    UNIQUE (aggregate_type, aggregate_id, aggregate_version, event_type),
    CONSTRAINT outbox_claim_pair_check
        CHECK ((claim_owner IS NULL) = (claim_expires_at IS NULL)),
    CONSTRAINT outbox_published_unclaimed_check
        CHECK (published_at IS NULL OR claim_owner IS NULL)
);

CREATE INDEX outbox_events_pending_idx
    ON outbox_events (available_at, event_id)
    WHERE published_at IS NULL;

CREATE INDEX outbox_events_expired_claim_idx
    ON outbox_events (claim_expires_at, event_id)
    WHERE published_at IS NULL AND claim_owner IS NOT NULL;
