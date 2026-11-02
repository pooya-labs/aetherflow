-- AetherFlow PostgreSQL schema, version 3.
--
-- Node membership and worker registrations are database-owned leases. The
-- task_runs fencing token remains the per-task execution ownership guard.

CREATE TABLE cluster_nodes (
    node_id TEXT PRIMARY KEY
        CHECK (node_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    incarnation_id TEXT NOT NULL
        CHECK (incarnation_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    membership_epoch BIGINT NOT NULL CHECK (membership_epoch > 0),
    status TEXT NOT NULL CHECK (status IN ('active', 'draining')),
    lease_expires_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX cluster_nodes_expired_lease_idx
    ON cluster_nodes (lease_expires_at, node_id);

CREATE TABLE worker_registrations (
    worker_id TEXT PRIMARY KEY
        CHECK (worker_id ~ '^[A-Za-z0-9_-]{1,128}$'),
    node_id TEXT NOT NULL
        REFERENCES cluster_nodes (node_id) ON DELETE CASCADE,
    membership_epoch BIGINT NOT NULL CHECK (membership_epoch > 0),
    status TEXT NOT NULL CHECK (status IN ('active', 'draining')),
    concurrency_limit INTEGER NOT NULL CHECK (concurrency_limit > 0),
    lease_expires_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX worker_registrations_node_epoch_idx
    ON worker_registrations (node_id, membership_epoch, status, worker_id);

CREATE INDEX worker_registrations_expired_lease_idx
    ON worker_registrations (lease_expires_at, worker_id)
    WHERE status = 'active';
