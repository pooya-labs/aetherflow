-- AetherFlow PostgreSQL schema, version 8.
--
-- Read-only dashboard queries use stable keyset pagination. These indexes
-- keep tenant filtering and descending timeline scans bounded as event history
-- grows; all existing migrations remain immutable.

CREATE INDEX workflow_runs_tenant_created_idx
    ON workflow_runs (tenant_id, created_at DESC, workflow_run_id DESC);

CREATE INDEX outbox_events_tenant_history_idx
    ON outbox_events (tenant_id, event_id DESC);
