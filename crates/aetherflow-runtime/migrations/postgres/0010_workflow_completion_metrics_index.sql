-- AetherFlow PostgreSQL schema, version 10.
--
-- Recent terminal-outcome snapshots are filtered by tenant and finished_at.
-- This partial index keeps the rolling 30-minute completion query bounded as
-- the workflow history grows; existing migrations remain immutable.

CREATE INDEX workflow_runs_tenant_finished_idx
    ON workflow_runs (tenant_id, finished_at)
    WHERE finished_at IS NOT NULL;
