-- AetherFlow PostgreSQL schema, version 9.
--
-- The tenant operational-metrics endpoint reads only pending/processing inbox
-- rows and aggregates their age and count. Keep that query bounded as processed
-- history grows; the partial index contains no completed event rows.

CREATE INDEX consumer_inbox_tenant_unprocessed_idx
    ON consumer_inbox (tenant_id, received_at)
    WHERE status IN ('pending', 'processing');
