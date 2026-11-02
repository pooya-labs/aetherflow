-- AetherFlow PostgreSQL schema, version 2.
--
-- A durable inbox ledger for consumer-side deduplication. The broker remains
-- responsible for retaining and redelivering message payloads; this table
-- records ownership and completion by stable event identity.

CREATE TABLE consumer_inbox (
    consumer_name TEXT NOT NULL
        CHECK (consumer_name ~ '^[A-Za-z0-9_-]{1,128}$'),
    event_id TEXT NOT NULL
        CHECK (event_id ~ '^[!-~]{1,255}$'),
    status TEXT NOT NULL
        CHECK (status IN ('pending', 'processing', 'processed')),
    attempts BIGINT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    claim_owner TEXT
        CHECK (claim_owner IS NULL OR claim_owner ~ '^[A-Za-z0-9_-]{1,128}$'),
    claim_token BIGINT NOT NULL DEFAULT 0 CHECK (claim_token >= 0),
    lease_expires_at TIMESTAMPTZ,
    last_error TEXT
        CHECK (last_error IS NULL OR octet_length(last_error) <= 4096),
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    processed_at TIMESTAMPTZ,
    PRIMARY KEY (consumer_name, event_id),
    CONSTRAINT consumer_inbox_lease_state_check
        CHECK (
            (
                status = 'processing'
                AND claim_owner IS NOT NULL
                AND lease_expires_at IS NOT NULL
                AND processed_at IS NULL
            )
            OR (
                status = 'pending'
                AND claim_owner IS NULL
                AND lease_expires_at IS NULL
                AND processed_at IS NULL
            )
            OR (
                status = 'processed'
                AND claim_owner IS NULL
                AND lease_expires_at IS NULL
                AND processed_at IS NOT NULL
            )
        )
);

CREATE INDEX consumer_inbox_expired_lease_idx
    ON consumer_inbox (lease_expires_at, consumer_name, event_id)
    WHERE status = 'processing';

CREATE INDEX consumer_inbox_processed_retention_idx
    ON consumer_inbox (processed_at, consumer_name, event_id)
    WHERE status = 'processed';
