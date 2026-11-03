-- AetherFlow PostgreSQL schema, version 5.
--
-- The original v2 inbox primary key was global to (consumer_name, event_id).
-- v4 added a tenant-leading unique key but deliberately retained the old
-- primary key for compatibility. Replace it now so equal logical event IDs
-- can be deduplicated independently inside separate tenants.
--
-- No foreign keys reference the old inbox primary key. Existing v2-v4 rows
-- remain owned by legacy-default; this migration changes only key identity.

ALTER TABLE consumer_inbox
    DROP CONSTRAINT consumer_inbox_tenant_identity_uq;

ALTER TABLE consumer_inbox
    DROP CONSTRAINT consumer_inbox_pkey;

ALTER TABLE consumer_inbox
    ADD CONSTRAINT consumer_inbox_pkey
        PRIMARY KEY (tenant_id, consumer_name, event_id);
