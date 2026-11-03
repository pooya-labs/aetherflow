# PostgreSQL schema upgrade runbook

**Current schema in this repository:** v8 (migrations 0001–0008). Re-check `MIGRATION_VERSION` in `crates/aetherflow-storage-postgres/src/lib.rs` before each release; this value changes as new migrations are added.

## Upgrade contract

The migration runner is part of `PgStore::migrate()`. It:

1. Begins one PostgreSQL transaction and obtains a transaction-scoped advisory lock.
2. Reads `aetherflow_schema_migrations` in version order.
3. Rejects untracked history, version gaps, a schema newer than the binary, migration-name changes, and SHA-256 checksum drift.
4. Applies all unapplied migrations in that transaction and records each version/name/checksum in the same transaction.
5. Commits only after the complete pending migration set succeeds.

Pending migrations from one startup are atomic as a group: a DDL or ledger-insert failure rolls back the transaction rather than leaving a partially recorded migration sequence. The lock serializes competing startup attempts. The runner fails closed; it does not silently skip or rewrite migration history.

Migrations are embedded in the binary from `crates/aetherflow-runtime/migrations/postgres/`. Applied migration files are immutable. A schema change after a migration has shipped must be a new additive/fix-forward migration with the next version; never edit a released SQL file or manually adjust the ledger to bypass a checksum error.

The server uses separate database identities:

- `AETHERFLOW_MIGRATION_DATABASE_URL`: migration-owner/DDL pool.
- `DATABASE_URL`: restricted runtime pool, which must not be superuser, bypass RLS, own protected tables, or hold other role memberships.

The API server applies migrations with the migration-owner pool before it starts listening, then verifies the runtime identity and FORCE RLS boundary. Do not point both variables to the privileged migration login.

## Pre-upgrade checklist

- [ ] Identify the exact AetherFlow commit, binary/image digest, source schema version, target schema version, PostgreSQL version, and target NATS version.
- [ ] Read every new migration in the target source tree. Identify table rewrites, long-running indexes, lock acquisition, new privileges, and incompatible application behavior.
- [ ] Run the full Rust CI and PostgreSQL/NATS integration matrix on the exact target source. Include the previous-version-to-target migration path, not just a fresh database install.
- [ ] Complete a PostgreSQL + JetStream backup set and verify checksums. For a non-disposable deployment, perform a restore rehearsal before scheduling the upgrade; see [backup and restore](backup-restore.md).
- [ ] Confirm PostgreSQL free disk space, connection capacity, NATS stream health/quorum, and backup retention.
- [ ] Confirm the migration login can perform the required DDL and the runtime login still has least privilege. Never grant the runtime role membership in the migration role.
- [ ] Choose a maintenance window. This project does not promise zero-downtime or backward-compatible rolling schema upgrades.

## Upgrade procedure

1. Freeze new workflow submissions and stop all old AetherFlow API instances, workers, outbox dispatchers, and inbox consumers. Allow handlers to drain and finish before terminating worker processes. Keep PostgreSQL and NATS available.
2. Take the coordinated pre-upgrade PostgreSQL and JetStream recovery set. Record its timestamp, code SHA, database schema version, and stream metadata.
3. Deploy the exact target binary to one API instance first. Supply the migration-owner URL, restricted runtime URL, operator token, and normal TLS trust settings. In the local-only Compose environment the explicit `DATABASE_SSL_MODE=disable` setting is permitted only for the disposable loopback database; production connections must retain TLS verification.
4. Start that one API instance. Its startup runs `PgStore::migrate()` before opening the HTTP listener and checks the runtime role/RLS boundary. Keep external traffic and workers stopped while this completes.
5. Inspect logs for the final migration outcome and any checksum, newer-schema, ownership, or RLS errors. Do not treat process liveness alone as proof of database readiness.
6. Verify the migration ledger:

~~~sql
SELECT version, migration_name, checksum_sha256
FROM aetherflow_schema_migrations
ORDER BY version;
~~~

For this repository revision, the expected versions are contiguous from 1 through 8 and the latest migration is `0008_dashboard_history_indexes.sql`. Compare names and SHA-256 checksums with the exact target source. Do not copy a checksum from an older release or infer it from the version number.

7. Verify the tenant RLS boundary. The application startup verifier is authoritative; this query is a read-only cross-check for the ten protected tables:

~~~sql
SELECT count(*) AS forced_rls_tables
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind = 'r'
  AND c.relname = ANY (ARRAY[
    'workflow_runs', 'task_runs', 'task_run_dependencies',
    'task_attempts', 'outbox_events', 'consumer_inbox',
    'tenants', 'principals', 'tenant_credentials',
    'worker_tenant_grants'
  ])
  AND c.relrowsecurity
  AND c.relforcerowsecurity;
~~~

The expected result is `10`. Investigate any mismatch; never disable FORCE RLS to make startup pass.

8. Make an authenticated read-only API request against the upgraded database and verify workflow/task/event history. `GET /health` alone proves only process liveness.
9. Start target-version workers, outbox dispatchers, and inbox consumers. Verify they register and claim work with the expected membership epoch and tenant grants. Monitor pending outbox rows, task lease recovery, JetStream stream/consumer health, and application errors.
10. Run a controlled workflow through the normal API and worker path. Verify the terminal workflow/task outcome and event delivery, then reopen submissions gradually. Deploy the same exact target binary to remaining API instances.

## Failure handling

### Migration fails before commit

The migration transaction is rolled back, including DDL and migration-ledger inserts from that startup. Keep traffic and workers stopped. Capture the startup error, verify the ledger and schema against the previous backup/release, and determine whether the failure is environmental (privileges, disk, lock contention) or a migration defect. Correct the issue in a new code change and rerun the exact-source CI matrix. Do not manually insert a ledger row or mark the migration complete.

### Migration committed but the new service does not become healthy

Do not immediately redeploy the previous binary. Once the schema has advanced, an older binary detects a newer schema and fails closed. Prefer a corrective fix-forward release that understands the new schema. If recovery requires returning to the previous schema, restore the complete pre-upgrade recovery set into an isolated environment first, validate it, then cut traffic over deliberately. Restore PostgreSQL and corresponding JetStream streams from the same coordinated recovery point; do not restore one side to a different point in time.

### Checksum mismatch, version gap, or newer schema

Stop. These are integrity/compatibility guards, not warnings to bypass. Confirm the exact deployed binary and source tree. Restore from a known-good backup into an isolated database if necessary. Never edit old migration files, delete rows from `aetherflow_schema_migrations`, or fabricate checksums.

## Migration authoring rules

- Add a new numbered migration after the current highest version; keep all shipped migration files byte-for-byte immutable.
- Update the embedded migration list and supported version in the same change.
- Keep DDL and ledger recording transaction-compatible. The current runner wraps the complete pending migration set in one transaction; do not add statements that cannot run in a transaction (for example, `CREATE INDEX CONCURRENTLY`) unless the runner is deliberately redesigned and separately tested for that mode.
- Test both fresh installation and upgrade from the previous supported schema on real PostgreSQL in CI.
- Test the restricted runtime role, FORCE RLS on every protected table, and any newly added grants/policies.
- For large or rewrite-heavy migrations, document lock duration, table rewrite/storage requirements, maintenance window, and a rehearsed recovery plan before release.

## Operational limits

This runbook describes a controlled single-version upgrade with downtime. The current project does not provide automatic downgrade migrations, a zero-downtime compatibility contract, or a production deployment orchestrator. Schema rollback means restoring the coordinated pre-upgrade PostgreSQL and JetStream recovery set, or deploying a separately designed forward-compatible repair. Preserve failed migration logs and the exact code/backup identifiers in the incident record.
