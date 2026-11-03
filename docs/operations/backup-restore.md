# Backup and restore runbook

**Scope:** PostgreSQL state and NATS JetStream streams used by an AetherFlow deployment. This is a coordinated logical-backup and restore-drill procedure, not a claim that the current Compose stack is a production backup service.

## Recovery model

PostgreSQL is authoritative for workflow/task state, attempts, leases/fencing, the transactional outbox, and consumer inbox/deduplication state. JetStream stores messages and consumer delivery state. A recovery set must preserve both sides of that boundary.

Delivery is at-least-once. An outbox row may be published again after recovery if its PostgreSQL acknowledgement was not committed. A consumer may see a broker redelivery after recovery; its PostgreSQL inbox row suppresses a repeated logical event only when that row is present in the restored database. External handlers must still use stable event IDs as downstream idempotency keys, or make the external effect and inbox completion part of one transaction.

Do not take independent live PostgreSQL and NATS volume copies and assume that they describe one recovery point. Use a maintenance window and keep application writers stopped for the coordinated snapshot.

## Before the backup

1. Choose a maintenance window and a backup destination outside the Git checkout and outside Docker volumes.
2. Record the exact AetherFlow Git commit, deployed binary/image digest, PostgreSQL version, NATS server version, NATS CLI version, database name, and the names of every JetStream stream used by the deployment.
3. Stop new workflow submissions. Stop all AetherFlow API instances, workers, outbox dispatchers, and inbox-backed consumers. Let in-flight handlers settle before stopping the workers. Do not start publishers or consumers again until both PostgreSQL and JetStream snapshots have completed.
4. Confirm that no deployment, migration, stream reconfiguration, or other database writer is running.
5. Check that backup storage is encrypted, access-controlled, off-host, and has enough free space. The global-role dump and environment/credential files are sensitive even when role password hashes are excluded from the dump.

For the Compose development stack, load the local environment without printing it:

~~~bash
set -euo pipefail
set -a
. ./.env
set +a
umask 077

BACKUP_ROOT="${BACKUP_ROOT:-$HOME/aetherflow-backups}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_DIR="$BACKUP_ROOT/$STAMP"
mkdir -p "$BACKUP_DIR/postgres" "$BACKUP_DIR/nats"
~~~

## Back up PostgreSQL

The Compose bootstrap login (`POSTGRES_USER`) is the administrative role. It is used only for backup operations; the application itself uses the separate migration and restricted runtime roles.

~~~bash
docker compose exec -T postgres sh -ec '
  PGPASSWORD="$POSTGRES_PASSWORD" pg_dumpall \
    --host=127.0.0.1 --username="$POSTGRES_USER" \
    --globals-only --no-role-passwords
' > "$BACKUP_DIR/postgres/globals.sql"

docker compose exec -T postgres sh -ec '
  PGPASSWORD="$POSTGRES_PASSWORD" pg_dump \
    --host=127.0.0.1 --username="$POSTGRES_USER" \
    --format=custom --dbname="$POSTGRES_DB"
' > "$BACKUP_DIR/postgres/database.dump"

docker compose cp "$BACKUP_DIR/postgres/database.dump" postgres:/tmp/aetherflow-database.dump
docker compose exec -T postgres pg_restore --list /tmp/aetherflow-database.dump \
  > "$BACKUP_DIR/postgres/archive-contents.txt"
~~~

The custom archive is restored with `pg_restore`; it contains the database schema and data. The separate globals file records cluster-level roles and other global objects but deliberately omits role password hashes. Preserve the actual `.env` values or external secret-manager entries separately. Do not commit them or store them beside unencrypted backups.

For an externally managed PostgreSQL cluster, use the corresponding privileged backup account and store the globals dump separately from the per-database dump. Restore global roles into a clean recovery cluster before restoring the database, after reviewing role and tablespace names for conflicts. Reset passwords from the secret manager; the globals dump was created without password hashes. Do not blindly replay a globals dump over an existing cluster.

## Back up JetStream

Use a NATS CLI version compatible with the deployed server, and a credential that can inspect and snapshot every AetherFlow stream. The Compose file starts a three-node broker but does not create application stream definitions; stream names and subjects are deployment configuration.

List the streams, compare them with the deployment's stream/consumer manifest, and snapshot every required stream:

~~~bash
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
nats --server "$NATS_URL" stream ls
~~~

For each stream used by AetherFlow, run:

~~~bash
STREAM_NAME='REPLACE_WITH_CONFIGURED_STREAM_NAME'
nats --server "$NATS_URL" stream info "$STREAM_NAME"
nats --server "$NATS_URL" stream backup \
  "$STREAM_NAME" "$BACKUP_DIR/nats/$STREAM_NAME"
~~~

Repeat for every configured stream; do not assume that one stream contains every event category. Keep each generated backup directory intact, including its metadata files. Record the stream name, message count, last sequence, retention/storage configuration, replication setting, and matching backup directory in the recovery manifest.

JetStream's streaming backup/restore commands move stream data over the NATS network. Do not copy a live member's file-store directory as a substitute for a stream backup. See the [NATS JetStream documentation](https://docs.nats.io/learn/jetstream/) and [NATS release information](https://docs.nats.io/release-notes/).

## Seal and verify the recovery set

Record versions and the source revision in a manifest. Keep this file free of secrets:

~~~bash
{
  printf 'created_utc=%s\n' "$STAMP"
  printf 'aetherflow_git_sha=%s\n' "$(git rev-parse HEAD)"
  printf 'pg_dump='
  docker compose exec -T postgres pg_dump --version
  printf 'nats_cli='
  nats --version
  printf 'postgres_database=%s\n' "$POSTGRES_DB"
} > "$BACKUP_DIR/manifest.txt"

(
  cd "$BACKUP_DIR"
  find postgres nats -type f -print0 | sort -z | xargs -0 sha256sum
) > "$BACKUP_DIR/SHA256SUMS"

sha256sum --check "$BACKUP_DIR/SHA256SUMS"
~~~

Copy the complete recovery set to encrypted off-host storage and verify checksums again after transfer. Store the encrypted secret/configuration backup separately. A checksum detects corruption; it does not prove a restore works.

Once both snapshots and the manifest are safely stored, the maintenance window may end and services may resume. Record the snapshot time and any writes intentionally left pending in the PostgreSQL outbox.

## Restore into a clean Compose environment

Restore drills must use a fresh, isolated environment. Never test a restore over the only copy of a live database.

1. Verify backup checksums and decrypt the recovery set into a private directory.
2. Prepare a fresh Compose environment and protected `.env`. The PostgreSQL bootstrap script creates `aetherflow_migrator` and `aetherflow_runtime` on first volume initialization. For a Compose restore, do not replay `globals.sql` over those already-created roles; use credentials from the secret store.
3. Start the dependency stack:

~~~bash
docker compose up --detach --wait
~~~

4. Create a separate restore database owned by the migration role. This avoids overwriting the Compose bootstrap database:

~~~bash
RESTORE_DB="aetherflow_restore_$STAMP"
docker compose exec -T -e RESTORE_DB="$RESTORE_DB" postgres sh -ec '
  PGPASSWORD="$POSTGRES_PASSWORD" createdb \
    --host=127.0.0.1 --username="$POSTGRES_USER" \
    --owner=aetherflow_migrator "$RESTORE_DB"

  PGPASSWORD="$POSTGRES_PASSWORD" psql \
    --host=127.0.0.1 --username="$POSTGRES_USER" \
    --dbname=postgres --set ON_ERROR_STOP=1 \
    --set restore_db="$RESTORE_DB" <<SQL
GRANT CONNECT ON DATABASE :"restore_db" TO aetherflow_runtime;
SQL
'
~~~

5. Copy the archive into the container and restore it as the migration owner. The archive is restored without replaying old ownership/ACL statements; the next block reapplies the intended restricted runtime grants:

~~~bash
docker compose cp "$BACKUP_DIR/postgres/database.dump" postgres:/tmp/aetherflow-restore.dump

docker compose exec -T -e RESTORE_DB="$RESTORE_DB" postgres sh -ec '
  PGPASSWORD="$AETHERFLOW_MIGRATION_PASSWORD" pg_restore \
    --host=127.0.0.1 --username=aetherflow_migrator \
    --dbname="$RESTORE_DB" --exit-on-error --single-transaction \
    --no-owner --no-acl /tmp/aetherflow-restore.dump

  PGPASSWORD="$AETHERFLOW_MIGRATION_PASSWORD" psql \
    --host=127.0.0.1 --username=aetherflow_migrator \
    --dbname="$RESTORE_DB" --set ON_ERROR_STOP=1 <<SQL
GRANT USAGE ON SCHEMA public TO aetherflow_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO aetherflow_runtime;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO aetherflow_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE aetherflow_migrator IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO aetherflow_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE aetherflow_migrator IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO aetherflow_runtime;
SQL
'
~~~

6. Restore every JetStream stream into a clean recovery cluster, using the snapshot directory produced for that stream:

~~~bash
nats --server "$NATS_URL" stream restore "$BACKUP_DIR/nats/REPLACE_WITH_STREAM_NAME"
~~~

Repeat for every stream in the recovery manifest. A stream with the same name already present on the target is a conflict: do not purge or delete it casually. Use a clean recovery cluster or have an operator review the target before replacing anything.

7. Point both database URLs at the restored database and run the target AetherFlow binary. For the local Compose stack only:

~~~bash
export DATABASE_SSL_MODE=disable
export AETHERFLOW_MIGRATION_DATABASE_URL="postgres://aetherflow_migrator:${AETHERFLOW_MIGRATION_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/$RESTORE_DB"
export DATABASE_URL="postgres://aetherflow_runtime:${AETHERFLOW_RUNTIME_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/$RESTORE_DB"
export AETHERFLOW_API_TOKEN="$(openssl rand -hex 32)"
cargo run -p aetherflow-server
~~~

`DATABASE_SSL_MODE=disable` is only for this disposable loopback Compose database. Do not use it for a network or production database. Production connections must retain TLS verification.

## Restore validation and re-opening traffic

Before restarting workers or accepting new submissions:

- Check `aetherflow_schema_migrations` and compare all recorded versions, names, and checksums with the source release. The current repository migration set is v1–v8.
- Confirm all ten tenant-owned tables still have both row-level security enabled and forced; start the server with the restricted runtime URL so its startup verifier checks the runtime role and RLS boundary.
- Confirm workflow/task history and tenant ownership using an authenticated read-only API request. `GET /health` is process liveness only and is not a database readiness check.
- Compare each restored JetStream stream's message count/last sequence and consumer state with the recovery manifest. Confirm expected streams, subjects, retention, and replication settings.
- Inspect pending PostgreSQL outbox rows and consumer inbox state. Pending outbox events may be published after restart; this is expected and must remain safe under event-ID deduplication. Do not manually mark rows published or truncate the inbox/outbox to make counts look clean.
- Run a controlled test workflow through the normal API/worker path, then verify its terminal workflow/task state and event delivery.
- Start consumers and workers before gradually reopening workflow submissions. Watch outbox backlog, lease recovery, repeated delivery, database errors, and JetStream quorum/health.

Keep the original recovery set and the pre-restore environment until the restore has been validated and signed off.

## Production backup policy boundary

The Compose procedure above is a logical backup/restore and a recovery drill. PostgreSQL documents three separate approaches: SQL dumps, file-system-level backups, and continuous archiving. `pg_dump` creates a consistent logical export of one database; it does not provide WAL replay or point-in-time recovery. A production deployment with a meaningful RPO should use a designed physical base-backup plus WAL-archiving/PITR process (or a managed PostgreSQL service that provides equivalent guarantees), and test that process independently.

Choose and document explicit RPO/RTO objectives with the service owner. This repository does not implement a production backup scheduler, WAL archive, or PITR service.

References: [PostgreSQL 18 Backup and Restore](https://www.postgresql.org/docs/18/backup.html), [pg_dump](https://www.postgresql.org/docs/18/app-pgdump.html), [continuous archiving and PITR](https://www.postgresql.org/docs/18/continuous-archiving.html).
