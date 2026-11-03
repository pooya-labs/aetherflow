#!/usr/bin/env bash
set -Eeuo pipefail

readonly stream_name="AETHERFLOW_RESTORE_DRILL"
readonly stream_subject="aetherflow.restore.drill"
readonly restore_database="aetherflow_restore_drill"
readonly work_dir="$(mktemp -d)"
readonly nats_restore_container="aetherflow-nats-restore-drill"

cleanup() {
  dropdb --if-exists --host "$PGHOST" --port "$PGPORT" --username "$PGUSER" "$restore_database" >/dev/null 2>&1 || true
  docker rm --force "$nats_restore_container" >/dev/null 2>&1 || true
  rm -rf "$work_dir"
}
trap cleanup EXIT

psql_source() {
  psql --no-psqlrc --set ON_ERROR_STOP=1 --tuples-only --no-align \
    --host "$PGHOST" --port "$PGPORT" --username "$PGUSER" --dbname "$PGDATABASE" "$@"
}

psql_restore() {
  psql --no-psqlrc --set ON_ERROR_STOP=1 --tuples-only --no-align \
    --host "$PGHOST" --port "$PGPORT" --username "$PGUSER" --dbname "$restore_database" "$@"
}

echo "Checking the source PostgreSQL schema before backup..."
source_migrations="$(psql_source --command "SELECT count(*) || ':' || max(version) FROM aetherflow_schema_migrations")"
if [[ "$source_migrations" != "8:8" ]]; then
  printf 'Expected source schema migration ledger 8:8, got %s.\n' "$source_migrations" >&2
  exit 1
fi

source_rls="$(psql_source --command "
  SELECT count(*)
  FROM pg_class AS relation
  JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
  WHERE namespace.nspname = 'public'
    AND relation.relkind = 'r'
    AND relation.relname IN (
      'workflow_runs', 'task_runs', 'task_run_dependencies',
      'task_attempts', 'outbox_events', 'consumer_inbox',
      'tenants', 'tenant_principals', 'tenant_credentials', 'worker_tenant_grants'
    )
    AND relation.relrowsecurity
    AND relation.relforcerowsecurity
")"
if [[ "$source_rls" != "10" ]]; then
  printf 'Expected ten source FORCE-RLS tables, got %s.\n' "$source_rls" >&2
  exit 1
fi

echo "Backing up PostgreSQL database..."
mkdir -p "$work_dir/postgres" "$work_dir/nats"
pg_dump --host "$PGHOST" --port "$PGPORT" --username "$PGUSER" \
  --dbname "$PGDATABASE" --format=custom --file "$work_dir/postgres/database.dump"
pg_restore --list "$work_dir/postgres/database.dump" > "$work_dir/postgres/archive-contents.txt"
sha256sum "$work_dir/postgres/database.dump" > "$work_dir/postgres/SHA256SUMS"
sha256sum --check "$work_dir/postgres/SHA256SUMS"

echo "Restoring PostgreSQL into a fresh isolated database..."
createdb --host "$PGHOST" --port "$PGPORT" --username "$PGUSER" "$restore_database"
pg_restore --exit-on-error --no-owner --no-acl \
  --host "$PGHOST" --port "$PGPORT" --username "$PGUSER" \
  --dbname "$restore_database" "$work_dir/postgres/database.dump"

restored_migrations="$(psql_restore --command "SELECT count(*) || ':' || max(version) FROM aetherflow_schema_migrations")"
if [[ "$restored_migrations" != "$source_migrations" ]]; then
  printf 'Restored migration ledger mismatch: expected %s, got %s.\n' "$source_migrations" "$restored_migrations" >&2
  exit 1
fi

restored_rls="$(psql_restore --command "
  SELECT count(*)
  FROM pg_class AS relation
  JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
  WHERE namespace.nspname = 'public'
    AND relation.relkind = 'r'
    AND relation.relname IN (
      'workflow_runs', 'task_runs', 'task_run_dependencies',
      'task_attempts', 'outbox_events', 'consumer_inbox',
      'tenants', 'tenant_principals', 'tenant_credentials', 'worker_tenant_grants'
    )
    AND relation.relrowsecurity
    AND relation.relforcerowsecurity
")"
if [[ "$restored_rls" != "$source_rls" ]]; then
  printf 'Restored FORCE-RLS table count mismatch: expected %s, got %s.\n' "$source_rls" "$restored_rls" >&2
  exit 1
fi

restored_policy_count="$(psql_restore --command "
  SELECT count(*)
  FROM pg_policies
  WHERE schemaname = 'public'
    AND tablename IN (
      'workflow_runs', 'task_runs', 'task_run_dependencies',
      'task_attempts', 'outbox_events', 'consumer_inbox',
      'tenants', 'tenant_principals', 'tenant_credentials', 'worker_tenant_grants'
    )
")"
if [[ "$restored_policy_count" != "12" ]]; then
  printf 'Expected twelve restored tenant RLS policies, got %s.\n' "$restored_policy_count" >&2
  exit 1
fi

echo "Creating a three-replica JetStream stream and publishing retained messages..."
nats --server "$NATS_CLUSTER_URL" stream add "$stream_name" \
  --subjects "$stream_subject" --storage file --replicas 3 --defaults >/dev/null

for sequence in 1 2 3; do
  nats --server "$NATS_CLUSTER_URL" pub "$stream_subject" "aetherflow-restore-message-$sequence" >/dev/null
done

source_stream_info="$(nats --server "$NATS_CLUSTER_URL" stream info "$stream_name" --json)"
source_message_count="$(jq -er '.state.messages' <<<"$source_stream_info")"
if [[ "$source_message_count" != "3" ]]; then
  printf 'Expected three source JetStream messages, got %s.\n' "$source_message_count" >&2
  exit 1
fi

echo "Backing up the source JetStream stream with the pinned NATS CLI..."
nats --server "$NATS_CLUSTER_URL" stream backup "$stream_name" \
  "$work_dir/nats/$stream_name" --progress=false
test -s "$work_dir/nats/$stream_name/backup.json"

echo "Starting a separate JetStream restore target..."
docker run --detach --name "$nats_restore_container" \
  --publish 127.0.0.1:4225:4222 \
  --publish 127.0.0.1:8225:8222 \
  nats:2.12-alpine \
  -js -sd /data -m 8222 -name aetherflow-nats-restore-drill >/dev/null

target_ready=false
for attempt in $(seq 1 60); do
  if nats --server "$NATS_RESTORE_URL" stream ls >/dev/null 2>&1; then
    target_ready=true
    break
  fi
  sleep 1
done
if [[ "$target_ready" != "true" ]]; then
  docker logs "$nats_restore_container" >&2
  echo "The isolated NATS restore target did not become ready." >&2
  exit 1
fi

echo "Restoring the stream into the isolated server with a single replica..."
nats --server "$NATS_RESTORE_URL" stream restore \
  "$work_dir/nats/$stream_name" --progress=false --replicas 1

restored_stream_info="$(nats --server "$NATS_RESTORE_URL" stream info "$stream_name" --json)"
restored_message_count="$(jq -er '.state.messages' <<<"$restored_stream_info")"
restored_replica_count="$(jq -er '.config.replicas' <<<"$restored_stream_info")"
if [[ "$restored_message_count" != "$source_message_count" ]]; then
  printf 'Restored JetStream message count mismatch: expected %s, got %s.\n' "$source_message_count" "$restored_message_count" >&2
  exit 1
fi
if [[ "$restored_replica_count" != "1" ]]; then
  printf 'Expected one replica on the isolated restore target, got %s.\n' "$restored_replica_count" >&2
  exit 1
fi

restored_payload="$(nats --server "$NATS_RESTORE_URL" stream get "$stream_name" 2 --json | jq -er '.data' | base64 --decode)"
if [[ "$restored_payload" != "aetherflow-restore-message-2" ]]; then
  printf 'Restored JetStream message payload mismatch: %s.\n' "$restored_payload" >&2
  exit 1
fi

echo "PASS: PostgreSQL schema v8, ten FORCE-RLS tables, twelve RLS policies, JetStream configuration, and retained message payloads survived isolated backup/restore."
