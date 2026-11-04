#!/usr/bin/env bash
set -Eeuo pipefail

readonly stream_name="AETHERFLOW_RESTORE_DRILL"
readonly stream_subject="aetherflow.restore.drill"
readonly restore_database="aetherflow_restore_drill"
readonly work_dir="$(mktemp -d)"
readonly nats_restore_container="aetherflow-nats-restore-drill"
readonly repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly runtime_role="aetherflow_runtime_test"
readonly runtime_password="aetherflow-runtime-ci-password"
readonly api_bind="127.0.0.1:8081"
server_pid=""

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" >/dev/null 2>&1 || true
    wait "$server_pid" >/dev/null 2>&1 || true
  fi
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

echo "Granting the restored database to the restricted application role..."
psql_source --command "GRANT CONNECT ON DATABASE \"$restore_database\" TO $runtime_role"
psql_restore --command "
  GRANT USAGE ON SCHEMA public TO $runtime_role;
  GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES IN SCHEMA public TO $runtime_role;
  GRANT USAGE, SELECT
    ON ALL SEQUENCES IN SCHEMA public TO $runtime_role;
"

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
  "$work_dir/nats/$stream_name" --no-progress
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
  "$work_dir/nats/$stream_name" --no-progress --replicas 1

restored_stream_info="$(nats --server "$NATS_RESTORE_URL" stream info "$stream_name" --json)"
restored_message_count="$(jq -er '.state.messages' <<<"$restored_stream_info")"
restored_replica_count="$(jq -er '.config.num_replicas' <<<"$restored_stream_info")"
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

echo "Provisioning an application event stream on the restored JetStream target..."
nats --server "$NATS_RESTORE_URL" stream add AETHERFLOW_RECOVERY_EVENTS \
  --subjects aetherflow.recovery.events --storage file --replicas 1 --defaults >/dev/null

restore_admin_url="postgres://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$restore_database"
restore_runtime_url="postgres://$runtime_role:$runtime_password@$PGHOST:$PGPORT/$restore_database"
api_token="$(openssl rand -hex 32)"
api_base_url="http://$api_bind"

echo "Starting AetherFlow against the restored database with the restricted runtime role..."
DATABASE_URL="$restore_runtime_url" \
AETHERFLOW_MIGRATION_DATABASE_URL="$restore_admin_url" \
AETHERFLOW_API_TOKEN="$api_token" \
DATABASE_SSL_MODE=disable \
AETHERFLOW_BIND="$api_bind" \
OTEL_TRACES_EXPORTER=none \
  "$repo_root/target/release/aetherflow-server" >"$work_dir/aetherflow-server.log" 2>&1 &
server_pid=$!

api_ready=false
for attempt in $(seq 1 60); do
  if curl --silent --fail --output /dev/null \
    --header "Authorization: Bearer $api_token" \
    "$api_base_url/api/v1/workers"; then
    api_ready=true
    break
  fi
  if ! kill -0 "$server_pid" >/dev/null 2>&1; then
    cat "$work_dir/aetherflow-server.log" >&2
    echo "AetherFlow exited before the restored API became ready." >&2
    exit 1
  fi
  sleep 1
done
if [[ "$api_ready" != "true" ]]; then
  cat "$work_dir/aetherflow-server.log" >&2
  echo "AetherFlow did not become ready against the restored PostgreSQL database." >&2
  exit 1
fi

echo "Submitting a real workflow through the restored HTTP control plane..."
input_base64="$(printf '%s' 'restore-smoke-input' | base64 --wrap=0)"
jq --null-input --arg input "$input_base64" '{
  workflow_id: "restore_smoke_workflow",
  workflow_run_id: "restore_smoke_run",
  tasks: [{
    task_id: "restore_smoke_task",
    depends_on: [],
    input_base64: $input
  }],
  retry_policy: {
    max_attempts: 1,
    initial_backoff_ms: 0,
    max_backoff_ms: 0
  }
}' >"$work_dir/restore-workflow.json"

submit_status="$(curl --silent --show-error \
  --output "$work_dir/workflow-receipt.json" \
  --write-out '%{http_code}' \
  --request POST \
  --header "Authorization: Bearer $api_token" \
  --header 'Content-Type: application/json' \
  --data-binary "@$work_dir/restore-workflow.json" \
  "$api_base_url/api/v1/workflow-runs")"
if [[ "$submit_status" != "201" ]]; then
  cat "$work_dir/workflow-receipt.json" >&2
  printf 'Expected HTTP 201 from restored workflow submission, got %s.\n' "$submit_status" >&2
  exit 1
fi

jq --exit-status \
  '.workflow_run_id == "restore_smoke_run"
   and .status == "running"
   and (.task_runs | length) == 1
   and .task_runs[0].task_id == "restore_smoke_task"
   and .task_runs[0].status == "pending"' \
  "$work_dir/workflow-receipt.json" >/dev/null
task_run_id="$(jq --exit-status --raw-output '.task_runs[0].task_run_id' "$work_dir/workflow-receipt.json")"

echo "Running the durable PostgreSQL worker, outbox publisher, and JetStream inbox consumer..."
AETHERFLOW_RESTORE_DATABASE_URL="$restore_runtime_url" \
AETHERFLOW_RESTORE_ADMIN_DATABASE_URL="$restore_admin_url" \
AETHERFLOW_RESTORE_NATS_URL="$NATS_RESTORE_URL" \
AETHERFLOW_RESTORE_WORKFLOW_RUN_ID="restore_smoke_run" \
AETHERFLOW_RESTORE_TASK_RUN_ID="$task_run_id" \
  "$repo_root/target/release/examples/verify_restored_execution"

workflow_snapshot="$work_dir/workflow-snapshot.json"
curl --silent --show-error --fail \
  --header "Authorization: Bearer $api_token" \
  "$api_base_url/api/v1/workflow-runs/restore_smoke_run" >"$workflow_snapshot"
jq --exit-status \
  '.status == "succeeded"
   and (.task_runs | length) == 1
   and .task_runs[0].status == "succeeded"' \
  "$workflow_snapshot" >/dev/null

task_snapshot="$work_dir/task-snapshot.json"
curl --silent --show-error --fail \
  --header "Authorization: Bearer $api_token" \
  "$api_base_url/api/v1/task-runs/$task_run_id" >"$task_snapshot"
jq --exit-status \
  '.status == "succeeded"
   and .input_size_bytes == 19
   and .output_present == true
   and .output_size_bytes == 20' \
  "$task_snapshot" >/dev/null

aggregate_outbox_state="$(psql_restore --command "
  SELECT count(*) FILTER (WHERE published_at IS NOT NULL) || ':' ||
         count(*) FILTER (WHERE published_at IS NULL)
  FROM outbox_events
  WHERE tenant_id = 'legacy-default'
    AND aggregate_id IN ('restore_smoke_run', '$task_run_id')
")"
IFS=: read -r published_events pending_events <<<"$aggregate_outbox_state"
processed_inbox_events="$(psql_restore --command "
  SELECT count(*)
  FROM consumer_inbox
  WHERE tenant_id = 'legacy-default'
    AND consumer_name = 'restore_smoke_consumer'
    AND status = 'processed'
    AND event_id IN (
      SELECT event_id::TEXT
      FROM outbox_events
      WHERE tenant_id = 'legacy-default'
        AND aggregate_id IN ('restore_smoke_run', '$task_run_id')
    )
")"
if [[ "$published_events" -lt 1 || "$pending_events" -ne 0 || "$processed_inbox_events" -ne "$published_events" ]]; then
  printf 'Restored workflow event drain mismatch: published=%s pending=%s inbox=%s.\n' \
    "$published_events" "$pending_events" "$processed_inbox_events" >&2
  exit 1
fi

retained_after_execution="$(nats --server "$NATS_RESTORE_URL" stream info "$stream_name" --json | jq -er '.state.messages')"
if [[ "$retained_after_execution" != "3" ]]; then
  printf 'The original restored JetStream stream changed unexpectedly: expected 3 messages, got %s.\n' "$retained_after_execution" >&2
  exit 1
fi

echo "PASS: restored PostgreSQL v8/RLS state and JetStream messages survived restore; AetherFlow started, accepted an HTTP workflow, completed its task, published all outbox events, and committed every event to the PostgreSQL inbox."
