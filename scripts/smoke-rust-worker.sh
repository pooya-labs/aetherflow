#!/usr/bin/env bash
set -Eeuo pipefail

: "${AETHERFLOW_TEST_DATABASE_URL:?AETHERFLOW_TEST_DATABASE_URL is required}"
: "${AETHERFLOW_RUNTIME_ROLE_TEST_DATABASE_URL:?AETHERFLOW_RUNTIME_ROLE_TEST_DATABASE_URL is required}"
: "${AETHERFLOW_TEST_NATS_URL:?AETHERFLOW_TEST_NATS_URL is required}"
: "${GITHUB_RUN_ID:?GITHUB_RUN_ID is required}"
: "${GITHUB_RUN_ATTEMPT:?GITHUB_RUN_ATTEMPT is required}"

server_binary="target/release/aetherflow-server"
worker_binary="target/release/aetherflow-worker"
if [[ ! -x "$server_binary" || ! -x "$worker_binary" ]]; then
  echo "The release server and worker binaries must be built before this smoke test." >&2
  exit 1
fi

server_log="$(mktemp)"
worker_log="$(mktemp)"
server_pid=""
worker_pid=""
api_address="127.0.0.1:18080"
api_url="http://$api_address"
api_token="aetherflow-worker-smoke-token-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
stream_name="AETHERFLOW_WORKER_SMOKE_${GITHUB_RUN_ID}"
durable_name="aetherflow_worker_smoke_ci_${GITHUB_RUN_ID}"
node_id="worker-smoke-node-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
task_worker_id="worker-smoke-task-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
transport_worker_id="worker-smoke-transport-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
subject="aetherflow.worker.smoke"
workflow_run_id="worker-smoke-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"

cleanup() {
  local status=$?
  set +e
  if [[ -n "$worker_pid" ]] && kill -0 "$worker_pid" 2>/dev/null; then
    kill -INT "$worker_pid" 2>/dev/null
    wait "$worker_pid"
  fi
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill -TERM "$server_pid" 2>/dev/null
    wait "$server_pid"
  fi
  if [[ "$status" -ne 0 ]]; then
    echo "--- AetherFlow API server log ---" >&2
    cat "$server_log" >&2
    echo "--- AetherFlow Rust worker log ---" >&2
    cat "$worker_log" >&2
  fi
  rm -f "$server_log" "$worker_log"
  exit "$status"
}
trap cleanup EXIT

DATABASE_URL="$AETHERFLOW_RUNTIME_ROLE_TEST_DATABASE_URL" \
AETHERFLOW_MIGRATION_DATABASE_URL="$AETHERFLOW_TEST_DATABASE_URL" \
DATABASE_SSL_MODE=disable \
AETHERFLOW_API_TOKEN="$api_token" \
AETHERFLOW_BIND="$api_address" \
OTEL_SDK_DISABLED=true \
  "$server_binary" >"$server_log" 2>&1 &
server_pid=$!

server_ready=false
for attempt in $(seq 1 60); do
  if ! kill -0 "$server_pid" 2>/dev/null; then
    echo "AetherFlow API server exited before becoming healthy." >&2
    exit 1
  fi
  if curl --silent --show-error --fail "$api_url/health" >/dev/null 2>&1; then
    server_ready=true
    break
  fi
  sleep 1
done
if [[ "$server_ready" != "true" ]]; then
  echo "AetherFlow API server did not become healthy." >&2
  exit 1
fi

DATABASE_URL="$AETHERFLOW_RUNTIME_ROLE_TEST_DATABASE_URL" \
AETHERFLOW_NATS_URLS="$AETHERFLOW_TEST_NATS_URL" \
AETHERFLOW_NATS_STREAM="$stream_name" \
AETHERFLOW_NATS_SUBJECT="$subject" \
AETHERFLOW_NATS_DURABLE="$durable_name" \
AETHERFLOW_NODE_ID="$node_id" \
AETHERFLOW_TASK_WORKER_ID="$task_worker_id" \
AETHERFLOW_TRANSPORT_WORKER_ID="$transport_worker_id" \
AETHERFLOW_WORKER_CONCURRENCY=2 \
  "$worker_binary" >"$worker_log" 2>&1 &
worker_pid=$!

worker_ready=false
for attempt in $(seq 1 60); do
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "AetherFlow Rust worker exited before becoming ready." >&2
    exit 1
  fi
  if grep --quiet --fixed-strings "AetherFlow Rust worker is running." "$worker_log"; then
    worker_ready=true
    break
  fi
  sleep 1
done
if [[ "$worker_ready" != "true" ]]; then
  echo "AetherFlow Rust worker did not become ready." >&2
  exit 1
fi

# Registered workers do not silently inherit tenant access. The operator API
# writes legacy-default workflows, so the CI fixture explicitly grants that
# tenant to the task and transport worker registrations using the admin role.
# The grants are scoped to this disposable run and are created only after the
# worker registration rows exist (they are protected by foreign keys).
psql "$AETHERFLOW_TEST_DATABASE_URL" \
  --no-psqlrc --set ON_ERROR_STOP=1 \
  --set "task_worker_id=$task_worker_id" \
  --set "transport_worker_id=$transport_worker_id" \
  --command "INSERT INTO worker_tenant_grants (tenant_id, worker_id, status)
             VALUES
               ('legacy-default', :'task_worker_id', 'active'),
               ('legacy-default', :'transport_worker_id', 'active')
             ON CONFLICT (tenant_id, worker_id) DO UPDATE
             SET status = 'active',
                 granted_at = clock_timestamp(),
                 expires_at = NULL,
                 revoked_at = NULL"
echo "Granted legacy-default to the two registered demo workers."

input_base64="$(printf '%s' '{"a":19,"b":23}' | base64 --wrap=0)"
request_body="$(jq -n \
  --arg workflow_id "worker-smoke-workflow" \
  --arg workflow_run_id "$workflow_run_id" \
  --arg input_base64 "$input_base64" \
  '{
    workflow_id: $workflow_id,
    workflow_run_id: $workflow_run_id,
    tasks: [{
      task_id: "json_add",
      depends_on: [],
      input_base64: $input_base64
    }],
    retry_policy: {
      max_attempts: 3,
      initial_backoff_ms: 50,
      max_backoff_ms: 1000
    }
  }')"

receipt="$(curl --silent --show-error --fail \
  --request POST "$api_url/api/v1/workflow-runs" \
  --header "Authorization: Bearer $api_token" \
  --header "Content-Type: application/json" \
  --data "$request_body")"
task_run_id="$(jq --exit-status --raw-output '.task_runs[0].task_run_id' <<<"$receipt")"
echo "Submitted workflow $workflow_run_id with task $task_run_id."

task_succeeded=false
for attempt in $(seq 1 90); do
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "AetherFlow Rust worker exited while processing the task." >&2
    exit 1
  fi
  task_snapshot="$(curl --silent --show-error --fail \
    --header "Authorization: Bearer $api_token" \
    "$api_url/api/v1/task-runs/$task_run_id")"
  task_status="$(jq --raw-output '.status' <<<"$task_snapshot")"
  case "$task_status" in
    succeeded)
      [[ "$(jq --raw-output '.output_present' <<<"$task_snapshot")" == "true" ]]
      [[ "$(jq --raw-output '.output_size_bytes' <<<"$task_snapshot")" == "2" ]]
      task_succeeded=true
      break
      ;;
    failed|cancelled|timed_out)
      echo "Worker task reached terminal status: $task_status" >&2
      exit 1
      ;;
  esac
  sleep 1
done
if [[ "$task_succeeded" != "true" ]]; then
  echo "Task did not succeed before the smoke-test deadline." >&2
  exit 1
fi

output_hex="$(psql "$AETHERFLOW_TEST_DATABASE_URL" \
  --no-psqlrc --no-align --tuples-only --set ON_ERROR_STOP=1 \
  --set "task_run_id=$task_run_id" \
  --command "SELECT encode(output_payload, 'hex') FROM task_runs WHERE tenant_id = 'legacy-default' AND task_run_id = :'task_run_id'")"
if [[ "$output_hex" != "3432" ]]; then
  echo "Expected task output bytes for 42 (3432); received: $output_hex" >&2
  exit 1
fi

transport_processed=false
for attempt in $(seq 1 60); do
  processed_count="$(psql "$AETHERFLOW_TEST_DATABASE_URL" \
    --no-psqlrc --no-align --tuples-only --set ON_ERROR_STOP=1 \
    --set "task_run_id=$task_run_id" \
    --set "consumer_name=$durable_name" \
    --command "SELECT count(*) FROM outbox_events AS event JOIN consumer_inbox AS inbox ON inbox.tenant_id = event.tenant_id AND inbox.event_id = event.event_id::text WHERE event.tenant_id = 'legacy-default' AND event.aggregate_type = 'task_run' AND event.aggregate_id = :'task_run_id' AND event.event_type = 'task.succeeded' AND event.published_at IS NOT NULL AND inbox.consumer_name = :'consumer_name' AND inbox.status = 'processed'")"
  if [[ "$processed_count" -ge 1 ]]; then
    transport_processed=true
    break
  fi
  sleep 1
done
if [[ "$transport_processed" != "true" ]]; then
  echo "Task succeeded, but the task.succeeded event was not confirmed in the JetStream-backed inbox." >&2
  exit 1
fi

echo "Standalone Rust worker smoke test passed: task output=42; transactional outbox published; JetStream inbox processed."
