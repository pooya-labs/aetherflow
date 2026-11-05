#!/usr/bin/env bash
set -Eeuo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

migration_dir="crates/aetherflow-runtime/migrations/postgres"
migration_source="crates/aetherflow-storage-postgres/src/lib.rs"
rls_source="$migration_dir/0006_force_tenant_row_level_security.sql"
backup_doc="docs/operations/backup-restore.md"
production_doc="docs/operations/production-guide.md"
upgrade_doc="docs/operations/schema-upgrades.md"
restore_script="scripts/verify-operations-restore.sh"
restore_example="crates/aetherflow-transport-nats/examples/verify_restored_execution.rs"
postgres_workflow=".github/workflows/postgres-schema.yml"

for path in "$migration_source" "$rls_source" "$backup_doc" "$production_doc" "$upgrade_doc" "$restore_script" "$restore_example" "$postgres_workflow"; do
  if [[ ! -f "$path" ]]; then
    printf 'Missing operations validation input: %s\n' "$path" >&2
    exit 1
  fi
done

schema_version="$(sed -nE 's/^const MIGRATION_VERSION: i64 = ([0-9]+);$/\1/p' "$migration_source" | head -n 1)"
if [[ ! "$schema_version" =~ ^[0-9]+$ ]]; then
  echo "Could not read MIGRATION_VERSION from $migration_source" >&2
  exit 1
fi

migration_files=( "$migration_dir"/[0-9][0-9][0-9][0-9]_*.sql )
if (( ${#migration_files[@]} != schema_version )); then
  printf 'Migration count (%s) differs from MIGRATION_VERSION (%s).\n' "${#migration_files[@]}" "$schema_version" >&2
  exit 1
fi

for version in $(seq 1 "$schema_version"); do
  prefix="$(printf '%04d' "$version")"
  matches=( "$migration_dir"/"$prefix"_*.sql )
  if (( ${#matches[@]} != 1 )); then
    printf 'Expected exactly one migration numbered %s; found %s.\n' "$prefix" "${#matches[@]}" >&2
    exit 1
  fi
done

latest_migration="$(printf '%s\n' "${migration_files[@]}" | sort -V | tail -n 1)"
latest_name="$(basename "$latest_migration")"
if ! grep -Eq "\\*\\*Current schema in this repository:\\*\\* v${schema_version} " "$upgrade_doc"; then
  printf 'Schema-upgrade runbook does not declare schema v%s.\n' "$schema_version" >&2
  exit 1
fi
if ! grep -Fq "$latest_name" "$upgrade_doc"; then
  printf 'Schema-upgrade runbook does not name latest migration %s.\n' "$latest_name" >&2
  exit 1
fi

mapfile -t protected_tables < <(
  sed -n '/FOREACH protected_table IN ARRAY ARRAY\[/,/    \]/p' "$rls_source" |
    grep -oE "'[a-z_]+'" |
    tr -d "'"
)
mapfile -t documented_tables < <(
  awk '
    index($0, "AND c.relname = ANY (ARRAY[") { in_array = 1; next }
    in_array && index($0, "])") { exit }
    in_array { print }
  ' "$upgrade_doc" |
    grep -oE "'[a-z_]+'" |
    tr -d "'"
)

if (( ${#protected_tables[@]} != 10 )); then
  printf 'Expected 10 RLS-protected tables in the source migration; found %s.\n' "${#protected_tables[@]}" >&2
  exit 1
fi
if ! diff -u <(printf '%s\n' "${protected_tables[@]}") <(printf '%s\n' "${documented_tables[@]}"); then
  echo "RLS table list in schema-upgrades.md does not match migration 0006." >&2
  exit 1
fi

if ! grep -Fq 'natscli/releases/tag/v0.4.0' "$backup_doc" ||
   ! grep -Fq 'nats stream backup' "$backup_doc" ||
   ! grep -Fq 'nats stream restore' "$backup_doc"; then
  echo "Backup runbook must pin NATS CLI v0.4.0 and its NATS 2.12-compatible snapshot commands." >&2
  exit 1
fi

if ! grep -Fq '[backup and restore](backup-restore.md)' "$upgrade_doc"; then
  echo "Schema-upgrade runbook must link to the coordinated backup/restore runbook." >&2
  exit 1
fi

if ! grep -Fq 'target/release/aetherflow-server' "$restore_script" ||
   ! grep -Fq 'verify_restored_execution' "$restore_script" ||
   ! grep -Fq 'restore_smoke_run' "$restore_script" ||
   ! grep -Fq 'aetherflow.recovery.events' "$restore_script"; then
  echo "The restore rehearsal must start the server and execute its dedicated runtime verifier." >&2
  exit 1
fi
if ! grep -Fq 'kill -KILL "$server_pid"' "$restore_script" ||
   ! grep -Fq 'PASS: accepted workflow and pending task remained readable after abrupt API termination and restart.' "$restore_script" ||
   ! grep -Fq 'SIGKILL' "$backup_doc" ||
   ! grep -Fq 'before any worker starts' "$backup_doc"; then
  echo "The restore rehearsal must inject and document API process restart after workflow acceptance and before worker startup." >&2
  exit 1
fi
if ! grep -Fq 'Build restored server and execution verifier' "$postgres_workflow"; then
  echo "PostgreSQL CI must build the restored server and execution verifier before the rehearsal." >&2
  exit 1
fi
if ! grep -Fq 'POST /api/v1/workflow-runs' "$backup_doc" ||
   ! grep -Fq 'PostgreSQL worker process' "$backup_doc" ||
   ! grep -Fq 'PostgreSQL inbox consumer' "$backup_doc" ||
   ! grep -Fq 'before worker startup' "$production_doc"; then
  echo "Operations runbooks must describe the HTTP-to-worker-to-inbox and control-plane restart rehearsal." >&2
  exit 1
fi

echo "Operations runbooks match migration v${schema_version}, all 10 forced-RLS tables, the NATS CLI pin, and the restore/restart application recovery drills."
