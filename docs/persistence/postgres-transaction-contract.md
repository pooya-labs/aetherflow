# PostgreSQL persistence transaction contract

**Status:** M04 implementation contract. The accompanying initial migration, migration runner, and first task repository vertical slice are verified against PostgreSQL 18 in runs `41362986570` and `41365026199`; remaining operations are identified below as not yet implemented.

## Source of truth and time

PostgreSQL is authoritative for workflow, task, attempt, lease, and outbox state. A process restart reconstructs work from these rows, not from a scheduler's in-memory queues or futures.

Persist absolute `TIMESTAMPTZ` deadlines. Do not persist `MonotonicClock` ticks: a process-local monotonic origin has no meaning after restart or on another host. Lease comparisons and expiry decisions use database time (for example, `clock_timestamp()`) so workers do not decide ownership using skewed local clocks.

All state-changing operations run in short transactions. Never hold a database transaction open while a handler executes or while a network publish is in progress.

## Lifecycle and dependency promotion

Create a workflow run, all task-run rows, and ordered `task_run_dependencies` edges in one transaction. Initially tasks are `pending`. A task becomes `ready` only when its `available_at` is due and every direct dependency is `succeeded`. The promotion transaction must:

1. Lock eligible task rows.
2. Re-check that all parent task runs are successful.
3. Transition `pending` or `retry_waiting` to `ready` and increment the optimistic version.
4. Insert the corresponding outbox event before commit.

If a dependency becomes failed, cancelled, or timed out, the workflow coordinator settles the workflow and cancels pending descendants in explicit state transitions; a dependency is never considered satisfied merely because its row exists.

## Atomic task claim

A worker claims one ready task in a short transaction. Candidate selection is deterministic and uses row locking:

```sql
SELECT task_run_id
FROM task_runs
WHERE status = 'ready'
  AND available_at <= clock_timestamp()
ORDER BY available_at, created_at, task_run_id
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

In that same transaction, update the selected row to `running`, increment `version`, `attempts_started`, and `fencing_token`, set `claimed_by` and `lease_expires_at`, insert the `task_attempts` row, and append a `task.claimed` outbox event. If any operation fails, the entire claim rolls back. A successful claim returns the new fencing token and expected version to the worker.

PostgreSQL documents `SKIP LOCKED` as producing an inconsistent view unsuitable for general-purpose reads, while identifying queue-like multi-consumer access as an appropriate use. It skips rows whose row-level locks cannot be acquired immediately; it does not provide general fairness or eliminate starvation. See the official `SELECT` locking-clause documentation: https://www.postgresql.org/docs/current/sql-select.html.

## Heartbeat and fencing

A heartbeat extends a lease only when all of these still match: task-run ID, `status = 'running'`, `claimed_by`, the current `fencing_token`, and a lease that has not already expired according to database time. The same transaction updates the active attempt's `heartbeat_at`.

Every worker completion/failure write includes worker ID, fencing token, expected task-run version, and `lease_expires_at > clock_timestamp()`. A zero-row update means the worker no longer owns the task. It must discard the result and must not write an outbox event.

A new claim always increments the fencing token. If worker A pauses, loses its lease, and worker B reclaims the task, A's old token cannot commit after B takes ownership, even if A later resumes.

## Lease expiry and recovery

A reaper selects expired `running` rows with `FOR UPDATE SKIP LOCKED`. In one transaction it:

1. Marks the current attempt `lease_expired` and records its finish time.
2. Clears the lease owner/expiry.
3. Moves the task to `retry_waiting` with an application-computed retry deadline when the attempt budget permits, otherwise to `failed`.
4. Increments the task-run version and inserts the state-change outbox event.
5. Updates workflow state if the task has exhausted its attempt budget.

A worker with an expired lease must not extend or finish that attempt, even if the reaper has not run yet.

## Completion and transactional outbox

Successful completion is one database transaction:

1. Guard the task update by task-run ID, `running` status, worker ID, fencing token, expected version, and unexpired lease.
2. Set `status = 'succeeded'`, store validated output bytes, clear lease fields, increment version, and set `finished_at`.
3. Mark the active attempt `succeeded` and set its finish time.
4. Insert the `task.succeeded` outbox event using the new aggregate version.
5. Commit.

The same rule applies to failure, retry scheduling, timeout, cancellation, and workflow transitions: state change and outbox append commit together or neither does.

The outbox publisher claims pending rows with row locks, publishes outside the transaction, then marks an event published only when its claim token still matches. A crash after publish but before marking it published can cause redelivery. Delivery is therefore at-least-once; consumers deduplicate by stable `event_id`. The outbox prevents lost state-change notifications, but does not make external side effects exactly-once.

## Migration discipline

- Each migration is ordered, immutable after release, and checksummed.
- Apply a migration and record its version/name/checksum in `aetherflow_schema_migrations` in one transaction.
- Acquire a PostgreSQL advisory lock for migration execution so two server processes do not apply the same migration concurrently.
- Fail startup on an unknown newer schema or checksum drift; do not silently skip or rewrite applied migrations.
- Execute every migration against a real PostgreSQL service in CI before marking it verified.

## Deliberate non-guarantees

A lease is temporary permission to attempt work, not proof that the previous worker stopped executing. Fencing prevents stale database commits; it cannot undo an already-issued HTTP request or other external effect. Handlers still need stable idempotency keys or transactional integration at the downstream boundary.

## Implemented vertical slice and remaining scope

The current `PgTaskRepository` implements atomic workflow/task/dependency creation, dependency-gated ready promotion, `SKIP LOCKED` claims, attempt creation, fenced heartbeat, successful completion/output persistence, expired-lease recovery, and outbox append in the same transaction for those transitions. PostgreSQL 18 run `41365026199` exercises this path including stale-worker rejection after lease reclamation. The adapter is not yet integrated with the async scheduler. Handler-reported permanent/retryable failure, explicit cancellation/timeout, workflow terminal aggregation, outbox publisher claim/ack/redelivery, and full process-restart coordinator recovery remain future work.
