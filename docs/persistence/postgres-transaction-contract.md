# PostgreSQL persistence transaction contract

**Status:** M04 implementation contract. The migration runner and durable task/workflow repository operations have passed PostgreSQL 18 integration CI (latest verification: run `41367694624`; Rust quality CI: run `41367694577`). The adapter is still a persistence vertical slice: it is not yet connected to a durable coordinator/worker loop or a real broker publisher/consumer.

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

## Durable failure, cancellation, timeout, and workflow aggregation

A handler-reported failure is committed with its attempt outcome, task transition, and outbox event in one transaction. Retryable failures move to `retry_waiting` only while the retry budget persisted on the task row permits another attempt; otherwise the task becomes `failed`. Permanent failures become terminal immediately. The storage API accepts a typed `TaskFailure` with an application-defined kind, diagnostic message, and retryability classification.

Explicit task cancellation and timeout are version-guarded terminal transitions. The transaction locks the task row and checks its expected version. For a running task, it also closes the matching active attempt with outcome `cancelled` or `timed_out`; it then clears the lease, increments the task version, records the terminal timestamp, and appends `task.cancelled` or `task.timed_out` to the outbox. A stale worker cannot heartbeat or complete after that transition because the task is no longer running and its version has changed. Cancelling a task that has not been claimed does not fabricate an attempt row.

Workflow aggregation is an explicit `PgTaskRepository` operation. It locks the workflow row and its task rows in a stable order. The workflow becomes `succeeded` only when every task run is `succeeded`. When a task has reached `failed`, `cancelled`, or `timed_out`, the aggregate outcome is selected from the earliest persisted terminal task timestamp, with task-run ID as a deterministic tie-breaker. The same transaction changes the workflow status, cancels every remaining non-terminal task, closes any active attempt as cancelled, clears active leases, and appends the corresponding task and workflow outbox events. The database timestamp ordering is the persisted arbitration rule; it should not be interpreted as a guarantee about external side-effect or physical commit ordering.

The PostgreSQL worker invokes aggregation after task completion, handler failure, timeout, and recovered lease outcomes. Operators may also call it explicitly, for example after an administrative transition.

## Durable workflow coordinator and worker

`PgWorkflowCoordinator` accepts the runtime's validated `WorkflowDefinition`. It derives a stable task-run identifier from the workflow-run ID and task-definition ID, maps dependency task IDs to those task-run IDs, and calls the same atomic `create_workflow` transaction that writes workflow, task, dependency, and outbox rows. A caller supplies an absolute availability timestamp (`SystemTime` serialized as `TIMESTAMPTZ`); the default submission method uses wall-clock time, never the runtime's process-local monotonic clock. The workflow-run identifier remains caller-supplied and is the durable execution identity.

`PostgresTaskWorker` is an asynchronous worker loop over `PgTaskRepository`. It periodically recovers expired leases, promotes dependency-ready tasks, and claims ready work using the database lease and fencing contract above. Concurrency is bounded per worker process; multiple worker processes coordinate through PostgreSQL row locks and fencing tokens. Each claim is converted to the backend-neutral runtime `TaskExecutionClaim` and executed through the existing `TaskRegistry` and `TaskHandler`. The worker loads each direct predecessor's persisted successful output and exposes it via `TaskContext::dependency_output`; it does not concatenate payloads or transform them.

The worker heartbeats the lease while the handler runs. A success is stored with fenced completion; retryable/permanent handler errors use typed `TaskFailure`; configured attempt timeouts use the durable timeout transition. After terminal/retry outcomes it invokes workflow aggregation. If a worker process stops, its in-flight futures and local cancellation handles disappear; unfinished database claims remain leased until expiry and are then recovered by a later worker. Graceful shutdown stops new claims, drains handlers, signals cooperative cancellation after the drain deadline, and may abort stragglers after the cancellation grace period. Aborted stragglers are not falsely marked complete or cancelled by shutdown; PostgreSQL lease recovery remains the source of truth.

The integration contract is verified on PostgreSQL 18 and Rust CI runs `41369526661` and `41369526772`: the test submits a two-node dependency DAG, executes both handlers via the durable worker, verifies the child receives the root's stored output, and checks both persisted task state and terminal workflow success.

## Outbox dispatch boundary

The repository exposes outbox claim, retry/reschedule, and acknowledgement operations. Claims have an expiry and a monotonically increasing claim token; retrying and reclaiming an event advances the token, so an old publisher cannot acknowledge a newer publisher's claim. These APIs are integration-tested against PostgreSQL 18, including stale-publisher rejection.

The actual broker publish is deliberately outside the database transaction and is not yet implemented in this repository. The future publisher must publish after claiming and acknowledge only after the broker confirms acceptance. A crash between publish and acknowledgement can redeliver the event. Delivery is therefore at-least-once, and a consumer inbox or equivalent durable deduplication by stable `event_id` remains required. Do not describe outbox claim/ack support as a completed broker integration.

The PostgreSQL integration suite also verifies the database-side atomicity boundary: it injects a constraint violation specifically on the `task.succeeded` outbox row after task and attempt updates, confirms the task and attempt remain `running` and no success event exists, then removes the constraint and retries the same fenced completion successfully. This proves rollback when an outbox append fails; it does not exercise a process kill during `COMMIT` or the broker publish/ack crash window.

## Restart recovery verification

The PostgreSQL 18 integration suite includes a subprocess recovery scenario. The first test process creates and claims a task, expires its lease in PostgreSQL, then drops its pool and repository handles before spawning a separate test process. The child receives only the database URL and persisted workflow/task identifiers. It creates a fresh pool and repository, verifies the existing migration, recovers the expired lease, promotes and claims the task again, verifies attempt number 2 and fencing token 2, completes it, and aggregates the workflow to `succeeded`. This verifies that the tested recovery path reconstructs its state from PostgreSQL rather than relying on the first process's in-memory objects.

This is a focused process-restart integration test, not a claim of exhaustive crash consistency. Separately, PostgreSQL 18 failure-injection coverage now forces the outbox insert to violate a temporary check constraint after the task and attempt updates have run; the test verifies both updates roll back and no success event is stored, then removes the constraint and completes using the same valid claim. Process termination during database commit, broker publish/ack redelivery, and broader worker/coordinator restart tests remain outstanding.

## Migration discipline

- Each migration is ordered, immutable after release, and checksummed.
- Apply a migration and record its version/name/checksum in `aetherflow_schema_migrations` in one transaction.
- Acquire a PostgreSQL advisory lock for migration execution so two server processes do not apply the same migration concurrently.
- Fail startup on an unknown newer schema or checksum drift; do not silently skip or rewrite applied migrations.
- Execute every migration against a real PostgreSQL service in CI before marking it verified.

## Deliberate non-guarantees

A lease is temporary permission to attempt work, not proof that the previous worker stopped executing. Fencing prevents stale database commits; it cannot undo an already-issued HTTP request or other external effect. Handlers still need stable idempotency keys or transactional integration at the downstream boundary.

## Implemented vertical slice and remaining scope

The current M04 implementation includes `PgTaskRepository`, `PgWorkflowCoordinator`, and `PostgresTaskWorker`. It implements atomic workflow/task/dependency creation, deterministic task-run ID materialization from a workflow-run/task ID pair, dependency-gated ready promotion, `SKIP LOCKED` claims, attempt creation, fenced heartbeat/completion, output persistence, expired-lease recovery, typed retryable/permanent handler failure, version-guarded cancellation/timeout, workflow terminal aggregation, outbox claim/retry/ack with stale-publisher fencing, and a bounded async worker that runs existing runtime handlers with persisted dependency outputs. PostgreSQL 18 run `41369526661` and Rust CI run `41369526772` verify the two-task DAG from submission through handler execution and terminal workflow success. Earlier PostgreSQL 18 runs `41368104662` and `41368359268` cover subprocess lease recovery and rollback when outbox insertion fails. Real broker publishing, consumer inbox/deduplication, process termination around commit and publish/ack, and broader coordinator restart tests remain future work.
