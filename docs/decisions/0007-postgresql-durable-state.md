# ADR-0007: PostgreSQL durable-state and lease contract

- **Status:** Accepted as the M04 design baseline; initial DDL execution is pending
- **Date:** 2026-11-02
- **Scope:** Durable workflow/task records, attempts, dependency edges, leases, fencing, and transactional outbox

## Context

M03 proves the single-process execution contract but its in-memory repository loses all state on process exit. M04 needs a durable source of truth that supports concurrent workers, restart recovery, stale-worker rejection, and reliable publication of state changes without promising exactly-once external effects.

## Decisions

### PostgreSQL is authoritative

Persist workflow runs, task runs, ordered direct-dependency edges, attempt history, lease ownership, and outbox records in PostgreSQL. Do not treat in-memory queues, futures, or process-local cancellation registries as durable state.

Persist absolute timestamps as `TIMESTAMPTZ`; process-local monotonic clock ticks cannot be reused after restart or across hosts. Database time is authoritative for lease expiry checks.

### State and history are separate

`workflow_runs` and `task_runs` store the latest versioned lifecycle snapshots. `task_attempts` stores one row per claimed attempt, including worker ID, fencing token, heartbeat, terminal outcome, and error details. `task_run_dependencies` stores ordered direct predecessor task-run references so a restarted coordinator can reconstruct the DAG and handlers can load successful direct predecessor outputs.

Task input and output remain opaque byte payloads with the M03 1 MiB bound. The application still owns payload encoding and schema compatibility.

### Claim, heartbeat, and fencing

Claim ready work in a short transaction using `SELECT ... FOR UPDATE SKIP LOCKED`, deterministic ordering, and an atomic row update. Each claim increments the task-run optimistic version, attempt number, and monotonically increasing fencing token, then creates the attempt record and a `task.claimed` outbox event in the same transaction.

Heartbeat, completion, and failure require task-run ID, worker ID, current fencing token, expected version, and an unexpired lease according to database time. A stale or expired worker cannot commit after ownership changes.

Expired leases are recovered in a separate short transaction. The current attempt is marked `lease_expired`; the task transitions to `retry_waiting` or `failed` according to its persisted attempt budget; the lease is cleared; and a state-change event is appended.

### Transactional outbox

Every externally observable state transition writes an outbox row in the same transaction as the state change. A publisher claims outbox rows with row locking, publishes outside the transaction, and marks them published only if its own claim token still matches.

The publisher can crash after delivery but before marking the event published. Duplicate delivery is expected; consumers deduplicate by stable event ID. This is at-least-once delivery, not universal exactly-once execution.

### Migration discipline

Migrations are immutable, ordered, and checksummed. A migration runner records version/name/checksum in `aetherflow_schema_migrations` in the same transaction as applying the DDL and serializes migration execution with a PostgreSQL advisory lock. Unknown newer versions and checksum drift fail closed.

The initial schema is a design draft in `migrations/postgres/0001_initial_schema.sql`. It must pass real PostgreSQL migration and integration tests before being treated as verified.

## Consequences

- Worker claims are safe under row-level concurrency without holding transactions during task execution.
- Fencing rejects stale database writes, but cannot stop external side effects already issued by a stale worker.
- Workflow/task state and outbox events cannot diverge when all state changes use the transactional command path.
- PostgreSQL remains the source of truth; NATS or another broker is a delivery mechanism, not the durable state store.
- The runtime repository port must become asynchronous or gain a database-specific async port before this adapter is implemented.
