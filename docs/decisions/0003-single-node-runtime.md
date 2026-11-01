# ADR-0003: Single-node runtime and in-memory repository

- **Status:** Accepted for the M03 implementation baseline
- **Date:** 2026-11-02
- **Scope:** Process-local scheduling and repository contracts; durable storage remains out of scope

## Context

The domain state machines and retry policy are implemented and independently tested. The next step is to prove task eligibility, exclusive local claiming, attempt accounting, and stale-update rejection without adding PostgreSQL or a broker prematurely.

## Decisions

### Crate boundary

Add aetherflow-runtime as a crate that depends on aetherflow-core. The core must not depend on runtime, clocks, storage, or worker implementation details. A future PostgreSQL adapter can implement the repository port without changing domain transitions.

### In-memory storage and concurrency

The initial adapter uses one Mutex<BTreeMap<String, TaskRun>>. The mutex makes each repository operation atomic within one process and a B-tree provides deterministic iteration. Claim ordering is explicitly by availability time, creation time, then task-run identifier.

Repository methods are synchronous, short-lived operations. Runtime code must not hold a repository lock across an await. The single lock is deliberately simple for M03; finer-grained locking is justified only by measured contention.

### Scheduling and clocks

Scheduling APIs receive an explicit monotonic millisecond value. The runtime exposes an injectable Clock trait and an Instant-backed implementation. Monotonic values are process-local and must not be persisted or compared across restarts. Durable scheduling in M04 must use a database-safe time representation and authoritative database-side comparisons.

Pending and retry-waiting tasks become ready only when their supplied availability time is due. The repository does not sleep or run a background timer.

### Claim and state mutation

A local claim atomically moves a ready task to running, records one worker owner, and increments the one-based attempt count. A claim is exclusive only inside this repository instance. Mutations require the expected task-run version; completion, failure, retry scheduling, cancellation, and timeout all pass through the domain state machine.

Retry delay and retryability are computed by the caller using the domain retry policy. The repository stores the resulting retry deadline but does not choose jitter or classify errors.

### Explicit limitations

This adapter is process-local, non-durable, and not distributed. It has no lease expiry, heartbeat, fencing token, crash recovery, or cross-process coordination. A process exit loses all records. Cancellation or timeout changes orchestration state; it does not prove that an already-running external side effect was stopped or undone.

## Consequences

- Repository behavior is deterministic and testable without a database or async runtime.
- Concurrent local claim attempts are serialized and cannot claim the same task run twice.
- Version checks make stale completion and competing terminal updates explicit.
- PostgreSQL, durable leases, and recovery remain separate milestones rather than hidden claims of the in-memory adapter.
