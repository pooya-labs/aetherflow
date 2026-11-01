# ADR-0004: Bounded asynchronous task scheduler

- **Status:** Accepted for the M03 implementation baseline
- **Date:** 2026-11-02
- **Scope:** Single-process task registration, scheduling, retries, timeouts, and shutdown

## Context

M03 established a synchronous repository port and a verified process-local implementation. The runtime now needs an async execution loop without allowing unbounded in-flight task futures or coupling the domain core to an async framework.

## Decisions

### Async runtime dependency

Use Tokio `=1.54.1` in `aetherflow-runtime`, with only the macros, multi-thread runtime, sync, and time features required by this scheduler. Keep the exact dependency graph in Cargo.lock and require locked dependency resolution in CI. The core crate remains Tokio-free.

### Handler registry

A fixed registry maps a typed TaskId to one handler. Duplicate registrations are rejected. A missing handler is treated as a permanent task failure rather than silently leaving a claimed task in Running.

Handlers receive a snapshot of TaskClaim and return a future whose result is classified as retryable or permanent. Handler code owns the idempotency of external effects; the scheduler does not promise exactly-once execution.

### Bounded scheduling

The scheduler claims tasks only while the active JoinSet length is below the configured non-zero concurrency limit. Repository operations remain synchronous and complete before a handler future is awaited. The repository mutex is never held across an await.

### Retry and timeout behavior

Retryable failures are rescheduled only while RetryPolicy permits another attempt. The retry deadline uses the existing capped exponential calculation. Jitter is not added yet and remains a future injectable policy.

A configured per-attempt timeout drops the handler future and attempts to commit TimedOut using the task-run's expected version. This does not prove an already-issued external side effect was cancelled or undone.

### Cancellation and shutdown

Cancellation commits a terminal repository transition. A running handler future is not forcibly cancelled; when it later tries to commit, its stale version is ignored and the cancellation remains authoritative.

Shutdown stops claiming new work and drains active handlers before returning. There is no forced drain deadline yet, so a non-terminating handler can delay shutdown indefinitely.

### Explicit limitations

The scheduler is single-process and uses the in-memory repository. Worker panics terminate the scheduler and can leave their task run in Running until process shutdown. No lease expiry, fencing, recovery, persisted input/output, workflow DAG, or distributed delivery exists yet.

## Consequences

- Concurrent task attempts are bounded by explicit configuration.
- Handler errors have a deterministic retry/permanent classification.
- Timeouts and cancellation obey domain version checks.
- The runtime can later keep its handler contract while replacing the in-memory repository with durable adapters.
