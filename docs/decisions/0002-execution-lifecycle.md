# ADR-0002: Execution lifecycle and delivery semantics

- **Status:** Accepted for the initial domain implementation
- **Date:** 2026-11-02
- **Scope:** Core execution-domain contracts; persistence and runtime adapters remain out of scope

## Context

A distributed workflow engine must distinguish a workflow definition from each run,
a task definition from each task execution, and a task execution from its individual
attempts. State changes must be explicit and validated before they are persisted or
published. Retries must not be confused with replaying a successful side effect.

## Decision

### Identity boundaries

The core uses distinct validated identifier types for:

- WorkflowId: a workflow definition
- WorkflowRunId: one run of a workflow
- TaskId: a task definition
- TaskRunId: one execution of a task definition within a workflow run
- AttemptId: one attempt to execute a task run
- WorkerId: one worker process or instance

All currently share the bounded ASCII token syntax of EntityId. Distinct Rust
types prevent accidental substitution across domain boundaries.

### Workflow lifecycle

Allowed transitions:

- Pending -> Running | Cancelled | TimedOut
- Running -> Succeeded | Failed | Cancelled | TimedOut

Succeeded, Failed, Cancelled, and TimedOut are terminal. A terminal workflow
cannot be reopened by a later event.

### Task lifecycle

Allowed transitions:

- Pending -> Ready | Cancelled | TimedOut
- Ready -> Running | Cancelled | TimedOut
- Running -> Succeeded | Failed | RetryWaiting | Cancelled | TimedOut
- RetryWaiting -> Ready | Cancelled | TimedOut

Succeeded, Failed, Cancelled, and TimedOut are terminal. A retry is a new
attempt under the same task run, not a transition that erases the previous attempt.
RetryWaiting -> Ready means the scheduler has made the task eligible again; it
does not itself claim or execute the task.

### Versioning

Every accepted lifecycle transition increments a monotonically increasing u64
version exactly once. Rejected transitions leave both state and version unchanged.
Version overflow is an explicit error. A future persistence adapter should use this
version as an optimistic-concurrency / compare-and-swap condition, not as a global
ordering across unrelated entities.

### Retry policy

max_attempts includes the initial attempt. A retry is permitted only while the
number of attempts already started is less than max_attempts. The initial and
maximum backoff bounds are validated by the core. The core exposes a deterministic capped exponential backoff calculation; the runtime
will inject jitter and define retryable error classification and deadline interaction.
The first retry uses the initial delay, each subsequent retry doubles it, and the
configured maximum caps the result. Jitter is deliberately not generated in the
domain layer.

### Timeout and cancellation races

Timeout and cancellation are competing terminal outcomes, not independent flags
that can both win. The first valid terminal transition committed against the
current entity version wins; a later competing terminal transition is rejected as
stale/invalid. The domain does not read a wall clock or decide when a deadline has
elapsed. A runtime or persistence adapter must evaluate deadlines and commit the
transition using the entity version as a compare-and-swap guard. Cancellation
requests are not proof that an in-flight external side effect was undone.

### Idempotency keys

`IdempotencyKey` is a distinct validated type for one logical external
operation. The application supplies a stable key and reuses it across retries of
that logical operation. It must not be regenerated per attempt. The key is only
useful if the downstream system enforces deduplication or the application stores
and checks it transactionally; AetherFlow cannot make an arbitrary external API
idempotent by attaching a string.

### Delivery and side effects

The system will not promise universal exactly-once execution. A worker may perform
an external side effect and crash before recording success. Durable scheduling can
therefore redeliver work. Activities that cause external effects must use an
application-supplied idempotency key, a transactional integration, or a
compensating action appropriate to that external system.

The future durable runtime must document its claim/lease, heartbeat, fencing,
outbox/inbox, and crash-window behavior. A state transition in memory is not a
durability guarantee.

## Consequences

- Domain transitions can be tested without clocks, databases, network access, or
  an async runtime.
- Persistence can reject stale writes using per-entity versions.
- Attempt history remains distinct from task-run state.
- Runtime scheduling, jitter, retry classification, and worker ownership are intentionally deferred to later milestones.
- The current implementation is a domain foundation, not yet a scheduler or a
  durable execution engine.
