# Architecture Overview

**Status:** Proposed baseline; validate through implementation.

## Problem model
AetherFlow coordinates user-defined work across unreliable processes and machines. Durable intent must be separated from transient execution. A process-local future is not a durable workflow, and a message acknowledgement does not prove an external side effect happened exactly once.

## Core principles
1. Durable state is authoritative.
2. Workers may disappear; ownership must expire and be safely reassigned.
3. Messages may be duplicated or delayed; handlers must be idempotent or guarded.
4. Arbitrary external side effects cannot be made exactly-once by the orchestrator alone.
5. Bound concurrency, queue depth, retries, execution time, and plugin resources.
6. Make attempts, transitions, errors, and recovery decisions inspectable.
7. Keep domain semantics independent from databases, brokers, and UI.

## Logical layers
- **Domain:** identities, state machines, policies, transition validation, execution contracts.
- **Application:** commands and queries coordinating domain behavior and ports.
- **Runtime:** scheduling, worker lifecycle, cancellation, deadlines, task execution.
- **Adapters:** persistence, transport, HTTP/RPC, telemetry, WASM host integration.
- **Developer surfaces:** CLI, SDKs, examples, dashboard.

Prove domain correctness and single-process execution before adding distributed infrastructure.

## Intended execution flow
1. Client submits a workflow definition and idempotency key.
2. Application validates and creates a run.
3. Scheduler selects eligible tasks under configured limits.
4. Worker claims a task with an expiring lease and records an attempt.
5. Worker executes and reports a result.
6. Application validates and persists the transition.
7. Timeout or worker loss permits recovery according to retry policy.
8. Events and telemetry expose the sequence to operators.

This is a target design, not an implemented guarantee.

## Initial crate boundaries
Candidate crates: `aetherflow-domain`, `aetherflow-application`, `aetherflow-runtime`, `aetherflow-storage-postgres`, `aetherflow-api`, `aetherflow-wasm`, and `aetherflow-cli`. Create crates only when boundaries are useful and testable; avoid empty scaffolding.

## Consistency and delivery
PostgreSQL is the proposed source of truth for durable workflow state. Transitions should be transactional and guarded by expected state/version or equivalent concurrency control. Leases require explicit ownership and expiry. Outbox/inbox patterns may be needed to avoid lost publication windows. NATS JetStream is a transport candidate, not the source of truth.

## Reliability vocabulary
- **At-most-once:** not repeated, but may be lost.
- **At-least-once:** may be repeated until acknowledged or expired.
- **Effectively-once:** duplicates are harmless within a defined idempotency/transaction boundary.
- **Exactly-once:** never claim without naming the precise boundary and assumptions.

## Security boundaries
Workflow definitions and plugin modules are untrusted. Validate payload sizes, constrain resource use, authorize host capabilities, isolate tenants, protect secrets, and avoid logging credentials or sensitive payloads. Design authentication and tenancy before exposing a production control plane.

## WASM plugin boundary (M06 ABI v1)

ADR-0009 accepts the versioned Component Model contract in `crates/aetherflow-wasm/wit/plugin.wit` (`aetherflow:plugin@1.0.0`). The guest exports one task execution function and receives only an opaque payload plus bounded task metadata. The only v1 host import is bounded logging under the explicit `log.write` capability; capability sets default to empty. ABI v1 does not import WASI and has no filesystem, network, environment, secret, process, clock, or database access.

The host owns input/output limits, instruction fuel, host-call transfer fuel, component handle limits, linear memory/table limits, and an actively interruptible wall-clock deadline. Each invocation receives a fresh Store and component instance. Wasmtime 48.0.8 is the selected supported LTS line; engine integration must raise the workspace MSRV to 1.95.0 in the same dependency change. The ABI contract is implemented; the Wasmtime host runtime and adversarial execution tests remain M06 work.

## Initial non-goals
- Universal exactly-once execution across external systems.
- Unrestricted remote code execution.
- Premature Kubernetes/operator complexity.
- A dashboard before execution semantics and APIs stabilize.
- Performance claims without reproducible benchmarks.

## Open questions
Workflow definition/versioning; lease renewal and fencing tokens; event retention; plugin ABI compatibility; tenant isolation and authorization. Resolve these in ADRs before they become hard-to-change dependencies.
