# AetherFlow — Master Roadmap

This file is the **source of truth** for project progress. Update it in the same change as meaningful implementation work. If a chat or network session is interrupted, resume from **Current checkpoint** and the first unchecked task in the active phase.

Last updated: 2026-11-02  
Current branch: `main`  
Current milestone: **M00 — Project charter and architecture baseline**  
Overall status: **In progress**

## Progress rules

- [x] Create this persistent roadmap in the repository.
- [ ] A task is checked only after the artifact exists and its verification has passed.
- [ ] Record exact commands and outcomes; never mark an unrun test as passing.
- [ ] Keep this file current at every milestone boundary and before ending a work session.
- [ ] Prefer small, reviewable commits; do not claim a feature is complete because scaffolding exists.
- [ ] Record blockers, assumptions, and manual actions explicitly.

## Milestones

### M00 — Project charter and architecture baseline
- [x] Confirm public repository and write access.
- [x] Define project purpose, non-goals, and initial technology direction.
- [x] Establish this persistent roadmap and recovery protocol.
- [ ] Add architecture overview and initial ADRs.
- [ ] Add license, contribution, and security policy.
- [ ] Review naming, scope, and first implementation boundary.

### M01 — Rust workspace foundation
- [ ] Add pinned Rust toolchain and workspace manifest.
- [ ] Establish crate boundaries for domain, application, and adapters.
- [ ] Add formatting, linting, and test conventions.
- [ ] Add CI for formatting, Clippy, unit tests, and documentation.
- [ ] Add a minimal executable and health endpoint.
- [ ] Verify clean build from a fresh checkout.

### M02 — Execution domain and state machine
- [ ] Define workflow, run, task, attempt, and worker identities.
- [ ] Specify lifecycle states and legal transitions.
- [ ] Define retry, timeout, cancellation, and idempotency contracts.
- [ ] Implement deterministic transition tests and property-based tests.
- [ ] Document failure semantics and invariants.

### M03 — Single-node execution runtime
- [ ] Implement in-memory repository for fast deterministic tests.
- [ ] Implement bounded async scheduler and worker loop.
- [ ] Add task registration and workflow execution API.
- [ ] Add concurrency limits, deadlines, cancellation, and retry policy.
- [ ] Add integration tests for success and failure paths.

### M04 — Durable persistence
- [ ] Design PostgreSQL schema and migration strategy.
- [ ] Implement transactional state transitions.
- [ ] Implement claim/lease/heartbeat semantics.
- [ ] Implement recovery after process restart.
- [ ] Add crash-window and duplicate-delivery tests.

### M05 — Distributed coordination and event delivery
- [ ] Define node membership and worker ownership model.
- [ ] Add transactional outbox/inbox where required.
- [ ] Integrate NATS JetStream only after transport-independent contracts stabilize.
- [ ] Test redelivery, lease expiry, partition, and worker loss.
- [ ] Document delivery guarantees and operational limits.

### M06 — WASM extension runtime
- [ ] Define versioned plugin ABI and capability model.
- [ ] Integrate Wasmtime with fuel, memory, and execution limits.
- [ ] Add host-call authorization and deterministic error mapping.
- [ ] Test traps, resource exhaustion, and untrusted modules.
- [ ] Publish a minimal plugin example.

### M07 — Control plane and developer experience
- [ ] Expose versioned HTTP API and OpenAPI contract.
- [ ] Add CLI for inspect, submit, cancel, and worker status.
- [ ] Add Rust SDK; add TypeScript SDK only when API stabilizes.
- [ ] Build dashboard for workflows, runs, tasks, and event history.
- [ ] Add authentication, authorization, and tenant isolation design.

### M08 — Production engineering
- [ ] Add structured logs, metrics, and distributed tracing.
- [ ] Add Docker Compose development environment.
- [ ] Add backup/restore and schema upgrade runbooks.
- [ ] Add load and reproducible benchmark suites.
- [ ] Add chaos/failure-injection scenarios.
- [ ] Add dependency, license, secret, and supply-chain checks.
- [ ] Publish threat model, SLO proposal, and operational guide.

### M09 — Release candidate
- [ ] Complete end-to-end acceptance suite.
- [ ] Verify clean install and documented quickstart.
- [ ] Review API stability and compatibility policy.
- [ ] Publish limitations and known issues.
- [ ] Tag a version only after release criteria pass.

## Current checkpoint

**Completed:** Repository confirmed; initial project charter and roadmap committed.  
**In progress:** M00 architecture baseline.  
**Next action:** Add architecture overview and ADR-0001, then project policies.  
**Verification:** Repository content writes succeeded. No Rust build or tests exist yet.  
**Blockers:** None known.  
**Manual action required from owner:** None at this checkpoint.

## Session log

| Date | Milestone | Result |
|---|---|---|
| 2026-11-02 | M00 | Repository verified; charter and persistent roadmap created |

## Resume instructions

1. Open this file on the default branch.
2. Read the **Current checkpoint** section.
3. Inspect the latest commit and working milestone artifacts.
4. Continue with the first unchecked task; do not repeat completed work.
5. Update this file with verified outcomes before stopping.
