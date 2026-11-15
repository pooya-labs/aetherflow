# ADR-0013: Lifecycle state model testing and deferred external property-testing framework

- **Status:** Accepted (maintainer decision recorded)
- **Date:** 2026-11-15
- **Scope:** Verification strategy for core `WorkflowStatus` and `TaskStatus` lifecycle state machines, combinatorial transitions, sequence bounds, and property generation

## Context

In `aetherflow-core`, the execution state machines for workflow runs and task runs govern durable orchestration, state transitions, retry attempts, cancellation, and terminal completion:

1. **Workflow lifecycle:** `Pending -> Running | Cancelled | TimedOut` and `Running -> Succeeded | Failed | Cancelled | TimedOut`. `Succeeded`, `Failed`, `Cancelled`, and `TimedOut` are terminal states that absorb transitions and cannot be mutated or reopened.
2. **Task lifecycle:** `Pending -> Ready | Cancelled | TimedOut`, `Ready -> Running | Cancelled | TimedOut`, `Running -> Succeeded | Failed | RetryWaiting | Cancelled | TimedOut`, and `RetryWaiting -> Ready | Cancelled | TimedOut`.

Milestone M08 required an explicit maintainer decision on whether adopting an external randomized property-based testing framework (e.g. `proptest`, `quickcheck`) is required before v1.0.0, or whether the existing dependency-free deterministic sequence exploration and seeded pseudo-random model walks provide sufficient mathematical and empirical coverage.

## Existing Verification Coverage

The repository provides three complementary layers of deterministic verification in `crates/aetherflow-core/src/domain.rs`:

1. **Combinatorial Transition Matrices:**
   - `workflow_transition_matrix_is_exhaustive()` exercises all $6 \times 6 = 36$ source/destination pairs.
   - `task_transition_matrix_is_exhaustive()` exercises all $7 \times 7 = 49$ source/destination pairs.
   - Proves every allowed edge produces the expected state and increments the version counter by exactly one, while every disallowed edge is rejected, leaves state and version unchanged, and returns `TransitionError`.

2. **Bounded Exhaustive Sequence Exploration:**
   - `workflow_bounded_transition_sequences_preserve_state_and_version_invariants()` exhaustively explores all valid transition paths up to depth 5 from every initial state.
   - `task_bounded_transition_sequences_preserve_retry_and_terminal_invariants()` exhaustively explores all valid task transition paths up to depth 5 from every initial state, explicitly asserting retry-loop boundaries and terminal state absorption.

3. **Deterministic Seeded Model Walks (Merged in PR #18):**
   - `workflow_seeded_sequences_cover_every_transition_edge()` runs 1,024 seeds $\times$ 64 transition steps per seed.
   - `task_seeded_sequences_cover_every_transition_edge_and_retry_cycle()` runs 1,024 seeds $\times$ 64 transition steps per seed.
   - Uses a tiny deterministic PRNG (`next_seeded_value`) to generate repeatable sequences that favor legal edges (75% probability) to explore deep retry cycles, and illegal edges (25% probability) to assert immutability under rejection.
   - Proves that across $1,024 \times 64 = 65,536$ transitions:
     - 100% of defined state machine edges are exercised.
     - Terminal states absorb all subsequent mutations without panic or version drift.
     - Rejected transitions never mutate the version counter or inner status.
     - Test failures are 100% reproducible by fixing the failing seed value.

## Decision

The maintainers decide:

1. **Accept the existing deterministic verification suite as sufficient for v1.0.0.**
   The combination of exhaustive $N \times N$ matrices, depth-5 exhaustive sequence graphs, and 1,024-seed model walks provides complete edge coverage and verifies all required invariants without gaps.

2. **Defer external randomized property-testing frameworks (e.g. `proptest`) beyond v1.0.0.**
   - **Zero dependency overhead:** `aetherflow-core` remains minimal, pure Rust with no external runtime or testing framework dependencies.
   - **Predictable CI:** External property-testing frameworks with unseeded random generation introduce flaky/non-reproducible CI failures across architectures. The checked-in seeded walks guarantee bit-for-bit reproducibility on every platform.
   - **Performance:** The entire test suite in `aetherflow-core` executes in under 5 milliseconds on standard CI runners.

3. **Production Readiness Boundary:**
   Adopting an external property-testing crate is explicitly **not** a blocker for v1.0.0 release readiness. The maintainer decision gate in `ROADMAP.md` and `docs/releases/v1-readiness.md` is formally closed. This does not replace external independent human security review or penetration testing of deployment boundaries.
