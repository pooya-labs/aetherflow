//! Execution-domain identifiers, lifecycle state machines, and retry contracts.
//!
//! This module is deterministic and infrastructure-independent. It deliberately
//! contains no clocks, I/O, persistence, scheduling, or side-effect execution.

use std::num::NonZeroU32;

use crate::{EntityId, InvalidEntityId};

macro_rules! define_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Eq, Hash, PartialEq)]
        pub struct $name(EntityId);

        impl $name {
            /// Parses a stable identifier using the shared entity-ID rules.
            pub fn parse(value: impl Into<String>) -> Result<Self, InvalidEntityId> {
                EntityId::parse(value).map(Self)
            }

            /// Returns the identifier as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }
    };
}

define_id!(TenantId, "Identifies an owning tenant.");
define_id!(PrincipalId, "Identifies an authenticated tenant principal.");
define_id!(WorkflowId, "Identifies a workflow definition.");
define_id!(
    WorkflowRunId,
    "Identifies one execution of a workflow definition."
);
define_id!(TaskId, "Identifies a task definition within a workflow.");
define_id!(TaskRunId, "Identifies one execution of a task definition.");
define_id!(AttemptId, "Identifies one attempt to execute a task run.");
define_id!(WorkerId, "Identifies a worker process or worker instance.");
define_id!(NodeId, "Identifies a stable logical AetherFlow node.");
define_id!(
    NodeIncarnationId,
    "Identifies one process incarnation of a logical AetherFlow node."
);
define_id!(
    IdempotencyKey,
    "Identifies one logical side effect across delivery attempts."
);

/// Lifecycle state shared by versioned workflow and task executions.
pub trait Lifecycle: Copy + Eq {
    /// Returns whether a direct transition to the supplied state is legal.
    fn allows(self, next: Self) -> bool;

    /// Returns a stable diagnostic name for this state.
    fn name(self) -> &'static str;

    /// Returns whether this state is terminal.
    fn is_terminal(self) -> bool;
}

/// Lifecycle of one workflow execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkflowStatus {
    /// Accepted but not yet started.
    Pending,
    /// At least one task is eligible or executing.
    Running,
    /// All required work completed successfully.
    Succeeded,
    /// The workflow ended with an unrecovered failure.
    Failed,
    /// Cancellation was accepted and finalized.
    Cancelled,
    /// The workflow deadline elapsed.
    TimedOut,
}

impl Lifecycle for WorkflowStatus {
    fn allows(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Pending,
                Self::Running | Self::Cancelled | Self::TimedOut
            ) | (
                Self::Running,
                Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
            )
        )
    }

    fn name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
        }
    }

    fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }
}

/// Lifecycle of one task execution, distinct from its task definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskStatus {
    /// Created but not yet eligible for a worker.
    Pending,
    /// Eligible to be claimed by a worker.
    Ready,
    /// Currently owned by a worker attempt.
    Running,
    /// A failed attempt may be retried after its backoff.
    RetryWaiting,
    /// The task completed successfully.
    Succeeded,
    /// The retry budget was exhausted or failure was not retryable.
    Failed,
    /// The task was cancelled.
    Cancelled,
    /// The task deadline elapsed.
    TimedOut,
}

impl Lifecycle for TaskStatus {
    fn allows(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Pending,
                Self::Ready | Self::Cancelled | Self::TimedOut
            ) | (
                Self::Ready,
                Self::Running | Self::Cancelled | Self::TimedOut
            ) | (
                Self::Running,
                Self::Succeeded
                    | Self::Failed
                    | Self::RetryWaiting
                    | Self::Cancelled
                    | Self::TimedOut
            ) | (
                Self::RetryWaiting,
                Self::Ready | Self::Cancelled | Self::TimedOut
            )
        )
    }

    fn name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::RetryWaiting => "retry_waiting",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
        }
    }

    fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }
}

/// A versioned lifecycle value. The version increases once per accepted
/// transition and can later be used as a persistence compare-and-swap token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Versioned<S> {
    status: S,
    version: u64,
}

/// The result of one accepted state transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Transition<S> {
    /// State before the transition.
    pub from: S,
    /// State after the transition.
    pub to: S,
    /// New version after the transition.
    pub version: u64,
}

/// Why a lifecycle transition was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransitionError {
    /// The state machine does not permit this edge.
    InvalidTransition {
        /// Current state name.
        from: &'static str,
        /// Requested state name.
        to: &'static str,
    },
    /// The monotonically increasing version reached its integer limit.
    VersionOverflow,
}

impl std::fmt::Display for TransitionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTransition { from, to } => {
                write!(formatter, "invalid lifecycle transition: {from} -> {to}")
            }
            Self::VersionOverflow => formatter.write_str("lifecycle version overflow"),
        }
    }
}

impl std::error::Error for TransitionError {}

impl<S: Lifecycle> Versioned<S> {
    /// Creates a lifecycle value at version zero.
    #[must_use]
    pub const fn new(status: S) -> Self {
        Self { status, version: 0 }
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub const fn status(&self) -> S {
        self.status
    }

    /// Returns the current optimistic-concurrency version.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Applies a legal transition and returns its immutable transition record.
    pub fn transition(&mut self, next: S) -> Result<Transition<S>, TransitionError> {
        if !self.status.allows(next) {
            return Err(TransitionError::InvalidTransition {
                from: self.status.name(),
                to: next.name(),
            });
        }

        let version = self
            .version
            .checked_add(1)
            .ok_or(TransitionError::VersionOverflow)?;
        let transition = Transition {
            from: self.status,
            to: next,
            version,
        };
        self.status = next;
        self.version = version;
        Ok(transition)
    }

    /// Returns whether the current lifecycle state is terminal.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}

/// Bounded retry configuration. The maximum includes the initial attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    max_attempts: NonZeroU32,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
}

/// Invalid retry-policy configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryPolicyError {
    /// The initial backoff must not exceed the maximum backoff.
    InitialBackoffExceedsMaximum,
}

impl std::fmt::Display for RetryPolicyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InitialBackoffExceedsMaximum => {
                formatter.write_str("initial backoff must not exceed maximum backoff")
            }
        }
    }
}

impl std::error::Error for RetryPolicyError {}

impl RetryPolicy {
    /// Creates a bounded retry policy. Attempts include the first execution.
    pub fn new(
        max_attempts: NonZeroU32,
        initial_backoff_ms: u64,
        max_backoff_ms: u64,
    ) -> Result<Self, RetryPolicyError> {
        if initial_backoff_ms > max_backoff_ms {
            return Err(RetryPolicyError::InitialBackoffExceedsMaximum);
        }

        Ok(Self {
            max_attempts,
            initial_backoff_ms,
            max_backoff_ms,
        })
    }

    /// Returns the total allowed attempts, including the initial attempt.
    #[must_use]
    pub const fn max_attempts(self) -> NonZeroU32 {
        self.max_attempts
    }

    /// Returns the initial retry delay in milliseconds.
    #[must_use]
    pub const fn initial_backoff_ms(self) -> u64 {
        self.initial_backoff_ms
    }

    /// Returns the maximum retry delay in milliseconds.
    #[must_use]
    pub const fn max_backoff_ms(self) -> u64 {
        self.max_backoff_ms
    }

    /// Returns whether another attempt is allowed after the given number started.
    #[must_use]
    pub const fn permits_retry(self, attempts_started: u32) -> bool {
        attempts_started < self.max_attempts.get()
    }

    /// Returns the capped exponential delay for a one-based retry number.
    ///
    /// Retry number 1 uses the initial delay. Jitter is intentionally left to
    /// the runtime so it can be injected and tested without hidden randomness.
    #[must_use]
    pub const fn backoff_ms(self, retry_number: NonZeroU32) -> u64 {
        let mut delay = self.initial_backoff_ms;
        let mut remaining = retry_number.get() - 1;

        while remaining > 0 && delay < self.max_backoff_ms {
            delay = delay.saturating_mul(2);
            if delay > self.max_backoff_ms {
                delay = self.max_backoff_ms;
            }
            remaining -= 1;
        }

        delay
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AttemptId, IdempotencyKey, Lifecycle, PrincipalId, RetryPolicy, RetryPolicyError, TaskId,
        TaskRunId, TaskStatus, TenantId, TransitionError, Versioned, WorkerId, WorkflowId,
        WorkflowRunId, WorkflowStatus,
    };
    use std::num::NonZeroU32;

    /// Exhaustively explores every target-state sequence up to the supplied depth.
    ///
    /// The edge list is an independent reference model: it checks accepted and
    /// rejected transitions without relying on Lifecycle::allows for the oracle.
    fn explore_transition_sequences<S: Lifecycle + std::fmt::Debug>(
        state: Versioned<S>,
        states: &[S],
        allowed_edges: &[(usize, usize)],
        remaining_steps: usize,
    ) {
        if remaining_steps == 0 {
            return;
        }

        let current = state.status();
        let from_index = states
            .iter()
            .position(|candidate| *candidate == current)
            .expect("every lifecycle state belongs to the reference model");
        let current_version = state.version();
        let was_terminal = state.is_terminal();

        for (to_index, target) in states.iter().copied().enumerate() {
            let expected_allowed = allowed_edges.contains(&(from_index, to_index));
            let mut next = state;
            let result = next.transition(target);

            match result {
                Ok(transition) => {
                    assert!(
                        expected_allowed,
                        "unexpected accepted transition: {} -> {}",
                        current.name(),
                        target.name()
                    );
                    assert!(
                        !was_terminal,
                        "terminal state {} must not transition",
                        current.name()
                    );
                    assert_eq!(transition.from, current);
                    assert_eq!(transition.to, target);
                    assert_eq!(transition.version, current_version + 1);
                    assert_eq!(next.status(), target);
                    assert_eq!(next.version(), current_version + 1);
                }
                Err(error) => {
                    assert!(
                        !expected_allowed,
                        "reference model expected {} -> {} to be accepted",
                        current.name(),
                        target.name()
                    );
                    assert_eq!(
                        error,
                        TransitionError::InvalidTransition {
                            from: current.name(),
                            to: target.name(),
                        }
                    );
                    assert_eq!(next.status(), current);
                    assert_eq!(next.version(), current_version);
                }
            }

            if was_terminal {
                assert_eq!(next.status(), current);
                assert_eq!(next.version(), current_version);
            }

            explore_transition_sequences(next, states, allowed_edges, remaining_steps - 1);
        }
    }

    fn assert_bounded_transition_sequence_invariants<S: Lifecycle + std::fmt::Debug>(
        states: &[S],
        allowed_edges: &[(usize, usize)],
        depth: usize,
    ) {
        for initial in states.iter().copied() {
            explore_transition_sequences(Versioned::new(initial), states, allowed_edges, depth);
        }
    }

    /// Advances a tiny deterministic PRNG so generated failures are reproducible.
    fn next_seeded_value(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    /// Exercises repeatable generated walks, favoring legal edges so retry cycles
    /// are reached while still proposing arbitrary invalid transitions regularly.
    /// The reference edge list remains independent from Lifecycle::allows.
    fn assert_seeded_transition_sequence_invariants<S: Lifecycle + std::fmt::Debug>(
        states: &[S],
        allowed_edges: &[(usize, usize)],
    ) {
        let mut seen_edges = vec![false; allowed_edges.len()];
        let mut accepted = 0;
        let mut rejected = 0;

        for seed in 1..=1_024_u64 {
            let mut random = seed;
            let mut state = Versioned::new(states[0]);

            for _ in 0..64 {
                let value = next_seeded_value(&mut random);
                let current = state.status();
                let from_index = states
                    .iter()
                    .position(|candidate| *candidate == current)
                    .expect("every lifecycle state belongs to the reference model");
                let current_version = state.version();
                let was_terminal = state.is_terminal();
                let legal_target_count = allowed_edges
                    .iter()
                    .filter(|(from, _)| *from == from_index)
                    .count();

                let target_index = if value % 4 != 0 && legal_target_count > 0 {
                    let selected = (value >> 8) as usize % legal_target_count;
                    allowed_edges
                        .iter()
                        .filter(|(from, _)| *from == from_index)
                        .nth(selected)
                        .expect("selected legal edge exists")
                        .1
                } else {
                    ((value >> 32) as usize) % states.len()
                };
                let target = states[target_index];
                let expected_edge = allowed_edges
                    .iter()
                    .position(|edge| *edge == (from_index, target_index));
                let result = state.transition(target);

                match result {
                    Ok(transition) => {
                        let edge_index = expected_edge
                            .expect("generated transition must be present in the reference model");
                        assert!(
                            !was_terminal,
                            "terminal state {} must not transition",
                            current.name()
                        );
                        assert_eq!(transition.from, current);
                        assert_eq!(transition.to, target);
                        assert_eq!(transition.version, current_version + 1);
                        assert_eq!(state.status(), target);
                        assert_eq!(state.version(), current_version + 1);
                        seen_edges[edge_index] = true;
                        accepted += 1;
                    }
                    Err(error) => {
                        assert!(
                            expected_edge.is_none(),
                            "reference model expected {} -> {} to be accepted",
                            current.name(),
                            target.name()
                        );
                        assert_eq!(
                            error,
                            TransitionError::InvalidTransition {
                                from: current.name(),
                                to: target.name(),
                            }
                        );
                        assert_eq!(state.status(), current);
                        assert_eq!(state.version(), current_version);
                        rejected += 1;
                    }
                }

                if was_terminal {
                    assert_eq!(state.status(), current);
                    assert_eq!(state.version(), current_version);
                }
            }
        }

        assert!(accepted > 0, "generated sequences must accept transitions");
        assert!(rejected > 0, "generated sequences must reject invalid transitions");
        for (edge, seen) in allowed_edges.iter().zip(seen_edges) {
            assert!(seen, "generated sequences did not cover edge {edge:?}");
        }
    }

    #[test]
    fn domain_identifiers_are_type_distinct_and_share_validation() {
        let workflow = WorkflowId::parse("workflow_01").expect("valid workflow ID");
        let run = WorkflowRunId::parse("run_01").expect("valid run ID");
        let task = TaskId::parse("task_01").expect("valid task ID");
        let task_run = TaskRunId::parse("task-run_01").expect("valid task run ID");
        let attempt = AttemptId::parse("attempt_01").expect("valid attempt ID");
        let worker = WorkerId::parse("worker_01").expect("valid worker ID");
        let key = IdempotencyKey::parse("payment-order-123").expect("valid idempotency key");

        let tenant = TenantId::parse("tenant_acme").expect("valid tenant ID");

        assert_eq!(tenant.as_str(), "tenant_acme");
        let principal = PrincipalId::parse("principal_ops").expect("valid principal ID");
        assert_eq!(principal.as_str(), "principal_ops");
        assert!(PrincipalId::parse("principal/ops").is_err());
        assert!(TenantId::parse("../tenant").is_err());
        assert_eq!(workflow.as_str(), "workflow_01");
        assert_eq!(run.as_str(), "run_01");
        assert_eq!(task.as_str(), "task_01");
        assert_eq!(task_run.as_str(), "task-run_01");
        assert_eq!(attempt.as_str(), "attempt_01");
        assert_eq!(worker.as_str(), "worker_01");
        assert_eq!(key.as_str(), "payment-order-123");
        assert!(WorkerId::parse("../worker").is_err());
    }

    #[test]
    fn workflow_happy_path_increments_version() {
        let mut state = Versioned::new(WorkflowStatus::Pending);

        let started = state.transition(WorkflowStatus::Running).expect("start");
        let finished = state
            .transition(WorkflowStatus::Succeeded)
            .expect("succeed");

        assert_eq!(started.version, 1);
        assert_eq!(finished.version, 2);
        assert_eq!(state.status(), WorkflowStatus::Succeeded);
        assert!(state.is_terminal());
    }

    #[test]
    fn workflow_terminal_state_rejects_further_transitions() {
        let mut state = Versioned::new(WorkflowStatus::Succeeded);

        assert_eq!(
            state.transition(WorkflowStatus::Running),
            Err(TransitionError::InvalidTransition {
                from: "succeeded",
                to: "running",
            })
        );
        assert_eq!(state.version(), 0);
    }

    #[test]
    fn task_retry_path_returns_to_ready_before_another_attempt() {
        let mut state = Versioned::new(TaskStatus::Pending);

        state.transition(TaskStatus::Ready).expect("make ready");
        state.transition(TaskStatus::Running).expect("claim");
        state
            .transition(TaskStatus::RetryWaiting)
            .expect("schedule retry");
        state
            .transition(TaskStatus::Ready)
            .expect("retry becomes eligible");
        state.transition(TaskStatus::Running).expect("claim retry");
        state
            .transition(TaskStatus::Succeeded)
            .expect("complete retry");

        assert_eq!(state.version(), 6);
        assert_eq!(state.status(), TaskStatus::Succeeded);
    }

    #[test]
    fn task_cannot_succeed_before_it_is_running() {
        let mut state = Versioned::new(TaskStatus::Ready);

        assert!(matches!(
            state.transition(TaskStatus::Succeeded),
            Err(TransitionError::InvalidTransition { .. })
        ));
        assert_eq!(state.status(), TaskStatus::Ready);
        assert_eq!(state.version(), 0);
    }

    #[test]
    fn retry_policy_counts_the_initial_attempt() {
        let policy = RetryPolicy::new(NonZeroU32::new(3).expect("non-zero"), 100, 1_000)
            .expect("valid policy");

        assert!(policy.permits_retry(1));
        assert!(policy.permits_retry(2));
        assert!(!policy.permits_retry(3));
        assert_eq!(policy.max_attempts().get(), 3);
        assert_eq!(
            policy.backoff_ms(NonZeroU32::new(1).expect("non-zero")),
            100
        );
        assert_eq!(
            policy.backoff_ms(NonZeroU32::new(2).expect("non-zero")),
            200
        );
        assert_eq!(
            policy.backoff_ms(NonZeroU32::new(3).expect("non-zero")),
            400
        );
        assert_eq!(
            policy.backoff_ms(NonZeroU32::new(8).expect("non-zero")),
            1_000
        );
    }

    #[test]
    fn retry_policy_rejects_inverted_backoff_bounds() {
        assert_eq!(
            RetryPolicy::new(NonZeroU32::new(2).expect("non-zero"), 500, 100),
            Err(RetryPolicyError::InitialBackoffExceedsMaximum)
        );
    }
    #[test]
    fn workflow_transition_matrix_is_exhaustive() {
        let states = [
            WorkflowStatus::Pending,
            WorkflowStatus::Running,
            WorkflowStatus::Succeeded,
            WorkflowStatus::Failed,
            WorkflowStatus::Cancelled,
            WorkflowStatus::TimedOut,
        ];

        for from in states {
            for to in states {
                let expected = matches!(
                    (from, to),
                    (
                        WorkflowStatus::Pending,
                        WorkflowStatus::Running
                            | WorkflowStatus::Cancelled
                            | WorkflowStatus::TimedOut
                    ) | (
                        WorkflowStatus::Running,
                        WorkflowStatus::Succeeded
                            | WorkflowStatus::Failed
                            | WorkflowStatus::Cancelled
                            | WorkflowStatus::TimedOut
                    )
                );
                assert_eq!(
                    from.allows(to),
                    expected,
                    "{} -> {}",
                    from.name(),
                    to.name()
                );
            }
        }
    }

    #[test]
    fn task_transition_matrix_is_exhaustive() {
        let states = [
            TaskStatus::Pending,
            TaskStatus::Ready,
            TaskStatus::Running,
            TaskStatus::RetryWaiting,
            TaskStatus::Succeeded,
            TaskStatus::Failed,
            TaskStatus::Cancelled,
            TaskStatus::TimedOut,
        ];

        for from in states {
            for to in states {
                let expected = matches!(
                    (from, to),
                    (
                        TaskStatus::Pending,
                        TaskStatus::Ready | TaskStatus::Cancelled | TaskStatus::TimedOut
                    ) | (
                        TaskStatus::Ready,
                        TaskStatus::Running | TaskStatus::Cancelled | TaskStatus::TimedOut
                    ) | (
                        TaskStatus::Running,
                        TaskStatus::Succeeded
                            | TaskStatus::Failed
                            | TaskStatus::RetryWaiting
                            | TaskStatus::Cancelled
                            | TaskStatus::TimedOut
                    ) | (
                        TaskStatus::RetryWaiting,
                        TaskStatus::Ready | TaskStatus::Cancelled | TaskStatus::TimedOut
                    )
                );
                assert_eq!(
                    from.allows(to),
                    expected,
                    "{} -> {}",
                    from.name(),
                    to.name()
                );
            }
        }
    }

    #[test]
    fn first_committed_terminal_outcome_wins_competing_races() {
        let terminal_states = [
            WorkflowStatus::Succeeded,
            WorkflowStatus::Failed,
            WorkflowStatus::Cancelled,
            WorkflowStatus::TimedOut,
        ];

        for first in terminal_states {
            let mut state = Versioned::new(WorkflowStatus::Running);
            state
                .transition(first)
                .expect("first terminal outcome commits");

            for competing in terminal_states {
                assert!(matches!(
                    state.transition(competing),
                    Err(TransitionError::InvalidTransition { .. })
                ));
                assert_eq!(state.status(), first);
                assert_eq!(state.version(), 1);
            }
        }
    }

    #[test]
    fn workflow_bounded_transition_sequences_preserve_state_and_version_invariants() {
        let states = [
            WorkflowStatus::Pending,
            WorkflowStatus::Running,
            WorkflowStatus::Succeeded,
            WorkflowStatus::Failed,
            WorkflowStatus::Cancelled,
            WorkflowStatus::TimedOut,
        ];
        let allowed_edges = [(0, 1), (0, 4), (0, 5), (1, 2), (1, 3), (1, 4), (1, 5)];

        assert_bounded_transition_sequence_invariants(&states, &allowed_edges, 5);
    }

    #[test]
    fn workflow_seeded_sequences_cover_every_transition_edge() {
        let states = [
            WorkflowStatus::Pending,
            WorkflowStatus::Running,
            WorkflowStatus::Succeeded,
            WorkflowStatus::Failed,
            WorkflowStatus::Cancelled,
            WorkflowStatus::TimedOut,
        ];
        let allowed_edges = [(0, 1), (0, 4), (0, 5), (1, 2), (1, 3), (1, 4), (1, 5)];

        assert_seeded_transition_sequence_invariants(&states, &allowed_edges);
    }

    #[test]
    fn task_bounded_transition_sequences_preserve_retry_and_terminal_invariants() {
        let states = [
            TaskStatus::Pending,
            TaskStatus::Ready,
            TaskStatus::Running,
            TaskStatus::RetryWaiting,
            TaskStatus::Succeeded,
            TaskStatus::Failed,
            TaskStatus::Cancelled,
            TaskStatus::TimedOut,
        ];
        let allowed_edges = [
            (0, 1),
            (0, 6),
            (0, 7),
            (1, 2),
            (1, 6),
            (1, 7),
            (2, 3),
            (2, 4),
            (2, 5),
            (2, 6),
            (2, 7),
            (3, 1),
            (3, 6),
            (3, 7),
        ];

        assert_bounded_transition_sequence_invariants(&states, &allowed_edges, 5);
    }

    #[test]
    fn task_seeded_sequences_cover_every_transition_edge_and_retry_cycle() {
        let states = [
            TaskStatus::Pending,
            TaskStatus::Ready,
            TaskStatus::Running,
            TaskStatus::RetryWaiting,
            TaskStatus::Succeeded,
            TaskStatus::Failed,
            TaskStatus::Cancelled,
            TaskStatus::TimedOut,
        ];
        let allowed_edges = [
            (0, 1),
            (0, 6),
            (0, 7),
            (1, 2),
            (1, 6),
            (1, 7),
            (2, 3),
            (2, 4),
            (2, 5),
            (2, 6),
            (2, 7),
            (3, 1),
            (3, 6),
            (3, 7),
        ];

        assert_seeded_transition_sequence_invariants(&states, &allowed_edges);
    }

    #[test]
    fn retry_policy_boundary_grid_preserves_attempt_and_backoff_invariants() {
        let attempt_budgets = [1, 2, 3, 8, 31];
        let initial_delays = [0, 1, 3, 100, u64::MAX / 4, u64::MAX - 1, u64::MAX];
        let maximum_delays = [0, 1, 100, u64::MAX / 2, u64::MAX];

        for max_attempts in attempt_budgets {
            let non_zero_attempts =
                NonZeroU32::new(max_attempts).expect("test attempt budgets are non-zero");
            let policy = RetryPolicy::new(non_zero_attempts, 1, 100).expect("valid retry policy");

            for attempts_started in 0..=max_attempts + 1 {
                assert_eq!(
                    policy.permits_retry(attempts_started),
                    attempts_started < max_attempts,
                    "retry permission mismatch for budget {max_attempts} after {attempts_started} attempts"
                );
            }
        }

        for initial in initial_delays {
            for maximum in maximum_delays {
                let attempts = NonZeroU32::new(3).expect("non-zero attempt budget");
                if initial > maximum {
                    assert_eq!(
                        RetryPolicy::new(attempts, initial, maximum),
                        Err(RetryPolicyError::InitialBackoffExceedsMaximum),
                        "inverted delay bounds must be rejected: {initial} > {maximum}"
                    );
                    continue;
                }

                let policy =
                    RetryPolicy::new(attempts, initial, maximum).expect("valid delay bounds");
                let mut previous = 0;

                for retry_number in 1..=66 {
                    let retry = NonZeroU32::new(retry_number).expect("retry numbers are non-zero");
                    let delay = policy.backoff_ms(retry);

                    assert!(delay >= initial);
                    assert!(delay >= previous, "backoff must never decrease");
                    assert!(delay <= maximum, "backoff must not exceed its cap");
                    if initial > 0 {
                        assert_eq!(
                            delay,
                            if retry_number == 1 {
                                initial
                            } else {
                                previous.saturating_mul(2).min(maximum)
                            },
                            "unexpected capped exponential backoff at retry {retry_number}"
                        );
                    } else {
                        assert_eq!(delay, 0, "zero initial backoff remains zero");
                    }

                    previous = delay;
                }

                if initial > 0 {
                    assert_eq!(previous, maximum, "positive backoff must reach its cap");
                }
            }
        }
    }

    #[test]
    fn version_overflow_rejects_transition_without_mutation() {
        let mut state = Versioned::new(WorkflowStatus::Pending);
        state.version = u64::MAX;

        assert_eq!(
            state.transition(WorkflowStatus::Running),
            Err(TransitionError::VersionOverflow)
        );
        assert_eq!(state.status(), WorkflowStatus::Pending);
        assert_eq!(state.version(), u64::MAX);
    }
}
