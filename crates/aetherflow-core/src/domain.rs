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
        AttemptId, IdempotencyKey, Lifecycle, RetryPolicy, RetryPolicyError, TaskId, TaskRunId,
        TaskStatus, TransitionError, Versioned, WorkerId, WorkflowId, WorkflowRunId,
        WorkflowStatus,
    };
    use std::num::NonZeroU32;

    #[test]
    fn domain_identifiers_are_type_distinct_and_share_validation() {
        let workflow = WorkflowId::parse("workflow_01").expect("valid workflow ID");
        let run = WorkflowRunId::parse("run_01").expect("valid run ID");
        let task = TaskId::parse("task_01").expect("valid task ID");
        let task_run = TaskRunId::parse("task-run_01").expect("valid task run ID");
        let attempt = AttemptId::parse("attempt_01").expect("valid attempt ID");
        let worker = WorkerId::parse("worker_01").expect("valid worker ID");
        let key = IdempotencyKey::parse("payment-order-123").expect("valid idempotency key");

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
