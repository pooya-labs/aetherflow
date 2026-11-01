//! Process-local task repository with deterministic scheduling and claim semantics.
//!
//! All operations are synchronous and hold one mutex only for short in-memory
//! mutations. Callers must never hold this lock across an async suspension point.
//! This adapter is intentionally non-durable: a process restart loses its state.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::{Mutex, MutexGuard};

use aetherflow_core::domain::{
    TaskId, TaskRunId, TaskStatus, Transition, TransitionError, Versioned, WorkerId, WorkflowRunId,
};

type TaskRunMap = BTreeMap<String, TaskRun>;

/// One task execution tracked by the single-node runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskRun {
    task_run_id: TaskRunId,
    workflow_run_id: WorkflowRunId,
    task_id: TaskId,
    lifecycle: Versioned<TaskStatus>,
    attempts_started: u32,
    available_at_ms: u64,
    created_at_ms: u64,
    updated_at_ms: u64,
    claimed_by: Option<WorkerId>,
}

impl TaskRun {
    /// Creates a pending task run with an explicit deterministic schedule time.
    #[must_use]
    pub fn new(
        task_run_id: TaskRunId,
        workflow_run_id: WorkflowRunId,
        task_id: TaskId,
        created_at_ms: u64,
        available_at_ms: u64,
    ) -> Self {
        Self {
            task_run_id,
            workflow_run_id,
            task_id,
            lifecycle: Versioned::new(TaskStatus::Pending),
            attempts_started: 0,
            available_at_ms,
            created_at_ms,
            updated_at_ms: created_at_ms,
            claimed_by: None,
        }
    }

    /// Returns this task execution's identifier.
    #[must_use]
    pub fn task_run_id(&self) -> &TaskRunId {
        &self.task_run_id
    }

    /// Returns the parent workflow execution identifier.
    #[must_use]
    pub fn workflow_run_id(&self) -> &WorkflowRunId {
        &self.workflow_run_id
    }

    /// Returns the task definition identifier.
    #[must_use]
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub fn status(&self) -> TaskStatus {
        self.lifecycle.status()
    }

    /// Returns the per-task-run optimistic concurrency version.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.lifecycle.version()
    }

    /// Returns the number of attempts that have been claimed.
    #[must_use]
    pub fn attempts_started(&self) -> u32 {
        self.attempts_started
    }

    /// Returns the earliest monotonic time at which this task may become ready.
    #[must_use]
    pub fn available_at_ms(&self) -> u64 {
        self.available_at_ms
    }

    /// Returns the most recent repository mutation time.
    #[must_use]
    pub fn updated_at_ms(&self) -> u64 {
        self.updated_at_ms
    }

    /// Returns the current local worker owner, if this task is claimed.
    #[must_use]
    pub fn claimed_by(&self) -> Option<&WorkerId> {
        self.claimed_by.as_ref()
    }
}

/// An exclusive local claim for one task execution attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskClaim {
    task_run: TaskRun,
    attempt_number: NonZeroU32,
}

impl TaskClaim {
    /// Returns a snapshot of the claimed task run.
    #[must_use]
    pub fn task_run(&self) -> &TaskRun {
        &self.task_run
    }

    /// Returns the one-based attempt number for this claim.
    #[must_use]
    pub const fn attempt_number(&self) -> NonZeroU32 {
        self.attempt_number
    }
}

/// Repository failures returned by the in-memory adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RepositoryError {
    /// A task run with the same identifier already exists.
    DuplicateTaskRun(String),
    /// The requested task run does not exist.
    TaskRunNotFound(String),
    /// A caller attempted to mutate a stale task-run version.
    StaleVersion {
        /// Version the caller expected to mutate.
        expected: u64,
        /// Current version in the repository.
        actual: u64,
    },
    /// The caller is not the worker currently owning this claim.
    WorkerMismatch(String),
    /// The domain state machine rejected the transition.
    InvalidTransition(TransitionError),
    /// The number of claimed attempts reached the u32 limit.
    AttemptCountOverflow,
    /// A previous panic poisoned the repository mutex.
    LockPoisoned,
}

impl std::fmt::Display for RepositoryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateTaskRun(id) => write!(formatter, "task run already exists: {id}"),
            Self::TaskRunNotFound(id) => write!(formatter, "task run not found: {id}"),
            Self::StaleVersion { expected, actual } => {
                write!(
                    formatter,
                    "stale task-run version: expected {expected}, actual {actual}"
                )
            }
            Self::WorkerMismatch(id) => {
                write!(formatter, "worker does not own task-run claim: {id}")
            }
            Self::InvalidTransition(error) => error.fmt(formatter),
            Self::AttemptCountOverflow => formatter.write_str("task attempt count overflow"),
            Self::LockPoisoned => formatter.write_str("in-memory repository lock is poisoned"),
        }
    }
}

impl std::error::Error for RepositoryError {}

impl From<TransitionError> for RepositoryError {
    fn from(value: TransitionError) -> Self {
        Self::InvalidTransition(value)
    }
}

/// Storage port used by the local scheduler and future persistence adapters.
pub trait TaskRepository: Send + Sync {
    /// Inserts a new pending task run.
    fn insert(&self, task_run: TaskRun) -> Result<(), RepositoryError>;

    /// Returns a snapshot of the requested task run.
    fn get(&self, task_run_id: &TaskRunId) -> Result<TaskRun, RepositoryError>;

    /// Makes due pending/retry-waiting tasks eligible for a worker.
    fn promote_due(&self, now_ms: u64) -> Result<Vec<TaskRunId>, RepositoryError>;

    /// Atomically claims the oldest eligible task for one local worker.
    fn claim_next(
        &self,
        worker_id: &WorkerId,
        now_ms: u64,
    ) -> Result<Option<TaskClaim>, RepositoryError>;

    /// Commits successful completion for the current owner and expected version.
    fn complete(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        now_ms: u64,
    ) -> Result<Transition<TaskStatus>, RepositoryError>;

    /// Commits a permanent task failure for the current owner and expected version.
    fn fail(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        now_ms: u64,
    ) -> Result<Transition<TaskStatus>, RepositoryError>;

    /// Schedules another attempt after a caller-computed backoff deadline.
    fn schedule_retry(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        retry_at_ms: u64,
        now_ms: u64,
    ) -> Result<Transition<TaskStatus>, RepositoryError>;

    /// Commits cancellation against the current expected version.
    fn cancel(
        &self,
        task_run_id: &TaskRunId,
        expected_version: u64,
        now_ms: u64,
    ) -> Result<Transition<TaskStatus>, RepositoryError>;

    /// Commits timeout against the current expected version.
    fn timeout(
        &self,
        task_run_id: &TaskRunId,
        expected_version: u64,
        now_ms: u64,
    ) -> Result<Transition<TaskStatus>, RepositoryError>;
}

/// One-process repository with atomic claims and deterministic task ordering.
///
/// This is a development/test adapter, not durable storage and not a distributed
/// ownership mechanism. It intentionally has no leases or cross-process fencing.
#[derive(Debug, Default)]
pub struct InMemoryRepository {
    tasks: Mutex<TaskRunMap>,
}

impl InMemoryRepository {
    /// Creates an empty in-memory repository.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> Result<MutexGuard<'_, TaskRunMap>, RepositoryError> {
        self.tasks.lock().map_err(|_| RepositoryError::LockPoisoned)
    }
}

fn get_task_mut<'a>(
    tasks: &'a mut TaskRunMap,
    task_run_id: &TaskRunId,
) -> Result<&'a mut TaskRun, RepositoryError> {
    tasks
        .get_mut(task_run_id.as_str())
        .ok_or_else(|| RepositoryError::TaskRunNotFound(task_run_id.as_str().to_owned()))
}

fn verify_version(task: &TaskRun, expected_version: u64) -> Result<(), RepositoryError> {
    let actual = task.version();
    if actual == expected_version {
        Ok(())
    } else {
        Err(RepositoryError::StaleVersion {
            expected: expected_version,
            actual,
        })
    }
}

fn verify_worker(task: &TaskRun, worker_id: &WorkerId) -> Result<(), RepositoryError> {
    if task.claimed_by.as_ref() == Some(worker_id) {
        Ok(())
    } else {
        Err(RepositoryError::WorkerMismatch(
            task.task_run_id.as_str().to_owned(),
        ))
    }
}

impl TaskRepository for InMemoryRepository {
    fn insert(&self, task_run: TaskRun) -> Result<(), RepositoryError> {
        let mut tasks = self.lock()?;
        let key = task_run.task_run_id.as_str().to_owned();
        if tasks.contains_key(&key) {
            return Err(RepositoryError::DuplicateTaskRun(key));
        }
        tasks.insert(key, task_run);
        Ok(())
    }

    fn get(&self, task_run_id: &TaskRunId) -> Result<TaskRun, RepositoryError> {
        let tasks = self.lock()?;
        tasks
            .get(task_run_id.as_str())
            .cloned()
            .ok_or_else(|| RepositoryError::TaskRunNotFound(task_run_id.as_str().to_owned()))
    }

    fn promote_due(&self, now_ms: u64) -> Result<Vec<TaskRunId>, RepositoryError> {
        let mut tasks = self.lock()?;
        let mut promoted = Vec::new();

        for task in tasks.values_mut() {
            if task.available_at_ms > now_ms {
                continue;
            }

            if matches!(
                task.status(),
                TaskStatus::Pending | TaskStatus::RetryWaiting
            ) {
                task.lifecycle.transition(TaskStatus::Ready)?;
                task.updated_at_ms = now_ms;
                promoted.push(task.task_run_id.clone());
            }
        }

        Ok(promoted)
    }

    fn claim_next(
        &self,
        worker_id: &WorkerId,
        now_ms: u64,
    ) -> Result<Option<TaskClaim>, RepositoryError> {
        let mut tasks = self.lock()?;
        let candidate_key = tasks
            .iter()
            .filter(|(_, task)| task.status() == TaskStatus::Ready && task.claimed_by.is_none())
            .min_by(|(_, left), (_, right)| {
                left.available_at_ms
                    .cmp(&right.available_at_ms)
                    .then_with(|| left.created_at_ms.cmp(&right.created_at_ms))
                    .then_with(|| left.task_run_id.as_str().cmp(right.task_run_id.as_str()))
            })
            .map(|(key, _)| key.clone());

        let Some(candidate_key) = candidate_key else {
            return Ok(None);
        };

        let task = tasks
            .get_mut(&candidate_key)
            .ok_or_else(|| RepositoryError::TaskRunNotFound(candidate_key.clone()))?;
        let attempts_started = task
            .attempts_started
            .checked_add(1)
            .ok_or(RepositoryError::AttemptCountOverflow)?;
        let attempt_number =
            NonZeroU32::new(attempts_started).ok_or(RepositoryError::AttemptCountOverflow)?;

        task.lifecycle.transition(TaskStatus::Running)?;
        task.attempts_started = attempts_started;
        task.claimed_by = Some(worker_id.clone());
        task.updated_at_ms = now_ms;

        Ok(Some(TaskClaim {
            task_run: task.clone(),
            attempt_number,
        }))
    }

    fn complete(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        now_ms: u64,
    ) -> Result<Transition<TaskStatus>, RepositoryError> {
        let mut tasks = self.lock()?;
        let task = get_task_mut(&mut tasks, task_run_id)?;
        verify_version(task, expected_version)?;
        verify_worker(task, worker_id)?;

        let transition = task.lifecycle.transition(TaskStatus::Succeeded)?;
        task.claimed_by = None;
        task.updated_at_ms = now_ms;
        Ok(transition)
    }

    fn fail(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        now_ms: u64,
    ) -> Result<Transition<TaskStatus>, RepositoryError> {
        let mut tasks = self.lock()?;
        let task = get_task_mut(&mut tasks, task_run_id)?;
        verify_version(task, expected_version)?;
        verify_worker(task, worker_id)?;

        let transition = task.lifecycle.transition(TaskStatus::Failed)?;
        task.claimed_by = None;
        task.updated_at_ms = now_ms;
        Ok(transition)
    }

    fn schedule_retry(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        retry_at_ms: u64,
        now_ms: u64,
    ) -> Result<Transition<TaskStatus>, RepositoryError> {
        let mut tasks = self.lock()?;
        let task = get_task_mut(&mut tasks, task_run_id)?;
        verify_version(task, expected_version)?;
        verify_worker(task, worker_id)?;

        let transition = task.lifecycle.transition(TaskStatus::RetryWaiting)?;
        task.available_at_ms = retry_at_ms;
        task.claimed_by = None;
        task.updated_at_ms = now_ms;
        Ok(transition)
    }

    fn cancel(
        &self,
        task_run_id: &TaskRunId,
        expected_version: u64,
        now_ms: u64,
    ) -> Result<Transition<TaskStatus>, RepositoryError> {
        let mut tasks = self.lock()?;
        let task = get_task_mut(&mut tasks, task_run_id)?;
        verify_version(task, expected_version)?;

        let transition = task.lifecycle.transition(TaskStatus::Cancelled)?;
        task.claimed_by = None;
        task.updated_at_ms = now_ms;
        Ok(transition)
    }

    fn timeout(
        &self,
        task_run_id: &TaskRunId,
        expected_version: u64,
        now_ms: u64,
    ) -> Result<Transition<TaskStatus>, RepositoryError> {
        let mut tasks = self.lock()?;
        let task = get_task_mut(&mut tasks, task_run_id)?;
        verify_version(task, expected_version)?;

        let transition = task.lifecycle.transition(TaskStatus::TimedOut)?;
        task.claimed_by = None;
        task.updated_at_ms = now_ms;
        Ok(transition)
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemoryRepository, RepositoryError, TaskRepository, TaskRun};
    use aetherflow_core::domain::{TaskId, TaskRunId, TaskStatus, WorkerId, WorkflowRunId};
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn task_run(id: &str, created_at_ms: u64, available_at_ms: u64) -> TaskRun {
        TaskRun::new(
            TaskRunId::parse(id).expect("valid task-run ID"),
            WorkflowRunId::parse("workflow-run-01").expect("valid workflow-run ID"),
            TaskId::parse("task-01").expect("valid task ID"),
            created_at_ms,
            available_at_ms,
        )
    }

    fn worker(id: &str) -> WorkerId {
        WorkerId::parse(id).expect("valid worker ID")
    }

    #[test]
    fn insert_rejects_duplicate_task_run_ids() {
        let repository = InMemoryRepository::new();
        let task = task_run("task-run-01", 0, 0);
        repository.insert(task.clone()).expect("insert task");

        assert_eq!(
            repository.insert(task),
            Err(RepositoryError::DuplicateTaskRun("task-run-01".to_owned()))
        );
    }

    #[test]
    fn due_tasks_become_ready_only_at_the_scheduled_time() {
        let repository = InMemoryRepository::new();
        let task = task_run("task-run-01", 10, 50);
        let id = task.task_run_id().clone();
        repository.insert(task).expect("insert task");

        assert!(
            repository
                .promote_due(49)
                .expect("promote early")
                .is_empty()
        );
        assert_eq!(
            repository.get(&id).expect("read task").status(),
            TaskStatus::Pending
        );
        assert_eq!(
            repository.promote_due(50).expect("promote due"),
            vec![id.clone()]
        );
        assert_eq!(
            repository.get(&id).expect("read ready task").status(),
            TaskStatus::Ready
        );
    }

    #[test]
    fn claims_are_exclusive_and_deterministically_ordered() {
        let repository = InMemoryRepository::new();
        repository
            .insert(task_run("task-run-b", 10, 10))
            .expect("insert b");
        repository
            .insert(task_run("task-run-a", 10, 10))
            .expect("insert a");
        repository.promote_due(10).expect("promote due");

        let first = repository
            .claim_next(&worker("worker-01"), 11)
            .expect("claim first")
            .expect("first task exists");
        let second = repository
            .claim_next(&worker("worker-02"), 12)
            .expect("claim second")
            .expect("second task exists");
        let third = repository
            .claim_next(&worker("worker-03"), 13)
            .expect("claim third");

        assert_eq!(first.task_run().task_run_id().as_str(), "task-run-a");
        assert_eq!(second.task_run().task_run_id().as_str(), "task-run-b");
        assert!(third.is_none());
        assert_eq!(first.attempt_number().get(), 1);
        assert_eq!(second.attempt_number().get(), 1);
    }

    #[test]
    fn retry_waiting_task_returns_to_ready_and_increments_attempt() {
        let repository = InMemoryRepository::new();
        let task = task_run("task-run-01", 0, 0);
        let id = task.task_run_id().clone();
        repository.insert(task).expect("insert task");
        repository.promote_due(0).expect("promote initial task");

        let first_claim = repository
            .claim_next(&worker("worker-01"), 1)
            .expect("first claim")
            .expect("task ready");
        let first_version = first_claim.task_run().version();
        repository
            .schedule_retry(&id, &worker("worker-01"), first_version, 100, 20)
            .expect("schedule retry");

        assert!(repository.promote_due(99).expect("too early").is_empty());
        assert_eq!(
            repository.promote_due(100).expect("retry due"),
            vec![id.clone()]
        );

        let second_claim = repository
            .claim_next(&worker("worker-02"), 101)
            .expect("second claim")
            .expect("retry ready");
        assert_eq!(second_claim.attempt_number().get(), 2);
        assert_eq!(second_claim.task_run().attempts_started(), 2);
    }

    #[test]
    fn stale_versions_and_wrong_workers_cannot_complete_a_task() {
        let repository = InMemoryRepository::new();
        let task = task_run("task-run-01", 0, 0);
        let id = task.task_run_id().clone();
        repository.insert(task).expect("insert task");
        repository.promote_due(0).expect("promote task");

        let claim = repository
            .claim_next(&worker("worker-01"), 1)
            .expect("claim")
            .expect("task ready");
        let expected_version = claim.task_run().version();

        assert_eq!(
            repository.complete(&id, &worker("worker-02"), expected_version, 2),
            Err(RepositoryError::WorkerMismatch("task-run-01".to_owned()))
        );
        repository
            .complete(&id, &worker("worker-01"), expected_version, 3)
            .expect("complete");
        assert_eq!(
            repository.complete(&id, &worker("worker-01"), expected_version, 4),
            Err(RepositoryError::StaleVersion {
                expected: expected_version,
                actual: expected_version + 1,
            })
        );
        assert_eq!(
            repository.get(&id).expect("read").status(),
            TaskStatus::Succeeded
        );
    }

    #[test]
    fn cancellation_wins_against_later_worker_completion() {
        let repository = InMemoryRepository::new();
        let task = task_run("task-run-01", 0, 0);
        let id = task.task_run_id().clone();
        repository.insert(task).expect("insert task");
        repository.promote_due(0).expect("promote task");

        let claim = repository
            .claim_next(&worker("worker-01"), 1)
            .expect("claim")
            .expect("task ready");
        let version = claim.task_run().version();

        repository.cancel(&id, version, 2).expect("cancel task");
        assert_eq!(
            repository.complete(&id, &worker("worker-01"), version, 3),
            Err(RepositoryError::StaleVersion {
                expected: version,
                actual: version + 1,
            })
        );
        assert_eq!(
            repository.get(&id).expect("read").status(),
            TaskStatus::Cancelled
        );
    }

    #[test]
    fn concurrent_workers_cannot_claim_the_same_task_run() {
        let repository = Arc::new(InMemoryRepository::new());
        let task = task_run("task-run-01", 0, 0);
        repository.insert(task).expect("insert task");
        repository.promote_due(0).expect("promote task");

        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();

        for worker_id in ["worker-01", "worker-02"] {
            let repository = Arc::clone(&repository);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let worker = worker(worker_id);
                barrier.wait();
                repository
                    .claim_next(&worker, 1)
                    .expect("claim from concurrent worker")
                    .is_some()
            }));
        }

        barrier.wait();
        let claims = handles
            .into_iter()
            .map(|handle| handle.join().expect("join worker"))
            .filter(|claimed| *claimed)
            .count();

        assert_eq!(claims, 1);
    }

    #[test]
    fn timeout_and_terminal_transitions_are_version_guarded() {
        let repository = InMemoryRepository::new();
        let task = task_run("task-run-01", 0, 10);
        let id = task.task_run_id().clone();
        repository.insert(task).expect("insert task");

        let timeout = repository
            .timeout(&id, 0, 10)
            .expect("time out pending task");
        assert_eq!(timeout.to, TaskStatus::TimedOut);
        assert_eq!(
            repository.cancel(&id, 0, 11),
            Err(RepositoryError::StaleVersion {
                expected: 0,
                actual: 1,
            })
        );
    }
}
