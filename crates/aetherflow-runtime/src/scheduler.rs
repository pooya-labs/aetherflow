//! Bounded asynchronous task scheduler and local worker loop.

use std::collections::HashMap;
use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use aetherflow_core::domain::{RetryPolicy, TaskId, TaskRunId, TaskStatus, Transition, WorkerId};
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::clock::Clock;
use crate::repository::{RepositoryError, TaskClaim, TaskRepository, TaskRun};

/// Boxed asynchronous task-handler future.
pub type TaskFuture =
    Pin<Box<dyn Future<Output = Result<(), TaskExecutionError>> + Send + 'static>>;

/// Executes one claimed task attempt.
///
/// Implementations receive an immutable snapshot. They must make external side
/// effects idempotent when retries are possible; the scheduler does not promise
/// exactly-once execution.
pub trait TaskHandler: Send + Sync + 'static {
    /// Executes the claimed attempt.
    fn execute(&self, claim: TaskClaim) -> TaskFuture;
}

impl<F, Fut> TaskHandler for F
where
    F: Fn(TaskClaim) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), TaskExecutionError>> + Send + 'static,
{
    fn execute(&self, claim: TaskClaim) -> TaskFuture {
        Box::pin((self)(claim))
    }
}

/// A task-handler failure classified for retry decisions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskExecutionError {
    /// A transient failure that may be retried while the attempt budget remains.
    Retryable(String),
    /// A permanent failure that must not be retried.
    Permanent(String),
}

impl TaskExecutionError {
    /// Creates a retryable failure with a diagnostic message.
    #[must_use]
    pub fn retryable(message: impl Into<String>) -> Self {
        Self::Retryable(message.into())
    }

    /// Creates a permanent failure with a diagnostic message.
    #[must_use]
    pub fn permanent(message: impl Into<String>) -> Self {
        Self::Permanent(message.into())
    }

    fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

impl std::fmt::Display for TaskExecutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable(message) => write!(formatter, "retryable task failure: {message}"),
            Self::Permanent(message) => write!(formatter, "permanent task failure: {message}"),
        }
    }
}

impl std::error::Error for TaskExecutionError {}

/// Registry validation failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskRegistryError {
    /// A handler is already registered for the task identifier.
    DuplicateHandler(String),
}

impl std::fmt::Display for TaskRegistryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateHandler(task_id) => {
                write!(formatter, "task handler already registered: {task_id}")
            }
        }
    }
}

impl std::error::Error for TaskRegistryError {}

/// Immutable mapping from task definition identifiers to execution handlers.
#[derive(Default)]
pub struct TaskRegistry {
    handlers: HashMap<String, Arc<dyn TaskHandler>>,
}

impl TaskRegistry {
    /// Creates an empty task-handler registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a handler for a task definition.
    ///
    /// Duplicate registration is rejected instead of silently replacing a handler.
    pub fn register<H>(
        &mut self,
        task_id: TaskId,
        handler: H,
    ) -> Result<(), TaskRegistryError>
    where
        H: TaskHandler,
    {
        let key = task_id.as_str().to_owned();
        if self.handlers.contains_key(&key) {
            return Err(TaskRegistryError::DuplicateHandler(key));
        }

        self.handlers.insert(key, Arc::new(handler));
        Ok(())
    }

    fn handler_for(&self, task_id: &TaskId) -> Option<Arc<dyn TaskHandler>> {
        self.handlers.get(task_id.as_str()).cloned()
    }
}

/// Configuration validation errors for the scheduler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerConfigError {
    /// The scheduler poll interval must be greater than zero.
    ZeroPollInterval,
}

impl std::fmt::Display for SchedulerConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroPollInterval => {
                formatter.write_str("scheduler poll interval must be non-zero")
            }
        }
    }
}

impl std::error::Error for SchedulerConfigError {}

/// Runtime limits and retry behavior for a local scheduler.
#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    max_concurrency: NonZeroUsize,
    poll_interval: Duration,
    task_timeout: Option<Duration>,
    retry_policy: RetryPolicy,
}

impl SchedulerConfig {
    /// Creates scheduler configuration with a non-zero poll interval.
    pub fn new(
        max_concurrency: NonZeroUsize,
        poll_interval: Duration,
        task_timeout: Option<Duration>,
        retry_policy: RetryPolicy,
    ) -> Result<Self, SchedulerConfigError> {
        if poll_interval.is_zero() {
            return Err(SchedulerConfigError::ZeroPollInterval);
        }

        Ok(Self {
            max_concurrency,
            poll_interval,
            task_timeout,
            retry_policy,
        })
    }

    /// Returns the maximum number of in-flight task attempts.
    #[must_use]
    pub const fn max_concurrency(&self) -> NonZeroUsize {
        self.max_concurrency
    }

    /// Returns the interval used while waiting for due tasks.
    #[must_use]
    pub const fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Returns the optional per-attempt timeout.
    #[must_use]
    pub const fn task_timeout(&self) -> Option<Duration> {
        self.task_timeout
    }

    /// Returns the retry policy applied to handler failures.
    #[must_use]
    pub const fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy
    }
}

/// Scheduler failures that require the worker loop to stop.
#[derive(Debug)]
pub enum SchedulerError {
    /// The repository returned an unexpected error.
    Repository(RepositoryError),
    /// A worker task panicked or was aborted outside normal scheduler control.
    WorkerTaskPanicked(String),
}

impl std::fmt::Display for SchedulerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Repository(error) => error.fmt(formatter),
            Self::WorkerTaskPanicked(message) => {
                write!(formatter, "worker task terminated unexpectedly: {message}")
            }
        }
    }
}

impl std::error::Error for SchedulerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Repository(error) => Some(error),
            Self::WorkerTaskPanicked(_) => None,
        }
    }
}

impl From<RepositoryError> for SchedulerError {
    fn from(value: RepositoryError) -> Self {
        Self::Repository(value)
    }
}

/// Single-process async scheduler backed by a task repository.
///
/// Concurrency is bounded by the number of active tasks in the join set.
/// Shutdown stops new claims and waits for all currently running handlers.
pub struct TaskScheduler<R: TaskRepository + 'static> {
    repository: Arc<R>,
    clock: Arc<dyn Clock>,
    worker_id: WorkerId,
    registry: Arc<TaskRegistry>,
    config: SchedulerConfig,
}

impl<R: TaskRepository + 'static> TaskScheduler<R> {
    /// Creates a scheduler using a fixed handler registry and worker identity.
    #[must_use]
    pub fn new(
        repository: Arc<R>,
        clock: Arc<dyn Clock>,
        worker_id: WorkerId,
        registry: TaskRegistry,
        config: SchedulerConfig,
    ) -> Self {
        Self {
            repository,
            clock,
            worker_id,
            registry: Arc::new(registry),
            config,
        }
    }

    /// Inserts a task run into the backing repository.
    pub fn submit_task_run(&self, task_run: TaskRun) -> Result<(), SchedulerError> {
        self.repository.insert(task_run)?;
        Ok(())
    }

    /// Returns a snapshot of a task run.
    pub fn get_task_run(&self, task_run_id: &TaskRunId) -> Result<TaskRun, SchedulerError> {
        Ok(self.repository.get(task_run_id)?)
    }

    /// Cancels a task run if its current version matches the caller's expectation.
    pub fn cancel_task_run(
        &self,
        task_run_id: &TaskRunId,
        expected_version: u64,
    ) -> Result<Transition<TaskStatus>, SchedulerError> {
        Ok(self
            .repository
            .cancel(task_run_id, expected_version, self.clock.now_ms())?)
    }

    /// Runs the worker loop until shutdown is signaled.
    ///
    /// Shutdown stops additional claims and drains active handlers before returning.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), SchedulerError> {
        let mut active_tasks: JoinSet<Result<(), SchedulerError>> = JoinSet::new();
        let mut stop_claiming = *shutdown.borrow_and_update();

        loop {
            if !stop_claiming {
                self.repository.promote_due(self.clock.now_ms())?;

                while active_tasks.len() < self.config.max_concurrency.get() {
                    if *shutdown.borrow() {
                        stop_claiming = true;
                        break;
                    }

                    let Some(claim) = self
                        .repository
                        .claim_next(&self.worker_id, self.clock.now_ms())?
                    else {
                        break;
                    };

                    let handler = self.registry.handler_for(claim.task_run().task_id());
                    let repository = Arc::clone(&self.repository);
                    let clock = Arc::clone(&self.clock);
                    let worker_id = self.worker_id.clone();
                    let config = self.config.clone();

                    active_tasks.spawn(async move {
                        execute_claim(repository, clock, worker_id, config, claim, handler).await
                    });
                }
            }

            if stop_claiming && active_tasks.is_empty() {
                return Ok(());
            }

            tokio::select! {
                changed = shutdown.changed(), if !stop_claiming => {
                    stop_claiming = changed.is_err() || *shutdown.borrow_and_update();
                }
                joined = active_tasks.join_next(), if !active_tasks.is_empty() => {
                    match joined {
                        Some(Ok(result)) => result?,
                        Some(Err(error)) => {
                            return Err(SchedulerError::WorkerTaskPanicked(error.to_string()));
                        }
                        None => {}
                    }
                }
                _ = tokio::time::sleep(self.config.poll_interval), if !stop_claiming => {}
            }
        }
    }
}

async fn execute_claim<R: TaskRepository + 'static>(
    repository: Arc<R>,
    clock: Arc<dyn Clock>,
    worker_id: WorkerId,
    config: SchedulerConfig,
    claim: TaskClaim,
    handler: Option<Arc<dyn TaskHandler>>,
) -> Result<(), SchedulerError> {
    let task_run_id = claim.task_run().task_run_id().clone();
    let task_id = claim.task_run().task_id().clone();
    let expected_version = claim.task_run().version();
    let attempt_number = claim.attempt_number();

    let future: TaskFuture = match handler {
        Some(handler) => handler.execute(claim),
        None => {
            let message = format!("no handler registered for task '{}'", task_id.as_str());
            Box::pin(async move { Err(TaskExecutionError::permanent(message)) })
        }
    };

    let outcome = match config.task_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, future).await {
            Ok(result) => Some(result),
            Err(_) => None,
        },
        None => Some(future.await),
    };

    let now_ms = clock.now_ms();
    match outcome {
        Some(Ok(())) => settle(repository.complete(
            &task_run_id,
            &worker_id,
            expected_version,
            now_ms,
        )),
        Some(Err(error)) => {
            eprintln!("task run {} failed: {error}", task_run_id.as_str());

            if error.is_retryable() && config.retry_policy.permits_retry(attempt_number.get()) {
                let delay_ms = config.retry_policy.backoff_ms(attempt_number);
                let retry_at_ms = now_ms.saturating_add(delay_ms);
                settle(repository.schedule_retry(
                    &task_run_id,
                    &worker_id,
                    expected_version,
                    retry_at_ms,
                    now_ms,
                ))
            } else {
                settle(repository.fail(
                    &task_run_id,
                    &worker_id,
                    expected_version,
                    now_ms,
                ))
            }
        }
        None => settle(repository.timeout(
            &task_run_id,
            expected_version,
            now_ms,
        )),
    }
}

fn settle<T>(result: Result<T, RepositoryError>) -> Result<(), SchedulerError> {
    match result {
        Ok(_) | Err(RepositoryError::StaleVersion { .. }) => Ok(()),
        Err(error) => Err(SchedulerError::Repository(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::{SchedulerConfig, TaskExecutionError, TaskRegistry, TaskScheduler};
    use crate::clock::MonotonicClock;
    use crate::repository::{InMemoryRepository, TaskRun};
    use aetherflow_core::domain::{
        RetryPolicy, TaskId, TaskRunId, TaskStatus, WorkerId, WorkflowRunId,
    };
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::watch;

    fn task_run(task_run_id: &str) -> TaskRun {
        TaskRun::new(
            TaskRunId::parse(task_run_id).expect("valid task run ID"),
            WorkflowRunId::parse("workflow-run-01").expect("valid workflow run ID"),
            TaskId::parse("task-01").expect("valid task ID"),
            0,
            0,
        )
    }

    fn task_id() -> TaskId {
        TaskId::parse("task-01").expect("valid task ID")
    }

    fn worker_id() -> WorkerId {
        WorkerId::parse("worker-01").expect("valid worker ID")
    }

    fn retry_policy(max_attempts: u32) -> RetryPolicy {
        RetryPolicy::new(
            NonZeroU32::new(max_attempts).expect("non-zero attempts"),
            0,
            0,
        )
        .expect("valid retry policy")
    }

    fn config(max_concurrency: usize, timeout: Option<Duration>, attempts: u32) -> SchedulerConfig {
        SchedulerConfig::new(
            NonZeroUsize::new(max_concurrency).expect("non-zero concurrency"),
            Duration::from_millis(1),
            timeout,
            retry_policy(attempts),
        )
        .expect("valid scheduler config")
    }

    async fn wait_for_status(
        repository: &InMemoryRepository,
        id: &TaskRunId,
        expected: TaskStatus,
    ) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if repository.get(id).expect("read task run").status() == expected {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("task reached expected state");
    }

    #[tokio::test]
    async fn executes_registered_handler_and_marks_task_succeeded() {
        let repository = Arc::new(InMemoryRepository::new());
        let mut registry = TaskRegistry::new();
        registry
            .register(task_id(), |_claim| async {
                Ok::<(), TaskExecutionError>(())
            })
            .expect("register handler");
        let scheduler = TaskScheduler::new(
            Arc::clone(&repository),
            Arc::new(MonotonicClock::new()),
            worker_id(),
            registry,
            config(1, None, 1),
        );
        let task = task_run("task-run-01");
        let id = task.task_run_id().clone();
        scheduler.submit_task_run(task).expect("submit task run");

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { scheduler.run(shutdown_rx).await });
        wait_for_status(&repository, &id, TaskStatus::Succeeded).await;
        shutdown_tx.send(true).expect("signal shutdown");
        handle.await.expect("join scheduler").expect("scheduler run");
    }

    #[tokio::test]
    async fn retries_retryable_failure_within_attempt_budget() {
        let repository = Arc::new(InMemoryRepository::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = TaskRegistry::new();
        let handler_calls = Arc::clone(&calls);
        registry
            .register(task_id(), move |_claim| {
                let calls = Arc::clone(&handler_calls);
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(TaskExecutionError::retryable("temporary failure"))
                    } else {
                        Ok(())
                    }
                }
            })
            .expect("register handler");
        let scheduler = TaskScheduler::new(
            Arc::clone(&repository),
            Arc::new(MonotonicClock::new()),
            worker_id(),
            registry,
            config(1, None, 2),
        );
        let task = task_run("task-run-02");
        let id = task.task_run_id().clone();
        scheduler.submit_task_run(task).expect("submit task run");

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { scheduler.run(shutdown_rx).await });
        wait_for_status(&repository, &id, TaskStatus::Succeeded).await;
        shutdown_tx.send(true).expect("signal shutdown");
        handle.await.expect("join scheduler").expect("scheduler run");

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(repository.get(&id).expect("read task run").attempts_started(), 2);
    }

    #[tokio::test]
    async fn task_timeout_commits_timed_out_state() {
        let repository = Arc::new(InMemoryRepository::new());
        let mut registry = TaskRegistry::new();
        registry
            .register(task_id(), |_claim| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<(), TaskExecutionError>(())
            })
            .expect("register handler");
        let scheduler = TaskScheduler::new(
            Arc::clone(&repository),
            Arc::new(MonotonicClock::new()),
            worker_id(),
            registry,
            config(1, Some(Duration::from_millis(5)), 1),
        );
        let task = task_run("task-run-03");
        let id = task.task_run_id().clone();
        scheduler.submit_task_run(task).expect("submit task run");

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { scheduler.run(shutdown_rx).await });
        wait_for_status(&repository, &id, TaskStatus::TimedOut).await;
        shutdown_tx.send(true).expect("signal shutdown");
        handle.await.expect("join scheduler").expect("scheduler run");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scheduler_never_exceeds_configured_concurrency() {
        let repository = Arc::new(InMemoryRepository::new());
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let mut registry = TaskRegistry::new();
        let handler_active = Arc::clone(&active);
        let handler_maximum = Arc::clone(&maximum);
        registry
            .register(task_id(), move |_claim| {
                let active = Arc::clone(&handler_active);
                let maximum = Arc::clone(&handler_maximum);
                async move {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(current, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok::<(), TaskExecutionError>(())
                }
            })
            .expect("register handler");
        let scheduler = TaskScheduler::new(
            Arc::clone(&repository),
            Arc::new(MonotonicClock::new()),
            worker_id(),
            registry,
            config(2, None, 1),
        );
        let ids = (0..4)
            .map(|index| {
                let id = format!("task-run-{index:02}");
                let task = task_run(&id);
                let task_run_id = task.task_run_id().clone();
                scheduler.submit_task_run(task).expect("submit task run");
                task_run_id
            })
            .collect::<Vec<_>>();

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { scheduler.run(shutdown_rx).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let all_succeeded = ids.iter().all(|id| {
                    repository
                        .get(id)
                        .expect("read task run")
                        .status()
                        == TaskStatus::Succeeded
                });
                if all_succeeded {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("all tasks completed");

        shutdown_tx.send(true).expect("signal shutdown");
        handle.await.expect("join scheduler").expect("scheduler run");
        assert_eq!(maximum.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn cancellation_invalidates_completion_from_running_handler() {
        let repository = Arc::new(InMemoryRepository::new());
        let finished = Arc::new(AtomicUsize::new(0));
        let handler_finished = Arc::clone(&finished);
        let mut registry = TaskRegistry::new();
        registry
            .register(task_id(), move |_claim| {
                let finished = Arc::clone(&handler_finished);
                async move {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    finished.store(1, Ordering::SeqCst);
                    Ok::<(), TaskExecutionError>(())
                }
            })
            .expect("register handler");

        let clock = Arc::new(MonotonicClock::new());
        let scheduler = Arc::new(TaskScheduler::new(
            Arc::clone(&repository),
            clock,
            worker_id(),
            registry,
            config(1, None, 1),
        ));
        let task = task_run("task-run-04");
        let id = task.task_run_id().clone();
        scheduler.submit_task_run(task).expect("submit task run");

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running_scheduler = Arc::clone(&scheduler);
        let handle = tokio::spawn(async move { running_scheduler.run(shutdown_rx).await });
        wait_for_status(&repository, &id, TaskStatus::Running).await;
        let version = repository.get(&id).expect("read running task").version();
        scheduler.cancel_task_run(&id, version).expect("cancel task");

        tokio::time::timeout(Duration::from_secs(2), async {
            while finished.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("handler drains after state cancellation");
        shutdown_tx.send(true).expect("signal shutdown");
        handle.await.expect("join scheduler").expect("scheduler run");
        assert_eq!(repository.get(&id).expect("read task").status(), TaskStatus::Cancelled);
    }
}
