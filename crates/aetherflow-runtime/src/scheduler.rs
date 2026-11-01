//! Bounded asynchronous task scheduler and local worker loop.

use std::collections::{BTreeMap, BTreeSet, HashMap, hash_map::Entry};
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aetherflow_core::domain::{
    Lifecycle, RetryPolicy, TaskId, TaskRunId, TaskStatus, Transition, Versioned, WorkerId,
    WorkflowRunId, WorkflowStatus,
};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

use crate::clock::Clock;
use crate::repository::{
    RepositoryError, TaskClaim, TaskInput, TaskOutput, TaskRepository, TaskRun,
};
use crate::workflow::{
    TaskRunIdGenerator, WorkflowDefinition, WorkflowExecutionError, WorkflowExecutionResult,
};

/// Boxed asynchronous task-handler future.
pub type TaskFuture =
    Pin<Box<dyn Future<Output = Result<TaskOutput, TaskExecutionError>> + Send + 'static>>;

/// Cooperative cancellation signal supplied to a running task handler.
#[derive(Clone, Debug)]
pub struct TaskCancellation {
    receiver: watch::Receiver<bool>,
}

impl TaskCancellation {
    /// Returns whether cancellation has already been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Waits until cancellation is requested or the scheduler drops the signal.
    ///
    /// A handler should check this signal at safe interruption points and
    /// arrange its own cleanup for resources or external operations.
    pub async fn cancelled(&mut self) {
        loop {
            if *self.receiver.borrow_and_update() {
                return;
            }

            if self.receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Context passed to every task-handler attempt.
#[derive(Clone, Debug)]
pub struct TaskContext {
    claim: TaskClaim,
    cancellation: TaskCancellation,
}

impl TaskContext {
    /// Returns the immutable claim and attempt snapshot.
    #[must_use]
    pub fn claim(&self) -> &TaskClaim {
        &self.claim
    }

    /// Returns the validated opaque input bytes for this task run.
    #[must_use]
    pub fn input(&self) -> &TaskInput {
        self.claim.task_run().input()
    }

    /// Returns a cloneable cooperative cancellation handle.
    #[must_use]
    pub fn cancellation(&self) -> TaskCancellation {
        self.cancellation.clone()
    }
}

/// Executes one claimed task attempt.
///
/// Implementations receive the claim and a cooperative cancellation signal.
/// They must make external side effects idempotent when retries are possible;
/// the scheduler does not promise exactly-once execution.
pub trait TaskHandler: Send + Sync + 'static {
    /// Executes the claimed attempt with its cancellation context.
    fn execute(&self, context: TaskContext) -> TaskFuture;
}

impl<F, Fut> TaskHandler for F
where
    F: Fn(TaskContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<TaskOutput, TaskExecutionError>> + Send + 'static,
{
    fn execute(&self, context: TaskContext) -> TaskFuture {
        Box::pin((self)(context))
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
    pub fn register<H>(&mut self, task_id: TaskId, handler: H) -> Result<(), TaskRegistryError>
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
    /// The graceful shutdown drain timeout must be greater than zero.
    ZeroShutdownDrainTimeout,
    /// The cooperative cancellation grace period must be greater than zero.
    ZeroCancellationGracePeriod,
}

impl std::fmt::Display for SchedulerConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroPollInterval => {
                formatter.write_str("scheduler poll interval must be non-zero")
            }
            Self::ZeroShutdownDrainTimeout => {
                formatter.write_str("shutdown drain timeout must be non-zero")
            }
            Self::ZeroCancellationGracePeriod => {
                formatter.write_str("cancellation grace period must be non-zero")
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
    shutdown_drain_timeout: Duration,
    cancellation_grace_period: Duration,
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
            shutdown_drain_timeout: Duration::from_secs(30),
            cancellation_grace_period: Duration::from_secs(5),
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

    /// Returns the graceful drain period before cooperative cancellation is requested.
    #[must_use]
    pub const fn shutdown_drain_timeout(&self) -> Duration {
        self.shutdown_drain_timeout
    }

    /// Returns the cleanup period allowed after cancellation is signalled.
    #[must_use]
    pub const fn cancellation_grace_period(&self) -> Duration {
        self.cancellation_grace_period
    }

    /// Overrides the graceful-drain and cooperative-cancellation periods.
    pub fn with_shutdown_timeouts(
        mut self,
        shutdown_drain_timeout: Duration,
        cancellation_grace_period: Duration,
    ) -> Result<Self, SchedulerConfigError> {
        if shutdown_drain_timeout.is_zero() {
            return Err(SchedulerConfigError::ZeroShutdownDrainTimeout);
        }
        if cancellation_grace_period.is_zero() {
            return Err(SchedulerConfigError::ZeroCancellationGracePeriod);
        }

        self.shutdown_drain_timeout = shutdown_drain_timeout;
        self.cancellation_grace_period = cancellation_grace_period;
        Ok(self)
    }
}

/// Scheduler failures that require the worker loop to stop.
#[derive(Debug)]
pub enum SchedulerError {
    /// The repository returned an unexpected error.
    Repository(RepositoryError),
    /// A task-run already has a live cancellation registration.
    DuplicateActiveClaim(String),
    /// A worker task panicked or was aborted outside normal scheduler control.
    WorkerTaskPanicked(String),
}

impl std::fmt::Display for SchedulerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Repository(error) => error.fmt(formatter),
            Self::DuplicateActiveClaim(task_run_id) => {
                write!(
                    formatter,
                    "task run already has an active claim: {task_run_id}"
                )
            }
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
            Self::DuplicateActiveClaim(_) | Self::WorkerTaskPanicked(_) => None,
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
    cancellations: Arc<Mutex<HashMap<String, watch::Sender<bool>>>>,
}

impl<R: TaskRepository + 'static> Clone for TaskScheduler<R> {
    fn clone(&self) -> Self {
        Self {
            repository: Arc::clone(&self.repository),
            clock: Arc::clone(&self.clock),
            worker_id: self.worker_id.clone(),
            registry: Arc::clone(&self.registry),
            config: self.config.clone(),
            cancellations: Arc::clone(&self.cancellations),
        }
    }
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
            cancellations: Arc::new(Mutex::new(HashMap::new())),
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
        self.cancel_task_run_at(task_run_id, expected_version, self.clock.now_ms())
    }

    fn cancel_task_run_at(
        &self,
        task_run_id: &TaskRunId,
        expected_version: u64,
        now_ms: u64,
    ) -> Result<Transition<TaskStatus>, SchedulerError> {
        let transition = self
            .repository
            .cancel(task_run_id, expected_version, now_ms)?;
        self.signal_task_cancellation(task_run_id);
        Ok(transition)
    }

    fn register_task_cancellation(
        &self,
        task_run_id: &TaskRunId,
        sender: watch::Sender<bool>,
    ) -> Result<(), SchedulerError> {
        let key = task_run_id.as_str().to_owned();
        let mut cancellations = self
            .cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        match cancellations.entry(key.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(sender);
                Ok(())
            }
            Entry::Occupied(_) => Err(SchedulerError::DuplicateActiveClaim(key)),
        }
    }

    fn signal_task_cancellation(&self, task_run_id: &TaskRunId) {
        let cancellations = self
            .cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if let Some(sender) = cancellations.get(task_run_id.as_str()) {
            let _ = sender.send(true);
        }
    }

    fn remove_task_cancellation(
        cancellations: &Mutex<HashMap<String, watch::Sender<bool>>>,
        task_run_id: &TaskRunId,
    ) {
        let mut cancellations = cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cancellations.remove(task_run_id.as_str());
    }

    fn cancel_workflow_tasks(
        &self,
        activated: &BTreeSet<String>,
        task_run_ids: &BTreeMap<String, TaskRunId>,
        now_ms: u64,
    ) -> Result<(), WorkflowExecutionError> {
        for task_key in activated {
            let task_run_id = task_run_ids
                .get(task_key)
                .expect("activated task has a generated run ID");

            loop {
                let task_run = self.repository.get(task_run_id)?;
                if task_run.status().is_terminal() {
                    break;
                }

                match self.cancel_task_run_at(task_run_id, task_run.version(), now_ms) {
                    Ok(_) => break,
                    Err(SchedulerError::Repository(
                        RepositoryError::StaleVersion { .. }
                        | RepositoryError::InvalidTransition(_),
                    )) => continue,
                    Err(error) => return Err(error.into()),
                }
            }
        }

        Ok(())
    }

    /// Executes a validated workflow definition using dependency-aware task activation.
    ///
    /// Only root tasks are inserted initially. A downstream task is inserted
    /// once every dependency has reached Succeeded. A task failure, timeout, or
    /// cancellation terminates the workflow and cancels all other active task runs.
    ///
    /// Reuse the supplied ID generator across workflow runs sharing this repository.
    /// The returned workflow state is process-local and is not persisted.
    pub async fn execute_workflow(
        &self,
        definition: WorkflowDefinition,
        workflow_run_id: WorkflowRunId,
        id_generator: &dyn TaskRunIdGenerator,
        mut external_shutdown: watch::Receiver<bool>,
    ) -> Result<WorkflowExecutionResult, WorkflowExecutionError> {
        let now_ms = self.clock.now_ms();
        let mut workflow_lifecycle = Versioned::new(WorkflowStatus::Pending);
        workflow_lifecycle.transition(WorkflowStatus::Running)?;

        let mut task_run_ids = BTreeMap::new();
        for task in definition.tasks() {
            task_run_ids.insert(task.task_id().as_str().to_owned(), id_generator.next_id()?);
        }

        let mut activated = BTreeSet::new();
        for task in definition
            .tasks()
            .filter(|task| task.dependencies().is_empty())
        {
            let task_key = task.task_id().as_str().to_owned();
            let task_run_id = task_run_ids
                .get(&task_key)
                .expect("every definition task has a generated run ID")
                .clone();
            self.repository.insert(TaskRun::new_with_input(
                task_run_id,
                workflow_run_id.clone(),
                task.task_id().clone(),
                task.input().clone(),
                now_ms,
                now_ms,
            ))?;
            activated.insert(task_key);
        }

        if *external_shutdown.borrow_and_update() {
            workflow_lifecycle.transition(WorkflowStatus::Cancelled)?;
            self.cancel_workflow_tasks(&activated, &task_run_ids, self.clock.now_ms())?;
            return collect_workflow_result(
                self.repository.as_ref(),
                &definition,
                workflow_run_id,
                workflow_lifecycle,
                &activated,
                &task_run_ids,
            );
        }

        let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);
        let worker_scheduler = self.clone();
        let mut worker_handle =
            tokio::spawn(async move { worker_scheduler.run(worker_shutdown_rx).await });

        loop {
            if worker_handle.is_finished() {
                return match worker_handle.await {
                    Ok(Ok(())) => Err(WorkflowExecutionError::WorkerLoopExited),
                    Ok(Err(error)) => Err(WorkflowExecutionError::Scheduler(error)),
                    Err(error) => Err(WorkflowExecutionError::WorkerJoin(error.to_string())),
                };
            }

            if external_shutdown.has_changed().unwrap_or(true)
                && *external_shutdown.borrow_and_update()
            {
                workflow_lifecycle.transition(WorkflowStatus::Cancelled)?;
                self.cancel_workflow_tasks(&activated, &task_run_ids, self.clock.now_ms())?;
                let _ = worker_shutdown_tx.send(true);
                join_worker(&mut worker_handle).await?;
                return collect_workflow_result(
                    self.repository.as_ref(),
                    &definition,
                    workflow_run_id,
                    workflow_lifecycle,
                    &activated,
                    &task_run_ids,
                );
            }

            let mut observed = BTreeMap::new();
            for task_key in &activated {
                let task_run_id = task_run_ids
                    .get(task_key)
                    .expect("activated task has a generated run ID");
                let task_run = self.repository.get(task_run_id)?;
                observed.insert(task_key.clone(), task_run);
            }

            let terminal_outcome = observed
                .iter()
                .filter(|(_, task_run)| {
                    matches!(
                        task_run.status(),
                        TaskStatus::Failed | TaskStatus::TimedOut | TaskStatus::Cancelled
                    )
                })
                .min_by_key(|(task_key, task_run)| (task_run.updated_at_ms(), (*task_key).clone()))
                .map(|(_, task_run)| task_run.status());

            if let Some(task_status) = terminal_outcome {
                let workflow_status = match task_status {
                    TaskStatus::Failed => WorkflowStatus::Failed,
                    TaskStatus::TimedOut => WorkflowStatus::TimedOut,
                    TaskStatus::Cancelled => WorkflowStatus::Cancelled,
                    _ => unreachable!("only non-success terminal task states are selected"),
                };
                workflow_lifecycle.transition(workflow_status)?;
                self.cancel_workflow_tasks(&activated, &task_run_ids, self.clock.now_ms())?;
                let _ = worker_shutdown_tx.send(true);
                join_worker(&mut worker_handle).await?;
                return collect_workflow_result(
                    self.repository.as_ref(),
                    &definition,
                    workflow_run_id,
                    workflow_lifecycle,
                    &activated,
                    &task_run_ids,
                );
            }

            let dependency_states = observed
                .iter()
                .map(|(task_key, task_run)| (task_key.clone(), task_run.status()))
                .collect::<BTreeMap<_, _>>();

            for task_id in definition.execution_order() {
                let task_key = task_id.as_str();
                if activated.contains(task_key) {
                    continue;
                }

                let task = definition
                    .task(task_id)
                    .expect("execution-order task exists in definition");
                let dependencies_succeeded = task.dependencies().iter().all(|dependency| {
                    dependency_states.get(dependency.as_str()) == Some(&TaskStatus::Succeeded)
                });

                if dependencies_succeeded {
                    let task_run_id = task_run_ids
                        .get(task_key)
                        .expect("every definition task has a generated run ID")
                        .clone();
                    self.repository.insert(TaskRun::new_with_input(
                        task_run_id,
                        workflow_run_id.clone(),
                        task_id.clone(),
                        task.input().clone(),
                        self.clock.now_ms(),
                        self.clock.now_ms(),
                    ))?;
                    activated.insert(task_key.to_owned());
                }
            }

            let all_tasks_succeeded = activated.len() == definition.task_count()
                && observed.len() == definition.task_count()
                && observed
                    .values()
                    .all(|task_run| task_run.status() == TaskStatus::Succeeded);

            if all_tasks_succeeded {
                workflow_lifecycle.transition(WorkflowStatus::Succeeded)?;
                let _ = worker_shutdown_tx.send(true);
                join_worker(&mut worker_handle).await?;
                return collect_workflow_result(
                    self.repository.as_ref(),
                    &definition,
                    workflow_run_id,
                    workflow_lifecycle,
                    &activated,
                    &task_run_ids,
                );
            }

            let shutdown_requested = tokio::select! {
                changed = external_shutdown.changed() => {
                    changed.is_err() || *external_shutdown.borrow_and_update()
                }
                _ = tokio::time::sleep(self.config.poll_interval) => false,
            };

            if shutdown_requested {
                workflow_lifecycle.transition(WorkflowStatus::Cancelled)?;
                self.cancel_workflow_tasks(&activated, &task_run_ids, self.clock.now_ms())?;
                let _ = worker_shutdown_tx.send(true);
                join_worker(&mut worker_handle).await?;
                return collect_workflow_result(
                    self.repository.as_ref(),
                    &definition,
                    workflow_run_id,
                    workflow_lifecycle,
                    &activated,
                    &task_run_ids,
                );
            }
        }
    }

    /// Runs the worker loop until shutdown is signaled.
    ///
    /// Shutdown stops additional claims and drains active handlers before returning.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), SchedulerError> {
        let mut active_tasks: JoinSet<Result<(), SchedulerError>> = JoinSet::new();
        let active_task_runs = Arc::new(Mutex::new(HashMap::<String, TaskRunId>::new()));
        let mut stop_claiming = *shutdown.borrow_and_update();
        let mut shutdown_started_at = stop_claiming.then(tokio::time::Instant::now);
        let mut cancellation_started_at = None;

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
                    let task_run_id = claim.task_run().task_run_id().clone();
                    let expected_version = claim.task_run().version();
                    let (cancellation_tx, cancellation_rx) = watch::channel(false);
                    self.register_task_cancellation(&task_run_id, cancellation_tx.clone())?;

                    let latest = self.repository.get(&task_run_id)?;
                    if latest.status() != TaskStatus::Running
                        || latest.version() != expected_version
                    {
                        let _ = cancellation_tx.send(true);
                    }

                    let repository = Arc::clone(&self.repository);
                    let clock = Arc::clone(&self.clock);
                    let worker_id = self.worker_id.clone();
                    let config = self.config.clone();
                    let cancellations = Arc::clone(&self.cancellations);
                    let active_for_cleanup = Arc::clone(&active_task_runs);
                    let active_key = task_run_id.as_str().to_owned();
                    active_task_runs
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(active_key.clone(), task_run_id.clone());

                    active_tasks.spawn(async move {
                        let result = execute_claim(ClaimExecution {
                            repository,
                            clock,
                            worker_id,
                            config,
                            claim,
                            handler,
                            cancellation_rx,
                            cancellation_tx,
                        })
                        .await;
                        Self::remove_task_cancellation(&cancellations, &task_run_id);
                        active_for_cleanup
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&active_key);
                        result
                    });
                }
            }

            if stop_claiming && shutdown_started_at.is_none() {
                shutdown_started_at = Some(tokio::time::Instant::now());
            }

            if stop_claiming && active_tasks.is_empty() {
                return Ok(());
            }

            let now = tokio::time::Instant::now();
            if stop_claiming
                && cancellation_started_at.is_none()
                && shutdown_started_at
                    .is_some_and(|started| now >= started + self.config.shutdown_drain_timeout)
            {
                self.cancel_active_task_runs(&active_task_runs)?;
                cancellation_started_at = Some(now);
            }

            if stop_claiming
                && cancellation_started_at
                    .is_some_and(|started| now >= started + self.config.cancellation_grace_period)
            {
                active_tasks.abort_all();

                while let Some(joined) = active_tasks.join_next().await {
                    match joined {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => return Err(error),
                        Err(error) if error.is_cancelled() => {}
                        Err(error) => {
                            return Err(SchedulerError::WorkerTaskPanicked(error.to_string()));
                        }
                    }
                }

                let active_ids = active_task_runs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .values()
                    .cloned()
                    .collect::<Vec<_>>();
                for task_run_id in active_ids {
                    Self::remove_task_cancellation(&self.cancellations, &task_run_id);
                }
                active_task_runs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clear();
                return Ok(());
            }

            let next_shutdown_deadline = cancellation_started_at
                .map(|started| started + self.config.cancellation_grace_period)
                .or_else(|| {
                    shutdown_started_at.map(|started| started + self.config.shutdown_drain_timeout)
                });

            tokio::select! {
                changed = shutdown.changed(), if !stop_claiming => {
                    let requested = changed.is_err() || *shutdown.borrow_and_update();
                    if requested {
                        stop_claiming = true;
                        shutdown_started_at = Some(tokio::time::Instant::now());
                    }
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
                _ = tokio::time::sleep_until(
                    next_shutdown_deadline.unwrap_or_else(tokio::time::Instant::now)
                ), if stop_claiming => {}
                _ = tokio::time::sleep(self.config.poll_interval), if !stop_claiming => {}
            }
        }
    }

    fn cancel_active_task_runs(
        &self,
        active_task_runs: &Mutex<HashMap<String, TaskRunId>>,
    ) -> Result<(), SchedulerError> {
        let task_run_ids = active_task_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();

        for task_run_id in task_run_ids {
            loop {
                let task_run = self.repository.get(&task_run_id)?;
                if task_run.status().is_terminal() {
                    break;
                }

                match self.cancel_task_run(&task_run_id, task_run.version()) {
                    Ok(_) => break,
                    Err(SchedulerError::Repository(
                        RepositoryError::StaleVersion { .. }
                        | RepositoryError::InvalidTransition(_),
                    )) => continue,
                    Err(error) => return Err(error),
                }
            }
        }

        Ok(())
    }
}

async fn join_worker(
    handle: &mut JoinHandle<Result<(), SchedulerError>>,
) -> Result<(), WorkflowExecutionError> {
    match handle.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(WorkflowExecutionError::Scheduler(error)),
        Err(error) => Err(WorkflowExecutionError::WorkerJoin(error.to_string())),
    }
}

fn collect_workflow_result<R: TaskRepository>(
    repository: &R,
    definition: &WorkflowDefinition,
    workflow_run_id: WorkflowRunId,
    lifecycle: Versioned<WorkflowStatus>,
    activated: &BTreeSet<String>,
    task_run_ids: &BTreeMap<String, TaskRunId>,
) -> Result<WorkflowExecutionResult, WorkflowExecutionError> {
    let mut task_runs = Vec::with_capacity(activated.len());
    for task_key in activated {
        let task_run_id = task_run_ids
            .get(task_key)
            .expect("activated task has a generated run ID");
        task_runs.push(repository.get(task_run_id)?);
    }

    Ok(WorkflowExecutionResult::new(
        definition.workflow_id().clone(),
        workflow_run_id,
        lifecycle.status(),
        lifecycle.version(),
        task_runs,
    ))
}

struct ClaimExecution<R: TaskRepository + 'static> {
    repository: Arc<R>,
    clock: Arc<dyn Clock>,
    worker_id: WorkerId,
    config: SchedulerConfig,
    claim: TaskClaim,
    handler: Option<Arc<dyn TaskHandler>>,
    cancellation_rx: watch::Receiver<bool>,
    cancellation_tx: watch::Sender<bool>,
}

async fn execute_claim<R: TaskRepository + 'static>(
    execution: ClaimExecution<R>,
) -> Result<(), SchedulerError> {
    let ClaimExecution {
        repository,
        clock,
        worker_id,
        config,
        claim,
        handler,
        cancellation_rx,
        cancellation_tx,
    } = execution;
    let task_run_id = claim.task_run().task_run_id().clone();
    let task_id = claim.task_run().task_id().clone();
    let expected_version = claim.task_run().version();
    let attempt_number = claim.attempt_number();

    let future: TaskFuture = match handler {
        Some(handler) => handler.execute(TaskContext {
            claim,
            cancellation: TaskCancellation {
                receiver: cancellation_rx,
            },
        }),
        None => {
            let message = format!("no handler registered for task '{}'", task_id.as_str());
            Box::pin(async move { Err(TaskExecutionError::permanent(message)) })
        }
    };

    let outcome = match config.task_timeout {
        Some(timeout) => {
            let result = tokio::time::timeout(timeout, future).await;
            if result.is_err() {
                let _ = cancellation_tx.send(true);
            }
            result.ok()
        }
        None => Some(future.await),
    };

    let now_ms = clock.now_ms();
    match outcome {
        Some(Ok(output)) => {
            settle(repository.complete(&task_run_id, &worker_id, expected_version, output, now_ms))
        }
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
                settle(repository.fail(&task_run_id, &worker_id, expected_version, now_ms))
            }
        }
        None => settle(repository.timeout(&task_run_id, expected_version, now_ms)),
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
    use crate::repository::{InMemoryRepository, TaskInput, TaskOutput, TaskRepository, TaskRun};
    use aetherflow_core::domain::{
        RetryPolicy, TaskId, TaskRunId, TaskStatus, WorkerId, WorkflowRunId,
    };
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
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

    #[test]
    fn scheduler_rejects_zero_shutdown_timeouts() {
        assert!(matches!(
            config(1, None, 1).with_shutdown_timeouts(Duration::ZERO, Duration::from_millis(1)),
            Err(super::SchedulerConfigError::ZeroShutdownDrainTimeout)
        ));
        assert!(matches!(
            config(1, None, 1).with_shutdown_timeouts(Duration::from_millis(1), Duration::ZERO),
            Err(super::SchedulerConfigError::ZeroCancellationGracePeriod)
        ));
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
                Ok::<TaskOutput, TaskExecutionError>(TaskOutput::empty())
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
        handle
            .await
            .expect("join scheduler")
            .expect("scheduler run");
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
                        Ok(TaskOutput::empty())
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
        handle
            .await
            .expect("join scheduler")
            .expect("scheduler run");

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            repository
                .get(&id)
                .expect("read task run")
                .attempts_started(),
            2
        );
    }

    #[tokio::test]
    async fn task_timeout_commits_timed_out_state() {
        let repository = Arc::new(InMemoryRepository::new());
        let mut registry = TaskRegistry::new();
        registry
            .register(task_id(), |_claim| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<TaskOutput, TaskExecutionError>(TaskOutput::empty())
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
        handle
            .await
            .expect("join scheduler")
            .expect("scheduler run");
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
                    Ok::<TaskOutput, TaskExecutionError>(TaskOutput::empty())
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
                    repository.get(id).expect("read task run").status() == TaskStatus::Succeeded
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
        handle
            .await
            .expect("join scheduler")
            .expect("scheduler run");
        assert_eq!(maximum.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn workflow_activates_tasks_only_after_dependencies_succeed() {
        use crate::workflow::{SequenceTaskRunIdGenerator, WorkflowDefinition, WorkflowTask};
        use aetherflow_core::domain::{WorkflowId, WorkflowRunId};
        use std::sync::Mutex;

        let repository = Arc::new(InMemoryRepository::new());
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut registry = TaskRegistry::new();

        for task_name in ["task-a", "task-b", "task-c"] {
            let order = Arc::clone(&order);
            let name = task_name.to_owned();
            registry
                .register(
                    TaskId::parse(task_name).expect("valid task ID"),
                    move |_claim| {
                        let order = Arc::clone(&order);
                        let name = name.clone();
                        async move {
                            order.lock().expect("lock order").push(name);
                            Ok::<TaskOutput, TaskExecutionError>(TaskOutput::empty())
                        }
                    },
                )
                .expect("register task handler");
        }

        let scheduler = TaskScheduler::new(
            Arc::clone(&repository),
            Arc::new(MonotonicClock::new()),
            worker_id(),
            registry,
            config(3, None, 1),
        );
        let definition = WorkflowDefinition::new(
            WorkflowId::parse("workflow-chain").expect("valid workflow ID"),
            vec![
                WorkflowTask::new(
                    TaskId::parse("task-c").expect("valid task ID"),
                    vec![TaskId::parse("task-b").expect("valid dependency")],
                ),
                WorkflowTask::new(
                    TaskId::parse("task-b").expect("valid task ID"),
                    vec![TaskId::parse("task-a").expect("valid dependency")],
                ),
                WorkflowTask::new(TaskId::parse("task-a").expect("valid task ID"), Vec::new()),
            ],
        )
        .expect("valid workflow DAG");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let result = scheduler
            .execute_workflow(
                definition,
                WorkflowRunId::parse("workflow-run-chain").expect("valid workflow-run ID"),
                &SequenceTaskRunIdGenerator::new(),
                shutdown_rx,
            )
            .await
            .expect("workflow executes");

        assert_eq!(
            result.status(),
            aetherflow_core::domain::WorkflowStatus::Succeeded
        );
        assert_eq!(result.task_runs().len(), 3);
        assert!(
            result
                .task_runs()
                .iter()
                .all(|task_run| task_run.status() == TaskStatus::Succeeded)
        );
        assert_eq!(
            *order.lock().expect("read execution order"),
            ["task-a", "task-b", "task-c"]
        );
        drop(shutdown_tx);
    }

    #[tokio::test]
    async fn workflow_task_input_reaches_handler_and_output_is_returned() {
        use crate::workflow::{SequenceTaskRunIdGenerator, WorkflowDefinition, WorkflowTask};
        use aetherflow_core::domain::{WorkflowId, WorkflowRunId};

        let repository = Arc::new(InMemoryRepository::new());
        let mut registry = TaskRegistry::new();
        registry
            .register(task_id(), |context: super::TaskContext| async move {
                assert_eq!(context.input().as_bytes(), b"workflow-input");
                Ok::<TaskOutput, TaskExecutionError>(
                    TaskOutput::new(b"processed".to_vec()).expect("valid task output"),
                )
            })
            .expect("register handler");
        let scheduler = TaskScheduler::new(
            Arc::clone(&repository),
            Arc::new(MonotonicClock::new()),
            worker_id(),
            registry,
            config(1, None, 1),
        );
        let input = TaskInput::new(b"workflow-input".to_vec()).expect("valid task input");
        let definition = WorkflowDefinition::new(
            WorkflowId::parse("workflow-payload").expect("valid workflow ID"),
            vec![
                WorkflowTask::new(TaskId::parse("task-01").expect("valid task ID"), Vec::new())
                    .with_input(input.clone()),
            ],
        )
        .expect("valid workflow");

        let result = scheduler
            .execute_workflow(
                definition,
                WorkflowRunId::parse("workflow-run-payload").expect("valid workflow-run ID"),
                &SequenceTaskRunIdGenerator::new(),
                watch::channel(false).1,
            )
            .await
            .expect("workflow completes");

        assert_eq!(
            result.status(),
            aetherflow_core::domain::WorkflowStatus::Succeeded
        );
        assert_eq!(result.task_runs().len(), 1);
        assert_eq!(result.task_runs()[0].input(), &input);
        assert_eq!(
            result.task_runs()[0].output().map(TaskOutput::as_bytes),
            Some(&b"processed"[..])
        );
    }

    #[tokio::test]
    async fn failed_task_prevents_dependent_task_activation() {
        use crate::workflow::{SequenceTaskRunIdGenerator, WorkflowDefinition, WorkflowTask};
        use aetherflow_core::domain::{WorkflowId, WorkflowRunId};

        let repository = Arc::new(InMemoryRepository::new());
        let downstream_calls = Arc::new(AtomicUsize::new(0));
        let mut registry = TaskRegistry::new();
        registry
            .register(
                TaskId::parse("task-a").expect("valid task ID"),
                |_claim| async {
                    Err::<TaskOutput, TaskExecutionError>(TaskExecutionError::permanent("stop here"))
                },
            )
            .expect("register failing task");
        let downstream = Arc::clone(&downstream_calls);
        registry
            .register(
                TaskId::parse("task-b").expect("valid task ID"),
                move |_claim| {
                    let downstream = Arc::clone(&downstream);
                    async move {
                        downstream.fetch_add(1, Ordering::SeqCst);
                        Ok::<TaskOutput, TaskExecutionError>(TaskOutput::empty())
                    }
                },
            )
            .expect("register dependent task");

        let scheduler = TaskScheduler::new(
            Arc::clone(&repository),
            Arc::new(MonotonicClock::new()),
            worker_id(),
            registry,
            config(2, None, 1),
        );
        let definition = WorkflowDefinition::new(
            WorkflowId::parse("workflow-failure").expect("valid workflow ID"),
            vec![
                WorkflowTask::new(TaskId::parse("task-a").expect("valid task ID"), Vec::new()),
                WorkflowTask::new(
                    TaskId::parse("task-b").expect("valid task ID"),
                    vec![TaskId::parse("task-a").expect("valid dependency")],
                ),
            ],
        )
        .expect("valid workflow DAG");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let result = scheduler
            .execute_workflow(
                definition,
                WorkflowRunId::parse("workflow-run-failure").expect("valid workflow-run ID"),
                &SequenceTaskRunIdGenerator::new(),
                shutdown_rx,
            )
            .await
            .expect("workflow reaches failed state");

        assert_eq!(
            result.status(),
            aetherflow_core::domain::WorkflowStatus::Failed
        );
        assert_eq!(result.task_runs().len(), 1);
        assert_eq!(downstream_calls.load(Ordering::SeqCst), 0);
        drop(shutdown_tx);
    }

    #[tokio::test]
    async fn external_shutdown_cancels_workflow_and_drains_active_handler() {
        use crate::workflow::{SequenceTaskRunIdGenerator, WorkflowDefinition, WorkflowTask};
        use aetherflow_core::domain::{WorkflowId, WorkflowRunId, WorkflowStatus};

        let repository = Arc::new(InMemoryRepository::new());
        let finished = Arc::new(AtomicUsize::new(0));
        let handler_finished = Arc::clone(&finished);
        let mut registry = TaskRegistry::new();
        registry
            .register(
                TaskId::parse("task-a").expect("valid task ID"),
                move |context: super::TaskContext| {
                    let finished = Arc::clone(&handler_finished);
                    let mut cancellation = context.cancellation();
                    async move {
                        cancellation.cancelled().await;
                        finished.store(1, Ordering::SeqCst);
                        Ok::<TaskOutput, TaskExecutionError>(TaskOutput::empty())
                    }
                },
            )
            .expect("register handler");
        let scheduler = Arc::new(TaskScheduler::new(
            Arc::clone(&repository),
            Arc::new(MonotonicClock::new()),
            worker_id(),
            registry,
            config(1, None, 1),
        ));
        let definition = WorkflowDefinition::new(
            WorkflowId::parse("workflow-cancel").expect("valid workflow ID"),
            vec![WorkflowTask::new(
                TaskId::parse("task-a").expect("valid task ID"),
                Vec::new(),
            )],
        )
        .expect("valid workflow");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running_scheduler = Arc::clone(&scheduler);
        let handle = tokio::spawn(async move {
            running_scheduler
                .execute_workflow(
                    definition,
                    WorkflowRunId::parse("workflow-run-cancel").expect("valid workflow-run ID"),
                    &SequenceTaskRunIdGenerator::new(),
                    shutdown_rx,
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let has_running_task = (1..=1).any(|sequence| {
                    TaskRunId::parse(format!("task-run-{sequence}"))
                        .ok()
                        .and_then(|id| repository.get(&id).ok())
                        .is_some_and(|task_run| task_run.status() == TaskStatus::Running)
                });
                if has_running_task {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("workflow starts its root task");
        shutdown_tx.send(true).expect("signal external shutdown");

        let result = handle
            .await
            .expect("join workflow")
            .expect("workflow cancellation result");
        assert_eq!(result.status(), WorkflowStatus::Cancelled);
        assert_eq!(finished.load(Ordering::SeqCst), 1);
        assert_eq!(result.task_runs()[0].status(), TaskStatus::Cancelled);
    }

    #[tokio::test]
    async fn task_context_exposes_claim_and_cooperative_cancellation() {
        let repository = Arc::new(InMemoryRepository::new());
        let finished = Arc::new(AtomicUsize::new(0));
        let handler_finished = Arc::clone(&finished);
        let mut registry = TaskRegistry::new();
        registry
            .register(task_id(), move |context: super::TaskContext| {
                let finished = Arc::clone(&handler_finished);
                let claim = context.claim().clone();
                let mut cancellation = context.cancellation();
                async move {
                    assert_eq!(claim.attempt_number().get(), 1);
                    cancellation.cancelled().await;
                    finished.store(1, Ordering::SeqCst);
                    Ok::<TaskOutput, TaskExecutionError>(TaskOutput::empty())
                }
            })
            .expect("register cooperative handler");

        let scheduler = Arc::new(TaskScheduler::new(
            Arc::clone(&repository),
            Arc::new(MonotonicClock::new()),
            worker_id(),
            registry,
            config(1, None, 1),
        ));
        let task = task_run("task-run-cooperative");
        let id = task.task_run_id().clone();
        scheduler.submit_task_run(task).expect("submit task run");

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let running_scheduler = Arc::clone(&scheduler);
        let handle = tokio::spawn(async move { running_scheduler.run(shutdown_rx).await });
        wait_for_status(&repository, &id, TaskStatus::Running).await;
        let version = repository.get(&id).expect("read running task").version();
        scheduler
            .cancel_task_run(&id, version)
            .expect("cancel task");

        tokio::time::timeout(Duration::from_secs(2), async {
            while finished.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("cooperative handler observes cancellation");

        assert_eq!(
            repository.get(&id).expect("read task").status(),
            TaskStatus::Cancelled
        );
        shutdown_tx.send(true).expect("signal scheduler shutdown");
        handle
            .await
            .expect("join scheduler")
            .expect("scheduler run");
    }

    #[tokio::test]
    async fn shutdown_cancels_and_aborts_unresponsive_handlers_after_deadlines() {
        let repository = Arc::new(InMemoryRepository::new());
        let started = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let handler_started = Arc::clone(&started);
        let handler_completed = Arc::clone(&completed);
        let mut registry = TaskRegistry::new();
        registry
            .register(task_id(), move |_context: super::TaskContext| {
                let started = Arc::clone(&handler_started);
                let completed = Arc::clone(&handler_completed);
                async move {
                    started.store(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    completed.store(1, Ordering::SeqCst);
                    Ok::<TaskOutput, TaskExecutionError>(TaskOutput::empty())
                }
            })
            .expect("register unresponsive handler");

        let scheduler_config = config(1, None, 1)
            .with_shutdown_timeouts(Duration::from_millis(20), Duration::from_millis(20))
            .expect("valid shutdown timeouts");
        let scheduler = TaskScheduler::new(
            Arc::clone(&repository),
            Arc::new(MonotonicClock::new()),
            worker_id(),
            registry,
            scheduler_config,
        );
        let task = task_run("task-run-shutdown-bound");
        let id = task.task_run_id().clone();
        scheduler.submit_task_run(task).expect("submit task run");

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move { scheduler.run(shutdown_rx).await });
        wait_for_status(&repository, &id, TaskStatus::Running).await;

        shutdown_tx.send(true).expect("signal shutdown");
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("bounded shutdown returns")
            .expect("join scheduler")
            .expect("scheduler shutdown");

        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert_eq!(completed.load(Ordering::SeqCst), 0);
        assert_eq!(
            repository.get(&id).expect("read task").status(),
            TaskStatus::Cancelled
        );
    }

    #[tokio::test]
    async fn cancellation_invalidates_completion_from_running_handler() {
        let repository = Arc::new(InMemoryRepository::new());
        let finished = Arc::new(AtomicUsize::new(0));
        let handler_finished = Arc::clone(&finished);
        let mut registry = TaskRegistry::new();
        registry
            .register(task_id(), move |context: super::TaskContext| {
                let finished = Arc::clone(&handler_finished);
                let mut cancellation = context.cancellation();
                async move {
                    cancellation.cancelled().await;
                    finished.store(1, Ordering::SeqCst);
                    Ok::<TaskOutput, TaskExecutionError>(TaskOutput::empty())
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
        scheduler
            .cancel_task_run(&id, version)
            .expect("cancel task");

        tokio::time::timeout(Duration::from_secs(2), async {
            while finished.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("handler drains after state cancellation");
        shutdown_tx.send(true).expect("signal shutdown");
        handle
            .await
            .expect("join scheduler")
            .expect("scheduler run");
        assert_eq!(
            repository.get(&id).expect("read task").status(),
            TaskStatus::Cancelled
        );
    }
}
