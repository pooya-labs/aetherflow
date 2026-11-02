//! PostgreSQL-backed durable task worker loop.
//!
//! The worker shares the runtime's handler registry and handler context while
//! keeping all scheduling, lease, retry, and lifecycle state in PostgreSQL.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aetherflow_core::domain::{AttemptId, TaskRunId, TaskStatus, WorkerId};
use aetherflow_runtime::repository::{
    TaskDependencyOutput, TaskExecutionClaim, TaskExecutionSnapshot,
};
use aetherflow_runtime::scheduler::{
    TaskCancellationTrigger, TaskContext, TaskExecutionError, TaskFuture, TaskHandler, TaskRegistry,
};
use sha2::Digest;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::{PgTaskClaim, PgTaskRepository, StorageError, TaskFailure};

/// Configuration validation failures for a PostgreSQL worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PostgresWorkerConfigError {
    /// The polling interval must be greater than zero.
    ZeroPollInterval,
    /// The lease duration must be greater than zero.
    ZeroLeaseDuration,
    /// The heartbeat interval must be greater than zero.
    ZeroHeartbeatInterval,
    /// Heartbeats must occur before the lease can expire.
    HeartbeatNotBeforeLeaseExpiry,
    /// A configured task timeout must be greater than zero.
    ZeroTaskTimeout,
    /// The graceful shutdown drain timeout must be greater than zero.
    ZeroShutdownDrainTimeout,
    /// The cooperative cancellation grace period must be greater than zero.
    ZeroCancellationGracePeriod,
    /// Recovery and promotion batch sizes must be positive.
    InvalidBatchSize,
}

impl std::fmt::Display for PostgresWorkerConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroPollInterval => formatter.write_str("worker poll interval must be non-zero"),
            Self::ZeroLeaseDuration => {
                formatter.write_str("worker lease duration must be non-zero")
            }
            Self::ZeroHeartbeatInterval => {
                formatter.write_str("worker heartbeat interval must be non-zero")
            }
            Self::HeartbeatNotBeforeLeaseExpiry => {
                formatter.write_str("worker heartbeat interval must be shorter than its lease")
            }
            Self::ZeroTaskTimeout => formatter.write_str("task timeout must be non-zero"),
            Self::ZeroShutdownDrainTimeout => {
                formatter.write_str("shutdown drain timeout must be non-zero")
            }
            Self::ZeroCancellationGracePeriod => {
                formatter.write_str("cancellation grace period must be non-zero")
            }
            Self::InvalidBatchSize => formatter.write_str("worker batch sizes must be positive"),
        }
    }
}

impl std::error::Error for PostgresWorkerConfigError {}

/// Runtime limits for one PostgreSQL-backed worker process.
#[derive(Clone, Debug)]
pub struct PostgresWorkerConfig {
    max_concurrency: NonZeroUsize,
    poll_interval: Duration,
    lease_duration: Duration,
    heartbeat_interval: Duration,
    task_timeout: Option<Duration>,
    recovery_batch_size: i64,
    promotion_batch_size: i64,
    shutdown_drain_timeout: Duration,
    cancellation_grace_period: Duration,
}

impl PostgresWorkerConfig {
    /// Creates a worker configuration with bounded concurrency and fenced leases.
    pub fn new(
        max_concurrency: NonZeroUsize,
        poll_interval: Duration,
        lease_duration: Duration,
        heartbeat_interval: Duration,
        task_timeout: Option<Duration>,
    ) -> Result<Self, PostgresWorkerConfigError> {
        if poll_interval.is_zero() {
            return Err(PostgresWorkerConfigError::ZeroPollInterval);
        }
        if lease_duration.is_zero() {
            return Err(PostgresWorkerConfigError::ZeroLeaseDuration);
        }
        if heartbeat_interval.is_zero() {
            return Err(PostgresWorkerConfigError::ZeroHeartbeatInterval);
        }
        if heartbeat_interval >= lease_duration {
            return Err(PostgresWorkerConfigError::HeartbeatNotBeforeLeaseExpiry);
        }
        if task_timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Err(PostgresWorkerConfigError::ZeroTaskTimeout);
        }

        Ok(Self {
            max_concurrency,
            poll_interval,
            lease_duration,
            heartbeat_interval,
            task_timeout,
            recovery_batch_size: 64,
            promotion_batch_size: 64,
            shutdown_drain_timeout: Duration::from_secs(30),
            cancellation_grace_period: Duration::from_secs(5),
        })
    }

    /// Overrides the bounded lease-recovery and due-promotion batch sizes.
    pub fn with_batch_sizes(
        mut self,
        recovery_batch_size: i64,
        promotion_batch_size: i64,
    ) -> Result<Self, PostgresWorkerConfigError> {
        if recovery_batch_size <= 0 || promotion_batch_size <= 0 {
            return Err(PostgresWorkerConfigError::InvalidBatchSize);
        }
        self.recovery_batch_size = recovery_batch_size;
        self.promotion_batch_size = promotion_batch_size;
        Ok(self)
    }

    /// Overrides graceful drain and cooperative cancellation periods.
    pub fn with_shutdown_timeouts(
        mut self,
        shutdown_drain_timeout: Duration,
        cancellation_grace_period: Duration,
    ) -> Result<Self, PostgresWorkerConfigError> {
        if shutdown_drain_timeout.is_zero() {
            return Err(PostgresWorkerConfigError::ZeroShutdownDrainTimeout);
        }
        if cancellation_grace_period.is_zero() {
            return Err(PostgresWorkerConfigError::ZeroCancellationGracePeriod);
        }
        self.shutdown_drain_timeout = shutdown_drain_timeout;
        self.cancellation_grace_period = cancellation_grace_period;
        Ok(self)
    }

    /// Returns the configured in-flight attempt limit.
    #[must_use]
    pub const fn max_concurrency(&self) -> NonZeroUsize {
        self.max_concurrency
    }

    /// Returns the database polling interval.
    #[must_use]
    pub const fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Returns the lease duration assigned to each claim.
    #[must_use]
    pub const fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    /// Returns the periodic lease heartbeat interval.
    #[must_use]
    pub const fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// Returns the optional per-attempt handler timeout.
    #[must_use]
    pub const fn task_timeout(&self) -> Option<Duration> {
        self.task_timeout
    }
}

/// Errors returned by the PostgreSQL-backed worker loop.
#[derive(Debug)]
pub enum PostgresWorkerError {
    /// PostgreSQL rejected a durable operation.
    Storage(StorageError),
    /// A durable task references a dependency that has not succeeded.
    DependencyNotSucceeded(String),
    /// A succeeded dependency did not retain its output.
    DependencyOutputMissing(String),
    /// This worker process already has a live handler for the task run.
    DuplicateActiveClaim(String),
    /// A task handler panicked or was aborted unexpectedly.
    HandlerPanicked(String),
}

impl std::fmt::Display for PostgresWorkerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => error.fmt(formatter),
            Self::DependencyNotSucceeded(id) => {
                write!(formatter, "task dependency has not succeeded: {id}")
            }
            Self::DependencyOutputMissing(id) => {
                write!(formatter, "successful dependency has no output: {id}")
            }
            Self::DuplicateActiveClaim(id) => {
                write!(formatter, "worker already has an active task claim: {id}")
            }
            Self::HandlerPanicked(message) => {
                write!(
                    formatter,
                    "durable task handler terminated unexpectedly: {message}"
                )
            }
        }
    }
}

impl std::error::Error for PostgresWorkerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::DependencyNotSucceeded(_)
            | Self::DependencyOutputMissing(_)
            | Self::DuplicateActiveClaim(_)
            | Self::HandlerPanicked(_) => None,
        }
    }
}

impl From<StorageError> for PostgresWorkerError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

/// A worker that claims, heartbeats, executes, and settles tasks through PostgreSQL.
///
/// A process restart loses only local futures and cancellation handles. Task state,
/// attempts, deadlines, and lease ownership remain in PostgreSQL and are recovered
/// by the next worker through the expired-lease reaper.
pub struct PostgresTaskWorker {
    repository: Arc<PgTaskRepository>,
    worker_id: WorkerId,
    registry: Arc<TaskRegistry>,
    config: PostgresWorkerConfig,
    cancellations: Arc<Mutex<HashMap<String, TaskCancellationTrigger>>>,
    attempt_ids: AttemptIdGenerator,
}

impl Clone for PostgresTaskWorker {
    fn clone(&self) -> Self {
        Self {
            repository: Arc::clone(&self.repository),
            worker_id: self.worker_id.clone(),
            registry: Arc::clone(&self.registry),
            config: self.config.clone(),
            cancellations: Arc::clone(&self.cancellations),
            attempt_ids: self.attempt_ids.clone(),
        }
    }
}

impl PostgresTaskWorker {
    /// Creates a PostgreSQL worker using the shared runtime handler registry.
    #[must_use]
    pub fn new(
        repository: Arc<PgTaskRepository>,
        worker_id: WorkerId,
        registry: TaskRegistry,
        config: PostgresWorkerConfig,
    ) -> Self {
        Self {
            repository,
            attempt_ids: AttemptIdGenerator::new(&worker_id),
            worker_id,
            registry: Arc::new(registry),
            config,
            cancellations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Runs until shutdown is requested, draining handlers before aborting stragglers.
    ///
    /// Shutdown does not mark work cancelled: handlers receive a cooperative signal,
    /// and any aborted attempt is recovered after its PostgreSQL lease expires.
    pub async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), PostgresWorkerError> {
        let mut active_tasks: JoinSet<Result<(), PostgresWorkerError>> = JoinSet::new();
        let active_task_runs = Arc::new(Mutex::new(HashMap::<String, TaskRunId>::new()));
        let mut stop_claiming = *shutdown.borrow_and_update();
        let mut shutdown_started_at = stop_claiming.then(tokio::time::Instant::now);
        let mut cancellation_started_at = None;

        loop {
            if !stop_claiming {
                self.recover_expired_work().await?;
                self.repository
                    .promote_due_tasks(self.config.promotion_batch_size)
                    .await?;

                while active_tasks.len() < self.config.max_concurrency.get() {
                    if *shutdown.borrow() {
                        stop_claiming = true;
                        break;
                    }

                    let attempt_id = self.attempt_ids.next_id()?;
                    let Some(claim) = self
                        .repository
                        .claim_next(&self.worker_id, &attempt_id, self.config.lease_duration)
                        .await?
                    else {
                        break;
                    };

                    let task_run = claim.task_run();
                    let task_run_id = task_run.task_run_id().clone();
                    let dependency_outputs = self.load_dependency_outputs(task_run).await?;
                    let execution_claim = execution_claim_from_pg(&claim);
                    let (context, cancellation_trigger) =
                        TaskContext::new(execution_claim, dependency_outputs);
                    self.register_task_cancellation(&task_run_id, cancellation_trigger.clone())?;

                    let handler = self.registry.handler_for(task_run.task_id());
                    let repository = Arc::clone(&self.repository);
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
                        let result = execute_pg_claim(
                            repository,
                            worker_id,
                            claim,
                            handler,
                            context,
                            cancellation_trigger,
                            config,
                        )
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
                self.signal_active_cancellations();
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
                            return Err(PostgresWorkerError::HandlerPanicked(error.to_string()));
                        }
                    }
                }
                self.clear_active_cancellations(&active_task_runs);
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
                            return Err(PostgresWorkerError::HandlerPanicked(error.to_string()));
                        }
                        None => {}
                    }
                }
                _ = tokio::time::sleep_until(
                    next_shutdown_deadline.unwrap_or_else(|| {
                        tokio::time::Instant::now() + self.config.poll_interval
                    })
                ), if stop_claiming => {}
                _ = tokio::time::sleep(self.config.poll_interval), if !stop_claiming => {}
            }
        }
    }

    async fn recover_expired_work(&self) -> Result<(), PostgresWorkerError> {
        let recovered = self
            .repository
            .recover_expired_leases(self.config.recovery_batch_size)
            .await?;
        let mut workflow_ids = recovered
            .iter()
            .map(|lease| lease.task_run().workflow_run_id().clone())
            .collect::<Vec<_>>();
        workflow_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        workflow_ids.dedup();
        for workflow_run_id in workflow_ids {
            self.repository
                .aggregate_workflow_run(&workflow_run_id)
                .await?;
        }
        Ok(())
    }

    async fn load_dependency_outputs(
        &self,
        task_run: &crate::DurableTaskRun,
    ) -> Result<Vec<TaskDependencyOutput>, PostgresWorkerError> {
        let mut outputs = Vec::with_capacity(task_run.dependencies().len());
        for dependency_run_id in task_run.dependencies() {
            let dependency = self.repository.get_task_run(dependency_run_id).await?;
            if dependency.status() != TaskStatus::Succeeded {
                return Err(PostgresWorkerError::DependencyNotSucceeded(
                    dependency_run_id.as_str().to_owned(),
                ));
            }
            let output = dependency.output().cloned().ok_or_else(|| {
                PostgresWorkerError::DependencyOutputMissing(dependency_run_id.as_str().to_owned())
            })?;
            outputs.push(TaskDependencyOutput::new(
                dependency.task_id().clone(),
                dependency_run_id.clone(),
                output,
            ));
        }
        Ok(outputs)
    }

    fn register_task_cancellation(
        &self,
        task_run_id: &TaskRunId,
        trigger: TaskCancellationTrigger,
    ) -> Result<(), PostgresWorkerError> {
        let mut cancellations = self
            .cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cancellations.contains_key(task_run_id.as_str()) {
            return Err(PostgresWorkerError::DuplicateActiveClaim(
                task_run_id.as_str().to_owned(),
            ));
        }
        cancellations.insert(task_run_id.as_str().to_owned(), trigger);
        Ok(())
    }

    fn remove_task_cancellation(
        cancellations: &Mutex<HashMap<String, TaskCancellationTrigger>>,
        task_run_id: &TaskRunId,
    ) {
        cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(task_run_id.as_str());
    }

    fn signal_active_cancellations(&self) {
        let cancellations = self
            .cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for trigger in cancellations.values() {
            trigger.cancel();
        }
    }

    fn clear_active_cancellations(&self, active_task_runs: &Mutex<HashMap<String, TaskRunId>>) {
        let keys = active_task_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut cancellations = self
            .cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for key in keys {
            cancellations.remove(&key);
        }
        active_task_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
}

fn execution_claim_from_pg(claim: &PgTaskClaim) -> TaskExecutionClaim {
    let task_run = claim.task_run();
    TaskExecutionClaim::new(
        TaskExecutionSnapshot::new(
            task_run.task_run_id().clone(),
            task_run.workflow_run_id().clone(),
            task_run.task_id().clone(),
            task_run.input().clone(),
            task_run.dependencies().to_vec(),
            task_run.output().cloned(),
            task_run.status(),
            task_run.version(),
            task_run.attempts_started(),
        ),
        claim.attempt_number(),
        Some(claim.fencing_token()),
    )
}

#[derive(Clone)]
struct AttemptIdGenerator {
    process_nonce: u128,
    worker_fingerprint: String,
    next: Arc<AtomicU64>,
}

impl AttemptIdGenerator {
    fn new(worker_id: &WorkerId) -> Self {
        let process_nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let digest = sha2::Sha256::digest(worker_id.as_str().as_bytes());
        let worker_fingerprint = digest[..6]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Self {
            process_nonce,
            worker_fingerprint,
            next: Arc::new(AtomicU64::new(1)),
        }
    }

    fn next_id(&self) -> Result<AttemptId, StorageError> {
        let sequence = self
            .next
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                StorageError::InvalidStoredValue("attempt ID sequence exhausted".to_owned())
            })?;
        AttemptId::parse(format!(
            "a-{}-{:032x}-{sequence:x}",
            self.worker_fingerprint, self.process_nonce
        ))
        .map_err(|error| StorageError::InvalidStoredValue(error.to_string()))
    }
}

async fn execute_pg_claim(
    repository: Arc<PgTaskRepository>,
    worker_id: WorkerId,
    claim: PgTaskClaim,
    handler: Option<Arc<dyn TaskHandler>>,
    context: TaskContext,
    cancellation_trigger: TaskCancellationTrigger,
    config: PostgresWorkerConfig,
) -> Result<(), PostgresWorkerError> {
    let task_run = claim.task_run();
    let task_run_id = task_run.task_run_id().clone();
    let workflow_run_id = task_run.workflow_run_id().clone();
    let task_id = task_run.task_id().clone();
    let expected_version = task_run.version();
    let fencing_token = claim.fencing_token();

    let future: TaskFuture = match handler {
        Some(handler) => handler.execute(context),
        None => {
            let message = format!("no handler registered for task '{}'", task_id.as_str());
            Box::pin(async move { Err(TaskExecutionError::permanent(message)) })
        }
    };
    tokio::pin!(future);

    let timeout_wait = async {
        match config.task_timeout {
            Some(timeout) => tokio::time::sleep(timeout).await,
            None => std::future::pending::<()>().await,
        }
    };
    tokio::pin!(timeout_wait);

    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;

    let outcome = loop {
        tokio::select! {
            result = &mut future => break Some(result),
            _ = heartbeat.tick() => {
                match repository.heartbeat(
                    &task_run_id,
                    &worker_id,
                    expected_version,
                    fencing_token,
                    config.lease_duration,
                ).await {
                    Ok(_) => {}
                    Err(StorageError::ClaimLost(_)) => {
                        cancellation_trigger.cancel();
                        return Ok(());
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            _ = &mut timeout_wait => {
                cancellation_trigger.cancel();
                match repository.timeout_task_run(&task_run_id, expected_version).await {
                    Ok(_) => {
                        repository.aggregate_workflow_run(&workflow_run_id).await?;
                        return Ok(());
                    }
                    Err(StorageError::ClaimLost(_)) => return Ok(()),
                    Err(error) => return Err(error.into()),
                }
            }
        }
    };

    match outcome {
        Some(Ok(output)) => match repository
            .complete(
                &task_run_id,
                &worker_id,
                expected_version,
                fencing_token,
                output,
            )
            .await
        {
            Ok(_) => {
                repository.aggregate_workflow_run(&workflow_run_id).await?;
                Ok(())
            }
            Err(StorageError::ClaimLost(_)) => Ok(()),
            Err(error) => Err(error.into()),
        },
        Some(Err(error)) => {
            let failure = match error {
                TaskExecutionError::Retryable(message) => {
                    TaskFailure::retryable("handler", message)
                }
                TaskExecutionError::Permanent(message) => {
                    TaskFailure::permanent("handler", message)
                }
            };
            match repository
                .record_failure(
                    &task_run_id,
                    &worker_id,
                    expected_version,
                    fencing_token,
                    failure,
                )
                .await
            {
                Ok(_) => {
                    repository.aggregate_workflow_run(&workflow_run_id).await?;
                    Ok(())
                }
                Err(StorageError::ClaimLost(_)) => Ok(()),
                Err(error) => Err(error.into()),
            }
        }
        None => unreachable!("timeout returns directly from the select loop"),
    }
}
