//! Transactional PostgreSQL task repository operations.
//!
//! PostgreSQL owns durable deadlines and claim ownership. Each state transition
//! and its outbox row are committed in one short transaction.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};

use aetherflow_core::domain::{
    AttemptId, IdempotencyKey, RetryPolicy, TaskId, TaskRunId, TaskStatus, WorkerId, WorkflowId,
    WorkflowRunId, WorkflowStatus,
};
use aetherflow_runtime::repository::{TaskInput, TaskOutput};
use deadpool_postgres::Pool;
use serde_json::json;
use tokio_postgres::{Row, Transaction};

use crate::membership::lock_worker_registration;
use crate::{StorageError, WorkerRegistration};

const MAX_LEASE_MILLIS: u128 = i64::MAX as u128;

/// A handler-reported task failure and its retry classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskFailure {
    retryable: bool,
    kind: String,
    message: String,
}

impl TaskFailure {
    /// Creates a failure that may be retried while the persisted retry budget permits it.
    #[must_use]
    pub fn retryable(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            retryable: true,
            kind: kind.into(),
            message: message.into(),
        }
    }

    /// Creates a terminal failure that must not be retried.
    #[must_use]
    pub fn permanent(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            retryable: false,
            kind: kind.into(),
            message: message.into(),
        }
    }

    /// Returns whether the failure is eligible for retry.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        self.retryable
    }

    /// Returns the stable application-defined failure category.
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Returns the diagnostic failure message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// One task to insert as part of an atomic workflow creation.
#[derive(Clone, Debug)]
pub struct NewTaskRun {
    task_run_id: TaskRunId,
    task_id: TaskId,
    input: TaskInput,
    dependencies: Vec<TaskRunId>,
    retry_policy: RetryPolicy,
    available_at: SystemTime,
}

impl NewTaskRun {
    /// Creates a durable pending task-run definition.
    #[must_use]
    pub fn new(
        task_run_id: TaskRunId,
        task_id: TaskId,
        input: TaskInput,
        dependencies: Vec<TaskRunId>,
        retry_policy: RetryPolicy,
        available_at: SystemTime,
    ) -> Self {
        Self {
            task_run_id,
            task_id,
            input,
            dependencies,
            retry_policy,
            available_at,
        }
    }

    /// Returns this task-run identifier.
    #[must_use]
    pub fn task_run_id(&self) -> &TaskRunId {
        &self.task_run_id
    }
}

/// Snapshot of one task run loaded from PostgreSQL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableTaskRun {
    task_run_id: TaskRunId,
    workflow_run_id: WorkflowRunId,
    task_id: TaskId,
    status: TaskStatus,
    version: u64,
    attempts_started: u32,
    available_at: SystemTime,
    input: TaskInput,
    output: Option<TaskOutput>,
    dependencies: Vec<TaskRunId>,
    claimed_by: Option<WorkerId>,
    fencing_token: u64,
    lease_expires_at: Option<SystemTime>,
    created_at: SystemTime,
    updated_at: SystemTime,
}

impl DurableTaskRun {
    /// Returns the task-run identifier.
    #[must_use]
    pub fn task_run_id(&self) -> &TaskRunId {
        &self.task_run_id
    }

    /// Returns the parent workflow-run identifier.
    #[must_use]
    pub fn workflow_run_id(&self) -> &WorkflowRunId {
        &self.workflow_run_id
    }

    /// Returns the task definition identifier.
    #[must_use]
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Returns the latest lifecycle state.
    #[must_use]
    pub fn status(&self) -> TaskStatus {
        self.status
    }

    /// Returns the optimistic state version.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Returns the number of attempts claimed so far.
    #[must_use]
    pub fn attempts_started(&self) -> u32 {
        self.attempts_started
    }

    /// Returns the database scheduling timestamp.
    #[must_use]
    pub fn available_at(&self) -> SystemTime {
        self.available_at
    }

    /// Returns the immutable opaque input payload.
    #[must_use]
    pub fn input(&self) -> &TaskInput {
        &self.input
    }

    /// Returns the successful output payload, if present.
    #[must_use]
    pub fn output(&self) -> Option<&TaskOutput> {
        self.output.as_ref()
    }

    /// Returns direct predecessor task-run identifiers in definition order.
    #[must_use]
    pub fn dependencies(&self) -> &[TaskRunId] {
        &self.dependencies
    }

    /// Returns the current worker owner, if claimed.
    #[must_use]
    pub fn claimed_by(&self) -> Option<&WorkerId> {
        self.claimed_by.as_ref()
    }

    /// Returns the monotonically increasing fencing token.
    #[must_use]
    pub fn fencing_token(&self) -> u64 {
        self.fencing_token
    }

    /// Returns the current lease expiry.
    #[must_use]
    pub fn lease_expires_at(&self) -> Option<SystemTime> {
        self.lease_expires_at
    }

    /// Returns the database creation timestamp.
    #[must_use]
    pub fn created_at(&self) -> SystemTime {
        self.created_at
    }

    /// Returns the latest database update timestamp.
    #[must_use]
    pub fn updated_at(&self) -> SystemTime {
        self.updated_at
    }
}

/// Snapshot of one workflow run persisted in PostgreSQL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableWorkflowRun {
    workflow_run_id: WorkflowRunId,
    workflow_id: WorkflowId,
    status: WorkflowStatus,
    version: u64,
    created_at: SystemTime,
    updated_at: SystemTime,
    task_runs: Vec<DurableTaskRun>,
}

impl DurableWorkflowRun {
    /// Returns the workflow-run identifier.
    #[must_use]
    pub fn workflow_run_id(&self) -> &WorkflowRunId {
        &self.workflow_run_id
    }

    /// Returns the submitted workflow definition identifier.
    #[must_use]
    pub fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    /// Returns the persisted workflow lifecycle state.
    #[must_use]
    pub fn status(&self) -> WorkflowStatus {
        self.status
    }

    /// Returns the optimistic workflow-run version.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Returns the database creation timestamp.
    #[must_use]
    pub fn created_at(&self) -> SystemTime {
        self.created_at
    }

    /// Returns the latest database update timestamp.
    #[must_use]
    pub fn updated_at(&self) -> SystemTime {
        self.updated_at
    }

    /// Returns task snapshots in stable creation/identifier order.
    #[must_use]
    pub fn task_runs(&self) -> &[DurableTaskRun] {
        &self.task_runs
    }
}

/// A durable claim, including its attempt and fencing identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PgTaskClaim {
    task_run: DurableTaskRun,
    attempt_id: AttemptId,
}

impl PgTaskClaim {
    /// Returns the snapshot captured by the claim transaction.
    #[must_use]
    pub fn task_run(&self) -> &DurableTaskRun {
        &self.task_run
    }

    /// Returns the durable attempt identity.
    #[must_use]
    pub fn attempt_id(&self) -> &AttemptId {
        &self.attempt_id
    }

    /// Returns the one-based attempt number.
    #[must_use]
    pub fn attempt_number(&self) -> NonZeroU32 {
        NonZeroU32::new(self.task_run.attempts_started)
            .expect("a claimed task always has a non-zero attempt count")
    }

    /// Returns the token required for all worker writes.
    #[must_use]
    pub fn fencing_token(&self) -> u64 {
        self.task_run.fencing_token
    }
}

/// Result of recovering one expired worker lease.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredLease {
    task_run: DurableTaskRun,
    was_retried: bool,
}

impl RecoveredLease {
    /// Returns the task after lease recovery.
    #[must_use]
    pub fn task_run(&self) -> &DurableTaskRun {
        &self.task_run
    }

    /// Returns whether the task entered retry waiting.
    #[must_use]
    pub fn was_retried(&self) -> bool {
        self.was_retried
    }
}

/// A claimed outbox event. Publish it outside the database transaction, then
/// acknowledge or reschedule it with the same owner and claim token.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboxClaim {
    event_id: i64,
    aggregate_type: String,
    aggregate_id: String,
    aggregate_version: u64,
    event_type: String,
    payload: serde_json::Value,
    publisher_id: WorkerId,
    claim_token: u64,
    claim_expires_at: SystemTime,
    publish_attempts: u32,
}

impl OutboxClaim {
    /// Returns the stable database event ID.
    #[must_use]
    pub fn event_id(&self) -> i64 {
        self.event_id
    }

    /// Returns the aggregate category.
    #[must_use]
    pub fn aggregate_type(&self) -> &str {
        &self.aggregate_type
    }

    /// Returns the aggregate identifier.
    #[must_use]
    pub fn aggregate_id(&self) -> &str {
        &self.aggregate_id
    }

    /// Returns the aggregate lifecycle version.
    #[must_use]
    pub fn aggregate_version(&self) -> u64 {
        self.aggregate_version
    }

    /// Returns the event type.
    #[must_use]
    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    /// Returns the structured event payload.
    #[must_use]
    pub fn payload(&self) -> &serde_json::Value {
        &self.payload
    }

    /// Returns the publisher that owns this claim.
    #[must_use]
    pub fn publisher_id(&self) -> &WorkerId {
        &self.publisher_id
    }

    /// Returns the fencing token for this outbox claim.
    #[must_use]
    pub fn claim_token(&self) -> u64 {
        self.claim_token
    }

    /// Returns the lease expiry assigned by PostgreSQL.
    #[must_use]
    pub fn claim_expires_at(&self) -> SystemTime {
        self.claim_expires_at
    }

    /// Returns how many times this event has been claimed for publication.
    #[must_use]
    pub fn publish_attempts(&self) -> u32 {
        self.publish_attempts
    }
}

/// PostgreSQL-backed task repository with atomic claims and fencing.
#[derive(Clone)]
pub struct PgTaskRepository {
    pool: Pool,
}

impl PgTaskRepository {
    /// Creates a durable task repository from a caller-configured pool.
    #[must_use]
    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }

    /// Atomically creates a workflow, all task runs, and dependency edges.
    pub async fn create_workflow(
        &self,
        workflow_run_id: &WorkflowRunId,
        workflow_id: &WorkflowId,
        idempotency_key: Option<&IdempotencyKey>,
        tasks: &[NewTaskRun],
    ) -> Result<(), StorageError> {
        if tasks.is_empty() {
            return Err(StorageError::InvalidWorkflow(
                "workflow must contain at least one task".to_owned(),
            ));
        }

        let mut task_ids = BTreeSet::new();
        let mut task_run_ids = BTreeSet::new();
        for task in tasks {
            if !task_ids.insert(task.task_id.as_str().to_owned()) {
                return Err(StorageError::InvalidWorkflow(format!(
                    "duplicate task identifier: {}",
                    task.task_id.as_str()
                )));
            }
            if !task_run_ids.insert(task.task_run_id.as_str().to_owned()) {
                return Err(StorageError::InvalidWorkflow(format!(
                    "duplicate task-run identifier: {}",
                    task.task_run_id.as_str()
                )));
            }
            validate_retry_policy(task.retry_policy)?;
        }

        for task in tasks {
            let mut dependencies = BTreeSet::new();
            for dependency in &task.dependencies {
                if !task_run_ids.contains(dependency.as_str()) {
                    return Err(StorageError::InvalidWorkflow(format!(
                        "task {} references a dependency outside this workflow: {}",
                        task.task_id.as_str(),
                        dependency.as_str()
                    )));
                }
                if dependency == &task.task_run_id {
                    return Err(StorageError::InvalidWorkflow(format!(
                        "task {} cannot depend on itself",
                        task.task_id.as_str()
                    )));
                }
                if !dependencies.insert(dependency.as_str()) {
                    return Err(StorageError::InvalidWorkflow(format!(
                        "task {} declares dependency {} more than once",
                        task.task_id.as_str(),
                        dependency.as_str()
                    )));
                }
            }
        }

        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        transaction
            .execute(
                "INSERT INTO workflow_runs \
             (workflow_run_id, workflow_id, status, version, idempotency_key, started_at) \
             VALUES ($1, $2, 'running', 1, $3, clock_timestamp())",
                &[
                    &workflow_run_id.as_str(),
                    &workflow_id.as_str(),
                    &idempotency_key.map(IdempotencyKey::as_str),
                ],
            )
            .await
            .map_err(|error| StorageError::Database(format!("insert workflow run: {error:?}")))?;

        for task in tasks {
            let max_attempts = i32::try_from(task.retry_policy.max_attempts().get())
                .map_err(|_| StorageError::InvalidRetryPolicy)?;
            let initial_backoff = i64::try_from(task.retry_policy.initial_backoff_ms())
                .map_err(|_| StorageError::InvalidRetryPolicy)?;
            let maximum_backoff = i64::try_from(task.retry_policy.max_backoff_ms())
                .map_err(|_| StorageError::InvalidRetryPolicy)?;
            transaction
                .execute(
                    "INSERT INTO task_runs \
                 (task_run_id, workflow_run_id, task_id, status, max_attempts, \
                  initial_backoff_ms, max_backoff_ms, available_at, input_payload) \
                 VALUES ($1, $2, $3, 'pending', $4, $5, $6, $7, $8)",
                    &[
                        &task.task_run_id.as_str(),
                        &workflow_run_id.as_str(),
                        &task.task_id.as_str(),
                        &max_attempts,
                        &initial_backoff,
                        &maximum_backoff,
                        &task.available_at,
                        &task.input.as_bytes(),
                    ],
                )
                .await
                .map_err(|error| {
                    StorageError::Database(format!("insert task-run row: {error:?}"))
                })?;
        }

        for task in tasks {
            for (position, dependency) in task.dependencies.iter().enumerate() {
                let position = i32::try_from(position).map_err(|_| {
                    StorageError::InvalidWorkflow("too many dependencies".to_owned())
                })?;
                transaction
                    .execute(
                        "INSERT INTO task_run_dependencies \
                     (workflow_run_id, child_task_run_id, parent_task_run_id, dependency_position) \
                     VALUES ($1, $2, $3, $4)",
                        &[
                            &workflow_run_id.as_str(),
                            &task.task_run_id.as_str(),
                            &dependency.as_str(),
                            &position,
                        ],
                    )
                    .await
                    .map_err(|error| {
                        StorageError::Database(format!("insert dependency edge: {error:?}"))
                    })?;
            }
        }

        append_outbox(
            &transaction,
            "workflow_run",
            workflow_run_id.as_str(),
            1,
            "workflow.started",
            json!({"workflow_id": workflow_id.as_str()}),
        )
        .await?;

        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(())
    }

    /// Promotes due pending/retry-waiting tasks after every direct dependency succeeds.
    pub async fn promote_due_tasks(&self, limit: i64) -> Result<Vec<TaskRunId>, StorageError> {
        self.promote_due_tasks_inner(None, limit).await
    }

    /// Promotes tasks only while the supplied worker registration is active.
    pub async fn promote_due_tasks_registered(
        &self,
        registration: &WorkerRegistration,
        limit: i64,
    ) -> Result<Vec<TaskRunId>, StorageError> {
        self.promote_due_tasks_inner(Some(registration), limit)
            .await
    }

    async fn promote_due_tasks_inner(
        &self,
        registration: Option<&WorkerRegistration>,
        limit: i64,
    ) -> Result<Vec<TaskRunId>, StorageError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        if let Some(registration) = registration {
            lock_worker_registration(&transaction, registration, true).await?;
        }
        let rows = transaction.query(
            "WITH candidates AS ( \
                SELECT task.task_run_id \
                FROM task_runs AS task \
                WHERE task.status IN ('pending', 'retry_waiting') \
                  AND task.available_at <= clock_timestamp() \
                  AND NOT EXISTS ( \
                      SELECT 1 \
                      FROM task_run_dependencies AS edge \
                      JOIN task_runs AS parent \
                        ON parent.workflow_run_id = edge.workflow_run_id \
                       AND parent.task_run_id = edge.parent_task_run_id \
                      WHERE edge.workflow_run_id = task.workflow_run_id \
                        AND edge.child_task_run_id = task.task_run_id \
                        AND parent.status <> 'succeeded' \
                  ) \
                ORDER BY task.available_at, task.created_at, task.task_run_id \
                LIMIT $1 FOR UPDATE OF task SKIP LOCKED \
            ), updated AS ( \
                UPDATE task_runs AS task \
                SET status = 'ready', version = task.version + 1, updated_at = clock_timestamp() \
                FROM candidates \
                WHERE task.task_run_id = candidates.task_run_id \
                RETURNING task.task_run_id, task.version \
            ), events AS ( \
                INSERT INTO outbox_events \
                    (aggregate_type, aggregate_id, aggregate_version, event_type, payload) \
                SELECT 'task_run', task_run_id, version, 'task.ready', \
                       jsonb_build_object('task_run_id', task_run_id, 'status', 'ready', 'version', version) \
                FROM updated RETURNING event_id \
            ) \
            SELECT task_run_id FROM updated ORDER BY task_run_id",
            &[&limit],
        ).await.map_err(|error| StorageError::Database(error.to_string()))?;

        let mut promoted = Vec::with_capacity(rows.len());
        for row in rows {
            promoted.push(parse_task_run_id(row.get(0))?);
        }
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(promoted)
    }

    /// Claims one ready task with SKIP LOCKED, an attempt row, fencing token, and outbox event.
    ///
    /// This low-level API does not validate cluster membership. Distributed
    /// workers must use `claim_next_registered`.
    pub async fn claim_next(
        &self,
        worker_id: &WorkerId,
        attempt_id: &AttemptId,
        lease_duration: Duration,
    ) -> Result<Option<PgTaskClaim>, StorageError> {
        self.claim_next_inner(worker_id, attempt_id, lease_duration, None, None)
            .await
    }

    /// Claims work only while the supplied node incarnation and worker
    /// registration are active and unexpired in the same PostgreSQL transaction.
    pub async fn claim_next_registered(
        &self,
        registration: &WorkerRegistration,
        attempt_id: &AttemptId,
        lease_duration: Duration,
    ) -> Result<Option<PgTaskClaim>, StorageError> {
        self.claim_next_inner(
            registration.worker_id(),
            attempt_id,
            lease_duration,
            Some(registration),
            None,
        )
        .await
    }

    /// Claims a task only when its definition is supported by the worker's registry.
    ///
    /// The capability filter is applied in the same claim transaction, so a
    /// worker cannot lease another worker's task and then fail it for a missing
    /// handler. An empty supported-task list claims no work.
    pub async fn claim_next_registered_for_tasks(
        &self,
        registration: &WorkerRegistration,
        supported_task_ids: &[TaskId],
        attempt_id: &AttemptId,
        lease_duration: Duration,
    ) -> Result<Option<PgTaskClaim>, StorageError> {
        self.claim_next_inner(
            registration.worker_id(),
            attempt_id,
            lease_duration,
            Some(registration),
            Some(supported_task_ids),
        )
        .await
    }

    async fn claim_next_inner(
        &self,
        worker_id: &WorkerId,
        attempt_id: &AttemptId,
        lease_duration: Duration,
        registration: Option<&WorkerRegistration>,
        supported_task_ids: Option<&[TaskId]>,
    ) -> Result<Option<PgTaskClaim>, StorageError> {
        let lease_seconds = lease_seconds(lease_duration)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        if let Some(registration) = registration {
            if registration.worker_id() != worker_id {
                return Err(StorageError::WorkerRegistrationLost(
                    worker_id.as_str().to_owned(),
                ));
            }
            lock_worker_registration(&transaction, registration, true).await?;
        }
        let supported_task_ids = supported_task_ids.map(|task_ids| {
            task_ids
                .iter()
                .map(|task_id| task_id.as_str().to_owned())
                .collect::<Vec<_>>()
        });
        let candidate = transaction
            .query_opt(
                "SELECT task.task_run_id \
             FROM task_runs AS task \
             WHERE task.status = 'ready' \
               AND task.available_at <= clock_timestamp() \
               AND task.attempts_started < task.max_attempts \
               AND ($1::text[] IS NULL OR task.task_id = ANY($1)) \
               AND NOT EXISTS ( \
                   SELECT 1 \
                   FROM task_run_dependencies AS edge \
                   JOIN task_runs AS parent \
                     ON parent.workflow_run_id = edge.workflow_run_id \
                    AND parent.task_run_id = edge.parent_task_run_id \
                   WHERE edge.workflow_run_id = task.workflow_run_id \
                     AND edge.child_task_run_id = task.task_run_id \
                     AND parent.status <> 'succeeded' \
               ) \
             ORDER BY task.available_at, task.created_at, task.task_run_id \
             LIMIT 1 FOR UPDATE OF task SKIP LOCKED",
                &[&supported_task_ids],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        let Some(candidate) = candidate else {
            transaction
                .commit()
                .await
                .map_err(|error| StorageError::Database(error.to_string()))?;
            return Ok(None);
        };
        let task_run_id: String = candidate.get(0);
        let updated = transaction.query_opt(
            "UPDATE task_runs \
             SET status = 'running', version = version + 1, \
                 attempts_started = attempts_started + 1, fencing_token = fencing_token + 1, \
                 claimed_by = $2, \
                 lease_expires_at = clock_timestamp() + make_interval(secs => $3), \
                 started_at = COALESCE(started_at, clock_timestamp()), updated_at = clock_timestamp() \
             WHERE task_run_id = $1 AND status = 'ready' \
             RETURNING attempts_started, fencing_token, version",
            &[&task_run_id, &worker_id.as_str(), &lease_seconds],
        ).await.map_err(|error| StorageError::Database(error.to_string()))?
         .ok_or_else(|| StorageError::InconsistentState(
             "locked ready task disappeared before claim update".to_owned(),
         ))?;

        let attempt_number: i32 = updated.get(0);
        let fencing_token: i64 = updated.get(1);
        let version: i64 = updated.get(2);
        transaction
            .execute(
                "INSERT INTO task_attempts \
             (attempt_id, task_run_id, attempt_number, worker_id, fencing_token, outcome) \
             VALUES ($1, $2, $3, $4, $5, 'running')",
                &[
                    &attempt_id.as_str(),
                    &task_run_id,
                    &attempt_number,
                    &worker_id.as_str(),
                    &fencing_token,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        append_outbox(
            &transaction,
            "task_run",
            &task_run_id,
            version,
            "task.claimed",
            json!({
                "task_run_id": task_run_id,
                "worker_id": worker_id.as_str(),
                "attempt_number": attempt_number,
                "fencing_token": fencing_token
            }),
        )
        .await?;

        let snapshot = load_snapshot(&transaction, &task_run_id).await?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(Some(PgTaskClaim {
            task_run: snapshot,
            attempt_id: attempt_id.clone(),
        }))
    }

    /// Extends a live lease if owner, version, and fencing token still match.
    /// Extends a live lease if owner, version, and fencing token still match.
    /// This low-level API does not validate cluster membership.
    pub async fn heartbeat(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        fencing_token: u64,
        lease_duration: Duration,
    ) -> Result<SystemTime, StorageError> {
        self.heartbeat_inner(
            task_run_id,
            worker_id,
            expected_version,
            fencing_token,
            lease_duration,
            None,
        )
        .await
    }

    /// Heartbeats an existing attempt while its worker registration remains current.
    /// Draining registrations may finish in-flight work, but stale epochs are rejected.
    pub async fn heartbeat_registered(
        &self,
        task_run_id: &TaskRunId,
        registration: &WorkerRegistration,
        expected_version: u64,
        fencing_token: u64,
        lease_duration: Duration,
    ) -> Result<SystemTime, StorageError> {
        self.heartbeat_inner(
            task_run_id,
            registration.worker_id(),
            expected_version,
            fencing_token,
            lease_duration,
            Some(registration),
        )
        .await
    }

    async fn heartbeat_inner(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        fencing_token: u64,
        lease_duration: Duration,
        registration: Option<&WorkerRegistration>,
    ) -> Result<SystemTime, StorageError> {
        let lease_seconds = lease_seconds(lease_duration)?;
        let expected_version = sql_i64(expected_version)?;
        let fencing_token = sql_i64(fencing_token)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        if let Some(registration) = registration {
            if registration.worker_id() != worker_id {
                return Err(StorageError::WorkerRegistrationLost(
                    worker_id.as_str().to_owned(),
                ));
            }
            // Existing work may settle while draining, but a stale epoch cannot write.
            lock_worker_registration(&transaction, registration, false).await?;
        }
        let row = transaction
            .query_opt(
                "UPDATE task_runs \
             SET lease_expires_at = clock_timestamp() + make_interval(secs => $5) \
             WHERE task_run_id = $1 AND status = 'running' AND claimed_by = $2 \
               AND version = $3 AND fencing_token = $4 \
               AND lease_expires_at > clock_timestamp() \
             RETURNING attempts_started, lease_expires_at",
                &[
                    &task_run_id.as_str(),
                    &worker_id.as_str(),
                    &expected_version,
                    &fencing_token,
                    &lease_seconds,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let row = row.ok_or_else(|| StorageError::ClaimLost(task_run_id.as_str().to_owned()))?;
        let attempt_number: i32 = row.get(0);
        let expiry: SystemTime = row.get(1);
        let updated = transaction
            .execute(
                "UPDATE task_attempts SET heartbeat_at = clock_timestamp() \
             WHERE task_run_id = $1 AND attempt_number = $2 AND worker_id = $3 \
               AND fencing_token = $4 AND outcome = 'running'",
                &[
                    &task_run_id.as_str(),
                    &attempt_number,
                    &worker_id.as_str(),
                    &fencing_token,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        if updated != 1 {
            return Err(StorageError::ClaimLost(task_run_id.as_str().to_owned()));
        }
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(expiry)
    }

    /// Atomically stores successful output, completes the attempt, and appends the outbox event.
    /// Atomically stores successful output and appends the outbox event.
    /// This low-level API does not validate cluster membership.
    pub async fn complete(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        fencing_token: u64,
        output: TaskOutput,
    ) -> Result<DurableTaskRun, StorageError> {
        self.complete_inner(
            task_run_id,
            worker_id,
            expected_version,
            fencing_token,
            output,
            None,
        )
        .await
    }

    /// Completes an attempt only while the worker and parent node epoch remain current.
    /// A draining registration may settle a task that it already owns.
    pub async fn complete_registered(
        &self,
        task_run_id: &TaskRunId,
        registration: &WorkerRegistration,
        expected_version: u64,
        fencing_token: u64,
        output: TaskOutput,
    ) -> Result<DurableTaskRun, StorageError> {
        self.complete_inner(
            task_run_id,
            registration.worker_id(),
            expected_version,
            fencing_token,
            output,
            Some(registration),
        )
        .await
    }

    async fn complete_inner(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        fencing_token: u64,
        output: TaskOutput,
        registration: Option<&WorkerRegistration>,
    ) -> Result<DurableTaskRun, StorageError> {
        let expected_version = sql_i64(expected_version)?;
        let fencing_token = sql_i64(fencing_token)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        if let Some(registration) = registration {
            if registration.worker_id() != worker_id {
                return Err(StorageError::WorkerRegistrationLost(
                    worker_id.as_str().to_owned(),
                ));
            }
            // Existing work may settle while draining, but a stale epoch cannot write.
            lock_worker_registration(&transaction, registration, false).await?;
        }
        let row = transaction
            .query_opt(
                "UPDATE task_runs \
             SET status = 'succeeded', output_payload = $5, claimed_by = NULL, \
                 lease_expires_at = NULL, version = version + 1, \
                 updated_at = clock_timestamp(), finished_at = clock_timestamp() \
             WHERE task_run_id = $1 AND status = 'running' AND claimed_by = $2 \
               AND version = $3 AND fencing_token = $4 \
               AND lease_expires_at > clock_timestamp() \
             RETURNING version, attempts_started",
                &[
                    &task_run_id.as_str(),
                    &worker_id.as_str(),
                    &expected_version,
                    &fencing_token,
                    &output.as_bytes(),
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let row = row.ok_or_else(|| StorageError::ClaimLost(task_run_id.as_str().to_owned()))?;
        let version: i64 = row.get(0);
        let attempt_number: i32 = row.get(1);
        let attempt_updated = transaction
            .execute(
                "UPDATE task_attempts \
             SET outcome = 'succeeded', finished_at = clock_timestamp() \
             WHERE task_run_id = $1 AND attempt_number = $2 AND worker_id = $3 \
               AND fencing_token = $4 AND outcome = 'running'",
                &[
                    &task_run_id.as_str(),
                    &attempt_number,
                    &worker_id.as_str(),
                    &fencing_token,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        if attempt_updated != 1 {
            return Err(StorageError::InconsistentState(
                "successful task transition did not match one running attempt".to_owned(),
            ));
        }
        append_outbox(
            &transaction,
            "task_run",
            task_run_id.as_str(),
            version,
            "task.succeeded",
            json!({"task_run_id": task_run_id.as_str(), "status": "succeeded", "version": version}),
        )
        .await?;
        let snapshot = load_snapshot(&transaction, task_run_id.as_str()).await?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(snapshot)
    }

    /// Records a worker-reported failure and schedules retry or commits terminal failure.
    ///
    /// The task state, attempt outcome, and outbox event commit atomically. A
    /// retryable error only schedules another attempt while the persisted budget
    /// permits it; otherwise the task becomes failed.
    /// Records a worker-reported failure through the low-level membership-free API.
    pub async fn record_failure(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        fencing_token: u64,
        failure: TaskFailure,
    ) -> Result<DurableTaskRun, StorageError> {
        self.record_failure_inner(
            task_run_id,
            worker_id,
            expected_version,
            fencing_token,
            failure,
            None,
        )
        .await
    }

    /// Records a handler failure while validating its worker and node epoch.
    /// A draining registration may settle a task that it already owns.
    pub async fn record_failure_registered(
        &self,
        task_run_id: &TaskRunId,
        registration: &WorkerRegistration,
        expected_version: u64,
        fencing_token: u64,
        failure: TaskFailure,
    ) -> Result<DurableTaskRun, StorageError> {
        self.record_failure_inner(
            task_run_id,
            registration.worker_id(),
            expected_version,
            fencing_token,
            failure,
            Some(registration),
        )
        .await
    }

    async fn record_failure_inner(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        fencing_token: u64,
        failure: TaskFailure,
        registration: Option<&WorkerRegistration>,
    ) -> Result<DurableTaskRun, StorageError> {
        let expected_version = sql_i64(expected_version)?;
        let fencing_token = sql_i64(fencing_token)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        if let Some(registration) = registration {
            if registration.worker_id() != worker_id {
                return Err(StorageError::WorkerRegistrationLost(
                    worker_id.as_str().to_owned(),
                ));
            }
            // Existing work may settle while draining, but a stale epoch cannot write.
            lock_worker_registration(&transaction, registration, false).await?;
        }

        let row = transaction
            .query_opt(
                "SELECT attempts_started, max_attempts, initial_backoff_ms, max_backoff_ms \
             FROM task_runs \
             WHERE task_run_id = $1 AND status = 'running' AND claimed_by = $2 \
               AND version = $3 AND fencing_token = $4 \
               AND lease_expires_at > clock_timestamp() \
             FOR UPDATE",
                &[
                    &task_run_id.as_str(),
                    &worker_id.as_str(),
                    &expected_version,
                    &fencing_token,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?
            .ok_or_else(|| StorageError::ClaimLost(task_run_id.as_str().to_owned()))?;

        let attempts_started: i32 = row.get(0);
        let max_attempts: i32 = row.get(1);
        let initial_backoff_ms: i64 = row.get(2);
        let max_backoff_ms: i64 = row.get(3);
        let attempts = u32::try_from(attempts_started)
            .map_err(|_| StorageError::InvalidStoredValue("negative attempt count".to_owned()))?;
        let max_attempts = u32::try_from(max_attempts)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| StorageError::InvalidStoredValue("invalid retry budget".to_owned()))?;
        let policy = RetryPolicy::new(
            max_attempts,
            u64::try_from(initial_backoff_ms).map_err(|_| {
                StorageError::InvalidStoredValue("negative initial backoff".to_owned())
            })?,
            u64::try_from(max_backoff_ms).map_err(|_| {
                StorageError::InvalidStoredValue("negative maximum backoff".to_owned())
            })?,
        )
        .map_err(|error| StorageError::InvalidStoredValue(error.to_string()))?;

        let will_retry = failure.is_retryable() && policy.permits_retry(attempts);
        let retry_delay_seconds = if will_retry {
            let retry_number = NonZeroU32::new(attempts).ok_or_else(|| {
                StorageError::InvalidStoredValue("running task has no attempts".to_owned())
            })?;
            Duration::from_millis(policy.backoff_ms(retry_number)).as_secs_f64()
        } else {
            0.0
        };
        let attempt_outcome = if failure.is_retryable() {
            "retryable_failure"
        } else {
            "permanent_failure"
        };
        let attempt_updated = transaction.execute(
            "UPDATE task_attempts \
             SET outcome = $5, finished_at = clock_timestamp(), error_kind = $6, error_message = $7 \
             WHERE task_run_id = $1 AND attempt_number = $2 AND worker_id = $3 \
               AND fencing_token = $4 AND outcome = 'running'",
            &[
                &task_run_id.as_str(),
                &attempts_started,
                &worker_id.as_str(),
                &fencing_token,
                &attempt_outcome,
                &failure.kind(),
                &failure.message(),
            ],
        ).await.map_err(|error| StorageError::Database(error.to_string()))?;
        if attempt_updated != 1 {
            return Err(StorageError::InconsistentState(
                "task failure did not match one running attempt".to_owned(),
            ));
        }

        let next_status = if will_retry {
            "retry_waiting"
        } else {
            "failed"
        };
        let updated = transaction
            .query_one(
                "UPDATE task_runs \
             SET status = $5, claimed_by = NULL, lease_expires_at = NULL, \
                 available_at = CASE WHEN $5 = 'retry_waiting' \
                     THEN clock_timestamp() + make_interval(secs => $6) \
                     ELSE available_at END, \
                 version = version + 1, updated_at = clock_timestamp(), \
                 finished_at = CASE WHEN $5 = 'failed' THEN clock_timestamp() ELSE NULL END, \
                 last_error_kind = $7, last_error_message = $8 \
             WHERE task_run_id = $1 AND status = 'running' AND version = $2 \
               AND claimed_by = $3 AND fencing_token = $4 \
             RETURNING version",
                &[
                    &task_run_id.as_str(),
                    &expected_version,
                    &worker_id.as_str(),
                    &fencing_token,
                    &next_status,
                    &retry_delay_seconds,
                    &failure.kind(),
                    &failure.message(),
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let version: i64 = updated.get(0);
        let event_type = if will_retry {
            "task.retry_scheduled"
        } else {
            "task.failed"
        };
        append_outbox(
            &transaction,
            "task_run",
            task_run_id.as_str(),
            version,
            event_type,
            json!({
                "task_run_id": task_run_id.as_str(),
                "status": next_status,
                "error_kind": failure.kind(),
                "error_message": failure.message(),
                "version": version
            }),
        )
        .await?;
        let snapshot = load_snapshot(&transaction, task_run_id.as_str()).await?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(snapshot)
    }

    /// Commits task cancellation, attempt completion, and its outbox event atomically.
    ///
    /// Cancellation is version-guarded. A terminal task or stale version cannot
    /// overwrite a completion, timeout, or another cancellation.
    pub async fn cancel_task_run(
        &self,
        task_run_id: &TaskRunId,
        expected_version: u64,
    ) -> Result<DurableTaskRun, StorageError> {
        self.terminate_task_run(task_run_id, expected_version, "cancelled", None, None)
            .await
    }

    /// Cancels a running task only while the registered worker still owns
    /// its task fencing token and its parent node epoch remains live.
    /// Draining workers may cancel work already assigned to them.
    pub async fn cancel_task_run_registered(
        &self,
        task_run_id: &TaskRunId,
        registration: &WorkerRegistration,
        expected_version: u64,
        fencing_token: u64,
    ) -> Result<DurableTaskRun, StorageError> {
        self.terminate_task_run(
            task_run_id,
            expected_version,
            "cancelled",
            Some(registration),
            Some(fencing_token),
        )
        .await
    }

    /// Commits a task timeout, attempt completion, and its outbox event atomically.
    ///
    /// The runtime decides when a timeout deadline elapses; PostgreSQL serializes
    /// the terminal transition against concurrent completion and cancellation.
    pub async fn timeout_task_run(
        &self,
        task_run_id: &TaskRunId,
        expected_version: u64,
    ) -> Result<DurableTaskRun, StorageError> {
        self.terminate_task_run(task_run_id, expected_version, "timed_out", None, None)
            .await
    }

    /// Times out a running task only while the registered worker still owns its
    /// current task fencing token and its parent node epoch remains live.
    pub async fn timeout_task_run_registered(
        &self,
        task_run_id: &TaskRunId,
        registration: &WorkerRegistration,
        expected_version: u64,
        fencing_token: u64,
    ) -> Result<DurableTaskRun, StorageError> {
        self.terminate_task_run(
            task_run_id,
            expected_version,
            "timed_out",
            Some(registration),
            Some(fencing_token),
        )
        .await
    }

    async fn terminate_task_run(
        &self,
        task_run_id: &TaskRunId,
        expected_version: u64,
        next_status: &str,
        registration: Option<&WorkerRegistration>,
        expected_fencing_token: Option<u64>,
    ) -> Result<DurableTaskRun, StorageError> {
        let expected_version = sql_i64(expected_version)?;
        let (attempt_outcome, event_type, error_kind, error_message) = match next_status {
            "cancelled" => (
                "cancelled",
                "task.cancelled",
                "cancelled",
                "task cancellation requested",
            ),
            "timed_out" => (
                "timed_out",
                "task.timed_out",
                "timeout",
                "task attempt timed out",
            ),
            other => {
                return Err(StorageError::InvalidStoredValue(format!(
                    "unsupported terminal task status: {other}"
                )));
            }
        };

        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        if let Some(registration) = registration {
            lock_worker_registration(&transaction, registration, false).await?;
        }

        let row = transaction
            .query_opt(
                "SELECT status, attempts_started, version, claimed_by, fencing_token \
                 FROM task_runs WHERE task_run_id = $1 FOR UPDATE",
                &[&task_run_id.as_str()],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?
            .ok_or_else(|| StorageError::TaskRunNotFound(task_run_id.as_str().to_owned()))?;

        let current_status: String = row.get(0);
        let attempts_started: i32 = row.get(1);
        let actual_version: i64 = row.get(2);
        let claimed_by: Option<String> = row.get(3);
        let fencing_token: i64 = row.get(4);

        if let Some(registration) = registration {
            let expected_fencing_token = expected_fencing_token.ok_or_else(|| {
                StorageError::InconsistentState(
                    "registered timeout is missing a task fencing token".to_owned(),
                )
            })?;
            if current_status != "running"
                || claimed_by.as_deref() != Some(registration.worker_id().as_str())
                || u64::try_from(fencing_token).ok() != Some(expected_fencing_token)
            {
                return Err(StorageError::ClaimLost(task_run_id.as_str().to_owned()));
            }
        }

        if actual_version != expected_version {
            return Err(StorageError::ClaimLost(format!(
                "task run {} is at version {}, expected {}",
                task_run_id.as_str(),
                actual_version,
                expected_version
            )));
        }
        if !matches!(
            current_status.as_str(),
            "pending" | "ready" | "running" | "retry_waiting"
        ) {
            return Err(StorageError::ClaimLost(format!(
                "task run {} is already terminal ({current_status})",
                task_run_id.as_str()
            )));
        }

        if current_status == "running" {
            let attempt_number = attempts_started;
            if attempt_number <= 0 {
                return Err(StorageError::InconsistentState(format!(
                    "running task {} has no recorded attempt",
                    task_run_id.as_str()
                )));
            }
            let worker_id = claimed_by.ok_or_else(|| {
                StorageError::InconsistentState(format!(
                    "running task {} has no lease owner",
                    task_run_id.as_str()
                ))
            })?;
            if fencing_token <= 0 {
                return Err(StorageError::InconsistentState(format!(
                    "running task {} has an invalid fencing token",
                    task_run_id.as_str()
                )));
            }

            let attempt_updated = transaction
                .execute(
                    "UPDATE task_attempts \
                     SET outcome = $4, finished_at = clock_timestamp(), \
                         error_kind = $5, error_message = $6 \
                     WHERE task_run_id = $1 AND attempt_number = $2 \
                       AND worker_id = $3 AND fencing_token = $7 AND outcome = 'running'",
                    &[
                        &task_run_id.as_str(),
                        &attempt_number,
                        &worker_id,
                        &attempt_outcome,
                        &error_kind,
                        &error_message,
                        &fencing_token,
                    ],
                )
                .await
                .map_err(|error| StorageError::Database(error.to_string()))?;
            if attempt_updated != 1 {
                return Err(StorageError::InconsistentState(format!(
                    "terminal transition for task {} did not match one running attempt",
                    task_run_id.as_str()
                )));
            }
        }

        let updated = transaction
            .query_one(
                "UPDATE task_runs \
                 SET status = $3, claimed_by = NULL, lease_expires_at = NULL, \
                     version = version + 1, updated_at = clock_timestamp(), \
                     finished_at = clock_timestamp(), \
                     last_error_kind = CASE WHEN $3 = 'timed_out' THEN $4 ELSE last_error_kind END, \
                     last_error_message = CASE WHEN $3 = 'timed_out' THEN $5 ELSE last_error_message END \
                 WHERE task_run_id = $1 AND version = $2 AND status = $6 \
                 RETURNING version",
                &[
                    &task_run_id.as_str(),
                    &expected_version,
                    &next_status,
                    &error_kind,
                    &error_message,
                    &current_status,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let version: i64 = updated.get(0);

        append_outbox(
            &transaction,
            "task_run",
            task_run_id.as_str(),
            version,
            event_type,
            json!({
                "task_run_id": task_run_id.as_str(),
                "status": next_status,
                "version": version
            }),
        )
        .await?;

        let snapshot = load_snapshot(&transaction, task_run_id.as_str()).await?;
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(snapshot)
    }

    /// Recovers expired leases in bounded batches. A later claim gets a larger fencing token.
    pub async fn recover_expired_leases(
        &self,
        limit: i64,
    ) -> Result<Vec<RecoveredLease>, StorageError> {
        self.recover_expired_leases_inner(None, limit).await
    }

    /// Recovers expired leases only while this worker has a live active node
    /// and worker registration in the same PostgreSQL transaction.
    pub async fn recover_expired_leases_registered(
        &self,
        registration: &WorkerRegistration,
        limit: i64,
    ) -> Result<Vec<RecoveredLease>, StorageError> {
        self.recover_expired_leases_inner(Some(registration), limit)
            .await
    }

    async fn recover_expired_leases_inner(
        &self,
        registration: Option<&WorkerRegistration>,
        limit: i64,
    ) -> Result<Vec<RecoveredLease>, StorageError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        if let Some(registration) = registration {
            lock_worker_registration(&transaction, registration, true).await?;
        }
        let expired = transaction
            .query(
                "SELECT task_run_id, attempts_started, max_attempts, initial_backoff_ms, \
                    max_backoff_ms, claimed_by, fencing_token \
             FROM task_runs \
             WHERE status = 'running' AND lease_expires_at <= clock_timestamp() \
             ORDER BY lease_expires_at, task_run_id \
             LIMIT $1 FOR UPDATE SKIP LOCKED",
                &[&limit],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        let mut recovered = Vec::with_capacity(expired.len());
        for row in expired {
            let task_run_id: String = row.get(0);
            let attempts_started: i32 = row.get(1);
            let max_attempts: i32 = row.get(2);
            let initial_backoff_ms: i64 = row.get(3);
            let max_backoff_ms: i64 = row.get(4);
            let worker_id: String = row.get(5);
            let fencing_token: i64 = row.get(6);
            let attempts_u32 = u32::try_from(attempts_started).map_err(|_| {
                StorageError::InvalidStoredValue("negative attempt count".to_owned())
            })?;
            let max_attempts_u32 = u32::try_from(max_attempts)
                .ok()
                .and_then(NonZeroU32::new)
                .ok_or_else(|| {
                    StorageError::InvalidStoredValue("invalid retry budget".to_owned())
                })?;
            let policy = RetryPolicy::new(
                max_attempts_u32,
                u64::try_from(initial_backoff_ms).map_err(|_| {
                    StorageError::InvalidStoredValue("negative initial backoff".to_owned())
                })?,
                u64::try_from(max_backoff_ms).map_err(|_| {
                    StorageError::InvalidStoredValue("negative maximum backoff".to_owned())
                })?,
            )
            .map_err(|error| StorageError::InvalidStoredValue(error.to_string()))?;
            let will_retry = policy.permits_retry(attempts_u32);
            let delay = if will_retry {
                let retry_number = NonZeroU32::new(attempts_u32).ok_or_else(|| {
                    StorageError::InvalidStoredValue("running task has no attempts".to_owned())
                })?;
                Duration::from_millis(policy.backoff_ms(retry_number)).as_secs_f64()
            } else {
                0.0
            };

            let attempt_updated = transaction
                .execute(
                    "UPDATE task_attempts \
                 SET outcome = 'lease_expired', finished_at = clock_timestamp(), \
                     error_kind = 'lease_expired', error_message = 'worker lease expired' \
                 WHERE task_run_id = $1 AND attempt_number = $2 AND worker_id = $3 \
                   AND fencing_token = $4 AND outcome = 'running'",
                    &[&task_run_id, &attempts_started, &worker_id, &fencing_token],
                )
                .await
                .map_err(|error| StorageError::Database(error.to_string()))?;
            if attempt_updated != 1 {
                return Err(StorageError::InconsistentState(format!(
                    "expired task {task_run_id} had no matching running attempt"
                )));
            }

            let next_status = if will_retry {
                "retry_waiting"
            } else {
                "failed"
            };
            let updated = transaction.query_one(
                "UPDATE task_runs \
                 SET status = $2, claimed_by = NULL, lease_expires_at = NULL, \
                     available_at = CASE WHEN $2 = 'retry_waiting' \
                         THEN clock_timestamp() + make_interval(secs => $3) \
                         ELSE available_at END, \
                     version = version + 1, updated_at = clock_timestamp(), \
                     finished_at = CASE WHEN $2 = 'failed' THEN clock_timestamp() ELSE NULL END, \
                     last_error_kind = 'lease_expired', last_error_message = 'worker lease expired' \
                 WHERE task_run_id = $1 AND status = 'running' RETURNING version",
                &[&task_run_id, &next_status, &delay],
            ).await.map_err(|error| StorageError::Database(error.to_string()))?;
            let version: i64 = updated.get(0);
            append_outbox(
                &transaction, "task_run", &task_run_id, version,
                if will_retry { "task.retry_scheduled" } else { "task.failed" },
                json!({"task_run_id": task_run_id, "status": next_status, "reason": "lease_expired", "version": version}),
            ).await?;
            let id = parse_task_run_id(task_run_id)?;
            let snapshot = load_snapshot(&transaction, id.as_str()).await?;
            recovered.push(RecoveredLease {
                task_run: snapshot,
                was_retried: will_retry,
            });
        }

        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(recovered)
    }

    /// Claims pending outbox events for publication using row locks and claim fencing.
    ///
    /// Return these events to the caller, publish outside the transaction, then
    /// call acknowledge_outbox_event or retry_outbox_event with the returned token.
    pub async fn claim_outbox_events(
        &self,
        publisher_id: &WorkerId,
        limit: i64,
        lease_duration: Duration,
    ) -> Result<Vec<OutboxClaim>, StorageError> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let lease_seconds = lease_seconds(lease_duration)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let rows = transaction
            .query(
                "WITH candidates AS ( \
                SELECT event_id \
                FROM outbox_events \
                WHERE published_at IS NULL AND available_at <= clock_timestamp() \
                  AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp()) \
                ORDER BY available_at, event_id \
                LIMIT $1 FOR UPDATE SKIP LOCKED \
            ) \
            UPDATE outbox_events AS event \
            SET claim_owner = $2, claim_token = event.claim_token + 1, \
                claim_expires_at = clock_timestamp() + make_interval(secs => $3), \
                publish_attempts = event.publish_attempts + 1 \
            FROM candidates \
            WHERE event.event_id = candidates.event_id \
            RETURNING event.event_id, event.aggregate_type, event.aggregate_id, \
                      event.aggregate_version, event.event_type, event.payload, \
                      event.claim_owner, event.claim_token, event.claim_expires_at, \
                      event.publish_attempts",
                &[&limit, &publisher_id.as_str(), &lease_seconds],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let event_id: i64 = row.get(0);
            let aggregate_type: String = row.get(1);
            let aggregate_id: String = row.get(2);
            let aggregate_version: i64 = row.get(3);
            let event_type: String = row.get(4);
            let payload: serde_json::Value = row.get(5);
            let owner: String = row.get(6);
            let claim_token: i64 = row.get(7);
            let claim_expires_at: SystemTime = row.get(8);
            let publish_attempts: i32 = row.get(9);
            claims.push(OutboxClaim {
                event_id,
                aggregate_type,
                aggregate_id,
                aggregate_version: u64::try_from(aggregate_version).map_err(|_| {
                    StorageError::InvalidStoredValue("negative outbox aggregate version".to_owned())
                })?,
                event_type,
                payload,
                publisher_id: WorkerId::parse(owner)
                    .map_err(|error| StorageError::InvalidStoredValue(error.to_string()))?,
                claim_token: u64::try_from(claim_token).map_err(|_| {
                    StorageError::InvalidStoredValue("negative outbox claim token".to_owned())
                })?,
                claim_expires_at,
                publish_attempts: u32::try_from(publish_attempts).map_err(|_| {
                    StorageError::InvalidStoredValue("negative outbox publish count".to_owned())
                })?,
            });
        }
        claims.sort_by_key(OutboxClaim::event_id);
        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(claims)
    }

    /// Marks an event delivered only while the publisher still owns an unexpired claim.
    pub async fn acknowledge_outbox_event(
        &self,
        event_id: i64,
        publisher_id: &WorkerId,
        claim_token: u64,
    ) -> Result<(), StorageError> {
        let claim_token = sql_i64(claim_token)?;
        let client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let updated = client
            .execute(
                "UPDATE outbox_events \
             SET published_at = clock_timestamp(), claim_owner = NULL, claim_expires_at = NULL \
             WHERE event_id = $1 AND published_at IS NULL AND claim_owner = $2 \
               AND claim_token = $3 AND claim_expires_at > clock_timestamp()",
                &[&event_id, &publisher_id.as_str(), &claim_token],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        if updated != 1 {
            return Err(StorageError::ClaimLost(format!("outbox event {event_id}")));
        }
        Ok(())
    }

    /// Releases a failed publication attempt and schedules another delivery.
    ///
    /// A zero delay makes the event immediately eligible. The next claim receives
    /// a larger token, so a stale publisher cannot acknowledge the new attempt.
    pub async fn retry_outbox_event(
        &self,
        event_id: i64,
        publisher_id: &WorkerId,
        claim_token: u64,
        retry_delay: Duration,
        error_message: &str,
    ) -> Result<(), StorageError> {
        let claim_token = sql_i64(claim_token)?;
        let retry_delay_seconds = retry_delay.as_secs_f64();
        let client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let updated = client
            .execute(
                "UPDATE outbox_events \
             SET available_at = clock_timestamp() + make_interval(secs => $4), \
                 claim_owner = NULL, claim_expires_at = NULL, last_error = $5 \
             WHERE event_id = $1 AND published_at IS NULL AND claim_owner = $2 \
               AND claim_token = $3 AND claim_expires_at > clock_timestamp()",
                &[
                    &event_id,
                    &publisher_id.as_str(),
                    &claim_token,
                    &retry_delay_seconds,
                    &error_message,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        if updated != 1 {
            return Err(StorageError::ClaimLost(format!("outbox event {event_id}")));
        }
        Ok(())
    }

    /// Reconciles task outcomes into the durable workflow-run terminal state.
    ///
    /// The earliest persisted non-success terminal task outcome wins. A workflow
    /// succeeds only when every task run has succeeded. On failure, cancellation,
    /// or timeout, remaining non-terminal tasks and their active attempts are
    /// cancelled in the same transaction as the workflow terminal event.
    pub async fn aggregate_workflow_run(
        &self,
        workflow_run_id: &WorkflowRunId,
    ) -> Result<WorkflowStatus, StorageError> {
        self.aggregate_workflow_run_inner(workflow_run_id, None)
            .await
    }

    /// Aggregates workflow state after a registered worker transition.
    /// Draining workers may settle work that was already in flight.
    pub async fn aggregate_workflow_run_registered(
        &self,
        workflow_run_id: &WorkflowRunId,
        registration: &WorkerRegistration,
    ) -> Result<WorkflowStatus, StorageError> {
        self.aggregate_workflow_run_inner(workflow_run_id, Some(registration))
            .await
    }

    async fn aggregate_workflow_run_inner(
        &self,
        workflow_run_id: &WorkflowRunId,
        registration: Option<&WorkerRegistration>,
    ) -> Result<WorkflowStatus, StorageError> {
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        if let Some(registration) = registration {
            lock_worker_registration(&transaction, registration, false).await?;
        }

        let workflow_row = transaction
            .query_opt(
                "SELECT status FROM workflow_runs WHERE workflow_run_id = $1 FOR UPDATE",
                &[&workflow_run_id.as_str()],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?
            .ok_or_else(|| {
                StorageError::WorkflowRunNotFound(workflow_run_id.as_str().to_owned())
            })?;
        let current_status_text: String = workflow_row.get(0);
        let current_status = parse_workflow_status(&current_status_text)?;
        if workflow_status_is_terminal(current_status) {
            transaction
                .commit()
                .await
                .map_err(|error| StorageError::Database(error.to_string()))?;
            return Ok(current_status);
        }

        // Lock task rows in a stable order before deciding and applying the
        // aggregate result. A worker cannot commit against a row while it is
        // being reconciled here.
        let rows = transaction
            .query(
                "SELECT task_run_id, status, version, attempts_started, claimed_by, fencing_token, updated_at \
                 FROM task_runs WHERE workflow_run_id = $1 ORDER BY task_run_id FOR UPDATE",
                &[&workflow_run_id.as_str()],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        if rows.is_empty() {
            return Err(StorageError::InconsistentState(format!(
                "workflow run {} contains no task runs",
                workflow_run_id.as_str()
            )));
        }

        let mut task_runs = Vec::with_capacity(rows.len());
        for row in rows {
            let task_run_id: String = row.get(0);
            let status: String = row.get(1);
            let version: i64 = row.get(2);
            let attempts_started: i32 = row.get(3);
            let claimed_by: Option<String> = row.get(4);
            let fencing_token: i64 = row.get(5);
            let updated_at: SystemTime = row.get(6);
            if !matches!(
                status.as_str(),
                "pending"
                    | "ready"
                    | "running"
                    | "retry_waiting"
                    | "succeeded"
                    | "failed"
                    | "cancelled"
                    | "timed_out"
            ) {
                return Err(StorageError::InvalidStoredValue(format!(
                    "unknown task status while aggregating workflow: {status}"
                )));
            }
            task_runs.push((
                task_run_id,
                status,
                version,
                attempts_started,
                claimed_by,
                fencing_token,
                updated_at,
            ));
        }

        let first_terminal_failure = task_runs
            .iter()
            .filter(|(_, status, ..)| {
                matches!(status.as_str(), "failed" | "cancelled" | "timed_out")
            })
            .min_by(|left, right| left.6.cmp(&right.6).then_with(|| left.0.cmp(&right.0)));

        let next_status = if let Some((_, status, ..)) = first_terminal_failure {
            match status.as_str() {
                "failed" => WorkflowStatus::Failed,
                "cancelled" => WorkflowStatus::Cancelled,
                "timed_out" => WorkflowStatus::TimedOut,
                _ => unreachable!("filtered terminal failure status"),
            }
        } else if task_runs
            .iter()
            .all(|(_, status, ..)| status == "succeeded")
        {
            WorkflowStatus::Succeeded
        } else {
            transaction
                .commit()
                .await
                .map_err(|error| StorageError::Database(error.to_string()))?;
            return Ok(current_status);
        };

        let next_status_text = workflow_status_name(next_status);
        if next_status != WorkflowStatus::Succeeded {
            // Stop all remaining work after the first terminal non-success
            // result. Stale worker writes then fail their status/version guards.
            for (task_run_id, status, version, attempts_started, claimed_by, fencing_token, _) in
                &task_runs
            {
                if !matches!(
                    status.as_str(),
                    "pending" | "ready" | "running" | "retry_waiting"
                ) {
                    continue;
                }

                if status == "running" {
                    if *attempts_started <= 0 || *fencing_token <= 0 {
                        return Err(StorageError::InconsistentState(format!(
                            "running task {task_run_id} has invalid attempt or fencing metadata"
                        )));
                    }
                    let worker_id = claimed_by.as_deref().ok_or_else(|| {
                        StorageError::InconsistentState(format!(
                            "running task {task_run_id} has no lease owner"
                        ))
                    })?;
                    let updated_attempts = transaction
                        .execute(
                            "UPDATE task_attempts \
                             SET outcome = 'cancelled', finished_at = clock_timestamp(), \
                                 error_kind = 'workflow_terminal', error_message = $5 \
                             WHERE task_run_id = $1 AND attempt_number = $2 \
                               AND worker_id = $3 AND fencing_token = $4 AND outcome = 'running'",
                            &[
                                task_run_id,
                                attempts_started,
                                &worker_id,
                                fencing_token,
                                &format!("workflow ended as {next_status_text}"),
                            ],
                        )
                        .await
                        .map_err(|error| StorageError::Database(error.to_string()))?;
                    if updated_attempts != 1 {
                        return Err(StorageError::InconsistentState(format!(
                            "workflow termination did not match one active attempt for task {task_run_id}"
                        )));
                    }
                }

                let updated = transaction
                    .query_opt(
                        "UPDATE task_runs \
                         SET status = 'cancelled', claimed_by = NULL, lease_expires_at = NULL, \
                             version = version + 1, updated_at = clock_timestamp(), \
                             finished_at = clock_timestamp() \
                         WHERE task_run_id = $1 AND version = $2 AND status = $3 \
                         RETURNING version",
                        &[task_run_id, version, status],
                    )
                    .await
                    .map_err(|error| StorageError::Database(error.to_string()))?
                    .ok_or_else(|| {
                        StorageError::InconsistentState(format!(
                            "task {task_run_id} changed while its workflow was locked"
                        ))
                    })?;
                let task_version: i64 = updated.get(0);
                append_outbox(
                    &transaction,
                    "task_run",
                    task_run_id,
                    task_version,
                    "task.cancelled",
                    json!({
                        "task_run_id": task_run_id,
                        "status": "cancelled",
                        "reason": "workflow_terminal",
                        "workflow_status": next_status_text,
                        "version": task_version
                    }),
                )
                .await?;
            }
        }

        let updated_workflow = transaction
            .query_opt(
                "UPDATE workflow_runs \
                 SET status = $2, version = version + 1, updated_at = clock_timestamp(), \
                     finished_at = clock_timestamp() \
                 WHERE workflow_run_id = $1 AND status = $3 \
                 RETURNING version",
                &[
                    &workflow_run_id.as_str(),
                    &next_status_text,
                    &current_status_text,
                ],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?
            .ok_or_else(|| {
                StorageError::InconsistentState(format!(
                    "workflow run {} changed during aggregation",
                    workflow_run_id.as_str()
                ))
            })?;
        let workflow_version: i64 = updated_workflow.get(0);
        append_outbox(
            &transaction,
            "workflow_run",
            workflow_run_id.as_str(),
            workflow_version,
            &format!("workflow.{next_status_text}"),
            json!({
                "workflow_run_id": workflow_run_id.as_str(),
                "status": next_status_text,
                "version": workflow_version
            }),
        )
        .await?;

        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        Ok(next_status)
    }

    /// Loads a durable workflow-run summary and its task snapshots.
    ///
    /// The workflow row and task rows are read from the same PostgreSQL snapshot
    /// so the public API cannot combine unrelated workflow/task identifiers.
    pub async fn get_workflow_run(
        &self,
        workflow_run_id: &WorkflowRunId,
    ) -> Result<DurableWorkflowRun, StorageError> {
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        transaction
            .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let row = transaction
            .query_opt(
                "SELECT workflow_id, status, version, created_at, updated_at
                 FROM workflow_runs
                 WHERE workflow_run_id = $1",
                &[&workflow_run_id.as_str()],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?
            .ok_or_else(|| {
                StorageError::WorkflowRunNotFound(workflow_run_id.as_str().to_owned())
            })?;

        let workflow_id: String = row.get(0);
        let status: String = row.get(1);
        let version: i64 = row.get(2);
        let created_at: SystemTime = row.get(3);
        let updated_at: SystemTime = row.get(4);
        let task_rows = transaction
            .query(
                "SELECT task_run_id
                 FROM task_runs
                 WHERE workflow_run_id = $1
                 ORDER BY created_at, task_run_id",
                &[&workflow_run_id.as_str()],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let mut task_runs = Vec::with_capacity(task_rows.len());
        for task_row in task_rows {
            let task_run_id: String = task_row.get(0);
            task_runs.push(load_snapshot(&transaction, &task_run_id).await?);
        }

        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        Ok(DurableWorkflowRun {
            workflow_run_id: workflow_run_id.clone(),
            workflow_id: WorkflowId::parse(workflow_id)
                .map_err(|error| StorageError::InvalidStoredValue(error.to_string()))?,
            status: parse_workflow_status(&status)?,
            version: u64::try_from(version)
                .map_err(|_| StorageError::InvalidStoredValue("negative workflow version".to_owned()))?,
            created_at,
            updated_at,
            task_runs,
        })
    }

    /// Loads a durable task-run snapshot, including direct dependency references.
    pub async fn get_task_run(
        &self,
        task_run_id: &TaskRunId,
    ) -> Result<DurableTaskRun, StorageError> {
        let client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        load_snapshot_from_client(&client, task_run_id.as_str()).await
    }
}

fn parse_workflow_status(value: &str) -> Result<WorkflowStatus, StorageError> {
    match value {
        "pending" => Ok(WorkflowStatus::Pending),
        "running" => Ok(WorkflowStatus::Running),
        "succeeded" => Ok(WorkflowStatus::Succeeded),
        "failed" => Ok(WorkflowStatus::Failed),
        "cancelled" => Ok(WorkflowStatus::Cancelled),
        "timed_out" => Ok(WorkflowStatus::TimedOut),
        other => Err(StorageError::InvalidStoredValue(format!(
            "unknown workflow status: {other}"
        ))),
    }
}

fn workflow_status_name(status: WorkflowStatus) -> &'static str {
    match status {
        WorkflowStatus::Pending => "pending",
        WorkflowStatus::Running => "running",
        WorkflowStatus::Succeeded => "succeeded",
        WorkflowStatus::Failed => "failed",
        WorkflowStatus::Cancelled => "cancelled",
        WorkflowStatus::TimedOut => "timed_out",
    }
}

fn workflow_status_is_terminal(status: WorkflowStatus) -> bool {
    matches!(
        status,
        WorkflowStatus::Succeeded
            | WorkflowStatus::Failed
            | WorkflowStatus::Cancelled
            | WorkflowStatus::TimedOut
    )
}

fn validate_retry_policy(policy: RetryPolicy) -> Result<(), StorageError> {
    let attempts =
        i32::try_from(policy.max_attempts().get()).map_err(|_| StorageError::InvalidRetryPolicy)?;
    let initial =
        i64::try_from(policy.initial_backoff_ms()).map_err(|_| StorageError::InvalidRetryPolicy)?;
    let maximum =
        i64::try_from(policy.max_backoff_ms()).map_err(|_| StorageError::InvalidRetryPolicy)?;
    if attempts <= 0 || initial < 0 || maximum < initial {
        return Err(StorageError::InvalidRetryPolicy);
    }
    Ok(())
}

fn lease_seconds(duration: Duration) -> Result<f64, StorageError> {
    let millis = duration.as_millis();
    if millis == 0 || millis > MAX_LEASE_MILLIS {
        return Err(StorageError::InvalidLeaseDuration);
    }
    Ok(duration.as_secs_f64())
}

fn sql_i64(value: u64) -> Result<i64, StorageError> {
    i64::try_from(value).map_err(|_| {
        StorageError::InvalidStoredValue("integer exceeds PostgreSQL BIGINT".to_owned())
    })
}

async fn append_outbox(
    transaction: &Transaction<'_>,
    aggregate_type: &str,
    aggregate_id: &str,
    aggregate_version: i64,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<(), StorageError> {
    transaction
        .execute(
            "INSERT INTO outbox_events \
         (aggregate_type, aggregate_id, aggregate_version, event_type, payload) \
         VALUES ($1, $2, $3, $4, $5)",
            &[
                &aggregate_type,
                &aggregate_id,
                &aggregate_version,
                &event_type,
                &payload,
            ],
        )
        .await
        .map_err(|error| {
            StorageError::Database(format!("append outbox event {event_type}: {error:?}"))
        })?;
    Ok(())
}

async fn load_snapshot_from_client(
    client: &deadpool_postgres::Object,
    task_run_id: &str,
) -> Result<DurableTaskRun, StorageError> {
    let row = client
        .query_opt(
            "SELECT task_run_id, workflow_run_id, task_id, status, version, \
                attempts_started, available_at, input_payload, output_payload, \
                claimed_by, fencing_token, lease_expires_at, created_at, updated_at \
         FROM task_runs WHERE task_run_id = $1",
            &[&task_run_id],
        )
        .await
        .map_err(|error| StorageError::Database(error.to_string()))?
        .ok_or_else(|| StorageError::TaskRunNotFound(task_run_id.to_owned()))?;
    let dependencies = client
        .query(
            "SELECT parent_task_run_id FROM task_run_dependencies \
         WHERE child_task_run_id = $1 ORDER BY dependency_position",
            &[&task_run_id],
        )
        .await
        .map_err(|error| StorageError::Database(error.to_string()))?;
    build_snapshot(row, dependencies)
}

async fn load_snapshot(
    transaction: &Transaction<'_>,
    task_run_id: &str,
) -> Result<DurableTaskRun, StorageError> {
    let row = transaction
        .query_opt(
            "SELECT task_run_id, workflow_run_id, task_id, status, version, \
                attempts_started, available_at, input_payload, output_payload, \
                claimed_by, fencing_token, lease_expires_at, created_at, updated_at \
         FROM task_runs WHERE task_run_id = $1",
            &[&task_run_id],
        )
        .await
        .map_err(|error| StorageError::Database(error.to_string()))?
        .ok_or_else(|| StorageError::TaskRunNotFound(task_run_id.to_owned()))?;
    let dependencies = transaction
        .query(
            "SELECT parent_task_run_id FROM task_run_dependencies \
         WHERE child_task_run_id = $1 ORDER BY dependency_position",
            &[&task_run_id],
        )
        .await
        .map_err(|error| StorageError::Database(error.to_string()))?;
    build_snapshot(row, dependencies)
}

fn build_snapshot(row: Row, dependency_rows: Vec<Row>) -> Result<DurableTaskRun, StorageError> {
    let task_run_id: String = row.get(0);
    let workflow_run_id: String = row.get(1);
    let task_id: String = row.get(2);
    let status: String = row.get(3);
    let version: i64 = row.get(4);
    let attempts_started: i32 = row.get(5);
    let available_at: SystemTime = row.get(6);
    let input_bytes: Vec<u8> = row.get(7);
    let output_bytes: Option<Vec<u8>> = row.get(8);
    let claimed_by: Option<String> = row.get(9);
    let fencing_token: i64 = row.get(10);
    let lease_expires_at: Option<SystemTime> = row.get(11);
    let created_at: SystemTime = row.get(12);
    let updated_at: SystemTime = row.get(13);
    let dependencies = dependency_rows
        .into_iter()
        .map(|row| {
            let value: String = row.get(0);
            parse_task_run_id(value)
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(DurableTaskRun {
        task_run_id: parse_task_run_id(task_run_id)?,
        workflow_run_id: WorkflowRunId::parse(workflow_run_id)
            .map_err(|error| StorageError::InvalidStoredValue(error.to_string()))?,
        task_id: TaskId::parse(task_id)
            .map_err(|error| StorageError::InvalidStoredValue(error.to_string()))?,
        status: parse_status(&status)?,
        version: u64::try_from(version)
            .map_err(|_| StorageError::InvalidStoredValue("negative task version".to_owned()))?,
        attempts_started: u32::try_from(attempts_started)
            .map_err(|_| StorageError::InvalidStoredValue("invalid attempt count".to_owned()))?,
        available_at,
        input: TaskInput::new(input_bytes)
            .map_err(|error| StorageError::InvalidStoredValue(error.to_string()))?,
        output: output_bytes
            .map(TaskOutput::new)
            .transpose()
            .map_err(|error| StorageError::InvalidStoredValue(error.to_string()))?,
        dependencies,
        claimed_by: claimed_by
            .map(|value| {
                WorkerId::parse(value)
                    .map_err(|error| StorageError::InvalidStoredValue(error.to_string()))
            })
            .transpose()?,
        fencing_token: u64::try_from(fencing_token)
            .map_err(|_| StorageError::InvalidStoredValue("negative fencing token".to_owned()))?,
        lease_expires_at,
        created_at,
        updated_at,
    })
}

fn parse_status(value: &str) -> Result<TaskStatus, StorageError> {
    match value {
        "pending" => Ok(TaskStatus::Pending),
        "ready" => Ok(TaskStatus::Ready),
        "running" => Ok(TaskStatus::Running),
        "retry_waiting" => Ok(TaskStatus::RetryWaiting),
        "succeeded" => Ok(TaskStatus::Succeeded),
        "failed" => Ok(TaskStatus::Failed),
        "cancelled" => Ok(TaskStatus::Cancelled),
        "timed_out" => Ok(TaskStatus::TimedOut),
        other => Err(StorageError::InvalidStoredValue(format!(
            "unknown task status: {other}"
        ))),
    }
}

fn parse_task_run_id(value: String) -> Result<TaskRunId, StorageError> {
    TaskRunId::parse(value).map_err(|error| StorageError::InvalidStoredValue(error.to_string()))
}
