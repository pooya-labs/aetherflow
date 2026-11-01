//! Transactional PostgreSQL task repository operations.
//!
//! PostgreSQL owns durable deadlines and claim ownership. Each state transition
//! and its outbox row are committed in one short transaction.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};

use aetherflow_core::domain::{
    AttemptId, IdempotencyKey, RetryPolicy, TaskId, TaskRunId, TaskStatus, WorkerId, WorkflowId,
    WorkflowRunId,
};
use aetherflow_runtime::repository::{TaskInput, TaskOutput};
use deadpool_postgres::Pool;
use serde_json::json;
use tokio_postgres::{Row, Transaction};

use crate::StorageError;

const MAX_LEASE_MILLIS: u128 = i64::MAX as u128;

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
            .map_err(|error| StorageError::Database(error.to_string()))?;

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
                .map_err(|error| StorageError::Database(format!("insert task-run row: {error:?}")))?;
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
                    .map_err(|error| StorageError::Database(error.to_string()))?;
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
    pub async fn claim_next(
        &self,
        worker_id: &WorkerId,
        attempt_id: &AttemptId,
        lease_duration: Duration,
    ) -> Result<Option<PgTaskClaim>, StorageError> {
        let lease_millis = lease_millis(lease_duration)?;
        let mut client = self
            .pool
            .get()
            .await
            .map_err(|error| StorageError::Pool(error.to_string()))?;
        let transaction = client
            .transaction()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let candidate = transaction
            .query_opt(
                "SELECT task.task_run_id \
             FROM task_runs AS task \
             WHERE task.status = 'ready' \
               AND task.available_at <= clock_timestamp() \
               AND task.attempts_started < task.max_attempts \
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
                &[],
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
                 lease_expires_at = clock_timestamp() + ($3 * INTERVAL '1 millisecond'), \
                 started_at = COALESCE(started_at, clock_timestamp()), updated_at = clock_timestamp() \
             WHERE task_run_id = $1 AND status = 'ready' \
             RETURNING attempts_started, fencing_token, version",
            &[&task_run_id, &worker_id.as_str(), &lease_millis],
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
    pub async fn heartbeat(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        fencing_token: u64,
        lease_duration: Duration,
    ) -> Result<SystemTime, StorageError> {
        let lease_millis = lease_millis(lease_duration)?;
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
        let row = transaction
            .query_opt(
                "UPDATE task_runs \
             SET lease_expires_at = clock_timestamp() + ($5 * INTERVAL '1 millisecond') \
             WHERE task_run_id = $1 AND status = 'running' AND claimed_by = $2 \
               AND version = $3 AND fencing_token = $4 \
               AND lease_expires_at > clock_timestamp() \
             RETURNING attempts_started, lease_expires_at",
                &[
                    &task_run_id.as_str(),
                    &worker_id.as_str(),
                    &expected_version,
                    &fencing_token,
                    &lease_millis,
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
    pub async fn complete(
        &self,
        task_run_id: &TaskRunId,
        worker_id: &WorkerId,
        expected_version: u64,
        fencing_token: u64,
        output: TaskOutput,
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

    /// Recovers expired leases in bounded batches. A later claim gets a larger fencing token.
    pub async fn recover_expired_leases(
        &self,
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
                i64::try_from(policy.backoff_ms(retry_number))
                    .map_err(|_| StorageError::InvalidRetryPolicy)?
            } else {
                0
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
                         THEN clock_timestamp() + ($3 * INTERVAL '1 millisecond') \
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

fn lease_millis(duration: Duration) -> Result<i64, StorageError> {
    let millis = duration.as_millis();
    if millis == 0 || millis > MAX_LEASE_MILLIS {
        return Err(StorageError::InvalidLeaseDuration);
    }
    i64::try_from(millis).map_err(|_| StorageError::InvalidLeaseDuration)
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
    let payload = payload.to_string();
    transaction
        .execute(
            "INSERT INTO outbox_events \
         (aggregate_type, aggregate_id, aggregate_version, event_type, payload) \
         VALUES ($1, $2, $3, $4, $5::jsonb)",
            &[
                &aggregate_type,
                &aggregate_id,
                &aggregate_version,
                &event_type,
                &payload,
            ],
        )
        .await
        .map_err(|error| StorageError::Database(error.to_string()))?;
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
