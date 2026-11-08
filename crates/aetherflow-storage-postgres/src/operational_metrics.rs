//! Tenant-scoped snapshots of durable workflow and queue state.
//!
//! Every query runs in one read-only PostgreSQL transaction with the
//! authenticated tenant context applied transaction-locally. The repository
//! deliberately returns aggregates only; it never exposes resource identifiers
//! or raw event/task payloads.

use deadpool_postgres::Pool;

use crate::{StorageError, TenantContext};

/// Aggregate workflow and delivery state visible to one authenticated tenant.
#[derive(Clone, Debug, PartialEq)]
pub struct TenantOperationalMetrics {
    /// Current workflow-run counts grouped by the database's fixed status set.
    pub workflow_runs_by_status: Vec<(String, i64)>,
    /// Age of the oldest pending or running workflow, in seconds.
    pub oldest_active_workflow_age_seconds: f64,
    /// Terminal workflow outcomes recorded in the rolling 30-minute window.
    pub workflow_completions_last_30m_by_status: Vec<(String, i64)>,
    /// Successful workflow executions with usable start/finish timestamps in the rolling window.
    pub workflow_success_completion_samples_last_30m: i64,
    /// Median successful workflow execution duration from started_at to finished_at.
    pub workflow_success_completion_p50_seconds_last_30m: Option<f64>,
    /// 95th percentile successful workflow execution duration from started_at to finished_at.
    pub workflow_success_completion_p95_seconds_last_30m: Option<f64>,
    /// 99th percentile successful workflow execution duration from started_at to finished_at.
    pub workflow_success_completion_p99_seconds_last_30m: Option<f64>,
    /// Number of unpublished outbox events.
    pub outbox_pending_events: i64,
    /// Number of unpublished outbox events currently eligible for a claim.
    pub outbox_ready_events: i64,
    /// Time since the oldest currently claimable outbox event became available.
    pub outbox_oldest_ready_lag_seconds: f64,
    /// Number of unpublished outbox claims whose lease has expired.
    pub outbox_expired_claims: i64,
    /// Number of inbox events waiting to be processed.
    pub inbox_pending_events: i64,
    /// Number of inbox events currently being processed.
    pub inbox_processing_events: i64,
    /// Age of the oldest unprocessed inbox event, in seconds.
    pub inbox_oldest_unprocessed_age_seconds: f64,
    /// Number of inbox processing leases that have expired.
    pub inbox_expired_processing_leases: i64,
    /// Number of running task leases that have expired.
    pub task_expired_running_leases: i64,
}

/// Read-only aggregate queries for the active tenant in a validated context.
#[derive(Clone)]
pub struct PgTenantOperationalMetricsRepository {
    pool: Pool,
}

impl PgTenantOperationalMetricsRepository {
    /// Creates the repository from the application's configured PostgreSQL pool.
    #[must_use]
    pub fn from_pool(pool: Pool) -> Self {
        Self { pool }
    }

    /// Returns one consistent tenant-scoped operational snapshot.
    ///
    /// The transaction is explicitly read-only. The tenant context is installed
    /// using transaction-local PostgreSQL settings so it cannot leak through
    /// the pooled connection after commit or rollback.
    pub async fn snapshot(
        &self,
        context: &TenantContext,
    ) -> Result<TenantOperationalMetrics, StorageError> {
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
        context.apply_to_transaction(&transaction).await?;

        let tenant_id = context.tenant_id().as_str();

        let workflow_rows = transaction
            .query(
                "SELECT status, count(*)::BIGINT
                 FROM workflow_runs
                 WHERE tenant_id = $1
                 GROUP BY status",
                &[&tenant_id],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let workflow_runs_by_status = workflow_rows
            .iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
            .collect();

        let oldest_active_workflow_age_seconds: f64 = transaction
            .query_one(
                "SELECT COALESCE(
                    EXTRACT(EPOCH FROM (clock_timestamp() - min(created_at)))::DOUBLE PRECISION,
                    0.0
                 )
                 FROM workflow_runs
                 WHERE tenant_id = $1
                   AND status IN ('pending', 'running')",
                &[&tenant_id],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?
            .get(0);

        let workflow_completion_rows = transaction
            .query(
                "SELECT status, count(*)::BIGINT
                 FROM workflow_runs
                 WHERE tenant_id = $1
                   AND finished_at >= clock_timestamp() - INTERVAL '30 minutes'
                   AND status IN ('succeeded', 'failed', 'cancelled', 'timed_out')
                 GROUP BY status",
                &[&tenant_id],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let workflow_completions_last_30m_by_status = workflow_completion_rows
            .iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
            .collect();

        let workflow_success_completion_row = transaction
            .query_one(
                "SELECT
                    count(*)::BIGINT,
                    percentile_cont(0.50) WITHIN GROUP (
                        ORDER BY EXTRACT(EPOCH FROM (finished_at - started_at))::DOUBLE PRECISION
                    )::DOUBLE PRECISION,
                    percentile_cont(0.95) WITHIN GROUP (
                        ORDER BY EXTRACT(EPOCH FROM (finished_at - started_at))::DOUBLE PRECISION
                    )::DOUBLE PRECISION,
                    percentile_cont(0.99) WITHIN GROUP (
                        ORDER BY EXTRACT(EPOCH FROM (finished_at - started_at))::DOUBLE PRECISION
                    )::DOUBLE PRECISION
                 FROM workflow_runs
                 WHERE tenant_id = $1
                   AND status = 'succeeded'
                   AND started_at IS NOT NULL
                   AND finished_at IS NOT NULL
                   AND finished_at >= started_at
                   AND finished_at >= clock_timestamp() - INTERVAL '30 minutes'",
                &[&tenant_id],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let workflow_success_completion_samples_last_30m = workflow_success_completion_row.get(0);
        let workflow_success_completion_p50_seconds_last_30m: Option<f64> =
            workflow_success_completion_row.get(1);
        let workflow_success_completion_p95_seconds_last_30m: Option<f64> =
            workflow_success_completion_row.get(2);
        let workflow_success_completion_p99_seconds_last_30m: Option<f64> =
            workflow_success_completion_row.get(3);

        let outbox_row = transaction
            .query_one(
                "SELECT
                    count(*)::BIGINT,
                    count(*) FILTER (
                        WHERE available_at <= clock_timestamp()
                          AND (
                            claim_owner IS NULL
                            OR claim_expires_at <= clock_timestamp()
                          )
                    )::BIGINT,
                    COALESCE(
                        EXTRACT(EPOCH FROM (
                            clock_timestamp() -
                            min(available_at) FILTER (
                                WHERE available_at <= clock_timestamp()
                                  AND (
                                    claim_owner IS NULL
                                    OR claim_expires_at <= clock_timestamp()
                                  )
                            )
                        ))::DOUBLE PRECISION,
                        0.0
                    )
                 FROM outbox_events
                 WHERE tenant_id = $1
                   AND published_at IS NULL",
                &[&tenant_id],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let outbox_pending_events = outbox_row.get(0);
        let outbox_ready_events = outbox_row.get(1);
        let outbox_oldest_ready_lag_seconds = outbox_row.get(2);

        let outbox_expired_claims: i64 = transaction
            .query_one(
                "SELECT count(*)::BIGINT
                 FROM outbox_events
                 WHERE tenant_id = $1
                   AND published_at IS NULL
                   AND claim_owner IS NOT NULL
                   AND claim_expires_at <= clock_timestamp()",
                &[&tenant_id],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?
            .get(0);

        let inbox_row = transaction
            .query_one(
                "SELECT
                    count(*) FILTER (WHERE status = 'pending')::BIGINT,
                    count(*) FILTER (WHERE status = 'processing')::BIGINT,
                    COALESCE(
                        EXTRACT(EPOCH FROM (
                            clock_timestamp() - min(received_at)
                        ))::DOUBLE PRECISION,
                        0.0
                    )
                 FROM consumer_inbox
                 WHERE tenant_id = $1
                   AND status IN ('pending', 'processing')",
                &[&tenant_id],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;
        let inbox_pending_events = inbox_row.get(0);
        let inbox_processing_events = inbox_row.get(1);
        let inbox_oldest_unprocessed_age_seconds = inbox_row.get(2);

        let inbox_expired_processing_leases: i64 = transaction
            .query_one(
                "SELECT count(*)::BIGINT
                 FROM consumer_inbox
                 WHERE tenant_id = $1
                   AND status = 'processing'
                   AND lease_expires_at <= clock_timestamp()",
                &[&tenant_id],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?
            .get(0);

        let task_expired_running_leases: i64 = transaction
            .query_one(
                "SELECT count(*)::BIGINT
                 FROM task_runs
                 WHERE tenant_id = $1
                   AND status = 'running'
                   AND lease_expires_at <= clock_timestamp()",
                &[&tenant_id],
            )
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?
            .get(0);

        transaction
            .commit()
            .await
            .map_err(|error| StorageError::Database(error.to_string()))?;

        Ok(TenantOperationalMetrics {
            workflow_runs_by_status,
            oldest_active_workflow_age_seconds,
            workflow_completions_last_30m_by_status,
            workflow_success_completion_samples_last_30m,
            workflow_success_completion_p50_seconds_last_30m,
            workflow_success_completion_p95_seconds_last_30m,
            workflow_success_completion_p99_seconds_last_30m,
            outbox_pending_events,
            outbox_ready_events,
            outbox_oldest_ready_lag_seconds,
            outbox_expired_claims,
            inbox_pending_events,
            inbox_processing_events,
            inbox_oldest_unprocessed_age_seconds,
            inbox_expired_processing_leases,
            task_expired_running_leases,
        })
    }
}
