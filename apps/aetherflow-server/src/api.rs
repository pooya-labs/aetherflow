//! Versioned HTTP control plane for durable workflow execution.
//!
//! The v1 service is intentionally loopback-only until authentication and
//! tenant isolation are implemented. Payload bytes are accepted on submission
//! but are never included in workflow/task inspection responses.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::SystemTime;

use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, SecondsFormat, Utc};
use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};

use aetherflow_core::domain::{
    RetryPolicy, TaskId, TaskStatus, WorkflowId, WorkflowRunId, WorkflowStatus,
};
use aetherflow_runtime::repository::{MAX_TASK_PAYLOAD_BYTES, TaskInput};
use aetherflow_runtime::workflow::{WorkflowDefinition, WorkflowTask};
use aetherflow_storage_postgres::{
    DurableTaskRun, DurableWorkflowRun, PgMembershipRepository, PgTaskRepository,
    PgWorkflowCoordinator, StorageError,
};

/// Cloneable HTTP application state. The health endpoint remains available
/// when PostgreSQL is not configured; all durable routes fail closed.
#[derive(Clone, Default)]
pub struct AppState {
    database: Option<Arc<AppDatabase>>,
}

struct AppDatabase {
    coordinator: PgWorkflowCoordinator,
    tasks: Arc<PgTaskRepository>,
    membership: PgMembershipRepository,
}

impl AppState {
    /// Creates application state without a database connection.
    #[must_use]
    pub fn without_database() -> Self {
        Self::default()
    }

    /// Creates the durable repositories from a caller-configured connection pool.
    #[must_use]
    pub fn from_pool(pool: Pool) -> Self {
        let tasks = Arc::new(PgTaskRepository::from_pool(pool.clone()));
        let membership = PgMembershipRepository::from_pool(pool);
        let coordinator = PgWorkflowCoordinator::new(Arc::clone(&tasks));
        Self {
            database: Some(Arc::new(AppDatabase {
                coordinator,
                tasks,
                membership,
            })),
        }
    }

    fn database(&self) -> Result<&AppDatabase, ApiError> {
        self.database.as_deref().ok_or_else(|| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "database_unavailable",
                "PostgreSQL is not configured for this process",
            )
        })
    }
}

/// Builds the v1 router and retains the bootstrap health resource.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/workflow-runs", post(submit_workflow))
        .route(
            "/api/v1/workflow-runs/{workflow_run_id}",
            get(get_workflow_run),
        )
        .route("/api/v1/task-runs/{task_run_id}", get(get_task_run))
        .route(
            "/api/v1/task-runs/{task_run_id}/cancel",
            post(cancel_task_run),
        )
        .route("/api/v1/workers", get(list_active_workers))
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .with_state(state)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_owned(),
    })
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
        }
    }

    fn invalid_identifier() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_identifier",
            "an identifier does not satisfy the AetherFlow identifier contract",
        )
    }

    fn invalid_workflow() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_workflow",
            "the workflow definition or retry policy is invalid",
        )
    }

    fn invalid_request(status: StatusCode) -> Self {
        if status == StatusCode::PAYLOAD_TOO_LARGE {
            Self::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                "the request body exceeds the configured size limit",
            )
        } else {
            Self::new(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "the request body is malformed or does not match the API contract",
            )
        }
    }

    fn payload_too_large() -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "decoded task input exceeds the 1 MiB payload limit",
        )
    }

    fn from_storage(error: StorageError) -> Self {
        match error {
            StorageError::WorkflowRunConflict(_) => Self::new(
                StatusCode::CONFLICT,
                "conflict",
                "the workflow-run identifier has already been used",
            ),
            StorageError::TaskRunNotFound(_) | StorageError::WorkflowRunNotFound(_) => {
                Self::new(
                    StatusCode::NOT_FOUND,
                    "not_found",
                    "the requested workflow run or task run does not exist",
                )
            }
            StorageError::ClaimLost(_) => Self::new(
                StatusCode::CONFLICT,
                "stale_version",
                "the task run changed or became terminal before the requested transition",
            ),
            StorageError::Pool(_) | StorageError::Database(_) => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "database_unavailable",
                "the durable storage operation could not be completed",
            ),
            StorageError::InvalidWorkflow(_) | StorageError::InvalidRetryPolicy => {
                Self::invalid_workflow()
            }
            StorageError::NodeLeaseBusy(_)
            | StorageError::NodeMembershipLost(_)
            | StorageError::WorkerRegistrationBusy(_)
            | StorageError::WorkerRegistrationLost(_) => Self::new(
                StatusCode::CONFLICT,
                "conflict",
                "the requested operation conflicts with current worker membership",
            ),
            _ => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
                "the server could not complete the request",
            ),
        }
    }
}

#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorDetails,
}

#[derive(Serialize)]
struct ErrorDetails {
    code: &'static str,
    message: &'static str,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorResponse {
                error: ErrorDetails {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitWorkflowRequest {
    workflow_id: String,
    workflow_run_id: String,
    tasks: Vec<WorkflowTaskRequest>,
    retry_policy: RetryPolicyRequest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowTaskRequest {
    task_id: String,
    #[serde(default)]
    depends_on: Vec<String>,
    input_base64: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetryPolicyRequest {
    max_attempts: u32,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
}

#[derive(Serialize)]
struct WorkflowRunReceipt {
    workflow_id: String,
    workflow_run_id: String,
    status: &'static str,
    version: u64,
    task_runs: Vec<TaskRunReceipt>,
}

#[derive(Serialize)]
struct TaskRunReceipt {
    task_id: String,
    task_run_id: String,
    status: &'static str,
}

async fn submit_workflow(
    State(state): State<AppState>,
    payload: Result<Json<SubmitWorkflowRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<WorkflowRunReceipt>), ApiError> {
    let Json(request) = payload.map_err(|rejection| ApiError::invalid_request(rejection.status()))?;

    let workflow_id =
        WorkflowId::parse(request.workflow_id).map_err(|_| ApiError::invalid_identifier())?;
    let workflow_run_id = WorkflowRunId::parse(request.workflow_run_id)
        .map_err(|_| ApiError::invalid_identifier())?;

    if request.tasks.is_empty() || request.tasks.len() > 1000 {
        return Err(ApiError::invalid_workflow());
    }
    if request.retry_policy.max_attempts == 0
        || request.retry_policy.max_attempts > i32::MAX as u32
        || request.retry_policy.initial_backoff_ms > request.retry_policy.max_backoff_ms
        || request.retry_policy.max_backoff_ms > i64::MAX as u64
    {
        return Err(ApiError::invalid_workflow());
    }

    let mut workflow_tasks = Vec::with_capacity(request.tasks.len());
    for task in request.tasks {
        let task_id = TaskId::parse(task.task_id).map_err(|_| ApiError::invalid_identifier())?;
        let dependencies = task
            .depends_on
            .into_iter()
            .map(|value| TaskId::parse(value).map_err(|_| ApiError::invalid_identifier()))
            .collect::<Result<Vec<_>, _>>()?;

        let decoded_input = match task.input_base64 {
            Some(encoded) => BASE64.decode(encoded).map_err(|_| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_request",
                    "input_base64 must contain valid standard base64",
                )
            })?,
            None => Vec::new(),
        };
        if decoded_input.len() > MAX_TASK_PAYLOAD_BYTES {
            return Err(ApiError::payload_too_large());
        }
        let input = TaskInput::new(decoded_input).map_err(|_| ApiError::payload_too_large())?;
        workflow_tasks.push(WorkflowTask::new(task_id, dependencies).with_input(input));
    }

    let definition = WorkflowDefinition::new(workflow_id.clone(), workflow_tasks)
        .map_err(|_| ApiError::invalid_workflow())?;
    let retry_policy = RetryPolicy::new(
        NonZeroU32::new(request.retry_policy.max_attempts)
            .ok_or_else(ApiError::invalid_workflow)?,
        request.retry_policy.initial_backoff_ms,
        request.retry_policy.max_backoff_ms,
    )
    .map_err(|_| ApiError::invalid_workflow())?;

    let database = state.database()?;
    let submission = database
        .coordinator
        .submit_workflow(&workflow_run_id, &definition, retry_policy, None)
        .await
        .map_err(ApiError::from_storage)?;

    let task_runs = submission
        .task_runs()
        .map(|(task_id, task_run_id)| TaskRunReceipt {
            task_id: task_id.to_owned(),
            task_run_id: task_run_id.as_str().to_owned(),
            status: "pending",
        })
        .collect();

    Ok((
        StatusCode::CREATED,
        Json(WorkflowRunReceipt {
            workflow_id: workflow_id.as_str().to_owned(),
            workflow_run_id: workflow_run_id.as_str().to_owned(),
            status: "running",
            version: 1,
            task_runs,
        }),
    ))
}

async fn get_workflow_run(
    State(state): State<AppState>,
    Path(raw_workflow_run_id): Path<String>,
) -> Result<Json<WorkflowRunSnapshot>, ApiError> {
    let workflow_run_id = WorkflowRunId::parse(raw_workflow_run_id)
        .map_err(|_| ApiError::invalid_identifier())?;
    let database = state.database()?;
    let snapshot = database
        .tasks
        .get_workflow_run(&workflow_run_id)
        .await
        .map_err(ApiError::from_storage)?;
    Ok(Json(WorkflowRunSnapshot::from(snapshot)))
}

#[derive(Serialize)]
struct WorkflowRunSnapshot {
    workflow_id: String,
    workflow_run_id: String,
    status: &'static str,
    version: u64,
    task_runs: Vec<TaskRunReceipt>,
    created_at: String,
    updated_at: String,
}

impl From<DurableWorkflowRun> for WorkflowRunSnapshot {
    fn from(value: DurableWorkflowRun) -> Self {
        Self {
            workflow_id: value.workflow_id().as_str().to_owned(),
            workflow_run_id: value.workflow_run_id().as_str().to_owned(),
            status: workflow_status_name(value.status()),
            version: value.version(),
            task_runs: value
                .task_runs()
                .iter()
                .map(|task| TaskRunReceipt {
                    task_id: task.task_id().as_str().to_owned(),
                    task_run_id: task.task_run_id().as_str().to_owned(),
                    status: task_status_name(task.status()),
                })
                .collect(),
            created_at: format_timestamp(value.created_at()),
            updated_at: format_timestamp(value.updated_at()),
        }
    }
}

async fn get_task_run(
    State(state): State<AppState>,
    Path(raw_task_run_id): Path<String>,
) -> Result<Json<TaskRunSnapshot>, ApiError> {
    let task_run_id =
        aetherflow_core::domain::TaskRunId::parse(raw_task_run_id)
            .map_err(|_| ApiError::invalid_identifier())?;
    let database = state.database()?;
    let snapshot = database
        .tasks
        .get_task_run(&task_run_id)
        .await
        .map_err(ApiError::from_storage)?;
    Ok(Json(TaskRunSnapshot::from(&snapshot)))
}

#[derive(Serialize)]
struct TaskRunSnapshot {
    task_run_id: String,
    workflow_run_id: String,
    task_id: String,
    status: &'static str,
    version: u64,
    attempts_started: u32,
    available_at: String,
    input_size_bytes: usize,
    output_size_bytes: Option<usize>,
    output_present: bool,
    dependency_task_run_ids: Vec<String>,
    claimed_by: Option<String>,
    lease_expires_at: Option<String>,
    created_at: String,
    updated_at: String,
}

impl From<&DurableTaskRun> for TaskRunSnapshot {
    fn from(value: &DurableTaskRun) -> Self {
        Self {
            task_run_id: value.task_run_id().as_str().to_owned(),
            workflow_run_id: value.workflow_run_id().as_str().to_owned(),
            task_id: value.task_id().as_str().to_owned(),
            status: task_status_name(value.status()),
            version: value.version(),
            attempts_started: value.attempts_started(),
            available_at: format_timestamp(value.available_at()),
            input_size_bytes: value.input().len(),
            output_size_bytes: value.output().map(|output| output.len()),
            output_present: value.output().is_some(),
            dependency_task_run_ids: value
                .dependencies()
                .iter()
                .map(|dependency| dependency.as_str().to_owned())
                .collect(),
            claimed_by: value.claimed_by().map(|worker| worker.as_str().to_owned()),
            lease_expires_at: value.lease_expires_at().map(format_timestamp),
            created_at: format_timestamp(value.created_at()),
            updated_at: format_timestamp(value.updated_at()),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelTaskRunRequest {
    expected_version: u64,
}

#[derive(Serialize)]
struct CancelTaskRunResponse {
    task_run: TaskRunSnapshot,
    workflow_status: &'static str,
}

async fn cancel_task_run(
    State(state): State<AppState>,
    Path(raw_task_run_id): Path<String>,
    payload: Result<Json<CancelTaskRunRequest>, JsonRejection>,
) -> Result<Json<CancelTaskRunResponse>, ApiError> {
    let Json(request) = payload.map_err(|rejection| ApiError::invalid_request(rejection.status()))?;
    let task_run_id =
        aetherflow_core::domain::TaskRunId::parse(raw_task_run_id)
            .map_err(|_| ApiError::invalid_identifier())?;
    let database = state.database()?;
    let cancelled = database
        .tasks
        .cancel_task_run(&task_run_id, request.expected_version)
        .await
        .map_err(ApiError::from_storage)?;
    let workflow_status = database
        .tasks
        .aggregate_workflow_run(cancelled.workflow_run_id())
        .await
        .map_err(ApiError::from_storage)?;

    Ok(Json(CancelTaskRunResponse {
        task_run: TaskRunSnapshot::from(&cancelled),
        workflow_status: workflow_status_name(workflow_status),
    }))
}

#[derive(Serialize)]
struct WorkerListResponse {
    workers: Vec<WorkerSnapshot>,
}

#[derive(Serialize)]
struct WorkerSnapshot {
    worker_id: String,
    node_id: String,
    node_incarnation_id: String,
    membership_epoch: u64,
    status: &'static str,
    concurrency_limit: u32,
    lease_expires_at: String,
}

async fn list_active_workers(
    State(state): State<AppState>,
) -> Result<Json<WorkerListResponse>, ApiError> {
    let database = state.database()?;
    let workers = database
        .membership
        .active_workers()
        .await
        .map_err(ApiError::from_storage)?
        .into_iter()
        .map(|worker| WorkerSnapshot {
            worker_id: worker.worker_id().as_str().to_owned(),
            node_id: worker.node().node_id().as_str().to_owned(),
            node_incarnation_id: worker.node().incarnation_id().as_str().to_owned(),
            membership_epoch: worker.node().epoch(),
            status: "active",
            concurrency_limit: worker.concurrency_limit().get(),
            lease_expires_at: format_timestamp(worker.lease_expires_at()),
        })
        .collect();

    Ok(Json(WorkerListResponse { workers }))
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

fn task_status_name(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "pending",
        TaskStatus::Ready => "ready",
        TaskStatus::Running => "running",
        TaskStatus::RetryWaiting => "retry_waiting",
        TaskStatus::Succeeded => "succeeded",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
        TaskStatus::TimedOut => "timed_out",
    }
}

fn format_timestamp(value: SystemTime) -> String {
    DateTime::<Utc>::from(value).to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::{AppState, router};
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_is_available_without_database() {
        let response = router(AppState::without_database())
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .expect("valid health request"),
            )
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("read response");
        assert_eq!(&body[..], br#"{"status":"ok"}"#);
    }

    #[tokio::test]
    async fn durable_routes_fail_closed_without_database() {
        let response = router(AppState::without_database())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/workers")
                    .body(Body::empty())
                    .expect("valid workers request"),
            )
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn malformed_workflow_identifiers_are_rejected_before_storage_access() {
        let body = r#"{
            "workflow_id": "../bad",
            "workflow_run_id": "run-01",
            "tasks": [{"task_id": "task-01"}],
            "retry_policy": {
                "max_attempts": 1,
                "initial_backoff_ms": 0,
                "max_backoff_ms": 0
            }
        }"#;
        let response = router(AppState::without_database())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/workflow-runs")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("valid submit request"),
            )
            .await
            .expect("router response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("read response");
        let response: serde_json::Value =
            serde_json::from_slice(&body).expect("stable error JSON");
        assert_eq!(
            response["error"]["code"],
            serde_json::Value::String("invalid_identifier".to_owned())
        );
    }
}
