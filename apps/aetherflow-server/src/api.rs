//! Versioned HTTP control plane for durable workflow execution.
//!
//! The v1 service separates operator and tenant-authenticated control-plane
//! routes, with tenant identity enforced by storage authorization and RLS.
//! Payload bytes are accepted on submission but never included in inspection.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Extension, Path, Query, Request, State};
use axum::http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD as BASE64_URL};
use chrono::{DateTime, SecondsFormat, Utc};
use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use aetherflow_core::domain::{
    RetryPolicy, TaskId, TaskStatus, WorkflowId, WorkflowRunId, WorkflowStatus,
};
use aetherflow_runtime::repository::{MAX_TASK_PAYLOAD_BYTES, TaskInput};
use aetherflow_runtime::workflow::{WorkflowDefinition, WorkflowTask};
use aetherflow_storage_postgres::{
    DurableTaskRun, DurableWorkflowRun, PgMembershipRepository, PgTaskRepository,
    PgTenantCredentialRepository, PgWorkflowCoordinator, StorageError, TenantContext,
    TenantPermission, WorkflowEventSummary, WorkflowRunSummary,
};

/// Cloneable HTTP application state. The health endpoint remains available
/// when PostgreSQL is not configured; all durable routes fail closed.
#[derive(Clone, Default)]
pub struct AppState {
    database: Option<Arc<AppDatabase>>,
    api_token_hash: Option<[u8; 32]>,
}

struct AppDatabase {
    coordinator: PgWorkflowCoordinator,
    tasks: Arc<PgTaskRepository>,
    membership: PgMembershipRepository,
    tenant_credentials: PgTenantCredentialRepository,
}

impl AppState {
    /// Creates application state without a database connection.
    #[must_use]
    pub fn without_database() -> Self {
        Self::default()
    }

    /// Creates the durable repositories and stores only a digest of the operator token.
    pub fn from_pool(pool: Pool, api_token: &str) -> Result<Self, std::io::Error> {
        let api_token_hash = hash_api_token(api_token)?;
        let tasks = Arc::new(PgTaskRepository::from_pool(pool.clone()));
        let tenant_credentials = PgTenantCredentialRepository::from_pool(pool.clone());
        let membership = PgMembershipRepository::from_pool(pool.clone());
        let coordinator = PgWorkflowCoordinator::new(Arc::clone(&tasks));
        Ok(Self {
            database: Some(Arc::new(AppDatabase {
                coordinator,
                tasks,
                membership,
                tenant_credentials,
            })),
            api_token_hash: Some(api_token_hash),
        })
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

/// Builds the v1 router. Health is public; durable control-plane routes require Bearer auth.
pub fn router(state: AppState) -> Router {
    let operator_control_plane = Router::new()
        .route(
            "/api/v1/workflow-runs",
            get(list_workflow_runs).post(submit_workflow),
        )
        .route(
            "/api/v1/workflow-runs/{workflow_run_id}/events",
            get(list_workflow_events),
        )
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
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer_token,
        ));

    // Tenant API keys are deliberately separate from the platform operator
    // credential. No tenant ID is accepted from request data or headers.
    let tenant_control_plane = Router::new()
        .route(
            "/api/v1/tenant/workflow-runs",
            get(list_tenant_workflow_runs).post(submit_tenant_workflow),
        )
        .route(
            "/api/v1/tenant/workflow-runs/{workflow_run_id}/events",
            get(list_tenant_workflow_events),
        )
        .route(
            "/api/v1/tenant/workflow-runs/{workflow_run_id}",
            get(get_tenant_workflow_run),
        )
        .route(
            "/api/v1/tenant/task-runs/{task_run_id}",
            get(get_tenant_task_run),
        )
        .route(
            "/api/v1/tenant/task-runs/{task_run_id}/cancel",
            post(cancel_tenant_task_run),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_tenant_bearer,
        ));

    Router::new()
        .route("/health", get(health))
        .merge(operator_control_plane)
        .merge(tenant_control_plane)
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .with_state(state)
}

fn hash_api_token(api_token: &str) -> Result<[u8; 32], std::io::Error> {
    let valid_alphabet = api_token
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
    if !(32..=256).contains(&api_token.len()) || !valid_alphabet {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "AETHERFLOW_API_TOKEN must be 32-256 ASCII letters, digits, '-' or '_' characters",
        ));
    }

    Ok(Sha256::digest(api_token.as_bytes()).into())
}

fn authorization_matches(expected: &[u8; 32], headers: &HeaderMap) -> bool {
    let Some(value) = headers
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
    else {
        return false;
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return false;
    };
    if token.is_empty() {
        return false;
    }

    let supplied: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    bool::from(expected.as_slice().ct_eq(supplied.as_slice()))
}

async fn require_bearer_token(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    if state.database.is_none() {
        return next.run(request).await;
    }

    let Some(expected) = state.api_token_hash.as_ref() else {
        return ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth_not_configured",
            "the control-plane operator credential is not configured",
        )
        .into_response();
    };

    if !authorization_matches(expected, request.headers()) {
        let mut response = ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "a valid Bearer token is required for this resource",
        )
        .into_response();
        response
            .headers_mut()
            .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        return response;
    }

    next.run(request).await
}

async fn require_tenant_bearer(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let token = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty());
    let Some(token) = token else {
        return unauthorized_bearer_response();
    };

    let database = match state.database() {
        Ok(database) => database,
        Err(error) => return error.into_response(),
    };
    let context = match database.tenant_credentials.authenticate_token(token).await {
        Ok(context) => context,
        Err(StorageError::InvalidTenantCredential) => {
            return unauthorized_bearer_response();
        }
        Err(error) => return ApiError::from_storage(error).into_response(),
    };

    request.extensions_mut().insert(context);
    next.run(request).await
}

fn unauthorized_bearer() -> ApiError {
    ApiError::new(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "a valid tenant Bearer credential is required for this resource",
    )
}

fn unauthorized_bearer_response() -> Response {
    let mut response = unauthorized_bearer().into_response();
    response
        .headers_mut()
        .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}

fn require_tenant_permission(
    context: &TenantContext,
    permission: TenantPermission,
) -> Result<(), ApiError> {
    if context.allows(permission) {
        Ok(())
    } else {
        Err(ApiError::forbidden())
    }
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

    fn forbidden() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            "the authenticated tenant role does not permit this operation",
        )
    }

    fn invalid_cursor() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_cursor",
            "the pagination cursor is malformed or does not match this resource",
        )
    }

    fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "the server could not complete the request",
        )
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
            StorageError::InvalidTenantCredential => unauthorized_bearer(),
            StorageError::WorkflowRunConflict(_) => Self::new(
                StatusCode::CONFLICT,
                "conflict",
                "the workflow-run identifier has already been used",
            ),
            StorageError::TaskRunNotFound(_) | StorageError::WorkflowRunNotFound(_) => Self::new(
                StatusCode::NOT_FOUND,
                "not_found",
                "the requested workflow run or task run does not exist",
            ),
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

fn parse_workflow_request(
    request: SubmitWorkflowRequest,
) -> Result<(WorkflowId, WorkflowRunId, WorkflowDefinition, RetryPolicy), ApiError> {
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

    Ok((workflow_id, workflow_run_id, definition, retry_policy))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowRunListQuery {
    status: Option<String>,
    limit: Option<u16>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventHistoryQuery {
    limit: Option<u16>,
    cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowRunCursor {
    created_at_micros: u64,
    workflow_run_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventHistoryCursor {
    before_event_id: i64,
}

#[derive(Serialize)]
struct WorkflowRunListResponse {
    items: Vec<WorkflowRunListItem>,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
struct WorkflowRunListItem {
    workflow_id: String,
    workflow_run_id: String,
    status: &'static str,
    version: u64,
    task_count: u64,
    created_at: String,
    updated_at: String,
}

impl From<&WorkflowRunSummary> for WorkflowRunListItem {
    fn from(value: &WorkflowRunSummary) -> Self {
        Self {
            workflow_id: value.workflow_id().as_str().to_owned(),
            workflow_run_id: value.workflow_run_id().as_str().to_owned(),
            status: workflow_status_name(value.status()),
            version: value.version(),
            task_count: value.task_count(),
            created_at: format_timestamp(value.created_at()),
            updated_at: format_timestamp(value.updated_at()),
        }
    }
}

#[derive(Serialize)]
struct WorkflowEventListResponse {
    events: Vec<WorkflowEventListItem>,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
struct WorkflowEventListItem {
    event_id: i64,
    aggregate_type: String,
    aggregate_id: String,
    aggregate_version: u64,
    event_type: String,
    status: Option<String>,
    created_at: String,
    published_at: Option<String>,
}

impl From<&WorkflowEventSummary> for WorkflowEventListItem {
    fn from(value: &WorkflowEventSummary) -> Self {
        Self {
            event_id: value.event_id(),
            aggregate_type: value.aggregate_type().to_owned(),
            aggregate_id: value.aggregate_id().to_owned(),
            aggregate_version: value.aggregate_version(),
            event_type: value.event_type().to_owned(),
            status: value.status().map(str::to_owned),
            created_at: format_timestamp(value.created_at()),
            published_at: value.published_at().map(format_timestamp),
        }
    }
}

fn decode_cursor<T>(encoded: &str) -> Result<T, ApiError>
where
    T: for<'de> Deserialize<'de>,
{
    if encoded.is_empty() || encoded.len() > 1024 {
        return Err(ApiError::invalid_cursor());
    }
    let decoded = BASE64_URL
        .decode(encoded)
        .map_err(|_| ApiError::invalid_cursor())?;
    if decoded.is_empty() || decoded.len() > 512 {
        return Err(ApiError::invalid_cursor());
    }
    serde_json::from_slice(&decoded).map_err(|_| ApiError::invalid_cursor())
}

fn encode_cursor<T: Serialize>(cursor: &T) -> Result<String, ApiError> {
    let encoded = serde_json::to_vec(cursor).map_err(|_| ApiError::internal())?;
    if encoded.len() > 512 {
        return Err(ApiError::internal());
    }
    Ok(BASE64_URL.encode(encoded))
}

fn page_limit(requested: Option<u16>) -> Result<i64, ApiError> {
    let limit = requested.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::invalid_request(StatusCode::BAD_REQUEST));
    }
    Ok(i64::from(limit))
}

fn validate_workflow_status_filter(status: Option<&str>) -> Result<(), ApiError> {
    if status.is_some_and(|value| {
        !matches!(
            value,
            "pending" | "running" | "succeeded" | "failed" | "cancelled" | "timed_out"
        )
    }) {
        return Err(ApiError::invalid_request(StatusCode::BAD_REQUEST));
    }
    Ok(())
}

fn workflow_run_cursor_position(
    cursor: Option<&str>,
) -> Result<Option<(SystemTime, WorkflowRunId)>, ApiError> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let cursor: WorkflowRunCursor = decode_cursor(cursor)?;
    let created_at = UNIX_EPOCH
        .checked_add(Duration::from_micros(cursor.created_at_micros))
        .ok_or_else(ApiError::invalid_cursor)?;
    let workflow_run_id =
        WorkflowRunId::parse(cursor.workflow_run_id).map_err(|_| ApiError::invalid_cursor())?;
    Ok(Some((created_at, workflow_run_id)))
}

fn timestamp_micros(value: SystemTime) -> Result<u64, ApiError> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ApiError::internal())?;
    u64::try_from(duration.as_micros()).map_err(|_| ApiError::internal())
}

async fn list_workflow_runs(
    State(state): State<AppState>,
    query: Result<Query<WorkflowRunListQuery>, QueryRejection>,
) -> Result<Json<WorkflowRunListResponse>, ApiError> {
    let Query(query) = query.map_err(|rejection| {
        ApiError::invalid_request(rejection.into_response().status())
    })?;
    validate_workflow_status_filter(query.status.as_deref())?;
    let limit = page_limit(query.limit)?;
    let position = workflow_run_cursor_position(query.cursor.as_deref())?;
    let (before_created_at, before_workflow_run_id) = position
        .as_ref()
        .map_or((None, None), |(time, id)| (Some(*time), Some(id)));
    let page = state
        .database()?
        .tasks
        .list_legacy_workflow_runs(
            query.status.as_deref(),
            limit,
            before_created_at,
            before_workflow_run_id,
        )
        .await
        .map_err(ApiError::from_storage)?;
    let next_cursor = if page.has_more() {
        let last = page.items().last().ok_or_else(ApiError::internal)?;
        Some(encode_cursor(&WorkflowRunCursor {
            created_at_micros: timestamp_micros(last.created_at())?,
            workflow_run_id: last.workflow_run_id().as_str().to_owned(),
        })?)
    } else {
        None
    };
    let items = page.items().iter().map(WorkflowRunListItem::from).collect();
    Ok(Json(WorkflowRunListResponse { items, next_cursor }))
}

async fn list_tenant_workflow_runs(
    State(state): State<AppState>,
    Extension(context): Extension<TenantContext>,
    query: Result<Query<WorkflowRunListQuery>, QueryRejection>,
) -> Result<Json<WorkflowRunListResponse>, ApiError> {
    require_tenant_permission(&context, TenantPermission::Read)?;
    let Query(query) = query.map_err(|rejection| {
        ApiError::invalid_request(rejection.into_response().status())
    })?;
    validate_workflow_status_filter(query.status.as_deref())?;
    let limit = page_limit(query.limit)?;
    let position = workflow_run_cursor_position(query.cursor.as_deref())?;
    let (before_created_at, before_workflow_run_id) = position
        .as_ref()
        .map_or((None, None), |(time, id)| (Some(*time), Some(id)));
    let page = state
        .database()?
        .tasks
        .list_workflow_runs_for_tenant(
            &context,
            query.status.as_deref(),
            limit,
            before_created_at,
            before_workflow_run_id,
        )
        .await
        .map_err(ApiError::from_storage)?;
    let next_cursor = if page.has_more() {
        let last = page.items().last().ok_or_else(ApiError::internal)?;
        Some(encode_cursor(&WorkflowRunCursor {
            created_at_micros: timestamp_micros(last.created_at())?,
            workflow_run_id: last.workflow_run_id().as_str().to_owned(),
        })?)
    } else {
        None
    };
    let items = page.items().iter().map(WorkflowRunListItem::from).collect();
    Ok(Json(WorkflowRunListResponse { items, next_cursor }))
}

async fn list_workflow_events(
    State(state): State<AppState>,
    Path(raw_workflow_run_id): Path<String>,
    query: Result<Query<EventHistoryQuery>, QueryRejection>,
) -> Result<Json<WorkflowEventListResponse>, ApiError> {
    let workflow_run_id =
        WorkflowRunId::parse(raw_workflow_run_id).map_err(|_| ApiError::invalid_identifier())?;
    let Query(query) = query.map_err(|rejection| {
        ApiError::invalid_request(rejection.into_response().status())
    })?;
    let limit = page_limit(query.limit)?;
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_cursor::<EventHistoryCursor>)
        .transpose()?;
    if cursor.as_ref().is_some_and(|value| value.before_event_id <= 0) {
        return Err(ApiError::invalid_cursor());
    }
    let page = state
        .database()?
        .tasks
        .list_legacy_workflow_events(
            &workflow_run_id,
            limit,
            cursor.map(|value| value.before_event_id),
        )
        .await
        .map_err(ApiError::from_storage)?;
    let next_cursor = if page.has_more() {
        let last = page.events().last().ok_or_else(ApiError::internal)?;
        Some(encode_cursor(&EventHistoryCursor {
            before_event_id: last.event_id(),
        })?)
    } else {
        None
    };
    let events = page.events().iter().map(WorkflowEventListItem::from).collect();
    Ok(Json(WorkflowEventListResponse { events, next_cursor }))
}

async fn list_tenant_workflow_events(
    State(state): State<AppState>,
    Extension(context): Extension<TenantContext>,
    Path(raw_workflow_run_id): Path<String>,
    query: Result<Query<EventHistoryQuery>, QueryRejection>,
) -> Result<Json<WorkflowEventListResponse>, ApiError> {
    require_tenant_permission(&context, TenantPermission::Read)?;
    let workflow_run_id =
        WorkflowRunId::parse(raw_workflow_run_id).map_err(|_| ApiError::invalid_identifier())?;
    let Query(query) = query.map_err(|rejection| {
        ApiError::invalid_request(rejection.into_response().status())
    })?;
    let limit = page_limit(query.limit)?;
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_cursor::<EventHistoryCursor>)
        .transpose()?;
    if cursor.as_ref().is_some_and(|value| value.before_event_id <= 0) {
        return Err(ApiError::invalid_cursor());
    }
    let page = state
        .database()?
        .tasks
        .list_workflow_events_for_tenant(
            &context,
            &workflow_run_id,
            limit,
            cursor.map(|value| value.before_event_id),
        )
        .await
        .map_err(ApiError::from_storage)?;
    let next_cursor = if page.has_more() {
        let last = page.events().last().ok_or_else(ApiError::internal)?;
        Some(encode_cursor(&EventHistoryCursor {
            before_event_id: last.event_id(),
        })?)
    } else {
        None
    };
    let events = page.events().iter().map(WorkflowEventListItem::from).collect();
    Ok(Json(WorkflowEventListResponse { events, next_cursor }))
}

async fn submit_workflow(
    State(state): State<AppState>,
    payload: Result<Json<SubmitWorkflowRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<WorkflowRunReceipt>), ApiError> {
    let Json(request) = payload
        .map_err(|rejection| ApiError::invalid_request(rejection.into_response().status()))?;
    let (workflow_id, workflow_run_id, definition, retry_policy) = parse_workflow_request(request)?;

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

async fn submit_tenant_workflow(
    State(state): State<AppState>,
    Extension(context): Extension<TenantContext>,
    payload: Result<Json<SubmitWorkflowRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<WorkflowRunReceipt>), ApiError> {
    require_tenant_permission(&context, TenantPermission::Submit)?;
    let Json(request) = payload
        .map_err(|rejection| ApiError::invalid_request(rejection.into_response().status()))?;
    let (workflow_id, workflow_run_id, definition, retry_policy) = parse_workflow_request(request)?;

    let database = state.database()?;
    let submission = database
        .coordinator
        .submit_workflow_for_tenant(&context, &workflow_run_id, &definition, retry_policy, None)
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

async fn get_tenant_workflow_run(
    State(state): State<AppState>,
    Extension(context): Extension<TenantContext>,
    Path(raw_workflow_run_id): Path<String>,
) -> Result<Json<WorkflowRunSnapshot>, ApiError> {
    require_tenant_permission(&context, TenantPermission::Read)?;
    let workflow_run_id =
        WorkflowRunId::parse(raw_workflow_run_id).map_err(|_| ApiError::invalid_identifier())?;
    let database = state.database()?;
    let snapshot = database
        .tasks
        .get_workflow_run_for_tenant(&context, &workflow_run_id)
        .await
        .map_err(ApiError::from_storage)?;
    Ok(Json(WorkflowRunSnapshot::from(snapshot)))
}

async fn get_tenant_task_run(
    State(state): State<AppState>,
    Extension(context): Extension<TenantContext>,
    Path(raw_task_run_id): Path<String>,
) -> Result<Json<TaskRunSnapshot>, ApiError> {
    require_tenant_permission(&context, TenantPermission::Read)?;
    let task_run_id = aetherflow_core::domain::TaskRunId::parse(raw_task_run_id)
        .map_err(|_| ApiError::invalid_identifier())?;
    let database = state.database()?;
    let snapshot = database
        .tasks
        .get_task_run_for_tenant(&context, &task_run_id)
        .await
        .map_err(ApiError::from_storage)?;
    Ok(Json(TaskRunSnapshot::from(&snapshot)))
}

async fn cancel_tenant_task_run(
    State(state): State<AppState>,
    Extension(context): Extension<TenantContext>,
    Path(raw_task_run_id): Path<String>,
    payload: Result<Json<CancelTaskRunRequest>, JsonRejection>,
) -> Result<Json<CancelTaskRunResponse>, ApiError> {
    require_tenant_permission(&context, TenantPermission::Cancel)?;
    let Json(request) = payload
        .map_err(|rejection| ApiError::invalid_request(rejection.into_response().status()))?;
    let task_run_id = aetherflow_core::domain::TaskRunId::parse(raw_task_run_id)
        .map_err(|_| ApiError::invalid_identifier())?;
    let database = state.database()?;
    let cancelled = database
        .tasks
        .cancel_task_run_for_tenant(&context, &task_run_id, request.expected_version)
        .await
        .map_err(ApiError::from_storage)?;
    let workflow_status = database
        .tasks
        .aggregate_workflow_run_for_tenant(&context, cancelled.workflow_run_id())
        .await
        .map_err(ApiError::from_storage)?;

    Ok(Json(CancelTaskRunResponse {
        task_run: TaskRunSnapshot::from(&cancelled),
        workflow_status: workflow_status_name(workflow_status),
    }))
}

async fn get_workflow_run(
    State(state): State<AppState>,
    Path(raw_workflow_run_id): Path<String>,
) -> Result<Json<WorkflowRunSnapshot>, ApiError> {
    let workflow_run_id =
        WorkflowRunId::parse(raw_workflow_run_id).map_err(|_| ApiError::invalid_identifier())?;
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
    let task_run_id = aetherflow_core::domain::TaskRunId::parse(raw_task_run_id)
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
    let Json(request) = payload
        .map_err(|rejection| ApiError::invalid_request(rejection.into_response().status()))?;
    let task_run_id = aetherflow_core::domain::TaskRunId::parse(raw_task_run_id)
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
    use super::{AppState, authorization_matches, hash_api_token, router};
    use axum::body::{Body, to_bytes};
    use axum::http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
    use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
    use tower::ServiceExt;

    const TEST_API_TOKEN: &str = "test-only-aetherflow-api-token-32-bytes";

    #[test]
    fn operator_tokens_require_high_entropy_url_safe_material() {
        assert!(hash_api_token(TEST_API_TOKEN).is_ok());
        assert!(hash_api_token("short").is_err());
        assert!(hash_api_token("a token containing spaces and punctuation!").is_err());
        assert!(hash_api_token(&"a".repeat(257)).is_err());
    }

    #[test]
    fn bearer_authentication_requires_exact_scheme_and_a_matching_digest() {
        let expected = hash_api_token(TEST_API_TOKEN).expect("valid test token");
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer test-only-aetherflow-api-token-32-bytes"),
        );
        assert!(authorization_matches(&expected, &headers));

        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("bearer test-only-aetherflow-api-token-32-bytes"),
        );
        assert!(!authorization_matches(&expected, &headers));

        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer incorrect-test-token-material-long-enough"),
        );
        assert!(!authorization_matches(&expected, &headers));

        headers.remove(AUTHORIZATION);
        assert!(!authorization_matches(&expected, &headers));
    }

    #[tokio::test]
    async fn dashboard_routes_validate_query_and_cursor_before_database_access() {
        let invalid_cursor = router(AppState::without_database())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/workflow-runs?cursor=not-a-valid-cursor")
                    .body(Body::empty())
                    .expect("valid workflow list request"),
            )
            .await
            .expect("invalid cursor response");
        assert_eq!(invalid_cursor.status(), StatusCode::BAD_REQUEST);
        let invalid_cursor_body = to_bytes(invalid_cursor.into_body(), 1024)
            .await
            .expect("read cursor error");
        let invalid_cursor_error: serde_json::Value =
            serde_json::from_slice(&invalid_cursor_body).expect("valid cursor error JSON");
        assert_eq!(invalid_cursor_error["error"]["code"], "invalid_cursor");

        let invalid_status = router(AppState::without_database())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/workflow-runs?status=unknown")
                    .body(Body::empty())
                    .expect("valid status filter request"),
            )
            .await
            .expect("invalid status response");
        assert_eq!(invalid_status.status(), StatusCode::BAD_REQUEST);

        let too_many = router(AppState::without_database())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/workflow-runs?limit=101")
                    .body(Body::empty())
                    .expect("valid page-size request"),
            )
            .await
            .expect("invalid page-size response");
        assert_eq!(too_many.status(), StatusCode::BAD_REQUEST);
    }

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
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("read database-unavailable response");
        let error: serde_json::Value =
            serde_json::from_slice(&body).expect("valid service error JSON");
        assert_eq!(error["error"]["code"], "database_unavailable");
    }

    #[tokio::test]
    async fn storage_failures_map_to_safe_service_unavailable_errors() {
        use super::{ApiError, StorageError};
        use axum::response::IntoResponse;

        for error in [
            StorageError::Pool("private pool diagnostic".to_owned()),
            StorageError::Database("private database diagnostic".to_owned()),
        ] {
            let response = ApiError::from_storage(error).into_response();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            let body = to_bytes(response.into_body(), 1024)
                .await
                .expect("read stable storage error");
            let error: serde_json::Value =
                serde_json::from_slice(&body).expect("valid service error JSON");
            assert_eq!(error["error"]["code"], "database_unavailable");
            let message = error["error"]["message"]
                .as_str()
                .expect("public safe error message");
            assert!(!message.contains("private"));
        }
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
        let response: serde_json::Value = serde_json::from_slice(&body).expect("stable error JSON");
        assert_eq!(
            response["error"]["code"],
            serde_json::Value::String("invalid_identifier".to_owned())
        );
    }

    #[tokio::test]
    async fn postgres_http_workflow_lifecycle_is_versioned_and_redacts_payloads() {
        use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
        use tokio_postgres::{Config, NoTls};

        let Ok(database_url) = std::env::var("AETHERFLOW_TEST_DATABASE_URL") else {
            eprintln!(
                "AETHERFLOW_TEST_DATABASE_URL is not set; skipping HTTP/PostgreSQL integration test"
            );
            return;
        };

        let config: Config = database_url.parse().expect("valid PostgreSQL URL");
        let manager = Manager::from_config(
            config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(8)
            .build()
            .expect("build PostgreSQL test pool");
        aetherflow_storage_postgres::PgStore::from_pool(pool.clone())
            .migrate()
            .await
            .expect("ensure PostgreSQL schema is current");

        let state = AppState::from_pool(pool.clone(), TEST_API_TOKEN)
            .expect("configure integration API token");
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos()
            .to_string();
        let workflow_id = format!("api-workflow-{suffix}");
        let workflow_run_id = format!("api-run-{suffix}");
        let task_id = format!("api-task-{suffix}");
        let body = serde_json::json!({
            "workflow_id": workflow_id,
            "workflow_run_id": workflow_run_id,
            "tasks": [{
                "task_id": task_id,
                "depends_on": [],
                "input_base64": "cHJpdmF0ZS1wYXlsb2Fk"
            }],
            "retry_policy": {
                "max_attempts": 1,
                "initial_backoff_ms": 0,
                "max_backoff_ms": 0
            }
        })
        .to_string();

        let unauthenticated = router(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/workers")
                    .body(Body::empty())
                    .expect("valid unauthenticated request"),
            )
            .await
            .expect("unauthenticated response");
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            unauthenticated.headers().get("www-authenticate"),
            Some(&HeaderValue::from_static("Bearer"))
        );

        let submit = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .method("POST")
                    .uri("/api/v1/workflow-runs")
                    .header("content-type", "application/json")
                    .body(Body::from(body.clone()))
                    .expect("valid workflow submission request"),
            )
            .await
            .expect("workflow submission response");
        assert_eq!(submit.status(), StatusCode::CREATED);
        let submit_body = to_bytes(submit.into_body(), 16 * 1024)
            .await
            .expect("read workflow receipt");
        let receipt: serde_json::Value =
            serde_json::from_slice(&submit_body).expect("valid workflow receipt JSON");
        assert_eq!(receipt["status"], "running");
        assert_eq!(receipt["task_runs"][0]["status"], "pending");
        let task_run_id = receipt["task_runs"][0]["task_run_id"]
            .as_str()
            .expect("task-run ID in receipt")
            .to_owned();

        let duplicate = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .method("POST")
                    .uri("/api/v1/workflow-runs")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .expect("valid duplicate workflow submission request"),
            )
            .await
            .expect("duplicate submission response");
        assert_eq!(duplicate.status(), StatusCode::CONFLICT);

        let workflow = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .uri(format!("/api/v1/workflow-runs/{workflow_run_id}"))
                    .body(Body::empty())
                    .expect("valid workflow inspection request"),
            )
            .await
            .expect("workflow inspection response");
        assert_eq!(workflow.status(), StatusCode::OK);
        let workflow_body = to_bytes(workflow.into_body(), 16 * 1024)
            .await
            .expect("read workflow snapshot");
        let workflow_snapshot: serde_json::Value =
            serde_json::from_slice(&workflow_body).expect("valid workflow snapshot JSON");
        assert_eq!(workflow_snapshot["workflow_id"], workflow_id);
        assert_eq!(
            workflow_snapshot["task_runs"].as_array().map(Vec::len),
            Some(1)
        );

        let missing_workflow = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .uri(format!("/api/v1/workflow-runs/{workflow_run_id}-missing"))
                    .body(Body::empty())
                    .expect("valid missing workflow inspection request"),
            )
            .await
            .expect("missing workflow response");
        assert_eq!(missing_workflow.status(), StatusCode::NOT_FOUND);
        let missing_workflow_body = to_bytes(missing_workflow.into_body(), 1024)
            .await
            .expect("read missing workflow response");
        let missing_workflow_error: serde_json::Value =
            serde_json::from_slice(&missing_workflow_body).expect("valid error JSON");
        assert_eq!(missing_workflow_error["error"]["code"], "not_found");

        let task = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .uri(format!("/api/v1/task-runs/{task_run_id}"))
                    .body(Body::empty())
                    .expect("valid task inspection request"),
            )
            .await
            .expect("task inspection response");
        assert_eq!(task.status(), StatusCode::OK);

        let missing_task = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .uri(format!("/api/v1/task-runs/{task_run_id}-missing"))
                    .body(Body::empty())
                    .expect("valid missing task inspection request"),
            )
            .await
            .expect("missing task response");
        assert_eq!(missing_task.status(), StatusCode::NOT_FOUND);
        let missing_task_body = to_bytes(missing_task.into_body(), 1024)
            .await
            .expect("read missing task response");
        let missing_task_error: serde_json::Value =
            serde_json::from_slice(&missing_task_body).expect("valid error JSON");
        assert_eq!(missing_task_error["error"]["code"], "not_found");

        let task_body = to_bytes(task.into_body(), 16 * 1024)
            .await
            .expect("read task snapshot");
        let task_snapshot: serde_json::Value =
            serde_json::from_slice(&task_body).expect("valid task snapshot JSON");
        assert_eq!(task_snapshot["input_size_bytes"], 15);
        assert_eq!(task_snapshot["output_present"], false);
        assert!(task_snapshot.get("input_base64").is_none());
        assert!(task_snapshot.get("input_payload").is_none());
        assert!(task_snapshot.get("output_payload").is_none());

        let cancel = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .method("POST")
                    .uri(format!("/api/v1/task-runs/{task_run_id}/cancel"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"expected_version":0}"#))
                    .expect("valid task cancellation request"),
            )
            .await
            .expect("task cancellation response");
        assert_eq!(cancel.status(), StatusCode::OK);
        let cancel_body = to_bytes(cancel.into_body(), 16 * 1024)
            .await
            .expect("read cancellation response");
        let cancelled: serde_json::Value =
            serde_json::from_slice(&cancel_body).expect("valid cancellation response JSON");
        assert_eq!(cancelled["task_run"]["status"], "cancelled");
        assert_eq!(cancelled["workflow_status"], "cancelled");

        let stale_cancel = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .method("POST")
                    .uri(format!("/api/v1/task-runs/{task_run_id}/cancel"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"expected_version":0}"#))
                    .expect("valid stale cancellation request"),
            )
            .await
            .expect("stale cancellation response");
        assert_eq!(stale_cancel.status(), StatusCode::CONFLICT);

        use std::num::NonZeroU32;
        use std::time::Duration;

        use aetherflow_core::domain::{NodeId, NodeIncarnationId, WorkerId};
        use aetherflow_storage_postgres::PgMembershipRepository;

        let membership = PgMembershipRepository::from_pool(pool.clone());
        let node_id = NodeId::parse(format!("api-node-{suffix}")).expect("valid node ID");
        let incarnation_id = NodeIncarnationId::parse(format!("api-incarnation-{suffix}"))
            .expect("valid node incarnation ID");
        let worker_id = WorkerId::parse(format!("api-worker-{suffix}")).expect("valid worker ID");
        let node = membership
            .join_node(&node_id, &incarnation_id, Duration::from_secs(60))
            .await
            .expect("create active node membership");
        membership
            .register_worker(
                &node,
                &worker_id,
                NonZeroU32::new(2).expect("non-zero worker concurrency"),
                Duration::from_secs(30),
            )
            .await
            .expect("register active worker");

        let workers = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .uri("/api/v1/workers")
                    .body(Body::empty())
                    .expect("valid worker listing request"),
            )
            .await
            .expect("worker listing response");
        assert_eq!(workers.status(), StatusCode::OK);
        let workers_body = to_bytes(workers.into_body(), 16 * 1024)
            .await
            .expect("read active worker list");
        let worker_list: serde_json::Value =
            serde_json::from_slice(&workers_body).expect("valid worker list JSON");
        assert_eq!(worker_list["workers"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            worker_list["workers"][0]["worker_id"].as_str(),
            Some(worker_id.as_str())
        );
        assert_eq!(worker_list["workers"][0]["status"], "active");

        // Tenant credentials are independent from the platform operator token.
        // The middleware derives tenant identity only from the credential record.
        use sha2::{Digest, Sha256};

        let tenant_id = format!("api-tenant-{suffix}");
        let tenant_display = format!("API tenant {suffix}");
        let foreign_tenant_id = format!("api-foreign-tenant-{suffix}");
        let foreign_tenant_display = format!("Foreign API tenant {suffix}");
        let tenant_principal_id = format!("api-principal-{suffix}");
        let reader_principal_id = format!("api-reader-{suffix}");
        let tenant_token = format!("tenant-api-token-{suffix}-abcdefghijklmnopqrstuvwxyz");
        let reader_token = format!("tenant-reader-token-{suffix}-abcdefghijklmnopqrstuvwxyz");
        let tenant_token_digest = Sha256::digest(tenant_token.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let reader_token_digest = Sha256::digest(reader_token.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();

        let database_client = pool
            .get()
            .await
            .expect("get tenant HTTP integration database connection");
        database_client
            .execute(
                "INSERT INTO tenants (tenant_id, display_name, status)
                 VALUES ($1, $2, 'active'), ($3, $4, 'active')",
                &[
                    &tenant_id,
                    &tenant_display,
                    &foreign_tenant_id,
                    &foreign_tenant_display,
                ],
            )
            .await
            .expect("seed two active HTTP tenants");
        database_client
            .execute(
                "INSERT INTO tenant_principals
                   (principal_id, tenant_id, display_name, status)
                 VALUES ($1, $2, 'Tenant operator', 'active'),
                        ($3, $2, 'Tenant reader', 'active')",
                &[&tenant_principal_id, &tenant_id, &reader_principal_id],
            )
            .await
            .expect("seed tenant operator and reader principals");
        database_client
            .execute(
                "INSERT INTO tenant_credentials
                   (credential_id, tenant_id, principal_id, token_sha256,
                    token_prefix, role, status)
                 VALUES ($1, $2, $3, $4, $5, 'tenant_operator', 'active'),
                        ($6, $2, $7, $8, $9, 'tenant_reader', 'active')",
                &[
                    &format!("api-operator-credential-{suffix}"),
                    &tenant_id,
                    &tenant_principal_id,
                    &tenant_token_digest,
                    &&tenant_token[..12],
                    &format!("api-reader-credential-{suffix}"),
                    &reader_principal_id,
                    &reader_token_digest,
                    &&reader_token[..12],
                ],
            )
            .await
            .expect("seed tenant operator and reader credentials");

        let foreign_workflow_run_id = format!("foreign-api-run-{suffix}");
        let foreign_workflow_id = format!("foreign-api-workflow-{suffix}");
        let foreign_task_run_id = format!("foreign-api-task-run-{suffix}");
        let foreign_task_id = format!("foreign-api-task-{suffix}");
        database_client
            .execute(
                "INSERT INTO workflow_runs
                   (workflow_run_id, workflow_id, tenant_id, status)
                 VALUES ($1, $2, $3, 'pending')",
                &[
                    &foreign_workflow_run_id,
                    &foreign_workflow_id,
                    &foreign_tenant_id,
                ],
            )
            .await
            .expect("seed foreign tenant workflow");
        database_client
            .execute(
                "INSERT INTO task_runs
                   (task_run_id, workflow_run_id, task_id, tenant_id, status)
                 VALUES ($1, $2, $3, $4, 'pending')",
                &[
                    &foreign_task_run_id,
                    &foreign_workflow_run_id,
                    &foreign_task_id,
                    &foreign_tenant_id,
                ],
            )
            .await
            .expect("seed foreign tenant task");

        let tenant_workflow_id = format!("tenant-api-workflow-{suffix}");
        let tenant_workflow_run_id = format!("tenant-api-run-{suffix}");
        let tenant_task_id = format!("tenant-api-task-{suffix}");
        let tenant_body = serde_json::json!({
            "workflow_id": tenant_workflow_id,
            "workflow_run_id": tenant_workflow_run_id,
            "tasks": [{
                "task_id": tenant_task_id,
                "depends_on": [],
                "input_base64": "dGVuYW50LWJvdW5k"
            }],
            "retry_policy": {
                "max_attempts": 1,
                "initial_backoff_ms": 0,
                "max_backoff_ms": 0
            }
        })
        .to_string();

        let operator_token_on_tenant_route = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .method("POST")
                    .uri("/api/v1/tenant/workflow-runs")
                    .header("content-type", "application/json")
                    .body(Body::from(tenant_body.clone()))
                    .expect("valid operator-token tenant-route request"),
            )
            .await
            .expect("tenant route operator-token response");
        assert_eq!(
            operator_token_on_tenant_route.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            operator_token_on_tenant_route
                .headers()
                .get(WWW_AUTHENTICATE),
            Some(&HeaderValue::from_static("Bearer"))
        );

        let invalid_tenant_token = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(
                        AUTHORIZATION,
                        "Bearer invalid-tenant-credential-material-that-is-long-enough",
                    )
                    .uri("/api/v1/tenant/workflow-runs/unknown")
                    .body(Body::empty())
                    .expect("valid invalid-credential request"),
            )
            .await
            .expect("invalid tenant credential response");
        assert_eq!(invalid_tenant_token.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            invalid_tenant_token.headers().get(WWW_AUTHENTICATE),
            Some(&HeaderValue::from_static("Bearer"))
        );

        let tenant_submit = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {tenant_token}"))
                    .method("POST")
                    .uri("/api/v1/tenant/workflow-runs")
                    .header("content-type", "application/json")
                    .body(Body::from(tenant_body.clone()))
                    .expect("valid tenant workflow submission request"),
            )
            .await
            .expect("tenant workflow submission response");
        assert_eq!(tenant_submit.status(), StatusCode::CREATED);
        let tenant_submit_body = to_bytes(tenant_submit.into_body(), 16 * 1024)
            .await
            .expect("read tenant workflow receipt");
        let tenant_receipt: serde_json::Value =
            serde_json::from_slice(&tenant_submit_body).expect("valid tenant receipt JSON");
        let tenant_task_run_id = tenant_receipt["task_runs"][0]["task_run_id"]
            .as_str()
            .expect("tenant task-run ID in receipt")
            .to_owned();

        let own_tenant_workflow = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {tenant_token}"))
                    .uri(format!(
                        "/api/v1/tenant/workflow-runs/{tenant_workflow_run_id}"
                    ))
                    .body(Body::empty())
                    .expect("valid own-tenant workflow read"),
            )
            .await
            .expect("own-tenant workflow read response");
        assert_eq!(own_tenant_workflow.status(), StatusCode::OK);

        let own_tenant_task = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {tenant_token}"))
                    .uri(format!("/api/v1/tenant/task-runs/{tenant_task_run_id}"))
                    .body(Body::empty())
                    .expect("valid own-tenant task read"),
            )
            .await
            .expect("own-tenant task read response");
        assert_eq!(own_tenant_task.status(), StatusCode::OK);

        let tenant_cancel = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {tenant_token}"))
                    .method("POST")
                    .uri(format!(
                        "/api/v1/tenant/task-runs/{tenant_task_run_id}/cancel"
                    ))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"expected_version":0}"#))
                    .expect("valid tenant cancellation request"),
            )
            .await
            .expect("tenant cancellation response");
        assert_eq!(tenant_cancel.status(), StatusCode::OK);
        let tenant_cancel_body = to_bytes(tenant_cancel.into_body(), 16 * 1024)
            .await
            .expect("read tenant cancellation response");
        let tenant_cancelled: serde_json::Value =
            serde_json::from_slice(&tenant_cancel_body).expect("valid tenant cancellation JSON");
        assert_eq!(tenant_cancelled["task_run"]["status"], "cancelled");
        assert_eq!(tenant_cancelled["workflow_status"], "cancelled");

        let foreign_workflow_read = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {tenant_token}"))
                    .uri(format!(
                        "/api/v1/tenant/workflow-runs/{foreign_workflow_run_id}"
                    ))
                    .body(Body::empty())
                    .expect("valid foreign-tenant workflow read"),
            )
            .await
            .expect("foreign-tenant workflow read response");
        assert_eq!(foreign_workflow_read.status(), StatusCode::NOT_FOUND);
        let missing_tenant_workflow_read = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {tenant_token}"))
                    .uri(format!(
                        "/api/v1/tenant/workflow-runs/{foreign_workflow_run_id}-missing"
                    ))
                    .body(Body::empty())
                    .expect("valid missing-tenant workflow read"),
            )
            .await
            .expect("missing-tenant workflow read response");
        assert_eq!(missing_tenant_workflow_read.status(), StatusCode::NOT_FOUND);

        let reader_read = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {reader_token}"))
                    .uri(format!(
                        "/api/v1/tenant/workflow-runs/{tenant_workflow_run_id}"
                    ))
                    .body(Body::empty())
                    .expect("valid tenant reader request"),
            )
            .await
            .expect("tenant reader read response");
        assert_eq!(reader_read.status(), StatusCode::OK);

        let reader_submit = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {reader_token}"))
                    .method("POST")
                    .uri("/api/v1/tenant/workflow-runs")
                    .header("content-type", "application/json")
                    .body(Body::from(tenant_body))
                    .expect("valid tenant reader submit request"),
            )
            .await
            .expect("tenant reader submit response");
        assert_eq!(reader_submit.status(), StatusCode::FORBIDDEN);

        let reader_cancel = router(state)
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {reader_token}"))
                    .method("POST")
                    .uri(format!(
                        "/api/v1/tenant/task-runs/{tenant_task_run_id}/cancel"
                    ))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"expected_version":0}"#))
                    .expect("valid tenant reader cancel request"),
            )
            .await
            .expect("tenant reader cancel response");
        assert_eq!(reader_cancel.status(), StatusCode::FORBIDDEN);
    }
}
