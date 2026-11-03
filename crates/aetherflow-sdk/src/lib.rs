//! Typed HTTP client for the AetherFlow v1 control plane.
//!
//! The SDK does not connect to PostgreSQL or launch workers. Operator and
//! tenant credentials use separate route scopes. Plain HTTP is allowed only
//! for loopback development; remote control planes require HTTPS.

use std::collections::{HashMap, HashSet, VecDeque};
use std::error::Error;
use std::fmt;
use std::io::{self, Write};
use std::net::IpAddr;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use reqwest::{Client, Method, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Maximum HTTP request and response body accepted by the v1 server.
pub const MAX_HTTP_BODY_BYTES: usize = 2 * 1024 * 1024;
/// Maximum decoded input bytes accepted for one task.
pub const MAX_TASK_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_BASE64_TASK_PAYLOAD_BYTES: usize = MAX_TASK_PAYLOAD_BYTES.div_ceil(3) * 4;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Errors returned by local validation, HTTP transport, or the control plane.
#[derive(Debug)]
pub enum ClientError {
    /// The base URL is malformed or contains credentials, a query, or a fragment.
    InvalidBaseUrl,
    /// Remote plain HTTP is refused to protect Bearer credentials in transit.
    InsecureRemoteHttp,
    /// A selected credential is empty.
    EmptyCredential,
    /// An authenticated operation was attempted with an anonymous client.
    AuthenticationRequired,
    /// An entity identifier does not match the v1 ASCII identifier contract.
    InvalidIdentifier,
    /// A workflow definition or retry policy is invalid.
    InvalidWorkflow(&'static str),
    /// A decoded task input exceeds the v1 per-task limit.
    TaskPayloadTooLarge,
    /// A serialized request exceeds the v1 HTTP body limit.
    RequestTooLarge,
    /// A control-plane response exceeds the SDK response limit.
    ResponseTooLarge,
    /// Worker inventory is available only to operator credentials.
    OperatorOnly,
    /// The API returned a non-success HTTP response.
    Api {
        status: u16,
        code: Option<String>,
        message: Option<String>,
        request_id: Option<String>,
    },
    /// The HTTP request failed before a response was received.
    Transport(reqwest::Error),
    /// A request or response could not be encoded or decoded as JSON.
    Json(serde_json::Error),
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBaseUrl => formatter.write_str(
                "server URL must be an http(s) base URL without credentials, query, or fragment",
            ),
            Self::InsecureRemoteHttp => formatter.write_str(
                "remote control-plane URLs must use HTTPS; plain HTTP is allowed only on loopback",
            ),
            Self::EmptyCredential => formatter.write_str("the selected Bearer credential is empty"),
            Self::AuthenticationRequired => {
                formatter.write_str("this operation requires an operator or tenant credential")
            }
            Self::InvalidIdentifier => formatter
                .write_str("IDs must contain 1–128 ASCII letters, digits, underscores, or hyphens"),
            Self::InvalidWorkflow(message) => write!(formatter, "invalid workflow: {message}"),
            Self::TaskPayloadTooLarge => write!(
                formatter,
                "decoded task input exceeds the {MAX_TASK_PAYLOAD_BYTES}-byte limit"
            ),
            Self::RequestTooLarge => write!(
                formatter,
                "serialized request exceeds the {MAX_HTTP_BODY_BYTES}-byte HTTP body limit"
            ),
            Self::ResponseTooLarge => write!(
                formatter,
                "control-plane response exceeds the {MAX_HTTP_BODY_BYTES}-byte SDK limit"
            ),
            Self::OperatorOnly => {
                formatter.write_str("active worker inventory requires an operator credential")
            }
            Self::Api {
                status,
                code,
                message,
                request_id,
            } => {
                write!(formatter, "control-plane returned HTTP {status}")?;
                if let Some(code) = code {
                    write!(formatter, " ({code})")?;
                }
                if let Some(message) = message {
                    write!(formatter, ": {message}")?;
                }
                if let Some(request_id) = request_id {
                    write!(formatter, " [request_id={request_id}]")?;
                }
                Ok(())
            }
            Self::Transport(error) => write!(formatter, "control-plane transport error: {error}"),
            Self::Json(error) => write!(formatter, "control-plane JSON error: {error}"),
        }
    }
}

impl Error for ClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

/// Validate an AetherFlow entity ID before using it in a URL path or request.
pub fn validate_entity_id(value: &str) -> Result<(), ClientError> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(ClientError::InvalidIdentifier)
    }
}

/// Workflow lifecycle values returned by the v1 API.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

/// Task lifecycle values returned by the v1 API.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Ready,
    Running,
    RetryWaiting,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

/// Public liveness response. Health is process-level, not database readiness.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HealthResponse {
    /// Always `ok` when the process accepts health requests.
    pub status: String,
}

/// Retry policy used when a workflow run is submitted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    /// Maximum attempts, including the initial attempt.
    pub max_attempts: u32,
    /// Initial retry backoff in milliseconds.
    pub initial_backoff_ms: u64,
    /// Maximum retry backoff in milliseconds.
    pub max_backoff_ms: u64,
}

impl RetryPolicy {
    /// Construct a retry policy satisfying the server's numeric bounds.
    pub fn new(
        max_attempts: u32,
        initial_backoff_ms: u64,
        max_backoff_ms: u64,
    ) -> Result<Self, ClientError> {
        let policy = Self {
            max_attempts,
            initial_backoff_ms,
            max_backoff_ms,
        };
        policy.validate()?;
        Ok(policy)
    }

    fn validate(&self) -> Result<(), ClientError> {
        if self.max_attempts == 0 || self.max_attempts > i32::MAX as u32 {
            return Err(ClientError::InvalidWorkflow(
                "max_attempts must be between 1 and 2147483647",
            ));
        }
        if self.initial_backoff_ms > self.max_backoff_ms || self.max_backoff_ms > i64::MAX as u64 {
            return Err(ClientError::InvalidWorkflow(
                "backoff values must fit signed 64-bit milliseconds and max_backoff_ms must be at least initial_backoff_ms",
            ));
        }
        Ok(())
    }
}

/// One task definition in a workflow submission.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowTaskInput {
    /// Unique task ID within this workflow.
    pub task_id: String,
    /// IDs of tasks that must succeed before this task is promoted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    /// Optional standard-base64 encoded opaque input bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_base64: Option<String>,
}

impl WorkflowTaskInput {
    /// Create a task definition without dependencies or input bytes.
    #[must_use]
    pub fn new(task_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            depends_on: Vec::new(),
            input_base64: None,
        }
    }

    /// Attach task IDs that must succeed before this task is promoted.
    #[must_use]
    pub fn with_dependencies(mut self, dependencies: Vec<String>) -> Self {
        self.depends_on = dependencies;
        self
    }

    /// Encode opaque task input using the standard base64 alphabet.
    pub fn with_input_bytes(mut self, input: &[u8]) -> Result<Self, ClientError> {
        if input.len() > MAX_TASK_PAYLOAD_BYTES {
            return Err(ClientError::TaskPayloadTooLarge);
        }
        self.input_base64 = Some(BASE64.encode(input));
        Ok(self)
    }
}

/// A complete, caller-supplied workflow-run submission.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowRunSubmission {
    /// Workflow identity.
    pub workflow_id: String,
    /// Caller-supplied unique run identity.
    pub workflow_run_id: String,
    /// Tasks and their dependency graph.
    pub tasks: Vec<WorkflowTaskInput>,
    /// Retry policy shared by the tasks in this run.
    pub retry_policy: RetryPolicy,
}

impl WorkflowRunSubmission {
    /// Construct a workflow submission. Validation runs before network I/O.
    #[must_use]
    pub fn new(
        workflow_id: impl Into<String>,
        workflow_run_id: impl Into<String>,
        tasks: Vec<WorkflowTaskInput>,
        retry_policy: RetryPolicy,
    ) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            workflow_run_id: workflow_run_id.into(),
            tasks,
            retry_policy,
        }
    }

    /// Validate IDs, retry bounds, input encoding, dependency references, and DAG acyclicity.
    pub fn validate(&self) -> Result<(), ClientError> {
        validate_entity_id(&self.workflow_id)?;
        validate_entity_id(&self.workflow_run_id)?;
        self.retry_policy.validate()?;
        if self.tasks.is_empty() || self.tasks.len() > 1000 {
            return Err(ClientError::InvalidWorkflow(
                "a workflow must contain between 1 and 1000 tasks",
            ));
        }

        let mut task_ids = HashSet::with_capacity(self.tasks.len());
        for task in &self.tasks {
            validate_entity_id(&task.task_id)?;
            if !task_ids.insert(task.task_id.as_str()) {
                return Err(ClientError::InvalidWorkflow("task IDs must be unique"));
            }
            if let Some(encoded) = &task.input_base64 {
                // A valid standard-base64 string for a 1 MiB payload cannot
                // exceed this length. Reject before allocating decoded bytes.
                if encoded.len() > MAX_BASE64_TASK_PAYLOAD_BYTES {
                    return Err(ClientError::TaskPayloadTooLarge);
                }
                let decoded = BASE64.decode(encoded).map_err(|_| {
                    ClientError::InvalidWorkflow("input_base64 must use standard base64")
                })?;
                if decoded.len() > MAX_TASK_PAYLOAD_BYTES {
                    return Err(ClientError::TaskPayloadTooLarge);
                }
            }
        }

        let mut indegree: HashMap<&str, usize> = self
            .tasks
            .iter()
            .map(|task| (task.task_id.as_str(), 0))
            .collect();
        let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
        for task in &self.tasks {
            let mut unique_dependencies = HashSet::with_capacity(task.depends_on.len());
            for dependency in &task.depends_on {
                validate_entity_id(dependency)?;
                if !unique_dependencies.insert(dependency.as_str()) {
                    return Err(ClientError::InvalidWorkflow(
                        "dependency IDs must be unique within each task",
                    ));
                }
                if dependency == &task.task_id {
                    return Err(ClientError::InvalidWorkflow(
                        "a task cannot depend on itself",
                    ));
                }
                if !task_ids.contains(dependency.as_str()) {
                    return Err(ClientError::InvalidWorkflow(
                        "every dependency must reference a task in this workflow",
                    ));
                }
                *indegree
                    .get_mut(task.task_id.as_str())
                    .expect("validated task ID is present") += 1;
                dependents
                    .entry(dependency.as_str())
                    .or_default()
                    .push(task.task_id.as_str());
            }
        }

        let mut ready = indegree
            .iter()
            .filter_map(|(task_id, degree)| (*degree == 0).then_some(*task_id))
            .collect::<VecDeque<_>>();
        let mut visited = 0usize;
        while let Some(task_id) = ready.pop_front() {
            visited += 1;
            if let Some(children) = dependents.get(task_id) {
                for child in children {
                    let degree = indegree
                        .get_mut(*child)
                        .expect("validated dependent task is present");
                    *degree -= 1;
                    if *degree == 0 {
                        ready.push_back(child);
                    }
                }
            }
        }
        if visited != self.tasks.len() {
            return Err(ClientError::InvalidWorkflow(
                "task dependencies must form an acyclic directed graph",
            ));
        }
        Ok(())
    }
}

/// Task-run identity and status returned in workflow receipts and snapshots.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskRunReceipt {
    /// Task identity.
    pub task_id: String,
    /// Durable task-run identity.
    pub task_run_id: String,
    /// Initial or current task status.
    pub status: TaskStatus,
}

/// Receipt returned after a workflow run is durably created.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowRunReceipt {
    /// Workflow identity.
    pub workflow_id: String,
    /// Durable workflow-run identity.
    pub workflow_run_id: String,
    /// Newly submitted workflow status.
    pub status: WorkflowStatus,
    /// Optimistic concurrency version.
    pub version: u64,
    /// Task-run receipt list.
    pub task_runs: Vec<TaskRunReceipt>,
}

/// Durable workflow-run snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowRunSnapshot {
    /// Workflow identity.
    pub workflow_id: String,
    /// Durable workflow-run identity.
    pub workflow_run_id: String,
    /// Current workflow lifecycle status.
    pub status: WorkflowStatus,
    /// Optimistic concurrency version.
    pub version: u64,
    /// Task-run summaries.
    pub task_runs: Vec<TaskRunReceipt>,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// RFC 3339 update timestamp.
    pub updated_at: String,
}

/// Durable task-run snapshot. Payload bytes are intentionally omitted by v1.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskRunSnapshot {
    /// Durable task-run identity.
    pub task_run_id: String,
    /// Parent workflow-run identity.
    pub workflow_run_id: String,
    /// Task definition identity.
    pub task_id: String,
    /// Current task lifecycle status.
    pub status: TaskStatus,
    /// Optimistic concurrency version.
    pub version: u64,
    /// Number of attempts started.
    pub attempts_started: u32,
    /// RFC 3339 time at which the task may next be attempted.
    pub available_at: String,
    /// Decoded input size; input bytes are never returned.
    pub input_size_bytes: u64,
    /// Decoded output size, if output exists.
    pub output_size_bytes: Option<u64>,
    /// Whether output bytes exist.
    pub output_present: bool,
    /// Task-run dependencies.
    pub dependency_task_run_ids: Vec<String>,
    /// Worker currently holding the claim, when present.
    pub claimed_by: Option<String>,
    /// RFC 3339 worker lease expiry, when claimed.
    pub lease_expires_at: Option<String>,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// RFC 3339 update timestamp.
    pub updated_at: String,
}

/// Result of version-guarded task cancellation and parent aggregation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CancelTaskRunResponse {
    /// Updated task snapshot.
    pub task_run: TaskRunSnapshot,
    /// Aggregated parent workflow status.
    pub workflow_status: WorkflowStatus,
}

/// One active worker registration. Credentials are never exposed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerSnapshot {
    /// Worker identity.
    pub worker_id: String,
    /// Node identity.
    pub node_id: String,
    /// Node incarnation identity.
    pub node_incarnation_id: String,
    /// Monotonically increasing membership epoch.
    pub membership_epoch: u64,
    /// Membership status.
    pub status: String,
    /// Maximum concurrent tasks registered for this worker.
    pub concurrency_limit: u32,
    /// RFC 3339 lease expiry timestamp.
    pub lease_expires_at: String,
}

/// Response returned by the operator-only worker inventory endpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerListResponse {
    /// Currently active worker registrations.
    pub workers: Vec<WorkerSnapshot>,
}

/// API error details returned by the control plane.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ApiErrorDetails {
    /// Stable machine-readable error code.
    pub code: String,
    /// Human-readable error message.
    pub message: String,
    /// Request identifier, when supplied by the server.
    #[serde(default)]
    pub request_id: Option<String>,
}

/// Standard v1 error response envelope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ErrorResponse {
    /// Error details.
    pub error: ApiErrorDetails,
}

#[derive(Clone)]
enum Credential {
    Anonymous,
    Operator(String),
    Tenant(String),
}

impl Credential {
    fn token(&self) -> Option<&str> {
        match self {
            Self::Anonymous => None,
            Self::Operator(token) | Self::Tenant(token) => Some(token),
        }
    }

    fn is_tenant(&self) -> bool {
        matches!(self, Self::Tenant(_))
    }

    fn is_anonymous(&self) -> bool {
        matches!(self, Self::Anonymous)
    }

    fn scope(&self) -> &'static str {
        match self {
            Self::Anonymous => "anonymous",
            Self::Operator(_) => "operator",
            Self::Tenant(_) => "tenant",
        }
    }
}

/// Async HTTP client for the AetherFlow v1 control-plane API.
///
/// Use an anonymous client for public health checks, an operator client for
/// platform routes, or a tenant client for tenant-owned workflow/task routes.
/// Credentials are excluded from Debug output.
#[derive(Clone)]
pub struct ControlPlaneClient {
    http: Client,
    base_url: Url,
    credential: Credential,
}

impl fmt::Debug for ControlPlaneClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneClient")
            .field("base_url", &self.base_url)
            .field("credential_scope", &self.credential.scope())
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

impl ControlPlaneClient {
    /// Create a client that can call only the public health endpoint.
    pub fn anonymous(base_url: &str) -> Result<Self, ClientError> {
        Self::new(base_url, Credential::Anonymous)
    }

    /// Create a client for platform/operator routes.
    pub fn operator(base_url: &str, token: impl Into<String>) -> Result<Self, ClientError> {
        Self::new(base_url, Credential::Operator(token.into()))
    }

    /// Create a client for tenant-authenticated workflow/task routes.
    pub fn tenant(base_url: &str, token: impl Into<String>) -> Result<Self, ClientError> {
        Self::new(base_url, Credential::Tenant(token.into()))
    }

    fn new(base_url: &str, credential: Credential) -> Result<Self, ClientError> {
        if credential
            .token()
            .is_some_and(|token| token.trim().is_empty())
        {
            return Err(ClientError::EmptyCredential);
        }
        let base_url = normalize_base_url(base_url)?;
        let http = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(ClientError::Transport)?;
        Ok(Self {
            http,
            base_url,
            credential,
        })
    }

    fn endpoint(&self, route: &str) -> Result<Url, ClientError> {
        let prefix = if self.credential.is_tenant() {
            "api/v1/tenant/"
        } else {
            "api/v1/"
        };
        self.base_url
            .join(&format!("{prefix}{route}"))
            .map_err(|_| ClientError::InvalidBaseUrl)
    }

    async fn request_json<T: DeserializeOwned>(
        &self,
        method: Method,
        route: &str,
        body: Option<Vec<u8>>,
        authenticated: bool,
    ) -> Result<T, ClientError> {
        if authenticated && self.credential.is_anonymous() {
            return Err(ClientError::AuthenticationRequired);
        }

        let url = if route == "health" {
            self.base_url
                .join("health")
                .map_err(|_| ClientError::InvalidBaseUrl)?
        } else {
            self.endpoint(route)?
        };
        let mut request = self.http.request(method, url);
        if authenticated {
            let token = self
                .credential
                .token()
                .ok_or(ClientError::AuthenticationRequired)?;
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body);
        }

        let mut response = request.send().await.map_err(ClientError::Transport)?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_HTTP_BODY_BYTES as u64)
        {
            return Err(ClientError::ResponseTooLarge);
        }

        // Bound streaming responses too: Content-Length may be absent or false.
        let mut response_body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(ClientError::Transport)? {
            if response_body.len().saturating_add(chunk.len()) > MAX_HTTP_BODY_BYTES {
                return Err(ClientError::ResponseTooLarge);
            }
            response_body.extend_from_slice(&chunk);
        }

        let status = response.status();
        if !status.is_success() {
            let details = serde_json::from_slice::<ErrorResponse>(&response_body)
                .ok()
                .map(|response| response.error);
            return Err(ClientError::Api {
                status: status.as_u16(),
                code: details.as_ref().map(|details| details.code.clone()),
                message: details.as_ref().map(|details| details.message.clone()),
                request_id: details.and_then(|details| details.request_id),
            });
        }
        serde_json::from_slice(&response_body).map_err(ClientError::Json)
    }

    /// Check process liveness. This public endpoint does not verify PostgreSQL readiness.
    pub async fn health(&self) -> Result<HealthResponse, ClientError> {
        self.request_json(Method::GET, "health", None, false).await
    }

    /// List active workers. This endpoint requires an operator-scoped client.
    pub async fn list_active_workers(&self) -> Result<WorkerListResponse, ClientError> {
        if self.credential.is_tenant() {
            return Err(ClientError::OperatorOnly);
        }
        self.request_json(Method::GET, "workers", None, true).await
    }

    /// Submit a workflow run using the operator or tenant route selected at construction.
    pub async fn submit_workflow_run(
        &self,
        submission: &WorkflowRunSubmission,
    ) -> Result<WorkflowRunReceipt, ClientError> {
        submission.validate()?;
        let body = encode_request_body(submission)?;
        self.request_json(Method::POST, "workflow-runs", Some(body), true)
            .await
    }

    /// Read a workflow run from the selected operator or tenant scope.
    pub async fn get_workflow_run(
        &self,
        workflow_run_id: &str,
    ) -> Result<WorkflowRunSnapshot, ClientError> {
        validate_entity_id(workflow_run_id)?;
        self.request_json(
            Method::GET,
            &format!("workflow-runs/{workflow_run_id}"),
            None,
            true,
        )
        .await
    }

    /// Read a task run from the selected operator or tenant scope.
    pub async fn get_task_run(&self, task_run_id: &str) -> Result<TaskRunSnapshot, ClientError> {
        validate_entity_id(task_run_id)?;
        self.request_json(Method::GET, &format!("task-runs/{task_run_id}"), None, true)
            .await
    }

    /// Cancel a task run using its latest TaskRunSnapshot version.
    pub async fn cancel_task_run(
        &self,
        task_run_id: &str,
        expected_version: u64,
    ) -> Result<CancelTaskRunResponse, ClientError> {
        validate_entity_id(task_run_id)?;
        if expected_version > i64::MAX as u64 {
            return Err(ClientError::InvalidWorkflow(
                "expected_version must fit in a signed 64-bit integer",
            ));
        }
        let body = encode_request_body(&CancelTaskRunRequest { expected_version })?;
        self.request_json(
            Method::POST,
            &format!("task-runs/{task_run_id}/cancel"),
            Some(body),
            true,
        )
        .await
    }
}

#[derive(Serialize)]
struct CancelTaskRunRequest {
    expected_version: u64,
}

#[derive(Default)]
struct BoundedRequestWriter {
    bytes: Vec<u8>,
    exceeded: bool,
}

impl Write for BoundedRequestWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let remaining = MAX_HTTP_BODY_BYTES.saturating_sub(self.bytes.len());
        if buffer.len() > remaining {
            self.exceeded = true;
            return Err(io::Error::other("HTTP request body limit exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_request_body<T: Serialize>(value: &T) -> Result<Vec<u8>, ClientError> {
    let mut writer = BoundedRequestWriter::default();
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        if writer.exceeded {
            return Err(ClientError::RequestTooLarge);
        }
        return Err(ClientError::Json(error));
    }
    Ok(writer.bytes)
}

fn normalize_base_url(raw: &str) -> Result<Url, ClientError> {
    let mut value = raw.trim().to_owned();
    if value.is_empty() {
        return Err(ClientError::InvalidBaseUrl);
    }
    if !value.ends_with('/') {
        value.push('/');
    }
    let url = Url::parse(&value).map_err(|_| ClientError::InvalidBaseUrl)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ClientError::InvalidBaseUrl);
    }

    let host = url
        .host_str()
        .unwrap_or_default()
        .trim_start_matches('[')
        .trim_end_matches(']');
    let is_loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if url.scheme() == "http" && !is_loopback {
        return Err(ClientError::InsecureRemoteHttp);
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::{
        BoundedRequestWriter, ClientError, ControlPlaneClient, MAX_BASE64_TASK_PAYLOAD_BYTES,
        RetryPolicy, WorkflowRunSubmission, WorkflowTaskInput, encode_request_body,
        normalize_base_url, validate_entity_id,
    };

    #[test]
    fn base_url_preserves_reverse_proxy_prefix() {
        let url = normalize_base_url("https://example.test/control").unwrap();
        assert_eq!(url.as_str(), "https://example.test/control/");
    }

    #[test]
    fn base_url_allows_plain_http_only_on_loopback() {
        for value in [
            "http://localhost:8080",
            "http://127.0.0.1:8080",
            "http://127.32.5.9:8080",
            "http://[::1]:8080",
        ] {
            assert!(normalize_base_url(value).is_ok(), "{value}");
        }
        assert!(normalize_base_url("https://control.example.test").is_ok());
        assert!(matches!(
            normalize_base_url("http://control.example.test"),
            Err(ClientError::InsecureRemoteHttp)
        ));
        assert!(matches!(
            normalize_base_url("http://192.168.1.25:8080"),
            Err(ClientError::InsecureRemoteHttp)
        ));
    }

    #[test]
    fn base_url_rejects_credentials_query_and_fragment() {
        assert!(normalize_base_url("https://user:secret@example.test").is_err());
        assert!(normalize_base_url("https://example.test?token=secret").is_err());
        assert!(normalize_base_url("https://example.test/#fragment").is_err());
        assert!(normalize_base_url("file:///tmp/control-plane").is_err());
    }

    #[test]
    fn entity_ids_follow_the_server_contract() {
        assert!(validate_entity_id("run_01-abc").is_ok());
        assert!(validate_entity_id("").is_err());
        assert!(validate_entity_id("../admin").is_err());
        assert!(validate_entity_id("a".repeat(129).as_str()).is_err());
        assert!(validate_entity_id("سلام").is_err());
    }

    #[test]
    fn workflow_validation_rejects_duplicate_and_missing_dependencies() {
        let retry = RetryPolicy::new(2, 0, 1_000).unwrap();
        let duplicate = WorkflowRunSubmission::new(
            "workflow",
            "run-1",
            vec![
                WorkflowTaskInput::new("task-a"),
                WorkflowTaskInput::new("task-a"),
            ],
            retry.clone(),
        );
        assert!(duplicate.validate().is_err());

        let missing = WorkflowRunSubmission::new(
            "workflow",
            "run-2",
            vec![WorkflowTaskInput::new("task-a").with_dependencies(vec!["unknown".to_owned()])],
            retry,
        );
        assert!(missing.validate().is_err());
    }

    #[test]
    fn workflow_validation_rejects_dependency_cycles() {
        let submission = WorkflowRunSubmission::new(
            "workflow",
            "run-1",
            vec![
                WorkflowTaskInput::new("task-a").with_dependencies(vec!["task-b".to_owned()]),
                WorkflowTaskInput::new("task-b").with_dependencies(vec!["task-a".to_owned()]),
            ],
            RetryPolicy::new(1, 0, 0).unwrap(),
        );
        assert!(matches!(
            submission.validate(),
            Err(ClientError::InvalidWorkflow(_))
        ));
    }

    #[test]
    fn workflow_validation_rejects_bad_base64_and_oversized_task_input() {
        let invalid = WorkflowRunSubmission::new(
            "workflow",
            "run-1",
            vec![WorkflowTaskInput {
                task_id: "task-a".to_owned(),
                depends_on: Vec::new(),
                input_base64: Some("not valid base64!".to_owned()),
            }],
            RetryPolicy::new(1, 0, 0).unwrap(),
        );
        assert!(invalid.validate().is_err());
        assert!(
            WorkflowTaskInput::new("task-a")
                .with_input_bytes(&vec![b'x'; super::MAX_TASK_PAYLOAD_BYTES + 1])
                .is_err()
        );
    }

    #[test]
    fn oversized_base64_is_rejected_before_decode() {
        let encoded = "A".repeat(MAX_BASE64_TASK_PAYLOAD_BYTES + 1);
        let submission = WorkflowRunSubmission::new(
            "workflow",
            "run-1",
            vec![WorkflowTaskInput {
                task_id: "task-a".to_owned(),
                depends_on: Vec::new(),
                input_base64: Some(encoded),
            }],
            RetryPolicy::new(1, 0, 0).unwrap(),
        );
        assert!(matches!(
            submission.validate(),
            Err(ClientError::TaskPayloadTooLarge)
        ));
    }

    #[test]
    fn bounded_request_writer_never_exceeds_the_body_limit() {
        let mut writer = BoundedRequestWriter::default();
        let prefix = vec![b'x'; super::MAX_HTTP_BODY_BYTES - 5];
        std::io::Write::write_all(&mut writer, &prefix).unwrap();
        let overflow = [b'y'; 10];
        assert!(std::io::Write::write_all(&mut writer, &overflow).is_err());
        assert!(writer.exceeded);
        assert_eq!(writer.bytes.len(), super::MAX_HTTP_BODY_BYTES - 5);
    }

    #[test]
    fn serialized_workflow_body_is_bounded() {
        let payload = vec![b'x'; 600_000];
        let submission = WorkflowRunSubmission::new(
            "workflow",
            "run-1",
            vec![
                WorkflowTaskInput::new("task-a")
                    .with_input_bytes(&payload)
                    .unwrap(),
                WorkflowTaskInput::new("task-b")
                    .with_input_bytes(&payload)
                    .unwrap(),
                WorkflowTaskInput::new("task-c")
                    .with_input_bytes(&payload)
                    .unwrap(),
            ],
            RetryPolicy::new(1, 0, 0).unwrap(),
        );
        assert!(submission.validate().is_ok());
        assert!(matches!(
            encode_request_body(&submission),
            Err(ClientError::RequestTooLarge)
        ));
    }

    #[test]
    fn client_debug_redacts_credentials_and_separates_route_scopes() {
        let secret = "example-operator-token-never-print";
        let operator = ControlPlaneClient::operator("https://example.test", secret).unwrap();
        let tenant = ControlPlaneClient::tenant("https://example.test", "tenant-token").unwrap();
        let anonymous = ControlPlaneClient::anonymous("https://example.test").unwrap();
        let debug = format!("{operator:?}");
        assert!(!debug.contains(secret));
        assert!(debug.contains("[REDACTED]"));
        assert_eq!(
            operator.endpoint("workflow-runs").unwrap().as_str(),
            "https://example.test/api/v1/workflow-runs"
        );
        assert_eq!(
            tenant.endpoint("workflow-runs").unwrap().as_str(),
            "https://example.test/api/v1/tenant/workflow-runs"
        );
        assert!(anonymous.credential.token().is_none());
        assert!(operator.credential.token().is_some());
        assert!(tenant.credential.token().is_some());
    }

    #[test]
    fn clients_reject_empty_credentials() {
        assert!(matches!(
            ControlPlaneClient::operator("https://example.test", " "),
            Err(ClientError::EmptyCredential)
        ));
        assert!(matches!(
            ControlPlaneClient::tenant("https://example.test", ""),
            Err(ClientError::EmptyCredential)
        ));
        assert!(ControlPlaneClient::anonymous("https://example.test").is_ok());
    }
}
