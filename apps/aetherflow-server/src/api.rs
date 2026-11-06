//! Versioned HTTP control plane for durable workflow execution.
//!
//! The v1 service separates operator and tenant-authenticated control-plane
//! routes, with tenant identity enforced by storage authorization and RLS.
//! Payload bytes are accepted on submission but never included in inspection.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Extension, Path, Query, Request, State};
use axum::http::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_MAX_AGE, ACCESS_CONTROL_REQUEST_HEADERS,
    ACCESS_CONTROL_REQUEST_METHOD, AUTHORIZATION, CONTENT_TYPE, ORIGIN, RETRY_AFTER, VARY,
    WWW_AUTHENTICATE,
};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD as BASE64_URL};
use chrono::{DateTime, SecondsFormat, Utc};
use deadpool_postgres::Pool;
use opentelemetry::trace::TraceContextExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::trace_context::{extract_trace_context, inject_trace_context};

const HTTP_LATENCY_BUCKETS: [(u64, &str); 10] = [
    (5_000, "0.005"),
    (10_000, "0.01"),
    (25_000, "0.025"),
    (50_000, "0.05"),
    (100_000, "0.1"),
    (250_000, "0.25"),
    (500_000, "0.5"),
    (1_000_000, "1"),
    (2_500_000, "2.5"),
    (5_000_000, "5"),
];

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

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
    cors_allowed_origins: Arc<HashSet<String>>,
    principal_rate_limiter: Arc<PrincipalRateLimiter>,
    metrics: Arc<HttpMetrics>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
enum HttpOperation {
    Health = 0,
    Metrics,
    OperatorRead,
    OperatorSubmit,
    OperatorMutation,
    TenantRead,
    TenantSubmit,
    TenantMutation,
    Other,
}

impl HttpOperation {
    const COUNT: usize = 9;
    const ALL: [Self; Self::COUNT] = [
        Self::Health,
        Self::Metrics,
        Self::OperatorRead,
        Self::OperatorSubmit,
        Self::OperatorMutation,
        Self::TenantRead,
        Self::TenantSubmit,
        Self::TenantMutation,
        Self::Other,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::Metrics => "metrics",
            Self::OperatorRead => "operator_read",
            Self::OperatorSubmit => "operator_submit",
            Self::OperatorMutation => "operator_mutation",
            Self::TenantRead => "tenant_read",
            Self::TenantSubmit => "tenant_submit",
            Self::TenantMutation => "tenant_mutation",
            Self::Other => "other",
        }
    }

    fn classify(method: &Method, path: &str) -> Self {
        let is_read = method == &Method::GET || method == &Method::HEAD;
        if path == "/health" && is_read {
            return Self::Health;
        }
        if path == "/api/v1/metrics" && is_read {
            return Self::Metrics;
        }

        let operator_resource = path == "/api/v1/workflow-runs"
            || path.starts_with("/api/v1/workflow-runs/")
            || path.starts_with("/api/v1/task-runs/");
        if operator_resource {
            if is_read {
                return Self::OperatorRead;
            }
            if method == &Method::POST {
                return if path == "/api/v1/workflow-runs" {
                    Self::OperatorSubmit
                } else {
                    Self::OperatorMutation
                };
            }
            return Self::Other;
        }
        if path == "/api/v1/workers" && is_read {
            return Self::OperatorRead;
        }

        let tenant_resource = path == "/api/v1/tenant/workflow-runs"
            || path.starts_with("/api/v1/tenant/workflow-runs/")
            || path.starts_with("/api/v1/tenant/task-runs/");
        if tenant_resource {
            if is_read {
                return Self::TenantRead;
            }
            if method == &Method::POST {
                return if path == "/api/v1/tenant/workflow-runs" {
                    Self::TenantSubmit
                } else {
                    Self::TenantMutation
                };
            }
        }

        Self::Other
    }
}

#[derive(Default)]
struct HttpOperationMetrics {
    requests_total: AtomicU64,
    requests_failed_total: AtomicU64,
    request_duration_sum_micros: AtomicU64,
    request_duration_buckets: [AtomicU64; 10],
}

impl HttpOperationMetrics {
    fn observe(&self, status_code: u16, elapsed: Duration) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        if status_code >= 500 {
            self.requests_failed_total.fetch_add(1, Ordering::Relaxed);
        }

        let elapsed_micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let _ = self.request_duration_sum_micros.try_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_add(elapsed_micros)),
        );
        for ((upper_bound, _), bucket) in HTTP_LATENCY_BUCKETS
            .iter()
            .zip(self.request_duration_buckets.iter())
        {
            if elapsed_micros <= *upper_bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Default)]
struct HttpMetrics {
    requests_total: AtomicU64,
    requests_failed_total: AtomicU64,
    requests_rate_limited_total: AtomicU64,
    requests_in_flight: AtomicU64,
    request_duration_sum_micros: AtomicU64,
    request_duration_buckets: [AtomicU64; 10],
    operations: [HttpOperationMetrics; HttpOperation::COUNT],
}

impl HttpMetrics {
    fn observe(&self, operation: HttpOperation, status_code: u16, elapsed: Duration) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        if status_code >= 500 {
            self.requests_failed_total.fetch_add(1, Ordering::Relaxed);
        }
        if status_code == StatusCode::TOO_MANY_REQUESTS.as_u16() {
            self.requests_rate_limited_total
                .fetch_add(1, Ordering::Relaxed);
        }

        let elapsed_micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let _ = self.request_duration_sum_micros.try_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_add(elapsed_micros)),
        );
        for ((upper_bound, _), bucket) in HTTP_LATENCY_BUCKETS
            .iter()
            .zip(self.request_duration_buckets.iter())
        {
            if elapsed_micros <= *upper_bound {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.operations[operation as usize].observe(status_code, elapsed);
    }

    fn render_prometheus(&self) -> String {
        let requests_total = self.requests_total.load(Ordering::Relaxed);
        let duration_sum =
            Duration::from_micros(self.request_duration_sum_micros.load(Ordering::Relaxed))
                .as_secs_f64();
        let mut output = String::new();

        writeln!(
            output,
            "# HELP aetherflow_http_requests_total Completed HTTP requests."
        )
        .expect("write metrics");
        writeln!(output, "# TYPE aetherflow_http_requests_total counter").expect("write metrics");
        writeln!(output, "aetherflow_http_requests_total {requests_total}").expect("write metrics");
        writeln!(output, "# HELP aetherflow_http_requests_failed_total HTTP requests completed with a 5xx response.").expect("write metrics");
        writeln!(
            output,
            "# TYPE aetherflow_http_requests_failed_total counter"
        )
        .expect("write metrics");
        writeln!(
            output,
            "aetherflow_http_requests_failed_total {}",
            self.requests_failed_total.load(Ordering::Relaxed)
        )
        .expect("write metrics");
        writeln!(output, "# HELP aetherflow_http_requests_rate_limited_total HTTP requests rejected by the authenticated-principal rate limiter.").expect("write metrics");
        writeln!(
            output,
            "# TYPE aetherflow_http_requests_rate_limited_total counter"
        )
        .expect("write metrics");
        writeln!(
            output,
            "aetherflow_http_requests_rate_limited_total {}",
            self.requests_rate_limited_total.load(Ordering::Relaxed)
        )
        .expect("write metrics");
        writeln!(
            output,
            "# HELP aetherflow_http_requests_in_flight HTTP requests currently being handled."
        )
        .expect("write metrics");
        writeln!(output, "# TYPE aetherflow_http_requests_in_flight gauge").expect("write metrics");
        writeln!(
            output,
            "aetherflow_http_requests_in_flight {}",
            self.requests_in_flight.load(Ordering::Relaxed)
        )
        .expect("write metrics");

        writeln!(
            output,
            "# HELP aetherflow_http_request_duration_seconds HTTP request latency distribution."
        )
        .expect("write metrics");
        writeln!(
            output,
            "# TYPE aetherflow_http_request_duration_seconds histogram"
        )
        .expect("write metrics");
        for ((_, bound), bucket) in HTTP_LATENCY_BUCKETS
            .iter()
            .zip(self.request_duration_buckets.iter())
        {
            writeln!(
                output,
                "aetherflow_http_request_duration_seconds_bucket{{le=\"{bound}\"}} {}",
                bucket.load(Ordering::Relaxed)
            )
            .expect("write metrics");
        }
        writeln!(
            output,
            "aetherflow_http_request_duration_seconds_bucket{{le=\"+Inf\"}} {requests_total}"
        )
        .expect("write metrics");
        writeln!(
            output,
            "aetherflow_http_request_duration_seconds_sum {duration_sum:.6}"
        )
        .expect("write metrics");
        writeln!(
            output,
            "aetherflow_http_request_duration_seconds_count {requests_total}"
        )
        .expect("write metrics");

        // Labels are selected from a fixed enum. Never add raw paths, tenant
        // IDs, workflow IDs, query strings, or caller-controlled values here.
        writeln!(output, "# HELP aetherflow_http_operation_requests_total Completed requests by bounded operation class.").expect("write metrics");
        writeln!(
            output,
            "# TYPE aetherflow_http_operation_requests_total counter"
        )
        .expect("write metrics");
        writeln!(output, "# HELP aetherflow_http_operation_requests_failed_total HTTP requests by operation class completed with a 5xx response.").expect("write metrics");
        writeln!(
            output,
            "# TYPE aetherflow_http_operation_requests_failed_total counter"
        )
        .expect("write metrics");
        for operation in HttpOperation::ALL {
            let metrics = &self.operations[operation as usize];
            writeln!(
                output,
                "aetherflow_http_operation_requests_total{{operation=\"{}\"}} {}",
                operation.label(),
                metrics.requests_total.load(Ordering::Relaxed)
            )
            .expect("write metrics");
            writeln!(
                output,
                "aetherflow_http_operation_requests_failed_total{{operation=\"{}\"}} {}",
                operation.label(),
                metrics.requests_failed_total.load(Ordering::Relaxed)
            )
            .expect("write metrics");
        }

        writeln!(output, "# HELP aetherflow_http_operation_request_duration_seconds HTTP request latency by bounded operation class.").expect("write metrics");
        writeln!(
            output,
            "# TYPE aetherflow_http_operation_request_duration_seconds histogram"
        )
        .expect("write metrics");
        for operation in HttpOperation::ALL {
            let metrics = &self.operations[operation as usize];
            let count = metrics.requests_total.load(Ordering::Relaxed);
            let sum =
                Duration::from_micros(metrics.request_duration_sum_micros.load(Ordering::Relaxed))
                    .as_secs_f64();
            for ((_, bound), bucket) in HTTP_LATENCY_BUCKETS
                .iter()
                .zip(metrics.request_duration_buckets.iter())
            {
                writeln!(output, "aetherflow_http_operation_request_duration_seconds_bucket{{operation=\"{}\",le=\"{bound}\"}} {}", operation.label(), bucket.load(Ordering::Relaxed)).expect("write metrics");
            }
            writeln!(output, "aetherflow_http_operation_request_duration_seconds_bucket{{operation=\"{}\",le=\"+Inf\"}} {count}", operation.label()).expect("write metrics");
            writeln!(output, "aetherflow_http_operation_request_duration_seconds_sum{{operation=\"{}\"}} {sum:.6}", operation.label()).expect("write metrics");
            writeln!(output, "aetherflow_http_operation_request_duration_seconds_count{{operation=\"{}\"}} {count}", operation.label()).expect("write metrics");
        }
        output
    }
}
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum PrincipalRateLimitKey {
    Operator,
    /// Shared fallback for authenticated identities beyond the per-process cap.
    Overflow,
    Tenant {
        tenant_id: String,
        principal_id: String,
    },
}

struct TokenBucket {
    tokens: f64,
    updated_at: Instant,
    last_seen: Instant,
}

struct PrincipalRateLimiterState {
    buckets: HashMap<PrincipalRateLimitKey, TokenBucket>,
    last_cleanup: Instant,
}

/// Bounded token buckets keyed only by identities already authenticated by the
/// control plane. This avoids trusting forwarded-IP headers and prevents an
/// arbitrary Authorization value from allocating a bucket.
struct PrincipalRateLimiter {
    burst_capacity: f64,
    refill_per_second: f64,
    buckets: Mutex<PrincipalRateLimiterState>,
}

impl Default for PrincipalRateLimiter {
    fn default() -> Self {
        Self::new(30, 2).expect("the built-in API rate-limit defaults are valid")
    }
}

impl PrincipalRateLimiter {
    const MAX_BUCKETS: usize = 4_096;
    const IDLE_TTL: Duration = Duration::from_secs(10 * 60);
    const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

    fn new(burst_capacity: u32, refill_per_second: u32) -> Result<Self, std::io::Error> {
        if burst_capacity == 0 || refill_per_second == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "API rate-limit burst and refill rate must both be positive integers",
            ));
        }

        Ok(Self {
            burst_capacity: f64::from(burst_capacity),
            refill_per_second: f64::from(refill_per_second),
            buckets: Mutex::new(PrincipalRateLimiterState {
                buckets: HashMap::new(),
                last_cleanup: Instant::now(),
            }),
        })
    }

    fn check(&self, key: PrincipalRateLimitKey) -> Option<Duration> {
        self.check_at(key, Instant::now())
    }

    fn check_at(&self, key: PrincipalRateLimitKey, now: Instant) -> Option<Duration> {
        let mut state = self
            .buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if now.saturating_duration_since(state.last_cleanup) >= Self::CLEANUP_INTERVAL {
            state.buckets.retain(|_, bucket| {
                now.saturating_duration_since(bucket.last_seen) < Self::IDLE_TTL
            });
            state.last_cleanup = now;
        }

        // Reserve one slot for a shared overflow bucket. Never evict an
        // established principal bucket: doing so would reset that principal's
        // burst allowance when it returns. New authenticated identities beyond
        // the cap share one conservative bucket instead.
        let key = if !state.buckets.contains_key(&key)
            && !matches!(&key, PrincipalRateLimitKey::Overflow)
            && state.buckets.len() >= Self::MAX_BUCKETS - 1
        {
            PrincipalRateLimitKey::Overflow
        } else {
            key
        };

        let bucket = state.buckets.entry(key).or_insert_with(|| TokenBucket {
            tokens: self.burst_capacity,
            updated_at: now,
            last_seen: now,
        });
        let elapsed = now
            .saturating_duration_since(bucket.updated_at)
            .as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.refill_per_second).min(self.burst_capacity);
        bucket.updated_at = now;
        bucket.last_seen = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            None
        } else {
            let wait_seconds = (1.0 - bucket.tokens) / self.refill_per_second;
            Some(Duration::from_secs_f64(wait_seconds.max(0.001)))
        }
    }

    #[cfg(test)]
    fn bucket_count(&self) -> usize {
        self.buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .buckets
            .len()
    }
}

fn rate_limited_response(retry_after: Duration) -> Response {
    let mut response = ApiError::new(
        StatusCode::TOO_MANY_REQUESTS,
        "rate_limited",
        "the authenticated principal has exceeded the API request rate",
    )
    .into_response();
    let retry_after_seconds = retry_after
        .as_secs()
        .saturating_add(u64::from(retry_after.subsec_nanos() > 0))
        .max(1);
    if let Ok(value) = HeaderValue::from_str(&retry_after_seconds.to_string()) {
        response.headers_mut().insert(RETRY_AFTER, value);
    }
    response
}

struct AppDatabase {
    coordinator: PgWorkflowCoordinator,
    tasks: Arc<PgTaskRepository>,
    membership: PgMembershipRepository,
    tenant_credentials: PgTenantCredentialRepository,
}

impl AppState {
    /// Configures exact browser origins allowed to call the control-plane API.
    ///
    /// The value is a comma-separated list of bare HTTP(S) origins. Wildcards,
    /// paths, query strings, credentials, and non-HTTP schemes are rejected.
    /// An empty value disables cross-origin browser access.
    pub fn with_cors_allowed_origins(
        mut self,
        comma_separated_origins: &str,
    ) -> Result<Self, std::io::Error> {
        let mut allowed = HashSet::new();
        for origin in comma_separated_origins
            .split(',')
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            if !valid_cors_origin(origin) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "AETHERFLOW_CORS_ALLOWED_ORIGINS must contain only exact HTTP(S) origins without paths or wildcards",
                ));
            }
            allowed.insert(origin.to_owned());
        }
        self.cors_allowed_origins = Arc::new(allowed);
        Ok(self)
    }

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
            cors_allowed_origins: Arc::default(),
            principal_rate_limiter: Arc::default(),
            metrics: Arc::default(),
        })
    }

    /// Replaces the per-authenticated-principal token-bucket limits.
    ///
    /// The burst is the maximum immediate request allowance; the refill rate
    /// is applied continuously in requests per second. Each tenant principal
    /// and the platform operator share separate bounded buckets.
    pub fn with_api_rate_limit(
        mut self,
        burst_capacity: u32,
        refill_per_second: u32,
    ) -> Result<Self, std::io::Error> {
        self.principal_rate_limiter = Arc::new(PrincipalRateLimiter::new(
            burst_capacity,
            refill_per_second,
        )?);
        Ok(self)
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
        .route("/api/v1/metrics", get(export_metrics))
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
        .layer(middleware::from_fn_with_state(
            state.clone(),
            apply_browser_cors,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            observe_http_request,
        ))
        .with_state(state)
}

async fn observe_http_request(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let method = match request.method().as_str() {
        "GET" => "GET",
        "POST" => "POST",
        "PUT" => "PUT",
        "PATCH" => "PATCH",
        "DELETE" => "DELETE",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        _ => "OTHER",
    };
    let operation = HttpOperation::classify(request.method(), request.uri().path());
    let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    state
        .metrics
        .requests_in_flight
        .fetch_add(1, Ordering::Relaxed);

    // Deliberately omit URI, query, headers, and body: these may contain
    // resource identifiers, credentials, or customer payloads.
    let span = tracing::info_span!(
        "http.request",
        request_id,
        http_method = method,
        operation = operation.label(),
        otel_trace_id = tracing::field::Empty,
        auth_scope = tracing::field::Empty,
        auth_result = tracing::field::Empty,
        auth_actor_type = tracing::field::Empty,
        auth_actor_id = tracing::field::Empty,
        auth_tenant_id = tracing::field::Empty,
        auth_principal_id = tracing::field::Empty,
        auth_throttled = tracing::field::Empty
    );
    if let Err(error) = span.set_parent(extract_trace_context(request.headers())) {
        tracing::warn!(%error, "unable to attach incoming W3C trace context");
    }
    let trace_id = span.context().span().span_context().trace_id().to_string();
    span.record("otel_trace_id", tracing::field::display(trace_id));
    let mut response = next.run(request).instrument(span.clone()).await;
    inject_trace_context(&span.context(), response.headers_mut());
    let elapsed = started.elapsed();
    let status_code = response.status().as_u16();

    state.metrics.observe(operation, status_code, elapsed);
    state
        .metrics
        .requests_in_flight
        .fetch_sub(1, Ordering::Relaxed);
    tracing::info!(
        parent: &span,
        status_code,
        latency_ms = elapsed.as_secs_f64() * 1_000.0,
        "request completed"
    );
    response
}

fn record_auth_outcome(scope: &'static str, result: &'static str) {
    let span = tracing::Span::current();
    span.record("auth_scope", tracing::field::display(scope));
    span.record("auth_result", tracing::field::display(result));
}

fn record_operator_identity() {
    let span = tracing::Span::current();
    span.record("auth_scope", tracing::field::display("operator"));
    span.record("auth_result", tracing::field::display("authenticated"));
    span.record(
        "auth_actor_type",
        tracing::field::display("operator_credential"),
    );
    // The current API has one shared operator Bearer token, not individual
    // human operator accounts. Keep that limitation explicit in audit data.
    span.record(
        "auth_actor_id",
        tracing::field::display("shared_operator_credential"),
    );
}

fn record_tenant_identity(context: &TenantContext) {
    let span = tracing::Span::current();
    span.record("auth_scope", tracing::field::display("tenant"));
    span.record("auth_result", tracing::field::display("authenticated"));
    span.record(
        "auth_actor_type",
        tracing::field::display("tenant_principal"),
    );
    span.record(
        "auth_tenant_id",
        tracing::field::display(context.tenant_id().as_str()),
    );
    span.record(
        "auth_principal_id",
        tracing::field::display(context.principal_id().as_str()),
    );
}

fn record_auth_throttled() {
    tracing::Span::current().record("auth_throttled", true);
}

async fn export_metrics(State(state): State<AppState>) -> Response {
    // In health-only mode the operator auth middleware intentionally permits
    // requests to reach handlers; metrics remain unavailable without a DB and
    // therefore cannot become an unauthenticated remote information endpoint.
    if state.database.is_none() {
        return ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "database_unavailable",
            "metrics are unavailable until PostgreSQL is configured",
        )
        .into_response();
    }

    let mut response = state.metrics.render_prometheus().into_response();
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
}

fn valid_cors_origin(value: &str) -> bool {
    if value.is_empty()
        || value == "*"
        || value
            .chars()
            .any(|character| matches!(character, '?' | '#' | ','))
    {
        return false;
    }
    let Ok(uri) = value.parse::<Uri>() else {
        return false;
    };
    let Some(scheme) = uri.scheme_str() else {
        return false;
    };
    let Some(authority) = uri.authority() else {
        return false;
    };
    if !matches!(scheme, "http" | "https")
        || authority.as_str().contains('@')
        || authority.host().is_empty()
        || uri.query().is_some()
    {
        return false;
    }
    let canonical = format!("{scheme}://{authority}");
    value == canonical
}

fn requested_cors_headers_are_allowed(value: &HeaderValue) -> bool {
    let Ok(value) = value.to_str() else {
        return false;
    };
    value.split(',').all(|header| {
        matches!(
            header.trim().to_ascii_lowercase().as_str(),
            "authorization" | "content-type" | "traceparent" | "tracestate"
        )
    })
}

fn attach_cors_origin(response: &mut Response, origin: &str) {
    if let Ok(value) = HeaderValue::from_str(origin) {
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, value);
        response
            .headers_mut()
            .insert(VARY, HeaderValue::from_static("Origin"));
        response.headers_mut().insert(
            ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static("traceparent, tracestate"),
        );
    }
}

async fn apply_browser_cors(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let origin = request
        .headers()
        .get(ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    if request.method() == Method::OPTIONS
        && request
            .headers()
            .contains_key(ACCESS_CONTROL_REQUEST_METHOD)
    {
        let Some(origin) = origin.as_deref() else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        if !state.cors_allowed_origins.contains(origin) {
            return StatusCode::FORBIDDEN.into_response();
        }
        let requested_method = request
            .headers()
            .get(ACCESS_CONTROL_REQUEST_METHOD)
            .and_then(|value| value.to_str().ok());
        if !matches!(requested_method, Some("GET" | "POST")) {
            return StatusCode::FORBIDDEN.into_response();
        }
        if request
            .headers()
            .get(ACCESS_CONTROL_REQUEST_HEADERS)
            .is_some_and(|value| !requested_cors_headers_are_allowed(value))
        {
            return StatusCode::FORBIDDEN.into_response();
        }

        let mut response = StatusCode::NO_CONTENT.into_response();
        attach_cors_origin(&mut response, origin);
        response.headers_mut().insert(
            ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, POST, OPTIONS"),
        );
        response.headers_mut().insert(
            ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static("authorization, content-type, traceparent, tracestate"),
        );
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("600"));
        return response;
    }

    let mut response = next.run(request).await;
    if let Some(origin) = origin.as_deref()
        && state.cors_allowed_origins.contains(origin)
    {
        attach_cors_origin(&mut response, origin);
    }
    response
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
        record_auth_outcome("operator", "health_only_mode");
        return next.run(request).await;
    }

    let Some(expected) = state.api_token_hash.as_ref() else {
        record_auth_outcome("operator", "unavailable");
        return ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "auth_not_configured",
            "the control-plane operator credential is not configured",
        )
        .into_response();
    };

    if !authorization_matches(expected, request.headers()) {
        record_auth_outcome("operator", "rejected");
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

    record_operator_identity();
    if let Some(retry_after) = state
        .principal_rate_limiter
        .check(PrincipalRateLimitKey::Operator)
    {
        record_auth_throttled();
        return rate_limited_response(retry_after);
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
        record_auth_outcome("tenant", "rejected");
        return unauthorized_bearer_response();
    };

    let database = match state.database() {
        Ok(database) => database,
        Err(error) => {
            record_auth_outcome("tenant", "unavailable");
            return error.into_response();
        }
    };
    let context = match database.tenant_credentials.authenticate_token(token).await {
        Ok(context) => context,
        Err(StorageError::InvalidTenantCredential) => {
            record_auth_outcome("tenant", "rejected");
            return unauthorized_bearer_response();
        }
        Err(error) => {
            record_auth_outcome("tenant", "error");
            return ApiError::from_storage(error).into_response();
        }
    };

    record_tenant_identity(&context);
    let key = PrincipalRateLimitKey::Tenant {
        tenant_id: context.tenant_id().as_str().to_owned(),
        principal_id: context.principal_id().as_str().to_owned(),
    };
    if let Some(retry_after) = state.principal_rate_limiter.check(key) {
        record_auth_throttled();
        return rate_limited_response(retry_after);
    }

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
    let Query(query) =
        query.map_err(|rejection| ApiError::invalid_request(rejection.into_response().status()))?;
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
    let Query(query) =
        query.map_err(|rejection| ApiError::invalid_request(rejection.into_response().status()))?;
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
    let Query(query) =
        query.map_err(|rejection| ApiError::invalid_request(rejection.into_response().status()))?;
    let limit = page_limit(query.limit)?;
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_cursor::<EventHistoryCursor>)
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|value| value.before_event_id <= 0)
    {
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
    let events = page
        .events()
        .iter()
        .map(WorkflowEventListItem::from)
        .collect();
    Ok(Json(WorkflowEventListResponse {
        events,
        next_cursor,
    }))
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
    let Query(query) =
        query.map_err(|rejection| ApiError::invalid_request(rejection.into_response().status()))?;
    let limit = page_limit(query.limit)?;
    let cursor = query
        .cursor
        .as_deref()
        .map(decode_cursor::<EventHistoryCursor>)
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|value| value.before_event_id <= 0)
    {
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
    let events = page
        .events()
        .iter()
        .map(WorkflowEventListItem::from)
        .collect();
    Ok(Json(WorkflowEventListResponse {
        events,
        next_cursor,
    }))
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
    use super::{
        AppState, HttpMetrics, HttpOperation, PrincipalRateLimitKey, PrincipalRateLimiter,
        authorization_matches, hash_api_token, router,
    };
    use axum::body::{Body, to_bytes};
    use axum::http::header::{
        ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_REQUEST_HEADERS,
        ACCESS_CONTROL_REQUEST_METHOD, AUTHORIZATION, ORIGIN, WWW_AUTHENTICATE,
    };
    use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode};
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    const TEST_API_TOKEN: &str = "test-only-aetherflow-api-token-32-bytes";

    #[test]
    fn prometheus_metrics_use_bounded_latency_buckets_and_aggregate_only() {
        let metrics = HttpMetrics::default();
        metrics.observe(HttpOperation::OperatorRead, 200, Duration::from_millis(3));
        metrics.observe(HttpOperation::TenantSubmit, 503, Duration::from_millis(300));

        let output = metrics.render_prometheus();
        assert!(output.contains("aetherflow_http_requests_total 2"));
        assert!(output.contains("aetherflow_http_requests_failed_total 1"));
        assert!(output.contains("aetherflow_http_requests_rate_limited_total 0"));
        assert!(
            output.contains(r#"aetherflow_http_request_duration_seconds_bucket{le="0.005"} 1"#)
        );
        assert!(output.contains(r#"aetherflow_http_request_duration_seconds_bucket{le="0.5"} 2"#));
        assert!(output.contains(r#"aetherflow_http_request_duration_seconds_bucket{le="+Inf"} 2"#));
        assert!(
            output.contains(
                r#"aetherflow_http_operation_requests_total{operation="operator_read"} 1"#
            )
        );
        assert!(output.contains(
            r#"aetherflow_http_operation_requests_failed_total{operation="tenant_submit"} 1"#
        ));
        assert!(output.contains(r#"aetherflow_http_operation_request_duration_seconds_bucket{operation="tenant_submit",le="0.5"} 1"#));
        assert!(output.contains(r#"aetherflow_http_operation_request_duration_seconds_count{operation="tenant_submit"} 1"#));
        assert!(!output.contains("workflow_run_id"));
        assert!(!output.contains("authorization"));
        assert!(!output.contains("tenant-private"));
    }

    #[test]
    fn http_operation_classification_uses_only_bounded_route_classes() {
        assert_eq!(
            HttpOperation::classify(&Method::GET, "/health"),
            HttpOperation::Health
        );
        assert_eq!(
            HttpOperation::classify(&Method::GET, "/api/v1/metrics"),
            HttpOperation::Metrics
        );
        assert_eq!(
            HttpOperation::classify(&Method::POST, "/api/v1/workflow-runs"),
            HttpOperation::OperatorSubmit
        );
        assert_eq!(
            HttpOperation::classify(
                &Method::GET,
                "/api/v1/workflow-runs/workflow-private-123/events"
            ),
            HttpOperation::OperatorRead
        );
        assert_eq!(
            HttpOperation::classify(&Method::POST, "/api/v1/task-runs/task-private-456/cancel"),
            HttpOperation::OperatorMutation
        );
        assert_eq!(
            HttpOperation::classify(&Method::POST, "/api/v1/tenant/workflow-runs"),
            HttpOperation::TenantSubmit
        );
        assert_eq!(
            HttpOperation::classify(
                &Method::GET,
                "/api/v1/tenant/workflow-runs/tenant-private-789/events"
            ),
            HttpOperation::TenantRead
        );
        assert_eq!(
            HttpOperation::classify(
                &Method::POST,
                "/api/v1/tenant/task-runs/task-private-abc/cancel"
            ),
            HttpOperation::TenantMutation
        );
        assert_eq!(
            HttpOperation::classify(&Method::GET, "/unmatched/customer-private"),
            HttpOperation::Other
        );
        assert_eq!(
            HttpOperation::classify(&Method::OPTIONS, "/api/v1/workflow-runs"),
            HttpOperation::Other
        );
    }

    #[test]
    fn principal_rate_limiter_enforces_burst_refill_and_identity_isolation() {
        let limiter = PrincipalRateLimiter::new(2, 1).expect("valid test rate-limit policy");
        let now = std::time::Instant::now();
        let operator = PrincipalRateLimitKey::Operator;
        let tenant_a = PrincipalRateLimitKey::Tenant {
            tenant_id: "tenant-a".to_owned(),
            principal_id: "principal-a".to_owned(),
        };
        let tenant_b = PrincipalRateLimitKey::Tenant {
            tenant_id: "tenant-a".to_owned(),
            principal_id: "principal-b".to_owned(),
        };

        assert_eq!(limiter.check_at(operator.clone(), now), None);
        assert_eq!(limiter.check_at(operator.clone(), now), None);
        assert_eq!(
            limiter.check_at(operator.clone(), now),
            Some(Duration::from_secs(1))
        );
        // Tenant principals and the operator never consume one another's quota.
        assert_eq!(limiter.check_at(tenant_a.clone(), now), None);
        assert_eq!(limiter.check_at(tenant_b, now), None);
        // A token is replenished continuously at the configured rate.
        assert_eq!(
            limiter.check_at(operator, now + Duration::from_secs(1)),
            None
        );
        assert_eq!(limiter.bucket_count(), 3);
    }

    #[test]
    fn principal_rate_limiter_rejects_zero_configuration_and_bounds_identity_storage() {
        assert!(PrincipalRateLimiter::new(0, 1).is_err());
        assert!(PrincipalRateLimiter::new(1, 0).is_err());

        let limiter = PrincipalRateLimiter::new(1, 1).expect("valid rate-limit policy");
        let now = std::time::Instant::now();
        for index in 0..(PrincipalRateLimiter::MAX_BUCKETS - 1) {
            let key = PrincipalRateLimitKey::Tenant {
                tenant_id: "tenant-bounded".to_owned(),
                principal_id: format!("principal-{index}"),
            };
            assert_eq!(limiter.check_at(key, now), None);
        }
        assert_eq!(
            limiter.bucket_count(),
            PrincipalRateLimiter::MAX_BUCKETS - 1
        );

        // The first identity beyond the cap creates the shared overflow
        // bucket. Every later overflow identity shares its depleted allowance.
        let first_overflow = PrincipalRateLimitKey::Tenant {
            tenant_id: "tenant-overflow".to_owned(),
            principal_id: "overflow-a".to_owned(),
        };
        let second_overflow = PrincipalRateLimitKey::Tenant {
            tenant_id: "tenant-overflow".to_owned(),
            principal_id: "overflow-b".to_owned(),
        };
        assert_eq!(limiter.check_at(first_overflow, now), None);
        assert_eq!(limiter.bucket_count(), PrincipalRateLimiter::MAX_BUCKETS);
        assert_eq!(
            limiter.check_at(second_overflow, now),
            Some(Duration::from_secs(1))
        );

        // A previously admitted principal keeps its exhausted bucket instead
        // of receiving a fresh burst through an eviction/reset cycle.
        let established = PrincipalRateLimitKey::Tenant {
            tenant_id: "tenant-bounded".to_owned(),
            principal_id: "principal-0".to_owned(),
        };
        assert_eq!(
            limiter.check_at(established, now),
            Some(Duration::from_secs(1))
        );
        assert_eq!(limiter.bucket_count(), PrincipalRateLimiter::MAX_BUCKETS);
    }

    #[tokio::test]
    async fn request_middleware_tracks_health_without_recording_query_values() {
        let state = AppState::without_database();
        let metrics = Arc::clone(&state.metrics);
        let response = router(state)
            .oneshot(
                Request::builder()
                    .uri("/health?secret=must-not-be-recorded")
                    .body(Body::empty())
                    .expect("valid health request"),
            )
            .await
            .expect("health response");
        assert_eq!(response.status(), StatusCode::OK);

        let output = metrics.render_prometheus();
        assert!(output.contains("aetherflow_http_requests_total 1"));
        assert!(output.contains("aetherflow_http_requests_in_flight 0"));
        assert!(!output.contains("must-not-be-recorded"));
    }

    #[tokio::test]
    async fn metrics_endpoint_is_unavailable_in_health_only_mode() {
        let response = router(AppState::without_database())
            .oneshot(
                Request::builder()
                    .uri("/api/v1/metrics")
                    .body(Body::empty())
                    .expect("valid metrics request"),
            )
            .await
            .expect("metrics response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

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
    async fn cors_requires_an_exact_allowlisted_origin_and_bounded_preflight() {
        let state = AppState::without_database()
            .with_cors_allowed_origins("https://pooya16pg-spec.github.io")
            .expect("valid exact Pages origin");
        let preflight = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/api/v1/workflow-runs")
                    .header(ORIGIN, "https://pooya16pg-spec.github.io")
                    .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header(
                        ACCESS_CONTROL_REQUEST_HEADERS,
                        "authorization, traceparent, tracestate",
                    )
                    .body(Body::empty())
                    .expect("valid Pages preflight request"),
            )
            .await
            .expect("preflight response");
        assert_eq!(preflight.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            preflight.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static(
                "https://pooya16pg-spec.github.io"
            ))
        );
        assert_eq!(
            preflight.headers().get(ACCESS_CONTROL_ALLOW_METHODS),
            Some(&HeaderValue::from_static("GET, POST, OPTIONS"))
        );
        assert_eq!(
            preflight.headers().get("access-control-allow-headers"),
            Some(&HeaderValue::from_static(
                "authorization, content-type, traceparent, tracestate"
            ))
        );
        assert_eq!(
            preflight.headers().get("access-control-expose-headers"),
            Some(&HeaderValue::from_static("traceparent, tracestate"))
        );
        assert!(
            preflight
                .headers()
                .get("access-control-allow-credentials")
                .is_none()
        );

        let blocked_origin = router(state.clone())
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/api/v1/workflow-runs")
                    .header(ORIGIN, "https://attacker.example")
                    .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .body(Body::empty())
                    .expect("valid foreign-origin preflight"),
            )
            .await
            .expect("blocked preflight response");
        assert_eq!(blocked_origin.status(), StatusCode::FORBIDDEN);
        assert!(
            blocked_origin
                .headers()
                .get(ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none()
        );

        let blocked_header = router(state)
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/api/v1/workflow-runs")
                    .header(ORIGIN, "https://pooya16pg-spec.github.io")
                    .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header(
                        ACCESS_CONTROL_REQUEST_HEADERS,
                        "authorization, x-private-header",
                    )
                    .body(Body::empty())
                    .expect("valid disallowed-header preflight"),
            )
            .await
            .expect("blocked preflight response");
        assert_eq!(blocked_header.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn cors_origin_configuration_rejects_wildcards_paths_and_non_http_schemes() {
        for invalid in [
            "*",
            "https://example.com/path",
            "https://example.com?query=1",
            "file://example.com",
            "https://user@example.com",
        ] {
            assert!(
                AppState::without_database()
                    .with_cors_allowed_origins(invalid)
                    .is_err(),
                "origin {invalid} must be rejected"
            );
        }
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
        let limited_operator_state = AppState::from_pool(pool.clone(), TEST_API_TOKEN)
            .expect("configure limited operator API state")
            .with_api_rate_limit(1, 1)
            .expect("configure one-request operator burst");
        let operator_metrics = Arc::clone(&limited_operator_state.metrics);
        let operator_first = router(limited_operator_state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .uri("/api/v1/workflow-runs?limit=1")
                    .body(Body::empty())
                    .expect("valid first limited operator request"),
            )
            .await
            .expect("first limited operator response");
        assert_eq!(operator_first.status(), StatusCode::OK);
        let operator_second = router(limited_operator_state)
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .uri("/api/v1/workflow-runs?limit=1")
                    .body(Body::empty())
                    .expect("valid second limited operator request"),
            )
            .await
            .expect("second limited operator response");
        assert_eq!(operator_second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            operator_second.headers().get("retry-after"),
            Some(&HeaderValue::from_static("1"))
        );
        let operator_limited_body = to_bytes(operator_second.into_body(), 1024)
            .await
            .expect("read operator rate-limit response");
        let operator_limited_json: serde_json::Value =
            serde_json::from_slice(&operator_limited_body).expect("valid rate-limit JSON");
        assert_eq!(operator_limited_json["error"]["code"], "rate_limited");
        assert!(
            operator_metrics
                .render_prometheus()
                .contains("aetherflow_http_requests_rate_limited_total 1")
        );

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

        let second_workflow_id = format!("api-workflow-second-{suffix}");
        let second_workflow_run_id = format!("api-run-second-{suffix}");
        let second_task_id = format!("api-task-second-{suffix}");
        let second_body = serde_json::json!({
            "workflow_id": second_workflow_id,
            "workflow_run_id": second_workflow_run_id,
            "tasks": [{"task_id": second_task_id, "depends_on": []}],
            "retry_policy": {
                "max_attempts": 1,
                "initial_backoff_ms": 0,
                "max_backoff_ms": 0
            }
        })
        .to_string();
        let second_submit = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .method("POST")
                    .uri("/api/v1/workflow-runs")
                    .header("content-type", "application/json")
                    .body(Body::from(second_body))
                    .expect("valid second workflow submission"),
            )
            .await
            .expect("second workflow submission response");
        assert_eq!(second_submit.status(), StatusCode::CREATED);

        let first_page = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .uri("/api/v1/workflow-runs?limit=1")
                    .body(Body::empty())
                    .expect("valid first workflow page request"),
            )
            .await
            .expect("first workflow page response");
        assert_eq!(first_page.status(), StatusCode::OK);
        let first_page_body = to_bytes(first_page.into_body(), 16 * 1024)
            .await
            .expect("read first workflow page");
        let first_page_json: serde_json::Value =
            serde_json::from_slice(&first_page_body).expect("valid first workflow page JSON");
        assert_eq!(first_page_json["items"].as_array().map(Vec::len), Some(1));
        let next_cursor = first_page_json["next_cursor"]
            .as_str()
            .expect("a second workflow page exists")
            .to_owned();

        let second_page = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .uri(format!(
                        "/api/v1/workflow-runs?limit=1&cursor={next_cursor}"
                    ))
                    .body(Body::empty())
                    .expect("valid second workflow page request"),
            )
            .await
            .expect("second workflow page response");
        assert_eq!(second_page.status(), StatusCode::OK);
        let second_page_body = to_bytes(second_page.into_body(), 16 * 1024)
            .await
            .expect("read second workflow page");
        let second_page_json: serde_json::Value =
            serde_json::from_slice(&second_page_body).expect("valid second workflow page JSON");
        assert_eq!(second_page_json["items"].as_array().map(Vec::len), Some(1));
        assert_ne!(
            first_page_json["items"][0]["workflow_run_id"],
            second_page_json["items"][0]["workflow_run_id"],
            "keyset pagination must not repeat a workflow across adjacent pages"
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

        let event_history = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .uri(format!(
                        "/api/v1/workflow-runs/{workflow_run_id}/events?limit=1"
                    ))
                    .body(Body::empty())
                    .expect("valid operator event-history request"),
            )
            .await
            .expect("operator event-history response");
        assert_eq!(event_history.status(), StatusCode::OK);
        let event_history_body = to_bytes(event_history.into_body(), 16 * 1024)
            .await
            .expect("read operator event history");
        let event_history_json: serde_json::Value =
            serde_json::from_slice(&event_history_body).expect("valid event-history JSON");
        assert_eq!(
            event_history_json["events"].as_array().map(Vec::len),
            Some(1)
        );
        assert!(
            event_history_json["events"][0].get("payload").is_none(),
            "event history must never return raw outbox payloads"
        );

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

        let limited_tenant_state = AppState::from_pool(pool.clone(), TEST_API_TOKEN)
            .expect("configure limited tenant API state")
            .with_api_rate_limit(1, 1)
            .expect("configure one-request tenant burst");
        let tenant_first = router(limited_tenant_state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {tenant_token}"))
                    .uri("/api/v1/tenant/workflow-runs?limit=1")
                    .body(Body::empty())
                    .expect("valid first limited tenant request"),
            )
            .await
            .expect("first limited tenant response");
        assert_eq!(tenant_first.status(), StatusCode::OK);
        let tenant_second = router(limited_tenant_state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {tenant_token}"))
                    .uri("/api/v1/tenant/workflow-runs?limit=1")
                    .body(Body::empty())
                    .expect("valid second limited tenant request"),
            )
            .await
            .expect("second limited tenant response");
        assert_eq!(tenant_second.status(), StatusCode::TOO_MANY_REQUESTS);
        let independent_reader = router(limited_tenant_state)
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {reader_token}"))
                    .uri("/api/v1/tenant/workflow-runs?limit=1")
                    .body(Body::empty())
                    .expect("valid independent-principal request"),
            )
            .await
            .expect("independent-principal response");
        assert_eq!(independent_reader.status(), StatusCode::OK);

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

        let tenant_workflow_list = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {tenant_token}"))
                    .uri("/api/v1/tenant/workflow-runs?limit=100")
                    .body(Body::empty())
                    .expect("valid tenant workflow-list request"),
            )
            .await
            .expect("tenant workflow-list response");
        assert_eq!(tenant_workflow_list.status(), StatusCode::OK);
        let tenant_workflow_list_body = to_bytes(tenant_workflow_list.into_body(), 16 * 1024)
            .await
            .expect("read tenant workflow list");
        let tenant_workflow_list_json: serde_json::Value =
            serde_json::from_slice(&tenant_workflow_list_body).expect("valid tenant list JSON");
        let tenant_workflow_ids = tenant_workflow_list_json["items"]
            .as_array()
            .expect("tenant workflow list array")
            .iter()
            .filter_map(|item| item["workflow_run_id"].as_str())
            .collect::<Vec<_>>();
        assert!(tenant_workflow_ids.contains(&tenant_workflow_run_id.as_str()));
        assert!(!tenant_workflow_ids.contains(&foreign_workflow_run_id.as_str()));

        let operator_legacy_list = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {TEST_API_TOKEN}"))
                    .uri("/api/v1/workflow-runs?limit=100")
                    .body(Body::empty())
                    .expect("valid legacy workflow-list request"),
            )
            .await
            .expect("legacy workflow-list response");
        assert_eq!(operator_legacy_list.status(), StatusCode::OK);
        let operator_legacy_list_body = to_bytes(operator_legacy_list.into_body(), 16 * 1024)
            .await
            .expect("read legacy workflow list");
        let operator_legacy_list_json: serde_json::Value =
            serde_json::from_slice(&operator_legacy_list_body).expect("valid legacy list JSON");
        let legacy_workflow_ids = operator_legacy_list_json["items"]
            .as_array()
            .expect("legacy workflow list array")
            .iter()
            .filter_map(|item| item["workflow_run_id"].as_str())
            .collect::<Vec<_>>();
        assert!(!legacy_workflow_ids.contains(&foreign_workflow_run_id.as_str()));

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

        let tenant_event_history = router(state.clone())
            .oneshot(
                Request::builder()
                    .header(AUTHORIZATION, format!("Bearer {tenant_token}"))
                    .uri(format!(
                        "/api/v1/tenant/workflow-runs/{tenant_workflow_run_id}/events?limit=1"
                    ))
                    .body(Body::empty())
                    .expect("valid tenant event-history request"),
            )
            .await
            .expect("tenant event-history response");
        assert_eq!(tenant_event_history.status(), StatusCode::OK);
        let tenant_event_history_body = to_bytes(tenant_event_history.into_body(), 16 * 1024)
            .await
            .expect("read tenant event history");
        let tenant_event_history_json: serde_json::Value =
            serde_json::from_slice(&tenant_event_history_body).expect("valid tenant events JSON");
        assert_eq!(
            tenant_event_history_json["events"].as_array().map(Vec::len),
            Some(1)
        );
        assert!(
            tenant_event_history_json["events"][0]
                .get("payload")
                .is_none(),
            "tenant event history must never expose outbox JSON"
        );

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
