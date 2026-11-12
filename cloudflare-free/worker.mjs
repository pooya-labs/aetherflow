import { WorkflowEntrypoint } from "cloudflare:workers";
import { readBoundedRequestText, RequestBodyError } from "./request-body.mjs";
import "../serverless-shared/workflow-core.js";

const CORE = globalThis.AetherFlowCore;
const TERMINAL = new Set(["succeeded", "failed", "cancelled", "timed_out"]);
const DEFAULTS = Object.freeze({
  maxDailyReads: 12000,
  maxDailyMutations: 2500,
  maxDailyBootstraps: 60,
  maxDailyWorkflowRuns: 25,
  maxDailyWorkflowSteps: 2000,
  maxSessionReads: 250,
  maxSessionMutations: 60,
  maxBodyBytes: 16384,
  maxTaskBytes: 8192,
  maxTasks: 20,
  maxVisitors: 500,
  maxWorkflowsPerWorkspace: 50
});

class HttpError extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

function nowIso() { return new Date().toISOString(); }
function nowMs() { return Date.now(); }

function limitFromEnv(env, key) {
  const raw = env[key];
  if (raw === undefined || raw === null || raw === "") return DEFAULTS[key];
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 1 || value > 1000000) {
    throw new Error("Invalid runtime setting: " + key);
  }
  return value;
}

function errorJson(status, code, message) {
  return new Response(JSON.stringify({
    error: { code: code, message: message, request_id: "cf-" + crypto.randomUUID() }
  }), {
    status: status,
    headers: { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" }
  });
}

function json(data, status) {
  return new Response(JSON.stringify(data), {
    status: status || 200,
    headers: { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" }
  });
}

function cors(response, request, allowedOrigin) {
  const headers = new Headers(response.headers);
  headers.set("Cache-Control", "no-store");
  headers.set("X-Content-Type-Options", "nosniff");
  headers.set("Vary", "Origin");
  if (request.headers.get("Origin") === allowedOrigin) {
    headers.set("Access-Control-Allow-Origin", allowedOrigin);
    headers.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    headers.set("Access-Control-Allow-Headers", "Authorization, Content-Type");
    headers.set("Access-Control-Max-Age", "86400");
  }
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers: headers });
}

async function readJson(request, maxBytes) {
  const type = request.headers.get("Content-Type") || "";
  if (!/^application\\/(?:json|[a-z0-9.+-]+\\+json)(?:\\s*;|$)/i.test(type)) {
    throw new HttpError(415, "content_type_required", "Use application/json for API requests.");
  }

  let text;
  try {
    text = await readBoundedRequestText(request, maxBytes);
  } catch (error) {
    if (error instanceof RequestBodyError) {
      throw new HttpError(error.status, error.code, error.message);
    }
    throw new HttpError(400, "invalid_request_body", "The request body could not be read.");
  }

  try { return JSON.parse(text); }
  catch (ignored) { throw new HttpError(400, "invalid_json", "The request body must be valid JSON."); }
}

async function sha256(value) {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(String(value)));
  return Array.from(new Uint8Array(digest), function (byte) {
    return byte.toString(16).padStart(2, "0");
  }).join("");
}

function base64ToBytes(value) {
  if (!value) return new Uint8Array(0);
  const binary = atob(value);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
  return bytes;
}

function decodeTaskInput(value) {
  if (!value) return { type: "echo", value: null };
  try {
    const decoded = new TextDecoder("utf-8", { fatal: true }).decode(base64ToBytes(value));
    const parsed = JSON.parse(decoded);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("Task input must be a JSON object.");
    }
    return parsed;
  } catch (error) {
    throw new HttpError(400, "invalid_task_input", "Task input must be base64-encoded UTF-8 JSON.");
  }
}

function encodeUtf8Json(value) {
  const bytes = new TextEncoder().encode(JSON.stringify(value));
  let binary = "";
  for (let index = 0; index < bytes.length; index += 1) binary += String.fromCharCode(bytes[index]);
  return { base64: btoa(binary), size: bytes.length };
}

function dailyKey() { return new Date().toISOString().slice(0, 10); }

async function consumeDaily(db, kind, cap) {
  const result = await db.prepare(
    "INSERT INTO demo_daily_usage (usage_day, kind, count) VALUES (?, ?, 1) " +
    "ON CONFLICT(usage_day, kind) DO UPDATE SET count = count + 1 " +
    "WHERE count < ? RETURNING count"
  ).bind(dailyKey(), kind, cap).first();
  return Boolean(result);
}

async function consumeDailyUnits(db, kind, units, cap) {
  if (!Number.isSafeInteger(units) || units < 1 || units > cap) return false;
  const result = await db.prepare(
    "INSERT INTO demo_daily_usage (usage_day, kind, count) VALUES (?, ?, ?) " +
    "ON CONFLICT(usage_day, kind) DO UPDATE SET count = count + ? " +
    "WHERE count + ? <= ? RETURNING count"
  ).bind(dailyKey(), kind, units, units, units, cap).first();
  return Boolean(result);
}

async function insertEvent(db, workspaceId, workflowRunId, aggregateType, aggregateId, version, eventType, stamp) {
  return db.prepare(
    "INSERT INTO demo_events (workspace_id, workflow_run_id, aggregate_type, aggregate_id, aggregate_version, event_type, status, created_at, published_at) " +
    "VALUES (?, ?, ?, ?, ?, ?, 'recorded', ?, NULL)"
  ).bind(workspaceId, workflowRunId, aggregateType, aggregateId, version, eventType, stamp);
}

async function deterministicTaskId(workflowRunId, taskId) {
  return "tr-" + (await sha256(workflowRunId + "\u0000" + taskId)).slice(0, 40);
}

async function bootstrap(db, body, limits) {
  if (!body || typeof body !== "object" || Array.isArray(body) ||
      typeof body.client_id !== "string" || !/^[a-f0-9-]{36}$/i.test(body.client_id)) {
    throw new HttpError(400, "invalid_client_id", "The demo browser identifier is invalid.");
  }
  if (!await consumeDaily(db, "bootstraps", limits.maxDailyBootstraps)) {
    throw new HttpError(429, "daily_demo_limit", "The public demo launch limit has been reached for today.");
  }

  const browserHash = await sha256(body.client_id);
  await db.prepare("DELETE FROM demo_sessions WHERE expires_at <= ?").bind(nowMs()).run();

  let visitor = await db.prepare(
    "SELECT workspace_id FROM demo_visitors WHERE browser_hash = ?"
  ).bind(browserHash).first();

  let workspaceId = visitor && visitor.workspace_id;
  if (!workspaceId) {
    const count = await db.prepare("SELECT COUNT(*) AS count FROM demo_visitors").first();
    if (Number(count && count.count || 0) >= limits.maxVisitors) {
      throw new HttpError(429, "demo_capacity_reached", "The public demo workspace limit has been reached.");
    }
    workspaceId = crypto.randomUUID();
    const stamp = nowIso();
    const statements = [
      db.prepare("INSERT INTO demo_visitors (browser_hash, workspace_id, created_at) VALUES (?, ?, ?)")
        .bind(browserHash, workspaceId, stamp)
    ];

    const samples = [
      {
        workflow_id: "invoice-pipeline", workflow_run_id: "demo-invoice-001", status: "succeeded",
        tasks: [
          { task_id: "validate-order", status: "succeeded", attempts: 1, deps: [] },
          { task_id: "charge-card", status: "succeeded", attempts: 1, deps: ["validate-order"] },
          { task_id: "send-receipt", status: "succeeded", attempts: 1, deps: ["charge-card"] }
        ]
      },
      {
        workflow_id: "media-export", workflow_run_id: "demo-media-002", status: "running",
        tasks: [
          { task_id: "load-source", status: "succeeded", attempts: 1, deps: [] },
          { task_id: "transcode-video", status: "running", attempts: 1, deps: ["load-source"] },
          { task_id: "publish-asset", status: "pending", attempts: 0, deps: ["transcode-video"] }
        ]
      },
      {
        workflow_id: "nightly-index", workflow_run_id: "demo-index-003", status: "failed",
        tasks: [
          { task_id: "scan-records", status: "succeeded", attempts: 1, deps: [] },
          { task_id: "compact-index", status: "failed", attempts: 2, deps: ["scan-records"] }
        ]
      },
      {
        workflow_id: "retry-example", workflow_run_id: "demo-retry-004", status: "running",
        tasks: [
          { task_id: "fetch-upstream", status: "retry_waiting", attempts: 2, deps: [] },
          { task_id: "normalize-result", status: "pending", attempts: 0, deps: ["fetch-upstream"] }
        ]
      }
    ];

    for (let index = 0; index < samples.length; index += 1) {
      const sample = samples[index];
      const created = new Date(nowMs() - (index + 1) * 3600000).toISOString();
      const updated = sample.status === "running" ? stamp : created;
      const finished = TERMINAL.has(sample.status) ? created : null;
      const workflowVersion = finished ? 4 : 2;
      statements.push(db.prepare(
        "INSERT INTO demo_workflows (workspace_id, workflow_run_id, workflow_id, workflow_instance_id, status, version, created_at, updated_at, finished_at) " +
        "VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?)"
      ).bind(workspaceId, sample.workflow_run_id, sample.workflow_id, sample.status, workflowVersion, created, updated, finished));

      const ids = {};
      for (const task of sample.tasks) ids[task.task_id] = await deterministicTaskId(sample.workflow_run_id, task.task_id);

      for (let taskIndex = 0; taskIndex < sample.tasks.length; taskIndex += 1) {
        const task = sample.tasks[taskIndex];
        const taskCreated = created;
        const taskUpdated = task.status === "running" ? stamp : created;
        const taskFinished = TERMINAL.has(task.status) ? created : null;
        const deps = task.deps.map(function (id) { return ids[id]; });
        statements.push(db.prepare(
          "INSERT INTO demo_tasks (workspace_id, workflow_run_id, task_run_id, task_id, status, version, attempts_started, max_attempts, initial_backoff_ms, max_backoff_ms, available_at, input_base64, input_size_bytes, output_size_bytes, output_present, dependency_task_run_ids_json, claimed_by, lease_expires_at, created_at, updated_at, started_at, finished_at, last_error_kind, last_error_message) " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, 3, 1000, 30000, ?, '', 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)"
        ).bind(
          workspaceId, sample.workflow_run_id, ids[task.task_id], task.task_id, task.status,
          TERMINAL.has(task.status) ? 3 : 1, task.attempts, created,
          task.status === "succeeded" ? 48 : null, task.status === "succeeded" ? 1 : 0,
          JSON.stringify(deps), task.status === "running" ? "demo-worker-01" : null,
          task.status === "running" ? new Date(nowMs() + 60000).toISOString() : null,
          taskCreated, taskUpdated, task.attempts ? taskCreated : null, taskFinished
        ));
        if (task.status !== "pending") {
          statements.push(await insertEvent(db, workspaceId, sample.workflow_run_id, "task_run",
            ids[task.task_id], taskIndex + 1, "task." + task.status, created));
        }
      }
      statements.push(await insertEvent(db, workspaceId, sample.workflow_run_id, "workflow_run",
        sample.workflow_run_id, 1, "workflow.submitted", created));
      if (finished) {
        statements.push(await insertEvent(db, workspaceId, sample.workflow_run_id, "workflow_run",
          sample.workflow_run_id, workflowVersion, "workflow." + sample.status, finished));
      }
    }

    try {
      await db.batch(statements);
    } catch (error) {
      const existing = await db.prepare("SELECT workspace_id FROM demo_visitors WHERE browser_hash = ?")
        .bind(browserHash).first();
      if (!existing) throw error;
      workspaceId = existing.workspace_id;
    }
  }

  const sessionToken = crypto.randomUUID().replace(/-/g, "") + crypto.randomUUID().replace(/-/g, "");
  await db.prepare(
    "INSERT INTO demo_sessions (token_hash, workspace_id, expires_at, read_count, mutation_count, created_at) VALUES (?, ?, ?, 0, 0, ?)"
  ).bind(await sha256(sessionToken), workspaceId, nowMs() + 21600000, nowIso()).run();

  return {
    sessionToken: sessionToken,
    expiresInSeconds: 21600,
    user: { sub: "demo:" + browserHash.slice(0, 16), name: "AetherFlow Serverless Operator", preferred_username: "demo-operator" },
    workspace: { id: workspaceId, name: "AetherFlow Serverless Workspace" },
    mode: "cloudflare-workflows-d1"
  };
}

async function authorize(db, request, mutation, limits) {
  const header = request.headers.get("Authorization") || "";
  const match = header.match(/^Bearer ([A-Za-z0-9_-]{32,128})$/);
  if (!match) throw new HttpError(401, "unauthorized", "A valid demo session is required.");
  const tokenHash = await sha256(match[1]);
  const now = nowMs();
  const column = mutation ? "mutation_count" : "read_count";
  const cap = mutation ? limits.maxSessionMutations : limits.maxSessionReads;
  const row = await db.prepare(
    "UPDATE demo_sessions SET " + column + " = " + column + " + 1 " +
    "WHERE token_hash = ? AND expires_at > ? AND " + column + " < ? RETURNING workspace_id"
  ).bind(tokenHash, now, cap).first();

  if (!row) {
    const existing = await db.prepare("SELECT expires_at FROM demo_sessions WHERE token_hash = ?")
      .bind(tokenHash).first();
    if (existing && Number(existing.expires_at) > now) {
      throw new HttpError(429, "session_rate_limited", "The demo session request limit was reached.");
    }
    throw new HttpError(401, "demo_session_expired", "The demo session expired. Reload the page to start again.");
  }

  if (!await consumeDaily(db, mutation ? "mutations" : "reads",
    mutation ? limits.maxDailyMutations : limits.maxDailyReads)) {
    throw new HttpError(429, "daily_demo_limit", "The daily serverless request budget has been reached.");
  }
  return row.workspace_id;
}

async function workflowList(db, workspaceId, url) {
  const status = url.searchParams.get("status") || "";
  const allowed = ["pending", "running", "succeeded", "failed", "cancelled", "timed_out"];
  if (status && allowed.indexOf(status) < 0) throw new HttpError(400, "invalid_status", "The workflow status filter is invalid.");
  const limit = safeLimit(url.searchParams.get("limit"), 25, 100);
  const offsetRaw = url.searchParams.get("cursor") || "0";
  const offset = Number(offsetRaw);
  if (!Number.isSafeInteger(offset) || offset < 0 || offset > 10000) throw new HttpError(400, "invalid_cursor", "The workflow cursor is invalid.");

  const rows = await db.prepare(
    "SELECT w.workflow_id, w.workflow_run_id, w.status, w.version, w.created_at, w.updated_at, " +
    "(SELECT COUNT(*) FROM demo_tasks t WHERE t.workspace_id = w.workspace_id AND t.workflow_run_id = w.workflow_run_id) AS task_count " +
    "FROM demo_workflows w WHERE w.workspace_id = ? AND (? = '' OR w.status = ?) " +
    "ORDER BY w.created_at DESC, w.workflow_run_id DESC LIMIT ? OFFSET ?"
  ).bind(workspaceId, status, status, limit + 1, offset).all();

  return {
    items: rows.results.slice(0, limit),
    next_cursor: rows.results.length > limit ? String(offset + limit) : null
  };
}

async function workflowSnapshot(db, workspaceId, workflowRunId) {
  const workflow = await db.prepare(
    "SELECT workflow_id, workflow_run_id, status, version, created_at, updated_at FROM demo_workflows WHERE workspace_id = ? AND workflow_run_id = ?"
  ).bind(workspaceId, workflowRunId).first();
  if (!workflow) throw new HttpError(404, "not_found", "The workflow run was not found.");
  const tasks = await db.prepare(
    "SELECT task_id, task_run_id, status FROM demo_tasks WHERE workspace_id = ? AND workflow_run_id = ? ORDER BY task_id"
  ).bind(workspaceId, workflowRunId).all();
  return {
    workflow_id: workflow.workflow_id,
    workflow_run_id: workflow.workflow_run_id,
    status: workflow.status,
    version: workflow.version,
    task_runs: tasks.results,
    created_at: workflow.created_at,
    updated_at: workflow.updated_at
  };
}

async function taskSnapshot(db, workspaceId, taskRunIdValue) {
  const task = await db.prepare(
    "SELECT task_run_id, workflow_run_id, task_id, status, version, attempts_started, available_at, input_size_bytes, output_size_bytes, output_present, dependency_task_run_ids_json, claimed_by, lease_expires_at, created_at, updated_at " +
    "FROM demo_tasks WHERE workspace_id = ? AND task_run_id = ?"
  ).bind(workspaceId, taskRunIdValue).first();
  if (!task) throw new HttpError(404, "not_found", "The task run was not found.");
  let dependencies = [];
  try { dependencies = JSON.parse(task.dependency_task_run_ids_json || "[]"); } catch (ignored) {}
  return {
    task_run_id: task.task_run_id,
    workflow_run_id: task.workflow_run_id,
    task_id: task.task_id,
    status: task.status,
    version: task.version,
    attempts_started: task.attempts_started,
    available_at: task.available_at,
    input_size_bytes: task.input_size_bytes,
    output_size_bytes: task.output_size_bytes,
    output_present: Boolean(task.output_present),
    dependency_task_run_ids: dependencies,
    claimed_by: task.claimed_by || null,
    lease_expires_at: task.lease_expires_at || null,
    created_at: task.created_at,
    updated_at: task.updated_at
  };
}

async function submitWorkflow(db, env, workspaceId, body, limits) {
  let submission;
  try {
    submission = CORE.validateWorkflowSubmission(body, {
      maxTasks: limits.maxTasks,
      maxBodyBytes: limits.maxBodyBytes,
      maxTaskBytes: limits.maxTaskBytes
    });
  } catch (error) {
    const code = String(error && error.message || "invalid_workflow");
    const status = code === "request_too_large" || code === "task_payload_too_large" ? 413 : 400;
    throw new HttpError(status, code, "The workflow submission is invalid: " + code.replace(/_/g, " ") + ".");
  }

  if (!env.AETHERFLOW_EXECUTIONS || typeof env.AETHERFLOW_EXECUTIONS.create !== "function") {
    throw new HttpError(503, "workflow_binding_unavailable", "The Cloudflare Workflow binding is not configured.");
  }
  if (!await consumeDaily(db, "workflow_runs", limits.maxDailyWorkflowRuns)) {
    throw new HttpError(429, "workflow_daily_limit", "The free-tier workflow launch budget has been reached for today.");
  }

  const existing = await db.prepare(
    "SELECT workflow_run_id FROM demo_workflows WHERE workspace_id = ? AND workflow_run_id = ?"
  ).bind(workspaceId, submission.workflow_run_id).first();
  if (existing) throw new HttpError(409, "workflow_conflict", "That workflow-run identifier already exists in this workspace.");

  // Reserve the worst-case attempt count before admission. Workflow start and
  // completion each use the platform default of up to six attempts; task steps
  // reserve their explicit retry-policy ceiling.
  const stepReservation = 12 + submission.tasks.length * submission.retry_policy.max_attempts;
  if (!await consumeDailyUnits(db, "workflow_steps", stepReservation, limits.maxDailyWorkflowSteps)) {
    throw new HttpError(429, "workflow_step_budget_exhausted", "The daily Workflow step budget has been reserved. Try again after the UTC daily reset.");
  }

  const retained = await db.prepare(
    "SELECT workflow_run_id FROM demo_workflows WHERE workspace_id = ? ORDER BY created_at ASC, workflow_run_id ASC"
  ).bind(workspaceId).all();
  if (retained.results.length >= limits.maxWorkflowsPerWorkspace) {
    const oldestTerminal = await db.prepare(
      "SELECT workflow_run_id FROM demo_workflows WHERE workspace_id = ? " +
      "AND status IN ('succeeded','failed','cancelled','timed_out') " +
      "ORDER BY updated_at ASC, workflow_run_id ASC LIMIT 1"
    ).bind(workspaceId).first();
    if (!oldestTerminal) {
      throw new HttpError(429, "workspace_history_limit", "This demo workspace has reached its workflow history limit. No terminal run is available for cleanup.");
    }
    // D1 foreign-key cascades remove the terminal run's tasks and redacted events.
    await db.prepare(
      "DELETE FROM demo_workflows WHERE workspace_id = ? AND workflow_run_id = ? " +
      "AND status IN ('succeeded','failed','cancelled','timed_out')"
    ).bind(workspaceId, oldestTerminal.workflow_run_id).run();
  }

  const stamp = nowIso();
  const workflowInstanceId = "af-" + (await sha256(workspaceId + "\u0000" + submission.workflow_run_id)).slice(0, 56);
  const taskIds = {};
  for (const task of submission.tasks) taskIds[task.task_id] = await deterministicTaskId(submission.workflow_run_id, task.task_id);

  const statements = [
    db.prepare(
      "INSERT INTO demo_workflows (workspace_id, workflow_run_id, workflow_id, workflow_instance_id, status, version, created_at, updated_at, finished_at) " +
      "VALUES (?, ?, ?, ?, 'pending', 1, ?, ?, NULL)"
    ).bind(workspaceId, submission.workflow_run_id, submission.workflow_id, workflowInstanceId, stamp, stamp)
  ];

  for (const task of submission.tasks) {
    const status = task.depends_on.length ? "pending" : "ready";
    const dependencies = task.depends_on.map(function (id) { return taskIds[id]; });
    statements.push(db.prepare(
      "INSERT INTO demo_tasks (workspace_id, workflow_run_id, task_run_id, task_id, status, version, attempts_started, max_attempts, initial_backoff_ms, max_backoff_ms, available_at, input_base64, input_size_bytes, output_size_bytes, output_present, dependency_task_run_ids_json, claimed_by, lease_expires_at, created_at, updated_at, started_at, finished_at, last_error_kind, last_error_message) " +
      "VALUES (?, ?, ?, ?, ?, 1, 0, ?, ?, ?, ?, ?, ?, NULL, 0, ?, NULL, NULL, ?, ?, NULL, NULL, NULL, NULL)"
    ).bind(
      workspaceId, submission.workflow_run_id, taskIds[task.task_id], task.task_id, status,
      submission.retry_policy.max_attempts,
      submission.retry_policy.initial_backoff_ms,
      submission.retry_policy.max_backoff_ms,
      stamp, task.input_base64, task.input_size_bytes, JSON.stringify(dependencies), stamp, stamp
    ));
  }

  statements.push(await insertEvent(db, workspaceId, submission.workflow_run_id, "workflow_run",
    submission.workflow_run_id, 1, "workflow.submitted", stamp));
  for (const task of submission.tasks) {
    statements.push(await insertEvent(db, workspaceId, submission.workflow_run_id, "task_run",
      taskIds[task.task_id], 1, task.depends_on.length ? "task.pending" : "task.ready", stamp));
  }

  await db.batch(statements);

  try {
    await env.AETHERFLOW_EXECUTIONS.create({
      id: workflowInstanceId,
      params: {
        workspace_id: workspaceId,
        workflow_run_id: submission.workflow_run_id,
        workflow_id: submission.workflow_id,
        retry_policy: submission.retry_policy,
        tasks: submission.tasks,
        execution_order: submission.execution_order,
        task_run_ids: taskIds
      }
    });
  } catch (error) {
    const failureStamp = nowIso();
    await db.batch([
      db.prepare(
        "UPDATE demo_workflows SET status = 'failed', version = version + 1, updated_at = ?, finished_at = ? " +
        "WHERE workspace_id = ? AND workflow_run_id = ? AND status = 'pending'"
      ).bind(failureStamp, failureStamp, workspaceId, submission.workflow_run_id),
      await insertEvent(db, workspaceId, submission.workflow_run_id, "workflow_run",
        submission.workflow_run_id, 2, "workflow.failed", failureStamp)
    ]);
    throw new HttpError(503, "workflow_start_failed", "Cloudflare could not start the durable workflow. The failed submission was recorded.");
  }

  return {
    status: 201,
    body: {
      workflow_id: submission.workflow_id,
      workflow_run_id: submission.workflow_run_id,
      status: "pending",
      version: 1,
      workflow_engine: "cloudflare-workflows",
      task_runs: submission.tasks.map(function (task) {
        return {
          task_id: task.task_id,
          task_run_id: taskIds[task.task_id],
          status: task.depends_on.length ? "pending" : "ready"
        };
      })
    }
  };
}

async function markTaskReady(db, payload, task) {
  const stamp = nowIso();
  const row = await db.prepare(
    "UPDATE demo_tasks SET status = 'ready', version = version + 1, updated_at = ? " +
    "WHERE workspace_id = ? AND workflow_run_id = ? AND task_id = ? AND status = 'pending' RETURNING version"
  ).bind(stamp, payload.workspace_id, payload.workflow_run_id, task.task_id).first();
  if (row) {
    await insertEvent(db, payload.workspace_id, payload.workflow_run_id, "task_run",
      payload.task_run_ids[task.task_id], row.version, "task.ready", stamp).then(function (statement) { return db.batch([statement]); });
  }
}

async function markTaskRunning(db, payload, task, attempt) {
  const stamp = nowIso();
  const row = await db.prepare(
    "UPDATE demo_tasks SET status = 'running', version = version + 1, attempts_started = ?, " +
    "started_at = COALESCE(started_at, ?), updated_at = ?, last_error_kind = NULL, last_error_message = NULL " +
    "WHERE workspace_id = ? AND workflow_run_id = ? AND task_id = ? AND status IN ('ready', 'running', 'retry_waiting') RETURNING version"
  ).bind(attempt, stamp, stamp, payload.workspace_id, payload.workflow_run_id, task.task_id).first();
  if (!row) throw new Error("The task is no longer runnable.");
  await db.batch([await insertEvent(db, payload.workspace_id, payload.workflow_run_id, "task_run",
    payload.task_run_ids[task.task_id], row.version, "task.running", stamp)]);
  return row.version;
}

async function markTaskRetryWaiting(db, payload, task, attempt, error) {
  const stamp = nowIso();
  const message = String(error && error.message || "Task attempt failed").slice(0, 300);
  const row = await db.prepare(
    "UPDATE demo_tasks SET status = 'retry_waiting', version = version + 1, attempts_started = ?, " +
    "updated_at = ?, last_error_kind = 'retryable_error', last_error_message = ?, claimed_by = NULL, lease_expires_at = NULL " +
    "WHERE workspace_id = ? AND workflow_run_id = ? AND task_id = ? AND status = 'running' RETURNING version"
  ).bind(attempt, stamp, message, payload.workspace_id, payload.workflow_run_id, task.task_id).first();
  if (!row) return;
  await db.batch([await insertEvent(db, payload.workspace_id, payload.workflow_run_id, "task_run",
    payload.task_run_ids[task.task_id], row.version, "task.retry_waiting", stamp)]);
}

async function executeBuiltinTask(task, dependencies, attempt) {
  const input = decodeTaskInput(task.input_base64);
  const kind = typeof input.type === "string" ? input.type : "echo";
  const value = input.input !== undefined ? input.input :
    input.value !== undefined ? input.value : dependencies;

  if (kind === "echo") return value;
  if (kind === "collect") return dependencies;
  if (kind === "uppercase") return String(value === null ? "" : value).toUpperCase();
  if (kind === "sha256") return { sha256: await sha256(JSON.stringify(value)) };
  if (kind === "add") {
    const values = Array.isArray(input.values) ? input.values : Object.values(dependencies);
    if (!values.length || values.some(function (item) { return typeof item !== "number" || !Number.isFinite(item); })) {
      throw new Error("The add task requires an array of finite numbers.");
    }
    return values.reduce(function (sum, item) { return sum + item; }, 0);
  }
  if (kind === "fail_once") {
    if (attempt === 1) throw new Error("Intentional first-attempt failure for retry verification.");
    return { ok: true, attempts: attempt, recovered: true };
  }
  throw new Error("Unsupported built-in task type: " + kind);
}

async function finishTask(db, payload, task, output) {
  const encoded = encodeUtf8Json(output);
  if (encoded.size > 8192) throw new Error("Task output exceeds the 8 KiB serverless limit.");
  const stamp = nowIso();
  const row = await db.prepare(
    "UPDATE demo_tasks SET status = 'succeeded', version = version + 1, output_size_bytes = ?, output_present = 1, " +
    "updated_at = ?, finished_at = ?, claimed_by = NULL, lease_expires_at = NULL " +
    "WHERE workspace_id = ? AND workflow_run_id = ? AND task_id = ? AND status = 'running' RETURNING version"
  ).bind(encoded.size, stamp, stamp, payload.workspace_id, payload.workflow_run_id, task.task_id).first();
  if (!row) throw new Error("The task state changed before completion.");
  await db.batch([await insertEvent(db, payload.workspace_id, payload.workflow_run_id, "task_run",
    payload.task_run_ids[task.task_id], row.version, "task.succeeded", stamp)]);
  return { size: encoded.size, digest: await sha256(encoded.base64) };
}

async function failWorkflow(db, payload, failedTask, error) {
  const stamp = nowIso();
  const reason = String(error && error.message || "task execution failed").slice(0, 300);
  const failed = await db.prepare(
    "UPDATE demo_tasks SET status = 'failed', version = version + 1, updated_at = ?, finished_at = ?, " +
    "last_error_kind = 'execution_error', last_error_message = ?, claimed_by = NULL, lease_expires_at = NULL " +
    "WHERE workspace_id = ? AND workflow_run_id = ? AND task_id = ? AND status = 'running' RETURNING version"
  ).bind(stamp, stamp, reason, payload.workspace_id, payload.workflow_run_id, failedTask.task_id).first();

  const statements = [];
  if (failed) statements.push(await insertEvent(db, payload.workspace_id, payload.workflow_run_id, "task_run",
    payload.task_run_ids[failedTask.task_id], failed.version, "task.failed", stamp));

  const pending = await db.prepare(
    "SELECT task_id, task_run_id, version FROM demo_tasks WHERE workspace_id = ? AND workflow_run_id = ? AND task_id <> ? AND status IN ('pending', 'ready')"
  ).bind(payload.workspace_id, payload.workflow_run_id, failedTask.task_id).all();

  for (const row of pending.results) {
    const updated = await db.prepare(
      "UPDATE demo_tasks SET status = 'cancelled', version = version + 1, updated_at = ?, finished_at = ? " +
      "WHERE workspace_id = ? AND task_run_id = ? AND version = ? AND status IN ('pending','ready') RETURNING version"
    ).bind(stamp, stamp, payload.workspace_id, row.task_run_id, row.version).first();
    if (updated) statements.push(await insertEvent(db, payload.workspace_id, payload.workflow_run_id, "task_run",
      row.task_run_id, updated.version, "task.cancelled", stamp));
  }

  const workflow = await db.prepare(
    "UPDATE demo_workflows SET status = 'failed', version = version + 1, updated_at = ?, finished_at = ? " +
    "WHERE workspace_id = ? AND workflow_run_id = ? AND status IN ('pending','running') RETURNING version"
  ).bind(stamp, stamp, payload.workspace_id, payload.workflow_run_id).first();
  if (workflow) statements.push(await insertEvent(db, payload.workspace_id, payload.workflow_run_id, "workflow_run",
    payload.workflow_run_id, workflow.version, "workflow.failed", stamp));

  if (statements.length) await db.batch(statements);
}

export class AetherFlowExecution extends WorkflowEntrypoint {
  async run(event, step) {
    // Workflow binding calls pass an object; the REST API passes JSON-encoded params.
    const payload = typeof event.payload === "string" ? JSON.parse(event.payload) : event.payload;
    if (!payload || typeof payload !== "object" || !Array.isArray(payload.tasks) ||
        !Array.isArray(payload.execution_order) || !payload.task_run_ids) {
      throw new Error("The workflow event payload is invalid.");
    }
    const db = this.env.DB;
    const taskMap = new Map(payload.tasks.map(function (task) { return [task.task_id, task]; }));
    const results = {};

    await step.do("workflow-start", async function () {
      const stamp = nowIso();
      const row = await db.prepare(
        "UPDATE demo_workflows SET status = 'running', version = version + 1, updated_at = ? " +
        "WHERE workspace_id = ? AND workflow_run_id = ? AND status = 'pending' RETURNING version"
      ).bind(stamp, payload.workspace_id, payload.workflow_run_id).first();
      if (row) {
        await db.batch([await insertEvent(db, payload.workspace_id, payload.workflow_run_id, "workflow_run",
          payload.workflow_run_id, row.version, "workflow.started", stamp)]);
      }
      return { started: true };
    });

    for (const taskId of payload.execution_order) {
      const task = taskMap.get(taskId);
      if (!task) throw new Error("The workflow definition changed after it was accepted.");
      await markTaskReady(db, payload, task);

      const dependencies = {};
      for (const dependencyId of task.depends_on || []) {
        if (!Object.prototype.hasOwnProperty.call(results, dependencyId)) {
          throw new Error("A task dependency did not complete successfully.");
        }
        dependencies[dependencyId] = results[dependencyId];
      }

      try {
        const output = await step.do(
          "task-" + task.task_id,
          {
            retries: {
              limit: Math.max(0, payload.retry_policy.max_attempts - 1),
              delay: function (context) {
                const initial = payload.retry_policy.initial_backoff_ms;
                const maximum = payload.retry_policy.max_backoff_ms;
                if (initial <= 0 || maximum <= 0) return 0;
                return Math.min(maximum, initial * Math.pow(2, Math.max(0, context.ctx.attempt - 1)));
              },
              backoff: "constant"
            },
            timeout: "1 minute"
          },
          async function (context) {
            const attempt = Number(context && context.attempt) || 1;
            await markTaskRunning(db, payload, task, attempt);
            try {
              const result = await executeBuiltinTask(task, dependencies, attempt);
              await finishTask(db, payload, task, result);
              return result;
            } catch (error) {
              if (attempt < payload.retry_policy.max_attempts) {
                await markTaskRetryWaiting(db, payload, task, attempt, error);
              }
              throw error;
            }
          }
        );
        results[task.task_id] = output;
      } catch (error) {
        await failWorkflow(db, payload, task, error);
        throw error;
      }
    }

    await step.do("workflow-complete", async function () {
      const stamp = nowIso();
      const row = await db.prepare(
        "UPDATE demo_workflows SET status = 'succeeded', version = version + 1, updated_at = ?, finished_at = ? " +
        "WHERE workspace_id = ? AND workflow_run_id = ? AND status = 'running' RETURNING version"
      ).bind(stamp, stamp, payload.workspace_id, payload.workflow_run_id).first();
      if (row) {
        await db.batch([await insertEvent(db, payload.workspace_id, payload.workflow_run_id, "workflow_run",
          payload.workflow_run_id, row.version, "workflow.succeeded", stamp)]);
      }
      return { completed: true };
    });

    return { workflow_run_id: payload.workflow_run_id, status: "succeeded", task_results: results };
  }
}

function safeLimit(value, fallback, maximum) {
  if (value === undefined || value === null || value === "") return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > maximum) {
    throw new HttpError(400, "invalid_pagination", "A pagination value is invalid.");
  }
  return parsed;
}

function loadLimits(env) {
  const result = {};
  for (const key of Object.keys(DEFAULTS)) result[key] = limitFromEnv(env, key);
  return result;
}

async function routeApi(db, env, request, workspaceId, limits) {
  const url = new URL(request.url);
  const path = url.pathname;

  if (request.method === "GET" && path === "/api/v1/workflow-runs") {
    return workflowList(db, workspaceId, url);
  }
  if (request.method === "GET" && path === "/api/v1/workers") {
    return { workers: [] };
  }
  if (request.method === "GET" && path === "/api/v1/metrics") {
    const workflows = await db.prepare(
      "SELECT status, COUNT(*) AS count FROM demo_workflows WHERE workspace_id = ? GROUP BY status"
    ).bind(workspaceId).all();
    const tasks = await db.prepare(
      "SELECT status, COUNT(*) AS count FROM demo_tasks WHERE workspace_id = ? GROUP BY status"
    ).bind(workspaceId).all();
    const lines = ["# AetherFlow Cloudflare Workflows and D1", "aetherflow_serverless_runtime 1"];
    workflows.results.forEach(function (row) {
      lines.push("aetherflow_workflows{status=\"" + row.status + "\"} " + row.count);
    });
    tasks.results.forEach(function (row) {
      lines.push("aetherflow_tasks{status=\"" + row.status + "\"} " + row.count);
    });
    return new Response(lines.join("\n") + "\n", {
      headers: { "Content-Type": "text/plain; version=0.0.4; charset=utf-8", "Cache-Control": "no-store" }
    });
  }

  if (request.method === "POST" && path === "/api/v1/workflow-runs") {
    return submitWorkflow(db, env, workspaceId, await readJson(request, limits.maxBodyBytes), limits);
  }

  const eventMatch = path.match(/^\/api\/v1\/workflow-runs\/([A-Za-z0-9_-]{1,128})\/events$/);
  if (request.method === "GET" && eventMatch) {
    const workflowId = eventMatch[1];
    const exists = await db.prepare(
      "SELECT workflow_run_id FROM demo_workflows WHERE workspace_id = ? AND workflow_run_id = ?"
    ).bind(workspaceId, workflowId).first();
    if (!exists) throw new HttpError(404, "not_found", "The workflow run was not found.");
    const limit = safeLimit(url.searchParams.get("limit"), 50, 100);
    const offset = Number(url.searchParams.get("cursor") || "0");
    if (!Number.isSafeInteger(offset) || offset < 0 || offset > 10000) throw new HttpError(400, "invalid_cursor", "The event cursor is invalid.");
    const events = await db.prepare(
      "SELECT event_id, aggregate_type, aggregate_id, aggregate_version, event_type, status, created_at, published_at " +
      "FROM demo_events WHERE workspace_id = ? AND workflow_run_id = ? ORDER BY event_id LIMIT ? OFFSET ?"
    ).bind(workspaceId, workflowId, limit + 1, offset).all();
    return { events: events.results.slice(0, limit), next_cursor: events.results.length > limit ? String(offset + limit) : null };
  }

  const workflowMatch = path.match(/^\/api\/v1\/workflow-runs\/([A-Za-z0-9_-]{1,128})$/);
  if (request.method === "GET" && workflowMatch) {
    return workflowSnapshot(db, workspaceId, workflowMatch[1]);
  }

  const cancelMatch = path.match(/^\/api\/v1\/task-runs\/([A-Za-z0-9_-]{1,128})\/cancel$/);
  if (request.method === "POST" && cancelMatch) {
    const body = await readJson(request, limits.maxBodyBytes);
    const expected = Number(body && body.expected_version);
    if (!Number.isSafeInteger(expected) || expected < 0) {
      throw new HttpError(400, "invalid_request", "expected_version is required.");
    }
    const current = await db.prepare(
      "SELECT task_run_id, workflow_run_id, status, version FROM demo_tasks WHERE workspace_id = ? AND task_run_id = ?"
    ).bind(workspaceId, cancelMatch[1]).first();
    if (!current) throw new HttpError(404, "not_found", "The task run was not found.");
    if (Number(current.version) !== expected) {
      throw new HttpError(409, "version_conflict", "The task run changed. Refresh and retry.");
    }
    if (TERMINAL.has(current.status)) {
      throw new HttpError(409, "invalid_transition", "This task run is already terminal.");
    }

    const workflow = await db.prepare(
      "SELECT workflow_id, workflow_instance_id, status, version FROM demo_workflows WHERE workspace_id = ? AND workflow_run_id = ?"
    ).bind(workspaceId, current.workflow_run_id).first();
    if (!workflow) throw new HttpError(404, "not_found", "The workflow run was not found.");

    const pendingTasks = await db.prepare(
      "SELECT task_run_id, version FROM demo_tasks WHERE workspace_id = ? AND workflow_run_id = ? AND status NOT IN ('succeeded','failed','cancelled','timed_out')"
    ).bind(workspaceId, current.workflow_run_id).all();
    const stamp = nowIso();
    const statements = [];
    const taskVersions = [];
    for (const task of pendingTasks.results) {
      const update = db.prepare(
        "UPDATE demo_tasks SET status = 'cancelled', version = version + 1, updated_at = ?, finished_at = ?, claimed_by = NULL, lease_expires_at = NULL " +
        "WHERE workspace_id = ? AND workflow_run_id = ? AND task_run_id = ? AND version = ? AND status NOT IN ('succeeded','failed','cancelled','timed_out') RETURNING version"
      ).bind(stamp, stamp, workspaceId, current.workflow_run_id, task.task_run_id, task.version);
      const result = await update.first();
      if (result) {
        taskVersions.push({ task_run_id: task.task_run_id, version: result.version });
        statements.push(await insertEvent(db, workspaceId, current.workflow_run_id, "task_run",
          task.task_run_id, result.version, "task.cancelled", stamp));
      }
    }
    const workflowUpdate = await db.prepare(
      "UPDATE demo_workflows SET status = 'cancelled', version = version + 1, updated_at = ?, finished_at = ? " +
      "WHERE workspace_id = ? AND workflow_run_id = ? AND version = ? AND status IN ('pending','running') RETURNING version"
    ).bind(stamp, stamp, workspaceId, current.workflow_run_id, workflow.version).first();
    if (!workflowUpdate) throw new HttpError(409, "version_conflict", "The workflow run changed while cancellation was being applied.");
    statements.push(await insertEvent(db, workspaceId, current.workflow_run_id, "workflow_run",
      current.workflow_run_id, workflowUpdate.version, "workflow.cancelled", stamp));
    if (statements.length) await db.batch(statements);

    if (workflow.workflow_instance_id && env.AETHERFLOW_EXECUTIONS) {
      try {
        const instance = await env.AETHERFLOW_EXECUTIONS.get(workflow.workflow_instance_id);
        await instance.terminate();
      } catch (ignored) {
        // The durable engine may already have settled between the D1 transition and termination.
      }
    }
    return {
      task_run: await taskSnapshot(db, workspaceId, current.task_run_id),
      workflow_status: "cancelled"
    };
  }

  const taskMatch = path.match(/^\/api\/v1\/task-runs\/([A-Za-z0-9_-]{1,128})$/);
  if (request.method === "GET" && taskMatch) {
    return taskSnapshot(db, workspaceId, taskMatch[1]);
  }

  throw new HttpError(404, "route_not_found", "The requested API route was not found.");
}

export default {
  async fetch(request, env) {
    const allowedOrigin = env.ALLOWED_ORIGIN || "https://pooya-labs.github.io";
    const origin = request.headers.get("Origin");
    if (origin && origin !== allowedOrigin) {
      return errorJson(403, "origin_not_allowed", "This browser origin is not allowed.");
    }
    if (request.method === "OPTIONS") {
      return cors(new Response(null, { status: 204 }), request, allowedOrigin);
    }
    if (request.method !== "GET" && request.method !== "POST") {
      return cors(errorJson(405, "method_not_allowed", "This API method is not supported."), request, allowedOrigin);
    }
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/health") {
      return cors(json({ ready: true, mode: "cloudflare-workflows-d1" }), request, allowedOrigin);
    }
    if (!env.DB) {
      return cors(errorJson(503, "database_unavailable", "The Cloudflare D1 binding is not configured."), request, allowedOrigin);
    }

    let limits;
    try { limits = loadLimits(env); }
    catch (error) {
      return cors(errorJson(503, "runtime_misconfigured", "The Cloudflare runtime configuration is invalid."), request, allowedOrigin);
    }

    try {
      if (request.method === "POST" && url.pathname === "/api/v1/demo/bootstrap") {
        const body = await readJson(request, limits.maxBodyBytes);
        return cors(json(await bootstrap(env.DB, body, limits)), request, allowedOrigin);
      }
      if (!url.pathname.startsWith("/api/v1/")) {
        throw new HttpError(404, "route_not_found", "The requested API route was not found.");
      }

      const workspaceId = await authorize(env.DB, request, request.method === "POST", limits);
      const result = await routeApi(env.DB, env, request, workspaceId, limits);
      if (result instanceof Response) return cors(result, request, allowedOrigin);
      if (result && result.status && result.body !== undefined) {
        return cors(json(result.body, result.status), request, allowedOrigin);
      }
      return cors(json(result), request, allowedOrigin);
    } catch (error) {
      if (error instanceof HttpError) {
        return cors(errorJson(error.status, error.code, error.message), request, allowedOrigin);
      }
      return cors(errorJson(500, "runtime_error", "The serverless runtime could not complete this request."), request, allowedOrigin);
    }
  }
};
