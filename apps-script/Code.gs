/**
 * AetherFlow Apps Script runtime.
 * Zero-cost public demo/control-plane profile backed by one private owner Sheet.
 * It does not execute user code, WASM plugins, NATS messages, or real workloads.
 */
var AETHERFLOW_GAS = Object.freeze({
  appUrl: "https://pooya-labs.github.io/aetherflow/dashboard/?aetherflowRuntime=apps-script",
  databaseProperty: "AETHERFLOW_DEMO_SPREADSHEET_ID",
  sheetName: "records",
  schemaVersion: 1,
  sessionTtlSeconds: 21600,
  maxRequestsPerSession: 250,
  maxMutationsPerSession: 60,
  maxBodyBytes: 16384,
  maxTaskBytes: 8192,
  maxRecords: 10000,
  maxVisitors: 200,
  maxDailyBootstraps: 25,
  maxDailyRequests: 800,
  maxDailyMutations: 200
});
var AETHERFLOW_HEADERS = ["collection", "id", "workspace_id", "payload_json", "updated_at"];

function doGet() {
  var template = HtmlService.createTemplateFromFile("Index");
  template.appUrl = AETHERFLOW_GAS.appUrl;
  return template.evaluate()
    .setTitle("AetherFlow — Apps Script Demo")
    .addMetaTag("viewport", "width=device-width, initial-scale=1");
}

/** Owner-only setup. Run initializeAetherFlowDemo from the Apps Script editor. */
function initializeAetherFlowDemo() {
  var lock = LockService.getScriptLock();
  lock.waitLock(10000);
  try {
    var props = PropertiesService.getScriptProperties();
    var id = props.getProperty(AETHERFLOW_GAS.databaseProperty);
    if (!id) {
      var sheetFile = SpreadsheetApp.create("AetherFlow Demo Database");
      id = sheetFile.getId();
      props.setProperty(AETHERFLOW_GAS.databaseProperty, id);
    }
    var sheet = ensureSheet_(id);
    if (readRecords_(sheet, "visitors", "").length === 0) {
      writeRecord_(sheet, "runtime_meta", "seeded", {
        id: "seeded", schema_version: AETHERFLOW_GAS.schemaVersion, created_at: now_()
      });
    }
    return { ready: true, schemaVersion: AETHERFLOW_GAS.schemaVersion };
  } finally {
    lock.releaseLock();
  }
}

/** Apps Script iframe bootstrap. The browser identifier is pseudonymous and hashed. */
function aetherflowBootstrap(clientId) {
  var lock = LockService.getScriptLock();
  lock.waitLock(10000);
  try {
    var sheet = getSheet_();
    if (typeof clientId !== "string" || !/^[a-f0-9-]{36}$/i.test(clientId)) {
      throw safeError_(400, "invalid_client_id", "The demo browser identifier is invalid. Reload the page.");
    }
    if (!consumeDailyBudget_("bootstraps", AETHERFLOW_GAS.maxDailyBootstraps)) {
      throw safeError_(429, "daily_demo_limit", "The public demo launch limit has been reached for today.");
    }

    var browserHash = sha256_(clientId);
    var visitor = readOne_(sheet, "visitors", "", browserHash);
    var workspace = visitor ? readOne_(sheet, "workspaces", visitor.workspace_id, visitor.workspace_id) : null;
    if (!workspace) {
      if (readRecords_(sheet, "visitors", "").length >= AETHERFLOW_GAS.maxVisitors) {
        throw safeError_(429, "demo_capacity_reached", "The public demo workspace limit has been reached.");
      }
      workspace = createWorkspace_(sheet, browserHash);
      visitor = {
        id: browserHash, workspace_id: workspace.id,
        browser_key_hash: browserHash, created_at: now_()
      };
      writeRecord_(sheet, "visitors", "", visitor);
    }

    var token = Utilities.getUuid().replace(/-/g, "") + Utilities.getUuid().replace(/-/g, "");
    CacheService.getScriptCache().put(
      "session:" + sha256_(token),
      JSON.stringify({ workspace_id: workspace.id }),
      AETHERFLOW_GAS.sessionTtlSeconds
    );
    return {
      sessionToken: token,
      expiresInSeconds: AETHERFLOW_GAS.sessionTtlSeconds,
      user: { sub: "demo:" + browserHash.slice(0, 16), name: "AetherFlow Demo Operator", preferred_username: "demo-operator" },
      workspace: { id: workspace.id, name: workspace.name },
      mode: "apps-script-demo"
    };
  } finally {
    lock.releaseLock();
  }
}

/** Fixed allowlisted API adapter. It is called only by the trusted iframe wrapper. */
function aetherflowApi(request) {
  var lock;
  try {
    if (!request || typeof request !== "object" || Array.isArray(request)) {
      return errorResponse_(400, "invalid_request", "The API request is invalid.");
    }
    var method = String(request.method || "").toUpperCase();
    var path = String(request.path || "");
    var token = String(request.sessionToken || "");
    var body = request.body === undefined ? {} : request.body;
    if (["GET", "POST"].indexOf(method) < 0) {
      return errorResponse_(405, "method_not_allowed", "This demo API method is not supported.");
    }
    if (!path.startsWith("/api/v1/") || path.length > 1200 || path.indexOf("..") >= 0) {
      return errorResponse_(404, "route_not_found", "The requested demo API route was not found.");
    }
    var bodyJson = JSON.stringify(body);
    if (utf8Length_(bodyJson) > AETHERFLOW_GAS.maxBodyBytes) {
      return errorResponse_(413, "request_too_large", "The demo request is too large.");
    }

    lock = LockService.getScriptLock();
    lock.waitLock(10000);
    var session = CacheService.getScriptCache().get("session:" + sha256_(token));
    if (!session) return errorResponse_(401, "demo_session_expired", "The demo session expired. Reload the page.");
    var workspaceId = JSON.parse(session).workspace_id;
    var mutation = method !== "GET";
    if (!consumeSessionBudget_(token, mutation)) {
      return errorResponse_(429, "rate_limited", "The demo request limit was reached. Try again later.");
    }
    var sheet = getSheet_();
    var result = routeApi_(sheet, method, path, body, workspaceId);
    return { status: result && result.status ? result.status : 200, body: result && result.body !== undefined ? result.body : result };
  } catch (error) {
    return errorResponse_(
      error && error.status ? Number(error.status) : 500,
      error && error.code ? String(error.code) : "demo_runtime_error",
      error && error.safeMessage ? String(error.safeMessage) : "The demo service could not complete the request."
    );
  } finally {
    if (lock && lock.hasLock()) lock.releaseLock();
  }
}

function createWorkspace_(sheet, browserHash) {
  var stamp = now_();
  var workspaceId = uuid_();
  var workspace = {
    id: workspaceId,
    name: "AetherFlow Demo Workspace",
    slug: "aetherflow-demo-" + browserHash.slice(0, 8),
    created_at: stamp,
    updated_at: stamp
  };
  writeRecord_(sheet, "workspaces", workspaceId, workspace);

  var sampleRuns = [
    {
      workflow_id: "invoice-pipeline", workflow_run_id: "demo-invoice-001", status: "succeeded",
      tasks: [
        { task_id: "validate-order", status: "succeeded", attempts_started: 1 },
        { task_id: "charge-card", status: "succeeded", attempts_started: 1, depends_on: ["validate-order"] },
        { task_id: "send-receipt", status: "succeeded", attempts_started: 1, depends_on: ["charge-card"] }
      ]
    },
    {
      workflow_id: "media-export", workflow_run_id: "demo-media-002", status: "running",
      tasks: [
        { task_id: "load-source", status: "succeeded", attempts_started: 1 },
        { task_id: "transcode-video", status: "running", attempts_started: 1, depends_on: ["load-source"] },
        { task_id: "publish-asset", status: "pending", attempts_started: 0, depends_on: ["transcode-video"] }
      ]
    },
    {
      workflow_id: "nightly-index", workflow_run_id: "demo-index-003", status: "failed",
      tasks: [
        { task_id: "scan-records", status: "succeeded", attempts_started: 1 },
        { task_id: "compact-index", status: "failed", attempts_started: 2, depends_on: ["scan-records"] }
      ]
    },
    {
      workflow_id: "retry-example", workflow_run_id: "demo-retry-004", status: "running",
      tasks: [
        { task_id: "fetch-upstream", status: "retry_waiting", attempts_started: 2 },
        { task_id: "normalize-result", status: "pending", attempts_started: 0, depends_on: ["fetch-upstream"] }
      ]
    }
  ];

  sampleRuns.forEach(function (sample, index) {
    var created = new Date(Date.now() - (index + 1) * 3600000).toISOString();
    var workflow = {
      workflow_id: sample.workflow_id,
      workflow_run_id: sample.workflow_run_id,
      status: sample.status,
      version: sample.status === "succeeded" || sample.status === "failed" ? 4 : 2,
      created_at: created,
      updated_at: index === 1 ? now_() : created,
      finished_at: sample.status === "succeeded" || sample.status === "failed" ? created : null
    };
    writeRecord_(sheet, "workflow_runs", workspaceId, workflow);
    var taskIds = {};
    sample.tasks.forEach(function (task) { taskIds[task.task_id] = "tr-" + sha256_(sample.workflow_run_id + "\u0000" + task.task_id).slice(0, 40); });
    sample.tasks.forEach(function (task, taskIndex) {
      var taskRun = {
        task_run_id: taskIds[task.task_id],
        workflow_run_id: sample.workflow_run_id,
        task_id: task.task_id,
        status: task.status,
        version: task.status === "succeeded" || task.status === "failed" ? 3 : 1,
        attempts_started: task.attempts_started,
        available_at: created,
        input_size_bytes: 32 + taskIndex * 12,
        output_size_bytes: task.status === "succeeded" ? 48 : null,
        output_present: task.status === "succeeded",
        dependency_task_run_ids: (task.depends_on || []).map(function (id) { return taskIds[id]; }),
        claimed_by: task.status === "running" ? "demo-worker-01" : null,
        lease_expires_at: task.status === "running" ? new Date(Date.now() + 60000).toISOString() : null,
        created_at: created,
        updated_at: task.status === "running" ? now_() : created,
        finished_at: task.status === "succeeded" || task.status === "failed" ? created : null
      };
      writeRecord_(sheet, "task_runs", workspaceId, taskRun);
    });
    appendEvent_(sheet, workspaceId, sample.workflow_run_id, "workflow_run", sample.workflow_run_id, 1, "workflow.submitted", created);
    sample.tasks.forEach(function (task, taskIndex) {
      if (task.status !== "pending") {
        appendEvent_(sheet, workspaceId, sample.workflow_run_id, "task_run", taskIds[task.task_id], taskIndex + 1,
          "task." + task.status, created);
      }
    });
    if (workflow.finished_at) appendEvent_(sheet, workspaceId, sample.workflow_run_id, "workflow_run", sample.workflow_run_id, workflow.version,
      "workflow." + workflow.status, workflow.finished_at);
  });

  return workspace;
}

function routeApi_(sheet, method, path, body, workspaceId) {
  var parsed = parsePath_(path);
  var route = parsed.pathname;
  var query = parsed.query;
  if (method === "GET" && route === "/api/v1/workflow-runs") {
    var items = readRecords_(sheet, "workflow_runs", workspaceId);
    if (query.status) items = items.filter(function (item) { return item.status === query.status; });
    items.sort(function (a, b) { return b.created_at.localeCompare(a.created_at) || b.workflow_run_id.localeCompare(a.workflow_run_id); });
    var offset = boundedInt_(query.cursor, 0, 0, 10000);
    var limit = boundedInt_(query.limit, 25, 1, 100);
    var page = items.slice(offset, offset + limit);
    var next = offset + limit < items.length ? String(offset + limit) : null;
    return { items: page.map(function (item) { return workflowListItem_(sheet, workspaceId, item); }), next_cursor: next };
  }

  var match = route.match(/^\/api\/v1\/workflow-runs\/([A-Za-z0-9_-]{1,128})\/events$/);
  if (match && method === "GET") {
    requireRecord_(sheet, "workflow_runs", workspaceId, match[1]);
    var events = readRecords_(sheet, "workflow_events", workspaceId).filter(function (item) {
      return item.aggregate_type === "workflow_run" && item.aggregate_id === match[1] ||
        item.workflow_run_id === match[1];
    }).sort(function (a, b) { return a.event_id - b.event_id; });
    var eventOffset = boundedInt_(query.cursor, 0, 0, 10000);
    var eventLimit = boundedInt_(query.limit, 50, 1, 100);
    var eventPage = events.slice(eventOffset, eventOffset + eventLimit);
    return { events: eventPage, next_cursor: eventOffset + eventLimit < events.length ? String(eventOffset + eventLimit) : null };
  }

  match = route.match(/^\/api\/v1\/workflow-runs\/([A-Za-z0-9_-]{1,128})$/);
  if (match && method === "GET") {
    var run = requireRecord_(sheet, "workflow_runs", workspaceId, match[1]);
    var tasks = readRecords_(sheet, "task_runs", workspaceId).filter(function (item) { return item.workflow_run_id === match[1]; });
    tasks.sort(function (a, b) { return a.task_id.localeCompare(b.task_id); });
    return workflowSnapshot_(run, tasks);
  }

  if (route === "/api/v1/workflow-runs" && method === "POST") {
    var submission = AetherFlowCore.validateWorkflowSubmission(body, {
      maxTasks: 100, maxBodyBytes: AETHERFLOW_GAS.maxBodyBytes, maxTaskBytes: AETHERFLOW_GAS.maxTaskBytes
    });
    if (readOne_(sheet, "workflow_runs", workspaceId, submission.workflow_run_id)) {
      throw safeError_(409, "workflow_conflict", "That workflow-run identifier already exists in this demo workspace.");
    }
    var stamp = now_();
    var taskIds = {};
    submission.tasks.forEach(function (task) {
      taskIds[task.task_id] = "tr-" + sha256_(submission.workflow_run_id + "\u0000" + task.task_id).slice(0, 40);
    });
    var workflow = {
      workflow_id: submission.workflow_id, workflow_run_id: submission.workflow_run_id,
      status: "running", version: 1, created_at: stamp, updated_at: stamp, finished_at: null
    };
    writeRecord_(sheet, "workflow_runs", workspaceId, workflow);
    submission.tasks.forEach(function (task) {
      var row = {
        task_run_id: taskIds[task.task_id], workflow_run_id: submission.workflow_run_id,
        task_id: task.task_id, status: task.depends_on.length ? "pending" : "ready",
        version: 1, attempts_started: 0, max_attempts: submission.retry_policy.max_attempts,
        retry_policy: submission.retry_policy, available_at: stamp,
        input_base64: task.input_base64, input_size_bytes: task.input_size_bytes,
        output_size_bytes: null, output_present: false,
        dependency_task_run_ids: task.depends_on.map(function (id) { return taskIds[id]; }),
        claimed_by: null, lease_expires_at: null, created_at: stamp, updated_at: stamp, finished_at: null
      };
      writeRecord_(sheet, "task_runs", workspaceId, row);
    });
    appendEvent_(sheet, workspaceId, submission.workflow_run_id, "workflow_run", submission.workflow_run_id, 1, "workflow.submitted", stamp);
    submission.tasks.forEach(function (task) {
      appendEvent_(sheet, workspaceId, submission.workflow_run_id, "task_run", taskIds[task.task_id], 1,
        task.depends_on.length ? "task.pending" : "task.ready", stamp);
    });
    var taskReceipts = submission.tasks.map(function (task) {
      return { task_id: task.task_id, task_run_id: taskIds[task.task_id], status: task.depends_on.length ? "pending" : "ready" };
    });
    return { status: 201, body: {
      workflow_id: workflow.workflow_id, workflow_run_id: workflow.workflow_run_id,
      status: workflow.status, version: workflow.version, task_runs: taskReceipts
    }};
  }

  match = route.match(/^\/api\/v1\/task-runs\/([A-Za-z0-9_-]{1,128})$/);
  if (match && method === "GET") {
    return taskSnapshot_(requireRecord_(sheet, "task_runs", workspaceId, match[1]));
  }

  match = route.match(/^\/api\/v1\/task-runs\/([A-Za-z0-9_-]{1,128})\/cancel$/);
  if (match && method === "POST") {
    var current = requireRecord_(sheet, "task_runs", workspaceId, match[1]);
    var expected = Number(body && body.expected_version);
    if (!Number.isSafeInteger(expected)) throw safeError_(400, "invalid_request", "expected_version is required.");
    var updated;
    try {
      updated = AetherFlowCore.transitionTask(current, "cancelled", expected, now_());
    } catch (error) {
      var code = String(error && error.message || "");
      if (code === "version_conflict") throw safeError_(409, "version_conflict", "The task-run changed. Refresh and retry.");
      throw safeError_(409, "invalid_transition", "This task-run can no longer be cancelled.");
    }
    updated.workflow_run_id = current.workflow_run_id;
    updated.task_id = current.task_id;
    updated.task_run_id = current.task_run_id;
    updated.input_size_bytes = current.input_size_bytes;
    updated.output_size_bytes = current.output_size_bytes;
    updated.output_present = current.output_present;
    updated.dependency_task_run_ids = current.dependency_task_run_ids;
    updated.claimed_by = null;
    updated.lease_expires_at = null;
    writeRecord_(sheet, "task_runs", workspaceId, updated);
    var tasks = readRecords_(sheet, "task_runs", workspaceId).filter(function (item) { return item.workflow_run_id === current.workflow_run_id; });
    var run = requireRecord_(sheet, "workflow_runs", workspaceId, current.workflow_run_id);
    var nextStatus = AetherFlowCore.workflowStatus(tasks);
    run.status = nextStatus;
    run.version += 1;
    run.updated_at = now_();
    if (AetherFlowCore.terminalStates.indexOf(nextStatus) >= 0) run.finished_at = run.updated_at;
    writeRecord_(sheet, "workflow_runs", workspaceId, run);
    appendEvent_(sheet, workspaceId, current.workflow_run_id, "task_run", current.task_run_id, updated.version, "task.cancelled", updated.updated_at);
    appendEvent_(sheet, workspaceId, run.workflow_run_id, "workflow_run", run.workflow_run_id, run.version, "workflow." + run.status, run.updated_at);
    return { task_run: taskSnapshot_(updated), workflow_status: run.status };
  }

  if (method === "GET" && route === "/api/v1/workers") return { workers: [] };
  if (method === "GET" && route === "/api/v1/metrics") return { status: 200, body: "# AetherFlow Apps Script demo metrics\naetherflow_demo_runtime 1\naetherflow_demo_workflows " + readRecords_(sheet, "workflow_runs", workspaceId).length + "\n", content_type: "text/plain; version=0.0.4" };
  if (method === "GET" && route === "/health") return { ready: true, mode: "apps-script-demo" };

  throw safeError_(404, "route_not_found", "The requested demo API route was not found.");
}

function workflowListItem_(sheet, workspaceId, run) {
  var tasks = readRecords_(sheet, "task_runs", workspaceId);
  var count = tasks.filter(function (task) { return task.workflow_run_id === run.workflow_run_id; }).length;
  return {
    workflow_id: run.workflow_id, workflow_run_id: run.workflow_run_id,
    status: run.status, version: run.version, task_count: count,
    created_at: run.created_at, updated_at: run.updated_at
  };
}

function workflowSnapshot_(run, tasks) {
  return {
    workflow_id: run.workflow_id, workflow_run_id: run.workflow_run_id,
    status: run.status, version: run.version,
    task_runs: tasks.map(function (task) {
      return { task_id: task.task_id, task_run_id: task.task_run_id, status: task.status };
    }),
    created_at: run.created_at, updated_at: run.updated_at
  };
}

function taskSnapshot_(task) {
  return {
    task_run_id: task.task_run_id, workflow_run_id: task.workflow_run_id, task_id: task.task_id,
    status: task.status, version: task.version, attempts_started: task.attempts_started,
    available_at: task.available_at, input_size_bytes: task.input_size_bytes,
    output_size_bytes: task.output_size_bytes, output_present: task.output_present,
    dependency_task_run_ids: task.dependency_task_run_ids || [],
    claimed_by: task.claimed_by || null, lease_expires_at: task.lease_expires_at || null,
    created_at: task.created_at, updated_at: task.updated_at
  };
}

function appendEvent_(sheet, workspaceId, workflowRunId, aggregateType, aggregateId, version, type, stamp) {
  var sequenceKey = "AETHERFLOW_DEMO_EVENT_SEQ";
  var props = PropertiesService.getScriptProperties();
  var next = Number(props.getProperty(sequenceKey) || 0) + 1;
  props.setProperty(sequenceKey, String(next));
  var event = {
    event_id: next, workspace_id: workspaceId,
    workflow_run_id: workflowRunId,
    aggregate_type: aggregateType, aggregate_id: aggregateId,
    aggregate_version: version, event_type: type, status: "recorded",
    created_at: stamp, published_at: null
  };
  writeRecord_(sheet, "workflow_events", workspaceId, event);
}

function parsePath_(path) {
  var question = path.indexOf("?");
  var pathname = question < 0 ? path : path.slice(0, question);
  var raw = question < 0 ? "" : path.slice(question + 1);
  var query = {};
  if (raw) raw.split("&").forEach(function (part) {
    var split = part.indexOf("=");
    var key = decodeURIComponent(split < 0 ? part : part.slice(0, split).replace(/\+/g, " "));
    var value = decodeURIComponent(split < 0 ? "" : part.slice(split + 1).replace(/\+/g, " "));
    if (key && query[key] === undefined) query[key] = value;
  });
  return { pathname: pathname, query: query };
}

function requireRecord_(sheet, collection, workspaceId, id) {
  var record = readOne_(sheet, collection, workspaceId, id);
  if (!record) throw safeError_(404, "not_found", "The requested resource was not found.");
  return record;
}

function getSheet_() {
  var id = PropertiesService.getScriptProperties().getProperty(AETHERFLOW_GAS.databaseProperty);
  if (!id) throw safeError_(503, "demo_not_initialized", "The owner must initialize the Apps Script demo first.");
  return ensureSheet_(id);
}

function ensureSheet_(id) {
  var spreadsheet = SpreadsheetApp.openById(id);
  var sheet = spreadsheet.getSheetByName(AETHERFLOW_GAS.sheetName);
  if (!sheet) {
    var first = spreadsheet.getSheets()[0];
    if (first && first.getLastRow() === 0 && first.getName() === "Sheet1") {
      first.setName(AETHERFLOW_GAS.sheetName);
      sheet = first;
    } else {
      sheet = spreadsheet.insertSheet(AETHERFLOW_GAS.sheetName);
    }
  }
  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, AETHERFLOW_HEADERS.length).setValues([AETHERFLOW_HEADERS]);
    sheet.setFrozenRows(1);
  }
  return sheet;
}

function readRecords_(sheet, collection, workspaceId) {
  var last = sheet.getLastRow();
  if (last < 2) return [];
  var rows = sheet.getRange(2, 1, last - 1, AETHERFLOW_HEADERS.length).getValues();
  var result = [];
  rows.forEach(function (row) {
    if (String(row[0]) !== collection || (workspaceId !== undefined && String(row[2]) !== String(workspaceId))) return;
    try {
      var value = JSON.parse(String(row[3]));
      if (value) result.push(value);
    } catch (ignored) {}
  });
  return result;
}

function readOne_(sheet, collection, workspaceId, id) {
  var records = readRecords_(sheet, collection, workspaceId);
  for (var index = 0; index < records.length; index += 1) {
    if (String(records[index].id || records[index].workflow_run_id || records[index].task_run_id || "") === String(id)) return records[index];
  }
  return null;
}

function writeRecord_(sheet, collection, workspaceId, value) {
  var payload = JSON.stringify(value);
  if (utf8Length_(payload) > 45000) throw safeError_(413, "record_too_large", "The demo record is too large.");
  var last = sheet.getLastRow();
  if (last >= 2) {
    var rows = sheet.getRange(2, 1, last - 1, 3).getValues();
    var identity = String(value.id || value.workflow_run_id || value.task_run_id || value.event_id || "");
    for (var index = rows.length - 1; index >= 0; index -= 1) {
      if (String(rows[index][0]) === collection && String(rows[index][1]) === identity &&
          String(rows[index][2]) === String(workspaceId || "")) {
        sheet.getRange(index + 2, 4, 1, 2).setValues([[payload, now_()]]);
        return;
      }
    }
  }
  if (last >= AETHERFLOW_GAS.maxRecords + 1) throw safeError_(429, "demo_storage_limit", "The Apps Script demo storage limit has been reached.");
  var key = String(value.id || value.workflow_run_id || value.task_run_id || value.event_id || uuid_());
  sheet.appendRow([collection, key, workspaceId || "", payload, now_()]);
}

function consumeSessionBudget_(token, mutation) {
  var cache = CacheService.getScriptCache();
  var kind = mutation ? "write" : "read";
  var key = "quota:" + sha256_(token) + ":" + kind;
  var limit = mutation ? AETHERFLOW_GAS.maxMutationsPerSession : AETHERFLOW_GAS.maxRequestsPerSession;
  var current = Number(cache.get(key) || 0);
  if (current >= limit) return false;
  if (!consumeDailyBudget_(mutation ? "mutations" : "requests", mutation ? AETHERFLOW_GAS.maxDailyMutations : AETHERFLOW_GAS.maxDailyRequests)) return false;
  cache.put(key, String(current + 1), AETHERFLOW_GAS.sessionTtlSeconds);
  return true;
}

function consumeDailyBudget_(kind, limit) {
  var props = PropertiesService.getScriptProperties();
  var key = "AETHERFLOW_DEMO_DAILY_USAGE";
  var today = new Date().toISOString().slice(0, 10);
  var usage = {};
  try { usage = JSON.parse(props.getProperty(key) || "{}"); } catch (ignored) { usage = {}; }
  if (usage.day !== today) usage = { day: today };
  var value = Number(usage[kind] || 0);
  if (!Number.isFinite(value) || value >= limit) return false;
  usage[kind] = value + 1;
  props.setProperty(key, JSON.stringify(usage));
  return true;
}

function errorResponse_(status, code, message) {
  return {
    status: status,
    body: { error: { code: code, message: message, request_id: "gas-" + uuid_() } }
  };
}

function safeError_(status, code, message) {
  var error = new Error(message);
  error.status = status;
  error.code = code;
  error.safeMessage = message;
  return error;
}

function boundedInt_(value, fallback, minimum, maximum) {
  if (value === undefined || value === null || value === "") return fallback;
  var number = Number(value);
  if (!Number.isSafeInteger(number) || number < minimum || number > maximum) throw safeError_(400, "invalid_request", "A pagination value is invalid.");
  return number;
}

function sha256_(value) {
  var bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, String(value), Utilities.Charset.UTF_8);
  return bytes.map(function (byte) { return ("0" + ((byte + 256) % 256).toString(16)).slice(-2); }).join("");
}

function uuid_() { return Utilities.getUuid().replace(/-/g, ""); }
function now_() { return new Date().toISOString(); }
function utf8Length_(value) { return Utilities.newBlob(String(value)).getBytes().length; }
