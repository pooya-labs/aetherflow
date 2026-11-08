(() => {
  "use strict";

  const state = {
    base: "",
    token: "",
    scope: "operator",
    status: "",
    cursor: null,
    page: 0,
    runs: [],
    selectedId: "",
    selectedSnapshot: null,
    eventCursor: null,
    events: [],
    busy: false,
    connectionRevision: 0,
    selectionRevision: 0,
    taskRevision: 0,
    eventRequestRevision: 0,
    metricsRevision: 0,
    loadingOlderEvents: false,
    requestController: null
  };

  const $ = (id) => document.getElementById(id);
  const form = $("connectionForm");
  const runList = $("runList");
  const detail = $("detailContent");
  const themeButton = $("themeButton");
  const savedTheme = (() => {
    try {
      const value = localStorage.getItem("aetherflow-theme");
      return value === "light" || value === "dark" ? value : null;
    } catch (_) {
      return null;
    }
  })();
  if (savedTheme) document.documentElement.dataset.theme = savedTheme;

  function updateThemeButton() {
    const dark = document.documentElement.dataset.theme === "dark";
    themeButton.textContent = dark ? "◐" : "◑";
    themeButton.setAttribute("aria-label", dark ? "Switch to light theme" : "Switch to dark theme");
    themeButton.setAttribute("aria-pressed", String(!dark));
  }

  updateThemeButton();

  function setConnection(message, kind) {
    $("connectionStatus").textContent = message;
    $("connectionDot").className = "dot" + (kind ? " " + kind : "");
  }

  function setBusy(value) {
    state.busy = value;
    $("connectButton").disabled = value;
    // Keep disconnect available while loading so a slow API can be cancelled.
    $("disconnectButton").disabled = !state.token;
    $("refreshButton").disabled = value || !state.token;
    $("loadMore").disabled = value || !state.token || !state.cursor;
    $("applyFilter").disabled = value || !state.token;
    $("statusFilter").disabled = value || !state.token;
    $("workersButton").disabled = value || !state.token || state.scope !== "operator";
  }

  function isCurrentConnection(revision) {
    return revision === state.connectionRevision && Boolean(state.token);
  }

  function baseUrl(raw) {
    const parsed = new URL(raw);
    if (parsed.username || parsed.password || parsed.search || parsed.hash) {
      throw new Error("Use a base URL without credentials, query strings, or fragments.");
    }
    if (parsed.protocol !== "https:") {
      const localHosts = ["localhost", "127.0.0.1", "[::1]"];
      if (!(parsed.protocol === "http:" && localHosts.includes(parsed.hostname))) {
        throw new Error("Remote API endpoints must use HTTPS.");
      }
    }
    return parsed.href.replace(/\/+$/, "");
  }

  function routeRoot() {
    return state.scope === "tenant" ? "/api/v1/tenant" : "/api/v1";
  }

  function workflowCollection() {
    return routeRoot() + "/workflow-runs";
  }

  function taskRoute(taskRunId) {
    const prefix = state.scope === "tenant" ? "/api/v1/tenant/task-runs/" : "/api/v1/task-runs/";
    return prefix + encodeURIComponent(taskRunId);
  }

  async function api(path) {
    const bridge = window.AetherFlowAppsScriptBridge;
    if (bridge && bridge.isActive()) {
      const response = await bridge.request({ method: "GET", path: path });
      const payload = response.body;
      if (response.status < 200 || response.status >= 300) {
        const message = payload && payload.error && payload.error.message;
        throw new Error(message || ("API request failed with HTTP " + response.status + "."));
      }
      if (typeof payload === "string") {
        try { return JSON.parse(payload); }
        catch (_) { throw new Error("The API returned a non-JSON response."); }
      }
      return payload;
    }

    const response = await fetch(state.base + path, {
      method: "GET",
      headers: {
        "Accept": "application/json",
        "Authorization": "Bearer " + state.token
      },
      credentials: "omit",
      cache: "no-store",
      redirect: "error",
      signal: state.requestController ? state.requestController.signal : undefined
    });

    const raw = await response.text();
    let payload = null;
    if (raw) {
      try { payload = JSON.parse(raw); }
      catch (_) { throw new Error("The API returned a non-JSON response."); }
    }
    if (!response.ok) {
      const message = payload && payload.error && payload.error.message;
      throw new Error(message || ("API request failed with HTTP " + response.status + "."));
    }
    return payload;
  }

  async function apiText(path) {
    const bridge = window.AetherFlowAppsScriptBridge;
    if (bridge && bridge.isActive()) {
      const response = await bridge.request({ method: "GET", path: path });
      if (response.status < 200 || response.status >= 300) {
        const payload = response.body;
        const message = payload && payload.error && payload.error.message;
        throw new Error(message || ("API request failed with HTTP " + response.status + "."));
      }
      return typeof response.body === "string" ? response.body : JSON.stringify(response.body || {});
    }

    const response = await fetch(state.base + path, {
      method: "GET",
      headers: {
        "Accept": "text/plain",
        "Authorization": "Bearer " + state.token
      },
      credentials: "omit",
      cache: "no-store",
      redirect: "error",
      signal: state.requestController ? state.requestController.signal : undefined
    });
    const raw = await response.text();
    if (!response.ok) {
      let message = "";
      try {
        const payload = raw ? JSON.parse(raw) : null;
        message = payload && payload.error && payload.error.message;
      } catch (_) {}
      throw new Error(message || ("API request failed with HTTP " + response.status + "."));
    }
    return raw;
  }

  function readGauge(exposition, metricName, labels) {
    let selector = metricName;
    if (labels && Object.keys(labels).length) {
      const encoded = Object.entries(labels).map(([key, value]) => key + "=\"" + value + "\"").join(",");
      selector += "{" + encoded + "}";
    }
    const prefix = selector + " ";
    const line = exposition.split(/\r?\n/).find((item) => item.startsWith(prefix));
    if (!line) return null;
    const value = Number(line.slice(prefix.length).trim());
    return Number.isFinite(value) ? value : null;
  }

  function formatCount(value) {
    return value === null ? "—" : new Intl.NumberFormat().format(value);
  }

  function formatDuration(value) {
    if (value === null) return "—";
    if (value < 60) return value.toFixed(1) + " s";
    if (value < 3600) {
      const totalSeconds = Math.floor(value);
      const minutePart = Math.floor(totalSeconds / 60);
      const secondPart = totalSeconds % 60;
      return minutePart + "m " + secondPart + "s";
    }
    const totalMinutes = Math.floor(value / 60);
    const hourPart = Math.floor(totalMinutes / 60);
    const minutePart = totalMinutes % 60;
    return hourPart + "h " + minutePart + "m";
  }

  function setOperationalValue(id, value) {
    $(id).textContent = value;
  }

  function resetOperationalSnapshot(hide) {
    state.metricsRevision += 1;
    const panel = $("opsSnapshot");
    if (hide) panel.classList.add("hidden");
    else panel.classList.remove("hidden");
    $("opsSnapshotStatus").textContent = hide ? "Available only with a tenant credential." : "Loading tenant operational data…";
    for (const id of [
      "opsSuccess", "opsFailed", "opsCancelled", "opsTimedOut",
      "opsCompletionSamples", "opsP50", "opsP95", "opsP99",
      "opsOutboxPending", "opsOutboxReady", "opsOutboxLag", "opsOutboxExpired",
      "opsInboxPending", "opsInboxProcessing", "opsInboxAge", "opsInboxExpired",
      "opsTaskExpired"
    ]) setOperationalValue(id, "—");
  }

  function renderOperationalSnapshot(exposition) {
    const value = (name, labels) => readGauge(exposition, name, labels);
    const count = (name, labels) => formatCount(value(name, labels));
    const sampleCount = value("aetherflow_tenant_workflow_success_completion_samples_last_30m");
    const duration = (name) => sampleCount > 0
      ? formatDuration(value(name))
      : "No samples";

    setOperationalValue("opsSuccess", count("aetherflow_tenant_workflow_completions_last_30m", { status: "succeeded" }));
    setOperationalValue("opsFailed", count("aetherflow_tenant_workflow_completions_last_30m", { status: "failed" }));
    setOperationalValue("opsCancelled", count("aetherflow_tenant_workflow_completions_last_30m", { status: "cancelled" }));
    setOperationalValue("opsTimedOut", count("aetherflow_tenant_workflow_completions_last_30m", { status: "timed_out" }));
    setOperationalValue("opsCompletionSamples", formatCount(sampleCount));
    setOperationalValue("opsP50", duration("aetherflow_tenant_workflow_success_completion_p50_seconds_last_30m"));
    setOperationalValue("opsP95", duration("aetherflow_tenant_workflow_success_completion_p95_seconds_last_30m"));
    setOperationalValue("opsP99", duration("aetherflow_tenant_workflow_success_completion_p99_seconds_last_30m"));
    setOperationalValue("opsOutboxPending", count("aetherflow_tenant_outbox_events", { state: "pending" }));
    setOperationalValue("opsOutboxReady", count("aetherflow_tenant_outbox_events", { state: "ready" }));
    setOperationalValue("opsOutboxLag", formatDuration(value("aetherflow_tenant_outbox_oldest_ready_lag_seconds")));
    setOperationalValue("opsOutboxExpired", count("aetherflow_tenant_outbox_expired_claims"));
    setOperationalValue("opsInboxPending", count("aetherflow_tenant_inbox_events", { state: "pending" }));
    setOperationalValue("opsInboxProcessing", count("aetherflow_tenant_inbox_events", { state: "processing" }));
    setOperationalValue("opsInboxAge", formatDuration(value("aetherflow_tenant_inbox_oldest_unprocessed_age_seconds")));
    setOperationalValue("opsInboxExpired", count("aetherflow_tenant_inbox_expired_processing_leases"));
    setOperationalValue("opsTaskExpired", count("aetherflow_tenant_task_expired_running_leases"));
  }

  async function loadOperationalSnapshot(connectionRevision) {
    if (!state.token || state.scope !== "tenant") {
      resetOperationalSnapshot(true);
      return;
    }
    const metricsRevision = ++state.metricsRevision;
    $("opsSnapshot").classList.remove("hidden");
    $("opsSnapshotStatus").textContent = "Loading tenant operational data…";
    try {
      const exposition = await apiText("/api/v1/tenant/metrics");
      if (!isCurrentConnection(connectionRevision) || metricsRevision !== state.metricsRevision) return;
      renderOperationalSnapshot(exposition);
      $("opsSnapshotStatus").textContent = "Tenant-scoped · acceptance to terminal (queue included) · refreshed " + new Date().toLocaleTimeString();
    } catch (error) {
      if (!isCurrentConnection(connectionRevision) || metricsRevision !== state.metricsRevision) return;
      $("opsSnapshotStatus").textContent = "Snapshot unavailable · " + error.message;
      for (const id of [
        "opsSuccess", "opsFailed", "opsCancelled", "opsTimedOut",
        "opsCompletionSamples", "opsP50", "opsP95", "opsP99",
        "opsOutboxPending", "opsOutboxReady", "opsOutboxLag", "opsOutboxExpired",
        "opsInboxPending", "opsInboxProcessing", "opsInboxAge", "opsInboxExpired",
        "opsTaskExpired"
      ]) setOperationalValue(id, "—");
    }
  }

  function formatDate(value) {
    if (!value) return "—";
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
  }

  function pill(status) {
    const el = document.createElement("span");
    el.className = "pill " + String(status || "unknown").toLowerCase().replace(/[^a-z0-9_-]/g, "");
    el.textContent = status || "unknown";
    return el;
  }

  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined && text !== null) node.textContent = String(text);
    return node;
  }

  function updateStats() {
    const counts = { running: 0, succeeded: 0, attention: 0 };
    for (const run of state.runs) {
      if (run.status === "running") counts.running++;
      if (run.status === "succeeded") counts.succeeded++;
      if (run.status === "failed" || run.status === "timed_out") counts.attention++;
    }
    $("statTotal").textContent = String(state.runs.length);
    $("statRunning").textContent = String(counts.running);
    $("statSucceeded").textContent = String(counts.succeeded);
    $("statAttention").textContent = String(counts.attention);
    $("pageStatus").textContent = "Page " + state.page + " · " + state.runs.length + " loaded";
  }

  function renderRuns() {
    runList.replaceChildren();
    if (!state.runs.length) {
      runList.append(element("div", "empty", "No workflow runs matched this view."));
      updateStats();
      return;
    }

    for (const run of state.runs) {
      const button = element("button", "run-item" + (run.workflow_run_id === state.selectedId ? " active" : ""));
      button.type = "button";
      button.addEventListener("click", () => selectRun(run.workflow_run_id));

      const top = element("div", "run-top");
      top.append(element("span", "run-id", run.workflow_run_id), pill(run.status));
      button.append(top);
      button.append(element("div", "run-workflow", run.workflow_id));
      const meta = element("div", "run-meta");
      const taskCount = Number.isInteger(run.task_count) && run.task_count >= 0 ? run.task_count : null;
      const version = Number.isInteger(run.version) && run.version >= 0 ? "v" + run.version : null;
      const createdAt = formatDate(run.created_at);
      if (taskCount !== null) meta.append(element("span", "", taskCount + (taskCount === 1 ? " task" : " tasks")));
      if (version) meta.append(element("span", "", version));
      if (createdAt !== "—") meta.append(element("span", "", createdAt));
      button.append(meta);
      runList.append(button);
    }
    updateStats();
  }

  function detailCell(label, value) {
    const cell = element("div", "detail-cell");
    cell.append(element("small", "", label), element("b", "", value));
    return cell;
  }

  function renderDetail(snapshot, events) {
    detail.replaceChildren();
    const head = element("div", "detail-head");
    const top = element("div", "run-top");
    top.append(element("div", "detail-title", snapshot.workflow_run_id), pill(snapshot.status));
    head.append(top, element("div", "detail-subtitle", snapshot.workflow_id));
    detail.append(head);

    const grid = element("div", "detail-grid");
    grid.append(
      detailCell("Version", snapshot.version),
      detailCell("Tasks", snapshot.task_runs.length),
      detailCell("Created", formatDate(snapshot.created_at)),
      detailCell("Updated", formatDate(snapshot.updated_at))
    );
    detail.append(grid);

    const tasksSection = element("div", "subsection");
    const tasksHead = element("div", "subsection-head");
    tasksHead.append(element("h3", "", "Task runs"), element("span", "small subtle", snapshot.task_runs.length + " tasks"));
    tasksSection.append(tasksHead);
    const tasks = element("div", "task-list");

    if (!snapshot.task_runs.length) {
      tasks.append(element("div", "empty", "No task runs in this workflow."));
    }
    for (const task of snapshot.task_runs) {
      const row = element("div", "task-row");
      const left = element("div");
      left.append(element("div", "task-id", task.task_id), element("div", "small subtle", task.task_run_id));
      const inspect = element("button", "", "Inspect");
      inspect.type = "button";
      inspect.addEventListener("click", () => inspectTask(task.task_run_id));
      const right = element("div", "row");
      right.append(pill(task.status), inspect);
      row.append(left, right);
      tasks.append(row);
    }
    tasksSection.append(tasks);
    detail.append(tasksSection);

    const eventsSection = element("div", "subsection");
    const eventsHead = element("div", "subsection-head");
    eventsHead.append(element("h3", "", "Event timeline"), element("span", "small subtle", events.length + " events"));
    eventsSection.append(eventsHead);
    const eventList = element("div", "event-list");
    if (!events.length) eventList.append(element("div", "empty", "No event records for this workflow yet."));
    for (const event of events) {
      const item = element("div", "event-row");
      item.append(
        element("div", "event-name", event.event_type + " · " + event.aggregate_type),
        element("div", "event-meta", "#" + event.event_id + " · " + event.aggregate_id),
        element("div", "event-meta", formatDate(event.created_at) + (event.published_at ? " · Published " + formatDate(event.published_at) : " · Awaiting publish"))
      );
      if (event.status) item.append(pill(event.status));
      eventList.append(item);
    }
    eventsSection.append(eventList);
    if (state.eventCursor) {
      const older = element("button", "btn ghost", "Load older events");
      older.type = "button";
      older.style.marginTop = "10px";
      older.addEventListener("click", loadOlderEvents);
      eventsSection.append(older);
    }
    detail.append(eventsSection);
  }

  async function loadRuns(append) {
    if (!state.token) return;
    const connectionRevision = state.connectionRevision;
    if (!append) {
      resetOperationalSnapshot(state.scope !== "tenant");
      state.selectionRevision += 1;
      state.taskRevision += 1;
      state.eventRequestRevision += 1;
      state.loadingOlderEvents = false;
      state.cursor = null;
      state.page = 0;
      state.runs = [];
      state.selectedId = "";
      state.selectedSnapshot = null;
      state.eventCursor = null;
      state.events = [];
      detail.className = "detail-empty";
      detail.textContent = "Select a workflow run to inspect its tasks and event timeline.";
    }

    const params = new URLSearchParams();
    params.set("limit", "25");
    if (state.status) params.set("status", state.status);
    if (append && state.cursor) params.set("cursor", state.cursor);

    setBusy(true);
    setConnection("Loading workflow runs…", "");
    try {
      const page = await api(workflowCollection() + "?" + params.toString());
      if (!isCurrentConnection(connectionRevision)) return;
      const items = Array.isArray(page.items) ? page.items : [];
      state.runs = append ? state.runs.concat(items) : items;
      state.cursor = page.next_cursor || null;
      state.page += 1;
      renderRuns();
      $("loadMore").disabled = !state.cursor;
      $("welcomeText").textContent = "Connected to " + state.base + " · " + (state.scope === "tenant" ? "Tenant view" : "Operator view");
      $("scopePill").textContent = state.scope === "tenant" ? "Tenant" : "Operator";
      setConnection("Connected · live data", "good");
      if (!append) {
        const metricsRequest = loadOperationalSnapshot(connectionRevision);
        const detailRequest = state.runs.length
          ? selectRun(state.runs[0].workflow_run_id)
          : Promise.resolve();
        await Promise.all([metricsRequest, detailRequest]);
      }
    } catch (error) {
      if (!isCurrentConnection(connectionRevision)) return;
      setConnection(error.message, "bad");
      if (!append) {
        runList.replaceChildren(element("div", "empty error", error.message));
        updateStats();
      }
    } finally {
      if (isCurrentConnection(connectionRevision)) setBusy(false);
    }
  }

  async function selectRun(id) {
    if (!state.token) return;
    const connectionRevision = state.connectionRevision;
    const selectionRevision = ++state.selectionRevision;
    state.taskRevision += 1;
    state.eventRequestRevision += 1;
    state.loadingOlderEvents = false;
    const isCurrentSelection = () =>
      isCurrentConnection(connectionRevision) &&
      selectionRevision === state.selectionRevision &&
      state.selectedId === id;
    state.selectedId = id;
    renderRuns();
    detail.className = "";
    detail.replaceChildren(element("div", "empty", "Loading run details and event history…"));
    try {
      const root = workflowCollection() + "/" + encodeURIComponent(id);
      const snapshot = await api(root);
      if (!isCurrentSelection()) return;
      const eventsPage = await api(root + "/events?limit=50");
      if (!isCurrentSelection()) return;
      state.selectedSnapshot = snapshot;
      state.eventCursor = eventsPage.next_cursor || null;
      state.events = Array.isArray(eventsPage.events) ? eventsPage.events : [];
      renderDetail(snapshot, state.events);
    } catch (error) {
      if (isCurrentSelection()) {
        detail.replaceChildren(element("div", "empty error", error.message));
      }
    }
  }

  async function loadOlderEvents() {
    if (!state.token || !state.eventCursor || !state.selectedId || !state.selectedSnapshot || state.loadingOlderEvents) return;
    const connectionRevision = state.connectionRevision;
    const selectionRevision = state.selectionRevision;
    const eventRequestRevision = ++state.eventRequestRevision;
    const selectedId = state.selectedId;
    const isCurrentEventLoad = () =>
      isCurrentConnection(connectionRevision) &&
      selectionRevision === state.selectionRevision &&
      eventRequestRevision === state.eventRequestRevision &&
      selectedId === state.selectedId;
    state.loadingOlderEvents = true;
    const root = workflowCollection() + "/" + encodeURIComponent(selectedId) + "/events";
    const params = new URLSearchParams();
    params.set("limit", "50");
    params.set("cursor", state.eventCursor);
    try {
      const page = await api(root + "?" + params.toString());
      if (!isCurrentEventLoad()) return;
      state.events = state.events.concat(Array.isArray(page.events) ? page.events : []);
      state.eventCursor = page.next_cursor || null;
      renderDetail(state.selectedSnapshot, state.events);
    } catch (error) {
      if (isCurrentEventLoad()) detail.append(element("div", "empty error", error.message));
    } finally {
      if (eventRequestRevision === state.eventRequestRevision) state.loadingOlderEvents = false;
    }
  }

  async function inspectTask(id) {
    const connectionRevision = state.connectionRevision;
    const selectionRevision = state.selectionRevision;
    const taskRevision = ++state.taskRevision;
    const selectedId = state.selectedId;
    const isCurrentTask = () =>
      isCurrentConnection(connectionRevision) &&
      selectionRevision === state.selectionRevision &&
      taskRevision === state.taskRevision &&
      selectedId === state.selectedId;
    detail.replaceChildren(element("div", "empty", "Loading task snapshot…"));
    try {
      const task = await api(taskRoute(id));
      if (!isCurrentTask()) return;
      detail.replaceChildren();
      const head = element("div", "detail-head");
      head.append(element("div", "detail-title", task.task_run_id), element("div", "detail-subtitle", task.task_id));
      detail.append(head);
      const grid = element("div", "detail-grid");
      grid.append(
        detailCell("Status", task.status),
        detailCell("Version", task.version),
        detailCell("Attempts", task.attempts_started),
        detailCell("Input size", task.input_size_bytes + " bytes"),
        detailCell("Output", task.output_present ? (task.output_size_bytes || 0) + " bytes" : "None"),
        detailCell("Available", formatDate(task.available_at)),
        detailCell("Claimed by", task.claimed_by || "—"),
        detailCell("Lease expires", formatDate(task.lease_expires_at))
      );
      detail.append(grid);
      const back = element("button", "btn ghost");
      back.type = "button";
      back.textContent = "← Back to workflow";
      back.style.marginTop = "15px";
      back.addEventListener("click", () => selectRun(state.selectedId));
      detail.append(back);
    } catch (error) {
      if (isCurrentTask()) detail.replaceChildren(element("div", "empty error", error.message));
    }
  }

  async function loadWorkers() {
    if (!state.token || state.scope !== "operator") return;
    const connectionRevision = state.connectionRevision;
    $("workersPanel").classList.remove("hidden");
    $("workerList").replaceChildren(element("div", "empty", "Loading active workers…"));
    try {
      const data = await api("/api/v1/workers");
      if (!isCurrentConnection(connectionRevision)) return;
      const workers = Array.isArray(data.workers) ? data.workers : [];
      $("workerList").replaceChildren();
      if (!workers.length) $("workerList").append(element("div", "empty", "No active worker registrations."));
      for (const worker of workers) {
        const row = element("div", "worker-row");
        const info = element("div");
        info.append(element("b", "", worker.worker_id), element("br"), element("small", "", worker.node_id + " · epoch " + worker.membership_epoch));
        row.append(info, pill("active"));
        $("workerList").append(row);
      }
    } catch (error) {
      if (isCurrentConnection(connectionRevision)) {
        $("workerList").replaceChildren(element("div", "empty error", error.message));
      }
    }
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const nextBase = baseUrl($("apiBase").value.trim());
      const nextToken = $("apiToken").value.trim();
      const nextScope = $("scope").value;
      if (!nextToken) throw new Error("Paste a read-capable Bearer token.");
      if (!/^[A-Za-z0-9_-]{32,256}$/.test(nextToken)) throw new Error("Token format must be 32–256 URL-safe characters.");

      state.requestController?.abort();
      state.requestController = new AbortController();
      state.connectionRevision += 1;
      state.selectionRevision += 1;
      state.taskRevision += 1;
      state.eventRequestRevision += 1;
      state.loadingOlderEvents = false;
      state.base = nextBase;
      state.token = nextToken;
      state.scope = nextScope;
      state.status = $("statusFilter").value;
      $("apiToken").value = "";
      await loadRuns(false);
    } catch (error) {
      setConnection(error.message, "bad");
    }
  });

  $("disconnectButton").addEventListener("click", () => {
    state.connectionRevision += 1;
    state.selectionRevision += 1;
    state.taskRevision += 1;
    state.eventRequestRevision += 1;
    state.loadingOlderEvents = false;
    resetOperationalSnapshot(true);
    state.requestController?.abort();
    state.requestController = null;
    state.token = "";
    state.base = "";
    state.cursor = null;
    state.page = 0;
    state.runs = [];
    state.selectedId = "";
    state.selectedSnapshot = null;
    state.eventCursor = null;
    state.events = [];
    $("apiToken").value = "";
    $("apiBase").value = "";
    runList.replaceChildren(element("div", "empty", "Disconnected. No credential is retained."));
    detail.className = "detail-empty";
    detail.textContent = "Select a workflow run to inspect its tasks and event timeline.";
    $("workersPanel").classList.add("hidden");
    $("scopePill").textContent = "Disconnected";
    $("welcomeText").textContent = "Connect to your AetherFlow control plane to load live runs and event history.";
    $("statTotal").textContent = $("statRunning").textContent = $("statSucceeded").textContent = $("statAttention").textContent = "—";
    setBusy(false);
    $("disconnectButton").disabled = true;
    setConnection("Not connected", "");
  });

  $("refreshButton").addEventListener("click", () => loadRuns(false));
  $("applyFilter").addEventListener("click", () => {
    state.status = $("statusFilter").value;
    loadRuns(false);
  });
  $("loadMore").addEventListener("click", () => loadRuns(true));
  $("workersButton").addEventListener("click", loadWorkers);
  $("closeWorkers").addEventListener("click", () => $("workersPanel").classList.add("hidden"));
  themeButton.addEventListener("click", () => {
    const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    try { localStorage.setItem("aetherflow-theme", next); } catch (_) {}
    updateThemeButton();
  });

  async function connectAppsScriptDemo() {
    const bridge = window.AetherFlowAppsScriptBridge;
    if (!bridge || !bridge.isActive()) return;

    form.classList.add("hidden");
    $("disconnectButton").classList.add("hidden");
    $("connectionTitle").textContent = "Demo workspace";
    $(".connection-copy")?.replaceChildren(document.createTextNode("Connected through the owner-managed Apps Script runtime. Each browser receives a separate synthetic workspace."));
    const securityNote = document.querySelector(".security-note");
    if (securityNote) securityNote.textContent = "Private demo session. The short-lived session capability stays inside the bridge and is never stored in the dashboard URL, cookies, or local storage.";
    const notice = document.querySelector(".notice span:last-child");
    if (notice) notice.textContent = "Apps Script demo mode. Workflow records are synthetic and stored in the owner's private Google Spreadsheet. No real task code is executed, and no NATS, WASM, or production telemetry is connected.";
    const footer = document.querySelector(".footer");
    if (footer && footer.children.length > 1) {
      footer.children[1].textContent = "Apps Script + private Sheet · Synthetic demo data · Session-only capability";
    }
    setConnection("Connecting to the Apps Script demo…", "");
    try {
      await bridge.start();
      state.connectionRevision += 1;
      state.base = "Google Apps Script demo";
      state.token = "apps-script-session";
      state.scope = "operator";
      state.status = "";
      setBusy(false);
      await loadRuns(false);
    } catch (error) {
      state.token = "";
      setConnection(error.message, "bad");
    }
  }

  connectAppsScriptDemo();
})();
