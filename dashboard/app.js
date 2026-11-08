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
    busy: false
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
    $("disconnectButton").disabled = value || !state.token;
    $("refreshButton").disabled = value || !state.token;
    $("loadMore").disabled = value || !state.token || !state.cursor;
    $("applyFilter").disabled = value || !state.token;
    $("statusFilter").disabled = value || !state.token;
    $("workersButton").disabled = value || !state.token || state.scope !== "operator";
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
    const response = await fetch(state.base + path, {
      method: "GET",
      headers: {
        "Accept": "application/json",
        "Authorization": "Bearer " + state.token
      },
      credentials: "omit",
      cache: "no-store",
      redirect: "error"
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
      meta.append(
        element("span", "", run.task_count + " tasks"),
        element("span", "", "v" + run.version),
        element("span", "", formatDate(run.created_at))
      );
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
    if (!append) {
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
      const items = Array.isArray(page.items) ? page.items : [];
      state.runs = append ? state.runs.concat(items) : items;
      state.cursor = page.next_cursor || null;
      state.page += 1;
      renderRuns();
      $("loadMore").disabled = !state.cursor;
      $("welcomeText").textContent = "Connected to " + state.base + " · " + (state.scope === "tenant" ? "Tenant view" : "Operator view");
      $("scopePill").textContent = state.scope === "tenant" ? "Tenant" : "Operator";
      setConnection("Connected · live data", "good");
      if (!append && state.runs.length) await selectRun(state.runs[0].workflow_run_id);
    } catch (error) {
      setConnection(error.message, "bad");
      if (!append) {
        runList.replaceChildren(element("div", "empty error", error.message));
        updateStats();
      }
    } finally {
      setBusy(false);
    }
  }

  async function selectRun(id) {
    if (!state.token) return;
    state.selectedId = id;
    renderRuns();
    detail.className = "";
    detail.replaceChildren(element("div", "empty", "Loading run details and event history…"));
    try {
      const root = workflowCollection() + "/" + encodeURIComponent(id);
      const snapshot = await api(root);
      const eventsPage = await api(root + "/events?limit=50");
      state.selectedSnapshot = snapshot;
      state.eventCursor = eventsPage.next_cursor || null;
      state.events = Array.isArray(eventsPage.events) ? eventsPage.events : [];
      renderDetail(snapshot, state.events);
    } catch (error) {
      detail.replaceChildren(element("div", "empty error", error.message));
    }
  }

  async function loadOlderEvents() {
    if (!state.token || !state.eventCursor || !state.selectedId || !state.selectedSnapshot) return;
    const root = workflowCollection() + "/" + encodeURIComponent(state.selectedId) + "/events";
    const params = new URLSearchParams();
    params.set("limit", "50");
    params.set("cursor", state.eventCursor);
    try {
      const page = await api(root + "?" + params.toString());
      state.events = state.events.concat(Array.isArray(page.events) ? page.events : []);
      state.eventCursor = page.next_cursor || null;
      renderDetail(state.selectedSnapshot, state.events);
    } catch (error) {
      detail.append(element("div", "empty error", error.message));
    }
  }

  async function inspectTask(id) {
    detail.replaceChildren(element("div", "empty", "Loading task snapshot…"));
    try {
      const task = await api(taskRoute(id));
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
      detail.replaceChildren(element("div", "empty error", error.message));
    }
  }

  async function loadWorkers() {
    if (!state.token || state.scope !== "operator") return;
    $("workersPanel").classList.remove("hidden");
    $("workerList").replaceChildren(element("div", "empty", "Loading active workers…"));
    try {
      const data = await api("/api/v1/workers");
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
      $("workerList").replaceChildren(element("div", "empty error", error.message));
    }
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      state.base = baseUrl($("apiBase").value.trim());
      state.token = $("apiToken").value.trim();
      state.scope = $("scope").value;
      state.status = $("statusFilter").value;
      if (!state.token) throw new Error("Paste a read-capable Bearer token.");
      if (!/^[A-Za-z0-9_-]{32,256}$/.test(state.token)) throw new Error("Token format must be 32–256 URL-safe characters.");
      $("apiToken").value = "";
      await loadRuns(false);
    } catch (error) {
      setConnection(error.message, "bad");
    }
  });

  $("disconnectButton").addEventListener("click", () => {
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
})();
