(function (root) {
  "use strict";

  const IDENTIFIER = /^[A-Za-z0-9_-]{1,128}$/;
  const WORKFLOW_STATES = new Set(["pending", "running", "succeeded", "failed", "cancelled", "timed_out"]);
  const TASK_STATES = new Set([
    "pending", "ready", "running", "retry_waiting",
    "succeeded", "failed", "cancelled", "timed_out"
  ]);
  const TERMINAL = new Set(["succeeded", "failed", "cancelled", "timed_out"]);

  function isIdentifier(value) {
    return typeof value === "string" && IDENTIFIER.test(value);
  }

  function validateBase64(value) {
    if (value === undefined || value === null || value === "") return { value: "", bytes: 0 };
    if (typeof value !== "string" || value.length > 22000) throw new Error("invalid_base64");
    if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) {
      throw new Error("invalid_base64");
    }
    const padding = value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0;
    return { value, bytes: Math.max(0, Math.floor(value.length * 3 / 4) - padding) };
  }

  function validateWorkflowSubmission(input, options) {
    const config = Object.assign({ maxTasks: 20, maxBodyBytes: 16384, maxTaskBytes: 8192 }, options || {});
    if (!input || typeof input !== "object" || Array.isArray(input)) throw new Error("invalid_request");
    if (!isIdentifier(input.workflow_id) || !isIdentifier(input.workflow_run_id)) throw new Error("invalid_identifier");
    if (!Array.isArray(input.tasks) || input.tasks.length < 1 || input.tasks.length > config.maxTasks) {
      throw new Error("invalid_workflow");
    }
    const retry = input.retry_policy;
    if (!retry || !Number.isSafeInteger(retry.max_attempts) || retry.max_attempts < 1 || retry.max_attempts > 10 ||
        !Number.isSafeInteger(retry.initial_backoff_ms) || retry.initial_backoff_ms < 0 ||
        !Number.isSafeInteger(retry.max_backoff_ms) || retry.max_backoff_ms < retry.initial_backoff_ms ||
        retry.max_backoff_ms > 86400000) {
      throw new Error("invalid_retry_policy");
    }

    const ids = new Set();
    const tasks = input.tasks.map((task) => {
      if (!task || typeof task !== "object" || Array.isArray(task) || !isIdentifier(task.task_id)) {
        throw new Error("invalid_identifier");
      }
      if (ids.has(task.task_id)) throw new Error("duplicate_task");
      ids.add(task.task_id);
      const dependencies = task.depends_on === undefined ? [] : task.depends_on;
      if (!Array.isArray(dependencies) || dependencies.length > config.maxTasks) throw new Error("invalid_dependencies");
      const seen = new Set();
      for (const dependency of dependencies) {
        if (!isIdentifier(dependency)) throw new Error("invalid_identifier");
        if (dependency === task.task_id || seen.has(dependency)) throw new Error("invalid_dependencies");
        seen.add(dependency);
      }
      const encoded = validateBase64(task.input_base64);
      if (encoded.bytes > config.maxTaskBytes) throw new Error("task_payload_too_large");
      return {
        task_id: task.task_id,
        depends_on: dependencies.slice(),
        input_base64: encoded.value,
        input_size_bytes: encoded.bytes
      };
    });

    for (const task of tasks) {
      for (const dependency of task.depends_on) {
        if (!ids.has(dependency)) throw new Error("missing_dependency");
      }
    }

    const indegree = new Map();
    const children = new Map();
    for (const task of tasks) {
      indegree.set(task.task_id, task.depends_on.length);
      for (const dependency of task.depends_on) {
        if (!children.has(dependency)) children.set(dependency, []);
        children.get(dependency).push(task.task_id);
      }
    }
    for (const list of children.values()) list.sort();
    const ready = Array.from(indegree.entries()).filter((entry) => entry[1] === 0).map((entry) => entry[0]).sort();
    const order = [];
    while (ready.length) {
      const current = ready.shift();
      order.push(current);
      for (const child of children.get(current) || []) {
        const next = indegree.get(child) - 1;
        indegree.set(child, next);
        if (next === 0) {
          ready.push(child);
          ready.sort();
        }
      }
    }
    if (order.length !== tasks.length) throw new Error("workflow_cycle");

    if (JSON.stringify(input).length > config.maxBodyBytes) throw new Error("request_too_large");
    return {
      workflow_id: input.workflow_id,
      workflow_run_id: input.workflow_run_id,
      retry_policy: {
        max_attempts: retry.max_attempts,
        initial_backoff_ms: retry.initial_backoff_ms,
        max_backoff_ms: retry.max_backoff_ms
      },
      tasks,
      execution_order: order
    };
  }

  function retryDelay(policy, attemptsStarted) {
    const initial = Math.max(0, Number(policy && policy.initial_backoff_ms) || 0);
    const maximum = Math.max(initial, Number(policy && policy.max_backoff_ms) || 0);
    const attempt = Math.max(1, Math.floor(Number(attemptsStarted) || 1));
    if (initial === 0 || maximum === 0) return 0;
    let delay = initial;
    for (let index = 1; index < attempt && delay < maximum; index += 1) {
      delay = Math.min(maximum, delay > maximum / 2 ? maximum : delay * 2);
    }
    return Math.min(maximum, delay);
  }

  function transitionTask(task, nextStatus, expectedVersion, now) {
    if (!task || !TASK_STATES.has(task.status) || !TASK_STATES.has(nextStatus)) throw new Error("invalid_task_state");
    if (task.version !== expectedVersion) throw new Error("version_conflict");
    if (TERMINAL.has(task.status)) throw new Error("terminal_state");
    const allowed = {
      pending: new Set(["ready", "cancelled", "timed_out"]),
      ready: new Set(["running", "cancelled", "timed_out"]),
      running: new Set(["succeeded", "retry_waiting", "failed", "cancelled", "timed_out"]),
      retry_waiting: new Set(["ready", "cancelled", "timed_out"])
    };
    if (!allowed[task.status] || !allowed[task.status].has(nextStatus)) throw new Error("invalid_transition");
    const stamp = now || new Date().toISOString();
    const result = Object.assign({}, task, {
      status: nextStatus,
      version: task.version + 1,
      updated_at: stamp
    });
    if (nextStatus === "running" && !result.started_at) result.started_at = stamp;
    if (TERMINAL.has(nextStatus)) result.finished_at = stamp;
    return result;
  }

  function workflowStatus(tasks) {
    if (!Array.isArray(tasks) || !tasks.length) throw new Error("empty_workflow");
    const statuses = tasks.map((task) => task.status);
    if (statuses.some((status) => status === "failed")) return "failed";
    if (statuses.some((status) => status === "timed_out")) return "timed_out";
    if (statuses.every((status) => status === "succeeded")) return "succeeded";
    if (statuses.every((status) => TERMINAL.has(status))) return "cancelled";
    if (statuses.some((status) => status === "running" || status === "ready")) return "running";
    return "pending";
  }

  root.AetherFlowCore = Object.freeze({
    isIdentifier,
    validateBase64,
    validateWorkflowSubmission,
    retryDelay,
    transitionTask,
    workflowStatus,
    terminalStates: Object.freeze(Array.from(TERMINAL))
  });
})(globalThis);
