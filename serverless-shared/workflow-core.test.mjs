import assert from "node:assert/strict";
import test from "node:test";

import "./workflow-core.js";

const core = globalThis.AetherFlowCore;

const sample = () => ({
  workflow_id: "invoice-flow",
  workflow_run_id: "invoice-run-01",
  retry_policy: { max_attempts: 3, initial_backoff_ms: 100, max_backoff_ms: 500 },
  tasks: [
    { task_id: "charge", depends_on: [], input_base64: "e30=" },
    { task_id: "receipt", depends_on: ["charge"] },
    { task_id: "notify", depends_on: ["receipt"] }
  ]
});

test("workflow validation computes a deterministic topological order", () => {
  const result = core.validateWorkflowSubmission(sample());
  assert.deepEqual(result.execution_order, ["charge", "receipt", "notify"]);
});

test("workflow validation rejects duplicate, missing, self and cyclic dependencies", () => {
  const duplicate = sample();
  duplicate.tasks.push({ task_id: "charge" });
  assert.throws(() => core.validateWorkflowSubmission(duplicate), /duplicate_task/);

  const missing = sample();
  missing.tasks[1].depends_on = ["unknown"];
  assert.throws(() => core.validateWorkflowSubmission(missing), /missing_dependency/);

  const self = sample();
  self.tasks[0].depends_on = ["charge"];
  assert.throws(() => core.validateWorkflowSubmission(self), /invalid_dependencies/);

  const cyclic = sample();
  cyclic.tasks[0].depends_on = ["notify"];
  assert.throws(() => core.validateWorkflowSubmission(cyclic), /workflow_cycle/);
});

test("workflow validation enforces retry policy, identifiers and bounded inputs", () => {
  const bad = sample();
  bad.retry_policy.max_backoff_ms = 10;
  assert.throws(() => core.validateWorkflowSubmission(bad), /invalid_retry_policy/);

  const large = sample();
  large.tasks[0].input_base64 = "A".repeat(12000);
  assert.throws(() => core.validateWorkflowSubmission(large), /task_payload_too_large/);

  const malformed = sample();
  malformed.tasks[0].input_base64 = "not base64";
  assert.throws(() => core.validateWorkflowSubmission(malformed), /invalid_base64/);
});

test("retry delay doubles but remains bounded", () => {
  const policy = { initial_backoff_ms: 100, max_backoff_ms: 350 };
  assert.equal(core.retryDelay(policy, 1), 100);
  assert.equal(core.retryDelay(policy, 2), 200);
  assert.equal(core.retryDelay(policy, 3), 350);
  assert.equal(core.retryDelay(policy, 100), 350);
});

test("task transitions enforce optimistic versions and terminal absorption", () => {
  const task = { status: "pending", version: 0, created_at: "2026-01-01T00:00:00Z" };
  const ready = core.transitionTask(task, "ready", 0, "2026-01-01T00:00:01Z");
  assert.equal(ready.version, 1);
  const running = core.transitionTask(ready, "running", 1, "2026-01-01T00:00:02Z");
  const succeeded = core.transitionTask(running, "succeeded", 2, "2026-01-01T00:00:03Z");
  assert.equal(succeeded.finished_at, "2026-01-01T00:00:03Z");
  assert.throws(() => core.transitionTask(succeeded, "ready", 3), /terminal_state/);
  assert.throws(() => core.transitionTask(ready, "running", 0), /version_conflict/);
});

test("workflow status follows terminal and active task states", () => {
  assert.equal(core.workflowStatus([{ status: "ready" }, { status: "pending" }]), "running");
  assert.equal(core.workflowStatus([{ status: "succeeded" }, { status: "succeeded" }]), "succeeded");
  assert.equal(core.workflowStatus([{ status: "failed" }, { status: "cancelled" }]), "failed");
  assert.equal(core.workflowStatus([{ status: "succeeded" }, { status: "cancelled" }]), "cancelled");
  assert.equal(core.workflowStatus([{ status: "pending" }, { status: "pending" }]), "pending");
});
