# ADR-0005: Workflow DAG orchestration

- **Status:** Accepted; implementation hosted-verified in run `41356396516`
- **Date:** 2026-11-02
- **Scope:** Process-local workflow definition validation and dependency-aware task activation

## Context

The bounded scheduler executes task runs, but a workflow must ensure that downstream tasks are not eligible until every prerequisite has succeeded. Workflow definitions also need validation before any task-run records are created.

## Decisions

### Definition validation

A workflow definition contains a typed WorkflowId and a non-empty set of WorkflowTask entries. Each task declares typed TaskId dependencies. Construction rejects duplicate task IDs, self-dependencies, duplicate dependency declarations, missing references, and cycles. A deterministic Kahn topological sort uses lexicographical TaskId ordering to break ties.

### Activation semantics

A WorkflowTask may declare one static opaque TaskInput payload. The coordinator copies it into the task-run record when that task becomes eligible. A workflow execution inserts only root task runs initially. A dependent task run is materialized after all its dependencies have reached Succeeded. Tasks whose dependencies never succeed are never inserted into the repository. The existing TaskScheduler worker loop handles each activated run and its local retry policy.

### Workflow lifecycle aggregation

The workflow begins Pending and transitions to Running. When every task succeeds, the workflow becomes Succeeded. The first observed non-success terminal task outcome, ordered by task updated time and TaskId for ties, determines whether the workflow becomes Failed, TimedOut, or Cancelled. Remaining materialized non-terminal tasks are cancelled with expected-version checks.

External shutdown requests cancel the workflow and its materialized non-terminal tasks. Cancellation is propagated through each active task's cooperative TaskCancellation token. The worker loop then stops claiming and drains in-flight handlers. A handler can perform cleanup when it observes the token, but cancellation does not prove an external side effect was stopped or undone.

### Identifier generation

TaskRunIdGenerator is injected into workflow execution. SequenceTaskRunIdGenerator is a thread-safe process-local implementation for the in-memory runtime. Callers should reuse one generator for all workflow executions sharing the repository. It is not durable and must be replaced by a persistent generator/allocator when durable storage is introduced.

### Explicit limitations

Workflow-run lifecycle state is held in the execution call and returned as a terminal WorkflowExecutionResult; it is not persisted. Automatic predecessor-output mapping, templating, and transformation are not part of this baseline; applications own any mapping needed by handlers. Handler futures do not yet receive cooperative cancellation tokens; cancellation only commits orchestration state and invalidates later stale completion. Shutdown has no forced drain deadline. Workflow task input/output, workflow deadlines, compensation, distributed workers, durable state transitions, and lease/fencing are out of scope for M03.

## Consequences

- A validated DAG cannot deadlock due to a declared cycle or missing dependency.
- Downstream work is not visible to workers until all prerequisites succeed.
- Workflow completion/failure/cancellation has explicit aggregation semantics.
- The current implementation is appropriate for a single-process in-memory runtime, not a durable or distributed orchestrator.
