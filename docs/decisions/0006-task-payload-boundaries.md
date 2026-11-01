# ADR-0006: Task input and output payload boundaries

- **Status:** Accepted; implementation hosted-verified in run `41358509174`
- **Date:** 2026-11-02
- **Scope:** Single-process task input delivery, successful output retention, and payload size validation

## Context

The scheduler and workflow coordinator can activate tasks, but handlers need a stable way to receive application data and make successful results inspectable. The runtime should define bounded transport-neutral payload types without forcing one serialization format or introducing a schema framework prematurely.

## Decisions

### Opaque byte payloads

TaskInput and TaskOutput are opaque byte wrappers. AetherFlow does not prescribe JSON, protobuf, CBOR, or another encoding. Applications own encoding, content schemas, compatibility, and schema-version metadata.

Each individual task input and output payload is limited to 1 MiB. Constructors reject oversized values before they enter the repository. Empty input and output are represented by empty payload values rather than absence. A task run has an input from creation and an optional output that appears only after successful completion.

### Workflow task inputs

WorkflowTask may carry one static validated TaskInput. When the coordinator materializes its TaskRun after dependencies are satisfied, it copies that input into the task-run record. The coordinator stores direct predecessor TaskRunId references on each materialized task. Before invoking a handler, the scheduler loads each direct predecessor's successful TaskOutput and exposes it in TaskContext as a TaskDependencyOutput keyed by TaskId and TaskRunId. The runtime does not concatenate outputs into TaskInput or prescribe a transformation; handlers own mapping and serialization.

### Handler contract and persistence

TaskContext exposes the task's immutable input payload. A successful handler returns TaskOutput. The repository commits that output atomically with the Succeeded transition and optimistic version check. Failed, retried, cancelled, or timed-out attempts do not store a successful output.

WorkflowExecutionResult includes TaskRun snapshots, so outputs are available to the in-process caller after workflow completion. This does not imply persistence beyond process lifetime: the current adapter remains in-memory and non-durable.

## Consequences

- Payload transfer is bounded and independent of a serializer.
- Handler outputs are retained only when task completion commits successfully.
- A future database adapter can persist the opaque byte values and their size limits without changing handler semantics.
- Application-level schema versioning and transformations from predecessor outputs into a task's static input remain application responsibilities.
