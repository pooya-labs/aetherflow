# PostgreSQL and JetStream end-to-end load profiles

This harness measures the durable execution and event-delivery path rather than the process-local in-memory scheduler. It is intended for repeatable comparisons after the workload and runner are fixed; the first hosted run below is a smoke-scale observation, not a stable capacity baseline.

## What is exercised

For each submitted workflow, the example drives the existing production components:

1. `PgWorkflowCoordinator` persists a validated fan-out or dependency-chain workflow and its task graph in PostgreSQL.
2. A registered `PostgresTaskWorker` claims tasks through PostgreSQL leases/fencing and executes registered async handlers.
3. Task/workflow state changes append transactional outbox records.
4. `PostgresOutboxDispatcher` publishes the events and waits for JetStream storage acknowledgements.
5. A NATS JetStream pull consumer claims each event through `PgInboxRepository`, runs the handler, commits the inbox outcome, and acknowledges the broker delivery.
6. The profile waits for every workflow to reach `succeeded`, all outbox rows to be published, and all profile inbox rows to be processed.

The measured workflow latency is submission-to-observed-success. The profile also records whole-run elapsed time, workflow/task/event throughput, p50/p95/mean workflow latency, outbox/inbox counts, duplicate count, and error count. Completion is observed by polling PostgreSQL, so measured latency includes polling granularity. Whole-run throughput includes harness setup and the final outbox/inbox drain; it is not isolated handler execution throughput.

## Run it

Run against a dedicated disposable PostgreSQL database and a disposable three-node NATS 2.12 JetStream cluster. The example runs migrations, creates a uniquely named three-replica stream and durable consumer, and leaves its data behind for inspection. Do not point it at a shared or production database/cluster. Use a fresh database for each shape so the aggregate outbox/inbox counts are unambiguous.

From the repository root, set the connection and workload values:

~~~bash
export AETHERFLOW_LOAD_DATABASE_URL="postgres://USER:PASSWORD@127.0.0.1:5432/aetherflow_load"
export AETHERFLOW_LOAD_NATS_URL="nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224"
export AETHERFLOW_LOAD_SHAPE=fan_out
export AETHERFLOW_LOAD_WORKFLOWS=16
export AETHERFLOW_LOAD_TASKS=4
export AETHERFLOW_LOAD_PAYLOAD_BYTES=256
export AETHERFLOW_LOAD_WORKERS=4
export AETHERFLOW_LOAD_PRODUCER_RATE=8
export AETHERFLOW_LOAD_HANDLER_DELAY_MS=1
export AETHERFLOW_LOAD_TIMEOUT_SECS=120
export AETHERFLOW_LOAD_OUTPUT=target/aetherflow-load.csv

cargo run -p aetherflow-transport-nats --example end_to_end_load --locked
~~~

Run again with `AETHERFLOW_LOAD_SHAPE=chain` and a different fresh database. The harness validates bounds for workflow count, tasks/workflow, total task count, payload size (1 MiB maximum), worker concurrency, producer rate, handler delay, and timeout. Defaults are 16 workflows, 4 tasks/workflow, 256-byte payloads, concurrency 4, 8 workflow submissions/second, 1 ms handler delay, and a 120-second timeout. The output path can be changed with `AETHERFLOW_LOAD_OUTPUT`.

The producer rate paces workflow submissions; it is an offered rate, not a promise that the entire run will complete at that rate. A complete run also includes startup and message draining.

## Hosted CI profile

The PostgreSQL schema workflow creates separate empty databases for fan-out and chain and runs each against its three-member NATS 2.12 test cluster. Both profiles currently use:

| Parameter | Value |
|---|---:|
| Workflows | 16 |
| Tasks per workflow | 4 |
| Total tasks | 64 |
| Input payload per task | 256 bytes |
| Worker process concurrency | 4 |
| Offered workflow producer rate | 8 per second |
| Handler delay | 1 ms |
| Timeout | 120 seconds |

The workflow uploads two CSV files in the `aetherflow-end-to-end-load-<commit-sha>` artifact (7-day retention). The latest successful profile run is [PostgreSQL/NATS CI 41771482233](https://github.com/pooya-labs/aetherflow/actions/runs/41771482233), with artifact `aetherflow-end-to-end-load-b4367886c5c19144cd2b3add19b852f0a7840c2d`. The matching Rust quality run is [41771482264](https://github.com/pooya-labs/aetherflow/actions/runs/41771482264).

## First hosted result

The first successful run used PostgreSQL 18, three NATS 2.12 JetStream members, the pinned Rust toolchain 1.99.1, and the profile above. Results from its CSV artifacts:

| Metric | Fan-out | Chain |
|---|---:|---:|
| Whole-run elapsed | 3,225.708 ms | 3,668.390 ms |
| Observed workflows / second | 4.960 | 4.362 |
| Tasks / second | 19.841 | 17.446 |
| JetStream events / second | 69.442 | 61.062 |
| Workflow completion p50 | 966.983 ms | 1,064.013 ms |
| Workflow completion p95 | 2,251.496 ms | 2,232.980 ms |
| Workflow completion mean | 1,151.366 ms | 1,292.555 ms |
| Outbox events published | 224 | 224 |
| Inbox events processed | 224 | 224 |
| Duplicate events | 0 | 0 |
| Errors | 0 | 0 |

Each profile completed all 16 workflows and 64 tasks. All 224 outbox events were published and matched by 224 committed inbox events; neither shape observed a duplicate or an error. The observed workflows/second are lower than the offered rate because this small run includes harness setup, paced submissions, task execution, and transport drain.

## Scope and caveats

- This is not the HTTP/control API benchmark. It calls the durable workflow coordinator directly and excludes HTTP parsing, TLS, authentication, and request routing.
- It uses one worker process with a configurable local task concurrency and one inbox-backed consumer loop. It does not measure horizontal worker scaling or consumer-group scaling.
- The example uses the reserved `legacy-default` compatibility path for workflow creation, worker claims, outbox, and inbox; it does not benchmark tenant-credential/grant authorization.
- Each run creates a unique stream and leaves the test stream and database records in the disposable environment. Use separate fresh databases/cluster for clean comparisons.
- One hosted CI run on a shared ephemeral runner is not a statistically stable baseline. These figures must not be used for production sizing, an SLO, or a regression threshold.
- Delivery remains at-least-once. The inbox and JetStream message identity prevent duplicate logical processing within their stated boundaries; they do not make arbitrary external side effects exactly-once.

## Five-repeat reference baseline (2026-11-04)

A five-repeat baseline was captured in PostgreSQL/NATS CI [41776885575](https://github.com/pooya-labs/aetherflow/actions/runs/41776885575) on exact source `b9d30659caed6c525ec6a245f5b8d44bf5cc2dd3`. The job ran five fan-out and five chain repetitions serially on one hosted VM; each repetition received a newly created PostgreSQL database and a unique JetStream stream. Each completed profile reconciled 896 published outbox events with 896 processed inbox rows, with zero duplicate events and zero errors.

### Environment fingerprint

| Field | Recorded value |
|---|---|
| GitHub runner label | `ubuntu-24.04` |
| Hosted image | `ubuntu24` / `20261026.354.1` |
| Kernel | `7.0.0-1017-azure` |
| CPU | AMD EPYC 9V45 96-Core Processor |
| Allocated logical CPUs | 4 |
| Visible memory | 16,760,344,576 bytes (about 15.6 GiB) |
| Rust toolchain | `1.99.1` |
| PostgreSQL | `18.6 (Debian 18.6-1.pgdg13+2)` |
| NATS Server | `2.12.15` |
| Workload | 64 workflows × 4 tasks; 256-byte input |
| Worker | One process, concurrency 4 |
| Producer / handler | 8 offered workflows/s; 1 ms handler delay |
| Repetitions | 5 per shape, sequential, fresh PostgreSQL DB per repetition |

The runner label alone is not a hardware reservation. Treat the image version, CPU model, visible vCPU/memory, database versions, and workload parameters together as the comparison fingerprint. A later run with a changed fingerprint is a new baseline, not a directly comparable regression sample.

### Summary

All throughput values are whole-run rates, including setup and transport drain. Latency is submission-to-observed-workflow-success and includes the harness's PostgreSQL polling interval.

| Metric | Fan-out | Chain |
|---|---:|---:|
| Median workflows/s | 7.319 | 7.195 |
| Mean workflows/s | 7.278 | 7.203 |
| Sample standard deviation, workflows/s | 0.148 | 0.033 |
| Observed range, workflows/s | 7.044–7.407 | 7.164–7.243 |
| Median tasks/s | 29.278 | 28.778 |
| Median JetStream events/s | 102.472 | 100.723 |
| Median workflow p50 | 4,047.126 ms | 4,142.654 ms |
| Median workflow p95 | 7,863.758 ms | 8,003.402 ms |
| Sample standard deviation, p95 | 169.111 ms | 22.684 ms |
| Median whole-run elapsed | 8,743.824 ms | 8,895.681 ms |
| Outbox events per run | 896 | 896 |
| Inbox events processed per run | 896 | 896 |
| Duplicate events / errors | 0 / 0 | 0 / 0 |

The archived artifact is `aetherflow-end-to-end-baseline-b9d30659caed6c525ec6a245f5b8d44bf5cc2dd3` (90-day retention). It includes all ten per-run CSVs, a combined sample CSV, summary statistics, and `metadata.json` with the environment fingerprint.

### Interpretation and remaining validation

This is a repeat baseline on one ephemeral hosted VM, not a dedicated reserved performance host, production capacity result, or customer-facing SLO. The within-session variation is low for the chain profile and modest for fan-out, but one VM session cannot characterize day-to-day runner variance. Run the same profile in a second independent hosted session and compare the complete environment fingerprint before setting any regression threshold. No automated performance threshold is enabled yet.

The harness still exercises the coordinator directly rather than HTTP/TLS/authentication, uses the `legacy-default` compatibility tenant path, one worker process, and one inbox consumer loop. It does not measure multi-process scaling, per-tenant authorization overhead, external side effects, or production sizing.
