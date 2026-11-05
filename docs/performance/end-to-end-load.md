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


## Second five-repeat session — separate CPU fingerprint (2026-11-04)

A second PostgreSQL/NATS five-repeat session completed in CI [41860721271](https://github.com/pooya-labs/aetherflow/actions/runs/41860721271) attempt 2 on exact source `db204488edc219d105552ee646245054ac9ed913`. The job passed storage integration, restricted-role/API/TLS checks, three-node JetStream transport, schema invariants, both five-repeat workload profiles, and the isolated PostgreSQL/JetStream restore rehearsal. Each workload shape again reconciled 896 published outbox events with 896 processed inbox rows, with zero duplicates and zero errors.

### Environment fingerprint (second session)

| Field | Recorded value |
|---|---|
| GitHub runner label | `ubuntu-24.04` |
| Hosted image | `ubuntu24` / `20261026.354.1` |
| Kernel | `7.0.0-1017-azure` |
| CPU | AMD EPYC 7763 64-Core Processor |
| Allocated logical CPUs | 4 |
| Visible memory | 16,760,344,576 bytes (about 15.6 GiB) |
| Rust toolchain | `1.99.1` |
| PostgreSQL | `18.6 (Debian 18.6-1.pgdg13+2)` |
| NATS Server | `2.12.15` |
| Workload | 64 workflows × 4 tasks; 256-byte input |
| Worker | One process, concurrency 4 |
| Producer / handler | 8 offered workflows/s; 1 ms handler delay |
| Repetitions | 5 per shape, sequential, fresh PostgreSQL DB per repetition |

### Results (second session)

| Metric | Fan-out | Chain |
|---|---:|---:|
| Median workflows/s | 5.701 | 5.141 |
| Mean workflows/s | 5.561 | 5.164 |
| Sample standard deviation, workflows/s | 0.220 | 0.077 |
| Observed range, workflows/s | 5.295–5.733 | 5.111–5.298 |
| Median workflow p50 | 4,235.195 ms | 4,363.544 ms |
| Median workflow p95 | 8,192.001 ms | 8,454.851 ms |
| Sample standard deviation, p95 | 44.981 ms | 34.709 ms |
| Median whole-run elapsed | 11,225.796 ms | 12,448.748 ms |
| Outbox events per run | 896 | 896 |
| Inbox events processed per run | 896 | 896 |
| Duplicate events / errors | 0 / 0 | 0 / 0 |

The archived artifact is `aetherflow-end-to-end-baseline-db204488edc219d105552ee646245054ac9ed913` (run [41860721271](https://github.com/pooya-labs/aetherflow/actions/runs/41860721271), attempt 2). It contains all ten per-run CSVs, a combined sample CSV, summary statistics, and `metadata.json`.

**Comparison boundary:** the first session ran on an AMD EPYC 9V45 host; this session ran on an AMD EPYC 7763 host. The runner image, kernel, vCPU allocation, memory, Rust/PostgreSQL/NATS versions, and workload match, but the CPU model does not. Keep these as separate fingerprinted baselines; do not pool the samples or interpret the throughput difference as a regression. An independent repeat on the same practical AMD EPYC 9V45 fingerprint is now archived in the third-session section. The conservative fingerprint-aware CI gate is described above; thresholds remain limited to the matching 9V45 environment and are not SLOs.
## Third five-repeat session — repeated AMD EPYC 9V45 fingerprint (2026-11-05)

An independent five-repeat session completed in [PostgreSQL/NATS CI 42011508896](https://github.com/pooya-labs/aetherflow/actions/runs/42011508896) on exact source e112fa44bfb50da4bc60087ef6be6e0370826cae. It ran five fan-out and five chain repetitions, each using a fresh PostgreSQL database and unique JetStream stream. Every repetition reconciled 896 published outbox events with 896 processed inbox rows; duplicate and error counts were zero.

### Environment fingerprint

| Field | Recorded value |
|---|---|
| GitHub runner label | ubuntu-24.04 |
| Hosted image | ubuntu24 / 20261026.354.1 |
| Kernel | 7.0.0-1017-azure |
| CPU | AMD EPYC 9V45 96-Core Processor |
| Allocated logical CPUs | 4 |
| Visible memory | 16,760,340,480 bytes (about 15.6 GiB) |
| Rust toolchain | 1.99.1 |
| PostgreSQL | 18.6 (Debian 18.6-1.pgdg13+2) |
| NATS Server | 2.12.15 |
| Workload | 64 workflows × 4 tasks; 256-byte input |
| Worker | One process, concurrency 4 |
| Producer / handler | 8 offered workflows/s; 1 ms handler delay |
| Repetitions | 5 per shape, sequential, fresh PostgreSQL DB per repetition |

Compared with the first AMD EPYC 9V45 reference, all recorded hardware/software/workload fields match except memory_bytes, which is 4,096 bytes (one 4 KiB page) lower. Both values normalize to the same whole MiB. This is treated as the same practical runner fingerprint, while the raw metadata difference remains explicit. The archived artifact is aetherflow-end-to-end-baseline-e112fa44bfb50da4bc60087ef6be6e0370826cae (90-day retention); it contains all ten per-run CSVs, combined samples, summary statistics, and metadata.json.

### Results (third session)

| Metric | Fan-out | Chain |
|---|---:|---:|
| Median workflows/s | 7.415 | 7.298 |
| Mean workflows/s | 7.347 | 7.301 |
| Sample standard deviation, workflows/s | 0.139 | 0.038 |
| Observed range, workflows/s | 7.112–7.447 | 7.250–7.340 |
| Median tasks/s | 29.659 | 29.193 |
| Median workflow p50 | 4,026.491 ms | 4,083.682 ms |
| Median workflow p95 | 7,798.545 ms | 7,899.231 ms |
| Sample standard deviation, p95 | 171.279 ms | 38.213 ms |
| Median whole-run elapsed | 8,631.509 ms | 8,769.294 ms |
| Outbox events per run | 896 | 896 |
| Inbox events processed per run | 896 | 896 |
| Duplicate events / errors | 0 / 0 | 0 / 0 |

Relative to the first 9V45 session, median throughput is 1.31% higher for fan-out and 1.43% higher for chain; median p95 is 0.83% lower and 1.30% lower respectively. These are descriptive repeatability results, not evidence of a performance improvement: both sessions ran on shared ephemeral hosted VMs.

### Matched-fingerprint interpretation

The first and third sessions provide ten repetitions per workload shape across two independent sessions on the same practical AMD EPYC 9V45 runner fingerprint. Their distributions are close and all event reconciliation/duplicate/error checks pass. The AMD EPYC 7763 session remains a separate baseline and must not be pooled with these samples. The one-page memory delta is documented rather than hidden; the comparator now applies broad 10% throughput and 15% p95 regression limits only to that matching normalized fingerprint.

## Matched-fingerprint CI regression gate

The PostgreSQL/NATS workflow runs a standard-library Python comparator after uploading each raw benchmark artifact. The committed reference in docs/performance/reference/amd-epyc-9v45.json pools ten samples per shape from two independent five-repeat sessions on AMD EPYC 9V45.

The comparator applies these deliberately broad limits only when the current runner fingerprint and workload match the reference:
- Median whole-run throughput must remain at or above 90% of the reference median.
- Median workflow p95 must remain at or below 115% of the reference median.
- Every profile must still have five repeats, reconciled outbox/inbox counts, zero duplicate events, and zero errors.

The fingerprint includes runner label/OS/architecture, hosted image and version, kernel, CPU model, logical CPU count, Rust toolchain, PostgreSQL/NATS versions, and workload settings. Memory is normalized down to whole MiB because the two 9V45 sessions differed by only one 4 KiB page in /proc/meminfo. If any other fingerprint or workload field differs, the comparator reports SKIP and CI retains the uploaded artifact without applying a threshold. In particular, the AMD EPYC 7763 host is not compared with the 9V45 reference until its own matched repeat is available.

These limits are regression tripwires, not SLOs, production-capacity claims, or a substitute for dedicated performance infrastructure. The comparator's unit tests cover matching and mismatching fingerprints, workload drift, incomplete samples, event reconciliation, duplicate detection, throughput regression, and p95 regression.

### Interpretation and remaining validation

Both archived profiles are five-repeat baselines on ephemeral hosted VMs, not dedicated reserved performance hosts, production capacity results, or customer-facing SLOs. Within-session variation is recorded for each fingerprint. Because the CPU model differs between the two sessions, their throughput values are not a matched day-to-day repeatability comparison and must not be pooled. The same practical CPU/environment fingerprint now has two independent five-repeat sessions (ten samples per shape), with a one-page visible-memory difference documented. A future threshold must compare only compatible fingerprints and must not pool the EPYC 7763 samples. No automated performance threshold is enabled.

The harness still exercises the coordinator directly rather than HTTP/TLS/authentication, uses the `legacy-default` compatibility tenant path, one worker process, and one inbox consumer loop. It does not measure multi-process scaling, per-tenant authorization overhead, external side effects, or production sizing.
