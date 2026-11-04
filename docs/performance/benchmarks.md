# Reproducible scheduler benchmarks

The benchmark runner measures two distinct parts of the local workflow runtime:

- **Definition validation:** DAG construction and deterministic topological ordering for fan-out and linear-chain workflows.
- **Scheduler execution:** end-to-end execution of those workflows through the real asynchronous scheduler, using the in-memory repository and no-op task handlers.

The scheduler benchmark includes task activation, bounded claiming, handler dispatch, completion, dependency observation, and workflow aggregation. Definition, registry, repository, and Tokio runtime setup happen outside the timed interval.

## Run

From the repository root:

~~~bash
cargo bench -p aetherflow-runtime --bench scheduler
~~~

For a more deliberate local comparison, keep workload parameters identical between revisions:

~~~bash
cargo bench -p aetherflow-runtime --bench scheduler -- \
  --iterations 20 \
  --warmup 3 \
  --sizes 64,256,1024 \
  --concurrency 1,8,32
~~~

The runner writes CSV rows to standard output. Comment lines record operating system, architecture, logical CPU count, measured sample count, and warmup count. Each row includes median, p95, mean latency, and completed tasks per second. Use the same compiler/toolchain, hardware, power profile, and background-load conditions when comparing revisions.

## CI

Rust CI runs a small smoke workload and uploads its CSV output as a workflow artifact. This proves that the benchmark executable builds and that both DAG shapes finish successfully; the smoke run is not a performance gate and its timings are not a stable regression baseline.

## Scope and interpretation

These measurements cover only the process-local scheduler and in-memory repository. They do **not** measure PostgreSQL persistence, NATS JetStream delivery, HTTP request handling, network latency, multi-process coordination, or production capacity. Do not use these numbers for deployment sizing or an exactly-once/throughput claim.

For real persistence and transport behavior, use the [PostgreSQL/JetStream end-to-end load guide](end-to-end-load.md). The in-memory smoke and the end-to-end profile exercise different paths and their throughput numbers must not be compared as if they measured the same system. Neither CI smoke is a stable regression baseline.
