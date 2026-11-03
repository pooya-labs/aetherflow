//! Reproducible microbenchmarks for workflow validation and local execution.
//!
//! Results are descriptive, not CI performance thresholds. Scheduler cases use
//! the in-memory repository and deliberately exclude PostgreSQL and NATS.

use std::env;
use std::hint::black_box;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aetherflow_core::domain::{
    Lifecycle, RetryPolicy, TaskId, WorkerId, WorkflowId, WorkflowRunId, WorkflowStatus,
};
use aetherflow_runtime::clock::MonotonicClock;
use aetherflow_runtime::repository::{InMemoryRepository, TaskOutput};
use aetherflow_runtime::scheduler::{
    SchedulerConfig, TaskContext, TaskExecutionError, TaskRegistry, TaskScheduler,
};
use aetherflow_runtime::workflow::{
    SequenceTaskRunIdGenerator, WorkflowDefinition, WorkflowTask,
};
use tokio::runtime::Builder;
use tokio::sync::watch;

const DEFAULT_ITERATIONS: usize = 10;
const DEFAULT_WARMUP: usize = 2;
const MAX_TASKS: usize = 4096;
const MAX_CONCURRENCY: usize = 128;

#[derive(Clone, Copy)]
enum Scenario {
    FanOut,
    Chain,
}

impl Scenario {
    const ALL: [Self; 2] = [Self::FanOut, Self::Chain];

    const fn name(self) -> &'static str {
        match self {
            Self::FanOut => "fan_out",
            Self::Chain => "chain",
        }
    }
}

struct Config {
    iterations: usize,
    warmup: usize,
    sizes: Vec<usize>,
    concurrencies: Vec<usize>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            iterations: DEFAULT_ITERATIONS,
            warmup: DEFAULT_WARMUP,
            sizes: vec![64, 256],
            concurrencies: vec![1, 8],
        }
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("benchmark failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let Some(config) = parse_args()? else {
        return Ok(());
    };

    let logical_cpus = std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1);

    println!("# aetherflow_scheduler_benchmark_version=1");
    println!(
        "# os={},arch={},logical_cpus={},iterations={},warmup={}",
        env::consts::OS,
        env::consts::ARCH,
        logical_cpus,
        config.iterations,
        config.warmup
    );
    println!("scenario,tasks,concurrency,samples,median_ms,p95_ms,mean_ms,tasks_per_second");

    for size in &config.sizes {
        for scenario in Scenario::ALL {
            benchmark_definition(*size, scenario, config.iterations, config.warmup)?;

            for concurrency in &config.concurrencies {
                benchmark_scheduler(
                    *size,
                    scenario,
                    *concurrency,
                    config.iterations,
                    config.warmup,
                )?;
            }
        }
    }

    Ok(())
}

fn parse_args() -> Result<Option<Config>, String> {
    let mut config = Config::default();
    let mut arguments = env::args().skip(1);

    while let Some(argument) = arguments.next() {
        if argument == "--help" || argument == "-h" {
            print_usage();
            return Ok(None);
        }

        let value = arguments
            .next()
            .ok_or_else(|| format!("missing value for {argument}"))?;

        match argument.as_str() {
            "--iterations" => config.iterations = parse_usize(&value, "iterations")?,
            "--warmup" => config.warmup = parse_usize(&value, "warmup")?,
            "--sizes" => config.sizes = parse_list(&value, "sizes")?,
            "--concurrency" => config.concurrencies = parse_list(&value, "concurrency")?,
            _ => return Err(format!("unknown argument: {argument}")),
        }
    }

    if config.iterations == 0 || config.iterations > 1000 {
        return Err("iterations must be between 1 and 1000".to_owned());
    }
    if config.warmup > 100 {
        return Err("warmup must not exceed 100".to_owned());
    }
    if config.sizes.is_empty() || config.sizes.iter().any(|size| *size > MAX_TASKS) {
        return Err(format!("sizes must contain values between 1 and {MAX_TASKS}"));
    }
    if config.concurrencies.is_empty()
        || config
            .concurrencies
            .iter()
            .any(|value| *value > MAX_CONCURRENCY)
    {
        return Err(format!(
            "concurrency must contain values between 1 and {MAX_CONCURRENCY}"
        ));
    }

    Ok(Some(config))
}

fn parse_usize(value: &str, name: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|error| format!("invalid {name} value {value:?}: {error}"))
}

fn parse_list(value: &str, name: &str) -> Result<Vec<usize>, String> {
    value
        .split(',')
        .map(|part| {
            let parsed = parse_usize(part.trim(), name)?;
            if parsed == 0 {
                return Err(format!("{name} values must be greater than zero"));
            }
            Ok(parsed)
        })
        .collect()
}

fn print_usage() {
    println!(
        "AetherFlow scheduler benchmarks\n\
         \n\
         Options:\n\
           --iterations N     measured samples per scenario (default: 10)\n\
           --warmup N         discarded warmup samples (default: 2)\n\
           --sizes N,N        workflow task counts (default: 64,256)\n\
           --concurrency N,N  scheduler worker limits (default: 1,8)\n\
           --help             show this help\n\
         \n\
         Example:\n\
           cargo bench -p aetherflow-runtime --bench scheduler -- \\\n\
             --iterations 20 --warmup 3 --sizes 64,256,1024 --concurrency 1,8,32"
    );
}

fn task_ids(size: usize) -> Result<Vec<TaskId>, String> {
    (0..size)
        .map(|index| {
            TaskId::parse(format!("task-{index:05}"))
                .map_err(|error| error.to_string())
        })
        .collect()
}

fn workflow_tasks(size: usize, scenario: Scenario) -> Result<Vec<WorkflowTask>, String> {
    let identifiers = task_ids(size)?;
    let mut tasks = Vec::with_capacity(size);

    for (index, identifier) in identifiers.iter().enumerate() {
        let dependencies = match scenario {
            Scenario::FanOut => Vec::new(),
            Scenario::Chain if index > 0 => vec![identifiers[index - 1].clone()],
            Scenario::Chain => Vec::new(),
        };

        tasks.push(WorkflowTask::new(identifier.clone(), dependencies));
    }

    Ok(tasks)
}

fn workflow_definition(size: usize, scenario: Scenario) -> Result<WorkflowDefinition, String> {
    let workflow_id =
        WorkflowId::parse("benchmark-workflow").map_err(|error| error.to_string())?;
    WorkflowDefinition::new(workflow_id, workflow_tasks(size, scenario)?)
        .map_err(|error| error.to_string())
}

fn benchmark_definition(
    size: usize,
    scenario: Scenario,
    iterations: usize,
    warmup: usize,
) -> Result<(), String> {
    let mut samples = Vec::with_capacity(iterations);

    for sample in 0..(warmup + iterations) {
        let tasks = workflow_tasks(size, scenario)?;
        let workflow_id =
            WorkflowId::parse("benchmark-workflow").map_err(|error| error.to_string())?;

        let started = Instant::now();
        let definition =
            WorkflowDefinition::new(workflow_id, tasks).map_err(|error| error.to_string())?;
        let elapsed = started.elapsed();
        black_box(definition);

        if sample >= warmup {
            samples.push(elapsed);
        }
    }

    print_summary(
        &format!("definition_{}", scenario.name()),
        size,
        "-",
        &samples,
    );
    Ok(())
}

fn benchmark_scheduler(
    size: usize,
    scenario: Scenario,
    concurrency: usize,
    iterations: usize,
    warmup: usize,
) -> Result<(), String> {
    let runtime = Builder::new_multi_thread()
        .worker_threads(concurrency)
        .enable_time()
        .build()
        .map_err(|error| error.to_string())?;

    let mut samples = Vec::with_capacity(iterations);

    for sample in 0..(warmup + iterations) {
        let definition = workflow_definition(size, scenario)?;
        let mut registry = TaskRegistry::new();

        for task in definition.tasks() {
            registry
                .register(
                    task.task_id().clone(),
                    |_context: TaskContext| async {
                        Ok::<TaskOutput, TaskExecutionError>(TaskOutput::empty())
                    },
                )
                .map_err(|error| error.to_string())?;
        }

        let retry_policy = RetryPolicy::new(NonZeroU32::new(1).expect("one is non-zero"), 0, 0)
            .map_err(|error| error.to_string())?;
        let scheduler_config = SchedulerConfig::new(
            NonZeroUsize::new(concurrency).expect("validated non-zero concurrency"),
            Duration::from_millis(1),
            None,
            retry_policy,
        )
        .map_err(|error| error.to_string())?;

        let scheduler = TaskScheduler::new(
            Arc::new(InMemoryRepository::new()),
            Arc::new(MonotonicClock::new()),
            WorkerId::parse("benchmark-worker").map_err(|error| error.to_string())?,
            registry,
            scheduler_config,
        );

        let workflow_run_id = WorkflowRunId::parse(format!("benchmark-run-{sample:08}"))
            .map_err(|error| error.to_string())?;
        let id_generator = SequenceTaskRunIdGenerator::new();
        let (_shutdown_sender, shutdown_receiver) = watch::channel(false);

        let started = Instant::now();
        let result = runtime.block_on(scheduler.execute_workflow(
            definition,
            workflow_run_id,
            &id_generator,
            shutdown_receiver,
        ));
        let elapsed = started.elapsed();

        let result = result.map_err(|error| error.to_string())?;
        if result.status() != WorkflowStatus::Succeeded || result.task_runs().len() != size {
            return Err(format!(
                "workflow sample did not complete: status={}, task_runs={}, expected_tasks={size}",
                result.status().name(),
                result.task_runs().len()
            ));
        }

        if sample >= warmup {
            samples.push(elapsed);
        }
    }

    print_summary(
        &format!("scheduler_{}", scenario.name()),
        size,
        &concurrency.to_string(),
        &samples,
    );
    Ok(())
}

fn print_summary(
    scenario: &str,
    task_count: usize,
    concurrency: &str,
    samples: &[Duration],
) {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();

    let median = percentile_duration(&ordered, 50);
    let p95 = percentile_duration(&ordered, 95);
    let sample_count = u128::try_from(ordered.len()).expect("sample count fits u128");
    let total_nanos = ordered.iter().map(Duration::as_nanos).sum::<u128>();
    let mean = duration_from_nanos(total_nanos / sample_count);
    let safe_seconds = median.as_secs_f64().max(f64::EPSILON);
    let task_count_f64 = f64::from(u32::try_from(task_count).expect("task count is bounded"));
    let throughput = task_count_f64 / safe_seconds;

    println!(
        "{scenario},{task_count},{concurrency},{},{:.3},{:.3},{:.3},{throughput:.1}",
        ordered.len(),
        median.as_secs_f64() * 1000.0,
        p95.as_secs_f64() * 1000.0,
        mean.as_secs_f64() * 1000.0,
    );
}

fn percentile_duration(ordered: &[Duration], percentile: usize) -> Duration {
    let rank = ordered.len().saturating_mul(percentile).div_ceil(100);
    ordered[rank.saturating_sub(1).min(ordered.len() - 1)]
}

fn duration_from_nanos(nanos: u128) -> Duration {
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}
