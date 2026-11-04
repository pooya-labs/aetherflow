#!/usr/bin/env python3
"""Run repeatable PostgreSQL + NATS end-to-end performance samples.

Each repetition gets a fresh PostgreSQL database and unique JetStream stream.
The runner fingerprint must remain constant for samples to be comparable.
Only Python's standard library is used.
"""

from __future__ import annotations

import csv
import json
import math
import os
import platform
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "target" / "aetherflow-end-to-end-baseline"
METRICS = (
    "workflows_per_second",
    "tasks_per_second",
    "events_per_second",
    "p50_workflow_ms",
    "p95_workflow_ms",
    "mean_workflow_ms",
    "elapsed_ms",
)
ENVIRONMENT_KEYS = (
    "runner_label",
    "runner_os",
    "runner_arch",
    "runner_image_os",
    "runner_image_version",
    "kernel_release",
    "machine_architecture",
    "cpu_model",
    "logical_cpus",
    "memory_bytes",
    "rust_toolchain",
    "postgres_server_version",
    "nats_server_version",
)


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def positive_int(name: str, default: int, maximum: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from error
    if value < 1 or value > maximum:
        raise RuntimeError(f"{name} must be in [1, {maximum}], got {value}")
    return value


def read_cpu_model() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.lower().startswith(("model name", "hardware")):
                return line.split(":", 1)[1].strip()
    except (OSError, IndexError):
        pass
    return platform.processor() or "unknown"


def read_memory_bytes() -> int | None:
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def environment_metadata() -> dict[str, Any]:
    try:
        kernel_release = Path("/proc/sys/kernel/osrelease").read_text(
            encoding="utf-8"
        ).strip()
    except OSError:
        kernel_release = platform.release()

    return {
        "runner_label": required("AETHERFLOW_BASELINE_RUNNER_LABEL"),
        "runner_os": os.environ.get("RUNNER_OS", platform.system()),
        "runner_arch": os.environ.get("RUNNER_ARCH", platform.machine()),
        "runner_image_os": os.environ.get("ImageOS", "unknown"),
        "runner_image_version": os.environ.get("ImageVersion", "unknown"),
        "kernel_release": kernel_release,
        "machine_architecture": platform.machine(),
        "cpu_model": read_cpu_model(),
        "logical_cpus": os.cpu_count(),
        "memory_bytes": read_memory_bytes(),
        "rust_toolchain": required("AETHERFLOW_BASELINE_RUST_TOOLCHAIN"),
        "postgres_server_version": required("AETHERFLOW_BASELINE_POSTGRES_VERSION"),
        "nats_server_version": required("AETHERFLOW_BASELINE_NATS_VERSION"),
    }


def load_one_csv(path: Path) -> dict[str, Any]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) != 1:
        raise RuntimeError(f"{path} must contain exactly one result row, found {len(rows)}")

    row = rows[0]
    numeric_float = METRICS
    for key in numeric_float:
        value = float(row[key])
        if not math.isfinite(value):
            raise RuntimeError(f"{path}: {key} is not finite")
        row[key] = value

    for key in (
        "workflows",
        "tasks_per_workflow",
        "total_tasks",
        "payload_bytes",
        "worker_concurrency",
        "producer_rate_workflows_per_second",
        "outbox_events_published",
        "inbox_events_processed",
        "duplicate_events",
        "errors",
    ):
        row[key] = int(row[key])

    if row["errors"] != 0:
        raise RuntimeError(f"{path}: profile reported {row['errors']} errors")
    if row["duplicate_events"] != 0:
        raise RuntimeError(f"{path}: profile reported duplicate events")
    if row["outbox_events_published"] <= 0:
        raise RuntimeError(f"{path}: no outbox events were published")
    if row["outbox_events_published"] != row["inbox_events_processed"]:
        raise RuntimeError(f"{path}: outbox/inbox event counts do not reconcile")
    return row


def run_checked(command: list[str], *, env: dict[str, str] | None = None) -> None:
    subprocess.run(command, cwd=ROOT, env=env, check=True)


def aggregate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "shape": rows[0]["shape"],
        "repeat_count": len(rows),
        "workflows": rows[0]["workflows"],
        "tasks_per_workflow": rows[0]["tasks_per_workflow"],
        "payload_bytes": rows[0]["payload_bytes"],
        "worker_concurrency": rows[0]["worker_concurrency"],
        "producer_rate_workflows_per_second": rows[0][
            "producer_rate_workflows_per_second"
        ],
        "outbox_events_per_run": rows[0]["outbox_events_published"],
        "inbox_events_per_run": rows[0]["inbox_events_processed"],
        "duplicates_per_run": rows[0]["duplicate_events"],
        "errors_per_run": rows[0]["errors"],
    }
    for metric in METRICS:
        samples = [float(row[metric]) for row in rows]
        result[f"median_{metric}"] = statistics.median(samples)
        result[f"mean_{metric}"] = statistics.mean(samples)
        result[f"sample_stdev_{metric}"] = (
            statistics.stdev(samples) if len(samples) > 1 else 0.0
        )
        result[f"min_{metric}"] = min(samples)
        result[f"max_{metric}"] = max(samples)
    return result


def main() -> int:
    repeats = positive_int("AETHERFLOW_BASELINE_REPEATS", 5, 20)
    workflows = positive_int("AETHERFLOW_BASELINE_WORKFLOWS", 64, 10_000)
    tasks = positive_int("AETHERFLOW_BASELINE_TASKS", 4, 256)
    payload_bytes = positive_int("AETHERFLOW_BASELINE_PAYLOAD_BYTES", 256, 1_048_576)
    worker_concurrency = positive_int(
        "AETHERFLOW_BASELINE_WORKER_CONCURRENCY", 4, 128
    )
    producer_rate = positive_int("AETHERFLOW_BASELINE_PRODUCER_RATE", 8, 10_000)
    handler_delay_ms = positive_int(
        "AETHERFLOW_BASELINE_HANDLER_DELAY_MS", 1, 10_000
    )
    timeout_seconds = positive_int(
        "AETHERFLOW_BASELINE_TIMEOUT_SECS", 240, 3600
    )
    database_prefix = required("AETHERFLOW_BASELINE_DATABASE_URL_PREFIX")
    nats_url = required("AETHERFLOW_BASELINE_NATS_URL")
    binary = ROOT / "target" / "debug" / "examples" / "end_to_end_load"
    if not binary.is_file():
        raise RuntimeError(f"load-profile executable does not exist: {binary}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    metadata = environment_metadata()
    metadata["source_sha"] = os.environ.get("GITHUB_SHA", "unknown")
    metadata["workflow_run_id"] = os.environ.get("GITHUB_RUN_ID", "unknown")
    metadata["workflow_run_attempt"] = os.environ.get("GITHUB_RUN_ATTEMPT", "unknown")
    metadata["repeat_count_per_shape"] = repeats
    metadata["workload"] = {
        "workflows_per_repeat": workflows,
        "tasks_per_workflow": tasks,
        "payload_bytes": payload_bytes,
        "worker_concurrency": worker_concurrency,
        "offered_workflow_rate_per_second": producer_rate,
        "handler_delay_ms": handler_delay_ms,
        "timeout_seconds": timeout_seconds,
        "shapes": ["fan_out", "chain"],
    }

    all_rows: list[dict[str, Any]] = []
    shape_rows: dict[str, list[dict[str, Any]]] = {"fan_out": [], "chain": []}

    for shape in ("fan_out", "chain"):
        for repeat in range(1, repeats + 1):
            database = f"aetherflow_baseline_{shape}_{repeat:02d}"
            database_url = f"{database_prefix}{database}"
            output = OUTPUT_DIR / f"{shape}-{repeat:02d}.csv"
            output_env = os.environ.copy()
            output_env.update(
                {
                    "AETHERFLOW_LOAD_DATABASE_URL": database_url,
                    "AETHERFLOW_LOAD_NATS_URL": nats_url,
                    "AETHERFLOW_LOAD_SHAPE": shape,
                    "AETHERFLOW_LOAD_WORKFLOWS": str(workflows),
                    "AETHERFLOW_LOAD_TASKS": str(tasks),
                    "AETHERFLOW_LOAD_PAYLOAD_BYTES": str(payload_bytes),
                    "AETHERFLOW_LOAD_WORKERS": str(worker_concurrency),
                    "AETHERFLOW_LOAD_PRODUCER_RATE": str(producer_rate),
                    "AETHERFLOW_LOAD_HANDLER_DELAY_MS": str(handler_delay_ms),
                    "AETHERFLOW_LOAD_TIMEOUT_SECS": str(timeout_seconds),
                    "AETHERFLOW_LOAD_OUTPUT": str(output),
                }
            )
            created = False
            try:
                print(
                    f"Starting {shape} repetition {repeat}/{repeats} "
                    f"on database {database}",
                    flush=True,
                )
                run_checked(
                    [
                        "createdb",
                        "--host",
                        "127.0.0.1",
                        "--username",
                        "aetherflow",
                        database,
                    ]
                )
                created = True
                completed = subprocess.run(
                    [str(binary)],
                    cwd=ROOT,
                    env=output_env,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                if completed.stdout:
                    print(completed.stdout, end="", flush=True)
                if completed.returncode != 0:
                    if completed.stderr:
                        print(completed.stderr, file=sys.stderr, flush=True)
                    raise RuntimeError(
                        f"{shape} repetition {repeat} failed with exit "
                        f"code {completed.returncode}"
                    )
                row = load_one_csv(output)
                row["repeat"] = repeat
                shape_rows[shape].append(row)
                all_rows.append(row)
            finally:
                if created:
                    subprocess.run(
                        [
                            "dropdb",
                            "--if-exists",
                            "--host",
                            "127.0.0.1",
                            "--username",
                            "aetherflow",
                            database,
                        ],
                        cwd=ROOT,
                        check=True,
                    )

    # Repetitions are deliberately sequential on one pinned runner. Refuse to
    # publish a baseline if a runner/dependency fingerprint changed mid-run.
    for field in ENVIRONMENT_KEYS:
        if metadata.get(field) in (None, "", "unknown"):
            raise RuntimeError(f"baseline environment field {field} is unknown")

    expected_parameters = {
        "workflows": workflows,
        "tasks_per_workflow": tasks,
        "payload_bytes": payload_bytes,
        "worker_concurrency": worker_concurrency,
        "producer_rate_workflows_per_second": producer_rate,
    }
    for shape, rows in shape_rows.items():
        if len(rows) != repeats:
            raise RuntimeError(f"{shape}: expected {repeats} samples, found {len(rows)}")
        for row in rows:
            for key, expected in expected_parameters.items():
                if row[key] != expected:
                    raise RuntimeError(
                        f"{shape} repetition {row['repeat']}: {key}={row[key]}, "
                        f"expected {expected}"
                    )

    summary_rows = [aggregate(shape_rows[shape]) for shape in ("fan_out", "chain")]
    summary_path = OUTPUT_DIR / "summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summary_rows[0]))
        writer.writeheader()
        writer.writerows(summary_rows)

    samples_path = OUTPUT_DIR / "samples.csv"
    with samples_path.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = ["shape", "repeat", *all_rows[0].keys()]
        fieldnames = list(dict.fromkeys(fieldnames))
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    metadata["sample_count"] = len(all_rows)
    metadata["summary_file"] = "summary.csv"
    metadata["samples_file"] = "samples.csv"
    (OUTPUT_DIR / "metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    for result in summary_rows:
        print(
            f"BASELINE shape={result['shape']} repeats={result['repeat_count']} "
            f"median_workflows_per_second={result['median_workflows_per_second']:.3f} "
            f"stdev={result['sample_stdev_workflows_per_second']:.3f} "
            f"median_p95_ms={result['median_p95_workflow_ms']:.3f}",
            flush=True,
        )
    print(f"PASS: {len(all_rows)} isolated load samples; results in {OUTPUT_DIR}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # Print a useful CI failure without hiding it.
        print(f"end-to-end baseline failed: {error}", file=sys.stderr)
        raise SystemExit(1)
