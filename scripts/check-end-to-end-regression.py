#!/usr/bin/env python3
"""Compare a five-repeat end-to-end profile only with a matching runner fingerprint."""
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path
from typing import Any

MIB = 1024 * 1024
ENVIRONMENT_FIELDS = (
    "runner_label",
    "runner_os",
    "runner_arch",
    "runner_image_os",
    "runner_image_version",
    "kernel_release",
    "machine_architecture",
    "cpu_model",
    "logical_cpus",
    "rust_toolchain",
    "postgres_server_version",
    "nats_server_version",
)


def normalized_environment(metadata: dict[str, Any]) -> dict[str, Any]:
    environment = {key: metadata.get(key) for key in ENVIRONMENT_FIELDS}
    memory_bytes = metadata.get("memory_bytes")
    if not isinstance(memory_bytes, int) or memory_bytes <= 0:
        raise ValueError("metadata.memory_bytes must be a positive integer")
    # /proc/meminfo is page-granular. Normalize to whole MiB so a one-page
    # allocation-reporting delta does not split otherwise identical runners.
    environment["memory_mib"] = memory_bytes // MIB
    return environment


def compare(
    metadata: dict[str, Any],
    summary_rows: list[dict[str, Any]],
    reference: dict[str, Any],
) -> tuple[str, list[str]]:
    current_environment = normalized_environment(metadata)
    expected_environment = reference["environment"]
    mismatches = [
        f"{key}: current={current_environment.get(key)!r}, reference={expected!r}"
        for key, expected in expected_environment.items()
        if current_environment.get(key) != expected
    ]

    reference_repeat_count = int(reference["repeat_count_per_shape"])
    actual_repeat_count = int(metadata.get("repeat_count_per_shape", 0))
    if reference_repeat_count < 1:
        return "fail", ["FAIL: reference repeat_count_per_shape must be positive"]
    if actual_repeat_count < 1:
        return "fail", ["FAIL: metadata.repeat_count_per_shape must be positive"]
    expected_samples = 2 * actual_repeat_count
    if int(metadata.get("sample_count", 0)) != expected_samples:
        return "fail", [
            f"FAIL: expected {expected_samples} total samples, got {metadata.get('sample_count')}"
        ]

    expected_shapes = ("fan_out", "chain")
    by_shape: dict[str, dict[str, Any]] = {}
    for row in summary_rows:
        shape = row.get("shape")
        if shape not in expected_shapes:
            return "fail", [f"FAIL: unsupported summary shape {shape!r}"]
        if shape in by_shape:
            return "fail", [f"FAIL: duplicate {shape} summary row"]
        by_shape[shape] = row

    missing_shapes = [shape for shape in expected_shapes if shape not in by_shape]
    if missing_shapes:
        return "fail", [
            f"FAIL: missing summary for {', '.join(missing_shapes)}"
        ]

    validated_rows: dict[str, tuple[dict[str, Any], dict[str, Any], float, float]] = {}
    for shape in expected_shapes:
        row = by_shape[shape]
        policy = reference["shapes"].get(shape)
        if policy is None:
            return "fail", [f"FAIL: missing {shape} reference policy"]
        if int(row.get("repeat_count", 0)) != actual_repeat_count:
            return "fail", [
                f"FAIL: {shape} expected {actual_repeat_count} repeats, "
                f"got {row.get('repeat_count')}"
            ]
        if int(row.get("duplicates_per_run", -1)) != 0 or int(row.get("errors_per_run", -1)) != 0:
            return "fail", [f"FAIL: {shape} contains duplicate events or errors"]

        outbox_events = int(row.get("outbox_events_per_run", 0))
        inbox_events = int(row.get("inbox_events_per_run", -1))
        if outbox_events <= 0 or outbox_events != inbox_events:
            return "fail", [
                f"FAIL: {shape} outbox/inbox counts are empty or do not reconcile"
            ]

        try:
            throughput = float(row["median_workflows_per_second"])
            p95 = float(row["median_p95_workflow_ms"])
        except (KeyError, TypeError, ValueError) as error:
            return "fail", [f"FAIL: {shape} has invalid performance metrics: {error}"]
        if not math.isfinite(throughput) or throughput <= 0:
            return "fail", [
                f"FAIL: {shape} median throughput must be finite and positive"
            ]
        if not math.isfinite(p95) or p95 <= 0:
            return "fail", [
                f"FAIL: {shape} median p95 latency must be finite and positive"
            ]
        validated_rows[shape] = (row, policy, throughput, p95)

    # Validate artifact integrity before deciding to skip numeric comparison.
    # A different runner/workload cannot be compared to the reference, but it
    # must not turn malformed or incomplete benchmark output into a green run.
    if mismatches:
        return "skip", [
            "SKIP: runner fingerprint does not match the stored reference; no threshold was applied.",
            *mismatches,
        ]

    if metadata.get("workload") != reference.get("workload"):
        return "skip", [
            "SKIP: workload parameters differ from the stored reference; artifact retained without comparison."
        ]

    if actual_repeat_count != reference_repeat_count:
        return "fail", [
            f"FAIL: matching workload requires {reference_repeat_count} repeats per shape, "
            f"got {actual_repeat_count}"
        ]

    messages: list[str] = []
    failures: list[str] = []
    for shape in expected_shapes:
        _, policy, throughput, p95 = validated_rows[shape]
        throughput_floor = float(policy["median_workflows_per_second"]) * (
            1.0 - float(policy["max_throughput_drop_percent"]) / 100.0
        )
        p95_ceiling = float(policy["median_p95_workflow_ms"]) * (
            1.0 + float(policy["max_p95_increase_percent"]) / 100.0
        )
        messages.append(
            f"{shape}: median throughput {throughput:.3f} workflows/s "
            f"(minimum {throughput_floor:.3f}); median p95 {p95:.1f} ms "
            f"(maximum {p95_ceiling:.1f} ms)."
        )
        if throughput < throughput_floor:
            failures.append(
                f"{shape}: throughput {throughput:.3f} is below the {throughput_floor:.3f} workflows/s floor"
            )
        if p95 > p95_ceiling:
            failures.append(
                f"{shape}: p95 {p95:.1f} ms exceeds the {p95_ceiling:.1f} ms ceiling"
            )

    if failures:
        return "fail", ["FAIL: matched-fingerprint performance regression detected.", *messages, *failures]
    return "pass", [
        "PASS: matched-fingerprint performance checks are within conservative limits.",
        *messages,
    ]


def read_summary(path: Path) -> list[dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        for field in (
            "repeat_count",
            "outbox_events_per_run",
            "inbox_events_per_run",
            "duplicates_per_run",
            "errors_per_run",
        ):
            row[field] = int(row[field])
        for field in ("median_workflows_per_second", "median_p95_workflow_ms"):
            row[field] = float(row[field])
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    args = parser.parse_args()
    try:
        metadata = json.loads((args.results / "metadata.json").read_text(encoding="utf-8"))
        reference = json.loads(args.reference.read_text(encoding="utf-8"))
        status, messages = compare(metadata, read_summary(args.results / "summary.csv"), reference)
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
        print(f"FAIL: invalid benchmark comparison input: {error}", file=sys.stderr)
        return 1
    for message in messages:
        print(message)
    return 1 if status == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
