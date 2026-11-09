import importlib.util
import json
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "check_end_to_end_regression", REPO_ROOT / "scripts/check-end-to-end-regression.py"
)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)
REFERENCE = json.loads((REPO_ROOT / "docs/performance/reference/amd-epyc-9v45.json").read_text())


def fixture():
    metadata = {
        "runner_label": "ubuntu-24.04", "runner_os": "Linux", "runner_arch": "X64",
        "runner_image_os": "ubuntu24", "runner_image_version": "20261026.354.1",
        "kernel_release": "7.0.0-1017-azure", "machine_architecture": "x86_64",
        "cpu_model": "AMD EPYC 9V45 96-Core Processor", "logical_cpus": 4,
        "memory_bytes": 16760340480, "rust_toolchain": "1.99.1",
        "postgres_server_version": "18.6 (Debian 18.6-1.pgdg13+2)", "nats_server_version": "2.12.15",
        "workload": REFERENCE["workload"], "sample_count": 10,
    }
    rows = [
        {"shape": "fan_out", "repeat_count": 5, "outbox_events_per_run": 896,
         "inbox_events_per_run": 896, "duplicates_per_run": 0, "errors_per_run": 0,
         "median_workflows_per_second": 7.4, "median_p95_workflow_ms": 7800},
        {"shape": "chain", "repeat_count": 5, "outbox_events_per_run": 896,
         "inbox_events_per_run": 896, "duplicates_per_run": 0, "errors_per_run": 0,
         "median_workflows_per_second": 7.2, "median_p95_workflow_ms": 7900},
    ]
    return metadata, rows


class ComparatorTests(unittest.TestCase):
    def test_matching_fingerprint_accepts_one_page_memory_delta(self):
        metadata, rows = fixture()
        status, _ = module.compare(metadata, rows, REFERENCE)
        self.assertEqual(status, "pass")

    def test_exact_throughput_and_latency_thresholds_are_inclusive(self):
        metadata, rows = fixture()
        fan_out_policy = REFERENCE["shapes"]["fan_out"]
        rows[0]["median_workflows_per_second"] = (
            fan_out_policy["median_workflows_per_second"] * 0.90
        )
        rows[0]["median_p95_workflow_ms"] = (
            fan_out_policy["median_p95_workflow_ms"] * 1.15
        )
        status, _ = module.compare(metadata, rows, REFERENCE)
        self.assertEqual(status, "pass")

    def test_different_cpu_skips_without_failing(self):
        metadata, rows = fixture()
        metadata["cpu_model"] = "AMD EPYC 7763 64-Core Processor"
        status, messages = module.compare(metadata, rows, REFERENCE)
        self.assertEqual(status, "skip")
        self.assertIn("no threshold was applied", messages[0])

    def test_workload_mismatch_skips_without_failing(self):
        metadata, rows = fixture()
        metadata["workload"] = dict(metadata["workload"], payload_bytes=512)
        status, _ = module.compare(metadata, rows, REFERENCE)
        self.assertEqual(status, "skip")

    def test_incomplete_sample_count_fails(self):
        metadata, rows = fixture()
        metadata["sample_count"] = 8
        status, _ = module.compare(metadata, rows, REFERENCE)
        self.assertEqual(status, "fail")

    def test_throughput_regression_fails(self):
        metadata, rows = fixture()
        rows[0]["median_workflows_per_second"] = 6.0
        status, _ = module.compare(metadata, rows, REFERENCE)
        self.assertEqual(status, "fail")

    def test_latency_regression_fails(self):
        metadata, rows = fixture()
        rows[1]["median_p95_workflow_ms"] = 10000
        status, _ = module.compare(metadata, rows, REFERENCE)
        self.assertEqual(status, "fail")

    def test_non_finite_throughput_fails_closed(self):
        for value in (float("nan"), float("inf"), float("-inf"), 0.0, -1.0):
            with self.subTest(value=value):
                metadata, rows = fixture()
                rows[0]["median_workflows_per_second"] = value
                status, messages = module.compare(metadata, rows, REFERENCE)
                self.assertEqual(status, "fail")
                self.assertIn("throughput must be finite and positive", messages[0])

    def test_non_finite_or_non_positive_latency_fails_closed(self):
        for value in (float("nan"), float("inf"), float("-inf"), 0.0, -1.0):
            with self.subTest(value=value):
                metadata, rows = fixture()
                rows[1]["median_p95_workflow_ms"] = value
                status, messages = module.compare(metadata, rows, REFERENCE)
                self.assertEqual(status, "fail")
                self.assertIn("p95 latency must be finite and positive", messages[0])

    def test_duplicate_summary_shape_fails(self):
        metadata, rows = fixture()
        rows.append(dict(rows[0]))
        status, messages = module.compare(metadata, rows, REFERENCE)
        self.assertEqual(status, "fail")
        self.assertIn("duplicate fan_out", messages[0])

    def test_unexpected_summary_shape_fails(self):
        metadata, rows = fixture()
        rows.append(dict(rows[0], shape="unexpected"))
        status, messages = module.compare(metadata, rows, REFERENCE)
        self.assertEqual(status, "fail")
        self.assertIn("unsupported summary shape", messages[0])

    def test_empty_or_unreconciled_events_fail(self):
        metadata, rows = fixture()
        rows[0]["outbox_events_per_run"] = 0
        rows[0]["inbox_events_per_run"] = 0
        status, messages = module.compare(metadata, rows, REFERENCE)
        self.assertEqual(status, "fail")
        self.assertIn("empty or do not reconcile", messages[0])

    def test_duplicate_or_unreconciled_events_fail(self):
        metadata, rows = fixture()
        rows[0]["duplicates_per_run"] = 1
        status, _ = module.compare(metadata, rows, REFERENCE)
        self.assertEqual(status, "fail")


if __name__ == "__main__":
    unittest.main()
