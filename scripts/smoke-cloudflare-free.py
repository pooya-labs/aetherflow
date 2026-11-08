#!/usr/bin/env python3
"""Live acceptance smoke test for the deployed Cloudflare Free runtime."""

from __future__ import annotations

import base64
import json
import os
import time
import urllib.error
import urllib.request

API_BASE = os.environ.get(
    "AETHERFLOW_API_BASE",
    "https://aetherflow-free-runtime.pooyagame141.workers.dev",
).rstrip("/")
ORIGIN = "https://pooya-labs.github.io"


def request(path: str, method: str = "GET", body: dict | None = None, token: str | None = None):
    headers = {"Accept": "application/json", "Origin": ORIGIN, "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"}
    data = None
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = "Bearer " + token

    request = urllib.request.Request(API_BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read().decode("utf-8")
            content_type = response.headers.get("Content-Type", "")
            payload = json.loads(raw) if "json" in content_type else raw
            return response.status, payload, response.headers
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8", "replace")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = raw
        raise RuntimeError(f"{method} {path}: HTTP {error.code}: {payload}") from error


def main() -> None:
    status, health, _ = request("/health")
    if status != 200 or health.get("mode") != "cloudflare-workflows-d1":
        raise SystemExit(f"Unexpected Cloudflare health response: {health}")

    preflight = urllib.request.Request(
        API_BASE + "/api/v1/demo/bootstrap",
        headers={
            "Origin": ORIGIN,
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
        method="OPTIONS",
    )
    with urllib.request.urlopen(preflight, timeout=20) as response:
        if response.status != 204 or response.headers.get("Access-Control-Allow-Origin") != ORIGIN:
            raise SystemExit("Cloudflare CORS preflight did not accept the canonical Pages origin.")

    if os.environ.get("AETHERFLOW_FULL_WORKFLOW_SMOKE") != "1":
        print("Cloudflare Free edge smoke: health and CORS: OK (workflow quota preserved)")
        return

    # A stable CI browser identity reuses one isolated workspace and avoids
    # consuming the public visitor ceiling on every push.
    client_id = "00000000-0000-4000-8000-000000000001"
    status, session, _ = request(
        "/api/v1/demo/bootstrap",
        method="POST",
        body={"client_id": client_id},
    )
    token = session.get("sessionToken") if isinstance(session, dict) else None
    if status != 200 or not token:
        raise SystemExit(f"Cloudflare bootstrap did not return a session: {session}")

    run_id = os.environ.get("GITHUB_RUN_ID", str(int(time.time())))
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    workflow_run_id = f"ci-{run_id}-{attempt}"
    encoded_input = base64.b64encode(
        json.dumps({"type": "echo", "value": "cloudflare-free-ci-ok"}).encode("utf-8")
    ).decode("ascii")
    submission = {
        "workflow_id": "github-actions-smoke",
        "workflow_run_id": workflow_run_id,
        "tasks": [
            {
                "task_id": "echo-smoke",
                "depends_on": [],
                "input_base64": encoded_input,
            }
        ],
        "retry_policy": {
            "max_attempts": 2,
            "initial_backoff_ms": 100,
            "max_backoff_ms": 1000,
        },
    }
    status, receipt, _ = request(
        "/api/v1/workflow-runs",
        method="POST",
        body=submission,
        token=token,
    )
    if status != 201 or receipt.get("workflow_run_id") != workflow_run_id:
        raise SystemExit(f"Cloudflare did not accept the workflow submission: {receipt}")

    deadline = time.monotonic() + 45
    snapshot = None
    while time.monotonic() < deadline:
        _, snapshot, _ = request(
            "/api/v1/workflow-runs/" + workflow_run_id,
            token=token,
        )
        if snapshot.get("status") in ("succeeded", "failed", "timed_out", "cancelled"):
            break
        time.sleep(1)

    if not snapshot or snapshot.get("status") != "succeeded":
        raise SystemExit(f"Cloudflare Workflow did not succeed before timeout: {snapshot}")
    tasks = snapshot.get("task_runs", [])
    if len(tasks) != 1 or tasks[0].get("status") != "succeeded":
        raise SystemExit(f"Cloudflare task result did not reach Succeeded: {tasks}")

    _, events, _ = request(
        "/api/v1/workflow-runs/" + workflow_run_id + "/events?limit=100",
        token=token,
    )
    event_types = {event.get("event_type") for event in events.get("events", [])}
    if not {"task.succeeded", "workflow.succeeded"}.issubset(event_types):
        raise SystemExit(f"Cloudflare event history is incomplete: {event_types}")

    retry_workflow_id = f"ci-retry-{run_id}-{attempt}"
    retry_input = base64.b64encode(json.dumps({"type": "fail_once"}).encode("utf-8")).decode("ascii")
    collect_input = base64.b64encode(json.dumps({"type": "collect"}).encode("utf-8")).decode("ascii")
    retry_submission = {
        "workflow_id": "github-actions-retry-dag",
        "workflow_run_id": retry_workflow_id,
        "tasks": [
            {"task_id": "retry-once", "depends_on": [], "input_base64": retry_input},
            {"task_id": "collect-after-retry", "depends_on": ["retry-once"], "input_base64": collect_input},
        ],
        "retry_policy": {
            "max_attempts": 2,
            "initial_backoff_ms": 100,
            "max_backoff_ms": 500,
        },
    }
    status, retry_receipt, _ = request(
        "/api/v1/workflow-runs",
        method="POST",
        body=retry_submission,
        token=token,
    )
    if status != 201 or retry_receipt.get("workflow_run_id") != retry_workflow_id:
        raise SystemExit(f"Cloudflare did not accept the retry/DAG workflow: {retry_receipt}")

    retry_snapshot = None
    while time.monotonic() < deadline:
        _, retry_snapshot, _ = request(
            "/api/v1/workflow-runs/" + retry_workflow_id,
            token=token,
        )
        if retry_snapshot.get("status") in ("succeeded", "failed", "timed_out", "cancelled"):
            break
        time.sleep(1)

    if not retry_snapshot or retry_snapshot.get("status") != "succeeded":
        raise SystemExit(f"Cloudflare retry/DAG workflow did not succeed: {retry_snapshot}")
    retry_tasks = {task.get("task_id"): task for task in retry_snapshot.get("task_runs", [])}
    if set(retry_tasks) != {"retry-once", "collect-after-retry"} or any(
        task.get("status") != "succeeded" for task in retry_tasks.values()
    ):
        raise SystemExit(f"Cloudflare dependency chain did not complete: {retry_tasks}")

    _, retry_task, _ = request(
        "/api/v1/task-runs/" + retry_tasks["retry-once"]["task_run_id"],
        token=token,
    )
    if retry_task.get("attempts_started") != 2:
        raise SystemExit(f"Cloudflare built-in retry did not run exactly twice: {retry_task}")

    _, retry_events, _ = request(
        "/api/v1/workflow-runs/" + retry_workflow_id + "/events?limit=100",
        token=token,
    )
    retry_event_types = {event.get("event_type") for event in retry_events.get("events", [])}
    if not {"task.retry_waiting", "task.succeeded", "workflow.succeeded"}.issubset(retry_event_types):
        raise SystemExit(f"Cloudflare retry/DAG event history is incomplete: {retry_event_types}")

    print("Cloudflare Free live smoke: health, CORS, bootstrap, submit, durable execution, retry, DAG dependencies, D1 state and event history: OK")


if __name__ == "__main__":
    main()
