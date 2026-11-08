#!/usr/bin/env python3
"""Real Chromium smoke and interaction tests for the AetherFlow Pages experience."""

from __future__ import annotations

import functools
import json
import os
import threading
import unittest
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright


ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = Path(os.environ.get("BROWSER_QA_ARTIFACTS", ROOT / "artifacts" / "browser-qa"))
TOKEN = "browser-qa-token-" + ("x" * 32)
API_BASE = "https://api.aetherflow.test"
FIXED_TIME = "2026-11-08T12:00:00Z"


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, _format: str, *_args: object) -> None:
        pass


def json_response(route, payload: dict, status: int = 200) -> None:
    route.fulfill(
        status=status,
        content_type="application/json",
        headers={"cache-control": "no-store"},
        body=json.dumps(payload),
    )


def text_response(route, payload: str, status: int = 200) -> None:
    route.fulfill(
        status=status,
        content_type="text/plain; version=0.0.4; charset=utf-8",
        headers={"cache-control": "no-store"},
        body=payload,
    )


class AetherFlowBrowserQA(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        handler = functools.partial(QuietHandler, directory=str(ROOT))
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        cls.server.daemon_threads = True
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_address[1]}"

        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        cls.playwright = sync_playwright().start()
        launch_options = {"headless": True, "args": ["--no-sandbox", "--disable-dev-shm-usage"]}
        executable = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE")
        if executable:
            launch_options["executable_path"] = executable
        cls.browser: Browser = cls.playwright.chromium.launch(**launch_options)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.browser.close()
        cls.playwright.stop()
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join(timeout=3)

    def setUp(self) -> None:
        self.context: BrowserContext = self.browser.new_context(
            viewport={"width": 1440, "height": 1000},
            device_scale_factor=1,
            reduced_motion="reduce",
        )
        self.page: Page = self.context.new_page()
        self.page_errors: list[str] = []
        self.page.on("pageerror", lambda error: self.page_errors.append(str(error)))

    def tearDown(self) -> None:
        self.context.close()

    def assert_no_page_errors(self) -> None:
        self.assertEqual(self.page_errors, [], "Uncaught browser errors: " + " | ".join(self.page_errors))

    def mobile_width_is_safe(self) -> None:
        dimensions = self.page.evaluate(
            "() => ({viewport: window.innerWidth, document: document.documentElement.scrollWidth})"
        )
        self.assertLessEqual(
            dimensions["document"],
            dimensions["viewport"] + 1,
            f"Horizontal overflow at mobile width: {dimensions}",
        )

    def test_product_desktop_content_theme_and_clipboard(self) -> None:
        page = self.page
        page.goto(self.base_url + "/", wait_until="networkidle")

        self.assertIn("AetherFlow", page.title())
        owner = page.locator(".owner-credit")
        self.assertEqual(owner.get_attribute("href"), "https://pooya-labs.github.io/pooya-labs-platform/")
        self.assertIn("Pooya Labs Platform", owner.inner_text())
        self.assertEqual(owner.locator("img").get_attribute("src"), "assets/brand/pooya-monogram.webp")
        owner.scroll_into_view_if_needed()
        page.wait_for_function(
            "() => { const image = document.querySelector('.owner-credit img'); return Boolean(image && image.complete && image.naturalWidth > 0); }"
        )
        self.assertGreater(owner.locator("img").evaluate("img => img.naturalWidth"), 0)
        self.assertIn("Engineered for recovery", page.locator(".hero h1").inner_text())
        self.assertEqual(page.locator(".architecture .arch-card").count(), 4)
        self.assertEqual(page.locator(".cap-grid .cap-card").count(), 6)
        self.assertEqual(page.locator(".recovery-grid .recovery-card").count(), 4)
        self.assertEqual(page.locator(".docs-grid .doc-card").count(), 6)
        self.assertIn("Stable 1.0.0 is being prepared", page.locator(".release-panel").inner_text())
        self.assertIn("Not yet tagged as v1.0.0", page.locator(".release-state").inner_text())
        self.assertNotIn("v0 · experimental", page.locator("body").inner_text())
        self.assertNotIn("RUNNING", page.locator("body").inner_text())

        missing_anchors = page.locator('a[href^="#"]').evaluate_all(
            """links => links
              .map(link => link.getAttribute('href'))
              .filter(href => href && href.length > 1 && !document.querySelector(href))"""
        )
        self.assertEqual(missing_anchors, [], "Broken in-page navigation anchors")

        page.screenshot(path=str(ARTIFACTS / "product-desktop.png"), full_page=True)

        page.locator("#theme-toggle").click()
        self.assertEqual(page.locator("html").get_attribute("data-theme"), "light")
        self.assertEqual(page.evaluate("localStorage.getItem('aetherflow-theme')"), "light")
        page.reload(wait_until="networkidle")
        self.assertEqual(page.locator("html").get_attribute("data-theme"), "light")
        page.locator("#theme-toggle").click()
        self.assertEqual(page.locator("html").get_attribute("data-theme"), "dark")

        page.context.grant_permissions(["clipboard-read", "clipboard-write"], origin=self.base_url)
        page.locator('[data-copy="clone-code"]').click()
        page.wait_for_function("navigator.clipboard.readText().then(text => text.includes('git clone'))")
        clipboard = page.evaluate("navigator.clipboard.readText()")
        self.assertIn("git clone https://github.com/pooya-labs/aetherflow.git", clipboard)
        self.assert_no_page_errors()

    def test_product_mobile_navigation_and_layout(self) -> None:
        page = self.page
        page.set_viewport_size({"width": 390, "height": 844})
        page.goto(self.base_url + "/", wait_until="networkidle")

        page.locator("#menu-toggle").click()
        self.assertEqual(page.locator("#primary-nav").get_attribute("class"), "nav-links open")
        self.assertEqual(page.locator("#menu-toggle").get_attribute("aria-expanded"), "true")
        page.locator('#primary-nav a[href="#documentation"]').click()
        self.assertNotIn("open", page.locator("#primary-nav").get_attribute("class"))
        self.assertEqual(page.locator("#menu-toggle").get_attribute("aria-expanded"), "false")
        self.mobile_width_is_safe()

        page.screenshot(path=str(ARTIFACTS / "product-mobile.png"), full_page=True)
        self.assert_no_page_errors()

    def test_dashboard_is_honest_before_api_connection(self) -> None:
        page = self.page
        api_requests: list[str] = []
        page.on("request", lambda request: api_requests.append(request.url) if "/api/v1/" in request.url else None)
        page.goto(self.base_url + "/dashboard/", wait_until="networkidle")

        self.assertIn("Control console", page.locator(".welcome h1").inner_text())
        owner = page.locator(".owner-credit")
        self.assertEqual(owner.get_attribute("href"), "https://pooya-labs.github.io/pooya-labs-platform/")
        self.assertIn("Pooya Labs Platform", owner.inner_text())
        self.assertEqual(owner.locator("img").get_attribute("src"), "../assets/brand/pooya-monogram.webp")
        self.assertGreater(owner.locator("img").evaluate("img => img.naturalWidth"), 0)
        self.assertIn("Not connected", page.locator("#connectionStatus").inner_text())
        self.assertEqual(page.locator("#apiToken").get_attribute("type"), "password")
        self.assertEqual(page.locator("#statTotal").inner_text(), "—")
        self.assertIn("No data loaded", page.locator("#runList").inner_text())
        self.assertEqual(api_requests, [], "The disconnected dashboard must not call an API.")
        self.assertEqual(self.context.cookies(), [])
        self.assert_no_page_errors()

    def test_dashboard_rejects_remote_plain_http_before_sending_credentials(self) -> None:
        page = self.page
        api_requests: list[str] = []
        page.on("request", lambda request: api_requests.append(request.url) if "/api/v1/" in request.url else None)
        page.goto(self.base_url + "/dashboard/", wait_until="networkidle")
        page.locator("#apiBase").fill("http://api.example.test")
        page.locator("#apiToken").fill(TOKEN)
        page.locator("#connectButton").click()

        self.assertIn(
            "Remote API endpoints must use HTTPS",
            page.locator("#connectionStatus").inner_text(),
        )
        self.assertEqual(api_requests, [], "Credentials must not be sent to a remote HTTP endpoint.")
        self.assert_no_page_errors()

    def test_dashboard_reads_api_and_keeps_credentials_ephemeral(self) -> None:
        page = self.page
        requests: list[dict[str, str]] = []

        run_page = {
            "items": [
                {
                    "workflow_run_id": "run-browser-qa-001",
                    "workflow_id": "browser-qa-demo",
                    "status": "succeeded",
                    "version": 3,
                    "task_count": 1,
                    "created_at": FIXED_TIME,
                    "updated_at": FIXED_TIME,
                }
            ],
            "next_cursor": None,
        }
        snapshot = {
            "workflow_run_id": "run-browser-qa-001",
            "workflow_id": "browser-qa-demo",
            "status": "succeeded",
            "version": 3,
            "created_at": FIXED_TIME,
            "updated_at": FIXED_TIME,
            "task_runs": [
                {
                    "task_id": "task-browser-qa-001",
                    "task_run_id": "task-run-browser-qa-001",
                    "status": "succeeded",
                    "version": 3,
                    "attempts_started": 1,
                }
            ],
        }
        events = {
            "events": [
                {
                    "event_id": 101,
                    "event_type": "task.succeeded",
                    "aggregate_type": "task_run",
                    "aggregate_id": "task-run-browser-qa-001",
                    "created_at": FIXED_TIME,
                    "published_at": FIXED_TIME,
                    "status": "published",
                }
            ],
            "next_cursor": None,
        }
        task_snapshot = {
            "task_run_id": "task-run-browser-qa-001",
            "task_id": "task-browser-qa-001",
            "status": "succeeded",
            "version": 3,
            "attempts_started": 1,
            "input_size_bytes": 32,
            "output_present": True,
            "output_size_bytes": 64,
            "available_at": FIXED_TIME,
            "claimed_by": "worker-qa",
            "lease_expires_at": FIXED_TIME,
        }

        def mock_api(route) -> None:
            request = route.request
            parsed = urlparse(request.url)
            requests.append(
                {
                    "url": request.url,
                    "path": parsed.path,
                    "authorization": request.headers.get("authorization", ""),
                }
            )
            if parsed.path == "/api/v1/workflow-runs":
                json_response(route, run_page)
            elif parsed.path == "/api/v1/workflow-runs/run-browser-qa-001":
                json_response(route, snapshot)
            elif parsed.path == "/api/v1/workflow-runs/run-browser-qa-001/events":
                json_response(route, events)
            elif parsed.path == "/api/v1/task-runs/task-run-browser-qa-001":
                json_response(route, task_snapshot)
            elif parsed.path == "/api/v1/tenant/metrics":
                json_response(route, {"error": {"message": "Operator must not request tenant metrics"}}, status=403)
            else:
                json_response(route, {"error": {"message": "Unexpected test route"}}, status=404)

        page.route(API_BASE + "/**", mock_api)
        page.goto(self.base_url + "/dashboard/", wait_until="networkidle")
        page.locator("#apiBase").fill(API_BASE)
        page.locator("#apiToken").fill(TOKEN)
        page.locator("#connectButton").click()

        page.get_by_text("Connected · live data").wait_for()
        page.locator(".run-item").filter(has_text="run-browser-qa-001").wait_for()
        page.get_by_text("task.succeeded · task_run").wait_for()
        self.assertEqual(page.locator("#statTotal").inner_text(), "1")
        self.assertEqual(page.locator("#statSucceeded").inner_text(), "1")
        run_meta = page.locator(".run-item .run-meta").inner_text()
        self.assertIn("1 task", run_meta)
        self.assertIn("v3", run_meta)
        self.assertNotIn("undefined", run_meta)
        self.assertIn("browser-qa-demo", page.locator("#detailContent").inner_text())
        self.assertEqual(page.locator("#apiToken").input_value(), "")
        self.assertTrue(page.locator("#opsSnapshot").is_hidden())
        self.assertNotIn(
            "/api/v1/tenant/metrics",
            [item["path"] for item in requests],
            "Operator scope must never request tenant-scoped operational telemetry.",
        )

        storage = page.evaluate(
            """() => ({
              local: Object.fromEntries(Object.entries(localStorage)),
              session: Object.fromEntries(Object.entries(sessionStorage))
            })"""
        )
        self.assertNotIn(TOKEN, json.dumps(storage))
        self.assertNotIn(TOKEN, json.dumps(self.context.cookies()))
        self.assertTrue(requests)
        self.assertTrue(all(item["authorization"] == "Bearer " + TOKEN for item in requests))
        self.assertTrue(all(TOKEN not in item["url"] for item in requests))

        page.locator("#detailContent button").filter(has_text="Inspect").click()
        page.get_by_text("worker-qa", exact=True).wait_for()
        self.assertIn("64 bytes", page.locator("#detailContent").inner_text())

        page.screenshot(path=str(ARTIFACTS / "dashboard-connected.png"), full_page=True)
        page.locator("#disconnectButton").click()
        self.assertIn("Not connected", page.locator("#connectionStatus").inner_text())
        self.assertEqual(page.locator("#statTotal").inner_text(), "—")
        self.assertEqual(page.locator("#apiToken").input_value(), "")
        self.assert_no_page_errors()

    def test_dashboard_paginates_tenant_workflows_and_event_history(self) -> None:
        page = self.page
        requests: list[dict[str, str]] = []

        workflow_pages = [
            {
                "items": [
                    {
                        "workflow_run_id": "run-tenant-001",
                        "workflow_id": "tenant-sync",
                        "status": "succeeded",
                        "version": 1,
                        "task_count": 0,
                        "created_at": FIXED_TIME,
                        "updated_at": FIXED_TIME,
                    }
                ],
                "next_cursor": "workflow-page-2",
            },
            {
                "items": [
                    {
                        "workflow_run_id": "run-tenant-002",
                        "workflow_id": "tenant-export",
                        "status": "running",
                        "version": 2,
                        "task_count": 2,
                        "created_at": FIXED_TIME,
                        "updated_at": FIXED_TIME,
                    }
                ],
                "next_cursor": None,
            },
        ]
        snapshot = {
            "workflow_run_id": "run-tenant-001",
            "workflow_id": "tenant-sync",
            "status": "succeeded",
            "version": 1,
            "created_at": FIXED_TIME,
            "updated_at": FIXED_TIME,
            "task_runs": [],
        }
        metrics_exposition = """# HELP aetherflow_tenant_workflow_completions_last_30m recent workflow outcomes
# TYPE aetherflow_tenant_workflow_completions_last_30m gauge
aetherflow_tenant_workflow_completions_last_30m{status="succeeded"} 4
aetherflow_tenant_workflow_completions_last_30m{status="failed"} 1
aetherflow_tenant_workflow_completions_last_30m{status="cancelled"} 2
aetherflow_tenant_workflow_completions_last_30m{status="timed_out"} 0
aetherflow_tenant_workflow_success_completion_samples_last_30m 4
aetherflow_tenant_workflow_success_completion_p50_seconds_last_30m 2.250000
aetherflow_tenant_workflow_success_completion_p95_seconds_last_30m 5.000000
aetherflow_tenant_workflow_success_completion_p99_seconds_last_30m 8.000000
aetherflow_tenant_outbox_events{state="pending"} 3
aetherflow_tenant_outbox_events{state="ready"} 2
aetherflow_tenant_outbox_oldest_ready_lag_seconds 3599.900000
aetherflow_tenant_outbox_expired_claims 1
aetherflow_tenant_inbox_events{state="pending"} 2
aetherflow_tenant_inbox_events{state="processing"} 1
aetherflow_tenant_inbox_oldest_unprocessed_age_seconds 12.500000
aetherflow_tenant_inbox_expired_processing_leases 1
aetherflow_tenant_task_expired_running_leases 0
"""
        event_pages = [
            {
                "events": [
                    {
                        "event_id": 201,
                        "event_type": "task.succeeded",
                        "aggregate_type": "task_run",
                        "aggregate_id": "task-tenant-001",
                        "created_at": FIXED_TIME,
                        "published_at": FIXED_TIME,
                        "status": "published",
                    }
                ],
                "next_cursor": "older-events-2",
            },
            {
                "events": [
                    {
                        "event_id": 200,
                        "event_type": "task.started",
                        "aggregate_type": "task_run",
                        "aggregate_id": "task-tenant-001",
                        "created_at": FIXED_TIME,
                        "published_at": None,
                        "status": "pending",
                    }
                ],
                "next_cursor": None,
            },
        ]

        def mock_api(route) -> None:
            parsed = urlparse(route.request.url)
            requests.append({"path": parsed.path, "query": parsed.query})
            query = parse_qs(parsed.query)
            if parsed.path == "/api/v1/tenant/workflow-runs":
                page_index = 1 if query.get("cursor") == ["workflow-page-2"] else 0
                json_response(route, workflow_pages[page_index])
            elif parsed.path == "/api/v1/tenant/workflow-runs/run-tenant-001":
                json_response(route, snapshot)
            elif parsed.path == "/api/v1/tenant/workflow-runs/run-tenant-001/events":
                page_index = 1 if query.get("cursor") == ["older-events-2"] else 0
                json_response(route, event_pages[page_index])
            elif parsed.path == "/api/v1/tenant/metrics":
                text_response(route, metrics_exposition)
            else:
                json_response(route, {"error": {"message": "Unexpected tenant test route"}}, status=404)

        page.route(API_BASE + "/**", mock_api)
        page.goto(self.base_url + "/dashboard/", wait_until="networkidle")
        page.locator("#apiBase").fill(API_BASE)
        page.locator("#scope").select_option("tenant")
        page.locator("#apiToken").fill(TOKEN)
        page.locator("#connectButton").click()

        page.get_by_text("Connected · live data").wait_for()
        page.locator("#opsSnapshot").wait_for(state="visible")
        page.locator(".run-item").filter(has_text="run-tenant-001").wait_for()
        self.assertEqual(page.locator("#opsSuccess").inner_text(), "4")
        self.assertEqual(page.locator("#opsFailed").inner_text(), "1")
        self.assertEqual(page.locator("#opsCompletionSamples").inner_text(), "4")
        self.assertEqual(page.locator("#opsP95").inner_text(), "5.0 s")
        self.assertIn("acceptance to terminal (queue included)", page.locator("#opsSnapshotStatus").inner_text())
        self.assertEqual(page.locator("#opsOutboxPending").inner_text(), "3")
        self.assertEqual(page.locator("#opsOutboxLag").inner_text(), "59m 59s")
        self.assertEqual(page.locator("#opsInboxProcessing").inner_text(), "1")
        page.get_by_text("task.succeeded · task_run").wait_for()
        self.assertTrue(page.locator("#workersButton").is_disabled())
        self.assertEqual(page.locator(".event-row").count(), 1)

        page.get_by_role("button", name="Load older events").click()
        page.get_by_text("task.started · task_run").wait_for()
        self.assertEqual(page.locator(".event-row").count(), 2)

        page.locator("#loadMore").click()
        page.locator(".run-item").filter(has_text="run-tenant-002").wait_for()
        self.assertEqual(page.locator(".run-item").count(), 2)
        self.assertEqual(page.locator("#statTotal").inner_text(), "2")

        paths = [request["path"] for request in requests]
        self.assertIn("/api/v1/tenant/metrics", paths)
        self.assertTrue(paths)
        self.assertTrue(
            all(path.startswith("/api/v1/tenant/") for path in paths),
            "Tenant scope must never call operator endpoints: " + ", ".join(paths),
        )
        self.assertIn(
            "cursor=older-events-2",
            next(request["query"] for request in requests if request["path"].endswith("/events") and "cursor=" in request["query"]),
        )
        self.assertIn(
            "cursor=workflow-page-2",
            next(request["query"] for request in requests if request["path"] == "/api/v1/tenant/workflow-runs" and "cursor=" in request["query"]),
        )
        page.set_viewport_size({"width": 390, "height": 844})
        self.mobile_width_is_safe()
        self.assert_no_page_errors()

    def test_dashboard_ignores_stale_workflow_selection_responses(self) -> None:
        page = self.page
        page.add_init_script(
            """
            (() => {
              window.__apiRequests = [];
              window.fetch = (url, options = {}) => new Promise((resolve, reject) => {
                const request = { url: String(url), options, resolve, reject };
                window.__apiRequests.push(request);
                if (options.signal) {
                  options.signal.addEventListener(
                    "abort",
                    () => reject(new DOMException("The request was aborted.", "AbortError")),
                    { once: true }
                  );
                }
              });
              window.__respondApi = (index, payload, status = 200) => {
                const request = window.__apiRequests[index];
                request.resolve({
                  ok: status >= 200 && status < 300,
                  status,
                  text: async () => JSON.stringify(payload)
                });
              };
            })();
            """
        )
        page.goto(self.base_url + "/dashboard/", wait_until="networkidle")
        page.locator("#apiBase").fill(API_BASE)
        page.locator("#apiToken").fill(TOKEN)
        page.locator("#connectButton").click()
        page.wait_for_function("window.__apiRequests.length === 1")

        def respond(index: int, payload: dict) -> None:
            page.evaluate(
                "([index, payload]) => window.__respondApi(index, payload)",
                [index, payload],
            )

        respond(
            0,
            {
                "items": [
                    {
                        "workflow_run_id": "run-race-a",
                        "workflow_id": "workflow-a",
                        "status": "running",
                        "version": 1,
                        "task_count": 0,
                        "created_at": FIXED_TIME,
                        "updated_at": FIXED_TIME,
                    },
                    {
                        "workflow_run_id": "run-race-b",
                        "workflow_id": "workflow-b",
                        "status": "succeeded",
                        "version": 2,
                        "task_count": 0,
                        "created_at": FIXED_TIME,
                        "updated_at": FIXED_TIME,
                    },
                ],
                "next_cursor": None,
            },
        )
        page.locator(".run-item").filter(has_text="run-race-a").wait_for()
        page.wait_for_function("window.__apiRequests.length === 2")

        page.locator(".run-item").filter(has_text="run-race-b").click()
        page.wait_for_function("window.__apiRequests.length === 3")

        respond(
            2,
            {
                "workflow_run_id": "run-race-b",
                "workflow_id": "workflow-b",
                "status": "succeeded",
                "version": 2,
                "created_at": FIXED_TIME,
                "updated_at": FIXED_TIME,
                "task_runs": [],
            },
        )
        page.wait_for_function("window.__apiRequests.length === 4")
        respond(3, {"events": [], "next_cursor": None})
        page.locator("#detailContent .detail-subtitle").filter(has_text="workflow-b").wait_for()

        # The first selection resolves last. It must not trigger an event fetch
        # or replace the newer workflow currently shown in the console.
        respond(
            1,
            {
                "workflow_run_id": "run-race-a",
                "workflow_id": "workflow-a",
                "status": "running",
                "version": 1,
                "created_at": FIXED_TIME,
                "updated_at": FIXED_TIME,
                "task_runs": [],
            },
        )
        page.wait_for_timeout(100)
        self.assertEqual(page.evaluate("window.__apiRequests.length"), 4)
        self.assertIn("run-race-b", page.locator("#detailContent").inner_text())
        self.assertNotIn("run-race-a", page.locator("#detailContent").inner_text())
        self.assert_no_page_errors()

    def test_dashboard_disconnect_cancels_pending_api_requests(self) -> None:
        page = self.page
        page.add_init_script(
            """
            (() => {
              window.__apiRequests = [];
              window.fetch = (url, options = {}) => new Promise((resolve, reject) => {
                const request = { url: String(url), options, resolve, reject };
                window.__apiRequests.push(request);
                if (options.signal) {
                  options.signal.addEventListener(
                    "abort",
                    () => reject(new DOMException("The request was aborted.", "AbortError")),
                    { once: true }
                  );
                }
              });
              window.__respondApi = (index, payload, status = 200) => {
                const request = window.__apiRequests[index];
                request.resolve({
                  ok: status >= 200 && status < 300,
                  status,
                  text: async () => JSON.stringify(payload)
                });
              };
            })();
            """
        )
        page.goto(self.base_url + "/dashboard/", wait_until="networkidle")
        page.locator("#apiBase").fill(API_BASE)
        page.locator("#apiToken").fill(TOKEN)
        page.locator("#connectButton").click()
        page.wait_for_function("window.__apiRequests.length === 1")

        self.assertFalse(page.locator("#disconnectButton").is_disabled())
        page.locator("#disconnectButton").click()
        page.evaluate(
            """() => window.__respondApi(0, {
              items: [{
                workflow_run_id: "late-run",
                workflow_id: "late-workflow",
                status: "succeeded",
                version: 1,
                task_count: 0
              }],
              next_cursor: null
            })"""
        )
        page.wait_for_timeout(100)

        self.assertIn("Not connected", page.locator("#connectionStatus").inner_text())
        self.assertIn("Disconnected", page.locator("#runList").inner_text())
        self.assertEqual(page.locator("#statTotal").inner_text(), "—")
        self.assertEqual(page.locator(".run-item").count(), 0)
        self.assert_no_page_errors()

    def test_dashboard_runs_a_sample_workflow_on_cloudflare_free(self) -> None:
        page = self.page
        created: list[dict] = []

        def runtime_config(route) -> None:
            route.fulfill(
                status=200,
                content_type="application/javascript",
                body=(
                    "window.AETHERFLOW_RUNTIME_CONFIG = Object.freeze({"
                    + "mode: 'cloudflare-free', "
                    + "apiBase: '"
                    + API_BASE
                    + "', bootstrapPath: '/api/v1/demo/bootstrap'});"
                ),
            )

        def mock_api(route) -> None:
            request = route.request
            parsed = urlparse(request.url)
            if parsed.path == "/api/v1/demo/bootstrap":
                json_response(
                    route,
                    {
                        "sessionToken": "cloudflare-browser-qa-session-" + ("x" * 32),
                        "expiresInSeconds": 21600,
                        "workspace": {"id": "workspace-browser-qa", "name": "Browser QA"},
                        "user": {"name": "Browser QA"},
                    },
                )
                return

            self.assertEqual(request.headers.get("authorization"), "Bearer cloudflare-browser-qa-session-" + ("x" * 32))
            if parsed.path == "/api/v1/workflow-runs" and request.method == "POST":
                body = request.post_data_json
                created.append(body)
                json_response(
                    route,
                    {
                        "workflow_id": body["workflow_id"],
                        "workflow_run_id": body["workflow_run_id"],
                        "status": "pending",
                        "version": 1,
                        "task_runs": [
                            {"task_id": task["task_id"], "task_run_id": "tr-" + task["task_id"], "status": "ready" if not task.get("depends_on") else "pending"}
                            for task in body["tasks"]
                        ],
                    },
                    status=201,
                )
                return

            if parsed.path == "/api/v1/workflow-runs":
                run_id = created[-1]["workflow_run_id"] if created else "unused"
                items = []
                if created:
                    items = [{
                        "workflow_id": "serverless-showcase",
                        "workflow_run_id": run_id,
                        "status": "pending",
                        "version": 1,
                        "task_count": 3,
                        "created_at": FIXED_TIME,
                        "updated_at": FIXED_TIME,
                    }]
                json_response(route, {"items": items, "next_cursor": None})
                return

            if parsed.path.startswith("/api/v1/workflow-runs/") and parsed.path.endswith("/events"):
                json_response(route, {"events": [], "next_cursor": None})
                return

            if parsed.path.startswith("/api/v1/workflow-runs/"):
                run_id = created[-1]["workflow_run_id"]
                json_response(
                    route,
                    {
                        "workflow_id": "serverless-showcase",
                        "workflow_run_id": run_id,
                        "status": "pending",
                        "version": 1,
                        "created_at": FIXED_TIME,
                        "updated_at": FIXED_TIME,
                        "task_runs": [
                            {"task_id": "format-message", "task_run_id": "tr-format-message", "status": "ready"},
                            {"task_id": "fingerprint-message", "task_run_id": "tr-fingerprint-message", "status": "ready"},
                            {"task_id": "collect-results", "task_run_id": "tr-collect-results", "status": "pending"},
                        ],
                    },
                )
                return

            json_response(route, {"error": {"message": "Unexpected test route: " + parsed.path}}, status=404)

        page.route("**/runtime-config.js", runtime_config)
        page.route(API_BASE + "/**", mock_api)
        page.goto(self.base_url + "/dashboard/", wait_until="networkidle")

        page.get_by_text("Connected · Cloudflare Workflows + D1").wait_for()
        page.locator("#runSample").wait_for(state="visible")
        self.assertFalse(page.locator("#runSample").is_disabled())

        page.locator("#runSample").click()
        page.get_by_text("Workflow accepted", exact=False).wait_for()
        page.locator(".run-item .run-workflow").filter(has_text="serverless-showcase").wait_for()
        page.locator("#detailContent .task-id").filter(has_text="collect-results").wait_for()

        self.assertEqual(len(created), 1)
        submission = created[0]
        self.assertEqual(submission["workflow_id"], "serverless-showcase")
        self.assertEqual(
            [task["task_id"] for task in submission["tasks"]],
            ["format-message", "fingerprint-message", "collect-results"],
        )
        self.assertEqual(
            submission["tasks"][2]["depends_on"],
            ["format-message", "fingerprint-message"],
        )
        self.assertTrue(all(task.get("input_base64") for task in submission["tasks"]))
        self.assert_no_page_errors()

    def test_dashboard_hides_sample_submission_for_manual_profile(self) -> None:
        page = self.page
        page.goto(self.base_url + "/dashboard/", wait_until="networkidle")
        self.assertTrue(page.locator("#runSample").is_hidden())

    def test_dashboard_persists_sample_workflow_on_apps_script_profile(self) -> None:
        page = self.page

        bridge_mock = r"""
        (() => {
          const KEY = "browser-qa-apps-script-sheet";
          const seeded = [
            ["demo-invoice-001", "invoice-pipeline", "succeeded", 3],
            ["demo-media-002", "media-export", "running", 3],
            ["demo-index-003", "nightly-index", "failed", 2],
            ["demo-retry-004", "retry-example", "running", 2]
          ];
          const now = "2026-11-08T12:00:00.000Z";
          function load() {
            try {
              const stored = JSON.parse(localStorage.getItem(KEY) || "null");
              if (stored && Array.isArray(stored.runs)) return stored;
            } catch (_) {}
            const runs = seeded.map(([id, workflow, status, taskCount], index) => ({
              workflow_run_id: id, workflow_id: workflow, status, version: status === "failed" || status === "succeeded" ? 4 : 2,
              task_count: taskCount, created_at: new Date(Date.parse(now) - (index + 1) * 3600000).toISOString(), updated_at: now
            }));
            const store = { runs, snapshots: {}, events: {} };
            localStorage.setItem(KEY, JSON.stringify(store));
            return store;
          }
          function save(store) { localStorage.setItem(KEY, JSON.stringify(store)); }
          const store = load();
          window.__appsScriptBridgeRequests = [];
          window.AetherFlowAppsScriptBridge = Object.freeze({
            isActive: () => true,
            start: async () => ({ mode: "apps-script-demo", expiresInSeconds: 21600 }),
            request: async ({ method, path, body = {} }) => {
              window.__appsScriptBridgeRequests.push({ method, path, body });
              const pathname = path.split("?")[0];
              if (method === "GET" && pathname === "/api/v1/workflow-runs") {
                return { status: 200, body: { items: store.runs.slice().sort((a,b) => b.created_at.localeCompare(a.created_at)), next_cursor: null } };
              }
              if (method === "POST" && pathname === "/api/v1/workflow-runs") {
                if (store.runs.some(run => run.workflow_run_id === body.workflow_run_id)) {
                  return { status: 409, body: { error: { message: "workflow_conflict" } } };
                }
                const stamp = new Date().toISOString();
                const snapshot = {
                  workflow_id: body.workflow_id, workflow_run_id: body.workflow_run_id, status: "pending", version: 1,
                  created_at: stamp, updated_at: stamp,
                  task_runs: body.tasks.map(task => ({
                    task_id: task.task_id, task_run_id: "tr-" + task.task_id, status: "pending",
                    version: 1, attempts_started: 0, input_size_bytes: 0, output_present: false
                  }))
                };
                store.runs.unshift({
                  workflow_id: body.workflow_id, workflow_run_id: body.workflow_run_id, status: "pending", version: 1,
                  task_count: body.tasks.length, created_at: stamp, updated_at: stamp
                });
                store.snapshots[body.workflow_run_id] = snapshot;
                store.events[body.workflow_run_id] = [{
                  event_id: 1001, event_type: "workflow.submitted", aggregate_type: "workflow_run",
                  aggregate_id: body.workflow_run_id, created_at: stamp, published_at: null, status: "recorded"
                }, {
                  event_id: 1002, event_type: "task.pending", aggregate_type: "task_run",
                  aggregate_id: "tr-stored-only-record", created_at: stamp, published_at: null, status: "recorded"
                }];
                save(store);
                return { status: 201, body: {
                  workflow_id: body.workflow_id, workflow_run_id: body.workflow_run_id, status: "pending", version: 1,
                  task_runs: snapshot.task_runs.map(task => ({ task_id: task.task_id, task_run_id: task.task_run_id, status: task.status }))
                }};
              }
              const detailMatch = pathname.match(/^\/api\/v1\/workflow-runs\/([A-Za-z0-9_-]+)$/);
              if (method === "GET" && detailMatch) {
                const run = store.runs.find(item => item.workflow_run_id === detailMatch[1]);
                const snapshot = store.snapshots[detailMatch[1]];
                if (snapshot) return { status: 200, body: snapshot };
                if (run) return { status: 200, body: {
                  workflow_id: run.workflow_id, workflow_run_id: run.workflow_run_id, status: run.status, version: run.version,
                  created_at: run.created_at, updated_at: run.updated_at, task_runs: []
                }};
                return { status: 404, body: { error: { message: "not_found" } } };
              }
              const eventsMatch = pathname.match(/^\/api\/v1\/workflow-runs\/([A-Za-z0-9_-]+)\/events$/);
              if (method === "GET" && eventsMatch) {
                return { status: 200, body: { events: store.events[eventsMatch[1]] || [], next_cursor: null } };
              }
              return { status: 404, body: { error: { message: "Unexpected bridge route: " + method + " " + pathname } } };
            }
          });
        })();
        """
        page.route(
            "**/apps-script-bridge.js",
            lambda route: route.fulfill(status=200, content_type="application/javascript", body=bridge_mock),
        )
        page.goto(self.base_url + "/dashboard/?aetherflowRuntime=apps-script", wait_until="networkidle")

        page.get_by_text("Connected · synthetic demo data").wait_for()
        page.locator(".run-item").filter(has_text="demo-index-003").wait_for()
        page.locator(".run-item").filter(has_text="demo-retry-004").wait_for()
        self.assertEqual(page.locator(".run-item").count(), 4)

        store_button = page.locator("#runSample")
        store_button.wait_for(state="visible")
        self.assertIn("Store persistence test", store_button.inner_text())
        self.assertFalse(store_button.is_disabled())
        store_button.click()

        page.get_by_text("Workflow stored · task not executed", exact=False).wait_for()
        new_run = page.evaluate(
            """() => {
              const data = JSON.parse(localStorage.getItem("browser-qa-apps-script-sheet"));
              return data.runs.find(run => run.workflow_id === "apps-script-persistence-smoke");
            }"""
        )
        self.assertIsNotNone(new_run)
        self.assertEqual(new_run["status"], "pending")
        workflow_id = new_run["workflow_run_id"]
        self.assertTrue(workflow_id.startswith("gas-smoke-"))
        self.assertEqual(page.locator(".run-item").count(), 5)
        page.locator(".run-item").filter(has_text=workflow_id).wait_for()
        self.assertIn("stored-only-record", page.locator("#detailContent").inner_text())
        self.assertIn("task.pending · task_run", page.locator("#detailContent").inner_text())
        self.assertIn("workflow.submitted · workflow_run", page.locator("#detailContent").inner_text())

        post_requests = page.evaluate(
            "() => window.__appsScriptBridgeRequests.filter(request => request.method === 'POST')"
        )
        self.assertEqual(len(post_requests), 1)
        self.assertEqual(post_requests[0]["path"], "/api/v1/workflow-runs")
        submission = post_requests[0]["body"]
        self.assertEqual(submission["workflow_id"], "apps-script-persistence-smoke")
        self.assertEqual([task["task_id"] for task in submission["tasks"]], ["stored-only-record"])
        self.assertEqual(submission["retry_policy"]["max_attempts"], 1)
        self.assert_no_page_errors()

        page.reload(wait_until="networkidle")
        page.get_by_text("Connected · synthetic demo data").wait_for()
        page.locator(".run-item").filter(has_text=workflow_id).wait_for()
        self.assertEqual(page.locator(".run-item").count(), 5)
        self.assertIn("stored-only-record", page.locator("#detailContent").inner_text())
        self.assertIn("task.pending · task_run", page.locator("#detailContent").inner_text())
        self.assertIn("workflow.submitted · workflow_run", page.locator("#detailContent").inner_text())
        self.assert_no_page_errors()

    def test_dashboard_mobile_layout_and_theme_persistence(self) -> None:
        page = self.page
        page.set_viewport_size({"width": 390, "height": 844})
        page.goto(self.base_url + "/dashboard/", wait_until="networkidle")
        self.mobile_width_is_safe()
        self.assertTrue(page.locator(".owner-credit").is_visible())
        self.assertGreater(page.locator(".owner-credit img").evaluate("img => img.naturalWidth"), 0)

        page.locator("#themeButton").click()
        self.assertEqual(page.locator("html").get_attribute("data-theme"), "light")
        page.reload(wait_until="networkidle")
        self.assertEqual(page.locator("html").get_attribute("data-theme"), "light")
        page.screenshot(path=str(ARTIFACTS / "dashboard-mobile.png"), full_page=True)
        self.assert_no_page_errors()


if __name__ == "__main__":
    unittest.main(verbosity=2)
