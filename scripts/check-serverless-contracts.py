#!/usr/bin/env python3
"""Static contract checks for the no-Origin Apps Script and Cloudflare runtimes."""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit("Serverless contract check failed: " + message)


shared = read("serverless-shared/workflow-core.js")
gas_core = read("apps-script/Core.gs")
gas = read("apps-script/Code.gs")
gas_html = read("apps-script/Index.html")
gas_manifest = json.loads(read("apps-script/appsscript.json"))
cloudflare = read("cloudflare-free/worker.mjs")
cloudflare_sql = read("cloudflare-free/migrations/0001_control_plane.sql")
cloudflare_config_text = read("cloudflare-free/wrangler.jsonc")
cloudflare_config = json.loads(cloudflare_config_text)
runtime_config = read("dashboard/runtime-config.js")
dashboard = read("dashboard/app.js")
dashboard_html = read("dashboard/index.html")
bridge = read("dashboard/apps-script-bridge.js")

require(shared == gas_core, "Apps Script Core.gs must match the shared workflow core byte-for-byte")
for symbol in (
    "function initializeAetherFlowDemo(",
    "function aetherflowBootstrap(",
    "function aetherflowApi(",
    "function routeApi_(",
):
    require(symbol in gas, "missing Apps Script entrypoint: " + symbol)
require(gas_manifest.get("runtimeVersion") == "V8", "Apps Script must use V8")
webapp = gas_manifest.get("webapp", {})
require(webapp.get("executeAs") == "USER_DEPLOYING", "Apps Script must run as its owner")
require(webapp.get("access") == "ANYONE_ANONYMOUS", "Apps Script manifest must declare public demo access")
require("google.script.run" in gas_html, "Apps Script wrapper must call google.script.run")
require("event.origin !== PAGES_ORIGIN" in gas_html, "Apps Script wrapper must validate the dashboard origin")
require("event.source !== frame.contentWindow" in gas_html, "Apps Script wrapper must validate iframe source")
require("ORBIT" not in gas and "AETHERFLOW_DEMO_SPREADSHEET_ID" in gas, "Apps Script runtime must use its own private datastore")
for forbidden in ("UrlFetchApp.fetch", "MailApp.sendEmail", "GmailApp.sendEmail", "ScriptApp.getOAuthToken"):
    require(forbidden not in gas, "Apps Script demo must not use " + forbidden)

require('import { WorkflowEntrypoint } from "cloudflare:workers";' in cloudflare, "Cloudflare WorkflowEntrypoint must be present")
require('import "../serverless-shared/workflow-core.js";' in cloudflare, "Cloudflare runtime must import the shared core")
require("export class AetherFlowExecution extends WorkflowEntrypoint" in cloudflare, "durable Workflow class is missing")
for route in (
    "/api/v1/demo/bootstrap",
    "/api/v1/workflow-runs",
    "/api/v1/workers",
    "/api/v1/metrics",
):
    require(route in cloudflare, "missing Cloudflare API route: " + route)
for operation in ("echo", "collect", "uppercase", "sha256", "add", "fail_once"):
    require('kind === "' + operation + '"' in cloudflare, "missing built-in task operation: " + operation)
require("AETHERFLOW_EXECUTIONS.create" in cloudflare, "workflow submissions must use the durable Workflow binding")
require("AETHERFLOW_EXECUTIONS.get" in cloudflare, "workflow cancellation must address the durable Workflow instance")
require("create TABLE" not in cloudflare_sql, "SQL migration casing should be canonical")
for table in ("demo_visitors", "demo_sessions", "demo_daily_usage", "demo_workflows", "demo_tasks", "demo_events"):
    require(re.search(r"CREATE TABLE IF NOT EXISTS " + table + r"\b", cloudflare_sql, re.IGNORECASE) is not None,
            "missing D1 table: " + table)
require("PRAGMA" not in cloudflare_sql, "D1 migration must not depend on restricted PRAGMA statements")

require(cloudflare_config.get("name") == "aetherflow-free-runtime", "Wrangler project name mismatch")
require(cloudflare_config.get("main") == "worker.mjs", "Wrangler entrypoint mismatch")
require(cloudflare_config.get("workers_dev") is True, "free workers.dev endpoint must remain enabled")
d1 = cloudflare_config.get("d1_databases", [])
require(len(d1) == 1 and d1[0].get("binding") == "DB", "Wrangler D1 binding mismatch")
workflows = cloudflare_config.get("workflows", [])
require(len(workflows) == 1 and workflows[0].get("binding") == "AETHERFLOW_EXECUTIONS",
        "Wrangler Workflow binding mismatch")
require("https://aetherflow-free-runtime.pooyagame141.workers.dev" in runtime_config,
        "dashboard must use the configured free workers.dev endpoint")

require('<script src="runtime-config.js" defer></script>' in dashboard_html, "runtime config must load in dashboard HTML")
require(dashboard_html.index('src="runtime-config.js"') < dashboard_html.index('src="app.js"'),
        "runtime config must load before dashboard app")
require(dashboard_html.index('src="apps-script-bridge.js"') < dashboard_html.index('src="app.js"'),
        "Apps Script bridge must load before dashboard app")
require("function connectCloudflareFree()" in dashboard, "dashboard automatic Cloudflare connection is missing")
require("function connectAppsScriptDemo()" in dashboard, "dashboard automatic Apps Script connection is missing")
require("AetherFlowAppsScriptBridge" in bridge, "Apps Script bridge client is missing")
require("connectDefaultRuntime();" in dashboard, "dashboard must initialize a default serverless runtime")

print("AetherFlow serverless runtime contracts: OK")
