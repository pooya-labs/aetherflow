# AetherFlow Apps Script runtime

This is the zero-cost, low-volume serverless demo profile for AetherFlow. It follows the Apps Script pattern used by ORBIT: GitHub Pages dashboard, an owner-deployed Apps Script Web App wrapper, a google.script.run message bridge, and one private Google Spreadsheet owned by the deployer.

This profile is not the Rust/PostgreSQL/NATS engine. It persists the bounded workflow control-plane demo contract only. It does not run arbitrary task code, WASM plugins, worker leases, NATS JetStream, PostgreSQL outbox/inbox delivery, production tenant identity, or real customer workflows. The displayed sample workflow records are synthetic.

## Included

- Apps Script V8 Web App wrapper embedding the GitHub Pages dashboard.
- Exact-origin and iframe-source validation for the bridge.
- One private owner-owned Google Spreadsheet with JSON records.
- Pseudonymous browser workspaces and random six-hour in-memory session capabilities.
- Workflow submission with bounded DAG validation, unique task IDs, valid dependencies, cycle rejection, and retry-policy validation.
- Workflow/task snapshots, redacted event history, task cancellation with optimistic version checks, and a bounded read-only worker list.
- Per-session and per-day request/mutation budgets, bounded payloads, record count ceiling, and a single ScriptLock around state changes.
- No arbitrary network fetches, email delivery, webhooks, credentials, telemetry ingestion, or code execution.

## First-time setup

1. Sign in at https://script.google.com/ using the Google account that will own the demo.
2. Create a standalone Apps Script project named AetherFlow Demo Runtime.
3. In Project Settings, enable Show appsscript.json manifest file in editor.
4. Create a script file named Code and paste apps-script/Code.gs.
5. Create a second script file named Core and paste apps-script/Core.gs. Keep it byte-for-byte aligned with serverless-shared/workflow-core.js.
6. Create an HTML file named Index and paste apps-script/Index.html.
7. Replace appsscript.json with the manifest in this directory and save.
8. Select initializeAetherFlowDemo in the function selector, click Run, review the Google Sheets authorization, and approve it. This creates the private AetherFlow Demo Database spreadsheet.
9. Choose Deploy → New deployment → Web app. Set Execute as to Me and Who has access to Anyone (anonymous access).
10. Deploy. The iframe URL is embedded in the wrapper source and will connect automatically.

Do not share the spreadsheet. Public visitors receive only a workspace-scoped session capability; they never receive the spreadsheet ID or URL.

Google may restrict anonymous Web App deployments for some account types or regions. Do not enable billing or choose a paid Google Cloud plan to work around a restriction. If the deployment selector does not offer anonymous access, stop and report the exact message.

## Update deployment

After modifying the files, paste the updated source into the corresponding Apps Script editor files, save, then Deploy → Manage deployments → Edit and create a new version. The /exec URL remains the same.

## Limits and safety

| Guard | Default |
| --- | ---: |
| Session lifetime | 6 hours |
| API requests per session | 250 |
| Mutations per session | 60 |
| Request body | 16 KiB |
| Task input | 8 KiB |
| New browser workspaces per UTC day | 25 |
| API requests per UTC day | 800 |
| Mutations per UTC day | 200 |
| Browser workspaces | 200 |
| Stored records | 10,000 |

The shared workflow core is tested with Node's built-in test runner. Apps Script itself is not executed by GitHub CI; repository checks validate the GAS entrypoints and bridge contract.

Apps Script quotas are account-based and may change. Google's quota guide currently lists a six-minute maximum per execution and 30 simultaneous executions per user. This profile is deliberately low-volume and must not be used for sustained queue processing or telemetry: https://developers.google.com/apps-script/guides/services/quotas

Do not submit production/customer data, secrets, or private task payloads. This public profile is a bounded demo adapter, not a production multi-tenant workflow service.
