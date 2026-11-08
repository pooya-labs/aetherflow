# AetherFlow Cloudflare Free Runtime

This is the no-Origin serverless runtime. Cloudflare Workers serves the API directly on the account's `workers.dev` subdomain, D1 stores bounded workflow/task/session/event metadata, and Cloudflare Workflows executes each accepted workflow as durable steps with retries. It does not call a separate VPS, PostgreSQL, or NATS endpoint.

The runtime accepts only a small set of built-in task operations (`echo`, `collect`, `uppercase`, `sha256`, `add`, and `fail_once`). It does not evaluate user code, load WASM plugins, or expose a general-purpose internet fetch task. The Rust/PostgreSQL/NATS runtime remains the advanced self-hosted profile; this Cloudflare profile is an operationally limited serverless execution target.

## Free-tier safety budget

| Guard | Project default |
| --- | ---: |
| Daily workflow launches | 100 |
| Retained workflows per workspace | 50 (oldest terminal run and related records are pruned before accepting another) |
| Tasks per workflow | 20 |
| Tasks per session | governed by the 60 mutation / 250 read cap |
| Session lifetime | 6 hours |
| Request body | 16 KiB |
| Task input | 8 KiB |
| Daily D1 reads (internal) | 12,000 |
| Daily D1 mutations (internal) | 2,500 |
| Browser workspaces | 500 |
| New browser sessions/day | 60 |

Cloudflare documents Workers Free at 100,000 requests/day and D1 Free at 5 million rows read/day, 100,000 rows written/day and 5 GB total storage. Cloudflare Workflows Free includes 3,000 steps/day and 1 GB-month storage. AetherFlow sets lower internal budgets; monitor account usage before increasing them. See the official [Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/), [Workers limits](https://developers.cloudflare.com/workers/platform/limits/), [D1 pricing](https://developers.cloudflare.com/d1/platform/pricing/), and [Workflows pricing](https://developers.cloudflare.com/workflows/reference/pricing/).

## Task input contract

`input_base64` must contain standard base64-encoded UTF-8 JSON. Supported task payloads include:

- `{\"type\":\"echo\",\"value\":\"hello\"}`
- `{\"type\":\"uppercase\",\"input\":\"hello\"}`
- `{\"type\":\"sha256\",\"input\":\"hello\"}`
- `{\"type\":\"add\",\"values\":[2,3,5]}`
- `{\"type\":\"collect\"}` to collect results from dependency tasks
- `{\"type\":\"fail_once\"}` to verify the configured Workflow retry policy

Task payloads and outputs are not returned by the dashboard API; only size metadata and redacted event history are exposed.

## Current Cloudflare deployment

The account-owned runtime is configured at:

- API: https://aetherflow-free-runtime.pooyagame141.workers.dev
- Health endpoint: https://aetherflow-free-runtime.pooyagame141.workers.dev/health
- D1 database: `aetherflow-free` (primary region WEUR)
- Workflow: `aetherflow-execution`
- Worker: `aetherflow-free-runtime`

This deployment uses the account's existing `workers.dev` subdomain. It has no custom domain, public Origin server, VPS, PostgreSQL connection, or NATS endpoint.

## Rebuilding or deploying from this folder

The Wrangler configuration is in `wrangler.jsonc` and already includes the account's D1 binding and Cloudflare Workflow binding. From this directory:

1. Authenticate Wrangler with the Cloudflare account that owns the D1 database.
2. Review `wrangler.jsonc`; retain the existing `database_id` for the current environment or replace it with a separately created D1 database.
3. For a newly created D1 database, run the schema once with `npx wrangler d1 execute aetherflow-free --remote --file=./migrations/0001_control_plane.sql`. Do not run this SQL again against the already initialized database.
4. Deploy with `npx wrangler deploy`. The worker is configured to use the `workers.dev` subdomain and keeps preview URLs disabled.
5. Check `/health`, then open the GitHub Pages dashboard. On the direct Pages URL, the dashboard bootstraps a short-lived session from the Cloudflare runtime automatically. The Apps Script iframe path takes precedence when opened through the Google Web App.

Cloudflare's account settings were provisioned directly for the current deployment. If you create another account or database, update `wrangler.jsonc` and the dashboard runtime config together.

## Task input contract

`input_base64` must contain standard base64-encoded UTF-8 JSON. Supported task payloads include:

- `{"type":"echo","value":"hello"}`
- `{"type":"uppercase","input":"hello"}`
- `{"type":"sha256","input":"hello"}`
- `{"type":"add","values":[2,3,5]}`
- `{"type":"collect"}` to collect results from dependency tasks
- `{"type":"fail_once"}` to verify the configured Workflow retry policy

Task payloads and outputs are not returned by the dashboard API; only size metadata and redacted event history are exposed.

## Testing

From the repository root:

```sh
node --test serverless-shared/workflow-core.test.mjs
python scripts/check-serverless-contracts.py
node --check dashboard/runtime-config.js
node --check dashboard/apps-script-bridge.js
node --check dashboard/app.js
node --check cloudflare-free/worker.mjs
```

