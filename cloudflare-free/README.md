# AetherFlow Cloudflare Free Runtime

This is the no-Origin serverless runtime. Cloudflare Workers serves the API directly on the account's `workers.dev` subdomain, D1 stores bounded workflow/task/session/event metadata, and Cloudflare Workflows executes each accepted workflow as durable steps with retries. It does not call a separate VPS, PostgreSQL, or NATS endpoint.

The runtime accepts only a small set of built-in task operations (`echo`, `collect`, `uppercase`, `sha256`, `add`, and `fail_once`). It does not evaluate user code, load WASM plugins, or expose a general-purpose internet fetch task. The Rust/PostgreSQL/NATS runtime remains the advanced self-hosted profile; this Cloudflare profile is an operationally limited serverless execution target.

## Free-tier safety budget

| Guard | Project default |
| --- | ---: |
| Daily workflow launches | 100 |
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

## Provisioning

1. Create a dedicated D1 database named `aetherflow-free` and apply `migrations/0001_control_plane.sql`.
2. Deploy `worker.mjs` as an ES module Worker named `aetherflow-free-runtime`, with a D1 binding called `DB` and a Workflow binding called `AETHERFLOW_EXECUTIONS` pointing to Workflow `aetherflow-execution` and class `AetherFlowExecution`.
3. Create the Workflow resource `aetherflow-execution` against script `aetherflow-free-runtime` and class `AetherFlowExecution`.
4. Enable the `workers.dev` route and set `ALLOWED_ORIGIN` to `https://pooya-labs.github.io`.
5. Test `/health`, then the dashboard bootstrap flow from `https://pooya-labs.github.io/aetherflow/dashboard/`.

The Worker is configured for the user's existing `workers.dev` subdomain and does not require a paid Zone or custom domain.
