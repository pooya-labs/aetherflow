# AetherFlow Cloudflare Gateway

This directory contains the edge ingress for the AetherFlow HTTP API. It is designed to sit in front of one HTTPS API origin and to enforce a shared, per-client rate limit before forwarding traffic.

**Deployment status: source and CI tests are present; no live Gateway route is enabled.** The Cloudflare account currently has no active Zone or Tunnel, and AetherFlow has no verified API origin configured. The Worker configuration deliberately sets `workersDev: false` and disables preview URLs so an incomplete proxy is not publicly reachable. No custom domain needs to be purchased for a free hobby/test route: after a real HTTPS API origin and matching ingress secret exist, the Worker can be exposed on the account's `workers.dev` subdomain. Cloudflare describes `workers.dev` as suitable for personal/hobby projects, not business-critical production. This route does not make the Rust API origin optional.

## Hosted CI evidence

The dedicated [Cloudflare Gateway CI run](https://github.com/pooya-labs/aetherflow/actions/runs/42504504348) passed on source commit [`afe9850b9b34d24555eda781ceec72774c810e8f`](https://github.com/pooya-labs/aetherflow/commit/afe9850b9b34d24555eda781ceec72774c810e8f), with **13/13 Node policy tests passing**. JavaScript syntax checks and the static guard that verifies `workersDev: false`, `previewUrls: false`, and the unconfigured-origin placeholder also passed. The suite has since been expanded: PR [#16](https://github.com/pooya-labs/aetherflow/pull/16) passed [Cloudflare Gateway CI run 42646781773](https://github.com/pooya-labs/aetherflow/actions/runs/42646781773) on exact test source [`ef23eb4`](https://github.com/pooya-labs/aetherflow/commit/ef23eb423e492779af250d79980328888f7727ce), with **18/18 Node tests passing**. The five additional regressions cover `/health` limiter bypass with ingress-token forwarding, early method rejection, fail-closed behavior on Durable Object errors, redirect/`Location` suppression, and generic no-store handling for origin transport failure. The 18 Node policy tests still use deterministic fixtures for the proxy request contract. A separate Cloudflare Workers Vitest suite now executes the exported Durable Object in the actual Workers runtime with SQLite storage. It covers persisted bucket state across eviction, independent client buckets, concurrent token consumption, alarm rescheduling, full idle cleanup, and schema recreation after cleanup. This is local runtime integration coverage, not a live production route or origin acceptance test. The exact-source run [43063897214](https://github.com/pooya-labs/aetherflow/actions/runs/43063897214) passed both suites and verified that the production route remains disabled.

## Request path

```text
Client
  |
  | HTTPS
  v
Cloudflare Worker: aetherflow-gateway
  |
  +--> HMAC-SHA-256(CF-Connecting-IP, AETHERFLOW_INGRESS_TOKEN)
  |       |
  |       v
  |    One SQLite-backed Durable Object per client IP
  |       |
  |       +--> allow
  |       +--> 429 + Retry-After
  |
  +--> HTTPS to fixed AETHERFLOW_ORIGIN
          |
          +--> strip caller-supplied forwarding / identity headers
          +--> preserve the operator or tenant Bearer credential
          +--> inject x-aetherflow-ingress-token from a Worker Secret
          v
       AetherFlow Rust API
          |
          +--> requires the matching AETHERFLOW_INGRESS_TOKEN
              for every route except public /health when bound
              to a non-loopback address
```

The Worker never selects an origin from request headers, query parameters, or caller input. It only forwards `/api/v1/*` and `/health`. Upstream redirects are rejected rather than exposing an origin address in a `Location` response.

## Security properties

- The client identity is taken from Cloudflare's `CF-Connecting-IP` edge header. `X-Forwarded-For`, `Forwarded`, `X-Real-IP`, `True-Client-IP`, and other client-supplied identity headers are removed before proxying.
- Raw client addresses are keyed with HMAC-SHA-256 before use as Durable Object names, so an observer of DO names cannot cheaply enumerate likely client IPs.
- Each address maps to its own SQLite-backed Durable Object. This avoids one global hot object while keeping a client's token bucket strongly consistent across Worker replicas and locations. A 24-hour idle alarm re-checks the last-seen timestamp and clears all SQL/alarm storage with `deleteAll()` after a client has been inactive; active buckets extend the deadline when the alarm fires. The alarm is installed on first use, not rewritten on every request.
- Token-bucket reads and writes are synchronous SQLite operations in one Durable Object turn. The alarm handler uses a short `blockConcurrencyWhile` section only while checking expiry and deleting idle state, preventing a new request from racing with cleanup. A rejected request does not accumulate negative token debt.
- If the origin or ingress secret is missing, if the Durable Object cannot be reached, or if the client address is unavailable, the Worker fails closed.
- The upstream ingress token is added by the Worker after the untrusted inbound header is removed. The Rust API stores only its SHA-256 digest and uses constant-time comparison.
- The existing operator and tenant Bearer credentials are preserved unchanged. The ingress token is an additional edge-to-origin control, not a replacement for API authentication.
- The public `/health` route bypasses the edge rate limiter but still uses the fixed HTTPS origin and the gateway ingress token.

## Configuration

`cloudflare.config.ts` declares the Worker, a SQLite-backed Durable Object class, observability, and the Worker Secret binding. Before deployment, replace the deliberately invalid placeholder for `AETHERFLOW_ORIGIN` with the verified HTTPS origin of the AetherFlow API.

Required values:

| Setting | Location | Notes |
| --- | --- | --- |
| `AETHERFLOW_ORIGIN` | Worker text binding | HTTPS origin only; no path, query, fragment, or URL credentials |
| `AETHERFLOW_INGRESS_TOKEN` | Worker Secret | Same 32–256 character URL-safe random token configured in the Rust API |
| `AETHERFLOW_INGRESS_TOKEN` | API secret manager / process environment | Required when `AETHERFLOW_BIND` is non-loopback |
| `AETHERFLOW_GATEWAY_BURST` | Worker text binding | Initial default 60 requests |
| `AETHERFLOW_GATEWAY_PER_SECOND` | Worker text binding | Initial refill 20 requests/second |

Generate a high-entropy token, for example with `openssl rand -hex 32`. Store it only in the API secret manager and as the Worker Secret `AETHERFLOW_INGRESS_TOKEN`. Do not place it in Git, `.env.example`, logs, or ordinary Worker variables. Rotate it by updating both sides in a coordinated deployment. The same secret keys the per-client HMAC names; rotating it intentionally starts fresh Durable Object buckets for all clients.

The 60-request burst / 20-per-second refill is only a starting policy, not a production SLO. Tune it against expected concurrency, five-repeat API load tests, and legitimate shared-NAT behavior.

## Local tests

Install the exact dependency graph from the committed lockfile, then run both the fast policy suite and the Cloudflare Workers runtime suite:

```sh
npm ci
npm run check
npm test
npm run test:workers-runtime
```

The 18-test Node suite validates the proxy and request-policy contract. The Workers Vitest suite runs the Durable Object inside the Cloudflare runtime with SQLite and exercises eviction recovery, concurrent requests, alarm refresh, cleanup, and post-cleanup schema recreation. Both suites run locally through Miniflare; neither deploys a public Worker route or requires a Cloudflare account.

The Rust API also has tests proving that a public bind without the ingress token is rejected, a direct API request without the edge token receives 403, a valid edge token reaches the normal API middleware, and `/health` remains public.

## Toolchain setup

Use Node.js **22.23 or later**. The committed `package-lock.json` pins the 184-package test/tooling dependency graph; CI uses npm **11.19.1** for consistent lockfile resolution. The project pins the Cloudflare CLI beta (`cf` 1.0.0-beta.23) as a development dependency so the `cf/config` import resolves from this project rather than relying on a globally installed, unpinned copy.

From this directory:

```sh
node --version
npm install
npm run check
npm test
npx cf --version
npx cf auth login
```

The `cf` CLI is currently in beta. Review its [current deployment documentation](https://developers.cloudflare.com/cf/projects/) before changing production resources.

## Build and deploy (only after prerequisites)

The project uses Cloudflare's `cf` CLI and `cloudflare.config.ts`, not a Wrangler configuration. Install Node.js 22.23 or later and run `npm ci` to install the pinned project-local CLI and exact dependency graph. Authenticate with `npx cf auth login`. The CLI is currently in beta; check its current [project commands](https://developers.cloudflare.com/cf/projects/) before an actual deployment.

From this directory, after completing Toolchain setup:

```sh
npm run check
npm test
npx cf deploy --dry-run --secrets-file .env.production
```

The ignored local file `.env.production` may contain only the runtime Worker secret, for example:

```dotenv
AETHERFLOW_INGRESS_TOKEN=<same high-entropy token configured on the API>
```

Do not commit this file. The repository's root `.gitignore` excludes `.env.*` files. Replace the placeholder HTTPS origin in `cloudflare.config.ts` before building, and do not perform a live `npx cf deploy --secrets-file .env.production` until the API origin, API-side secret, network restriction, and production route prerequisites below are satisfied. The current Cloudflare account has no active Zone or Tunnel and there is no verified API origin, so the live deployment command is intentionally not run.

## Deployment prerequisites

1. Deploy the AetherFlow API on an HTTPS origin and configure `AETHERFLOW_INGRESS_TOKEN` in its secret manager. A non-loopback Rust bind now refuses to start without the token.
2. Restrict origin network access to Cloudflare egress or connect the origin through a Cloudflare Tunnel. The API-side token is an additional bypass control, not a reason to leave an origin unnecessarily exposed.
3. Replace the placeholder `AETHERFLOW_ORIGIN` with the verified API origin and set the matching Worker Secret.
4. Install and authenticate the Cloudflare CLI (`cf`) and deploy this directory's `cloudflare.config.ts`. Keep `workersDev: false` until the verified API origin and matching secret are provisioned. For a free hobby/test exposure, a reviewed follow-up can enable the account's `workers.dev` subdomain without buying a custom domain; this is not a business-critical production hostname.
5. A custom hostname requires an active Cloudflare Zone; it is optional for the free `workers.dev` hobby/test route. Do not buy or onboard a domain solely to test this Gateway.
6. Verify 401/403/429 behavior, direct-origin rejection without the ingress token, cross-replica rate limits, TLS validation, request-size limits, and controlled workflow submission before opening traffic.

## Usage and free-tier note

A rate-limited API request makes one Worker request, one Durable Object request, one SQLite row read, and one SQLite row write. The first request for a client bucket also installs a 24-hour idle alarm; each alarm firing is billed as a Durable Object request, and rescheduling an active bucket writes alarm metadata. Cloudflare's current Free plan includes 100,000 Worker requests/day, 100,000 Durable Object requests/day, 100,000 Durable Object SQL row writes/day, and 5 million SQL row reads/day. Because each API request writes the bucket row, the practical free-tier ceiling is approximately 100,000 such requests per day, before other account usage and alarm activity. Free-tier operations fail once a daily limit is exhausted; this is an availability ceiling, not an automatic paid overage. Review [current Durable Objects pricing and limits](https://developers.cloudflare.com/durable-objects/platform/pricing/) and [Workers limits](https://developers.cloudflare.com/workers/platform/limits/) before production use. For higher usage, evaluate the plan and cost against measured traffic rather than assuming this edge layer is unlimited.

This Gateway is an ingress component only. It does not provision PostgreSQL, NATS server accounts, NATS ACLs, backups, or a production API host.
