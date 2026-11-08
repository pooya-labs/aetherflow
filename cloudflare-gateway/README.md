# AetherFlow Cloudflare Gateway

This directory contains the edge ingress for the AetherFlow HTTP API. It is designed to sit in front of one HTTPS API origin and to enforce a shared, per-client rate limit before forwarding traffic.

**Deployment status: source and CI tests are present; no live route is enabled.** The Cloudflare account currently has no active Zone or Tunnel, and AetherFlow has no verified API origin configured. The Worker configuration deliberately sets `workersDev: false` and disables preview URLs so an incomplete proxy is not publicly reachable.

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
- Each address maps to its own SQLite-backed Durable Object. This avoids one global hot object while keeping a client's token bucket strongly consistent across Worker replicas and locations.
- Token-bucket reads and writes are synchronous SQLite operations in one Durable Object turn. A rejected request does not accumulate negative token debt.
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

The policy and Worker request flow use Node's built-in test runner; they require no third-party test dependencies. From this directory:

```sh
npm run check
npm test
```

The equivalent direct commands are `node --check policy.mjs`, `node --check worker.mjs`, `node --check index.mjs`, and `node --test policy.test.mjs`. The Durable Object SQL contract test uses a deterministic mock for the storage interface; it is not a deployed Workers-runtime/real SQLite integration test.

The Rust API also has tests proving that a public bind without the ingress token is rejected, a direct API request without the edge token receives 403, a valid edge token reaches the normal API middleware, and `/health` remains public.

## Build and deploy (only after prerequisites)

The project uses Cloudflare's `cf` CLI and `cloudflare.config.ts`, not a Wrangler configuration. Install Node.js 22.18 or later, install the Cloudflare CLI (`npm install --global cf`), and authenticate with `cf auth login`. The CLI is currently in beta; check its current [project commands](https://developers.cloudflare.com/cf/projects/) before an actual deployment.

From this directory:

```sh
npm run check
npm test
cf deploy --dry-run --secrets-file .env.production
```

The ignored local file `.env.production` may contain only the runtime Worker secret, for example:

```dotenv
AETHERFLOW_INGRESS_TOKEN=<same high-entropy token configured on the API>
```

Do not commit this file. The repository's root `.gitignore` excludes `.env.*` files. Replace the placeholder HTTPS origin in `cloudflare.config.ts` before building, and do not perform a live `cf deploy --secrets-file .env.production` until the API origin, API-side secret, network restriction, and production route prerequisites below are satisfied. The current Cloudflare account has no active Zone or Tunnel and there is no verified API origin, so the live deployment command is intentionally not run.

## Deployment prerequisites

1. Deploy the AetherFlow API on an HTTPS origin and configure `AETHERFLOW_INGRESS_TOKEN` in its secret manager. A non-loopback Rust bind now refuses to start without the token.
2. Restrict origin network access to Cloudflare egress or connect the origin through a Cloudflare Tunnel. The API-side token is an additional bypass control, not a reason to leave an origin unnecessarily exposed.
3. Replace the placeholder `AETHERFLOW_ORIGIN` with the verified API origin and set the matching Worker Secret.
4. Install and authenticate the Cloudflare CLI (`cf`) and deploy this directory's `cloudflare.config.ts`. The config keeps workers.dev and preview routes disabled.
5. After validation, attach a production hostname through an active Cloudflare Zone. The account currently has no active Zone, so no production route can be attached yet.
6. Verify 401/403/429 behavior, direct-origin rejection without the ingress token, cross-replica rate limits, TLS validation, request-size limits, and controlled workflow submission before opening traffic.

## Usage and free-tier note

A rate-limited API request makes one Worker request, one Durable Object request, one SQLite row read, and one SQLite row write. Cloudflare's current Free plan includes 100,000 Worker requests/day, 100,000 Durable Object requests/day, 100,000 Durable Object SQL row writes/day, and 5 million SQL row reads/day. Because every rate-limited request writes the bucket row, the practical free-tier ceiling is approximately 100,000 such requests per day, before other account usage. Free-tier operations fail once a daily limit is exhausted; this is an availability ceiling, not an automatic paid overage. Review [current Durable Objects pricing and limits](https://developers.cloudflare.com/durable-objects/platform/pricing/) and [Workers limits](https://developers.cloudflare.com/workers/platform/limits/) before production use. For higher usage, evaluate the plan and cost against measured traffic rather than assuming this edge layer is unlimited.

This Gateway is an ingress component only. It does not provision PostgreSQL, NATS server accounts, NATS ACLs, backups, or a production API host.
