# AetherFlow — scoped maintainer static review

**Review date:** 2026-11-11  
**Reviewed source:** `main` at `612593e9efb2354541d04754750b2277c39cb2fc`  
**Review type:** AI-assisted, source-level maintainer pass  
**Status:** Limited review recorded; **not** an independent security audit, penetration test, or production approval

## Scope

The review was limited to the following security-sensitive paths:

- `cloudflare-gateway/worker.mjs` and `cloudflare-gateway/policy.mjs`
- `cloudflare-gateway/policy.test.mjs` and `cloudflare-gateway/cloudflare.config.ts`
- `apps/aetherflow-server/src/main.rs` and the relevant ingress/authentication middleware in `apps/aetherflow-server/src/api.rs`
- `crates/aetherflow-storage-postgres/src/tenant_auth.rs`
- `crates/aetherflow-runtime/migrations/postgres/0006_force_tenant_row_level_security.sql`
- The worker's optional metrics listener in `crates/aetherflow-transport-nats/src/bin/aetherflow-worker.rs`
- `docs/security/threat-model.md` and `docs/operations/production-acceptance.md`

This was a source inspection against the stated design and regression tests. It was not a line-by-line audit of the complete codebase.

## Result

No confirmed critical- or high-severity source-level defect was identified in the bounded scope above. This statement is deliberately narrow: it is not a claim that the platform is secure, complete, or production-ready.

### Controls observed in source

1. **Fixed origin and ingress boundary.** The gateway accepts a configured HTTPS origin only, rejects credentials/path/query/fragment in that origin, constructs upstream URLs from the fixed origin rather than the caller's host, removes the configured client-controlled forwarding and ingress headers, and inserts its server-side ingress token. Gateway requests fail closed when origin/secret configuration or the shared limiter is unavailable.
2. **Shared per-client throttling design.** The gateway derives a keyed HMAC name from Cloudflare's `CF-Connecting-IP`, uses a SQLite-backed Durable Object token bucket, and returns bounded `Retry-After` responses on denial. The API separately rejects non-loopback startup without both TLS and an ingress token; when configured, the token middleware protects every route except the intentionally public `/health` liveness route.
3. **Tenant credential handling.** Tenant tokens are shape-bounded, only SHA-256 digests are stored, status and expiry are checked within a transaction while credential/principal/tenant rows are locked, and the presented digest is compared using constant-time equality after lookup. The tenant-owned tables covered by migration 0006 have RLS enabled and forced.
4. **Worker scrape boundary.** The optional worker `/metrics` listener is disabled unless explicitly configured, accepts only a numeric loopback bind, requires a bounded Bearer token, stores only its digest, and emits fixed-name metrics without caller-controlled labels.

## Open risks and required evidence

- The Cloudflare API gateway is not deployed or routed to a verified AetherFlow HTTPS origin. Direct-origin, alternate-replica, IPv4, IPv6, NAT, source-header spoofing, and cross-edge quota behavior have not been accepted over a real network.
- The gateway's Node tests use a deterministic mock for the Durable Object SQL storage interface. They are not a live Cloudflare Workers runtime test of SQLite behavior, alarms, or deployment bindings.
- The edge's declared `Content-Length` guard and the API's request-body limit need an end-to-end deployment test for streamed/chunked bodies as well as ordinary JSON requests.
- `/health` deliberately bypasses both the gateway per-client limiter and the API ingress-token check. It must remain a minimal liveness response; do not expose database state, version details, diagnostics, or sensitive deployment metadata there.
- The ingress token is a shared edge-to-origin secret, not a substitute for private origin networking/security groups. Rotate the edge and origin values together; the gateway also uses the value as its HMAC key for Durable Object names, so rotation changes client bucket identities.
- Production PostgreSQL RPO/RTO, managed backup plus WAL/PITR, production NATS server-side authentication/ACL/TLS/retention, cross-process metrics aggregation, protected remote scraping, and approved SLO/alert ownership are still open.
- No independent human security reviewer or penetration tester participated. Maintainer self-review and green CI must not be described as independent approval.

## Verification evidence

The exact merge SHA used for this review passed:

- [Rust CI](https://github.com/pooya-labs/aetherflow/actions/runs/43053833698) — 20/20 steps, including formatting, JavaScript/runbook checks, Clippy, workspace tests/build, benchmark smoke, WASM lint and ABI example, documentation build, Compose validation, and the separate no-Origin demo smoke.
- [Supply-chain and secret checks](https://github.com/pooya-labs/aetherflow/actions/runs/43053833861) — full-history Gitleaks scan and cargo-deny audit passed.
- [GitHub Pages build/deployment](https://github.com/pooya-labs/aetherflow/actions/runs/43053832910) — passed.

CI checks validate the repository state and tested disposable fixtures. They do not test the missing production origin or gateway deployment and do not close the independent-review gate.

## Recommended next sequence

1. Add a real Cloudflare Workers-runtime test for the Durable Object SQLite and alarm lifecycle before deployment.
2. Provision the HTTPS API origin and private ingress network; deploy the gateway only after the matching token exists at both ends.
3. Run the real-network spoofed-header, IPv4/IPv6/NAT, cross-replica quota, request-size, and direct-origin bypass acceptance cases.
4. Obtain independent security review and record the reviewer, scope, findings, remediation, and sign-off separately from this maintainer note.
