# AetherFlow 1.0.0 release readiness

**Release target:** v1.0.0  
**Current state:** Not released · M08 production engineering remains open  
**Last reviewed:** 2026-11-08

AetherFlow already has a versioned v1 control-plane API, but that does not make the complete platform a stable 1.0.0 release or a production-ready service. This checklist is the release gate. Keep workspace crate versions at 0.1.0 until the gates are closed; do not publish a 1.0.0 tag merely because the API path contains /v1.

## Verified engineering baseline

The following workflows passed on the same source commit, d208d568584a54c60e7917e809ebe4271c280cd1:

- [Rust CI](https://github.com/pooya-labs/aetherflow/actions/runs/42476492695) — formatting, Clippy, workspace tests, release build, scheduler benchmark smoke, WebAssembly guest lint/build, ABI v1 example execution, documentation build, and Docker Compose checks.
- [PostgreSQL schema validation](https://github.com/pooya-labs/aetherflow/actions/runs/42476492721) — storage and migration tests, RLS and runtime-role checks, TLS-backed API integration, JetStream transport, schema invariants, five-repeat PostgreSQL/JetStream benchmark, and isolated backup/restore-to-runtime rehearsal.
- [Supply-chain and secret checks](https://github.com/pooya-labs/aetherflow/actions/runs/42476492590).
- [GitHub Pages build and deployment](https://github.com/pooya-labs/aetherflow/actions/runs/42476492054).

The restore rehearsal proved that a restored PostgreSQL v10/RLS database and restored JetStream stream can be used by a restarted AetherFlow API and an independent worker/publisher/consumer process. The v10 PostgreSQL/JetStream workflow [42494177393](https://github.com/pooya-labs/aetherflow/actions/runs/42494177393) passed all 28 steps, including the v10 migration ledger, tenant-isolated HTTP/TLS operational metrics, JetStream validation, benchmark repetitions, schema invariants, and restore-to-runtime verification. The accepted workflow completed, its task output persisted, and five target outbox events were committed to the PostgreSQL inbox with zero duplicates. This is an isolated CI rehearsal, not a production point-in-time recovery service or an owner-approved RPO/RTO guarantee.

The exact-source PostgreSQL/NATS run archived five repeats for each shape: fan-out median throughput 7.314 workflows/s and p95 7,863.339 ms; chain median throughput 7.244 workflows/s and p95 7,999.233 ms. Each repeat reconciled 896 outbox/inbox events with zero duplicate events or errors. The candidate runner fingerprint did not match the stored AMD EPYC 9V45 reference, so the comparator correctly reported SKIP and applied no numeric threshold. The repository does contain two independent five-repeat EPYC 9V45 reference sessions (ten samples per shape), but the current candidate has not yet exercised the comparator's numeric branch on that matching fingerprint. These numbers are not production SLOs.

## Additional verified engineering slices

- The schema v10 operational-metrics extension passed PostgreSQL/JetStream CI [42494177393](https://github.com/pooya-labs/aetherflow/actions/runs/42494177393) with 28/28 steps. It validates tenant-isolated HTTP/TLS metrics, migration/index assertions, JetStream, benchmark profiles and restore-to-runtime.
- Tenant dashboard browser acceptance passed 9/9 desktop/mobile tests in [42495589050](https://github.com/pooya-labs/aetherflow/actions/runs/42495589050); the Pages deployment [42495588609](https://github.com/pooya-labs/aetherflow/actions/runs/42495588609) and security checks [42495589031](https://github.com/pooya-labs/aetherflow/actions/runs/42495589031) also passed.
- These runs cover the v10 operational-metrics and dashboard slices, but they do not constitute a green exact-SHA release matrix for the current main head. The core lifecycle-sequence and retry-boundary tests added after the recorded baseline still require hosted CI confirmation.
- The NATS transport now provides a shared client connection configuration for `.creds` authentication, TLS enforcement, custom CA bundles and optional client mTLS. The integration-test helper, end-to-end load profile and restore verifier use it. A new hosted integration fixture starts a TLS-enabled NATS JetStream server, verifies a trusted-CA publish acknowledgement, and checks rejection of a hostname mismatch. This test has been added to PostgreSQL/NATS CI but still requires a completed run on the resulting exact source SHA. This client-side change does not complete server-side account/subject ACLs or production cluster acceptance.
- The Cloudflare edge gateway implementation is now in `cloudflare-gateway/`: a Worker proxy, per-client HMAC-SHA-256 routing to SQLite-backed Durable Objects, token-bucket decisions, forwarding-header stripping, a 2 MiB declared-body guard, fail-closed behavior, and a private `cloudflare.config.ts` with workers.dev and preview URLs disabled. Node built-in tests cover token-bucket transitions (including clock rollback), HMAC bucket naming/rotation, origin validation, token injection, header spoofing, no-store responses, 429 behavior, and the Durable Object SQL interface through a deterministic storage mock. Per-client storage is cleaned after 24 hours of inactivity using a Durable Object alarm; active buckets reschedule the alarm and the cleanup path serializes against requests. The dedicated [Cloudflare Gateway CI run 42504504348](https://github.com/pooya-labs/aetherflow/actions/runs/42504504348) passed **13/13 tests** on exact source commit [afe9850](https://github.com/pooya-labs/aetherflow/commit/afe9850b9b34d24555eda781ceec72774c810e8f), including syntax checks and the static closed-route guard. The DO SQL contract test uses a deterministic mock, not the real Cloudflare Workers runtime/SQLite; a deployed runtime integration test remains an acceptance item. The Rust API requires a shared `AETHERFLOW_INGRESS_TOKEN` on all non-health routes when bound to a non-loopback address and refuses a public bind without it.
- This does not satisfy production ingress acceptance: the Worker is not deployed, no route is attached, the Cloudflare account currently has no active Zone or Tunnel, no AetherFlow API origin is configured, and no origin firewall policy has been tested. The gateway gate remains open until a real HTTPS API origin and matching Worker Secret exist and direct-origin/cross-replica tests pass.

## Required before publishing 1.0.0

- [x] Pre-release engineering baseline verified on one exact source commit: Rust, PostgreSQL/JetStream, security and Pages workflows all passed on d208d568584a54c60e7917e809ebe4271c280cd1. Re-run the full matrix after any subsequent release-candidate change.
- [ ] Run the comparator-enabled workflow on the matching normalized EPYC 9V45 fingerprint so the numeric 10% throughput / 15% p95 branch is exercised. The reference sessions exist; the latest candidate run only validated the mismatch/skip path. Archive raw samples and the environment fingerprint.
- [ ] Deploy and route the Cloudflare Worker gateway from `cloudflare-gateway/` only after a verified HTTPS API origin and matching `AETHERFLOW_INGRESS_TOKEN` are provisioned. Attach a production hostname through an active Zone (or an explicitly tested Tunnel route), verify shared Durable Object throttling across replicas/locations, request-size and concurrency controls, and prove direct-origin bypass is blocked. The code is present; the Cloudflare account currently has no active Zone/Tunnel and no AetherFlow API origin, so this gate is still open.
- [ ] Define and test the deployment topology for API processes, workers, PostgreSQL, NATS, secrets, certificates, networking, and alert ownership. Prove aggregate rate limiting and direct-origin bypass protection across replicas.
- [ ] Obtain owner-approved availability objectives, RPO/RTO, and a production backup policy. Configure and rehearse managed database backups or WAL archiving/PITR; the repository's logical restore drill is not a substitute.
- [ ] Configure and independently verify production NATS server authentication, TLS listener/certificate policy, account and subject ACLs, storage/retention, monitoring, and a documented quorum/restore policy. Client-side `.creds`/TLS/mTLS support is implemented, but the local Compose cluster remains unauthenticated development infrastructure and does not satisfy this server-side deployment gate.
- [ ] Approve the SLO and alert design for workflow completion latency, oldest pending outbox age, oldest unprocessed inbox age, worker lease health, and failure attribution. The SLO proposal now includes read-only SQL snapshots and PromQL examples, but no thresholds or owner approvals are active.
- [ ] Complete the independent security/failure-injection review and record an explicit maintainer decision on whether randomized property-based transition generation is required. Dependency-free bounded exhaustive sequence tests now explore workflow and task lifecycle paths to depth five, but they do not replace randomized generation or an independent review.
- [ ] Review public API and SDK compatibility, migration upgrade/downgrade policy, CLI behavior, documentation, and examples as one release candidate. Preserve the explicit at-least-once delivery contract; external side effects still require application-level idempotency or a transaction shared with inbox completion.
- [ ] Prepare the release notes and changelog, verify all package metadata, bump publishable workspace crates from 0.1.0 to 1.0.0, build release artifacts, and produce checksums and provenance/SBOM evidence.
- [ ] Create the v1.0.0 tag and GitHub Release only after all previous gates are checked and the exact tagged commit has a green CI matrix.

## Deployment acceptance

The operator-owned deployment checklist is in [production-acceptance.md](../operations/production-acceptance.md). It covers gateway source throttling and origin bypass, TLS/secrets, restricted PostgreSQL roles, production NATS controls, approved recovery objectives, SLO evidence and sign-off. It is a template; no production owner has approved it yet.

## Licensing

The repository code is licensed under Apache License 2.0. The root LICENSE file is authoritative, and each Cargo package inherits the Apache-2.0 SPDX identifier through workspace metadata. The license does not grant rights to the AetherFlow or Pooya Labs names, logos, or trademarks. Verify third-party dependency and separately supplied asset notices as part of release packaging.

## Explicit non-goals and claims to avoid

- Do not claim universal exactly-once execution. Delivery is at-least-once; application-level idempotency or a transactional integration is required for external side effects.
- Do not call a passing CI restore drill production PITR.
- Do not describe process-local rate limiters as cluster-wide quotas.
- Do not label the control plane production-ready until the deployment, operations, recovery, and ingress gates above are satisfied.
