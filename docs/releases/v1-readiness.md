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

The restore rehearsal proved that a restored PostgreSQL v8/RLS database and restored JetStream stream can be used by a restarted AetherFlow API and an independent worker/publisher/consumer process. The accepted workflow completed, its task output persisted, and five target outbox events were committed to the PostgreSQL inbox with zero duplicates. This is an isolated CI rehearsal, not a production point-in-time recovery service or an owner-approved RPO/RTO guarantee.

The exact-source PostgreSQL/NATS run archived five repeats for each shape: fan-out median throughput 7.314 workflows/s and p95 7,863.339 ms; chain median throughput 7.244 workflows/s and p95 7,999.233 ms. Each repeat reconciled 896 outbox/inbox events with zero duplicate events or errors. The candidate runner fingerprint did not match the stored AMD EPYC 9V45 reference, so the comparator correctly reported SKIP and applied no numeric threshold. The repository does contain two independent five-repeat EPYC 9V45 reference sessions (ten samples per shape), but the current candidate has not yet exercised the comparator's numeric branch on that matching fingerprint. These numbers are not production SLOs.

## Required before publishing 1.0.0

- [x] Pre-release engineering baseline verified on one exact source commit: Rust, PostgreSQL/JetStream, security and Pages workflows all passed on d208d568584a54c60e7917e809ebe4271c280cd1. Re-run the full matrix after any subsequent release-candidate change.
- [ ] Run the comparator-enabled workflow on the matching normalized EPYC 9V45 fingerprint so the numeric 10% throughput / 15% p95 branch is exercised. The reference sessions exist; the latest candidate run only validated the mismatch/skip path. Archive raw samples and the environment fingerprint.
- [ ] Close the production ingress boundary: use a shared gateway that throttles actual client sources before forwarding, strips caller-supplied forwarding/identity headers, applies request/concurrency limits, and blocks direct access to every API origin and replica.
- [ ] Define and test the deployment topology for API processes, workers, PostgreSQL, NATS, secrets, certificates, networking, and alert ownership. Prove aggregate rate limiting and direct-origin bypass protection across replicas.
- [ ] Obtain owner-approved availability objectives, RPO/RTO, and a production backup policy. Configure and rehearse managed database backups or WAL archiving/PITR; the repository's logical restore drill is not a substitute.
- [ ] Configure production NATS authentication, TLS, account/subject ACLs, storage/retention, monitoring, and a documented quorum/restore policy. The local Compose cluster is a development fixture, not a production security profile.
- [ ] Approve the SLO and alert design for workflow completion latency, oldest pending outbox age, oldest unprocessed inbox age, worker lease health, and failure attribution. The SLO proposal now includes read-only SQL snapshots and PromQL examples, but no thresholds or owner approvals are active.
- [ ] Complete the independent security/failure-injection review and record an explicit owner decision on deferred property-based transition tests.
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
