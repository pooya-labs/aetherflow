# AetherFlow 1.0.0 release readiness

**Release target:** v1.0.0  
**Current state:** Not released · M08 production engineering remains open  
**Last reviewed:** 2026-11-08

AetherFlow already has a versioned v1 control-plane API, but that does not make the complete platform a stable 1.0.0 release or a production-ready service. This checklist is the release gate. Keep workspace crate versions at 0.1.0 until the gates are closed; do not publish a 1.0.0 tag merely because the API path contains /v1.

## Verified engineering baseline

The following workflows passed on the same source commit, 780e20d80b8a62c5e5c7a927a205d07c536ed713:

- [Rust CI](https://github.com/pooya-labs/aetherflow/actions/runs/42474546453) — formatting, Clippy, workspace tests, release build, scheduler benchmark smoke, WebAssembly guest lint/build, ABI v1 example execution, documentation build, and Docker Compose checks.
- [PostgreSQL schema validation](https://github.com/pooya-labs/aetherflow/actions/runs/42474546425) — storage and migration tests, RLS and runtime-role checks, TLS-backed API integration, JetStream transport, schema invariants, five-repeat PostgreSQL/JetStream benchmark, and isolated backup/restore-to-runtime rehearsal.
- [Supply-chain and secret checks](https://github.com/pooya-labs/aetherflow/actions/runs/42474546496).
- [GitHub Pages build and deployment](https://github.com/pooya-labs/aetherflow/actions/runs/42474545667).

The restore rehearsal proved that a restored PostgreSQL v8/RLS database and restored JetStream stream can be used by a restarted AetherFlow API and an independent worker/publisher/consumer process. The accepted workflow completed, its task output persisted, and five target outbox events were committed to the PostgreSQL inbox with zero duplicates. This is an isolated CI rehearsal, not a production point-in-time recovery service or an owner-approved RPO/RTO guarantee.

The latest CI benchmark archived five repeats for fan-out and chain workloads. Median throughput was 7.155 workflows/s (fan-out) and 6.978 workflows/s (chain). The GitHub-hosted runner fingerprint did not match the stored AMD EPYC 9V45 reference; the comparator correctly skipped numeric regression thresholds. A matched-fingerprint run is still needed to exercise those thresholds. These numbers are not production SLOs.

## Required before publishing 1.0.0

- [ ] Run the complete Rust, PostgreSQL/JetStream, security, and documentation workflows on one final candidate commit; all must pass on that exact commit.
- [ ] Complete a matched-fingerprint EPYC 9V45 benchmark run so the stored throughput and p95 regression limits are actually enforced. Archive raw samples and the environment fingerprint.
- [ ] Close the production ingress boundary: use a shared gateway that throttles actual client sources before forwarding, strips caller-supplied forwarding/identity headers, applies request/concurrency limits, and blocks direct access to every API origin and replica.
- [ ] Define and test the deployment topology for API processes, workers, PostgreSQL, NATS, secrets, certificates, networking, and alert ownership. Prove aggregate rate limiting and direct-origin bypass protection across replicas.
- [ ] Obtain owner-approved availability objectives, RPO/RTO, and a production backup policy. Configure and rehearse managed database backups or WAL archiving/PITR; the repository's logical restore drill is not a substitute.
- [ ] Configure production NATS authentication, TLS, account/subject ACLs, storage/retention, monitoring, and a documented quorum/restore policy. The local Compose cluster is a development fixture, not a production security profile.
- [ ] Complete the SLO and alert design for workflow completion latency, oldest pending outbox age, oldest unprocessed inbox age, worker lease health, and failure attribution. Assign operational owners and escalation paths.
- [ ] Finish the remaining security/failure-injection review and decide whether the deferred property-based transition tests are required before the stable release.
- [ ] Review public API and SDK compatibility, migration upgrade/downgrade policy, CLI behavior, documentation, and examples as one release candidate. Preserve the explicit at-least-once delivery contract; external side effects still require application-level idempotency or a transaction shared with inbox completion.
- [ ] Prepare the release notes and changelog, verify all package metadata, bump publishable workspace crates from 0.1.0 to 1.0.0, build release artifacts, and produce checksums and provenance/SBOM evidence.
- [ ] Create the v1.0.0 tag and GitHub Release only after all previous gates are checked and the exact tagged commit has a green CI matrix.

## Licensing

The repository code is licensed under Apache License 2.0. The root LICENSE file is authoritative, and each Cargo package inherits the Apache-2.0 SPDX identifier through workspace metadata. The license does not grant rights to the AetherFlow or Pooya Labs names, logos, or trademarks. Verify third-party dependency and separately supplied asset notices as part of release packaging.

## Explicit non-goals and claims to avoid

- Do not claim universal exactly-once execution. Delivery is at-least-once; application-level idempotency or a transactional integration is required for external side effects.
- Do not call a passing CI restore drill production PITR.
- Do not describe process-local rate limiters as cluster-wide quotas.
- Do not label the control plane production-ready until the deployment, operations, recovery, and ingress gates above are satisfied.
