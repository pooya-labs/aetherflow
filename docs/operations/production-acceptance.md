# Production deployment acceptance

**Status:** Operator-owned acceptance template · not an approval to expose the current experimental service  
**Applies to:** AetherFlow API replicas, durable workers, PostgreSQL, NATS JetStream, gateway, telemetry and recovery  
**Last reviewed:** 2026-11-08

This checklist turns the production requirements in the [operational guide](production-guide.md) into verifiable deployment evidence. The repository CI and local Compose stack do not provision or approve a real production environment. Keep each item open until the operator records the target, test result, evidence and owner.

## 1. Minimum deployment boundary

Expected request path:

~~~text
Internet clients
      |
      v
Managed WAF / shared API gateway
  - real connection-source identity
  - shared rate/concurrency/request-size limits
  - strip untrusted forwarding and identity headers
      |
      v
Private API service network
  - one or more AetherFlow API replicas
  - origin reachable only from gateway egress
      |
      +-------------------+
      |                   |
      v                   v
Private PostgreSQL     Private NATS JetStream
  - TLS / RLS            - TLS / credentials / ACLs
  - durable state        - replicated streams
      ^
      |
Independent worker / publisher / inbox-consumer processes
~~~ 

The process-local authenticated-principal and failed-authentication limiters are secondary controls. They do not coordinate budgets across API replicas. Behind a reverse proxy, the application sees the proxy's TCP peer address. The gateway must enforce the shared client-source policy before forwarding.

**Implementation checkpoint (not deployment acceptance):** [`cloudflare-gateway/`](../../cloudflare-gateway/README.md) now contains a Cloudflare Worker, SQLite-backed Durable Object rate limiter, a closed-by-default `cloudflare.config.ts`, and Node policy tests. The DO SQL interface test uses a deterministic storage mock, not the Cloudflare Workers runtime or a real SQLite database. The original [Gateway CI run](https://github.com/pooya-labs/aetherflow/actions/runs/42504504348) passed 13/13 on source `afe9850`. The expanded regression suite now passes [Gateway CI run 42646781773](https://github.com/pooya-labs/aetherflow/actions/runs/42646781773) with **18/18 tests** on source `ef23eb423e492779af250d79980328888f7727ce`, including health-path, method-rejection, limiter-outage, redirect, and origin-transport failure cases. These runs verify repository policy logic only, not a deployed edge route. The Worker derives identity from `CF-Connecting-IP`, uses HMAC-SHA-256 with the Worker Secret before DO routing, strips untrusted forwarding/identity headers, preserves API Bearer credentials, injects a Worker Secret as `X-AetherFlow-Ingress-Token`, returns `Cache-Control: no-store`, and fails closed when rate limiting is unavailable. Per-client Durable Objects install one idle-cleanup alarm on first use; the alarm reschedules from the last activity or clears all SQLite/alarm storage after 24 idle hours. The cleanup path uses `blockConcurrencyWhile` to prevent a request racing with storage deletion. This code is covered by Node tests with a mocked storage interface; it is not yet verified in the Cloudflare runtime. The Rust API now refuses non-loopback binds without `AETHERFLOW_INGRESS_TOKEN` and requires the matching header on every route except `/health`. These are repository implementation details only: the Worker is not deployed, the account has no active Zone or Tunnel, and AetherFlow has no verified API origin. Complete the evidence and owner sign-off below before marking this section accepted.


## 2. Gateway and origin acceptance

- [ ] All public API traffic enters through one managed gateway/WAF policy with a shared counter store across gateway nodes.
- [ ] Verify the deployed Worker uses the edge-provided `CF-Connecting-IP` value, uses HMAC-SHA-256 with the shared Worker Secret before DO routing, and behaves correctly for IPv4, IPv6, NATed clients and connection reuse. The code path is implemented but has not yet been exercised against a deployed gateway.
- [ ] Verify the deployed Worker strips `CF-Connecting-IP`, `True-Client-IP`, `X-Forwarded-For`, `Forwarded`, `X-Real-IP`, and caller-supplied `X-AetherFlow-Ingress-Token`; it injects only the server-side ingress token. No forwarded client identity is trusted by the API.
- [ ] Verify the deployed Durable Object token bucket rate-limits requests from each client address before any operator/tenant credential reaches the API, and that the same budget is shared across Worker replicas and locations. The Worker implementation covers all API requests, not only invalid Bearer tokens; production thresholds still require load/NAT tuning.
- [ ] Throttled requests return HTTP 429 with a bounded Retry-After. Requests below the throttle threshold continue to receive the authentication error contract, not an invented success response.
- [ ] Verify the 2 MiB declared-body check and the API's streamed-body limit. Separately configure and test header-size, connection-count, concurrent-request, idle-timeout and request-timeout controls at the deployed edge/origin boundary; those controls are not all supplied by the Worker code.
- [ ] API origins, alternate replicas, node ports and service IPs are not publicly reachable. Network policy/security groups permit ingress only from gateway egress.
- [ ] Bypass tests prove that direct requests to every origin/replica fail from an untrusted network and that repeated unauthenticated attempts across multiple gateway nodes share one quota.
- [ ] Spoofed forwarding headers do not reset, evade or transfer the source quota. Test IPv4, IPv6, NATed clients and connection reuse.

Record the evidence: gateway configuration revision, source-key policy, threshold rationale, test date, test network and results. Do not copy the application's per-process default burst values as a production edge policy without load testing legitimate NAT concentration.

## 3. API, identity and secrets

- [ ] API replicas use non-loopback binds only with valid TLS certificates/private keys and the shared `AETHERFLOW_INGRESS_TOKEN`; direct requests without `X-AetherFlow-Ingress-Token` must receive 403 on every route except `/health`. Remote PostgreSQL connections require certificate and hostname verification.
- [ ] The deployment has a secret manager for operator/tenant credentials, database credentials, NATS credentials, TLS keys and OTLP exporter headers. No secret is stored in the repository, image, logs, shell history or public deployment metadata.
- [ ] Credential issuance, revocation, expiry, emergency rotation and restart/rollout behavior are tested.
- [ ] Operator identity limitations are accepted explicitly: the current platform API uses one shared operator token, so audit spans can identify only a shared operator credential, not a human. Do not claim per-person operator attribution until distinct operator identities or an approved external identity layer exists.
- [ ] Tenant permissions are tested against at least two tenants, including cross-tenant read, submit, cancel and worker-grant denial cases.
- [ ] CORS contains exact trusted browser origins only. CORS is not treated as authentication.
- [ ] /health is used only for process liveness. A separate authenticated synthetic transaction checks the complete API → PostgreSQL → worker → outbox → JetStream → inbox path.

## 4. PostgreSQL acceptance

- [ ] PostgreSQL is private-network-only and uses certificate-validated TLS. DATABASE_SSL_MODE=disable is not used for a network deployment.
- [ ] Migration-owner and restricted runtime credentials are separate. The API/worker role is not SUPERUSER, BYPASSRLS, REPLICATION, CREATEROLE or CREATEDB; it is not a protected-table owner and has no unrelated role memberships.
- [ ] The runtime role cannot create objects in the application schema. All ten tenant-owned tables have ENABLE and FORCE ROW LEVEL SECURITY.
- [ ] Migrations run in a controlled deployment window. The migration ledger is checked after upgrade and applied migrations are never edited.
- [ ] Connection limits, pool sizes, statement timeouts, lock timeouts, storage growth, WAL retention, replication health and failover behavior are monitored.
- [ ] Backups are encrypted, off-host, access-controlled and have retention/restore ownership. The database provider's managed backup and PITR settings are verified from the actual target environment.

Do not grant BYPASSRLS to the application role to make monitoring queries easier. Aggregate cross-tenant monitoring must use a separately reviewed reporting/export path with explicit access controls and privacy review.

## 5. NATS JetStream acceptance

- [ ] Production NATS uses TLS and separately managed credentials. Anonymous access and development credentials are disabled.
- [ ] Accounts and subject permissions are least-privilege and separated for publishers, consumers and operators.
- [ ] Stream replicas and node placement cover the declared failure domains. Local three-node Compose success is not evidence of a production topology.
- [ ] Stream storage type, retention, maximum age/bytes/messages, duplicate window, consumer AckWait, max delivery and disk headroom match the expected workload.
- [ ] Alerts cover node/route health, stream quorum, storage pressure, consumer pending/ack-pending counts, redeliveries and reconnect/error rates.
- [ ] NATS stream snapshot/restore is rehearsed against the selected production server version and credentials.
- [ ] The team accepts the at-least-once contract: external task side effects need stable idempotency keys or a transaction coupled to the downstream state.

## 6. Recovery objectives and rehearsal

**Owner-approved values (must be filled by the deployment owner):**

| Decision | Approved value | Evidence |
|---|---|---|
| PostgreSQL RPO | TBD — owner approval required | Backup/PITR policy and restore result |
| PostgreSQL RTO | TBD — owner approval required | Timed restore rehearsal |
| NATS stream recovery objective | TBD — owner approval required | Snapshot/restore and quorum rehearsal |
| Maximum tolerated workflow interruption | TBD — owner approval required | End-to-end recovery test |
| Backup retention and deletion policy | TBD — owner approval required | Provider policy and access review |

- [ ] PostgreSQL uses managed backups plus WAL archiving/PITR, or an equivalent provider capability that meets the approved RPO/RTO.
- [ ] JetStream recovery material is captured according to a documented cadence. PostgreSQL and JetStream recovery points are treated as one coordinated system boundary.
- [ ] A restore test runs in an isolated target using production-equivalent versions, roles, TLS, ACLs, storage and network boundaries.
- [ ] The rehearsal measures time to database availability, API acceptance, worker recovery and completion of a representative workflow.
- [ ] The rehearsal records replay/duplicate behavior and proves that external side effects remain idempotent.
- [ ] Restore ownership, escalation, break-glass credentials, evidence retention and post-restore integrity checks are assigned.

The repository's CI restore rehearsal proves only the tested logical PostgreSQL/JetStream restore-to-runtime path in disposable infrastructure. It is not production PITR, a provider failover test or an approved recovery objective.

## 7. Monitoring and SLO acceptance

- [ ] Scraping /api/v1/metrics is authenticated and uses protected credentials. Metric storage and traces have retention/access controls.
- [ ] Dashboard panels separate operation classes and status classes. Do not add raw paths, tenant IDs, workflow IDs, task IDs, credential values or request bodies as metric labels.
- [ ] API 5xx ratio, p95 latency, in-flight requests, abandoned handler futures and 429 rate have alert ownership and runbooks.
- [ ] Workflow outcome, oldest unpublished outbox age, oldest unprocessed inbox age, expired task leases and worker health are monitored through an approved database/exporter path.
- [ ] Black-box synthetic checks originate from the same network boundary as users and exercise authentication plus a controlled end-to-end workflow.
- [ ] Candidate objectives in the [SLO proposal](slo-proposal.md) are reviewed and explicitly approved or replaced. No candidate in that draft is a current SLA.
- [ ] Alert paging hours, escalation contacts, maintenance exclusions, burn rates and error-budget policy are documented.

Read-only SQL snapshots and PromQL examples are provided in the SLO proposal. They are diagnostic templates, not a production dashboard or alert configuration.

## 8. Change management and release evidence

- [ ] API and worker versions, migration version, container/image digests, Cargo.lock, gateway revision and NATS configuration revision are recorded for each rollout.
- [ ] Schema changes use expand/contract and remain compatible with the deployed application during rolling replacement.
- [ ] A canary deployment exercises authenticated reads, submission, task completion, cancellation and worker restart before broad rollout.
- [ ] Rollback is rehearsed. Roll back application artifacts only when compatible with the already-applied additive schema; database migrations are not assumed to have automatic down migrations.
- [ ] Security review, dependency/advisory review, SBOM, release provenance and incident/on-call ownership are approved.
- [ ] The final release commit has an exact-SHA green Rust, PostgreSQL/JetStream, security and documentation CI matrix.

## 9. Sign-off record

| Role | Named owner | Decision / date | Evidence |
|---|---|---|---|
| Service owner | TBD | Not approved | TBD |
| Gateway/network owner | TBD | Not approved | TBD |
| Database/recovery owner | TBD | Not approved | TBD |
| NATS/platform owner | TBD | Not approved | TBD |
| Security reviewer | TBD | Not approved | TBD |
| Operations/on-call owner | TBD | Not approved | TBD |

Do not mark this document approved or expose the service publicly until the required rows are completed by the responsible operators.
