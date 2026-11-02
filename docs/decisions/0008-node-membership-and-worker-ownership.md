# ADR-0008: PostgreSQL-backed node membership and worker ownership

- **Status:** Accepted; migration v3 and the first membership repository slice are implemented on `main`. Task transaction gating remains open.
- **Date:** 2026-11-02
- **Scope:** Node incarnation, membership leases, worker registration, drain semantics, and fencing across multiple AetherFlow processes

## Implementation checkpoint

The first persistence slice is implemented in `0003_node_membership.sql` and `PgMembershipRepository`. It covers node join/takeover, epoch changes, database-time heartbeats, worker registration/capacity, worker/node drain, child-lease capping, active-worker discovery and stale membership-handle rejection. PostgreSQL 18 run `41382505310` and Rust CI run `41382505507` passed the migration, repository and integration tests. Task claim and settlement SQL has not yet been updated to require the matching worker registration and node epoch in the same transaction; that remains the next step under this ADR.

## Context

M04 now provides durable task leases, a transactional outbox, a PostgreSQL inbox, and a real NATS JetStream publisher/consumer. Those mechanisms make a single durable worker process recoverable, but they do not define which AetherFlow nodes are currently members of a cluster, how a restarted process supersedes an old incarnation, or how operators distinguish an active worker from a process whose lease has expired.

M05 must add cluster membership without creating a second, conflicting source of truth for task ownership or event delivery.

## Decisions

### PostgreSQL is the membership authority

PostgreSQL stores node and worker membership leases and is authoritative for membership liveness, ownership epochs, and admission to new work. Use PostgreSQL `clock_timestamp()` for all lease decisions. NATS JetStream remains an event transport; it does not elect members or decide task ownership.

This keeps durable workflow state, task leases, and membership leases under one time and transaction authority. During a database partition, a node fails closed: it stops claiming new tasks and cannot renew membership or task leases. It may still have in-flight code executing locally, so external side effects remain subject to downstream idempotency.

### Stable node identity and per-process incarnation

A configured `node_id` is the stable logical identity of a node. Every process start generates a new `incarnation_id`; it must not reuse the prior process's incarnation token.

A node membership record carries a monotonically increasing `membership_epoch`, lease expiry, last heartbeat, and lifecycle state (`active` or `draining`). The current incarnation may renew only while node ID, incarnation ID, epoch, state, and unexpired database lease match. A different incarnation may take over only after the prior membership lease expires; takeover increments the epoch. An old process cannot renew, drain, register workers, or mutate durable task state using an obsolete epoch.

Do not infer liveness from a persisted `online` boolean. A node is live only while its database lease is unexpired. Expired records may be retained for diagnostics and cleaned up by a separate retention policy.

### Workers are registered under a node incarnation

Each `worker_id` is unique to one process incarnation and belongs to exactly one `node_id` plus `membership_epoch`. A worker registration records its state (`active` or `draining`), concurrency capacity, heartbeat, and lease expiry. Worker heartbeats are valid only while the parent node incarnation and epoch are still current.

A draining node admits no new worker claims. It asks workers to stop claiming new tasks and finish in-flight work within their configured shutdown policy. On abrupt process loss, node and worker leases expire and become recovery candidates. Reusing a worker ID across restarts is prohibited; a restarted process gets new worker identities under its new node epoch.

### Task leases remain the execution ownership mechanism

Membership is an eligibility and liveness gate; it does not replace the existing task-run lease or fencing token. A task claim records its worker ID and existing task fencing token. Claim, heartbeat, completion, failure, cancellation, and lease recovery must additionally validate that the worker registration and parent node epoch are live and current.

The membership check and the task-row mutation must occur in the same short PostgreSQL transaction, with a consistent lock order. A reaper may recover a task before its task lease expires when the owning worker/node membership is definitively expired, but only after locking the task and verifying the matching worker registration and node epoch. Every recovery still increments the task fencing token and closes the corresponding attempt transactionally.

This prevents a stale process from committing after a new node incarnation takes over while preserving the task row's fencing token as the final per-task write guard. Membership expiry alone must never fabricate successful completion or imply that external work stopped.

### Scheduling remains database-coordinated

M05 initially retains PostgreSQL `FOR UPDATE SKIP LOCKED` for task admission across nodes. Do not add static sharding, rendezvous hashing, or a leader-elected scheduler until load tests demonstrate that the current database-coordinated claim path is insufficient.

Node membership is used for liveness, capacity visibility, admission, draining, and stale-incarnation fencing—not for assigning permanent task partitions. No global leader is required for workers to claim independent ready tasks.

### Partition and rejoin behavior

- If a node loses PostgreSQL connectivity, it fails closed for new claims and durable mutations. It cannot extend membership or task leases using local time.
- If the node returns before its membership lease expires with the same incarnation and epoch, it may renew normally.
- If the lease expired and another incarnation acquired a higher epoch, the old process is fenced and must shut down or re-register as a new incarnation; it cannot reclaim its former epoch.
- Workers with stale node epochs cannot renew their registration or finish previously claimed tasks.
- Task and broker delivery remain at-least-once. Membership fencing cannot undo an HTTP call or other external effect already issued by a stale handler.

## Consequences

- PostgreSQL is the single authority for node membership, worker eligibility, task ownership, and persisted lease time.
- Process restarts create a new node incarnation and a higher membership epoch rather than impersonating the old process.
- A node cannot silently keep claiming tasks after it has entered draining or lost its membership lease.
- Existing per-task fencing remains in place; future task repository methods gain an additional live-worker/node-epoch guard.
- Initial membership implementation requires an additive PostgreSQL migration and transaction tests for concurrent join, expired-lease takeover, stale epoch rejection, drain, and node/worker loss.
- Multi-node partition and failover chaos tests remain required before production claims. This ADR does not claim consensus, exactly-once side effects, or automatic fencing of downstream systems.
