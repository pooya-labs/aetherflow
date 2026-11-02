# ADR-0008: PostgreSQL-backed node membership and worker ownership

- **Status:** Accepted and implemented for worker-owned task transactions; multi-process partition/rejoin chaos coverage remains open.
- **Date:** 2026-11-02
- **Scope:** Node incarnation, membership leases, worker registration, drain semantics, and fencing across multiple AetherFlow processes

## Implementation checkpoint

The membership persistence slice is implemented in 0003_node_membership.sql and PgMembershipRepository. It covers node join/takeover, epoch changes, database-time heartbeats, worker registration/capacity, worker/node drain, child-lease capping, active-worker discovery and stale-handle rejection.

Task claim and all worker-owned heartbeat/settlement/recovery/promotion/aggregation operations validate the worker registration and parent node epoch in the same transaction as the task write. Claims are filtered by the task IDs present in the worker's local TaskRegistry. The worker loop renews membership while active and while draining in-flight handlers. PostgreSQL 18 + NATS run 41386154586 and Rust CI 41386154607 passed transaction-gate, capability-filter, stale-epoch takeover/recovery and long-handler drain tests. Multi-process partition/rejoin and abrupt worker-loss chaos tests remain future work.

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

Membership is an eligibility and liveness gate; it does not replace the existing task-run lease or fencing token. A task claim records its worker ID and existing task fencing token. Claim, heartbeat, completion, failure, registered cancellation, timeout, promotion, aggregation and lease recovery validate that the worker registration and parent node epoch are live and current.

The membership check and the task-row mutation occur in the same short PostgreSQL transaction, with lock order node → worker → task → attempt/outbox. The reaper currently recovers after the old task lease expires; it locks and verifies the replacement worker registration before recovery. Every recovery increments the task fencing token and closes the corresponding attempt transactionally. Membership takeover does not rewrite a still-live task lease.

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
- Existing per-task fencing remains in place and is combined with a live worker/node-epoch guard for registered worker operations.
- The membership implementation is covered by concurrent join, expired-lease takeover, stale-epoch task-write fencing, capability-filtered claims and graceful drain tests. Real network partition/rejoin and abrupt node/worker process-loss chaos tests remain required before production claims.
- Multi-node partition and failover chaos tests remain required before production claims. This ADR does not claim consensus, exactly-once side effects, or automatic fencing of downstream systems.
