# SLO proposal

**Status:** Draft for maintainer review; no objective or error budget in this document is approved or promised to users.

## Why targets are not active yet

The API now exposes two complementary Prometheus surfaces. The operator-only `GET /api/v1/metrics` reports process-local HTTP counters, bounded operation/status classes, concurrency, and latency histograms. The tenant-only `GET /api/v1/tenant/metrics` returns a read-only repeatable-read snapshot of workflow status counts, oldest active workflow age, outbox pending/ready counts and oldest ready lag, expired outbox claims, inbox pending/processing counts and oldest unprocessed age, expired inbox/task leases, terminal workflow outcome counts, and successful completion-latency p50/p95/p99 for the rolling 30-minute window. Completion latency is measured from persisted workflow acceptance (`started_at`) to terminal state (`finished_at`), including queue and scheduling time. Scope is derived from the validated credential and relies on FORCE RLS; only fixed status/state labels are exported, never tenant, workflow, task, event, credential, or payload identifiers.

These additions provide recent durable outcome and execution-latency observations, but not a complete workflow SLO. The HTTP metrics surfaces still lack an eligible-accepted denominator tied to an agreed deadline, platform-vs-handler failure attribution, and transport outcomes exported through the API. Publisher and consumer adapter handles render bounded process-local snapshots through `render_prometheus()` using fixed metric names and no caller-controlled labels. The standalone worker also has an optional `GET /metrics` Prometheus endpoint combining its one publisher and one consumer handle in that process. It is disabled by default, requires both `AETHERFLOW_METRICS_BIND` and a 32–256 character URL-safe Bearer token, stores only the SHA-256 token digest, and uses constant-time comparison. The listener rejects non-loopback bind addresses because it serves plain HTTP; remote collectors must use an authenticated TLS-terminating proxy on the same host. Its fixed-name output does not include the API process, aggregate multiple worker processes, or create a cross-replica/platform-wide series. The transport now also offers an opt-in `NatsConnectionConfig::connect_with_metrics()` path that returns the existing client plus a process-local lifecycle metrics handle for connected state, disconnects, successful reconnects, server errors, client errors, and terminal close events. The existing `connect()` method remains unchanged. The standalone worker now includes its opt-in connection handle in the same authenticated, loopback-only `GET /metrics` response as publisher and consumer snapshots; the listener remains disabled by default. Same-process aggregation for multiple adapter instances and cross-replica aggregation remain open. Tenant metrics must be scraped with tenant-scoped credentials and an access-controlled collector; tenant samples should not be mixed into an unrestricted central view. The CI performance baseline is a synthetic one-worker profile, not production traffic.

The proposal below defines the measurements and candidate objectives to review. Do not enforce alert thresholds or advertise an SLA until the missing instrumentation, deployment topology, exclusions, and owner-approved targets exist.

## Proposed service indicators

| SLI | Proposed measurement | Current status |
|---|---|---|
| API availability | Successful eligible API responses divided by eligible API requests; report 5xx and server-side timeouts separately | Aggregate and fixed operation-class request/response counters and 5xx counters exist; eligible traffic and timeout attribution need a documented collector |
| API latency | p50/p95/p99 for read and submit classes, excluding client-side validation errors | Bounded histograms exist per fixed operation class; query the read/submit classes separately, but confirm bucket precision and collector quantile rules before adopting targets |
| Workflow completion | Workflows reaching a successful terminal state before their configured deadline divided by eligible accepted workflows | Recent terminal outcome counts and successful completion-latency p50/p95/p99 are now available per tenant over 30 minutes; eligible-accepted denominator, agreed deadline, and platform-vs-handler attribution remain open |
| Queue/transport health | Oldest ready outbox lag, unpublished/ready count, expired claims, inbox pending/processing count and age, expired inbox/task leases | Tenant-scoped snapshot gauges and process-local publisher/consumer outcome counters now exist; transport counters are not exported by the API, and cross-replica aggregation, retention, and production alert rules remain open |
| Recovery | Successful restore drills and measured time from recovery start to service acceptance | Isolated CI logical restore rehearsal exists; production RPO/RTO and WAL/PITR procedure are not established |

## Candidate objectives for discussion

These are deliberately provisional starting points, not accepted service commitments:

| Objective candidate | Candidate window | Notes |
|---|---|---|
| API availability ≥ 99.5% | Rolling 30 days | Equivalent to at most 216 minutes of eligible unavailability per 30-day window; requires an agreed probe and maintenance policy |
| API p95 latency ≤ 500 ms for ordinary reads/submissions | Rolling 30 days | Must define payload size, page size, tenant count, database/network location, and load profile before acceptance |
| API p99 latency ≤ 2 s for ordinary reads/submissions | Rolling 30 days | Should be reviewed alongside p95 and saturation/timeout data |
| Workflow success rate | TBD | Do not count user cancellation or task-handler business errors as platform failures without a separately agreed classification |
| Outbox/inbox processing lag | TBD | Choose a target only after production event volume, broker retention, and consumer concurrency are measured |
| Recovery point objective / recovery time objective | Owner decision | Requires a managed PostgreSQL/WAL policy, JetStream snapshot cadence, and a coordinated restore test on the target deployment |

## Error-budget policy proposal

For an approved availability objective, calculate the error budget over the same rolling window. A 99.5% target yields 0.5% unavailability, or 216 minutes in 30 days. Freeze risky changes only after an agreed burn-rate policy is defined; do not translate the illustrative target into an operational freeze rule today.

Workflow completion and API availability must have separate budgets. A user-provided handler that returns a business error is not automatically an infrastructure availability failure. The event-delivery model is at-least-once and external side effects need their own idempotency boundary.

## Instrumentation required before adoption

1. Define the eligible-request denominator and collector rules for each bounded operation class; response classes and abandoned-handler-future counts are available, but server timeouts, client disconnects, and deployment restarts are not separately attributable. Decide whether 4xx/429 responses count as unavailable for each SLI. Do not add raw path, tenant, workflow, or task labels.
2. Define the eligible-accepted workflow denominator and deadline semantics, plus platform-vs-handler outcome attribution. The tenant snapshot now exposes recent terminal outcomes and successful completion-latency p50/p95/p99, but cancellation and handler business errors are not yet separated from platform failures.
3. The NATS transport exposes process-local outcome snapshots at each adapter boundary, plus Prometheus text renderers on the publisher and consumer metrics handles. The standalone worker exposes both handles through its optional authenticated loopback `GET /metrics` endpoint; the listener is disabled unless explicitly configured. Publisher counters report started attempts, JetStream storage confirmations, returned publish errors, interrupted futures, and in-flight work. Consumer counters report processed deliveries, inbox duplicate suppression, busy deferrals, handler failures, and broker/storage/protocol failures. The renderer uses fixed metric names without tenant, event, workflow, task, credential, subject, or payload labels. The worker currently creates one publisher and one consumer per process, plus one NATS connection lifecycle handle, so the endpoint combines those three process-local sources; if additional adapter instances are introduced, deliberate aggregation is required to avoid duplicate metric samples. Counters reset with the process and are not exported by the API server or aggregated across replicas; an access-controlled external collector, JetStream reconnect-specific signals, and production alert ownership remain open. The tenant snapshot continues to expose per-tenant outbox/inbox counts, age gauges, and expired lease counts.
4. Define black-box probes from the same network boundary as users; `GET /health` is process liveness only and must not be treated as API/database readiness.
5. Add alert ownership, paging hours, maintenance exclusions, and a runbook link to every production alert.



## Operational snapshots for review (not an SLO)

The following read-only SQL snapshots remain useful for an approved diagnostic/reporting connection. The tenant API endpoint is the preferred application-level view for one authenticated tenant; these direct SQL examples are not a substitute for that endpoint and must be executed with an explicitly reviewed row-visibility policy. They aggregate without exposing tenant, workflow, task or event identifiers. Do not grant BYPASSRLS to the application runtime role or weaken tenant policies for monitoring.

~~~sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '1s';

-- Terminal workflow outcomes and successful completion p95 for a recent window.
-- Match the tenant metrics contract: use finished_at for the rolling window
-- and started_at as the accepted-workflow timestamp for completion latency.
SELECT
    status,
    count(*) AS workflows,
    percentile_cont(0.95) WITHIN GROUP (
        ORDER BY extract(epoch FROM (finished_at - started_at))
    ) FILTER (
        WHERE status = 'succeeded'
          AND started_at IS NOT NULL
          AND finished_at >= started_at
    ) AS success_completion_p95_seconds
FROM workflow_runs
WHERE finished_at >= clock_timestamp() - INTERVAL '30 minutes'
  AND status IN ('succeeded', 'failed', 'cancelled', 'timed_out')
GROUP BY status
ORDER BY status;

-- Unpublished outbox volume, oldest age and expired claims.
SELECT
    count(*) FILTER (WHERE published_at IS NULL) AS unpublished_events,
    max(extract(epoch FROM (clock_timestamp() - created_at)))
        FILTER (WHERE published_at IS NULL) AS oldest_unpublished_seconds,
    count(*) FILTER (
        WHERE published_at IS NULL
          AND claim_owner IS NOT NULL
          AND claim_expires_at < clock_timestamp()
    ) AS expired_claims
FROM outbox_events;

-- Inbox backlog age and processing leases that have expired.
SELECT
    count(*) FILTER (WHERE status <> 'processed') AS unprocessed_events,
    max(extract(epoch FROM (clock_timestamp() - received_at)))
        FILTER (WHERE status <> 'processed') AS oldest_unprocessed_seconds,
    count(*) FILTER (
        WHERE status = 'processing'
          AND lease_expires_at < clock_timestamp()
    ) AS expired_processing_leases
FROM consumer_inbox;

-- Running tasks whose durable lease is already expired.
SELECT
    count(*) FILTER (
        WHERE status = 'running'
          AND lease_expires_at < clock_timestamp()
    ) AS expired_running_task_leases,
    count(*) FILTER (WHERE status = 'failed') AS failed_tasks,
    count(*) FILTER (WHERE status = 'timed_out') AS timed_out_tasks
FROM task_runs;

ROLLBACK;
~~~

The queries are snapshots, not durable time series. A production collector must poll at a documented interval, control query cost, retain timestamped samples, identify the observation source, and apply the approved tenant/privacy model. Workflow rows with status failed may reflect handler/business errors; do not count every failure as platform unavailability without an agreed attribution rule.

### PromQL examples for the existing bounded HTTP metrics

These expressions use the current fixed-cardinality metric names. They are examples for dashboard design; the scrape job must authenticate to the metrics endpoint and no numeric alert threshold is approved here.

~~~promql
# 5xx fraction by bounded operation class.
sum by (operation) (
  rate(aetherflow_http_operation_requests_failed_total[5m])
)
/
clamp_min(
  sum by (operation) (
    rate(aetherflow_http_operation_requests_total[5m])
  ),
  1e-9
)

# p95 request latency by bounded operation class.
histogram_quantile(
  0.95,
  sum by (operation, le) (
    rate(aetherflow_http_operation_request_duration_seconds_bucket[5m])
  )
)

# Response-class mix; 4xx includes authentication and rate-limit responses.
sum by (operation, status_class) (
  rate(aetherflow_http_operation_responses_total[5m])
)

# Handler futures dropped before a response was observed.
sum by (operation) (
  rate(aetherflow_http_operation_requests_abandoned_total[5m])
)

# Current API request concurrency.
aetherflow_http_requests_in_flight
~~~

The abandoned-request counter does not identify whether a future was dropped by a client disconnect, upstream deadline, panic or process shutdown. It is a diagnostic signal, not a standalone availability SLI. The tenant snapshot now provides per-tenant terminal workflow outcomes, successful completion-latency percentiles, queue-age gauges and expired-lease counts. It still does not provide an eligible-accepted denominator tied to an agreed deadline, platform-versus-handler failure attribution, cumulative publish/consume errors, or cross-replica aggregation. Use tenant-scoped credentials and an access-controlled collector; do not turn the per-tenant snapshot into an unrestricted platform-wide view.

See the [production deployment acceptance checklist](production-acceptance.md) for gateway, PostgreSQL, NATS, recovery, monitoring and owner sign-off gates.

## Decisions required from maintainers

- Confirm or replace the candidate API availability and latency objectives.
- Define eligible traffic, maintenance exclusions, and how client 4xx/429, timeouts, and deployment restarts are counted.
- Select production RPO/RTO and a storage/backup service that can actually meet them.
- Decide whether workflow completion is a product promise or an execution-engine internal indicator.
- Approve alert burn rates, on-call ownership, and a review cadence.

Until those decisions and measurements are complete, this remains a proposal and not an SLA.
