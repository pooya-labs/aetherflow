# SLO proposal

**Status:** Draft for maintainer review; no objective or error budget in this document is approved or promised to users.

## Why targets are not active yet

The current request telemetry provides aggregate request counters, a 5xx counter, an in-flight gauge with drop-safe accounting and an abandoned-handler-future counter, plus bounded operation-class request/5xx counters, 2xx/3xx/4xx/5xx/other response-class counters, and latency histograms. The operation label is selected from a fixed enum (health, metrics, operator/tenant read, submit, mutation, or other); raw paths and tenant, workflow, and task identifiers are deliberately excluded. It does not yet provide a durable-workflow success SLI, workflow completion latency distribution, oldest-outbox age, inbox processing lag, or end-to-end availability across PostgreSQL and JetStream. The CI performance baseline is a synthetic one-worker profile, not production traffic.

The proposal below defines the measurements and candidate objectives to review. Do not enforce alert thresholds or advertise an SLA until the missing instrumentation, deployment topology, exclusions, and owner-approved targets exist.

## Proposed service indicators

| SLI | Proposed measurement | Current status |
|---|---|---|
| API availability | Successful eligible API responses divided by eligible API requests; report 5xx and server-side timeouts separately | Aggregate and fixed operation-class request/response counters and 5xx counters exist; eligible traffic and timeout attribution need a documented collector |
| API latency | p50/p95/p99 for read and submit classes, excluding client-side validation errors | Bounded histograms exist per fixed operation class; query the read/submit classes separately, but confirm bucket precision and collector quantile rules before adopting targets |
| Workflow completion | Workflows reaching a successful terminal state before their configured deadline divided by eligible accepted workflows | Requires a durable outcome metric or an external PostgreSQL-derived exporter |
| Queue/transport health | Oldest unpublished outbox age, oldest unprocessed inbox age, and publish/consume error rate | Durable rows are queryable; production-safe metrics and alert rules are not yet implemented |
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
2. Add durable workflow terminal outcome and completion-latency observations, with explicit labels for platform error, handler error, cancellation, and deadline expiry.
3. Add oldest pending outbox age, oldest unprocessed inbox age, retry/lease-expiry counters, and JetStream reconnect/error signals.
4. Define black-box probes from the same network boundary as users; `GET /health` is process liveness only and must not be treated as API/database readiness.
5. Add alert ownership, paging hours, maintenance exclusions, and a runbook link to every production alert.



## Operational snapshots for review (not an SLO)

The following read-only snapshots help an operator validate the current database state while designing a collector/exporter. They deliberately aggregate across rows and do not expose tenant, workflow, task or event identifiers. Execute them only through an approved reporting connection with the intended row-visibility policy. Do not grant BYPASSRLS to the application runtime role or weaken tenant policies for monitoring.

~~~sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '1s';

-- Terminal workflow outcomes and successful completion p95 for a recent window.
SELECT
    status,
    count(*) AS workflows,
    percentile_cont(0.95) WITHIN GROUP (
        ORDER BY extract(epoch FROM (finished_at - created_at))
    ) FILTER (WHERE status = 'succeeded') AS success_completion_p95_seconds
FROM workflow_runs
WHERE created_at >= clock_timestamp() - INTERVAL '30 minutes'
  AND finished_at IS NOT NULL
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

The abandoned-request counter does not identify whether a future was dropped by a client disconnect, upstream deadline, panic or process shutdown. It is a diagnostic signal, not a standalone availability SLI. The existing metrics do not yet provide durable workflow completion outcomes, queue-age gauges or platform-versus-handler failure attribution; use the SQL/exporter path until those signals are deliberately implemented and reviewed.

See the [production deployment acceptance checklist](production-acceptance.md) for gateway, PostgreSQL, NATS, recovery, monitoring and owner sign-off gates.

## Decisions required from maintainers

- Confirm or replace the candidate API availability and latency objectives.
- Define eligible traffic, maintenance exclusions, and how client 4xx/429, timeouts, and deployment restarts are counted.
- Select production RPO/RTO and a storage/backup service that can actually meet them.
- Decide whether workflow completion is a product promise or an execution-engine internal indicator.
- Approve alert burn rates, on-call ownership, and a review cadence.

Until those decisions and measurements are complete, this remains a proposal and not an SLA.
