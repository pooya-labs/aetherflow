import {
  buildUpstreamRequest,
  clientBucketName,
  consumeTokenBucket,
  isAllowedPath,
  isValidIngressToken,
  jsonError,
  parseOrigin,
  positiveInteger,
} from "./policy.mjs";

/**
 * One SQLite-backed object per hashed client address. No single global object
 * becomes a throughput bottleneck; each client's token bucket is strongly
 * consistent across all Worker replicas and Cloudflare locations.
 */
const IDLE_BUCKET_TTL_MS = 24 * 60 * 60 * 1000;
const CREATE_BUCKET_TABLE = `
  CREATE TABLE IF NOT EXISTS client_rate_limit (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    tokens REAL NOT NULL,
    updated_at_ms INTEGER NOT NULL
  )
`;

export class ClientRateLimiter {
  constructor(ctx, now = () => Date.now()) {
    this.ctx = ctx;
    this.now = now;
    this.ensureSchema();
  }

  /**
   * deleteAll() in an idle alarm also removes the SQLite schema. A Durable
   * Object instance can remain alive after its alarm runs, so the next request
   * must be able to recreate the table without waiting for a new constructor.
   */
  ensureSchema() {
    this.ctx.storage.sql.exec(CREATE_BUCKET_TABLE);
  }

  async fetch(request) {
    if (request.method !== "POST") {
      return jsonError(405, "method_not_allowed", { Allow: "POST" });
    }

    // The idle alarm may have deleted every SQLite table while this instance
    // was still alive. Recreate the fixed schema before reading bucket state.
    this.ensureSchema();

    let settings;
    try {
      settings = await request.json();
    } catch {
      return jsonError(400, "invalid_rate_limit_request");
    }

    const capacity = Number(settings?.capacity);
    const refillPerSecond = Number(settings?.refill_per_second);
    if (
      !Number.isSafeInteger(capacity) ||
      capacity < 1 ||
      capacity > 1_000_000 ||
      !Number.isSafeInteger(refillPerSecond) ||
      refillPerSecond < 1 ||
      refillPerSecond > 1_000_000
    ) {
      return jsonError(400, "invalid_rate_limit_settings");
    }

    const previous = this.ctx.storage.sql
      .exec("SELECT tokens, updated_at_ms FROM client_rate_limit WHERE id = 1")
      .toArray()[0] ?? null;
    const nowMs = this.now();
    const decision = consumeTokenBucket(previous, nowMs, capacity, refillPerSecond);

    // Synchronous SQLite calls do not yield the Durable Object event loop.
    // The read/transition/write therefore executes as one non-interleaved turn.
    this.ctx.storage.sql.exec(
      `INSERT INTO client_rate_limit (id, tokens, updated_at_ms)
       VALUES (1, ?1, ?2)
       ON CONFLICT(id) DO UPDATE SET
         tokens = excluded.tokens,
         updated_at_ms = excluded.updated_at_ms`,
      decision.tokens,
      decision.updated_at_ms,
    );

    // Set the first idle-cleanup alarm only once per bucket lifetime. Active
    // buckets are rescheduled from their last request when the alarm runs.
    if (!previous) {
      await this.ctx.storage.setAlarm(nowMs + IDLE_BUCKET_TTL_MS);
    }

    return Response.json({
      allowed: decision.allowed,
      retry_after_seconds: decision.retry_after_seconds,
    });
  }

  async alarm() {
    // Alarms can run concurrently with requests. Block delivery while checking
    // the last-seen timestamp and either rescheduling or deleting all storage.
    await this.ctx.blockConcurrencyWhile(async () => {
      const row = this.ctx.storage.sql
        .exec("SELECT updated_at_ms FROM client_rate_limit WHERE id = 1")
        .toArray()[0] ?? null;

      if (!row) {
        await this.ctx.storage.deleteAll();
        return;
      }

      const expiresAt = Number(row.updated_at_ms) + IDLE_BUCKET_TTL_MS;
      if (this.now() < expiresAt) {
        await this.ctx.storage.setAlarm(expiresAt);
        return;
      }

      // deleteAll() clears the SQL schema and alarm metadata too. The next
      // request calls ensureSchema() before reading, even if this instance was
      // not evicted and its constructor therefore does not run again.
      await this.ctx.storage.deleteAll();
    });
  }
}

function gatewaySettings(env) {
  const origin = parseOrigin(env.AETHERFLOW_ORIGIN);
  if (!isValidIngressToken(env.AETHERFLOW_INGRESS_TOKEN)) {
    throw new TypeError("AETHERFLOW_INGRESS_TOKEN is required");
  }
  return {
    origin,
    ingressToken: env.AETHERFLOW_INGRESS_TOKEN,
    burst: positiveInteger(env.AETHERFLOW_GATEWAY_BURST, 60, "AETHERFLOW_GATEWAY_BURST"),
    refillPerSecond: positiveInteger(
      env.AETHERFLOW_GATEWAY_PER_SECOND,
      20,
      "AETHERFLOW_GATEWAY_PER_SECOND",
    ),
  };
}

async function applyClientRateLimit(request, env, settings) {
  const sourceIp = request.headers.get("CF-Connecting-IP");
  if (!sourceIp) return jsonError(400, "client_address_unavailable");

  let bucketName;
  try {
    bucketName = await clientBucketName(sourceIp, settings.ingressToken);
  } catch {
    return jsonError(400, "client_address_invalid");
  }

  try {
    const id = env.CLIENT_RATE_LIMITER.idFromName(bucketName);
    const stub = env.CLIENT_RATE_LIMITER.get(id);
    const response = await stub.fetch("https://rate-limiter.internal/check", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        capacity: settings.burst,
        refill_per_second: settings.refillPerSecond,
      }),
    });
    if (!response.ok) return jsonError(503, "rate_limiter_unavailable");
    const decision = await response.json();
    if (decision.allowed !== true) {
      return jsonError(429, "rate_limited", {
        "Retry-After": String(Math.max(1, Number(decision.retry_after_seconds) || 1)),
      });
    }
    return null;
  } catch {
    // Fail closed: if the shared limiter is unavailable, do not bypass it.
    return jsonError(503, "rate_limiter_unavailable");
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (!isAllowedPath(url.pathname)) return jsonError(404, "not_found");

    if (!["GET", "HEAD", "POST", "OPTIONS"].includes(request.method)) {
      return jsonError(405, "method_not_allowed", { Allow: "GET, HEAD, POST, OPTIONS" });
    }

    const contentLength = request.headers.get("content-length");
    if (contentLength !== null) {
      if (!/^[0-9]+$/.test(contentLength)) return jsonError(400, "invalid_content_length");
      if (Number(contentLength) > 2 * 1024 * 1024) {
        return jsonError(413, "payload_too_large");
      }
    }

    let settings;
    try {
      settings = gatewaySettings(env);
    } catch {
      // Publicly reachable but not configured is intentionally not a proxy.
      return jsonError(503, "gateway_not_configured");
    }

    if (url.pathname !== "/health") {
      const limited = await applyClientRateLimit(request, env, settings);
      if (limited) return limited;
    }

    let upstreamRequest;
    try {
      upstreamRequest = buildUpstreamRequest(request, settings.origin, settings.ingressToken);
    } catch {
      return jsonError(400, "invalid_upstream_request");
    }

    try {
      const upstream = await fetch(upstreamRequest);
      // The API contract is same-origin JSON. Do not expose a redirect Location
      // header that could reveal the origin address or invite direct-origin use.
      if (upstream.status >= 300 && upstream.status < 400) {
        return jsonError(502, "origin_redirect_rejected");
      }

      const headers = new Headers(upstream.headers);
      headers.delete("server");
      headers.delete("alt-svc");
      headers.set("Cache-Control", "no-store");
      return new Response(upstream.body, {
        status: upstream.status,
        statusText: upstream.statusText,
        headers,
      });
    } catch {
      return jsonError(502, "origin_unavailable");
    }
  },
};
