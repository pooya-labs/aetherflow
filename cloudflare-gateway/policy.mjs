const BLOCKED_REQUEST_HEADERS = [
  "connection",
  "content-length",
  "forwarded",
  "host",
  "keep-alive",
  "proxy-authorization",
  "proxy-connection",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "cf-connecting-ip",
  "true-client-ip",
  "x-aetherflow-ingress-token",
  "x-client-ip",
  "x-forwarded-for",
  "x-forwarded-host",
  "x-forwarded-port",
  "x-forwarded-proto",
  "x-original-url",
  "x-real-ip",
  "x-rewrite-url",
];

/** Parse a positive integer setting, using the supplied safe default when absent. */
export function positiveInteger(value, fallback, name) {
  if (value === undefined || value === null || value === "") return fallback;
  if (!/^[1-9]\d*$/.test(String(value))) {
    throw new TypeError(`${name} must be a positive integer`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed > 1_000_000) {
    throw new TypeError(`${name} must be a positive integer no greater than 1000000`);
  }
  return parsed;
}

/** Accept only a fixed HTTPS origin; request data can never select the upstream host. */
export function parseOrigin(value) {
  if (typeof value !== "string" || value.trim() === "") {
    throw new TypeError("AETHERFLOW_ORIGIN is required");
  }
  const origin = new URL(value);
  if (
    origin.protocol !== "https:" ||
    origin.username ||
    origin.password ||
    origin.pathname !== "/" ||
    origin.search ||
    origin.hash
  ) {
    throw new TypeError("AETHERFLOW_ORIGIN must be an HTTPS origin without credentials, path, query, or fragment");
  }
  return origin;
}

/** Only the versioned control-plane API and the public liveness route are exposed. */
export function isAllowedPath(pathname) {
  return pathname === "/health" || pathname.startsWith("/api/v1/");
}

/** SHA-256 keeps raw client addresses out of Durable Object names. */
export async function clientBucketName(sourceIp) {
  if (typeof sourceIp !== "string" || sourceIp.length < 2 || sourceIp.length > 64) {
    throw new TypeError("A trusted client address is required");
  }
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(sourceIp));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

/**
 * Pure token-bucket transition. SQLite reads/writes around this function occur
 * synchronously inside one Durable Object turn, so concurrent requests cannot
 * interleave between the read and write.
 */
export function consumeTokenBucket(previous, nowMs, capacity, refillPerSecond) {
  if (
    !Number.isSafeInteger(nowMs) ||
    !Number.isSafeInteger(capacity) ||
    capacity < 1 ||
    !Number.isSafeInteger(refillPerSecond) ||
    refillPerSecond < 1
  ) {
    throw new TypeError("Invalid token-bucket parameters");
  }

  const storedTokens = previous ? Number(previous.tokens) : capacity;
  const storedAt = previous ? Number(previous.updated_at_ms) : nowMs;
  if (!Number.isFinite(storedTokens) || !Number.isSafeInteger(storedAt)) {
    throw new TypeError("Stored token-bucket state is invalid");
  }

  const elapsedSeconds = Math.max(0, nowMs - storedAt) / 1000;
  const available = Math.min(capacity, Math.max(0, storedTokens) + elapsedSeconds * refillPerSecond);
  const allowed = available >= 1;
  const tokens = allowed ? available - 1 : available;
  const retryAfterSeconds = allowed
    ? 0
    : Math.max(1, Math.ceil((1 - available) / refillPerSecond));

  return {
    allowed,
    tokens,
    updated_at_ms: nowMs,
    retry_after_seconds: retryAfterSeconds,
  };
}

/** Build an origin-bound request and replace all caller-controlled identity headers. */
export function buildUpstreamRequest(request, origin, ingressToken) {
  const incomingUrl = new URL(request.url);
  const target = new URL(incomingUrl.pathname + incomingUrl.search, origin);
  const headers = new Headers(request.headers);

  for (const name of BLOCKED_REQUEST_HEADERS) headers.delete(name);
  headers.set("x-aetherflow-ingress-token", ingressToken);

  const init = {
    method: request.method,
    headers,
    redirect: "manual",
  };
  if (request.method !== "GET" && request.method !== "HEAD") {
    init.body = request.body;
    // Required by Node's fetch implementation for streamed bodies; harmless in Workers.
    init.duplex = "half";
  }
  return new Request(target, init);
}

export function jsonError(status, code, extraHeaders = {}) {
  return Response.json(
    { error: { code } },
    {
      status,
      headers: {
        "Cache-Control": "no-store",
        ...extraHeaders,
      },
    },
  );
}
