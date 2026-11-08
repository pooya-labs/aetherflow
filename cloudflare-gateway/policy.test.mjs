import assert from "node:assert/strict";
import test from "node:test";

import worker, {
  ClientRateLimiter,
} from "./worker.mjs";
import {
  buildUpstreamRequest,
  clientBucketName,
  consumeTokenBucket,
  isAllowedPath,
  parseOrigin,
  positiveInteger,
} from "./policy.mjs";

test("origin is fixed to a clean HTTPS origin", () => {
  assert.equal(parseOrigin("https://api.example.test").origin, "https://api.example.test");
  for (const value of [
    "http://api.example.test",
    "https://user:pass@api.example.test",
    "https://api.example.test/prefix",
    "https://api.example.test/?route=elsewhere",
    "https://api.example.test/#fragment",
    "",
  ]) {
    assert.throws(() => parseOrigin(value));
  }
});

test("only health and versioned API paths are exposed", () => {
  assert.equal(isAllowedPath("/health"), true);
  assert.equal(isAllowedPath("/api/v1/workflow-runs"), true);
  assert.equal(isAllowedPath("/admin"), false);
  assert.equal(isAllowedPath("/api/v2/workflow-runs"), false);
});

test("positive configuration rejects zero, signs, decimals and unbounded values", () => {
  assert.equal(positiveInteger(undefined, 60, "burst"), 60);
  assert.equal(positiveInteger("12", 60, "burst"), 12);
  for (const value of ["0", "-1", "1.2", "Infinity", "1000001", "a"]) {
    assert.throws(() => positiveInteger(value, 60, "burst"));
  }
});

test("token bucket enforces a burst and refills without accumulating rejection debt", () => {
  const first = consumeTokenBucket(null, 1_000, 2, 1);
  assert.deepEqual(
    { allowed: first.allowed, tokens: first.tokens, retry: first.retry_after_seconds },
    { allowed: true, tokens: 1, retry: 0 },
  );

  const second = consumeTokenBucket(
    { tokens: first.tokens, updated_at_ms: first.updated_at_ms },
    1_000,
    2,
    1,
  );
  assert.equal(second.allowed, true);
  assert.equal(second.tokens, 0);

  const blocked = consumeTokenBucket(
    { tokens: second.tokens, updated_at_ms: second.updated_at_ms },
    1_000,
    2,
    1,
  );
  assert.equal(blocked.allowed, false);
  assert.equal(blocked.retry_after_seconds, 1);
  assert.equal(blocked.tokens, 0);

  const refilled = consumeTokenBucket(
    { tokens: blocked.tokens, updated_at_ms: blocked.updated_at_ms },
    2_000,
    2,
    1,
  );
  assert.equal(refilled.allowed, true);
  assert.equal(refilled.tokens, 0);
});

test("token bucket clamps clock rollback and never exceeds capacity", () => {
  const result = consumeTokenBucket(
    { tokens: 1, updated_at_ms: 10_000 },
    1_000,
    4,
    3,
  );
  assert.equal(result.allowed, true);
  assert.equal(result.tokens, 0);
  const capped = consumeTokenBucket(
    { tokens: 4, updated_at_ms: 0 },
    100_000,
    4,
    99,
  );
  assert.equal(capped.tokens, 3);
});

test("clock rollback cannot create a second refill interval", () => {
  const rolledBack = consumeTokenBucket(
    { tokens: 0, updated_at_ms: 10_000 },
    1_000,
    4,
    3,
  );
  assert.equal(rolledBack.allowed, false);
  assert.equal(rolledBack.updated_at_ms, 10_000);

  const afterClockRecovers = consumeTokenBucket(
    { tokens: rolledBack.tokens, updated_at_ms: rolledBack.updated_at_ms },
    11_000,
    4,
    3,
  );
  assert.equal(afterClockRecovers.allowed, true);
  assert.equal(afterClockRecovers.tokens, 2);
  assert.equal(afterClockRecovers.updated_at_ms, 11_000);
});

test("client address is HMAC-keyed before it becomes a Durable Object name", async () => {
  const secret = "server-side-high-entropy-ingress-token";
  const name = await clientBucketName("203.0.113.10", secret);
  const otherName = await clientBucketName("203.0.113.11", secret);
  const rotatedName = await clientBucketName("203.0.113.10", "rotated-server-side-high-entropy-key");
  assert.equal(name.length, 64);
  assert.match(name, /^[a-f0-9]{64}$/);
  assert.notEqual(name, "203.0.113.10");
  assert.notEqual(name, otherName);
  assert.notEqual(name, rotatedName);
  await assert.rejects(() => clientBucketName(""));
  await assert.rejects(() => clientBucketName("203.0.113.10", "short"));
});

test("proxy request strips spoofable identity and ingress headers", () => {
  const request = new Request(
    "https://gateway.example.test/api/v1/workflow-runs?limit=10",
    {
      method: "POST",
      headers: {
        Authorization: "Bearer tenant-secret",
        "Content-Type": "application/json",
        "CF-Connecting-IP": "192.0.2.111",
        "Forwarded": "for=192.0.2.111",
        "X-Forwarded-For": "192.0.2.111",
        "X-Real-IP": "192.0.2.111",
        "X-AetherFlow-Ingress-Token": "attacker-value",
      },
      body: JSON.stringify({ workflow_id: "demo" }),
    },
  );

  const upstream = buildUpstreamRequest(
    request,
    parseOrigin("https://api.example.test"),
    "server-side-high-entropy-ingress-token",
  );

  assert.equal(upstream.url, "https://api.example.test/api/v1/workflow-runs?limit=10");
  assert.equal(upstream.headers.get("authorization"), "Bearer tenant-secret");
  assert.equal(
    upstream.headers.get("x-aetherflow-ingress-token"),
    "server-side-high-entropy-ingress-token",
  );
  for (const name of ["cf-connecting-ip", "forwarded", "x-forwarded-for", "x-real-ip"]) {
    assert.equal(upstream.headers.has(name), false);
  }
});

test("gateway rejects oversized declared request bodies before rate limiting", async () => {
  const response = await worker.fetch(
    new Request("https://gateway.example.test/api/v1/workflow-runs", {
      method: "POST",
      headers: {
        "CF-Connecting-IP": "203.0.113.5",
        "Content-Length": String(2 * 1024 * 1024 + 1),
      },
    }),
    {},
  );
  assert.equal(response.status, 413);
  assert.equal((await response.json()).error.code, "payload_too_large");
});

test("gateway fails closed without an origin or ingress secret", async () => {
  const response = await worker.fetch(
    new Request("https://gateway.example.test/api/v1/workflow-runs", {
      headers: { "CF-Connecting-IP": "203.0.113.5" },
    }),
    {},
  );
  assert.equal(response.status, 503);
  assert.equal((await response.json()).error.code, "gateway_not_configured");
});

test("gateway does not call the origin when the shared limiter denies a request", async () => {
  let originCalls = 0;
  const env = {
    AETHERFLOW_ORIGIN: "https://api.example.test",
    AETHERFLOW_INGRESS_TOKEN: "server-side-high-entropy-ingress-token",
    CLIENT_RATE_LIMITER: {
      idFromName(name) {
        assert.match(name, /^[a-f0-9]{64}$/);
        return name;
      },
      get() {
        return {
          async fetch() {
            return Response.json({ allowed: false, retry_after_seconds: 3 });
          },
        };
      },
    },
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    originCalls += 1;
    return new Response("must not be called");
  };

  try {
    const response = await worker.fetch(
      new Request("https://gateway.example.test/api/v1/workflow-runs", {
        headers: { "CF-Connecting-IP": "203.0.113.5" },
      }),
      env,
    );
    assert.equal(response.status, 429);
    assert.equal(response.headers.get("retry-after"), "3");
    assert.equal(originCalls, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("gateway replaces spoofed headers and preserves tenant Bearer credentials", async () => {
  let upstreamRequest;
  const env = {
    AETHERFLOW_ORIGIN: "https://api.example.test",
    AETHERFLOW_INGRESS_TOKEN: "server-side-high-entropy-ingress-token",
    CLIENT_RATE_LIMITER: {
      idFromName(name) { return name; },
      get() {
        return { async fetch() { return Response.json({ allowed: true, retry_after_seconds: 0 }); } };
      },
    },
  };
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (request) => {
    upstreamRequest = request;
    return new Response(JSON.stringify({ accepted: true }), {
      status: 202,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const response = await worker.fetch(
      new Request("https://gateway.example.test/api/v1/workflow-runs", {
        method: "POST",
        headers: {
          "CF-Connecting-IP": "203.0.113.5",
          "X-Forwarded-For": "198.51.100.90",
          "X-AetherFlow-Ingress-Token": "caller-value",
          Authorization: "Bearer tenant-credential",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ workflow_id: "wf-01" }),
      }),
      env,
    );
    assert.equal(response.status, 202);
    assert.equal(upstreamRequest.url, "https://api.example.test/api/v1/workflow-runs");
    assert.equal(upstreamRequest.headers.get("authorization"), "Bearer tenant-credential");
    assert.equal(
      upstreamRequest.headers.get("x-aetherflow-ingress-token"),
      env.AETHERFLOW_INGRESS_TOKEN,
    );
    assert.equal(upstreamRequest.headers.has("x-forwarded-for"), false);
    assert.equal(upstreamRequest.headers.has("cf-connecting-ip"), false);
    assert.equal(response.headers.get("cache-control"), "no-store");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("Durable Object uses SQLite storage and expires idle rate-limit buckets", async () => {
  let now = 1_000;
  const ttl = 24 * 60 * 60 * 1000;
  const state = {
    row: null,
    alarmAt: null,
    deleteAllCalls: 0,
    blockConcurrencyCalls: 0,
    blockConcurrencyWhile: async (callback) => {
      state.blockConcurrencyCalls += 1;
      return callback();
    },
    storage: {
      sql: {
        exec(sql, ...params) {
          if (sql.includes("CREATE TABLE")) return { toArray: () => [] };
          if (sql.includes("SELECT tokens, updated_at_ms")) {
            return { toArray: () => (state.row ? [state.row] : []) };
          }
          if (sql.includes("SELECT updated_at_ms")) {
            return { toArray: () => (state.row ? [{ updated_at_ms: state.row.updated_at_ms }] : []) };
          }
          if (sql.startsWith("INSERT")) {
            state.row = { tokens: params[0], updated_at_ms: params[1] };
            return { toArray: () => [] };
          }
          throw new Error("unexpected SQL in test");
        },
      },
      async setAlarm(timestamp) {
        state.alarmAt = timestamp;
      },
      async deleteAll() {
        state.deleteAllCalls += 1;
        state.row = null;
        state.alarmAt = null;
      },
    },
  };
  const limiter = new ClientRateLimiter(state, () => now);
  const first = await limiter.fetch(new Request("https://limiter.internal/check", {
    method: "POST",
    body: JSON.stringify({ capacity: 1, refill_per_second: 1 }),
  }));
  assert.equal(first.status, 200);
  assert.equal((await first.json()).allowed, true);
  assert.ok(state.row);
  assert.equal(state.alarmAt, now + ttl);

  now += 1_000;
  const second = await limiter.fetch(new Request("https://limiter.internal/check", {
    method: "POST",
    body: JSON.stringify({ capacity: 1, refill_per_second: 1 }),
  }));
  assert.equal((await second.json()).allowed, true);
  assert.equal(state.alarmAt, 1_000 + ttl, "ordinary requests do not reschedule the alarm");

  now = state.alarmAt;
  await limiter.alarm();
  assert.ok(state.row, "a recently active bucket is retained at its first alarm");
  assert.equal(state.alarmAt, state.row.updated_at_ms + ttl);

  now = state.alarmAt;
  await limiter.alarm();
  assert.equal(state.row, null, "an idle bucket deletes all SQLite and alarm state");
  assert.equal(state.alarmAt, null);
  assert.equal(state.deleteAllCalls, 1);
  assert.equal(state.blockConcurrencyCalls, 2, "both alarm paths serialize against incoming requests");
});
