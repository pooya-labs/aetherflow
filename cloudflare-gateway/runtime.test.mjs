import { env, exports } from "cloudflare:workers";
import {
  evictDurableObject,
  runDurableObjectAlarm,
  runInDurableObject,
} from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";

const IDLE_BUCKET_TTL_MS = 24 * 60 * 60 * 1000;

afterEach(() => {
  vi.unstubAllGlobals();
});

function getLimiter(name) {
  const id = env.CLIENT_RATE_LIMITER.idFromName(name);
  return env.CLIENT_RATE_LIMITER.get(id);
}

async function setClock(stub, clock) {
  await runInDurableObject(stub, (instance) => {
    instance.logic.now = () => clock.now;
  });
}

async function consume(stub, capacity = 2, refillPerSecond = 1) {
  const response = await stub.fetch("https://rate-limiter.internal/check", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      capacity,
      refill_per_second: refillPerSecond,
    }),
  });

  expect(response.status).toBe(200);
  return response.json();
}

async function readBucket(stub) {
  return runInDurableObject(stub, (_instance, state) => {
    const table = state.storage.sql
      .exec(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'client_rate_limit'",
      )
      .toArray()[0];
    if (!table) return null;

    return state.storage.sql
      .exec("SELECT tokens, updated_at_ms FROM client_rate_limit WHERE id = 1")
      .toArray()[0] ?? null;
  });
}

async function readAlarm(stub) {
  return runInDurableObject(stub, async (_instance, state) => {
    return state.storage.getAlarm();
  });
}

describe("Cloudflare Workers Durable Object runtime", () => {
  it("uses real SQLite storage and preserves a rate bucket across eviction", async () => {
    const stub = getLimiter("runtime-persistence");
    const clock = { now: 1_800_000_000_000 };
    await setClock(stub, clock);

    expect(await consume(stub, 2, 1)).toMatchObject({
      allowed: true,
      retry_after_seconds: 0,
    });
    expect(await consume(stub, 2, 1)).toMatchObject({
      allowed: true,
      retry_after_seconds: 0,
    });
    expect(await consume(stub, 2, 1)).toMatchObject({
      allowed: false,
      retry_after_seconds: 1,
    });

    const beforeEviction = await readBucket(stub);
    expect(beforeEviction.tokens).toBe(0);
    expect(beforeEviction.updated_at_ms).toBe(clock.now);

    await evictDurableObject(stub);
    await setClock(stub, clock);

    const afterEviction = await readBucket(stub);
    expect(afterEviction).toEqual(beforeEviction);
    expect(await consume(stub, 2, 1)).toMatchObject({
      allowed: false,
      retry_after_seconds: 1,
    });
  });

  it("keeps different client buckets isolated and serializes concurrent consumption", async () => {
    const clock = { now: 1_810_000_000_000 };
    const clientA = getLimiter("runtime-client-a");
    const clientB = getLimiter("runtime-client-b");
    await Promise.all([setClock(clientA, clock), setClock(clientB, clock)]);

    expect(await consume(clientA, 1, 1)).toMatchObject({ allowed: true });
    expect(await consume(clientA, 1, 1)).toMatchObject({ allowed: false });
    expect(await consume(clientB, 1, 1)).toMatchObject({ allowed: true });

    const concurrent = getLimiter("runtime-concurrent-client");
    await setClock(concurrent, clock);
    const decisions = await Promise.all(
      Array.from({ length: 12 }, () => consume(concurrent, 4, 1)),
    );

    expect(decisions.filter((decision) => decision.allowed)).toHaveLength(4);
    expect(decisions.filter((decision) => !decision.allowed)).toHaveLength(8);
    expect((await readBucket(concurrent)).tokens).toBe(0);
  });

  it("reschedules active cleanup alarms, deletes expired state, and recreates its schema", async () => {
    const stub = getLimiter("runtime-idle-cleanup");
    const clock = { now: 1_820_000_000_000 };
    await setClock(stub, clock);

    expect(await consume(stub, 1, 1)).toMatchObject({ allowed: true });
    expect(await readAlarm(stub)).toBe(clock.now + IDLE_BUCKET_TTL_MS);

    // A new request halfway through the idle window refreshes last-seen time.
    clock.now += IDLE_BUCKET_TTL_MS / 2;
    expect(await consume(stub, 1, 1)).toMatchObject({ allowed: true });

    // Run the originally scheduled alarm early. It must re-check SQLite and
    // move the alarm to 24 hours after the latest request.
    expect(await runDurableObjectAlarm(stub)).toBe(true);
    const refreshedExpiry = clock.now + IDLE_BUCKET_TTL_MS;
    expect(await readAlarm(stub)).toBe(refreshedExpiry);

    // Once the refreshed deadline has passed, the alarm must delete all
    // SQLite data and alarm metadata.
    clock.now = refreshedExpiry + 1;
    expect(await runDurableObjectAlarm(stub)).toBe(true);

    // The object remains alive after the alarm. The schema must be recreated
    // on the next request even though its constructor does not run again.
    expect(await readBucket(stub)).toBeNull();
    expect(await readAlarm(stub)).toBeNull();
    expect(await consume(stub, 1, 1)).toMatchObject({ allowed: true });

    // The first post-cleanup request gets a completely fresh bucket and alarm.
    expect(await readBucket(stub)).toEqual({
      tokens: 0,
      updated_at_ms: clock.now,
    });
    expect(await readAlarm(stub)).toBe(clock.now + IDLE_BUCKET_TTL_MS);
  });
});


describe("Cloudflare Gateway request routing in the Workers runtime", () => {
  it("enforces the real Durable Object bucket before forwarding sanitized requests to the fixed origin", async () => {
    const outboundRequests = [];
    const upstreamFetch = vi.fn(async (input) => {
      const request = input instanceof Request ? input : new Request(input);
      outboundRequests.push(request);
      return Response.json(
        { forwarded: true },
        { headers: { "Cache-Control": "public, max-age=3600" } },
      );
    });
    vi.stubGlobal("fetch", upstreamFetch);

    const clientIp = "198.51.100.247";
    const headers = {
      "CF-Connecting-IP": clientIp,
      "True-Client-IP": "203.0.113.10",
      "Forwarded": "for=203.0.113.11",
      "X-Forwarded-For": "203.0.113.12",
      "X-Real-IP": "203.0.113.13",
      "X-AetherFlow-Ingress-Token": "spoofed-client-token",
      "Authorization": "Bearer tenant-test-credential",
    };

    const responses = [];
    for (let index = 0; index < 5; index += 1) {
      responses.push(
        await exports.default.fetch(
          new Request("https://gateway-runtime.test/api/v1/workflow-runs", {
            method: "GET",
            headers,
          }),
        ),
      );
    }

    expect(responses.slice(0, 4).map((response) => response.status)).toEqual([
      200,
      200,
      200,
      200,
    ]);
    expect(responses[4].status).toBe(429);
    expect(responses[4].headers.get("Retry-After")).toBe("1");
    expect(upstreamFetch).toHaveBeenCalledTimes(4);
    expect(outboundRequests).toHaveLength(4);

    const upstream = outboundRequests[0];
    expect(upstream.url).toBe("https://origin-not-configured.invalid/api/v1/workflow-runs");
    expect(upstream.headers.get("Authorization")).toBe("Bearer tenant-test-credential");
    expect(upstream.headers.get("x-aetherflow-ingress-token")).toBe(
      env.AETHERFLOW_INGRESS_TOKEN,
    );

    for (const name of [
      "cf-connecting-ip",
      "true-client-ip",
      "forwarded",
      "x-forwarded-for",
      "x-real-ip",
    ]) {
      expect(upstream.headers.has(name)).toBe(false);
    }
    expect(responses[0].headers.get("Cache-Control")).toBe("no-store");
    expect(await responses[0].json()).toEqual({ forwarded: true });
  });
});


describe("Cloudflare Gateway origin failure handling in the Workers runtime", () => {
  it("rejects origin redirects without leaking the Location header", async () => {
    const upstreamFetch = vi.fn(async () =>
      new Response(null, {
        status: 302,
        headers: {
          Location: "https://origin-not-configured.invalid/login",
        },
      }),
    );
    vi.stubGlobal("fetch", upstreamFetch);

    const response = await exports.default.fetch(
      new Request("https://gateway-runtime.test/api/v1/workflow-runs", {
        headers: {
          "CF-Connecting-IP": "192.0.2.42",
          Authorization: "Bearer tenant-test-credential",
        },
      }),
    );

    expect(response.status).toBe(502);
    const payload = await response.text();
    expect(payload).toContain("origin_redirect_rejected");
    expect(payload).not.toContain("origin-not-configured.invalid");
    expect(response.headers.has("Location")).toBe(false);
    expect(response.headers.get("Cache-Control")).toBe("no-store");
    expect(upstreamFetch).toHaveBeenCalledTimes(1);
  });

  it("returns a generic no-store error when the upstream transport fails", async () => {
    const upstreamFetch = vi.fn(async () => {
      throw new Error("internal-origin.example: connection refused");
    });
    vi.stubGlobal("fetch", upstreamFetch);

    const response = await exports.default.fetch(
      new Request("https://gateway-runtime.test/api/v1/workflow-runs", {
        headers: {
          "CF-Connecting-IP": "192.0.2.43",
          Authorization: "Bearer tenant-test-credential",
        },
      }),
    );

    expect(response.status).toBe(502);
    const payload = await response.text();
    expect(payload).toContain("origin_unavailable");
    expect(payload).not.toContain("internal-origin.example");
    expect(payload).not.toContain("connection refused");
    expect(response.headers.get("Cache-Control")).toBe("no-store");
    expect(upstreamFetch).toHaveBeenCalledTimes(1);
  });
});
