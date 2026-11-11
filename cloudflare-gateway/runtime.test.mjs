import { env } from "cloudflare:workers";
import {
  evictDurableObject,
  runDurableObjectAlarm,
  runInDurableObject,
} from "cloudflare:test";
import { describe, expect, it } from "vitest";

const IDLE_BUCKET_TTL_MS = 24 * 60 * 60 * 1000;

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

  it("reschedules active cleanup alarms and removes expired SQLite state", async () => {
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
    // SQLite data and alarm metadata. The next request starts a fresh bucket.
    clock.now = refreshedExpiry + 1;
    expect(await runDurableObjectAlarm(stub)).toBe(true);

    await setClock(stub, clock);
    expect(await readBucket(stub)).toBeNull();
    expect(await readAlarm(stub)).toBeNull();
    expect(await consume(stub, 1, 1)).toMatchObject({ allowed: true });
  });
});
