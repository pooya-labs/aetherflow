import assert from "node:assert/strict";
import test from "node:test";

import worker from "./worker.mjs";

test("Worker route returns a no-store 413 envelope for an oversized streamed request without Content-Length", async () => {
  const maxBytes = 16 * 1024;
  let cancelled = false;
  let databaseCalls = 0;

  const body = new ReadableStream(
    {
      start(controller) {
        controller.enqueue(new Uint8Array(maxBytes + 1).fill(0x61));
      },
      cancel() {
        cancelled = true;
      },
    },
    { highWaterMark: 0 },
  );

  const request = new Request("https://aetherflow.test/api/v1/workflow-runs", {
    method: "POST",
    headers: {
      "Authorization": "Bearer " + "a".repeat(32),
      "Content-Type": "application/json",
      "Origin": "https://pooya-labs.github.io",
    },
    body,
    duplex: "half",
  });

  assert.equal(request.headers.has("content-length"), false);

  const db = {
    prepare(sql) {
      databaseCalls += 1;
      return {
        bind() {
          return {
            async first() {
              if (sql.startsWith("UPDATE demo_sessions")) {
                return { workspace_id: "workspace-route-test" };
              }
              if (sql.startsWith("INSERT INTO demo_daily_usage")) {
                return { count: 1 };
              }
              throw new Error("Unexpected D1 query during rejected request: " + sql);
            },
          };
        },
      };
    },
  };

  const response = await worker.fetch(request, {
    DB: db,
    ALLOWED_ORIGIN: "https://pooya-labs.github.io",
  });

  assert.equal(response.status, 413);
  assert.equal(response.headers.get("cache-control"), "no-store");
  assert.equal(response.headers.get("x-content-type-options"), "nosniff");
  assert.equal(
    response.headers.get("access-control-allow-origin"),
    "https://pooya-labs.github.io",
  );

  const payload = await response.json();
  assert.equal(payload.error.code, "request_too_large");
  assert.match(payload.error.request_id, /^cf-/);
  assert.equal(cancelled, true);
  assert.equal(databaseCalls, 2);
});
