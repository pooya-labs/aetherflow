import assert from "node:assert/strict";
import test from "node:test";

import {
  readBoundedRequestText,
  RequestBodyError,
} from "./request-body.mjs";

const encoder = new TextEncoder();

function makeRequest(chunks, headers = {}) {
  let index = 0;
  let cancelled = false;
  let pulls = 0;
  const stream = new ReadableStream(
    {
      pull(controller) {
        pulls += 1;
        if (index < chunks.length) {
          controller.enqueue(encoder.encode(chunks[index]));
          index += 1;
          return;
        }
        controller.close();
      },
      cancel() {
        cancelled = true;
      },
    },
    { highWaterMark: 0 },
  );
  const request = new Request("https://example.test/api/v1/workflow-runs", {
    method: "POST",
    headers,
    body: stream,
    duplex: "half",
  });
  return {
    request,
    wasCancelled: () => cancelled,
    pullCount: () => pulls,
  };
}

test("accepts a valid JSON body split across chunks at the exact byte limit", async () => {
  const body = '{"a":1}';
  const request = makeRequest(['{"a', '":', '1}']);

  assert.equal(request.request.headers.has("content-length"), false);
  assert.equal(
    await readBoundedRequestText(request.request, encoder.encode(body).byteLength),
    body,
  );
});

test("rejects a chunked body over the limit without Content-Length and cancels the stream", async () => {
  const request = makeRequest(["123", "456", "789"]);

  await assert.rejects(
    readBoundedRequestText(request.request, 5),
    (error) =>
      error instanceof RequestBodyError &&
      error.status === 413 &&
      error.code === "request_too_large",
  );
  assert.equal(request.wasCancelled(), true);
});

test("rejects a declared oversized body before reading the stream", async () => {
  const request = makeRequest(["123456"], { "Content-Length": "6" });

  await assert.rejects(
    readBoundedRequestText(request.request, 5),
    (error) =>
      error instanceof RequestBodyError &&
      error.status === 413 &&
      error.code === "request_too_large",
  );
  assert.equal(request.pullCount(), 0);
});

test("rejects malformed Content-Length without reading the stream", async () => {
  const request = makeRequest(["{}"], { "Content-Length": "5, 5" });

  await assert.rejects(
    readBoundedRequestText(request.request, 10),
    (error) =>
      error instanceof RequestBodyError &&
      error.status === 400 &&
      error.code === "invalid_content_length",
  );
  assert.equal(request.pullCount(), 0);
});

test("rejects invalid UTF-8 instead of silently replacing bytes", async () => {
  const stream = new ReadableStream(
    {
      start(controller) {
        controller.enqueue(new Uint8Array([0xc3, 0x28]));
        controller.close();
      },
    },
    { highWaterMark: 0 },
  );
  const request = new Request("https://example.test/api", {
    method: "POST",
    body: stream,
    duplex: "half",
  });

  await assert.rejects(
    readBoundedRequestText(request, 10),
    (error) =>
      error instanceof RequestBodyError &&
      error.status === 400 &&
      error.code === "invalid_json",
  );
});
