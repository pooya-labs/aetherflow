export class RequestBodyError extends Error {
  constructor(status, code, message) {
    super(message);
    this.name = "RequestBodyError";
    this.status = status;
    this.code = code;
  }
}

export async function readBoundedRequestText(request, maxBytes) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
    throw new TypeError("maxBytes must be a non-negative safe integer.");
  }

  const declared = request.headers.get("Content-Length");
  if (declared !== null) {
    if (!/^\d+$/.test(declared)) {
      throw new RequestBodyError(
        400,
        "invalid_content_length",
        "The request content length is invalid.",
      );
    }

    const declaredBytes = Number(declared);
    if (!Number.isSafeInteger(declaredBytes) || declaredBytes > maxBytes) {
      throw new RequestBodyError(
        413,
        "request_too_large",
        "The request body is too large.",
      );
    }
  }

  if (!request.body) return "";

  const reader = request.body.getReader();
  const bytes = new Uint8Array(maxBytes);
  let totalBytes = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
      if (chunk.byteLength > maxBytes - totalBytes) {
        try {
          await reader.cancel();
        } catch {
          // The request may already have been cancelled by the client.
        }
        throw new RequestBodyError(
          413,
          "request_too_large",
          "The request body is too large.",
        );
      }

      bytes.set(chunk, totalBytes);
      totalBytes += chunk.byteLength;
    }
  } catch (error) {
    if (error instanceof RequestBodyError) throw error;
    throw new RequestBodyError(
      400,
      "invalid_request_body",
      "The request body could not be read.",
    );
  } finally {
    try {
      reader.releaseLock();
    } catch {
      // A cancelled or errored stream may already have released its reader.
    }
  }

  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, totalBytes));
  } catch {
    throw new RequestBodyError(
      400,
      "invalid_json",
      "The request body must be valid UTF-8 JSON.",
    );
  }
}
