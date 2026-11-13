"""Unit tests for the Cloudflare Free live acceptance smoke helpers."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).with_name("smoke-cloudflare-free.py")
SPEC = importlib.util.spec_from_file_location("smoke_cloudflare_free", MODULE_PATH)
assert SPEC and SPEC.loader
SMOKE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SMOKE)


class FakeResponse:
    status = 413
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store",
    }

    def read(self) -> bytes:
        return json.dumps({
            "error": {
                "code": "request_too_large",
                "request_id": "cf-test-id",
            }
        }).encode("utf-8")


class FakeConnection:
    def __init__(self, host: str, port: int, timeout: int):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.request_line = None
        self.headers = []
        self.sent = []
        self.ended = False
        self.closed = False

    def putrequest(self, method: str, target: str, **kwargs) -> None:
        self.request_line = (method, target, kwargs)

    def putheader(self, name: str, value: str) -> None:
        self.headers.append((name.lower(), value))

    def endheaders(self) -> None:
        self.ended = True

    def send(self, data: bytes) -> None:
        self.sent.append(data)

    def getresponse(self) -> FakeResponse:
        return FakeResponse()

    def close(self) -> None:
        self.closed = True


class CloudflareSmokeHelpersTest(unittest.TestCase):
    def test_oversized_body_is_valid_json_exactly_one_byte_over_limit(self) -> None:
        body = SMOKE.build_oversized_body("ci-body-limit-unit-test")
        self.assertEqual(len(body), SMOKE.MAX_BODY_BYTES + 1)
        parsed = json.loads(body)
        self.assertEqual(parsed["workflow_run_id"], "ci-body-limit-unit-test")
        self.assertEqual(parsed["tasks"], [])

    def test_chunked_post_omits_content_length_and_writes_terminating_chunk(self) -> None:
        original_api_base = SMOKE.API_BASE
        connection = None

        def factory(host: str, port: int, timeout: int) -> FakeConnection:
            nonlocal connection
            connection = FakeConnection(host, port, timeout)
            return connection

        try:
            SMOKE.API_BASE = "https://smoke.example/prefix/"
            status, payload, headers = SMOKE.send_chunked_request(
                "/api/v1/workflow-runs",
                [b"hello", b"world!"],
                "session-token",
                connection_factory=factory,
            )
        finally:
            SMOKE.API_BASE = original_api_base

        self.assertEqual(status, 413)
        self.assertEqual(payload["error"]["code"], "request_too_large")
        self.assertEqual(headers.get("Cache-Control"), "no-store")
        assert connection is not None
        self.assertEqual(connection.host, "smoke.example")
        self.assertEqual(connection.port, 443)
        self.assertEqual(connection.request_line[0:2], ("POST", "/prefix/api/v1/workflow-runs"))
        sent_headers = dict(connection.headers)
        self.assertEqual(sent_headers["transfer-encoding"], "chunked")
        self.assertEqual(sent_headers["authorization"], "Bearer session-token")
        self.assertNotIn("content-length", sent_headers)
        self.assertEqual(
            b"".join(connection.sent),
            b"5\r\\nhello\r\\n6\r\\nworld!\r\\n0\r\\n\r\\n",
        )
        self.assertTrue(connection.ended)
        self.assertTrue(connection.closed)

    def test_chunked_post_rejects_non_https_base(self) -> None:
        original_api_base = SMOKE.API_BASE
        try:
            SMOKE.API_BASE = "http://smoke.example"
            with self.assertRaises(RuntimeError):
                SMOKE.send_chunked_request("/test", [b"x"], "token")
        finally:
            SMOKE.API_BASE = original_api_base


if __name__ == "__main__":
    unittest.main()
