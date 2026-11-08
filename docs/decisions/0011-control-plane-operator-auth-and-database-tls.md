# ADR-0011: Control-plane operator authentication and PostgreSQL TLS

- **Status:** Accepted; HTTP listener TLS boundary added
- **Date:** 2026-11-03
- **Scope:** HTTP control-plane credentials, HTTP listener TLS, and PostgreSQL client transport

## Context

M07 has a working PostgreSQL-backed HTTP surface, but a loopback-only listener is not an authentication mechanism. A local process, browser extension, or another service on the same host may still issue requests. The server also previously used `NoTls`, which sent database credentials and workflow data without transport encryption when connecting over TCP.

The existing storage model has global workflow/task IDs and no tenant key on durable rows. Adding a token and calling it tenant isolation would be incorrect: the current storage adapter cannot scope every read and write to an authenticated tenant.

## Decisions

### One high-entropy operator credential

1. When `DATABASE_URL` is configured, startup requires `AETHERFLOW_API_TOKEN`. Startup fails closed if it is missing or does not satisfy the token format.
2. The token must be 32–256 ASCII URL-safe characters: letters, digits, hyphen or underscore. Generate it from a cryptographically secure source (for example, 32 random bytes encoded as hexadecimal); never commit it or place it in the repository.
3. The server stores only a SHA-256 digest in process memory and compares supplied digests using constant-time equality. The raw token is not written to logs or persisted.
4. Every route below `/api/v1` requires the exact `Authorization: Bearer <token>` scheme. `GET /health` remains public and returns process liveness only.
5. The credential grants the same operator-level access to all v1 control-plane routes. It is not a user identity, role system, per-tenant credential, or delegated authorization token. Rotation requires replacing the environment value and restarting the process.

### Edge-to-origin gateway authentication

1. For any non-loopback API bind, startup requires `AETHERFLOW_INGRESS_TOKEN` and validates its 32–256 character URL-safe format before connecting to PostgreSQL or running migrations.
2. The server stores only the token's SHA-256 digest in process memory. Every request except public `GET /health` must include `X-AetherFlow-Ingress-Token`; supplied values are hashed and compared in constant time before any API route runs.
3. The Cloudflare Worker removes a caller-supplied ingress header, then injects the value from a Worker Secret. The Worker also removes untrusted forwarded/identity headers and preserves the operator/tenant Bearer token unchanged.
4. The Worker uses HMAC-SHA-256 with the same high-entropy ingress Secret to derive per-client Durable Object names. Rotating the Secret intentionally resets the per-client token-bucket identities and must be coordinated across the Worker and API.
5. This is an application-layer origin guard, not a substitute for network isolation. Production deployments must still restrict the origin to Cloudflare egress or a private Tunnel and prove that all alternate replicas and service ports are unreachable from untrusted networks.
6. The default loopback development bind does not require this extra token. A non-loopback bind without the token fails before database migration or listener startup.

The Worker source is in `cloudflare-gateway/`. It is intentionally configured with `workersDev: false` and preview URLs disabled. No production route is attached until a verified HTTPS origin and active Zone or Tunnel exist.

### PostgreSQL TLS

1. The server uses `tokio-postgres-rustls` with Rustls and the platform's native trusted CA store. No certificate-verification bypass or accept-all verifier is installed.
2. `DATABASE_SSL_MODE` defaults to `require`. The Rustls client validates the server certificate chain against the native roots and validates the server name during the TLS handshake.
3. `prefer` is an explicit compatibility mode and may fall back to plaintext if the server refuses TLS. `disable` sends plaintext and is reserved for a local disposable development database only.
4. Private database CAs must be installed in the operating system's native trust store before starting the service. The application does not silently trust a certificate supplied by an arbitrary endpoint.
5. This TLS setting protects the server-to-PostgreSQL connection. It does not enable HTTPS on the HTTP listener.

### HTTP listener TLS boundary

1. The listener defaults to `127.0.0.1:8080`; plaintext HTTP is permitted only on loopback.
2. A non-loopback bind requires both `AETHERFLOW_TLS_CERT_PATH` and `AETHERFLOW_TLS_KEY_PATH`. The server parses the PEM certificate chain and private key before database migrations or binding and fails closed for a partial or invalid pair.
3. A loopback bind may use plain HTTP for local development or HTTPS when both TLS paths are supplied.
4. Rustls listener tests verify a successful handshake, malformed PEM rejection, untrusted-chain rejection, hostname-mismatch rejection, and rejection of raw HTTP on the TLS socket. These passed Rust CI on source `54b17742aee822f37a4a3b034968bbcd3e851c13`.
5. Operators remain responsible for certificate issuance, renewal, private-key protection, and matching certificate SANs to the address clients use.

Tenant ownership, tenant-authenticated routes, forced RLS on tenant tables, and worker-to-tenant grants are implemented under ADR-0012. Direct non-loopback HTTP exposure is blocked unless HTTPS is configured. Live PostgreSQL Rustls trust/hostname verification passed hosted PostgreSQL/NATS CI run [41609705469](https://github.com/pooya16pg-spec/aetherflow/actions/runs/41609705469).

## Consequences

- Local API clients must supply a Bearer token whenever PostgreSQL-backed routes are enabled.
- Non-loopback API deployments also require a private edge-to-origin ingress token; direct requests without it are rejected before operator/tenant route handling. The matching Cloudflare Worker is source-complete but not deployed.
- Health checks remain unauthenticated and must not be interpreted as database health.
- A shared operator token is intentionally simple and is appropriate only for one trusted operator/service boundary.
- The PostgreSQL client now uses encrypted transport by default and fails closed on untrusted certificates or a hostname mismatch.
- `DATABASE_SSL_MODE=disable` is a deliberate local-development escape hatch; it must not be used for remote databases.
- Rate limiting, per-principal audit identity, token rotation without restart, stronger operational controls, and the M08 threat-model/SLO work remain future work.

## Revisit when

Before production deployment, complete live PostgreSQL certificate-chain and hostname verification, publish the threat model and operational guidance, and add the remaining M08 observability, backup/restore, load, and supply-chain controls.
