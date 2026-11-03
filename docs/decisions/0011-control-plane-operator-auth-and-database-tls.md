# ADR-0011: Control-plane operator authentication and PostgreSQL TLS

- **Status:** Accepted for the single-operator development boundary
- **Date:** 2026-11-03
- **Scope:** HTTP control-plane credentials and the server's PostgreSQL client transport

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

### PostgreSQL TLS

1. The server uses `tokio-postgres-rustls` with Rustls and the platform's native trusted CA store. No certificate-verification bypass or accept-all verifier is installed.
2. `DATABASE_SSL_MODE` defaults to `require`. The Rustls client validates the server certificate chain against the native roots and validates the server name during the TLS handshake.
3. `prefer` is an explicit compatibility mode and may fall back to plaintext if the server refuses TLS. `disable` sends plaintext and is reserved for a local disposable development database only.
4. Private database CAs must be installed in the operating system's native trust store before starting the service. The application does not silently trust a certificate supplied by an arbitrary endpoint.
5. This TLS setting protects the server-to-PostgreSQL connection. It does not enable HTTPS on the HTTP listener.

### Exposure and authorization boundary

The listener continues to default to loopback and rejects non-loopback binds. The operator token is a useful local control-plane gate, but **tenant isolation and multi-principal authorization remain unimplemented**. Durable records are still global to this server/database. Do not expose the listener to an untrusted network until the schema, repositories, API reads/writes, and authorization policy all enforce tenant ownership, and HTTP transport is protected by HTTPS or an equivalent trusted TLS-terminating boundary.

## Consequences

- Local API clients must supply a Bearer token whenever PostgreSQL-backed routes are enabled.
- Health checks remain unauthenticated and must not be interpreted as database health.
- A shared operator token is intentionally simple and is appropriate only for one trusted operator/service boundary.
- The PostgreSQL client now uses encrypted transport by default and fails closed on untrusted certificates or a hostname mismatch.
- `DATABASE_SSL_MODE=disable` is a deliberate local-development escape hatch; it must not be used for remote databases.
- Rate limiting, per-principal audit identity, token rotation without restart, RBAC, tenant-scoped storage, and HTTP HTTPS deployment remain future work.

## Revisit when

Before opening non-loopback access, design tenant identity and ownership in PostgreSQL, add tenant predicates to every durable query and mutation, add per-principal authorization and audit attribution, and document an HTTPS deployment boundary.
