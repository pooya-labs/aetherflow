# Security policy

AetherFlow is an experimental engineering project. It is not currently declared production-ready, and no independent production security audit has been completed.

## Reporting a vulnerability

Please do not publish exploit details, credentials, customer data, or other sensitive material in a public issue. Use GitHub's private vulnerability reporting for this repository when it is enabled. If that channel is unavailable, contact the repository owner privately and include the affected commit, impact, and a minimal safe reproduction. Do not include live secrets or personal data in the report.

## Implemented controls

The current codebase includes:

- A required high-entropy platform/operator Bearer token when PostgreSQL-backed API routes are enabled. Only its SHA-256 digest is retained in process memory; supplied token digests are compared in constant time.
- Separate tenant credentials and roles for tenant routes. Tenant identity is derived from the stored credential, not from request-supplied tenant IDs.
- PostgreSQL tenant ownership, forced row-level security on ten tenant tables, transaction-local tenant/credential context, and a restricted runtime DB role separate from the migration owner.
- PostgreSQL TLS verification by default and an HTTP listener that permits plaintext only on loopback. Non-loopback binds require a valid Rustls certificate/key pair.
- Redacted request telemetry, aggregate bounded-cardinality metrics, and opt-in OTLP export.
- A default-deny WebAssembly Component Model boundary with no WASI imports in ABI v1, explicit bounded logging capability, fresh Store/instance per invocation, and configured resource/fuel/deadline limits.

These are implemented controls, not a certification or a guarantee against a compromised host, arbitrary SQL execution, malicious dependencies, or engine vulnerabilities. The threat model documents the trust boundaries and remaining risks.

## Important limitations

- The platform/operator credential is a shared operator boundary, not per-human identity, and rotation requires process restart.
- Rate limiting, stronger per-principal audit identity, managed-secret integration, and a production key/certificate rotation process are not yet complete.
- Transaction-local PostgreSQL context and RLS are defense in depth. They do not make arbitrary SQL execution or SQL injection safe.
- Compose is a loopback-only development stack; its sample credentials and plaintext local PostgreSQL mode are not production settings.
- JetStream/PostgreSQL recovery is rehearsed in isolated CI fixtures. This is not a production point-in-time-recovery service or a proven coordinated production RPO/RTO.
- WASM fuel and epoch deadlines actively bound guest execution, but a blocking host thread is not force-aborted. OS-level sandboxing remains a separate deployment concern.
- No independent penetration test, cryptographic review, or production threat assessment has been completed.

Read [the threat model](docs/security/threat-model.md), [the operational guide](docs/operations/production-guide.md), and [the recovery runbooks](docs/operations/backup-restore.md) before deploying or exposing the control plane. Report suspected vulnerabilities privately.
