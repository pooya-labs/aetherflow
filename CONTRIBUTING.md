# Contributing to AetherFlow

Thanks for considering a contribution. The project is in its architecture/foundation phase, so design clarity and correctness matter more than feature volume.

## Before opening a change
- Read `README.md`, `ROADMAP.md`, and relevant architecture decision records.
- For a behavior or dependency that changes system boundaries, propose an ADR first.
- Keep changes focused and explain the failure modes they address.
- Never claim a test, benchmark, or security check passed unless it was run.

## Engineering expectations
- Prefer small modules with explicit ownership and error handling.
- Keep domain logic independent from I/O adapters.
- Add tests for success, invalid transitions, retries, cancellation, and failure paths as applicable.
- Document delivery guarantees and idempotency boundaries precisely.
- Do not add secrets, credentials, real user data, or generated build artifacts.

## Local checks
Once the Rust workspace exists, run the commands documented in the repository CI and development guide. Until then, do not assume build commands are available.

## Pull requests
Describe the motivation, design, alternatives, tests actually run, operational impact, and any follow-up work. Update `ROADMAP.md` when a milestone or checkpoint changes.
