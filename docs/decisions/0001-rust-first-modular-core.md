# ADR-0001: Rust-first modular execution core

- **Status:** Accepted as initial direction
- **Date:** 2026-11-02

## Context
AetherFlow is intended to demonstrate systems engineering: asynchronous execution, explicit state transitions, fault recovery, bounded resource use, and clear security boundaries. The architecture should support these goals without forcing premature infrastructure choices.

## Decision
Use Rust as the primary implementation language and Tokio for asynchronous runtime needs. Start with a modular, layered core. Keep domain types and transition rules independent from infrastructure adapters. Introduce PostgreSQL after the in-memory execution model and invariants are tested.

Axum, Tonic, NATS JetStream, and Wasmtime remain planned choices and should be introduced only when a concrete milestone requires them.

## Consequences
**Benefits:** strong type system; mature async ecosystem; deterministic tests without databases or brokers.

**Trade-offs:** steeper learning curve; async cancellation and shared-state design require care; modularity can become over-engineering if crate boundaries are created too early.

## Rejected alternatives
- **Microservices from day one:** rejected because operational complexity would multiply before domain semantics stabilize.
- **Broker-first architecture:** rejected because a broker does not replace durable state or solve execution correctness.
- **One large service module:** rejected because domain rules and infrastructure details would become tightly coupled as the system grows.

## Revisit when
Revisit if implementation shows that these boundaries impede correctness, testability, or operational simplicity.
