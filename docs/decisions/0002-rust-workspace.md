# ADR 0002: Rust core with boundary-specific workspace crates

- Status: accepted
- Date: 2026-07-30

## Decision

Use the crate boundaries described in `docs/architecture.md`. The core depends only on protocol and
async/domain libraries. OS transports depend inward on core; the application and Python binding
compose boundary crates. The store imports hook/MQTT configuration types deliberately so one strict
configuration schema can validate integration settings without starting either runtime.

## Consequences

The native CLI starts without Python. Protocol behavior cannot drift between CLI and SDK. Feature
target-specific code does not replace domain logic. More crates add workspace overhead, accepted in
exchange for explicit ownership and focused tests.
