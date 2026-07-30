# ADR 0001: Use Meshquill as the independent project name

- Status: accepted
- Date: 2026-07-30

## Decision

Use `Meshquill` for the project and `meshquill` for the executable/root Rust crate. Rust component
crates use `meshquill-*`. The Python distribution is `meshquill-sdk` and its import is
`meshcore_sdk`, matching the requested user-facing API without claiming the official `meshcore`
distribution namespace.

## Evidence and rationale

Checks repeated on 2026-07-30 found the intended GitHub repository URL and `meshquill-sdk` PyPI URL
unclaimed (HTTP 404); `cargo search meshquill` returned no matching base or component crates.
`meshcore`, `meshcore-cli` and the official Python `meshcore` namespace already exist. The
independent project/distribution names avoid suggesting official ownership. Registry availability
is rechecked immediately before publication because a search result is not a reservation.
