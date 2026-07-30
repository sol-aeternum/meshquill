# ADR 0005: Run configured Python hooks in an opt-in subprocess

- Status: accepted
- Date: 2026-07-30

## Decision

Invoke hooks only when configured, through a versioned JSON subprocess protocol. Apply strict
deadlines and output limits. `before_send` may return unchanged, replace bounded text/destination, or
reject; observation hooks cannot mutate core state. A crashed or timed-out hook produces a diagnostic
event and follows its configured fail-open/fail-closed policy.

## Consequences

Ordinary CLI startup has no Python dependency and hook crashes do not corrupt the Rust event loop.
Each validation or dispatch starts a fresh isolated-mode Python process; the configured semaphore
bounds concurrent starts. This costs process startup time but prevents module state from leaking
between events. Hooks are trusted local code and are not sandboxed; subprocess isolation is a
reliability boundary only.
