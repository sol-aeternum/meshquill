# Production plan

Last updated: 2026-07-30

This plan distinguishes locally completed release work from operations that require GitHub
credentials or physical hardware. Evidence is recorded in [STATUS](STATUS.md).

## Phase 0 — research and decisions

- [x] Inspect environment, authentication, and locally visible hardware.
- [x] Pin current upstream CLI, Python library, firmware, documentation, releases, and relevant
      issues/PRs/discussions.
- [x] Check the public project name on GitHub, crates.io, and PyPI.
- [x] Record capability accounting, protocol coverage, architecture, decisions, threat model, and
      upstream-watch findings.

## Phase 1 — deterministic vertical slice

- [x] Strict protocol/frame codecs with golden, truncation, malformed, oversized, property, and
      fuzz coverage.
- [x] Logical-packet transport contract and bounded deterministic virtual companion.
- [x] Managed client with serialized requests, bounded streams, cancellation, ACK retention, and
      explicit reconnect without mutation replay.
- [x] Demo CLI for discovery, info, contacts, send/ACK, inbox/watch, and human/JSON/JSONL.

## Phase 2 — native workflows

- [x] Versioned atomic configuration, platform paths, profiles, wizard, effective config,
      environment overrides, migration, repair, and legacy selection import.
- [x] TCP, serial, and BLE discovery/connect/reconnect implementations with bounded diagnostics.
- [x] Device, contact, channel, scope, messaging, ACK, history, remote, sensor, network, and batch
      operations with explicit destructive-action policy.
- [x] Portable line chat with visible target, incoming indication, ACK state, nonfatal timeout, and
      retained unsent draft. A full-screen TUI was deliberately deferred because it is not required
      for a usable keyboard-only RC and would add terminal-state risk without hardware evidence.
- [x] Doctor checks, completions, 77 man pages, examples, command suggestions, output contracts,
      redaction, and stable exit statuses.

## Phase 3 — automation integrations

- [x] Typed async PyO3 SDK sharing the Rust managed client, with lifecycle, reconnect, independent
      streams, discovery, contacts, sends/ACKs, information/statistics/self-telemetry, and typed
      errors.
- [x] CPython 3.9-compatible type stubs, generated API reference, examples, installed-wheel tests,
      and pinned multi-platform abi3 wheel workflow.
- [x] Versioned nine-event trusted Python hook API with bounded subprocesses, mutation/rejection,
      validation/test commands, and examples.
- [x] Optional MQTT 3.1.1/5 gateway with TLS/mTLS validation, runtime credentials, QoS, bounds,
      dedupe/loop guards, reconnect backoff, disabled-by-default send allowlist, hooks/history/ACK
      integration, and real Mosquitto tests.

## Phase 4 — hardening and local delivery

- [x] Bounded-input, no-replay, secret-redaction, no-unsafe, and no-placeholder review.
- [x] Rust, Python, docs, workflow, fuzz, MQTT, dependency-audit, and licence-policy local gates.
- [x] Demonstrate all 20 acceptance scenarios locally or record the exact unavailable physical/
      remote portion.
- [x] Clean-room usability, maintainer, security, CLI-consistency, documentation, and local release
      artifact reviews.
- [x] Generate and smoke-test the local Linux x86-64 archive, checksum, completions, man pages, and
      abi3 wheel.
- [ ] Run the prepared GitHub Actions matrix on Linux x86-64/ARM64, macOS Intel/ARM64, and Windows
      x86-64; inspect its five native archives and five wheels.
- [ ] Run the separate physical-hardware suite and add actual device/firmware/transport rows.
- [ ] Publish the repository, push the annotated `v0.1.0-rc.1` tag, inspect the draft artifacts,
      publish registry packages in dependency order, and publish the GitHub prerelease.

## Current blockers

1. No physical MeshCore companion is exposed to the environment. BLE/USB/radio smoke testing cannot
   be represented by the deterministic or TCP-loopback results.

The GitHub repository now exists and authentication is active. Remote gates and publication proceed
in the exact order kept in [the release runbook](docs/release.md).
