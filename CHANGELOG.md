# Changelog

Meshquill follows [Semantic Versioning](https://semver.org/) after the first release candidate.
This file follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.0-rc.1] - 2026-07-30

### Added

- A Rust-first MeshCore companion stack with bounded packet and outer-frame codecs, a managed async
  client, host-tested BLE, USB serial and TCP transports, atomic versioned profiles, and a
  deterministic in-memory companion for hardware-free tests and examples.
- A native `meshquill` CLI covering discovery and profiles, device/contact/channel operations,
  direct and channel messaging, ACK waits, inbox/watch, line chat, history, remote and sensor
  queries, scoped network operations, bounded batch execution, stable JSON/JSONL automation output,
  shell completions, and man pages.
- Opt-in trusted local Python hooks and an application-level MQTT gateway with bounded processing,
  TLS validation, redacted configuration, and an explicit allowlist for inbound radio sends.
- The typed async `meshquill-sdk` Python distribution, imported as `meshcore_sdk`, with CPython 3.9+
  stable-ABI wheels, explicit BLE/serial/TCP constructors, discovery, independent event/message
  streams, and the deterministic demo client.
- Prepared pinned CI and draft-release workflows for Linux x86-64/ARM64, macOS Intel/Apple Silicon,
  and Windows x86-64, including parser fuzz smoke jobs, dependency audit/licence gates,
  per-archive SHA-256 files, and a combined artifact checksum manifest.

### Known limitations

- This is a release candidate, not a production-readiness or stable-API claim.
- No physical companion or radio was available for verification. BLE, serial, and TCP behavior is
  host-tested and simulated, not hardware-verified against a physical device/firmware combination.
- Release archives, wheels, and checksum manifests are not cryptographically signed; macOS and
  Windows binaries are not code-signed.
- Chat is deliberately line-oriented rather than a full-screen TUI, named-channel sending and manual
  pending-contact acceptance remain unsupported, and ambiguous mutating operations are never
  automatically replayed.
- The tag workflow creates tested multi-platform artifacts and a draft GitHub release, but crates.io
  and PyPI publication remain explicit manual maintainer steps.
