# Changelog

Meshquill follows [Semantic Versioning](https://semver.org/) after the first release candidate.
This file follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.0-rc.2] - 2026-07-31

### Changed

- Added bounded three-attempt companion reconnect for `watch` and line chat, connection-scoped
  live/queued occurrence correlation, and host-level SIGINT cancellation tests. Reconnect never
  replays a mutation or an ambiguous radio write; identical messages observed through the same
  observation path remain distinct occurrences.
- Expanded portable chat with live incoming display during input, contact search and target
  switching, channel switching, conversation-filtered history, contextual help and suggestions,
  literal slash messages, and destination-bound retained drafts with explicit `/send` or `/discard`.
- Added a stable checked-in `meshquill.cli/v1` JSON schema and compatibility fixtures for finite and
  streaming output.
- Enforced a one-MiB configuration input bound, a 4096-byte line-input bound, and a 24-hour maximum
  for stored, CLI, transport, core, hook and Python operation timeouts.
- Added fuzz targets and corpora for every remote-payload parser family and the allowlisted MQTT
  command processor, alongside the existing inner-packet and outer-frame targets.
- Expanded real-broker CI to cover MQTT 3.1.1 and MQTT 5 round trips plus private-CA TLS,
  username/password, mTLS, wrong-trust, wrong-password, missing-client-identity and hostname-failure
  cases. Send-enabled MQTT now rejects persistent sessions because deduplication is process-local.
- Exact-pinned Python build/test tools are shared by CI, wheel smoke tests and supply-chain audit;
  release wheels are installed and exercised on both CPython 3.9 and 3.14 on every target.

### Fixed

- Added bounded, connection-local heuristic correlation for a likely live/queued double-observation
  without globally discarding messages based on a non-unique payload fingerprint. Correlation is
  limited by opposite observation path, multiplicity, five seconds, and 256 entries. Because the
  protocol has no globally unique message ID, a distinct identical opposite-path occurrence inside
  that window can collide.
- Prevented a failed chat draft from being silently retargeted or overwritten, preserved typed text
  through target/inbox reconnects, and made `/send` the only explicit resend path for an unconfirmed
  draft. If the original write succeeded but its response was lost, `/send` can duplicate delivery;
  `/discard` avoids that risk.
- Added explicit managed-operation cancellation so SIGINT cleanup cannot remain queued behind a
  dropped connect or ACK wait; preserved stale-event boundaries without automatic resend after
  cancellation, timeout, or disconnect.

### Known limitations

- No physical companion or radio was available. BLE, serial and companion TCP behavior remains
  host-tested and simulated rather than hardware-verified against a recorded firmware/device pair.
- GitHub archives and wheels are unsigned, and macOS/Windows binaries are not code-signed.
- Chat remains line-oriented; named-channel sending and manual pending-contact acceptance remain
  deliberately unsupported in this RC.
- crates.io and PyPI publication require independently scoped credentials that are not present in
  the maintainer environment; the GitHub prerelease is publishable independently.

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
