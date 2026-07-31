# Changelog

Meshquill follows [Semantic Versioning](https://semver.org/) after the first release candidate.
This file follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.0-rc.3] - 2026-07-31

### Added

- Added `profiles list`, `reconfigure`, `rename`, `delete`, and `set-default`, plus shared explicit,
  default, or sole-profile selection in the CLI and Python SDK. Interactive BLE/serial setup now
  performs bounded discovery, presents sorted choices, and retains a manual fallback.
- Added platform application-data roots, `--data-dir`/`MESHQUILL_DATA_DIR`, collision-resistant
  namespaces for explicit configuration paths, and bounded one-way reconciliation of legacy
  config-adjacent history.
- Added Draft 2020-12 schemas and compatibility fixtures for all nine hook events and the strict
  MQTT v1 wire contract, with independent Python validator tests.
- Added startup MQTT contact, battery, and raw-telemetry snapshots, typed contact/route/status and
  telemetry DTOs, debounced contact resynchronization, and fresh-process TLS/mTLS coverage for
  environment-backed password references.

### Changed

- Applied the firmware's 176-byte logical companion-packet bound independently from the defensive
  300-byte outer-frame bound. BLE writes now make one provider call: a reliable ATT long write when
  supported, or one bounded write-without-response packet that fits the negotiated MTU.
- Extended balanced hook lifecycle dispatch across ordinary commands, contact mutations,
  reconnecting watch/chat sessions, and the MQTT bridge while preserving primary failures and
  redacting secondary diagnostics.
- Made profile deletion retain history and external credentials explicitly; profile rename migrates
  history before changing configuration, refuses retained destination data, and reports the
  credential-identity limitation.
- Serialized complete configuration and history transactions with private cross-process sidecar
  locks. Profile identifiers now have a 64-byte ceiling, lower history-retention settings prune
  safely, and relative/empty platform storage roots cannot redirect persistence unexpectedly.
- Canonicalized configured MQTT TLS paths, applied one 4096-byte password bound to every secret
  source, and made send-capable connection tests wait for an exact successful SUBACK.

### Fixed

- Drained already-buffered asynchronous notifications before `SYNC_NEXT_MESSAGE` and conservatively
  reconciled a ready late response without issuing or replaying another sync command.
- Tracked the one outstanding sync response before the transport write. Cancellation or timeout now
  blocks later commands until the response is reconciled or the connection is reset.
- Replaced payload-fingerprint suppression with exact ephemeral observation IDs shared only by the
  returned message and its event-broadcast clone. Distinct identical direct or channel messages are
  no longer coalesced.
- Preserved genuine contact snapshot markers, separated legacy markerless contact listing, bounded
  MQTT command destinations/text independently of configuration, and refreshed snapshots only
  after an observed broker disconnect/reconnect transition.
- Rejected retained MQTT commands before parsing/deduplication, revalidated hook-modified commands,
  terminated the bridge cleanly on a companion disconnect, and published one full state snapshot
  after each observed broker reconnect.
- Preserved authoritative `queued`/`sent` and acknowledgement output when fail-closed post-send
  hooks or cleanup fail, preventing an already accepted radio message from looking safe to retry.
- Aligned the MQTT schema's expressible origin, destination, text, and `u64` timestamp constraints
  with the Rust contract while retaining explicit UTF-8 byte-bound enforcement in Rust.
- Aligned every documented CLI principal field and incoming-chat variant with the executable schema,
  and required canonical hyphenated UUID spellings at both MQTT decoding boundaries.
- Zeroized MQTT password buffers on every UTF-8 and validation rejection path, and made the maximum
  valid escaped history record round-trip within the same bound enforced while loading.

### Known limitations

- No physical companion or radio was available. BLE, serial and companion TCP behavior remains
  host-tested and simulated rather than hardware-verified against a recorded device/firmware pair.
- GitHub archives and wheels are unsigned, and macOS/Windows binaries are not code-signed.
- Chat remains line-oriented; named-channel sending and manual pending-contact acceptance remain
  deliberately unsupported in this RC.
- crates.io and PyPI publication require independently scoped credentials that are not present in
  the maintainer environment; GitHub prerelease publication remains an independent approval step.

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
