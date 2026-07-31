# Production plan

Last updated: 2026-07-31

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
      retained unconfirmed draft with explicit resend/discard handling. A full-screen TUI was
      deliberately deferred because it is not required for a usable keyboard-only RC and would add
      terminal-state risk without hardware evidence.
- [x] Doctor checks, completions, generated command-tree man pages, examples, command suggestions,
      output contracts, redaction, and stable exit statuses.

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

The completed gates in the first part of this phase record the RC1/RC2 baseline. RC3 has its own
fresh candidate-specific gates below; prior-candidate results are not carried forward.

- [x] Bounded-input, no-replay, secret-redaction, no-unsafe, and no-placeholder review.
- [x] Rust, Python, docs, workflow, fuzz, MQTT, dependency-audit, and licence-policy local gates.
- [x] Demonstrate all 20 acceptance scenarios locally or record the exact unavailable physical/
      remote portion.
- [x] Clean-room usability, maintainer, security, CLI-consistency, documentation, and local release
      artifact reviews.
- [x] Generate and smoke-test the local Linux x86-64 archive, checksum, completions, man pages, and
      abi3 wheel.
- [x] Run the exact pushed commit through pre-tag CI on Linux x86-64, macOS Intel/ARM64, and Windows
      x86-64, plus Python, MSRV, fuzz, broker, documentation, audit, and licence gates.
- [x] Publish the public repository and immutable `v0.1.0-rc.1` tag; run and inspect its five native
      archives and five wheels without moving or replacing the tag.
- [x] Harden RC2 reconnect/no-replay behavior, bounded live/queued correlation with documented
      collision limits, SIGINT cleanup, line chat, input/timeout/config bounds, output schemas,
      remote/MQTT fuzzing, real-broker security cases, and exact Python tool pins.
- [x] Run RC2 pre-tag CI and supply-chain gates, push the annotated `v0.1.0-rc.2` tag, then run its
      tagged Linux x86-64/ARM64, macOS Intel/ARM64, and Windows x86-64 artifact matrix. Preserve the
      immutable private draft as superseded after its packaged status documentation proved stale.
- [x] Prepare RC3 profile lifecycle and selection, platform data roots/history reconciliation,
      firmware-derived BLE bounds, exact incoming-observation correlation, balanced hook lifecycle,
      MQTT startup feeds/environment secrets, and executable CLI/hook/MQTT schemas.
- [x] Complete the fresh RC3 local gates, clean-room/PTY/artifact review, and all 20 acceptance
      scenarios, explicitly recording the unavailable physical-radio portion rather than claiming
      deterministic coverage as hardware evidence.
- [x] Push the exact RC3 candidate through CI and supply-chain gates and create immutable annotated
      `v0.1.0-rc.3`. Preserve that tag after its workflow built and validated all target artifacts
      but failed before draft assembly because wheel jobs invoked a CLI test without building the
      CLI; no RC3 release or release assets were created.
- [x] Audit and close the six outstanding dependency/tooling pull requests without merging changes
      that were incompatible, incomplete, unexercised, or behavior-regressing for this candidate.
- [x] Prepare RC4 with the release-wheel test scope corrected while retaining full CLI/schema
      coverage in the prerequisite CI workflow.
- [ ] Complete exact-source RC4 CI/supply-chain gates, create immutable annotated `v0.1.0-rc.4`,
      and inspect all five archives, five wheels, checksums, and packaged docs. Fresh local RC4
      delta validation is complete.
- [ ] Run the separate physical-hardware suite and add actual device/firmware/transport rows.
- [ ] Publish registry packages in dependency order when credentials exist, and request explicit
      maintainer approval before changing the tested RC4 GitHub draft to a public prerelease.

## Current blockers

1. No physical MeshCore companion is exposed to the environment. BLE/USB/radio smoke testing cannot
   be represented by the deterministic or TCP-loopback results.
2. No crates.io credential-provider token or PyPI upload token is available locally or in the
   repository. This blocks only those two registry uploads; it does not block the tagged artifacts
   or public GitHub prerelease.

The GitHub repository and superseded RC1/RC2 drafts exist, RC3's immutable failed tag is preserved,
authentication is active, and RC4 is being gated. Artifact and publication work proceeds in the
exact order kept in [the release runbook](docs/release.md).
