# Authoritative research

Research date: 2026-07-30 (Australia/Adelaide)

This file records the sources used to design behavior. Source commits are pinned because upstream
is active. Protocol decisions must be rechecked before each release.

## Pinned sources

| Source | Revision inspected | Relevant findings |
| --- | --- | --- |
| [meshcore-dev/meshcore-cli](https://github.com/meshcore-dev/meshcore-cli) | main `56b246b4d45174817936c0fc910897b52bec66b4`, 2026-07-30; release [v1.5.0](https://github.com/meshcore-dev/meshcore-cli/releases/tag/v1.5.0) | BLE/serial/TCP selection, contextual interactive grammar, command chaining, `apply_to`, JSON prefix, sensor/repeater/room workflows, serial repeater mode and region files. |
| [meshcore-dev/meshcore_py](https://github.com/meshcore-dev/meshcore_py) | [v2.3.8](https://github.com/meshcore-dev/meshcore_py/releases/tag/v2.3.8), `c487efbe187f4b000020afdfc0349c4cdf503c5a`, 2026-07-27 | Current command and push-code catalog, transport framing, event behavior, retry rules and scope semantics. `*` means explicitly unscoped; empty/`0` restores firmware default scope. |
| [meshcore-dev/MeshCore](https://github.com/meshcore-dev/MeshCore) | main `03b6ef4b0de98fc70b49ef10a6d0d61f8381fb7a`, 2026-07-28 | Firmware handlers and source-hosted docs. Latest stable companion firmware is [v1.16.0](https://github.com/meshcore-dev/MeshCore/releases/tag/companion-v1.16.0), released 2026-06-06. |
| [Companion protocol](https://docs.meshcore.io/companion_protocol/) | page says last updated 2026-03-08, companion v1.12.0+ | BLE UUIDs, sequencing, selected commands and payload layouts. The page warns it is incomplete/inaccurate, so firmware and current Python behavior are also required. |
| [Firmware CLI commands](https://docs.meshcore.io/cli_commands/) | main docs at pinned firmware revision | Local and remote `get`/`set`, region, radio, identity, bridge, sensor and administration grammar. |
| [Packet format](https://docs.meshcore.io/packet_format/) and [payloads](https://docs.meshcore.io/payloads/) | main docs at pinned firmware revision | Radio-layer context; Meshquill does not invent or expose an alternate radio transport. |

## Protocol findings that affect architecture

- BLE carries a raw companion packet per characteristic write/notification. The Nordic-UART-style
  service is `6e400001-b5a3-f393-e0a9-e50e24dcca9e`; app-to-device RX ends in `0002`, and
  device-to-app TX ends in `0003`.
- USB serial and TCP add an outer frame. App-to-device is `0x3c` plus a two-byte little-endian
  payload length; device-to-app is `0x3e` plus the length. Current Python rejects declared frames
  above 300 bytes. A streaming decoder must tolerate partial reads, reject oversized frames and
  resynchronize without unbounded buffering.
- The inner packet starts with a one-byte command, response or push code. Most request/response
  pairs have no sequence identifier. Concurrent ordinary commands can therefore consume the wrong
  response if matched only by type. Meshquill serializes these commands; binary/anonymous flows may
  additionally use their protocol tag.
- The official companion document lists only a subset of current commands and events. Current
  `meshcore_py` has command codes through `0x40` with gaps and push codes through `0x90`.
- Current documentation says channel text send returns `MSG_SENT (0x06)`, while current Python
  expects `OK (0x00)`. Compatibility must be firmware-version-aware and accept only the explicitly
  validated response set, recording which response occurred.
- Direct message ACK identifiers are derived by firmware and returned in `MSG_SENT`; ACK push
  notifications carry the matching code. A sent write is not delivery confirmation.
- Path prefixes became multi-byte in upstream CLI v1.5.0 / Python v2.3.0. Paths cannot be modeled
  as a list of single bytes.
- Flood scopes are transport codes introduced in firmware v1.10 and evolved through v1.15+.
  Default, explicitly unscoped and named/region scope are distinct states.
- A companion build commonly exposes only one of BLE or USB serial. Failure text must not imply
  that every firmware image supports both.

Known discrepancies and exact coverage live in [protocol-coverage.md](protocol-coverage.md).
Current release, issue, pull-request and discussion risks are maintained in
[upstream-watch.md](upstream-watch.md).

## Existing CLI interaction findings

The current CLI is not a flat command list. It has shell command mode, contextual chat mode,
contact-context compact commands, command files, an `apply_to` filter language, and a separate
serial-repeater console. Compatibility aliases are useful, but Meshquill will expose a coherent
hierarchical grammar and make legacy spellings explicit rather than silently guessing.

Notable existing-only behaviors that require an account in the capability matrix include:

- command chaining and `script` files;
- per-command `.` JSON selection;
- chat destinations (`to`, `/`, `..`, `!`) and context-dependent default behavior;
- `apply_to` filters for age, contact type, hop count, direct and flood paths;
- pending/manual contact workflows and automatic contact refresh;
- region upload/download in direct serial repeater mode;
- channel echoes built from RX logs;
- aliases and contact-specific timeouts.

## MQTT conclusion

No normative MeshCore MQTT protocol exists in the firmware repository, companion protocol, Python
library or CLI at the pinned revisions. [MeshCore issue #37](https://github.com/meshcore-dev/MeshCore/issues/37)
is an open enhancement covering non-LoRa bridges including MQTT. Official firmware currently has
RS-232 and ESP-NOW bridge support, which is different from an application event gateway. The
official [Home Assistant integration](https://github.com/meshcore-dev/meshcore-ha) has optional MQTT
upload features but does not define a general companion-command MQTT standard.

Meshquill therefore implements MQTT only as an opt-in application-level gateway attached to one
client. It will not describe MQTT as a MeshCore radio transport, and outbound transmission will be
disabled by default. See ADR 0003.

## Comparable CLI lessons

- [Meshtastic's Python CLI](https://meshtastic.org/docs/software/python/cli/) makes transport
  selection explicit, documents configuration import/export and treats shell use as first-class.
  Meshquill adopts the clarity, not its protocol or command vocabulary.
- [Reticulum's manual](https://reticulum.network/manual/using.html) separates long-running service
  behavior from focused status/path utilities and offers machine-readable output. Meshquill avoids
  a daemon until connection sharing demonstrates the need, but keeps gateway lifetime explicit.

These projects are UX/packaging references only and are not protocol sources.

## Environment research

- Host: x86-64 CachyOS Linux, kernel 7.1.5.
- Initially available: Python 3.14.6, `uv`, Git 2.55.0, GitHub CLI 2.96.0 and Docker 29.6.2.
- Initially absent: Rust/Cargo, maturin, Mosquitto clients, cargo-audit, cargo-deny and mdBook.
- A self-contained Rust 1.97.1 toolchain was installed under `/tmp` for development.
- No USB serial node is exposed. USB enumeration cannot initialize. Bluetooth tooling exists, but
  D-Bus access fails in this container. These facts prohibit a physical-test claim.
- GitHub CLI's stored credential for `sol-aeternum` is expired. Publication remains a final gate.
