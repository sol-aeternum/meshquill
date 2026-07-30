# Upstream compatibility watch

Snapshot: 2026-07-30. Open work is research input, not implemented protocol. The responses below
are release-watch requirements, not claims that Meshquill already implements them. Recheck status
and source before every release.

## Releases and active changes

| Upstream item | State at snapshot | Meshquill consequence |
| --- | --- | --- |
| [Companion firmware v1.16.0](https://github.com/meshcore-dev/MeshCore/releases/tag/companion-v1.16.0) | latest stable, 2026-06-06 | Primary hardware compatibility baseline. Negotiate rather than assume later protocol features. |
| [meshcore_py v2.3.8](https://github.com/meshcore-dev/meshcore_py/releases/tag/v2.3.8) | latest, 2026-07-27 | Scope behavior changed recently: `*` is force-unscoped, empty/`0` is default. Golden tests cover both. |
| [meshcore-cli v1.5.0](https://github.com/meshcore-dev/meshcore-cli/releases/tag/v1.5.0) | latest tagged CLI, 2026-03-10; main newer | Multi-byte paths are required. Main also contains behavior absent from the tagged README. |
| [PR #2888: node-to-companion fragmentation](https://github.com/meshcore-dev/MeshCore/pull/2888) | open, unmerged; proposes protocol 14 and push `0x91` | Do not emit protocol 14 yet. Preserve bounded unknown packets and design a reassembly extension point. If merged, add negotiated fragment IDs/ACKs and fuzz the reassembler before claiming support. |
| [PR #2972: multi-client Ethernet companion](https://github.com/meshcore-dev/MeshCore/pull/2972) | open, unmerged | TCP remains ordinary framed companion transport. Doctor must handle connection ownership/full slots and configurable ports; do not assume the proposed default 4403 or multiple clients on v1.16.0. |
| [PR #2717: selective companion repeat](https://github.com/meshcore-dev/MeshCore/pull/2717) | open, unmerged | Do not expose a repeat-mode command until a command code and firmware behavior merge. Contact favorite flags may gain routing consequences, so updates need explicit wording. |
| [PR #1779: dual BLE/USB interface](https://github.com/meshcore-dev/MeshCore/pull/1779) | closed without merge | Keep the documented rule that companion builds commonly expose one transport. Discovery may show alternatives but must never promise both. |

## Current issues that change UX or validation

| Issue | Observed risk | Required response |
| --- | --- | --- |
| [#2334 BLE ghost connection](https://github.com/meshcore-dev/MeshCore/issues/2334) | Firmware/display may report connected after DFU/reset when the phone is not connected. | Current doctor can enumerate providers and optionally test a protocol handshake, but cannot diagnose firmware/display state. Keep radio reset and stale-pairing recovery in troubleshooting guidance. |
| [#3050 persisted contact timestamps can corrupt RTC](https://github.com/meshcore-dev/MeshCore/issues/3050) | Reported and hardware-verified against v1.16.0: future `lastmod` can survive reboot, block backward clock correction and trigger room replay protection. | Meshquill does not currently audit device/contact timestamps or import/export them. Do not claim that diagnosis; if added, require read-back after clock sync and never rewrite timestamps silently. |
| [#2583 inconsistent group text capacity](https://github.com/meshcore-dev/MeshCore/issues/2583) | Effective group-text capacity varies with sender-name length and encryption block padding; long messages can be valid over radio but exceed companion reporting limits. | Compute conservative firmware-version limits, validate UTF-8 bytes rather than characters, show byte budget, and never split into multiple radio messages without explicit user choice. |
| [#3057 channel export omits region scope](https://github.com/meshcore-dev/MeshCore/issues/3057) | Official-app channel backup may lose per-channel region intent. | Meshquill has no channel import/export command in this RC. If one is added, preserve scope explicitly and warn when an upstream format cannot represent it; never infer an unscoped channel. |
| [#3053 anti-spam confirmation](https://github.com/meshcore-dev/MeshCore/issues/3053) | Frequent sends waste constrained airtime. | Current batch/MQTT inputs are bounded but have no airtime-aware rate limiter. Treat rate limiting and interactive burst warnings as future work. |
| [#2397 contact persistence blocks loop](https://github.com/meshcore-dev/MeshCore/issues/2397) | Contact writes may make firmware temporarily unresponsive and drop packets. | Use realistic timeouts and distinguish a late device from a disconnected transport; do not hammer retries. |

## Discussions and bridge semantics

- [Discussion #1614](https://github.com/meshcore-dev/MeshCore/discussions/1614) records the
  ongoing move from one-byte path prefixes toward variable/multi-byte paths. Meshquill models path
  hash width explicitly and keeps raw path bytes.
- [Issue #37](https://github.com/meshcore-dev/MeshCore/issues/37) and
  [discussion #1736](https://github.com/meshcore-dev/MeshCore/discussions/1736) show that MQTT/TCP
  backhaul remains proposal/community territory. Official RS-232/ESP-NOW bridge support does not
  establish a companion MQTT schema. ADR 0003 therefore remains valid.

## Release recheck procedure

1. Fetch upstream main and record commit IDs/dates.
2. Compare `CommandType`, `PacketType`, firmware frame constants and companion protocol docs.
3. Re-run GitHub queries for open/merged items above and new companion/protocol labels.
4. Update fixtures only from reviewed source or captured hardware frames, recording provenance.
5. Run compatibility tests against every supported stable firmware version before changing the
   documented support range.
