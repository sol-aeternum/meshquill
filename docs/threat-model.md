# Threat model

## Protected assets

- Device identity and private keys.
- Remote login passwords, BLE PINs and MQTT credentials/client keys.
- Message content, contact graph, position, paths and optional local history.
- Integrity of outbound radio messages and destructive administration choices.
- Host availability: memory, file descriptors, terminal state and log/storage bounds.

## Trust boundaries

Device frames, radio-derived payloads, TCP peers, serial bytes, MQTT brokers/messages and files being
imported are untrusted. Configuration is user-controlled but may be malformed. Hook code is trusted
local code; hook input/output is validated, and the process is failure-isolated, but no sandbox is
claimed. OS credential stores and release CI are separate trusted services with their own limits.

The companion TCP transport is the firmware's raw framed protocol: Meshquill does not add TLS,
server authentication, or application credentials to it. Use only a trusted host/network or a
separately authenticated tunnel. For MQTT, TLS authenticates the broker (and optional mTLS the
client), but the broker and its ACL administrators remain authoritative over every topic they may
publish. Enabling outbound sends therefore trusts both broker operation and ACL configuration.

## Required controls

- Length-before-allocation checks, bounded queues, a one-MiB configuration limit, 4096-byte
  interactive-line limit, 24-hour timeout ceiling, payload limits, four parser/command fuzz targets,
  and no panics on malformed external input.
- One request at a time unless the protocol provides a validated correlation tag.
- Ambiguous writes are never automatically resent after reconnect.
- TLS certificate validation by default; no hidden insecure MQTT switch.
- MQTT outbound commands disabled by default, allowlisted by command and topic, size-limited and
  deduplicated with event IDs. Because deduplication is process-local, send-enabled gateways require
  a clean broker session. Administrative commands remain unavailable through MQTT.
- Secrets represented by references/redacted wrappers, accepted via prompt/file descriptor/env where
  appropriate, never emitted by effective-config or debug formatting.
- Atomic configuration writes, restrictive file modes and safe same-directory temporary files.
- Confirmation for destructive operations; `--yes` only with an explicit target and operation.
- Hook deadlines, bounded output, process termination on timeout and errors converted to events.
- The portable line UI does not enter raw terminal mode or emit terminal-control sequences. SIGINT
  drives cooperative cancellation and bounded disconnect on supported hosts.

## Out of scope

- Protecting a host after trusted hook code is malicious.
- Securing a compromised OS, firmware, Bluetooth stack, serial driver or MQTT broker.
- Providing confidentiality or peer authentication for a raw companion TCP endpoint.
- Changing MeshCore's radio-layer cryptography or promising metadata anonymity not provided upstream.
- Treating application-level MQTT as off-grid or as an official MeshCore bridge protocol.

Security findings block a release candidate until resolved or documented as a specific non-critical
limitation with a defensible safe default.
