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

## Required controls

- Length-before-allocation checks, bounded queues, payload limits, parser fuzzing and no panics.
- One request at a time unless the protocol provides a validated correlation tag.
- Ambiguous writes are never automatically resent after reconnect.
- TLS certificate validation by default; no hidden insecure MQTT switch.
- MQTT outbound commands disabled by default, allowlisted by command and topic, size-limited and
  deduplicated with event IDs. Administrative commands remain unavailable through MQTT by default.
- Secrets represented by references/redacted wrappers, accepted via prompt/file descriptor/env where
  appropriate, never emitted by effective-config or debug formatting.
- Atomic configuration writes, restrictive file modes and safe same-directory temporary files.
- Confirmation for destructive operations; `--yes` only with an explicit target and operation.
- Hook deadlines, bounded output, process termination on timeout and errors converted to events.
- Terminal restoration through RAII and signal/cancellation handling.

## Out of scope

- Protecting a host after trusted hook code is malicious.
- Securing a compromised OS, firmware, Bluetooth stack, serial driver or MQTT broker.
- Changing MeshCore's radio-layer cryptography or promising metadata anonymity not provided upstream.
- Treating application-level MQTT as off-grid or as an official MeshCore bridge protocol.

Security findings block a release candidate until resolved or documented as a specific non-critical
limitation with a defensible safe default.
