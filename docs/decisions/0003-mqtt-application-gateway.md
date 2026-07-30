# ADR 0003: MQTT is an optional application event gateway

- Status: accepted
- Date: 2026-07-30

## Decision

Implement a versioned Meshquill MQTT event schema above the core client. Do not feed MQTT packets into
MeshCore routing and do not call MQTT a device/radio transport. Incoming application events may be
published. Outbound direct/channel send is disabled until explicitly enabled and allowlisted.
Arbitrary administrative operations are not supported over MQTT by default.

## Rationale

No normative upstream MQTT specification exists at the pinned revisions. Upstream issue #37 remains
an enhancement discussion; firmware's official RS-232/ESP-NOW bridge is a separate mechanism.
