# ADR 0004: Serialize companion requests without correlation IDs

- Status: accepted
- Date: 2026-07-30

## Decision

Allow only one ordinary request awaiting a response. Match against a command-specific response set
and timeout. Use protocol tags as additional correlation for binary/anonymous flows. Push events and
ACKs remain concurrent because they have distinct codes or ACK identifiers.

## Rationale

The companion protocol generally correlates by response type, not sequence. Concurrent same-type or
overlapping requests can otherwise receive one another's response. Serialization trades negligible
local throughput for deterministic behavior on a low-bandwidth radio device.
