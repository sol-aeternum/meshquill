# `meshquill.mqtt/v1` schema

The checked-in [Draft 2020-12 schema](../../schemas/meshquill-mqtt-v1.schema.json),
[positive fixtures](../../schemas/compat/mqtt-v1-valid.jsonl), and
[negative fixtures](../../schemas/compat/mqtt-v1-invalid.jsonl) are the executable contract.

Every MQTT application payload is one UTF-8 JSON object with exactly these envelope fields:

| Field | Type | Rule |
| --- | --- | --- |
| `schema` | string | exactly `meshquill.mqtt/v1` |
| `event_id` | canonical hyphenated UUID string | non-nil; used for bounded duplicate suppression |
| `origin` | string | 1–128 bytes, trimmed, no control characters |
| `timestamp` | integer | Unix epoch milliseconds in the unsigned 64-bit range |
| `type` | string | discriminator listed below |
| `data` | object | event-specific object, never a scalar or array |

Published event types are:

| Type/topic suffix | `data` |
| --- | --- |
| `incoming_message` / `events/incoming_message` | Stable MQTT representation: discriminated `source` and `route`, `txt_type`, `sender_timestamp`, optional lowercase-hex `signature`, `text`, optional `snr`, and discriminated `status` |
| `ack` / `events/ack` | Lowercase-hex acknowledgement `code` and optional `trip_time_ms` |
| `connection_state` / `events/connection_state` | `component` (`mesh_core` or `mqtt_broker`), `status` (`connected` or `disconnected`), optional stable `reason` |
| `contacts` / `events/contacts` | `contacts` array and companion `lastmod` sequence |
| `telemetry` / `events/telemetry` | Exact `kind`-discriminated object: `battery`, `stats_core`, `stats_radio`, `stats_packets`, or `raw_cayenne_lpp` |

All seven envelope variants and their typed `data` objects reject unknown fields. Byte-bearing keys,
paths, signatures, acknowledgement codes, telemetry prefixes, and raw payloads use lowercase hex as
specified by the schema.

The sole subscribed topic is `<prefix>/meshquill.mqtt/v1/outbound/send`. It accepts only:

```json
{"schema":"meshquill.mqtt/v1","event_id":"018f8f4c-9e5d-7d62-8bb8-94d547f2979b","origin":"remote-operator","timestamp":1785384000000,"type":"send_direct","data":{"destination":"Alice","text":"hello"}}
```

or:

```json
{"schema":"meshquill.mqtt/v1","event_id":"018f8f4c-9e5d-7d62-8bb8-94d547f2979c","origin":"remote-operator","timestamp":1785384000000,"type":"send_channel","data":{"channel":0,"text":"hello"}}
```

Unknown envelope or command-data fields are rejected. Defaults limit the entire payload to 64 KiB,
destinations to 128 UTF-8 bytes, text to 1024 UTF-8 bytes and channels to 0–7. The limits can be
tightened in configuration; validation imposes hard upper bounds. No administrative command type
is part of v1.

The checked-in JSON Schema expresses string `maxLength` in Unicode code points, as required by the
JSON Schema specification. The Rust decoder then applies the documented UTF-8 byte limits to
`origin`, `destination`, and `text`; multibyte input can therefore pass a generic schema validator
and still be rejected safely by the gateway. Both layers reject empty values where required,
leading/trailing destination or origin whitespace, control characters in destination/origin, NUL
in text, and timestamps outside `u64`.

Retained MQTT publications are never commands and are rejected before event-ID deduplication.
After an optional trusted `before_send` hook, the gateway revalidates the modified command against
the configured limits before any radio write. When outbound sends are enabled, a broker connection
is not command-ready until exactly one successful SUBACK is received for the sole outbound topic.
