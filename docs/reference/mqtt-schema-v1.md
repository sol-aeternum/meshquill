# `meshquill.mqtt/v1` schema

Every MQTT application payload is one UTF-8 JSON object with exactly these envelope fields:

| Field | Type | Rule |
| --- | --- | --- |
| `schema` | string | exactly `meshquill.mqtt/v1` |
| `event_id` | UUID string | non-nil; used for bounded duplicate suppression |
| `origin` | string | 1–128 bytes, trimmed, no control characters |
| `timestamp` | integer | Unix epoch milliseconds |
| `type` | string | discriminator listed below |
| `data` | object | event-specific object, never a scalar or array |

Published event types are:

| Type/topic suffix | `data` |
| --- | --- |
| `incoming_message` / `events/incoming_message` | Rust core message object: `source`, optional key/channel/path metadata, `text`, timestamp fields |
| `ack` / `events/ack` | acknowledgement code and optional metrics exposed by the companion |
| `connection_state` / `events/connection_state` | `component` (`mesh_core` or `mqtt_broker`), `status` (`connected` or `disconnected`), optional stable `reason` |
| `contacts` / `events/contacts` | `contacts` array and companion `lastmod` sequence |
| `telemetry` / `events/telemetry` | optional `source` and a `values` object |

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
