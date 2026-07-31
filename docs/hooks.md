# Trusted local Python hooks

Hooks are optional trusted local code. They run as the same operating-system user as
`meshquill`; the subprocess boundary limits crashes, output and execution time, but it is **not a
sandbox**. Review a script before enabling it. The native CLI does not otherwise require Python.

## Enable and verify a hook

Copy [the basic example](../examples/hooks/basic.py) somewhere writable, then add or update this
table in the Meshquill configuration file:

```toml
[hook]
enabled = true
script = "/absolute/path/to/basic.py"
python_executable = "python3"
timeout_ms = 5000
max_concurrency = 4
environment = { mode = "safe_inherited" }
observational_failure = "open"
before_send_failure = "closed"
```

Validate without connecting to a device, then exercise the configured `on_message` handler with a
bounded fixture:

```console
meshquill hooks validate /absolute/path/to/basic.py
meshquill hooks test on_message
meshquill hooks test before_send
meshquill hooks status
```

`hooks validate PATH` validates that path with default runner settings. `hooks test EVENT` uses the
script configured in the selected configuration. `MESHQUILL_HOOK_ENABLED` and
`MESHQUILL_HOOK_SCRIPT` are useful temporary overrides.

## Versioned contract

Every handler accepts exactly one positional dictionary. Synchronous and asynchronous handlers are
supported. The checked-in [Draft 2020-12 schema](../schemas/meshquill-hook-v1.schema.json) and
[nine-event compatibility fixture](../schemas/compat/hook-v1-valid.jsonl) are the normative
machine-readable contract. The envelope is:

```json
{
  "schema": "meshquill.hook/v1",
  "event_id": "<opaque unique string>",
  "timestamp": 1785384000000,
  "event": "on_message",
  "payload": {"source": "Alice", "text": "hello", "message_id": "019ad1d5-9d22-7b11-83a7-6f58d6054ad6"}
}
```

The schema governs event dictionaries delivered to handlers; `before_send` return objects use the
separate response contract below. Envelope fields are closed. Payload objects may gain compatible
fields, so hooks must ignore unfamiliar payload keys. Every key listed below is present; `peer`,
`reason`, applicable `message_id`, `source`, `round_trip_ms`, and `display_name` are nullable rather
than omitted.

The supported handlers and payload fields are:

| Handler | Payload |
| --- | --- |
| `on_connect` | `transport`, nullable `peer` |
| `on_disconnect` | `transport`, nullable `reason` |
| `on_message` | `source`, `text`, nullable `message_id` |
| `before_send` | `destination`, `text` |
| `after_send` | `destination`, `text`, nullable `message_id` |
| `on_ack` | `message_id`, nullable `source`, nullable `round_trip_ms` |
| `on_timeout` | `operation`, nullable `message_id` |
| `on_contact_update` | `contact_id`, nullable `display_name`, `change` (`added`, `updated`, or `removed`) |
| `on_error` | `operation`, sanitized `message` |

`message_id` is a Meshquill-local workflow identifier that relates local hook events and, when enabled,
the corresponding history record. It is not a MeshCore message ID, a durable identity across
process restarts, or a deduplication key.

Observational return values are discarded. `before_send` may return `None`,
`{"action":"allow"}`, `{"action":"modify","destination":"...","text":"..."}` (either
replacement may be omitted), or `{"action":"reject","reason":"..."}`. Rust revalidates all
modified strings before any send.

## Isolation, failures and concurrency

Each validation or dispatch launches a fresh `python -I -B` subprocess directly, never through a
shell. Input, stdout, stderr, script size and returned strings are bounded. The complete operation,
including concurrency-queue wait, has the configured timeout. At most `max_concurrency` hook
subprocesses run through one hook runtime.

Observational handlers default to fail-open: Meshquill records a redacted category and continues.
`before_send` defaults to fail-closed because silently bypassing a local send policy is unsafe.
If an operator deliberately makes observational handlers fail-closed, an `after_send` or `on_ack`
failure occurs after the companion may already have accepted the radio operation. Finite send and
line chat emit their authoritative `queued`/`sent` and acknowledgement state first, then exit with
hook status and an explicit “do not retry automatically” diagnostic. MQTT command results likewise
retain `queued: true` with a post-send failure reason. A hook failure never causes an automatic
radio replay.
The safe environment mode exposes only a conservative locale/time/path allowlist; `clear` exposes
none, and `{ mode = "allow_list", variables = ["NAME"] }` names explicit variables. Python
exception text and captured output are intentionally not copied into normal logs because they may
contain message content.

## Small cookbook

- Notifications: implement `on_message`, but invoke a bounded local notifier and return quickly.
- Local audit metadata: append `event_id`, sender and timestamp; storing `payload.text` creates an
  additional plaintext message-history store that you must secure and rotate yourself.
- Send policy: use `before_send` to reject or rewrite a destination. Keep the default closed policy.
- Monitoring: use `on_error` and `on_timeout`; do not put broker tokens or other secrets in hook
  source.

Run `meshquill hooks validate` after every edit. A handler must remain callable and accept exactly
one positional parameter.
