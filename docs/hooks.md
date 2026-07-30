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
supported. The envelope is:

```json
{
  "schema": "meshquill.hook/v1",
  "event_id": "<opaque unique string>",
  "timestamp": 1785384000000,
  "event": "on_message",
  "payload": {"source": "Alice", "text": "hello", "message_id": "1234"}
}
```

The supported handlers and payload fields are:

| Handler | Payload |
| --- | --- |
| `on_connect` | `transport`, optional `peer` |
| `on_disconnect` | `transport`, optional `reason` |
| `on_message` | `source`, `text`, optional `message_id` |
| `before_send` | `destination`, `text` |
| `after_send` | `destination`, `text`, optional `message_id` |
| `on_ack` | `message_id`, optional `source`, optional `round_trip_ms` |
| `on_timeout` | `operation`, optional `message_id` |
| `on_contact_update` | `contact_id`, optional `display_name`, `change` (`added`, `updated`, or `removed`) |
| `on_error` | `operation`, sanitized `message` |

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
