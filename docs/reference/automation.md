# Automation contract

Meshquill separates finite command results from live event streams. This page documents the
`meshquill.cli/v1` contract implemented by `0.1.0-rc.1`.

## Output modes

- `--output human` is the default. Human wording may improve between releases while preserving
  meaning.
- `--output json` emits exactly one JSON value for a finite command.
- `--output jsonl` emits one JSON object per line for a stream such as `watch`, `chat`, or
  `mqtt bridge`.
- A stream rejects `json`, and a finite command rejects `jsonl`. This prevents a script from
  silently consuming the wrong shape.
- Result data goes to stdout. Diagnostics always use plain `error:` and optional `hint:` lines on
  stderr, including when JSON output was requested. Meshquill does not currently emit JSON error
  records.
- Redirected stdout is plain: it has no colour, spinner, progress, or terminal-control output.

Every successful machine-readable record uses this envelope:

```json
{
  "schema": "meshquill.cli/v1",
  "type": "contacts",
  "data": {
    "profile": "demo",
    "contacts": []
  }
}
```

The `type` identifies the result or event shape. Compatible `meshquill.cli/v1` releases may add
optional properties under `data`; removing a property, renaming it, or changing its meaning
requires a new schema identifier. Fields explicitly described as keys, key prefixes, routes,
paths, signatures, acknowledgement codes, or opaque payloads are generally lowercase hexadecimal.
Do not assume every byte-bearing field is a hex string: for example, the current `self_info`
representation serializes its public key as a JSON byte array.

## Representative result types

This table lists the stable top-level fields most useful to automation. Nested protocol models may
contain additional fields.

| `type` | Command family | Principal `data` fields |
| --- | --- | --- |
| `contacts` | `contacts` | `profile`, `contacts` |
| `device` | `device info` | `profile`, `self_info`, `device_info` |
| `send` | `send` | `destination`, `channel`, `queued`, `ack_code`, `acknowledged`, `trip_time_ms` |
| `inbox` | `inbox` | `profile`, `messages`, `drained` |
| `network_discovery` | `network discover` | `profile`, `filter`, `scope`, `timeout_ms`, `nodes` |
| `history` | `history list` | `profile`, `enabled`, `storage`, `path`, `entries` |
| `configuration` | `config show` | `path`, `needs_migration`, `effective` |
| `hook_status` | `hooks status` | `protocol`, `enabled`, `configured`, hook failure policies |
| `mqtt_status` | `mqtt status` | `schema`, connection settings, authentication flags, `topic_prefix`, `allow_send`, `broker_state` |
| `batch_run` | `batch run` | `file`, `command_count`, `results` |
| `batch_contacts` | `batch contacts` | `profile`, `operation`, `dry_run`, `target_count`, `targets` |
| `event` | `watch` | `event`, `data` |

For example, a bounded watch session can be consumed one record at a time:

```console
$ meshquill --profile demo --output jsonl watch --count 2
{"schema":"meshquill.cli/v1","type":"event","data":{"event":"self_info","data":{...}}}
{"schema":"meshquill.cli/v1","type":"event","data":{"event":"connected","data":{...}}}
```

Fields shown as `{...}` above are abbreviated documentation, not literal output. Use the schema and
record type as the dispatch keys, and ignore unknown optional fields.

## Non-interactive operation

`--non-interactive` disables implicit terminal reads: Meshquill does not display confirmation,
password, destination-selection, or setup prompts. If a required value or explicit `--yes` is
missing, the command fails before the associated device mutation.

An option whose name explicitly requests stdin is different. In particular,
`mqtt configure --password-stdin` reads one bounded password from stdin even with
`--non-interactive`; this is an intentional data input, not a prompt. `batch run` always takes a
regular-file path, never `-` or stdin, so it cannot compete with an explicit password input.

Example failure contract:

```text
error: confirmation is required to reboot the local companion
hint: Review the operation and rerun it with --yes.
```

The text is designed for people and is not a machine schema. Automation should branch on the exit
status.

## Stable exit statuses

| Code | Name | Meaning |
| ---: | --- | --- |
| 0 | success | Requested operation completed. |
| 2 | usage | Invalid syntax, output shape, input, or missing non-interactive value. |
| 3 | configuration | Missing, malformed, unmigrated, or invalid configuration. |
| 4 | discovery | Discovery failed or found no requested device. |
| 5 | connection | Connect/disconnect failure or connection owned elsewhere. |
| 6 | protocol | Device error or malformed/unexpected frame. |
| 7 | timeout | Explicit deadline elapsed. |
| 8 | authentication | Login or credential lookup failed. |
| 9 | denied | Confirmation, policy, unsupported operation, or device refusal. |
| 10 | not found | Named profile, contact, channel, or target did not resolve. |
| 11 | hook | A configured hook failed under its selected policy. |
| 12 | MQTT | MQTT validation or gateway failure. |
| 130 | interrupted | User interruption such as Ctrl-C. |

## Command files

`meshquill batch run FILE` executes a bounded UTF-8 command file. Each non-empty line contains one
Meshquill command without the leading `meshquill`. A `#` outside quotes starts a comment. Single and
double quotes plus backslash escapes are supported; there is no variable expansion, command
substitution, globbing, redirection, pipeline, or arbitrary shell execution.

```text
# commands.mq
status
contacts --kind client
send "Alice Example" "scheduled hello"
```

The runner:

- accepts only a regular file of at most 262,144 bytes;
- accepts at most 1,000 commands and 4,096 bytes per line;
- rejects invalid UTF-8 and NUL bytes;
- ignores blank and comment-only lines;
- inherits the outer profile, config path, timeout, confirmation, and verbosity settings;
- forces each nested command to non-interactive, quiet, colour-free JSON mode;
- stops at the first failure and returns that command's exit status; and
- returns successful nested envelopes in the outer `batch_run.data.results` array together with
  their source line numbers.

Global options must appear on the outer invocation, not inside a command file. Nested `batch`,
`init`, configuration mutation, `mqtt configure`, `watch`, `chat`, `mqtt bridge`, streaming
`connect --watch`, completion generation, and manpage generation are rejected. `config show` is
allowed. Destructive commands still require explicit outer `--yes`; there is no implicit batch-wide
confirmation.

Filtered contact batching is a separate fixed-operation surface:

```console
meshquill --profile demo --output json batch contacts \
  --filter 'type=sensor,favorite=true' sensor-telemetry --dry-run
```

Supported operations are `remote-status`, `remote-owner`, `remote-regions`, `remote-clock`,
`sensor-telemetry`, `path-discover`, and `path-reset`. Review a `--dry-run` result before a large
operation; `path-reset` requires outer `--yes` when it is not a dry run.
