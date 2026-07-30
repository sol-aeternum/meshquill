# Messaging and chat

Messaging commands require a configured profile and a successful companion handshake. Start with
[getting started](getting-started.md) or inspect the selection with `meshquill status`.

## Find a destination

List or filter contacts:

```console
$ meshquill contacts
$ meshquill contacts --search alice
$ meshquill contacts --kind repeater
$ meshquill contacts --refresh
$ meshquill contacts show Alice
```

Commands resolve a direct destination by an exact, case-sensitive contact name or by a unique
hexadecimal public-key prefix. Duplicate exact names and ambiguous prefixes are rejected; use more
key characters to disambiguate.

The CLI grammar currently lists `contacts pending list`, `accept`, and `clear`, but the entire
pending-contact subtree is unsupported in this RC. Every pending command returns denied status
before reading configuration or accessing a device, and no device change is attempted.

## Send a direct message

```console
$ meshquill send Alice "Are you receiving this?"
$ meshquill send Alice "Please acknowledge" --wait
```

Without `--wait`, success means the companion accepted the send and returned tracking metadata; it
does not mean that the recipient read the message. `--wait` additionally waits for a matching direct
message acknowledgement. The acknowledgement deadline is the smaller of the firmware suggestion
and global `--timeout`.

The finite `send` command does not reconnect or retry after a transport failure. This avoids an
ambiguous write being transmitted twice; inspect the result and decide whether to send again.

Message limits are measured in UTF-8 bytes, not characters. Meshquill rejects empty or oversized
payloads and never splits one input into multiple radio messages. A transport's negotiated payload
limit can be lower than the protocol-wide bound.

### Temporary flood scope

Apply a scope for one command with `--scope`:

```console
$ meshquill send Alice "Scoped message" --scope '#field-team'
$ meshquill send Alice "Use the firmware default" --scope default
$ meshquill send Alice "Explicitly unscoped" --scope unscoped
```

Named scopes may be supplied with or without the leading `#`; after normalization they are at most
30 UTF-8 bytes and contain no control characters. `default` (also `0` or `none`) restores the
firmware default, while `unscoped` (also `*`) is explicit. A temporary scope is reset to the default
after the command, including normal error cleanup where possible.

## Send to a channel

Current runtime channel sending accepts only a numeric channel index, despite the broader
destination wording in command help:

```console
$ meshquill send 2 "Field team check-in" --channel
```

Named-channel resolution is not implemented for `send` or `chat`. `--wait` and `--channel` are
mutually invalid because this ACK wait is direct-message-only.

## Read the queued inbox

Drain messages until the companion reports the queue empty:

```console
$ meshquill inbox
```

Stop after a bounded number instead:

```console
$ meshquill inbox --limit 10
```

In JSON output, `drained` is `true` only when the companion's empty marker was reached. It is
normally `false` when `--limit` stopped the loop first.

## Watch live events

`watch` is a stream. Human output is the default; automation must use JSONL:

```console
$ meshquill watch
$ meshquill --output jsonl watch --event message --event ack
$ meshquill --output jsonl watch --event connection --count 5
```

Filters may be repeated and include `message`, `ack`, `contact`, `connection`, `telemetry`, and
`error`. With no filters, all public events are emitted. `--count` stops after that many matching
events. A lagging consumer receives a diagnostic that bounded events were skipped.

## Use line chat

This RC has no full-screen TUI. `chat`, with or without `--line`, runs the same portable
line-oriented loop; omitting `--line` only emits a diagnostic explaining the choice.

```console
$ meshquill chat Alice --line
$ meshquill chat 2 --line
```

A numeric destination is treated as a channel index. Every other destination is resolved as a
direct contact. With an interactive terminal, omitting the destination prompts on stderr. Piped or
`--non-interactive` input must name the destination:

```console
$ printf 'hello\n/quit\n' | meshquill --output jsonl chat Alice --line
```

The line commands are deliberately small:

- `/quit` exits.
- `/send` submits a draft retained after a reconnectable failure before companion acceptance and
  otherwise does nothing.
- A blank line is ignored.
- Every other line is message text; there are no TUI navigation commands or in-process shell pipes.

Before reading each input line, chat drains the companion's queued incoming messages. Incoming
display is not filtered to the selected destination. Input reading itself is blocking, so a newly
queued message is displayed before the next line rather than concurrently while the process waits
for typing.

Direct chat reports `sent` after companion acceptance, then waits once for the matching delivery
ACK and reports `acknowledged` or `timed_out`. The deadline is the smaller of the firmware
suggestion and global `--timeout`. An ACK timeout keeps the chat loop alive without retransmitting
the message. Numeric channel chat has no direct ACK tracking and reports only `sent`.

### One-shot reconnect without automatic resend

Reconnect handling is limited to a reconnectable failure while chat is attempting an outbound
send or waiting for its ACK. For a failure before companion acceptance, Meshquill:

1. marks that outgoing attempt failed in enabled history;
2. records the workflow disconnect;
3. attempts one reconnect and fresh handshake;
4. retains the unsent text if reconnect succeeds; and
5. requires `/send` before transmitting the retained draft.

If a reconnectable ACK wait fails after `sent`, Meshquill marks the tracking attempt failed and
attempts the same single reconnect, but does not retain or retransmit the already-sent message.
There is no retry/backoff loop and no automatic retransmission, including after cancellation. If
the single reconnect fails, chat exits. Failures while polling incoming messages do not use this
outbound-send reconnect path.

## Plaintext local history

Message persistence is opt-in and disabled by default. Enable it in TOML only if storing message
content in plaintext is acceptable:

```toml
[history]
enabled = true
max_messages = 256
```

The equivalent temporary overrides are `MESHQUILL_HISTORY_ENABLED=true` and
`MESHQUILL_HISTORY_MAX_MESSAGES=256`. When enabled, Meshquill writes
`history/<profile>.jsonl` beside the selected configuration. Each versioned record can contain the
peer/source label, channel, full message text, direction, timestamp, status, and acknowledgement
correlation. Statuses include `pending`, `acknowledged`, `timed_out`, `failed`, and `received`.

`send`, `chat`, `inbox`, and incoming `watch` messages use this history workflow. A finite send
without an acknowledgement wait and a channel chat acceptance remain `pending`; direct
`send --wait` and direct chat can transition to `acknowledged` or `timed_out`.

Inspect the newest retained entries or delete the selected profile's file:

```console
$ meshquill history list --limit 20
$ meshquill --yes history clear
```

`history list` can still read an existing file after persistence has been disabled. Disabling does
not erase prior data. On Unix, Meshquill uses restrictive directory/file modes where supported, but
history is not encrypted. See [configuration](configuration.md#opt-in-to-plaintext-message-history).

## Script safely

Finite message commands use `--output json`; `watch` and `chat` use `--output jsonl`. Add
`--non-interactive` when all input is explicit. Stable envelopes, stderr behavior, and exit statuses
are defined in the [automation reference](reference/automation.md).
