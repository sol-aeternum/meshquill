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

Once the companion accepts a send, a later fail-closed `after_send`/`on_ack` hook or cleanup failure
does not erase that fact. Machine output writes the authoritative `send` result first with
`queued: true`, then the process exits with the secondary failure and a “do not retry
automatically” hint. This ordering prevents automation from treating an accepted radio write as a
known-unsent operation.

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

MeshCore receive packets do not provide a globally unique message identifier. Meshquill assigns an
ephemeral, non-serialized observation ID to each decoded packet and coalesces only the returned inbox
value with its exact event-bus clone. Separately decoded occurrences always receive different IDs,
so identical direct or channel messages remain distinct. This is local delivery bookkeeping, not a
durable MeshCore message or retransmission identity; reconnecting clears the bounded correlation
state. Live radio arrivals produce a `MESSAGES_WAITING` notification; one `SYNC_NEXT_MESSAGE` then
returns exactly one message or terminal response. Before each inbox sync, already-buffered
asynchronous notifications are drained through the ordinary event path. A ready message or terminal
packet is conservatively reconciled as a late response to an earlier cancelled or timed-out sync,
without another write. The core records the outstanding response before awaiting the transport
write, publishes valid push packets while waiting, and blocks later commands until the response is
consumed or reconnect clears the ambiguous session state.

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

After a device disconnect event, `watch` re-establishes only the companion session. It makes at
most three reconnect attempts: one immediate attempt, then delays of `retry_timeout_ms` and twice
that value, each capped by `connect_timeout_ms`. It never replays a command or radio send. Exhausted
or unsupported reconnects end the stream with connection status.

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

The line commands are:

- `/help` shows the commands and the current target.
- `/contacts [query]` lists contacts, optionally filtering names case-insensitively.
- `/to <contact>` switches to an exact contact name or unique key prefix.
- `/channel <0..255>` switches to a numeric channel.
- `/history [N]` shows up to 100 newest retained entries for the current conversation; history must
  already be enabled explicitly.
- `/send` starts a new explicit send of the exact text and original destination retained after a
  reconnectable failure before companion acceptance was observed. It can duplicate delivery if the
  original write succeeded but its response was lost.
- `/discard` drops that retained message without transmitting it, avoiding that duplicate-send risk.
- `/quit` exits, and `//text` sends message text beginning with `/`.

Blank lines are ignored. Unknown or malformed commands produce a structured `command_error` and a
close-command suggestion where one is safe; they are never sent as message text. Each input line is
bounded to 4096 UTF-8 bytes before allocation. The actual device text limit is usually lower and is
validated separately.

Chat subscribes before connecting, drains the queued inbox while applying the bounded live/queued
occurrence correlation described above, then keeps the event stream active while a dedicated
bounded reader waits for keyboard or piped input. Incoming messages are therefore displayed while
the user is typing. Incoming display is not filtered to the selected destination, but `/history` is
conversation-filtered.

Direct chat reports `sent` after companion acceptance, then waits once for the matching delivery
ACK and reports `acknowledged` or `timed_out`. The deadline is the smaller of the firmware
suggestion and global `--timeout`. An ACK timeout keeps the chat loop alive without retransmitting
the message. Numeric channel chat has no direct ACK tracking and reports only `sent`.

As with finite send, fail-closed `after_send` and `on_ack` failures are deferred until chat has
emitted the accepted `sent` and, when observed, `acknowledged` state. Chat then exits rather than
continuing or retrying the accepted message.

### Bounded reconnect without automatic resend

Chat never automatically resends. It uses the same three-attempt companion reconnect policy as
`watch` after a disconnect event, a reconnectable failure while attempting an outbound send, or a
reconnectable ACK wait failure. It re-establishes only the session and never invokes the failed
mutation. For a failure before companion acceptance was observed, Meshquill:

1. marks that outgoing attempt failed in enabled history;
2. records the workflow disconnect;
3. makes up to three bounded reconnect attempts and a fresh handshake;
4. retains the exact unconfirmed text and its original destination if reconnect succeeds; and
5. blocks new message text until `/send` starts a new explicit send or `/discard` drops the retained
   message. Switching the visible target does not retarget it.

The original wire outcome can be ambiguous: `/send` can duplicate delivery if the write succeeded
but the companion response was lost. `/discard` avoids that risk.

If a reconnectable ACK wait fails after `sent`, Meshquill marks the tracking attempt failed and
attempts the same bounded reconnect, but does not retain or retransmit the already-sent message.
Cancellation never retransmits. If all attempts fail, the transport does not support reconnect, or
the managed actor has stopped, chat exits rather than spinning.

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
`history/<profile>.jsonl` under the platform application-data root. Explicit config files receive a
config-path digest namespace; `--data-dir`/`MESHQUILL_DATA_DIR` overrides the root. Each versioned
record can contain the peer/source label, channel, full message text, local direction and status,
local-host record time, and acknowledgement correlation. Incoming records do not retain the sender
timestamp, route, SNR, or signature. The stable `id` identifies only that local history record, not
a MeshCore protocol message or event. See [configuration](configuration.md#locate-or-override-application-data)
for exact platform paths and legacy reconciliation.

Directions and statuses are local bookkeeping, not wire truth. `outgoing` means a local outgoing
attempt, `pending` means no terminal local result was recorded, and `failed` means a local failure
whose wire outcome may be ambiguous. Other statuses are `acknowledged`, `timed_out`, and `received`.

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
