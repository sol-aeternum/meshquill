# Architecture

## Boundaries

Rust owns wire parsing, command sequencing, domain state, transport lifecycle and every ordinary CLI
path. PyO3 converts the same Rust models for `meshcore_sdk`. Python is launched only by the optional
SDK or an explicitly enabled hook. MQTT consumes/publishes application events and never becomes a
MeshCore radio transport.

```text
 CLI + line chat       async Python SDK       MQTT gateway       trusted hooks
        \                     |                    |                  /
         +------------- typed domain events and workflows -----------+
                                      |
                         serialized ManagedClient actor
                    pending request | ACK waiters | bounded events
                                      |
                    BLE raw packets / serial+TCP frames / mock
                                      |
                            MeshCore companion firmware
```

## Workspace ownership

| Crate | Owns |
| --- | --- |
| `meshquill-core` | Strict inner/outer codecs, domain types, response matching, managed request actor, ACK waits and explicit reconnect |
| `meshquill-transport` | BLE/serial/TCP targets, discovery and OS I/O; no command semantics |
| `meshquill-store` | Versioned profiles, pure platform-path resolution, atomic writes, secret references, hook/MQTT settings and opt-in history |
| `meshquill-cli` | Clap grammar, workflows, output/exit contracts, setup/doctor, portable line chat and integrations |
| `meshquill-python` | PyO3 conversions, async lifetime/stream adapters and Python packaging |
| `meshquill-hooks` | Fresh-process trusted-hook protocol, validation, limits, timeouts and failure policy |
| `meshquill-mqtt` | TLS/auth configuration, v1 topics/payloads, command allowlist, dedupe and broker backoff |
| `meshquill-test-support` | Deterministic companion, fixtures and fault injection; selected only through explicit demo/test configuration |

The store depends on hook/MQTT configuration types so one strict TOML schema can validate those
integrations. The CLI and Python crate compose several boundary crates. Neither relationship moves
protocol behavior out of the core.

## Requests, events and reconnect

Most companion requests lack a correlation ID, so the managed actor permits one ordinary pending
request and matches it against an operation-specific response set. Tagged binary, anonymous and
control operations also validate their protocol tags. Push events and ACKs remain concurrent.
Queues and broadcasts are bounded; a slow subscriber receives an explicit lag error rather than
causing unbounded memory growth.

Receive packets have no protocol-wide unique message ID. The core publishes every representable
message and never uses a payload fingerprint as identity. Each decoded message receives an
ephemeral, non-serialized client-local observation ID; the returned inbox value and its event-bus
clone carry the same ID. The CLI coalesces only that exact pair. Separately decoded packets always
receive distinct IDs, so identical direct or channel payloads remain distinct. Fresh UUIDv7 IDs
identify accepted local workflow/history observations. Reconnection clears the bounded correlation
state.

`SYNC_NEXT_MESSAGE` responses have no request tag. Live radio arrivals produce the distinct
`MESSAGES_WAITING` notification; each sync command then returns exactly one message or terminal
response. Before issuing that command, the core non-blockingly drains already-buffered asynchronous
notifications through the ordinary event path, with a fixed bound. A ready message or terminal
packet is conservatively reconciled as a late response to an earlier cancelled or timed-out sync,
without another write. The client records the outstanding response before awaiting the transport
write, publishes valid push packets while waiting, and blocks later commands until that one response
is consumed or reconnect clears the ambiguous session state.

Transport writes are never automatically replayed. A cancellation or disconnect after a write can
be ambiguous, and callers must reconcile before choosing another explicit send. Core reconnect is
one explicit transport reconnect followed by a fresh APP_START handshake. `watch` and line chat may
call that operation at most three times under a bounded CLI delay policy. They never pass a mutation
to the reconnect helper, and chat never auto-resends. A reconnectable failure before companion
acceptance was observed retains the exact unconfirmed text and original destination. `/send` starts
a new explicit send, which can duplicate delivery after a successful write whose response was lost;
`/discard` avoids that risk. An ACK-stage failure is never retained. MQTT broker reconnect is separate
and uses bounded exponential backoff.

Message history states are `pending`, `acknowledged`, `timed_out`, `failed` and `received`; there is
no invented protocol-level “unknown” delivery state. Direction and status are local bookkeeping, not
wire truth: `outgoing` is a local outgoing attempt, `pending` has no recorded terminal local result,
and `failed` records a local failure with a possibly ambiguous wire outcome. Each ID is a stable
local record ID, not protocol/event identity. Timestamps are local-host record time; sender
timestamp, route, SNR and signature are not retained.

## Untrusted-input policy

- Inner companion packets and serial/TCP frames are decoded separately.
- The firmware-derived logical companion-packet cap is 176 bytes and the independent defensive
  outer-frame cap is 300 bytes; the configuration input cap is one MiB, line input is 4096 bytes,
  operation timeouts are at most 24 hours, and command strings, paths, broker payloads, hook I/O and
  queue sizes have independent bounds.
- Lengths are checked before indexing/allocation; invalid UTF-8 returns a typed parse error and
  opaque bytes remain bytes.
- Unknown/future packets are bounded and observable instead of guessed into a typed model.
- Workspace Rust forbids `unsafe`; malformed-input tests reject truncation/oversize and four
  checked-in fuzz targets exercise inner packets, outer frames, remote payload families and MQTT
  command parsing. No property-test framework is claimed.

## Configuration, secrets and history

The v1 TOML path follows XDG on Linux, Application Support on macOS and AppData on Windows. The path
resolver is pure and cross-platform tested. Writes use a same-directory temporary file, flush/sync,
Unix `0600`/directory `0700` where supported, and atomic rename. Migration and repair retain a
timestamped backup; malformed configuration is never silently interpreted as current.

Secrets are stored as credential-store, environment or explicit prompt references. Effective config
and diagnostics expose only redacted status. MQTT/remote passwords are read from a secure terminal
or an explicit stdin option, never a password argv flag. See [configuration](configuration.md).

History is disabled by default. When enabled it stores bounded plaintext JSONL under the platform
application-data root, with a digest namespace for explicit config paths and bounded one-way
reconciliation from the previous config-adjacent location. Retention, permissions, contents and
deletion are documented in [messaging and chat](messaging-and-chat.md).

## Output and automation

Non-streaming successes use one `meshquill.cli/v1` human or JSON result. Streaming commands use
human records or one JSON object per line; JSON is rejected for streams and JSONL for single
results. Failures always use stable exit codes plus plain `error:`/`hint:` text on stderr—there is no
fictional JSON error envelope in v1. `--non-interactive` prevents implicit reads/prompts while
explicit inputs such as `--password-stdin` remain possible. Redirected output and diagnostics never
contain terminal-control sequences.

See [threat-model.md](threat-model.md), [automation](reference/automation.md) and the accepted ADRs
under [decisions](decisions/).
