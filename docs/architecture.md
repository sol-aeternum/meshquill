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

Transport writes are never automatically replayed. A cancellation or disconnect after a write can
be ambiguous, and callers must reconcile before choosing another explicit send. Core reconnect is
one explicit attempt followed by a fresh APP_START handshake. Line chat uses that attempt only after
a disconnected send, retains the draft, and requires `/send`; it does not retransmit. MQTT broker
reconnect is separate and uses bounded exponential backoff.

Message history states are `pending`, `acknowledged`, `timed_out`, `failed` and `received`; there is
no invented protocol-level “unknown” delivery state. Error text explains ambiguous-send cases.

## Untrusted-input policy

- Inner companion packets and serial/TCP frames are decoded separately.
- The outer payload cap is 300 bytes; command strings, files, paths, broker payloads, hook I/O and
  all queue sizes have independent bounds.
- Lengths are checked before indexing/allocation; invalid UTF-8 returns a typed parse error and
  opaque bytes remain bytes.
- Unknown/future packets are bounded and observable instead of guessed into a typed model.
- Workspace Rust forbids `unsafe`; malformed-input tests reject truncation/oversize and two checked-in
  fuzz targets exercise the highest-risk codecs. No property-test framework is claimed.

## Configuration, secrets and history

The v1 TOML path follows XDG on Linux, Application Support on macOS and AppData on Windows. The path
resolver is pure and cross-platform tested. Writes use a same-directory temporary file, flush/sync,
Unix `0600`/directory `0700` where supported, and atomic rename. Migration and repair retain a
timestamped backup; malformed configuration is never silently interpreted as current.

Secrets are stored as credential-store, environment or explicit prompt references. Effective config
and diagnostics expose only redacted status. MQTT/remote passwords are read from a secure terminal
or an explicit stdin option, never a password argv flag. See [configuration](configuration.md).

History is disabled by default. When enabled it stores bounded plaintext JSONL under
`history/<profile>.jsonl` beside the configuration. Retention, permissions, contents and deletion
are documented in [messaging and chat](messaging-and-chat.md).

## Output and automation

Non-streaming successes use one `meshquill.cli/v1` human or JSON result. Streaming commands use
human records or one JSON object per line; JSON is rejected for streams and JSONL for single
results. Failures always use stable exit codes plus plain `error:`/`hint:` text on stderr—there is no
fictional JSON error envelope in v1. `--non-interactive` prevents implicit reads/prompts while
explicit inputs such as `--password-stdin` remain possible. Redirected output and diagnostics never
contain terminal-control sequences.

See [threat-model.md](threat-model.md), [automation](reference/automation.md) and the accepted ADRs
under [decisions](decisions/).
