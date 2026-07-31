# Existing-client capability accounting

Baseline inspected: `meshcore-cli` main `56b246b4` and `meshcore_py` v2.3.8. Every notable legacy
surface is classified as implemented, superseded, core-only, or explicitly unsupported in this RC.
“Host-tested” never means physical radio testing.

## Connections, messaging and contacts

| Existing capability | Meshquill disposition | State/limit |
| --- | --- | --- |
| BLE/serial/TCP selection and discovery | `devices`, named profiles, `init`, explicit constructors in Python | implemented/host-tested; no physical test |
| Connection timeout, debug and colour | global `--timeout`, `-v/-vv/-vvv`, `--quiet`, terminal-safe `--color` | implemented |
| Info/version/battery/stats/time | `device info/firmware/telemetry/clock` | implemented |
| Raw local sensor telemetry | typed Rust core and Python `self_telemetry()`/`Event.telemetry`; distinct from device statistics | implemented/host-tested; no physical test |
| Direct and channel/public text | `send`; numeric channel with `--channel`; Python and MQTT allowlist | implemented; named channel send not supported |
| ACK wait and timeout | `send --wait`, `watch`, history state, Python receipt/wait | implemented |
| Receive/sync/subscribe | `inbox`, `watch`, Python async streams | implemented |
| Channel CRUD | `channels list/show/set/remove`; secret accepted only as a 16-byte file | implemented |
| Scope/default scope | `default`, `unscoped` and named `#scope` states; temporary scope restored | implemented |
| Contact list/search/type/info | `contacts --search/--kind`, `show`, `--refresh` | implemented |
| Contact rename/flags/path/remove | `contacts update/forget/path show/discover/reset/set` | implemented |
| Contact URI export/import | `contacts export/import` | implemented |
| Share-contact packet | typed Rust core only | core-only; no normal-user CLI need established |
| Pending/manual contacts | command subtree returns a stable unsupported diagnostic before device access | unsupported: one-shot CLI lacks the advert cache required for safe acceptance |
| Contact-specific timeout | profile/global request timeout, not per-contact persistence | superseded by coherent bounded operation timeouts |
| Channel echo/RX-log reconstruction | live message events with route/SNR metadata | superseded; no fabricated history from diagnostic logs |

## Remote, sensor and network administration

| Existing capability | Meshquill disposition | State/limit |
| --- | --- | --- |
| Login/logout/password forget | `remote login/logout/credentials-forget`; secure prompt/stdin/OS credential store | implemented |
| Remote CommonCLI | `remote run CONTACT COMMAND` with explicit local-vs-remote wording | implemented |
| Repeater/room status, neighbours, regions, owner, clock | typed `remote` subcommands | implemented for companion binary/anonymous APIs |
| Sensor telemetry/MMA/ACL | `sensor telemetry/summary/acl` | implemented |
| Node discovery | `network discover` with kind and scope filter | implemented/correlated control responses |
| Trace | `network trace` performs supported path discovery (`0x34`) | supersedes unverified legacy trace packet `0x24` |
| Batch `apply_to` | `batch contacts --filter ... OP`, dry-run, full-key targeting, one destructive confirmation | implemented for status/owner/regions/clock, sensor queries, path discover/reset |
| Factory reset/private-key/PIN/radio writes | typed Rust core where protocol is stable | core-only; deliberately absent from general CLI pending stronger policy/hardware evidence |
| Serial repeater console and region file upload/download | `remote run` handles explicit CommonCLI text; no raw serial console/file protocol | unsupported in RC; no silent claim of parity |
| Firmware bridge settings | explicit `remote run`; MQTT remains a separate application gateway | superseded for stable typed reads; obscure firmware mutations remain explicit raw remote commands |

## Shell, interaction and automation

| Existing capability | Meshquill disposition | State/limit |
| --- | --- | --- |
| Command files/chaining | `batch run FILE`, one shell-like command per line, no expansion/pipes, bounded/fail-fast | implemented; arbitrary chained positional grammar superseded |
| In-process aliases/echo | ordinary shell aliases and command files | deliberately superseded; no embedded shell |
| Per-command `.` JSON prefix | global `--output json`; streams require JSONL | superseded with versioned schema |
| Interactive contextual chat | portable `chat DESTINATION --line`, visible destination and live incoming display; `/contacts`, `/to`, `/channel`, conversation-filtered `/history`, and destination-bound retained-draft `/send` and `/discard` | implemented line mode; no full-screen TUI |
| Interactive `>`, `>>`, `|` | normal OS shell composition | deliberately superseded |
| Startup `.init` files | explicit profiles, config and bounded batch files | superseded; no hidden commands run at startup |
| Presentation preferences (`classic_prompt`, arrows/slashes, print/echo toggles) | stable human/JSON/JSONL contracts and TTY detection | superseded; legacy importer does not pretend to migrate them |
| Completions/man pages | generated Bash/Zsh/Fish/PowerShell completions and command-tree man pages | implemented and packaged |
| Doctor/setup | guided `init`, `devices`, `connect`, `doctor`, effective config/migrate/repair | implemented |

## Parameter-level `get`/`set` families

The legacy CLI hides local packets, UI preferences and remote CommonCLI behind the same words.
Meshquill keeps the trust boundary explicit:

| Legacy family | Disposition |
| --- | --- |
| Local radio/tx/coordinates/name/tuning/path-hash/auto-add/custom vars | strict Rust core APIs; high-risk writes are core-only in this RC |
| Battery/stats/status/repeat frequencies | typed device/core reads; common user reads exposed through `device telemetry`; Python `telemetry()` is the statistics query |
| Raw local sensor telemetry | typed core and Python `self_telemetry()` bytes; intentionally distinct from statistics |
| Private key/signing/PIN | bounded/redacted Rust core only; never ordinary argv |
| Telemetry modes, advert policy, multi-ACK/default scope | parsed self-info plus typed scope APIs; no unsafe generic local setter |
| Retry/prompt/display/echo preferences | profile timeouts and output contracts supersede legacy UI knobs |
| Remote `role`, `repeat`, ACL, radio, duty-cycle, owner/password, advert/flood and power settings | explicit `remote run`; typed status/regions/owner/clock where stable |
| Sensor raw `get`/`set` | typed read-only sensor queries; mutations require explicit `remote run` |
| RS-232/ESP-NOW bridge fields | explicit remote firmware CLI only; never confused with MQTT |
| Bootloader/power-management reads | explicit `remote run` |

Unknown local `get`/`set` text is never reinterpreted as a custom variable. `multi_ack` versus
`multi_acks` drift is documented rather than supported through a fictitious migration diagnostic.

## Python parity

`meshcore_sdk` shares the Rust managed client and exposes auto/BLE/serial/TCP/demo connections,
discovery, contacts, direct/channel sends, queued messages, ACK waits, device info/statistics, raw
self-telemetry queries and events, bounded event/message streams, reconnect, shutdown and async
context management. The SDK does not
expose private-key/signing and every obscure remote mutation merely to mirror old command classes;
those remain core-only until a stable safe Python surface is designed.

See [protocol coverage](protocol-coverage.md) for exact wire codes and [migration](migration.md) for
the intentionally narrow legacy configuration importer.
