# Companion protocol coverage

Baseline: MeshCore companion firmware v1.16.0, firmware main `03b6ef4b`, and
`meshcore_py` v2.3.8. “Implemented” means reviewed source exists and host tests exercise its bounds;
“CLI” means a user-facing route exists; “core only” means the typed Rust API is available but this
RC intentionally has no CLI command. No row is hardware-tested—see [hardware-testing.md](hardware-testing.md).

## Transports and framing

| Surface | State | Evidence/limit |
| --- | --- | --- |
| BLE Nordic-UART, raw inner packets | Implemented, host-tested | One complete write-with-response provider call (ATT long write permitted), without-response limited to `MTU - 3`, 176-byte firmware-frame cap, characteristic/timeout/no-replay tests; OS Bluetooth service required; no physical test. |
| USB serial app/device framing | Implemented, host-tested | `0x3c`/`0x3e` plus LE length, defensive 300-byte declared-frame decoder bound followed by the 176-byte firmware-packet cap, partial-frame/resync/malformed tests; libudev-backed fallible enumeration on Linux; no physical test. |
| TCP app/device framing | Implemented, host-tested | Same two-layer bounds and streaming codec, connect/read/write timeouts and loopback tests; no physical companion test. |
| Mock/virtual companion | Implemented, deterministic | Handshake, contacts, info, direct/channel send, queued receive, ACK/timeout, reconnect/no-replay, node discovery and fault injection. |

## Command codes

All “implemented” rows have a strict builder/parser and a serialized managed-client request. The
test column distinguishes full virtual-companion flows from builder/parser coverage.

| Code | Upstream operation | Meshquill exposure | Test state |
| ---: | --- | --- | --- |
| `0x01` | APP_START | connect/reconnect handshake, CLI/Python | mock end-to-end |
| `0x02` | SEND_TXT_MSG | `send`, chat, MQTT allowlist, Python | mock end-to-end + no-replay |
| `0x03` | SEND_CHANNEL_TXT_MSG | `send --channel`, MQTT, Python | mock end-to-end |
| `0x04` | GET_CONTACTS | `contacts`, resolution/filter/batch, Python | mock end-to-end |
| `0x05`/`0x06` | GET/SET_DEVICE_TIME | `device clock`, sync | codec/mock |
| `0x07` | SEND_SELF_ADVERT | `device advertise` | codec/mock |
| `0x08` | SET_ADVERT_NAME | typed Rust core only | codec |
| `0x09` | ADD_UPDATE_CONTACT | `contacts update`/path set | codec/mock |
| `0x0a` | SYNC_NEXT_MESSAGE | `inbox`, Python | mock end-to-end |
| `0x0b`/`0x0c` | SET_RADIO_PARAMS / TX_POWER | typed Rust core only; no regulatory-policy CLI | codec |
| `0x0d` | RESET_PATH | `contacts path reset` | process/mock |
| `0x0e` | SET_ADVERT_LATLON | typed Rust core only | codec |
| `0x0f` | REMOVE_CONTACT | `contacts forget` | process/mock |
| `0x10` | SHARE_CONTACT | typed Rust core only | codec |
| `0x11`/`0x12` | EXPORT/IMPORT_CONTACT | `contacts export/import` | process/mock |
| `0x13` | REBOOT | confirmed `device reboot` | codec/mock |
| `0x14` | GET_BATT_AND_STORAGE | `device telemetry` | codec/mock |
| `0x15` | SET_TUNING_PARAMS | typed Rust core only | codec |
| `0x16` | DEVICE_QUERY | `device info`/`device firmware`, Python | mock end-to-end |
| `0x17`/`0x18` | EXPORT/IMPORT_PRIVATE_KEY | typed Rust core only; deliberately absent from CLI | bounded secret/redaction tests |
| `0x19` | SEND_RAW_DATA | not exposed | enum retained; no safe normal-user workflow |
| `0x1a`–`0x1d` | LOGIN / STATUS / HAS_CONNECTION / LOGOUT | `remote login/status/logout` | process/mock, password redaction |
| `0x1e` | GET_CONTACT_BY_KEY | typed Rust core lookup | codec |
| `0x1f`/`0x20` | GET/SET_CHANNEL | `channels list/show/set/remove`, Python send | process/mock; 16-byte secret files |
| `0x21`–`0x23` | SIGN_START/DATA/FINISH | typed bounded Rust signing API | codec/multi-step tests |
| `0x24` | SEND_TRACE_PATH | not emitted | upstream enum retained; RC `network trace` uses supported path discovery (`0x34`) and says so |
| `0x25` | SET_DEVICE_PIN | typed Rust core only | bounds/codec |
| `0x26` | SET_OTHER_PARAMS | not exposed | ambiguous versioned payload; no guessed builder |
| `0x27` | SEND_TELEMETRY_REQ | typed core and Python `self_telemetry()` raw bytes | codec/mock + Python loopback |
| `0x28`/`0x29` | GET_CUSTOM_VARS / SET_CUSTOM_VAR | typed Rust core only | bounded parser/builder tests |
| `0x2a`/`0x2b` | GET_ADVERT_PATH / GET_TUNING_PARAMS | typed Rust core; contact path display uses synced metadata | codec |
| `0x2c`–`0x31` | reserved/current Python Wi-Fi slots | unsupported-versioned | never emitted |
| `0x32` | BINARY_REQ | remote status/neighbours and `sensor telemetry/summary/acl` | process/mock + bounded typed parsers |
| `0x33` | FACTORY_RESET | typed Rust core only; no RC CLI | explicit API, confirmation left to caller |
| `0x34` | PATH_DISCOVERY | `contacts path discover`, `network trace` | mock/process |
| `0x35` | unassigned | unsupported-versioned | never emitted |
| `0x36` | SET_FLOOD_SCOPE | temporary `--scope`, `network scope` | mock/process; restoration tested |
| `0x37` | SEND_CONTROL_DATA | `network discover` with kind/scope filters | correlated mock/process tests |
| `0x38` | GET_STATS | `device telemetry`, Python `telemetry(kind)` statistics | typed parser/mock |
| `0x39` | SEND_ANON_REQ | `remote regions/owner/clock` | process/mock |
| `0x3a`/`0x3b` | SET/GET_AUTOADD_CONFIG | typed Rust core only | codec |
| `0x3c` | GET_ALLOWED_REPEAT_FREQ | typed Rust core only | parser |
| `0x3d` | SET_PATH_HASH_MODE | typed Rust core only | bounds/codec |
| `0x3e` | SEND_CHANNEL_DATA | receive preserved; send unsupported | upstream/current-Python gap documented, no invented API |
| `0x3f`/`0x40` | SET/GET_DEFAULT_FLOOD_SCOPE | `network scope` and typed core | mock/codec |

## Responses and pushes

Typed decoding covers OK/error, contacts, self/device/channel info, message v1/v3, ACK tracking,
contact URI, battery/storage, time, signing, custom variables, tuning, stats, auto-add, default
scope, authentication, remote status, telemetry, binary/anonymous responses, path discovery and
control data. Python exposes raw local telemetry queries and unsolicited telemetry events as the
same immutable typed response, separately from statistics. Exact field bounds, invalid UTF-8,
truncated samples, unknown codes, 177-byte inner packets, and 301-byte declared outer frames have
unit tests. Four libFuzzer targets exercise two codecs (`protocol_packet` for inner
packets and `outer_frames` for transport framing) plus two remote/application parsers
(`remote_payloads` and `mqtt_commands`).

Advertisement/path-update/log/raw/trace/contact-deletion/channel-data packets whose current public
model would discard information remain bounded `UnknownPacket` events rather than being silently
misdecoded. Unknown events are redacted in human output and retain only bounded protocol metadata in
machine output. Proposed fragmentation push `0x91` is deliberately not accepted until its upstream
protocol is merged and version-negotiated.

## Known compatibility discrepancies

1. Companion docs describe a channel-send `MSG_SENT`; current Python expects `OK`. Meshquill accepts
   only the reviewed compatible response set and tests the observed result.
2. Official companion docs omit the serial/TCP outer envelope used by current libraries; the
   implementation follows pinned current source.
3. Codes `0x24`, `0x26`, `0x2c`–`0x31`, `0x35` and channel-data send `0x3e` are not guessed into a
   public workflow.
4. Multi-byte path prefixes are preserved with explicit hash width; no path is modeled as a list of
   single-byte hops.

Any hardware-derived change must record device, firmware, transport and captured-frame provenance
before this matrix gains a hardware-tested state.
