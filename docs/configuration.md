# Configuration

Meshquill stores all device profiles in one strict, versioned TOML file. `meshquill config show`
prints the effective non-secret configuration, including recognized environment overrides, and
redacts secret references.

## Locate or override the file

The default `config.toml` path is:

| Platform | Path |
| --- | --- |
| Linux | `$XDG_CONFIG_HOME/meshquill/config.toml`, or `$HOME/.config/meshquill/config.toml` |
| macOS | `$HOME/Library/Application Support/meshquill/config.toml` |
| Windows | `%APPDATA%\meshquill\config.toml`, falling back to `%LOCALAPPDATA%` |

Select another file with `--config PATH` or `MESHQUILL_CONFIG`. An explicit command-line value wins
over the environment-bound value. Overrides do not create a missing file; initialize it first:

```console
$ meshquill --config ./field.toml --non-interactive init --name field --serial /dev/ttyACM0
$ meshquill --config ./field.toml config show
```

Each configuration or history write uses a temporary file in its own destination directory followed
by an atomic replace. On Unix, newly created directories are mode `0700` and files/backups are mode
`0600`. Stable private `.lock` sidecars serialize each complete read/modify/write transaction across
processes; a process that cannot acquire the lock within ten seconds exits with configuration status
instead of overwriting a concurrent update. Do not treat those permissions as encryption.

Platform directory environment variables must resolve to nonempty absolute paths. Empty or relative
values are rejected rather than turning the current working directory into implicit persistent
storage. On Windows, configuration uses `%LOCALAPPDATA%` when `%APPDATA%` is unavailable.

## Locate or override application data

History and future non-configuration state use a separate application-data root:

| Platform | Default data root |
| --- | --- |
| Linux | `$XDG_DATA_HOME/meshquill`, or `$HOME/.local/share/meshquill` |
| macOS | `$HOME/Library/Application Support/meshquill` |
| Windows | `%LOCALAPPDATA%\meshquill`, falling back to `%APPDATA%\meshquill` |

`--data-dir PATH` or `MESHQUILL_DATA_DIR` selects the exact data root without changing the
configuration path. A relative explicit data root is resolved to an absolute path at process start;
use the same stable override on every invocation.

## Select a profile

Profile selection uses this order:

1. `--profile NAME`, or its `MESHQUILL_PROFILE` environment binding.
2. The effective `default_profile`, which `MESHQUILL_DEFAULT_PROFILE` may override temporarily.
3. The sole stored profile, when exactly one exists.

With multiple profiles and no default, a device command fails before connecting; pass `--profile`
or persist a choice. `init` never overwrites a profile and makes the new profile the default whenever
no persisted default exists. `--set-default` explicitly replaces an existing default.

Manage stored profiles without editing TOML:

```console
$ meshquill profiles list
$ meshquill profiles reconfigure field --serial /dev/ttyACM0
$ meshquill --yes profiles rename field field_unit
$ meshquill --yes profiles delete field_unit
$ meshquill profiles set-default gateway
```

`reconfigure` replaces only the transport, preserving that profile's timeout override and secret
reference. Rename requires confirmation, updates a matching default and migrates retained history;
it refuses to merge an existing destination history, which must first be inspected and explicitly
cleared. External remote-credential hashes are identity-keyed and cannot be renamed automatically. Delete
also requires confirmation and clears a matching default, but intentionally retains history and
external credentials for recovery. Profile mutations require a current v1 configuration; migrate a
versionless file first. `profile`, `edit`, and `remove` are convenience aliases for the canonical
plural commands.

## Create profiles

Interactive setup prompts for a profile name and one transport:

```console
$ meshquill init
```

Choosing BLE or serial runs bounded five-second discovery, displays sorted reusable candidates, and
accepts either a candidate number or a manual target. Discovery failure or an empty result falls
back to manual entry rather than silently selecting a device. TCP remains explicit because the CLI
does not scan the network for endpoints.

Non-interactive setup requires `--name` and exactly one of `--ble`, `--serial`, `--tcp`, or
`--demo`:

```console
$ meshquill --non-interactive init --name field_ble --ble 'ble:PLATFORM-ID'
$ meshquill --non-interactive init --name field_serial --serial /dev/ttyACM0
$ meshquill --non-interactive init --name gateway --tcp mesh.example:5000
$ meshquill --non-interactive init --name demo --demo
```

Names are at most 64 ASCII bytes, start with an ASCII letter or `_`, and contain only ASCII letters,
digits, `_`, and `-`. A leading `-` is rejected so a stored name cannot be confused with a
command-line option.

## Minimal schema

The active schema is `version = 1`. A minimal serial configuration is:

```toml
version = 1
default_profile = "field"

[device_profiles.field.transport]
type = "serial"
port = "/dev/ttyACM0"
baud = 115200
```

Transport tables have these forms:

```toml
[device_profiles.ble_node.transport]
type = "ble"
id = "ble:PLATFORM-ID"
name = "optional display name"

[device_profiles.serial_node.transport]
type = "serial"
port = "/dev/ttyUSB0"
baud = 115200

[device_profiles.tcp_node.transport]
type = "tcp"
host = "192.0.2.10"
port = 5000

[device_profiles.demo.transport]
type = "mock"
scenario = "demo"
```

The CLI runtime accepts the mock scenarios `demo`, `ack-timeout`, `reconnect-demo`,
`reconnect-fail`, and `send-disconnect`. The latter four are deterministic fault fixtures for
timeout, bounded reconnect, exhausted reconnect, and send recovery tests. `send-disconnect` is a
deterministic pre-write, known-unsent fixture; it does not cover ambiguous after-write response loss.
Mock profiles are explicit test/demo configuration; discovery never silently substitutes one for
failed hardware.

## Timeouts

Defaults are:

```toml
[timeout]
connect_timeout_ms = 5000
request_timeout_ms = 3000
retry_timeout_ms = 1000
```

The connection timeout bounds transport setup. The request timeout bounds ordinary companion
request/response work and can be overridden for one profile:

```toml
[device_profiles.field.transport_overrides]
request_timeout_ms = 5000
```

That profile value wins over the global request timeout. For `watch` and line chat,
`retry_timeout_ms` is the delay before the second reconnect attempt; the third delay is twice that
value, and both are capped by `connect_timeout_ms`. The first of at most three attempts is immediate.
This policy reconnects only the companion session and never replays a mutation. The global CLI
`--timeout` is a per-command deadline used by operations such as discovery and acknowledgement
waits; it does not rewrite these values.

All stored timeout values must be positive and no greater than 24 hours (86,400,000 milliseconds).
The same 24-hour ceiling applies to CLI duration arguments, transport setup, Rust client request
timeouts, hook deadlines, and Python SDK timeout values. The configuration file itself is read with
a one-MiB hard bound before TOML parsing.

## Opt in to plaintext message history

History is off by default:

```toml
[history]
enabled = false
max_messages = 256
```

To retain full message text locally, set `enabled = true`. `max_messages` must be from 1 through
100000. With the platform-default configuration, entries are plaintext JSONL at
`<data-root>/history/<profile>.jsonl`. Supplying the exact platform-default configuration path
explicitly uses that same namespace. Any other `--config` or `MESHQUILL_CONFIG` path uses
`<data-root>/history/<config-path-sha256>/<profile>.jsonl`, preventing unrelated configuration files
from colliding. The digest is a namespace, not a security boundary. `history list --output json`
reports the resolved canonical path.

On first access, Meshquill reconciles an older config-adjacent history file with the canonical file
under independent bounds, lets the canonical record win duplicate local UUIDs, durably writes the
merged result, and only then removes the legacy file. Profile rename migrates history even when
persistence is disabled; profile deletion intentionally retains it. Disabling history stops new
persistence but does not delete an existing file; inspect or remove both canonical and legacy state
with:

```console
$ meshquill --profile field history list --limit 20
$ meshquill --profile field --yes history clear
```

Lowering `max_messages` immediately retains and rewrites only the newest permitted records on the
next read. History reads have independent global line-size and entry-count bounds, so a previously
larger file cannot force unbounded allocation.

See [messaging and chat](messaging-and-chat.md#plaintext-local-history) for the recorded fields and
status behavior.

## Environment overrides

These variables override parsed TOML for the current process:

| Area | Variables |
| --- | --- |
| Selection | `MESHQUILL_DEFAULT_PROFILE` |
| Timeouts | `MESHQUILL_TIMEOUT_CONNECT_MS`, `MESHQUILL_TIMEOUT_REQUEST_MS`, `MESHQUILL_TIMEOUT_RETRY_MS` |
| History | `MESHQUILL_HISTORY_ENABLED`, `MESHQUILL_HISTORY_MAX_MESSAGES` |
| Hooks | `MESHQUILL_HOOK_ENABLED`, `MESHQUILL_HOOK_SCRIPT` |
| MQTT | `MESHQUILL_MQTT_ENABLED`, `MESHQUILL_MQTT_BROKER`, `MESHQUILL_MQTT_PORT`, `MESHQUILL_MQTT_TOPIC_PREFIX` |
| Queues | `MESHQUILL_QUEUES_INBOUND`, `MESHQUILL_QUEUES_OUTBOUND`, `MESHQUILL_QUEUES_EVENT` |

Boolean overrides accept case-insensitive `1`, `true`, `yes`, or `on`, and `0`, `false`, `no`, or
`off`. Overrides are applied after parsing and before validation, so an invalid effective value
fails the command. Configuration-writing commands load the on-disk values without transient
overrides, preventing an environment value from being written accidentally.

`MESHQUILL_CONFIG`, `MESHQUILL_DATA_DIR`, and `MESHQUILL_PROFILE` are global CLI bindings rather
than TOML field overrides.

## Hooks, MQTT, queues, and secrets

The v1 schema also contains `hook`, `mqtt`, and `queues` sections. Hooks and MQTT are disabled by
default; outbound MQTT sends are separately disabled by default. Use `meshquill mqtt configure` for
broker settings. `--password-stdin` stores a supplied password in the operating-system credential
store; `--password-env NAME` stores only a validated environment-variable reference for later
processes. Neither writes the password to TOML or argv. Use `meshquill hooks validate` after
manually enabling a trusted local hook.

Profile and MQTT secret fields store references (`credential_store`, `environment`, or `prompt`),
not an intended place for plaintext secrets. `config show` reports only redacted secret status.
Remote-login credentials are managed separately by `remote login --save`; see
[remote administration](remote-administration.md#log-in-without-exposing-the-password).

Queue sizes are validated from 1 through 1000000 and are shown in effective configuration. They are
an advanced schema surface; do not assume changing them repairs a transport, firmware, or event
consumer problem.

## Validate, migrate, or repair

After a manual edit, run:

```console
$ meshquill config show
$ meshquill doctor
```

Current v1 tables reject unknown fields. A versionless early Meshquill file needs
`meshquill config migrate`, which creates a backup. `config repair` is not a field-by-field repair:
after confirmation it backs up an existing file and replaces it with empty safe defaults, including
no profiles. Read [migration](migration.md) before either operation.
