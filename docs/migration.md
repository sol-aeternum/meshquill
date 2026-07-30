# Migration

Meshquill has two deliberately narrow compatibility paths. They solve different problems:

| Command | Source | Result |
| --- | --- | --- |
| `meshquill config migrate` | A versionless early Meshquill TOML file | Converts supported profile selection into schema v1 and writes a backup |
| `meshquill config import-legacy [PATH]` | meshcore-cli's `default_address` file | Adds one BLE profile named `legacy`; imports nothing else |

Neither command imports device state from a radio. Neither is a general meshcore-cli settings,
contacts, channels, keys, or message-history converter.

Use `--config PATH` on every command when migrating a non-default destination.

## Back up and inspect first

Before changing a file, keep your own copy and inspect the effective destination:

```console
$ meshquill --config ./config.toml config show
```

`config migrate` creates its own same-directory backup when it changes a file. `import-legacy` saves
the destination atomically but does not create a destination backup, so make one yourself when
adding to an existing configuration.

## Upgrade a versionless early Meshquill file

Only the absence of an integer `version` selects legacy conversion. An explicit `version = 0` is
unsupported, as is every explicit version other than the current `version = 1`.

The recognized versionless shape is limited to `default_profile` and a `devices` table:

```toml
default_profile = "desk"

[devices.desk]
transport = "serial"
port = "/dev/ttyUSB0"
baud = 9600
```

Supported transport fields are:

| Legacy transport | Imported fields | Default when omitted |
| --- | --- | --- |
| `ble` | `id`, optional `name` | No default for `id` |
| `serial` | `port`, optional `baud` | `baud = 115200` |
| `tcp` | `host`, optional `port` | `port = 5000` |
| `mock` | optional `scenario` | Empty, which then fails current validation |

Conversion renames `devices` to `device_profiles`, maps only those transport fields, and sets every
profile's transport override and secret reference to none. It preserves `default_profile`; if that
is absent, it chooses the lexicographically first imported profile. With no profiles, no default is
set.

All timeout, history, hook, MQTT, and queue sections are reset to current defaults. The legacy
reader is permissive about unrelated fields, so unsupported old fields may be ignored rather than
diagnosed. Review the output instead of assuming everything was preserved.

Run the migration:

```console
$ meshquill --config ./config.toml config migrate
```

If conversion is needed, Meshquill first creates a same-directory backup named like
`config.toml.<timestamp>.<pid>.bak`, then atomically writes schema v1. If the file is already current,
the command reports no change and creates no backup.

Verify:

```console
$ meshquill --config ./config.toml config show
$ meshquill --config ./config.toml status
```

Profile-adding and other configuration mutation commands refuse a versionless destination until
this migration is complete.

## Import meshcore-cli's selected BLE address

With no path argument, Meshquill looks only at:

```text
$HOME/.config/meshcore/default_address
```

It falls back from `HOME` to `USERPROFILE` for the base, but still appends
`.config/meshcore/default_address` on every platform. It does not consult XDG, AppData, macOS
Application Support, or any other meshcore-cli file. Pass the exact file path when the selection is
elsewhere:

```console
$ meshquill config import-legacy /path/to/default_address
```

The source must be:

- a regular file no larger than 512 bytes;
- valid UTF-8;
- non-empty after surrounding whitespace is trimmed;
- at most 128 UTF-8 bytes after trimming; and
- free of control characters after trimming.

The importer does not validate canonical Bluetooth address syntax. Any bounded non-control string
is stored as the BLE selector and is tested only when a later connection attempts to use it.

The destination behavior is exact:

- add a profile with the fixed name `legacy`;
- set transport type `ble` and copy the trimmed file contents into `id`;
- set no BLE display name, transport override, or profile secret;
- make `legacy` the default only if the destination has no default already;
- refuse to overwrite an existing profile named `legacy` (denied exit 9); and
- refuse a destination that still needs its own Meshquill schema migration.

The source file is read, not changed. The importer does not copy serial/TCP selection, contacts,
channels, identity/private keys, PINs, radio settings, login passwords, contact policies, retry or
presentation preferences, init files, hooks, MQTT settings, or message history.

After import:

```console
$ meshquill config show
$ meshquill --profile legacy status
$ meshquill --profile legacy doctor --connect
```

The first two commands inspect local state. The last opens BLE and performs a real companion
handshake; this repository does not currently claim that workflow has passed on physical hardware.
See [troubleshooting](troubleshooting.md#ble) if it fails.

## Repair is not migration

`meshquill config repair` always requires confirmation (or global `--yes`), backs up an existing
file, and replaces the destination with a new default v1 configuration containing no profiles. It
does not salvage recognized fields. Use it only when that complete reset is intended:

```console
$ meshquill --config ./broken.toml --yes config repair
$ meshquill --config ./broken.toml --non-interactive init --name field --serial /dev/ttyACM0
```

For current paths, profile schema, and environment precedence, continue with
[configuration](configuration.md).
