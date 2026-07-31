# Getting started

Meshquill `0.1.0-rc.3` is a pre-release command-line client in the current source checkout. Until
[live status](https://github.com/sol-aeternum/meshquill/blob/main/STATUS.md) records a published
`v0.1.0-rc.3` prerelease, its GitHub tag,
checksummed assets and tagged-install commands are future delivery paths, and no crates.io or PyPI
package is assumed available. Build the current checkout below; [installation](installation.md)
also documents the conditional release-asset flow. Physical BLE and serial verification has not
yet been run; the deterministic demo is software-only. See the
[hardware matrix](hardware-testing.md) for the recorded evidence.

## Build the CLI from this checkout

Install the Rust toolchain selected by `rust-toolchain.toml`, then run:

```console
$ cargo build --locked --release -p meshquill
$ ./target/release/meshquill --version
```

Either put `target/release` on your command path, install this checkout with
`cargo install --locked --path crates/meshquill-cli`, or substitute
`./target/release/meshquill` for `meshquill` below. A Linux source build also needs the D-Bus and
libudev development files used by the BLE and serial backends; see
[platform troubleshooting](troubleshooting.md#source-build-fails-on-linux).

## Run the deterministic demo

A demo profile is configuration, not a built-in implicit profile. Create it before using
`--profile demo`:

```console
$ meshquill --non-interactive init --name demo --demo --set-default
$ meshquill --profile demo connect
$ meshquill --profile demo devices --transport mock
$ meshquill --profile demo contacts
$ meshquill --profile demo send Alice "Hello from the demo" --wait
$ meshquill --profile demo inbox --limit 1
```

The fixture contains the contact `Alice`, one queued incoming message, and a deterministic direct
message acknowledgement. Each command constructs a fresh in-memory companion, so the fixture is
reset between invocations. Passing `--demo` never scans or opens physical hardware.

To exercise the full diagnostic exchange against that software-only fixture, run:

```console
$ meshquill --profile demo doctor --connect
```

The command reports separate `handshake` and `firmware_compatibility` checks after `APP_START` and
`DEVICE_QUERY`, then closes its single connection. The demo's known protocol level is deterministic;
this result is not physical-device evidence.

To use a disposable configuration instead of your normal platform path, keep the same `--config`
value on every command:

```console
$ meshquill --config ./demo-config.toml --non-interactive init --name demo --demo --set-default
$ meshquill --config ./demo-config.toml --profile demo contacts
```

`init` never overwrites an existing profile. A new profile becomes the default automatically when
none is persisted; `--set-default` explicitly replaces an existing default. With exactly one stored
profile, commands select it even without a default. Use `meshquill profiles list` and
`meshquill profiles set-default NAME` to inspect or persist selection.

## Try line chat

The current RC has a portable line interface only; it has no full-screen TUI. `--line` states that
choice explicitly:

```console
$ meshquill --profile demo chat Alice --line
```

The demo's queued incoming message is displayed before the first input line. Type `hello` to send,
then `/quit` to exit. Other slash-prefixed lines are chat commands; ordinary non-empty lines are
message text. See
[messaging and chat](messaging-and-chat.md) for channel targets, concurrent incoming events, chat
commands, and bounded reconnect behavior.

## Create a physical or TCP profile

Discovery reports reusable target data, but it does not create a profile or prove that a device can
complete the companion handshake.

For BLE, scan and copy the returned `target.selector` from JSON output:

```console
$ meshquill --output json devices --transport ble --scan-timeout 8s
$ meshquill --non-interactive init --name field_ble --ble 'SELECTOR' --set-default
$ meshquill --profile field_ble doctor --connect
```

For serial, copy `target.port`, not the transport-qualified discovery record ID:

```console
$ meshquill --output json devices --transport serial
$ meshquill --non-interactive init --name field_serial --serial /dev/ttyACM0 --set-default
$ meshquill --profile field_serial doctor --connect
```

On Windows the serial value is a COM name such as `COM7`. Serial profiles created by `init` use
115200 baud; edit the profile in TOML if the device requires another non-zero rate.

TCP is configured manually because Meshquill does not scan the network for endpoints:

```console
$ meshquill --non-interactive init --name gateway --tcp 192.0.2.10:5000 --set-default
$ meshquill --profile gateway doctor --connect
```

The project currently records no successful physical-device smoke run, so treat these as setup and
diagnostic workflows rather than hardware compatibility claims. If discovery, the handshake, or the
firmware metadata query fails, continue with [troubleshooting](troubleshooting.md). A compatibility
warning means the returned `DEVICE_INFO` level is outside the documented 3-through-10 layouts; it
does not by itself diagnose physical hardware.

## Human and machine output

Human output is the default. Finite commands use one JSON value:

```console
$ meshquill --profile demo --output json contacts
```

Streams use JSON Lines, one object per line:

```console
$ meshquill --profile demo --output jsonl watch --count 2
```

`--output json` is rejected for streams, and `--output jsonl` is rejected for finite commands. All
machine records use the `meshquill.cli/v1` envelope. Automation details and stable exit codes are in
the [automation reference](reference/automation.md).

## Next steps

- Learn profile paths, selection, overrides, and plaintext-history opt-in in
  [configuration](configuration.md).
- Send, receive, watch, and chat in [messaging and chat](messaging-and-chat.md).
- Use guarded repeater and sensor operations in
  [remote administration](remote-administration.md).
- Move an early Meshquill file or a legacy BLE selection with [migration](migration.md).
