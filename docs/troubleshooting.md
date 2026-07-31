# Troubleshooting

Start with bounded, read-only checks and add a real handshake only after the selected profile looks
correct:

```console
$ meshquill status
$ meshquill config show
$ meshquill doctor
$ meshquill devices --output json
$ meshquill doctor --connect
```

`status` shows saved selection without probing a connection. `doctor` validates configuration and
reports BLE and serial provider diagnostics; `--connect` additionally opens the selected profile and
runs the companion `APP_START` handshake, then makes one `DEVICE_QUERY` for firmware metadata before
closing that connection. The report keeps handshake and firmware compatibility as separate checks.
Use `-v` or `-vv` for more diagnostics; protocol metadata at `-vv` is designed to redact secrets.

`DEVICE_INFO` protocol levels 3 through 10 have documented layouts known to this release. A level
below 3 is reported as a legacy warning with reduced capability information. A level above 10 is a
newer-firmware warning: Meshquill reads the known fields, ignores extension fields, and does not
claim full compatibility. Either warning is advisory; a failed `DEVICE_QUERY` is a blocking
connection or protocol diagnostic. These checks validate only the bounded companion exchange and
known packet layout, not physical hardware, radio performance, or every firmware feature.

The project has not yet completed physical BLE or serial testing. A successful demo or loopback
test is not hardware evidence. Check [live status](https://github.com/sol-aeternum/meshquill/blob/main/STATUS.md) and the
[hardware matrix](hardware-testing.md) before interpreting a failure as a supported-device
regression.

## Configuration problems

### Configuration is missing or no profile is selected

Create a profile, or pass the intended file and profile explicitly:

```console
$ meshquill --config ./field.toml --non-interactive init --name field --serial /dev/ttyACM0
$ meshquill --config ./field.toml --profile field status
```

`--config`/`MESHQUILL_CONFIG` selects the file. `--profile`/`MESHQUILL_PROFILE` wins over
`default_profile`. Paths and precedence are detailed in [configuration](configuration.md).

### Configuration needs migration

Inspect and migrate the same path:

```console
$ meshquill --config ./old.toml config show
$ meshquill --config ./old.toml config migrate
```

Migration recognizes only a versionless early Meshquill schema and writes a backup. An explicit
`version = 0` is unsupported. See [migration](migration.md) for the exact conversion limits.

### Configuration is malformed

Fix the TOML from the error and retry `config show`. Use repair only when replacing the whole file is
acceptable:

```console
$ meshquill --config ./broken.toml --yes config repair
```

Repair backs up an existing file and replaces it with safe defaults containing no profiles. It
does not preserve valid sections.

## Discovery versus connection

Serial discovery enumerates candidate ports; it does not open them. BLE discovery observes devices
advertising the MeshCore Nordic UART service; it does not complete GATT discovery or the companion
handshake. Configured TCP endpoints are listed without a reachability claim. Follow discovery with
`doctor --connect` for the selected profile.

A companion firmware build commonly exposes BLE or USB serial, not necessarily both. The absence of
one transport is not by itself proof that the radio is faulty.

## BLE

Run a longer, transport-specific scan:

```console
$ meshquill devices --transport ble --scan-timeout 10s --output json
```

If nothing appears:

- Confirm the host has a powered BLE adapter and the companion is powered, nearby, advertising, and
  not in firmware-update mode.
- Close phone apps, browser Bluetooth sessions, and other desktop tools that may own the device.
- Meshquill post-filters for the Nordic UART service. A device visible in a generic Bluetooth list
  may still be absent if it is not advertising that service.
- Copy `data.devices[].target.selector` into the BLE profile. A display address or old cached
  platform ID may not be the selector returned by the current host backend.
- If connect reaches the device but service/characteristic discovery fails, verify companion
  firmware mode and retry after power-cycling the radio and host adapter. Remove a stale OS pairing
  only when the platform/device workflow calls for it.

Provider errors explicitly distinguish no adapter, all adapters powered off, provider failure, and
provider timeout. Connection errors also call out range, permissions, radio state, another owner,
missing Nordic UART service/characteristics, or a failed notification subscription.

### Linux BLE

The backend uses BlueZ over the system D-Bus. Check that Bluetooth is not radio-blocked, the BlueZ
service is running, and the current session can access its D-Bus API. Common diagnostics are:

```console
$ rfkill list bluetooth
$ bluetoothctl show
$ systemctl status bluetooth
```

Service managers vary, so translate the last command for the distribution. Containers need access
to the host D-Bus and adapter; merely installing Bluetooth tools inside a container is insufficient.
BlueZ merges scan filters across D-Bus clients, another reason to close competing scanners while
diagnosing unexpected results.

### macOS BLE

Enable Bluetooth and grant Bluetooth permission to the terminal application that launches
Meshquill under System Settings → Privacy & Security → Bluetooth. Granting permission to one
terminal does not grant it to another terminal, IDE, or packaged executable. Restart that
application after changing permission, then rerun discovery.

### Windows BLE

Confirm Bluetooth is enabled under Bluetooth & devices and that the adapter is healthy in Device
Manager. Close applications already connected to the companion. If the provider reports no adapter
or remains stale after a radio toggle, restart the Windows Bluetooth service or the host before
recreating the profile from a fresh Meshquill scan.

## Serial

Use JSON discovery to obtain the actual target port:

```console
$ meshquill devices --transport serial --output json
```

If enumeration succeeds but connection fails:

- Verify the profile uses `target.port`, not a discovery ID such as `serial:usb:...`.
- Confirm the device still exists and close serial monitors, firmware flashers, other Meshquill
  processes, and companion applications. Serial ports are commonly exclusive.
- `init --serial` uses 115200 baud. If the companion requires another rate, edit the profile's
  non-zero `baud` and re-run `config show`.
- Unplug/replug or reset the USB device after a flasher or suspended process leaves a stale handle.
- Enumeration success does not prove open permission; `doctor --connect` performs the open,
  handshake, and bounded firmware metadata query.

### Linux serial

Typical device names are `/dev/ttyACM*` and `/dev/ttyUSB*`:

```console
$ ls -l /dev/ttyACM* /dev/ttyUSB*
$ id
```

Check the device node's group and give the current user access according to the distribution
(commonly `dialout`, sometimes `uucp`), then sign out and back in so new group membership applies.
Do not solve access by making the port world-writable. If ModemManager or another service is probing
the port, confirm that from system logs before changing its configuration.

### macOS serial

The backend can enumerate both `/dev/cu.*` callout and `/dev/tty.*` dial-in entries for one device,
so two rows may refer to the same USB adapter. For an outgoing CLI connection, the `/dev/cu.*` entry
is generally the practical choice. Close any application using either sibling path. If a third-party
USB bridge does not appear, check its driver or approved system extension and reconnect the device.

### Windows serial

Use the COM name reported by Meshquill or Device Manager, for example `COM7`. Confirm the USB serial
driver loaded and close terminal/flash tools that hold the port. If the COM number changes after
replugging, update the profile; Meshquill does not silently retarget a saved port.

## TCP

TCP endpoints are configured, not discovered. A `devices` row only repeats the saved host and port.
Check DNS/address, port, firewall, routing, companion connection slots, and whether another client
owns the endpoint. Do not assume a port other than the one in the profile.

## Timeouts, disconnects, and duplicate sends

- Increase stored `timeout.connect_timeout_ms` for a slow provider open and
  `timeout.request_timeout_ms` (or the profile request override) for slow companion responses.
- `--timeout` separately bounds operations such as BLE scan phases and direct ACK waits.
- A finite `send` fails after a connection error and never automatically retries.
- Line chat and `watch` make at most three reconnect attempts: immediate, then after the configured
  retry delay and twice that delay, capped by the connect timeout. They reconnect only the session.
- Chat never auto-resends. After a reconnectable failure before companion acceptance was observed,
  it retains the exact unconfirmed text and original destination and requires `/send` or `/discard`.
  `/send` starts a new explicit send and can duplicate delivery if the original write succeeded but
  its response was lost; `/discard` avoids that risk.

See [messaging and chat](messaging-and-chat.md#bounded-reconnect-without-automatic-resend).

## Protocol or firmware errors

`the companion returned data that violates the supported protocol` means the transport opened but a
packet did not match supported bounds or layout. Run `doctor --connect`, record the exact device and
firmware version when the query returns them, and compare
[protocol coverage](protocol-coverage.md). A successful compatibility check recognizes the
`DEVICE_INFO` layout only; it does not certify the device or every firmware operation. Do not retry
a destructive command merely because its response was malformed or late.

## Output-mode errors

Use `--output json` for finite commands. Use `--output jsonl` for `watch`, `chat`, and other streams.
The CLI rejects the inverse combinations before device access. Redirected stdout contains results;
diagnostics go to stderr. See the [automation reference](reference/automation.md).

## Source build fails on Linux

The current dependency graph dynamically links the Linux D-Bus library for BLE and uses libudev for
serial enumeration. Install `pkg-config` plus the distribution's D-Bus and libudev development
packages, then rebuild with the pinned toolchain. Common package names are `libdbus-1-dev` and
`libudev-dev` on Debian-family systems, or `dbus-devel` and `systemd-devel` on Fedora-family
systems. These are build/runtime-provider prerequisites, not proof of radio access.

## Credential or history concerns

If `remote login --save` fails, verify an unlocked OS credential backend is available; use explicit
`--password-stdin` for non-interactive login. Never place a password in argv or TOML.

Message history is plaintext and disabled by default. Disabling it does not remove an old file. Use
`meshquill history list` to inspect the selected profile history and
`meshquill --yes history clear` to perform the confirmed deletion. Details are in
[configuration](configuration.md#opt-in-to-plaintext-message-history).

If history appears missing after changing paths, use the same `--config` and `--data-dir` (or
environment equivalents) on every invocation and inspect the canonical `path` in
`meshquill --output json history list`. Explicit config paths receive their own digest namespace.
The first access reconciles older config-adjacent history only when both path selections identify
the intended files; do not manually merge JSONL while Meshquill is running.
