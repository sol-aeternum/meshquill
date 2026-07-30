# Meshquill

Meshquill is an independent, Rust-first command-line client and async Python SDK for MeshCore
companion devices. It provides BLE, USB serial and TCP transports, clear interactive and automation
interfaces, trusted local Python hooks, and an optional application-level MQTT gateway. The native
CLI does not require Python.

This repository is prepared as `v0.1.0-rc.1`. Its protocol model is based on MeshCore companion
firmware v1.16.0, `meshcore_py` v2.3.8 and current upstream source pinned on 2026-07-30. No physical
radio was available in the build environment, so BLE/serial/TCP code is simulated and host-tested
but **not claimed as hardware-verified**. See [status and evidence](STATUS.md) and the
[hardware matrix](docs/hardware-testing.md).

## Install

Published release archives contain the binary, four shell completions, man pages, licences and a
checksum. After the repository and RC tag are pushed:

```console
cargo install --locked --git https://github.com/sol-aeternum/meshquill \
  --tag v0.1.0-rc.1 meshquill
```

From a source checkout:

```console
cargo install --path crates/meshquill-cli --locked
```

Linux source builds need a C compiler, `pkg-config`, D-Bus development headers and libudev
development headers. Enabling libudev keeps serial enumeration fallible instead of relying on an
upstream `/sys` fallback that can panic in restricted containers. Debian/Ubuntu uses
`libdbus-1-dev libudev-dev pkg-config`.
Release binaries are built for Linux x86-64/ARM64, macOS Intel/Apple Silicon and Windows x86-64;
the exact runtime baselines and clean-install procedure are in [installation](docs/installation.md).

## First connection and message

Discover BLE/serial companions, then run the guided wizard and enter the selected identifier, port,
or a TCP endpoint:

```console
meshquill devices
meshquill init
meshquill connect
meshquill device info
meshquill contacts
meshquill send Alice 'Are you receiving this?' --wait
meshquill inbox
meshquill watch
```

Profiles are named. Use `--profile field` on any operation, or make one the default during `init`.
`doctor` checks configuration and host transport support. Add `--connect` to attempt a protocol
handshake and classify the documented `DEVICE_INFO` layout; this bounded check does not replace
firmware-side or physical-radio troubleshooting:

```console
meshquill --profile field doctor --connect
```

For a five-minute walkthrough with platform permissions and recovery steps, use
[Getting started](docs/getting-started.md).

### Hardware-free tour

The demo transport is explicit and never selected as a silent fallback:

```console
meshquill --config /tmp/meshquill-demo.toml --non-interactive \
  init --name demo --demo --set-default
meshquill --config /tmp/meshquill-demo.toml device info
meshquill --config /tmp/meshquill-demo.toml contacts
meshquill --config /tmp/meshquill-demo.toml send Alice 'hello' --wait
meshquill --config /tmp/meshquill-demo.toml --output jsonl watch --count 2
meshquill --config /tmp/meshquill-demo.toml chat Alice --line
```

The deterministic demo exists only for learning, tests and automation examples. It does not claim
radio transmission.

## Messaging and chat

Direct sends can wait for an acknowledgement; channel sends use `--channel`. `watch` emits live
messages, ACKs, contact changes, telemetry, connection changes and sanitized errors. The portable
line chat keeps the destination visible, reports delivery state, drains incoming events between
input lines, and retains an unsent draft across a recoverable reconnect. It is not a full-screen
TUI in this RC.

Local message history is disabled by default. Enabling it writes bounded **plaintext JSONL** beside
the selected configuration; use `meshquill history clear --yes` to delete it. Details are in
[Messaging and chat](docs/messaging-and-chat.md).

## Automation

All ordinary operations have a non-interactive form. Successful non-streaming JSON uses a stable
`meshquill.cli/v1` envelope; streams require JSONL:

```console
meshquill --non-interactive --output json contacts --kind repeater
meshquill --non-interactive --output json send Alice 'hello' --wait
meshquill --non-interactive --output jsonl watch --event message --event ack
meshquill batch run commands.meshquill
meshquill batch contacts --filter 'type=repeater,favorite=true' remote-status --dry-run
```

Failures use stable exit statuses and plain diagnostics on stderr, even when success output is JSON.
`--non-interactive` forbids implicit prompts; an explicit data option such as `--password-stdin`
may still read stdin. See the [automation contract](docs/reference/automation.md).

## Python SDK

The optional `meshquill-sdk` wheel exposes the Rust core as `meshcore_sdk` with an async-first,
typed API:

```python
import asyncio
from meshcore_sdk import Client

async def main():
    async with await Client.auto() as mesh:
        receipt = await mesh.send("Alice", "Hello from Python")
        await mesh.wait_for_ack(receipt, timeout=5.0)
        async for message in mesh.messages():
            print(message.sender, message.text)
            break

asyncio.run(main())
```

Build/install and lifecycle details are in the [Python SDK guide](docs/python-sdk.md). Wheels use
the stable CPython 3.9+ ABI and do not depend on the old Python MeshCore implementation.

## Hooks and MQTT

- [Trusted Python hooks](docs/hooks.md) support nine versioned events, bounded subprocesses,
  validation, a mutating/rejecting `before_send`, and a working [example](examples/hooks/basic.py).
  They are local trusted code, not sandboxed plugins.
- [MQTT](docs/mqtt.md) is an optional foreground application gateway. TLS verification is on by
  default, inbound radio sends are off by default, and the only opt-in broker commands are bounded
  direct/channel text sends. It is not a MeshCore radio transport.

## Remote and network operations

Meshquill distinguishes local device commands from operations sent to a selected repeater, room or
sensor. Passwords come from a secure prompt, explicit stdin or the OS credential store. Destructive
contact/path/history operations require confirmation or `--yes`. Network discovery supports node
kind and flood-scope filters; scope changes used by one command are restored afterward. See
[Remote administration](docs/remote-administration.md) and the
[capability matrix](docs/capability-matrix.md).

## Documentation

- [Installation and release binaries](docs/installation.md)
- [Getting started](docs/getting-started.md)
- [Configuration, profiles, secrets and privacy](docs/configuration.md)
- [Messaging and line chat](docs/messaging-and-chat.md)
- [CLI and automation reference](docs/reference/automation.md)
- [Python SDK](docs/python-sdk.md), [hooks](docs/hooks.md), [MQTT](docs/mqtt.md)
- [Troubleshooting](docs/troubleshooting.md) and [migration](docs/migration.md)
- [Protocol coverage](docs/protocol-coverage.md) and [legacy capability accounting](docs/capability-matrix.md)
- [Research](docs/research.md), [architecture](docs/architecture.md), [threat model](docs/threat-model.md)
- [Contributing](CONTRIBUTING.md), [release process](docs/release.md), [security](SECURITY.md)

## RC limitations

- No physical device/firmware combination has been verified in this environment.
- Chat is deliberately line-oriented; there is no full-screen contact/history TUI yet.
- Manual pending-contact acceptance is surfaced with an explicit unsupported diagnostic because a
  safe one-shot CLI cannot reconstruct firmware advertisement state it never observed.
- Device reconnect is explicit and single-attempt; MQTT broker reconnect has bounded automatic
  backoff. Meshquill never guesses whether an ambiguous radio send should be replayed.
- Registry packages and GitHub artifacts exist only after maintainers publish the prepared tag;
  source and locally generated release artifacts remain installable meanwhile.

Meshquill is independent community software and is not an official MeshCore package. It is dual
licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
