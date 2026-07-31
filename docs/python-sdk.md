# Python SDK guide

The optional `meshquill-sdk` package exposes Meshquill's Rust managed client as the async
`meshcore_sdk` module. Protocol parsing, transport lifecycles, queue bounds and send semantics are
the same implementation used by the native CLI; the package has no runtime dependency on the
legacy Python MeshCore client.

The API supports CPython 3.9 and newer through one stable-ABI wheel per operating-system and CPU
target. The release workflow stages those wheels in a private draft. Until
[current status](../STATUS.md) records a published RC2 prerelease, build the source checkout; a
successful workflow draft alone is not a public download.

## Install

After current status records a published prerelease, install a downloaded wheel in an isolated
environment:

```console
python -m venv .venv
. .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install ./meshquill_sdk-0.1.0rc2-*.whl
python -c "import meshcore_sdk; print(meshcore_sdk.__version__)"
```

To build from a source checkout, install Rust 1.88 or newer plus the platform transport headers
listed in [installation](installation.md), then run:

```console
python -m venv .venv
. .venv/bin/activate
cd crates/meshquill-python
python -m pip install --requirement requirements-dev.txt
maturin develop --locked
```

The requirements file and package metadata exactly pin direct build/test tools. Cargo is locked;
Python transitive dependencies are resolved and audited in CI but are not represented by a fully
resolved checked-in lockfile in this RC.

## First message

The deterministic demo companion is safe for a first run and does not discover or open physical
hardware:

```python
import asyncio

from meshcore_sdk import Client


async def main() -> None:
    async with await Client.demo() as mesh:
        contacts = await mesh.list_contacts()
        print([contact.name for contact in contacts])
        receipt = await mesh.send("Alice", "Hello from Python")
        ack = await mesh.wait_for_ack(receipt)
        print(f"acknowledged by {ack.code_hex}")


asyncio.run(main())
```

For a configured device, replace `Client.demo()` with `Client.auto()`. `auto()` reads the same
versioned profile file and default-profile selection as the CLI. It accepts `profile="field"` and
`config_path="./field.toml"` when an application must make selection explicit. Configuration
environment overrides are also applied; secret references that require an interactive prompt are
not suitable for an unattended Python process.

## Discovery and explicit connections

Serial and BLE discovery are explicit async operations. A discovery result's `selector` is the
value accepted by `Client.ble`; its `port` is the value accepted by `Client.serial`. Discovery can
fail with `DiscoveryError` when an OS Bluetooth service is unavailable or permissions are missing.

```python
from meshcore_sdk import Client, discover_ble, discover_serial

serial_candidates = await discover_serial()
ble_candidates = await discover_ble(timeout=3.0)

mesh = await Client.serial("/dev/ttyACM0", baud=115_200)
mesh = await Client.ble("ble:PLATFORM-ID")
mesh = await Client.tcp("192.0.2.10", 5000)
```

TCP targets are never discovered or inferred. Each constructor accepts `connect_timeout` and
`request_timeout` in seconds. Construct only one active client for a companion unless its firmware
and transport explicitly permit multiple owners.

## Lifecycle and reconnect

Prefer the async context manager: it performs graceful shutdown on both normal and exceptional
exit. For longer-lived applications, lifecycle operations are explicit:

```python
mesh = await Client.auto(profile="field")
try:
    await mesh.disconnect()       # retain the selected target
    info = await mesh.reconnect() # fresh handshake; sends are never replayed
    print(info.name)
finally:
    await mesh.shutdown()         # idempotent after successful shutdown

assert mesh.is_closed
```

`disconnect()` is reversible; `shutdown()` stops the managed Rust actor and makes further
operations raise `ClientClosedError`. Reconnect is a deliberate single attempt, not an unbounded
background loop. Dropping or cancelling a Python waiter never causes Meshquill to replay a
mutating command.

## Contacts, sends and queued messages

`list_contacts()` returns immutable typed contacts. `send()` resolves a unique contact name or
public-key prefix. Ambiguous names raise `AmbiguousContactError`; use `send_direct()` with a
six-byte prefix or full 32-byte key when identity must be explicit.

```python
contacts = await mesh.list_contacts()
receipt = await mesh.send("Alice", "field check")
ack = await mesh.wait_for_ack(receipt, timeout=5.0)

await mesh.send_direct("aabbccddeeff", "explicit destination")
await mesh.send_channel(0, "numeric channel message")

message = await mesh.fetch_queued_message()
if message is not None:
    print(message.sender, message.text, message.sender_timestamp)
```

Direct sends return a `SendReceipt` containing the four-byte ACK code and the firmware's suggested
timeout. Channel send waits only for the immediate firmware response because channel broadcasts do
not have a direct-recipient ACK. An ACK timeout raises the SDK's typed `TimeoutError`.

Cancellation, timeout or disconnect can race with a completed device write. In that case the
radio outcome is ambiguous: reconcile with an ACK or application state before deciding to send the
same logical message again. Meshquill intentionally performs no automatic send retry.

## Independent event and message streams

`events()` and `messages()` create independent subscriptions. Each subscription replays retained
events from client construction—including the initial `self_info` and `connected` events—and then
continues live. Consuming one stream does not drain another.

```python
async with await Client.auto() as mesh:
    async for message in mesh.messages():
        print(message.sender, message.text)
```

For concurrent work, give each stream one consumer task and cancel that task during application
shutdown:

```python
async def receive(mesh: Client) -> None:
    async for event in mesh.events():
        if event.kind == "message" and event.message is not None:
            print(event.message.text)


async with await Client.auto() as mesh:
    receiver = asyncio.create_task(receive(mesh))
    try:
        receipt = await mesh.send("Alice", "still receiving")
        await mesh.wait_for_ack(receipt)
    finally:
        receiver.cancel()
        try:
            await receiver
        except asyncio.CancelledError:
            pass
```

Replay and live delivery are bounded. A receiver that falls behind raises `StreamLaggedError` with
the number of overwritten events instead of silently hiding loss; the caller can continue
iteration or rebuild state with explicit queries. Graceful client shutdown ends existing streams.
Python callbacks are not invoked on Rust runtime threads—the application owns its normal asyncio
tasks—so callback work cannot block the Rust event relay.

## Device information and telemetry

`device_info()` returns protocol version and the optional model, firmware build/version and device
limits reported by current firmware.

Two deliberately distinct telemetry APIs exist:

```python
stats = await mesh.telemetry()          # "core" device statistics
radio = await mesh.telemetry("radio")  # or "packets"
raw = await mesh.self_telemetry()       # local raw sensor response

print(stats.battery_mv)
print(raw.source_key_prefix, raw.payload)
```

`telemetry(kind)` returns a `DeviceStats`; fields outside the selected family are `None`.
`self_telemetry()` returns an immutable `TelemetryResponse` whose source prefix is lowercase hex
and whose bounded Cayenne-LPP-compatible payload is `bytes`. Its `repr` reveals only payload
length. Unsolicited equivalents arrive as `device_stats` and `telemetry` events.

## Typed errors and cancellation

All public failures inherit from `MeshcoreError`. Catch narrow types when the response differs:

```python
from meshcore_sdk import (
    AmbiguousContactError,
    ConfigurationError,
    MeshcoreError,
    TimeoutError,
    TransportError,
)

try:
    receipt = await mesh.send("Alice", "probe")
    await mesh.wait_for_ack(receipt)
except AmbiguousContactError as error:
    print(f"choose a key prefix: {error}")
except TimeoutError as error:
    print(f"no ACK before the bound: {error}")
except TransportError as error:
    print(f"connection failed: {error}")
except ConfigurationError as error:
    print(f"profile is invalid: {error}")
except MeshcoreError as error:
    print(f"MeshCore operation failed: {error}")
```

The hierarchy also includes protocol rejection, discovery, authentication, backpressure, stream
lag and closed-client errors. Exceptions retain normal Python module identity and can be pickled;
their messages include operation context and corrective detail where the Rust layer has it.
`asyncio.CancelledError` remains cancellation, not a wrapped SDK error.

## Examples, types and testing

Runnable examples live in
[`crates/meshquill-python/examples`](../crates/meshquill-python/examples):

- `quickstart.py` demonstrates contacts, direct send and ACK.
- `streaming.py` demonstrates independent replay/live subscriptions.
- `discovery.py` demonstrates serial and BLE enumeration.

The wheel contains `py.typed` and complete CPython 3.9-compatible `.pyi` declarations. See the
[generated API reference](reference/python-api.md) for every public class, method and property.
Applications can run `mypy` or another PEP 561-aware checker without installing Rust sources.

For integration tests that must not touch hardware, use `Client.demo()`. TCP loopback tests can
exercise exact framed protocol behaviour. Physical BLE/serial claims require a separately recorded
device and firmware matrix; the RC's repository validation does not imply such testing.
