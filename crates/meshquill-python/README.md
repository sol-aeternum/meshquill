# meshquill-sdk

`meshquill-sdk` is the optional async Python distribution for Meshquill. It deliberately uses the
branded distribution name while retaining the requested `meshcore_sdk` import package. The
`_native` extension uses
the same Rust `ManagedClient`, protocol parser, bounded event broadcast, configuration store, and
BLE/serial/TCP transports as the native project. It does not reimplement the MeshCore protocol in
Python, and the native CLI does not depend on Python.

Python 3.9 or newer is supported through CPython's stable ABI.

```console
python -m pip install meshquill-sdk
```

The installed import remains `meshcore_sdk` in code examples below.

## Quickstart

Configure a default Meshquill profile, then connect it with an async context manager:

```python
import asyncio

from meshcore_sdk import Client


async def main() -> None:
    async with await Client.auto() as mesh:
        contacts = await mesh.list_contacts()
        print([contact.name for contact in contacts])

        receipt = await mesh.send("Alice", "Hello from Python")
        ack = await mesh.wait_for_ack(receipt)
        print(ack.code_hex)


asyncio.run(main())
```

`Client.auto()` uses the store's `default_profile`, global connect/request timeouts, a per-profile
request-timeout override when present, and the configured bounded outbound capacity. Pass
`profile=` to select another stored profile or `config_path=` for a caller-owned config file.
Stored secret references are neither resolved for these companion transports nor exposed in
Python diagnostics.

Explicit constructors are also awaitable:

```python
mesh = await Client.tcp("127.0.0.1", 5000)
mesh = await Client.serial("/dev/ttyACM0", baud=115_200)
mesh = await Client.ble("ble:platform-device-id")
```

Use `await discover_ble(timeout=5.0)` or `await discover_serial()` to enumerate BLE and serial
candidates. TCP endpoints are explicit; the SDK does not pretend to discover them.

`Client.demo()` is a deterministic in-memory companion for documentation and tests only. It is
never selected by `Client.auto()`:

```python
async with await Client.demo() as mesh:
    receipt = await mesh.send("Alice", "deterministic hello")
    await mesh.wait_for_ack(receipt)
```

## Messages and events

Every `messages()` and `events()` call creates an independent receiver on the Rust client's
bounded broadcast channel:

```python
events = mesh.events()
messages = mesh.messages()

async for message in messages:
    print(message.text)
```

A slow receiver raises `StreamLaggedError` with the number of overwritten events. Loss is never
hidden. Iteration is cancellation-safe, and graceful client shutdown ends existing iterators.

`fetch_queued_message()` retrieves one firmware-queued message (or `None`). `send_direct()` accepts
a six-byte hex prefix/full public key, `send_channel()` sends channel text, `device_info()` queries
metadata, and `disconnect()`/`reconnect()` explicitly control the retained target.

## Send cancellation and ambiguous outcomes

The managed Rust actor serializes operations in a bounded queue. Once accepted, it finishes a
device operation even if its Python awaiter is cancelled. It never retries or replays a mutating
command. Cancellation, a timeout, or a transport failure can happen after bytes reached the
device, so the send outcome may be ambiguous. Reconcile with device state or an ACK before
sending the same logical message again.

## Statistics and raw self telemetry

`await mesh.telemetry()` requests core device statistics. Pass `"radio"` or `"packets"` to query
the other statistics families. The result is a typed `DeviceStats` object whose `kind` identifies
the returned family; fields that do not belong to that family are `None`. Unsolicited statistics
packets are also available as typed `device_stats` events.

`await mesh.self_telemetry()` is a separate raw local-sensor query. It returns an immutable
`TelemetryResponse` with the six-byte `source_key_prefix` as lowercase hex and the bounded
Cayenne-LPP-compatible `payload` as `bytes`. Its representation reveals only the payload length.
Unsolicited raw responses use event kind `telemetry` and are available through `Event.telemetry`.

## Development

From this directory, build and test in an isolated environment:

```console
python -m venv .venv
.venv/bin/python -m pip install maturin ruff mypy pytest pytest-asyncio
.venv/bin/maturin develop
.venv/bin/ruff check python examples tests
.venv/bin/mypy
.venv/bin/pytest
```

The package includes complete `.pyi` declarations and `py.typed` for downstream type checkers.
