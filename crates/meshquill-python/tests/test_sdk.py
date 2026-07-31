from __future__ import annotations

import ast
import asyncio
import importlib
import importlib.metadata
import pickle
import struct
from collections.abc import AsyncIterator
from pathlib import Path
from types import ModuleType
from typing import cast

import meshcore_sdk
import pytest
from meshcore_sdk import (
    Client,
    ClientClosedError,
    Event,
    InvalidArgumentError,
    Message,
    StreamLaggedError,
    TelemetryResponse,
    TimeoutError,
)


def test_installed_distribution_imports_without_legacy_meshcore_dependency() -> None:
    distribution = importlib.metadata.distribution("meshquill-sdk")
    assert meshcore_sdk.__version__ == "0.1.0-rc.2"
    requirements = distribution.requires or []
    assert not any(requirement.lower().startswith("meshcore") for requirement in requirements)


def _stub_exports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets
        ):
            continue
        raw = ast.literal_eval(node.value)
        if not isinstance(raw, list):
            raise AssertionError(f"{path}.__all__ must be a literal list")
        exports: set[str] = set()
        for item in raw:
            if not isinstance(item, str):
                raise AssertionError(f"{path}.__all__ contains a non-string entry")
            exports.add(item)
        return exports
    raise AssertionError(f"{path} does not define __all__")


def _runtime_exports(module: ModuleType) -> set[str]:
    return set(cast(list[str], module.__all__))


def test_runtime_and_stub_exports_match() -> None:
    native = importlib.import_module("meshcore_sdk._native")
    package_dir = Path(__file__).parents[1] / "python" / "meshcore_sdk"
    expected = _runtime_exports(meshcore_sdk)

    assert _runtime_exports(native) == expected
    assert _stub_exports(package_dir / "__init__.pyi") == expected
    assert _stub_exports(package_dir / "_native.pyi") == expected
    assert all(hasattr(meshcore_sdk, name) for name in expected)
    assert all(hasattr(native, name) for name in expected)
    assert "TelemetryResponse" in expected
    assert "EventKind" not in expected
    assert not hasattr(meshcore_sdk, "EventKind")
    assert not hasattr(native, "EventKind")


_EXCEPTION_TYPES: tuple[type[Exception], ...] = (
    meshcore_sdk.MeshcoreError,
    meshcore_sdk.ConfigurationError,
    meshcore_sdk.DiscoveryError,
    meshcore_sdk.TransportError,
    meshcore_sdk.ProtocolError,
    meshcore_sdk.DeviceRejectedError,
    meshcore_sdk.TimeoutError,
    meshcore_sdk.DisconnectedError,
    meshcore_sdk.InvalidArgumentError,
    meshcore_sdk.AmbiguousContactError,
    meshcore_sdk.BackpressureError,
    meshcore_sdk.StreamLaggedError,
    meshcore_sdk.UnsupportedFeatureError,
    meshcore_sdk.AuthenticationError,
    meshcore_sdk.ClientClosedError,
)


@pytest.mark.parametrize("error_type", _EXCEPTION_TYPES)
def test_exceptions_have_importable_pickle_identity(error_type: type[Exception]) -> None:
    assert error_type.__module__ == "meshcore_sdk._native"
    native = importlib.import_module(error_type.__module__)
    assert getattr(native, error_type.__name__) is error_type

    restored = pickle.loads(pickle.dumps(error_type("probe")))
    assert type(restored) is error_type
    assert restored.args == ("probe",)


def test_exception_hierarchy_is_preserved() -> None:
    assert issubclass(meshcore_sdk.DeviceRejectedError, meshcore_sdk.ProtocolError)
    assert issubclass(meshcore_sdk.DisconnectedError, meshcore_sdk.TransportError)
    assert issubclass(meshcore_sdk.BackpressureError, meshcore_sdk.TransportError)
    assert issubclass(meshcore_sdk.AmbiguousContactError, meshcore_sdk.InvalidArgumentError)


@pytest.mark.asyncio
async def test_quickstart_lists_contacts_and_waits_for_ack() -> None:
    client = await Client.demo()
    async with client as mesh:
        contacts = await mesh.list_contacts()
        assert [contact.name for contact in contacts] == ["Alice", "Bob"]
        receipt = await mesh.send("Alice", "hello")
        assert receipt.code_hex == "12345678"
        ack = await mesh.wait_for_ack(receipt)
        assert ack.code == b"\x12\x34\x56\x78"
    assert client.is_closed


@pytest.mark.asyncio
async def test_message_and_event_streams_are_independent() -> None:
    async with await Client.demo() as mesh:
        messages: AsyncIterator[Message] = mesh.messages()
        events: AsyncIterator[Event] = mesh.events()
        initial = [await events.__anext__(), await events.__anext__()]
        assert [event.kind for event in initial] == ["self_info", "connected"]
        streamed, event, fetched = await asyncio.gather(
            messages.__anext__(),
            events.__anext__(),
            mesh.fetch_queued_message(),
        )
        assert fetched is not None
        assert streamed.text == fetched.text
        assert streamed.sender == "channel:1"
        assert event.kind == "message"
        assert event.message is not None
        assert event.message.text == fetched.text
        assert event.message.sender == streamed.sender


@pytest.mark.asyncio
async def test_context_shutdown_stops_existing_stream_and_rejects_operations() -> None:
    client = await Client.demo()
    async with client as mesh:
        stream = mesh.events()
    with pytest.raises(StopAsyncIteration):
        await stream.__anext__()
    with pytest.raises(ClientClosedError):
        await client.device_info()


@pytest.mark.asyncio
async def test_slow_stream_surfaces_lag_instead_of_hiding_loss() -> None:
    async with await Client.demo() as mesh:
        stream = mesh.events()
        assert [
            (await stream.__anext__()).kind,
            (await stream.__anext__()).kind,
        ] == ["self_info", "connected"]
        for _ in range(300):
            await mesh.send_direct("22" * 6, "lag probe")
        await asyncio.sleep(0.01)
        with pytest.raises(StreamLaggedError, match="lost"):
            await stream.__anext__()


@pytest.mark.asyncio
async def test_cancellation_does_not_break_later_operations() -> None:
    async with await Client.demo() as mesh:
        waiting = asyncio.ensure_future(mesh.wait_for_ack("00000000", timeout=0.5))
        await asyncio.sleep(0.01)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        info = await asyncio.wait_for(mesh.device_info(), timeout=1.0)
        assert info.model == "meshlink-virtual"


@pytest.mark.asyncio
async def test_timeout_and_input_validation_are_typed() -> None:
    async with await Client.demo() as mesh:
        with pytest.raises(TimeoutError):
            await mesh.wait_for_ack("00000000", timeout=0.01)
        with pytest.raises(InvalidArgumentError):
            await mesh.send_direct("not-a-key", "hello")
        with pytest.raises(InvalidArgumentError):
            await mesh.telemetry("unknown")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_channel_send_disconnect_reconnect_and_device_info() -> None:
    async with await Client.demo() as mesh:
        await mesh.send_channel(1, "hello channel")
        await mesh.disconnect()
        reconnected = await mesh.reconnect()
        assert reconnected.public_key == mesh.self_info.public_key
        info = await mesh.device_info()
        assert info.protocol_version == 10
        assert not hasattr(info, "ble_pin")


def _self_info_packet(name: str) -> bytes:
    packet = bytearray(58)
    packet[0] = 0x05
    packet[1:4] = bytes((1, 10, 20))
    packet[4:36] = bytes([0x44]) * 32
    packet[48:52] = (915_000).to_bytes(4, "little")
    packet[52:56] = (62_500).to_bytes(4, "little")
    packet[56:58] = bytes((7, 5))
    packet.extend(name.encode())
    return bytes(packet)


def _companion_frame(payload: bytes) -> bytes:
    return b"\x3e" + struct.pack("<H", len(payload)) + payload


def _direct_message_packet(prefix: bytes, text: str) -> bytes:
    assert len(prefix) == 6
    return b"\x10\x00\x00\x00" + prefix + b"\xff\x00" + struct.pack("<I", 42) + text.encode()


def _channel_message_packet(channel: int, text: str) -> bytes:
    return (
        b"\x11\x00\x00\x00"
        + bytes((channel,))
        + b"\xff\x00"
        + struct.pack("<I", 43)
        + text.encode()
    )


def _telemetry_packet(prefix: bytes, payload: bytes) -> bytes:
    assert len(prefix) == 6
    return b"\x8b\x00" + prefix + payload


@pytest.mark.asyncio
async def test_constructor_replays_handshake_and_early_messages() -> None:
    async def companion(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        header = await reader.readexactly(3)
        assert header[0] == 0x3C
        payload = await reader.readexactly(struct.unpack("<H", header[1:])[0])
        assert payload == b"\x01\x03      mccli"

        packets = (
            _direct_message_packet(bytes.fromhex("010203040506"), "early direct"),
            _channel_message_packet(7, "early channel"),
            _self_info_packet("replay-test"),
        )
        for packet in packets:
            writer.write(_companion_frame(packet))
        await writer.drain()
        await reader.read()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(companion, "127.0.0.1", 0)
    socket = server.sockets[0]
    port = int(socket.getsockname()[1])
    client: Client | None = None
    try:
        client = await asyncio.wait_for(
            Client.tcp("127.0.0.1", port, request_timeout=1.0), timeout=2.0
        )
        events = client.events()
        replay = [await asyncio.wait_for(events.__anext__(), timeout=1.0) for _ in range(4)]
        assert [event.kind for event in replay] == [
            "message",
            "message",
            "self_info",
            "connected",
        ]

        messages = client.messages()
        direct = await asyncio.wait_for(messages.__anext__(), timeout=1.0)
        channel = await asyncio.wait_for(messages.__anext__(), timeout=1.0)
        assert direct.text == "early direct"
        assert direct.sender == "010203040506"
        assert direct.sender == direct.source_key_prefix
        assert direct.channel is None
        assert channel.text == "early channel"
        assert channel.sender == "channel:7"
        assert channel.source_key_prefix is None
        assert channel.channel == 7
    finally:
        if client is not None and not client.is_closed:
            await client.shutdown()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_self_telemetry_query_and_unsolicited_event_are_typed() -> None:
    event_prefix = bytes.fromhex("ABCDEF012345")
    event_payload = b"\x01\x67\x00\xd7"
    query_prefix = bytes.fromhex("1020304050A0")
    query_payload = b"\x02\x68\x64"

    async def companion(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        header = await reader.readexactly(3)
        assert header[0] == 0x3C
        payload = await reader.readexactly(struct.unpack("<H", header[1:])[0])
        assert payload == b"\x01\x03      mccli"

        writer.write(_companion_frame(_self_info_packet("telemetry-test")))
        writer.write(_companion_frame(_telemetry_packet(event_prefix, event_payload)))
        await writer.drain()

        query_header = await reader.readexactly(3)
        assert query_header[0] == 0x3C
        query = await reader.readexactly(struct.unpack("<H", query_header[1:])[0])
        assert query == b"\x27\x00\x00\x00"
        writer.write(_companion_frame(_telemetry_packet(query_prefix, query_payload)))
        await writer.drain()

        await reader.read()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(companion, "127.0.0.1", 0)
    socket = server.sockets[0]
    port = int(socket.getsockname()[1])
    client: Client | None = None
    try:
        client = await asyncio.wait_for(
            Client.tcp("127.0.0.1", port, request_timeout=1.0), timeout=2.0
        )
        events = client.events()
        replay = [await asyncio.wait_for(events.__anext__(), timeout=1.0) for _ in range(3)]
        assert [event.kind for event in replay] == ["self_info", "connected", "telemetry"]
        event_response = replay[2].telemetry
        assert isinstance(event_response, TelemetryResponse)
        assert event_response.source_key_prefix == "abcdef012345"
        assert event_response.payload == event_payload
        assert repr(event_response) == "TelemetryResponse(payload_len=4)"
        assert replay[0].telemetry is None

        response = await asyncio.wait_for(client.self_telemetry(), timeout=1.0)
        assert isinstance(response, TelemetryResponse)
        assert response.source_key_prefix == "1020304050a0"
        assert response.payload == query_payload
        assert repr(response) == "TelemetryResponse(payload_len=3)"
        assert query_prefix.hex() not in repr(response)
        assert query_payload.hex() not in repr(response)
        with pytest.raises(AttributeError):
            response.payload = b"changed"  # type: ignore[misc]
    finally:
        if client is not None and not client.is_closed:
            await client.shutdown()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_auto_selects_default_profile_and_configured_timeout(tmp_path: Path) -> None:
    connected = asyncio.Event()

    async def companion(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        header = await reader.readexactly(3)
        assert header[0] == 0x3C
        payload = await reader.readexactly(struct.unpack("<H", header[1:])[0])
        assert payload[0] == 0x01
        response = _self_info_packet("auto-profile")
        writer.write(_companion_frame(response))
        await writer.drain()
        connected.set()

        stats_header = await reader.readexactly(3)
        assert stats_header[0] == 0x3C
        stats_command = await reader.readexactly(struct.unpack("<H", stats_header[1:])[0])
        assert stats_command == b"\x38\x00"
        stats_response = bytearray((0x18, 0))
        stats_response.extend((4_125).to_bytes(2, "little"))
        stats_response.extend((86_400).to_bytes(4, "little"))
        stats_response.extend((3).to_bytes(2, "little"))
        stats_response.append(2)
        writer.write(_companion_frame(bytes(stats_response)))
        await writer.drain()

        await reader.read()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(companion, "127.0.0.1", 0)
    socket = server.sockets[0]
    port = int(socket.getsockname()[1])
    config = tmp_path / "config.toml"
    config.write_text(
        "\n".join(
            (
                "version = 1",
                'default_profile = "primary"',
                "",
                "[timeout]",
                "connect_timeout_ms = 1000",
                "request_timeout_ms = 321",
                "retry_timeout_ms = 1000",
                "",
                "[device_profiles.primary.transport]",
                'type = "tcp"',
                'host = "127.0.0.1"',
                f"port = {port}",
                "",
            )
        )
    )

    try:
        client = await Client.auto(config_path=config)
        await asyncio.wait_for(connected.wait(), timeout=1.0)
        assert client.profile_name == "primary"
        assert client.transport == "tcp"
        assert client.request_timeout == pytest.approx(0.321)
        assert client.self_info.name == "auto-profile"
        stats = await client.telemetry()
        assert stats.kind == "core"
        assert stats.battery_mv == 4_125
        assert stats.uptime_seconds == 86_400
        assert stats.errors == 3
        assert stats.queue_length == 2
        assert stats.noise_floor is None
        await client.shutdown()
    finally:
        server.close()
        await server.wait_closed()
