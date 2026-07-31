"""Executable compatibility checks for every public JSON envelope."""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

REPOSITORY = Path(__file__).resolve().parents[3]
SCHEMAS = REPOSITORY / "schemas"
COMPAT = SCHEMAS / "compat"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_jsonl(path: Path) -> list[Any]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def validator(name: str) -> Draft202012Validator:
    schema = load_json(SCHEMAS / name)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def cli_known_types(schema: dict[str, Any]) -> set[str]:
    known: set[str] = set()
    for conditional in schema["allOf"]:
        selector = conditional["if"]["properties"]["type"]
        if "const" in selector:
            known.add(selector["const"])
        else:
            known.update(selector["enum"])
    return known


def test_cli_v1_checked_in_results_and_streams_validate() -> None:
    contract = validator("meshquill-cli-v1.schema.json")
    contract.validate(load_json(COMPAT / "cli-v1-result.json"))
    for record in load_jsonl(COMPAT / "cli-v1-stream.jsonl"):
        contract.validate(record)


def test_cli_v1_every_known_record_type_has_a_valid_fixture() -> None:
    schema = load_json(SCHEMAS / "meshquill-cli-v1.schema.json")
    assert isinstance(schema, dict)
    contract = validator("meshquill-cli-v1.schema.json")
    records = load_jsonl(COMPAT / "cli-v1-known.jsonl")
    record_types = [record["type"] for record in records]

    assert len(records) == 69
    assert len(record_types) == len(set(record_types)), "known-type fixture has duplicates"
    assert set(record_types) == cli_known_types(schema)
    for record in records:
        contract.validate(record)


def test_cli_v1_documented_principal_fields_cannot_be_removed() -> None:
    contract = validator("meshquill-cli-v1.schema.json")
    records = {record["type"]: record for record in load_jsonl(COMPAT / "cli-v1-known.jsonl")}
    principal_fields = {
        "batch_contacts": {"profile", "operation", "dry_run", "target_count", "targets"},
        "chat": {"state", "source", "text", "message_id"},
        "contact": {
            "profile",
            "name",
            "public_key",
            "kind",
            "flags",
            "route",
            "path",
            "last_advert",
            "lastmod",
        },
        "history": {"profile", "enabled", "storage", "path", "entries"},
        "hook_status": {
            "protocol",
            "enabled",
            "configured",
            "observational_failure",
            "before_send_failure",
        },
        "inbox": {"profile", "messages", "drained"},
        "mqtt_status": {
            "schema",
            "enabled",
            "configured",
            "host",
            "port",
            "protocol",
            "qos",
            "tls",
            "custom_ca",
            "client_identity",
            "authentication",
            "topic_prefix",
            "allow_send",
            "broker_state",
        },
        "network_discovery": {"profile", "filter", "scope", "timeout_ms", "nodes"},
        "profile_deleted": {
            "profile",
            "default_cleared",
            "history_retained",
            "credentials_retained",
        },
        "profile_reconfigured": {"profile", "transport", "default"},
        "profile_renamed": {"old", "new", "default", "history_migrated", "warning"},
        "send": {
            "destination",
            "channel",
            "queued",
            "ack_code",
            "acknowledged",
            "trip_time_ms",
        },
    }

    for record_type, fields in principal_fields.items():
        record = records[record_type]
        assert fields <= record["data"].keys()
        for field in fields:
            truncated = json.loads(json.dumps(record))
            del truncated["data"][field]
            with pytest.raises(ValidationError, match="required property"):
                contract.validate(truncated)


def test_cli_v1_deterministic_contact_and_chat_outputs_validate(tmp_path: Path) -> None:
    contract = validator("meshquill-cli-v1.schema.json")
    configured_binary = os.environ.get("MESHQUILL_TEST_CLI")
    binary = Path(configured_binary) if configured_binary else REPOSITORY / "target/debug/meshquill"
    assert binary.is_file(), f"build the CLI schema test binary first: {binary}"
    config = tmp_path / "config.toml"

    def run(arguments: list[str], input_text: str | None = None) -> str:
        completed = subprocess.run(
            [str(binary), "--config", str(config), *arguments],
            input=input_text,
            text=True,
            capture_output=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr
        return completed.stdout

    run(
        [
            "--non-interactive",
            "init",
            "--name",
            "demo",
            "--demo",
            "--set-default",
        ]
    )
    contact = json.loads(run(["--output", "json", "contacts", "show", "Alice"]))
    contract.validate(contact)
    assert contact["data"]["profile"] == "demo"
    assert contact["data"]["name"] == "Alice"

    chat = [
        json.loads(line)
        for line in run(
            ["--output", "jsonl", "chat", "Alice", "--line"], input_text="/quit\n"
        ).splitlines()
        if line
    ]
    assert chat
    for record in chat:
        contract.validate(record)
    incoming = next(record for record in chat if record["data"]["state"] == "incoming")
    assert {"state", "source", "text", "message_id"} <= incoming["data"].keys()


def test_cli_v1_literal_production_record_types_are_declared() -> None:
    schema = load_json(SCHEMAS / "meshquill-cli-v1.schema.json")
    assert isinstance(schema, dict)
    known = cli_known_types(schema)
    literal_call = re.compile(r'\.(?:result|event)\(\s*"([a-z][a-z0-9_]*)"')
    source_directory = REPOSITORY / "crates" / "meshquill-cli" / "src"
    emitted: set[str] = set()
    for source in source_directory.glob("*.rs"):
        if source.name == "output.rs":
            continue
        emitted.update(literal_call.findall(source.read_text(encoding="utf-8")))

    # `device_info` chooses these two record types with an inline conditional expression.
    emitted.update({"device", "firmware"})
    assert emitted <= known, f"production record types missing from schema: {emitted - known}"


def test_cli_v1_known_records_require_principal_fields_but_allow_additions() -> None:
    contract = validator("meshquill-cli-v1.schema.json")
    with pytest.raises(ValidationError):
        contract.validate(
            {
                "schema": "meshquill.cli/v1",
                "type": "contacts",
                "data": {"contacts": []},
            }
        )

    contract.validate(
        {
            "schema": "meshquill.cli/v1",
            "type": "contacts",
            "data": {
                "profile": "demo",
                "refreshed": True,
                "refresh_requested": False,
                "contacts": [],
                "future_addition": {"is_ignored_by_old_consumers": True},
            },
        }
    )
    contract.validate(
        {
            "schema": "meshquill.cli/v1",
            "type": "vendor_extension",
            "data": {},
        }
    )


def test_hook_v1_every_event_fixture_validates() -> None:
    contract = validator("meshquill-hook-v1.schema.json")
    records = load_jsonl(COMPAT / "hook-v1-valid.jsonl")
    assert len(records) == 9
    for record in records:
        contract.validate(record)

    invalid = dict(records[2])
    invalid["payload"] = {"source": "Alice", "message_id": None}
    with pytest.raises(ValidationError):
        contract.validate(invalid)


def test_mqtt_v1_positive_and_negative_fixtures_match_contract() -> None:
    contract = validator("meshquill-mqtt-v1.schema.json")
    valid = load_jsonl(COMPAT / "mqtt-v1-valid.jsonl")
    invalid = load_jsonl(COMPAT / "mqtt-v1-invalid.jsonl")
    assert len(valid) == 11
    assert len(invalid) == 16
    for record in valid:
        contract.validate(record)
    for record in invalid:
        with pytest.raises(ValidationError):
            contract.validate(record)


def test_mqtt_v1_schema_documents_codepoint_caps_while_runtime_adds_utf8_byte_caps() -> None:
    contract = validator("meshquill-mqtt-v1.schema.json")
    base = {
        "schema": "meshquill.mqtt/v1",
        "event_id": "018f0f65-9b50-7cc2-a6e9-3b8b3a7f3299",
        "origin": "remote",
        "timestamp": 1725000001999,
        "type": "send_direct",
        "data": {"destination": "alice", "text": "hello"},
    }

    # JSON Schema maxLength counts Unicode code points. The Rust gateway separately enforces
    # the documented UTF-8 byte caps; the paired Rust compatibility test covers that boundary.
    multibyte_destination = dict(base)
    multibyte_destination["data"] = {"destination": "é" * 65, "text": "hello"}
    contract.validate(multibyte_destination)

    multibyte_origin = dict(base)
    multibyte_origin["origin"] = "é" * 65
    contract.validate(multibyte_origin)

    multibyte_text = dict(base)
    multibyte_text["data"] = {"destination": "alice", "text": "é" * 513}
    contract.validate(multibyte_text)
