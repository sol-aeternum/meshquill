//! Compatibility checks for the checked-in MQTT v1 schema and fixtures.

use meshquill_mqtt::{EventEnvelope, EventKind, SCHEMA_VERSION};
use serde_json::Value;

const JSON_SCHEMA_DRAFT: &str = "https://json-schema.org/draft/2020-12/schema";
const SCHEMA_DOCUMENT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/meshquill-mqtt-v1.schema.json"
));
const VALID_FIXTURES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/compat/mqtt-v1-valid.jsonl"
));
const INVALID_FIXTURES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/compat/mqtt-v1-invalid.jsonl"
));

fn parse_json(label: &str, input: &str) -> Value {
    serde_json::from_str(input).unwrap_or_else(|error| panic!("{label} is invalid JSON: {error}"))
}

#[test]
fn checked_in_schema_declares_all_exact_v1_discriminators() {
    let schema = parse_json("MQTT schema", SCHEMA_DOCUMENT);
    assert_eq!(schema["$schema"], JSON_SCHEMA_DRAFT);
    assert_eq!(schema["oneOf"].as_array().map(Vec::len), Some(7));

    let definitions = schema["$defs"]
        .as_object()
        .unwrap_or_else(|| panic!("MQTT schema definitions must be an object"));
    let base = &definitions["base_envelope"];
    assert_eq!(base["additionalProperties"], false);
    assert_eq!(base["properties"]["schema"]["const"], SCHEMA_VERSION);
    assert!(base["properties"]["origin"]["pattern"].is_string());
    assert_eq!(
        base["properties"]["timestamp"]["maximum"],
        Value::from(u64::MAX)
    );
    for definition in ["send_direct_data", "send_channel_data"] {
        assert!(
            definitions[definition]["properties"]["text"]["pattern"].is_string(),
            "{definition} must express the NUL rejection enforced by Rust"
        );
    }
    assert!(definitions["send_direct_data"]["properties"]["destination"]["pattern"].is_string());

    let expected = [
        ("incoming_message_envelope", "incoming_message"),
        ("ack_envelope", "ack"),
        ("connection_state_envelope", "connection_state"),
        ("contacts_envelope", "contacts"),
        ("telemetry_envelope", "telemetry"),
        ("send_direct_envelope", "send_direct"),
        ("send_channel_envelope", "send_channel"),
    ];
    for (definition, discriminator) in expected {
        assert_eq!(
            definitions[definition]["allOf"][1]["properties"]["type"]["const"],
            discriminator
        );
    }

    for definition in [
        "incoming_message_data",
        "ack_data",
        "connection_state_data",
        "contact",
        "contacts_data",
        "telemetry_battery",
        "telemetry_stats_core",
        "telemetry_stats_radio",
        "telemetry_stats_packets",
        "telemetry_raw_cayenne_lpp",
        "send_direct_data",
        "send_channel_data",
    ] {
        assert_eq!(
            definitions[definition]["additionalProperties"], false,
            "{definition} must reject accidental wire additions"
        );
    }
}

#[test]
fn valid_fixtures_cover_every_top_level_and_telemetry_variant() {
    let mut kinds = Vec::new();
    let mut telemetry_kinds = Vec::new();
    for (index, line) in VALID_FIXTURES.lines().enumerate() {
        assert!(!line.trim().is_empty(), "valid fixture line is empty");
        let envelope = EventEnvelope::decode(line.as_bytes())
            .unwrap_or_else(|error| panic!("valid fixture line {} failed: {error}", index + 1));
        kinds.push(envelope.kind);
        if envelope.kind == EventKind::Telemetry {
            telemetry_kinds.push(
                envelope.data["kind"]
                    .as_str()
                    .unwrap_or_else(|| panic!("telemetry fixture has no kind"))
                    .to_owned(),
            );
        }
    }

    for kind in [
        EventKind::IncomingMessage,
        EventKind::Ack,
        EventKind::ConnectionState,
        EventKind::Contacts,
        EventKind::Telemetry,
        EventKind::SendDirect,
        EventKind::SendChannel,
    ] {
        assert!(kinds.contains(&kind), "fixture missing {kind:?}");
    }
    assert_eq!(
        telemetry_kinds,
        [
            "battery",
            "stats_core",
            "stats_radio",
            "stats_packets",
            "raw_cayenne_lpp",
        ]
    );
}

#[test]
fn negative_fixtures_are_rejected_by_the_runtime_v1_contract() {
    for (index, line) in INVALID_FIXTURES.lines().enumerate() {
        assert!(!line.trim().is_empty(), "invalid fixture line is empty");
        assert!(
            EventEnvelope::decode(line.as_bytes()).is_err(),
            "invalid fixture line {} was accepted",
            index + 1
        );
    }
}
