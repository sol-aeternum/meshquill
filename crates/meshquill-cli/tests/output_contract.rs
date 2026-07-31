//! Compatibility tests for the versioned machine-readable CLI envelope.

use std::{collections::HashSet, process::Command};

use meshquill::{
    args::OutputMode,
    output::{CLI_SCHEMA, OutputWriter},
};
use serde_json::{Value, json};
use tempfile::TempDir;

const JSON_SCHEMA_DRAFT: &str = "https://json-schema.org/draft/2020-12/schema";
const TYPE_PATTERN: &str = "^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$";
const SCHEMA_DOCUMENT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/meshquill-cli-v1.schema.json"
));
const RESULT_FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/compat/cli-v1-result.json"
));
const STREAM_FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/compat/cli-v1-stream.jsonl"
));
const KNOWN_TYPES_FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schemas/compat/cli-v1-known.jsonl"
));

fn parse_json(label: &str, input: &str) -> Value {
    serde_json::from_str(input).unwrap_or_else(|error| panic!("{label} is invalid JSON: {error}"))
}

fn is_lower_snake_case(value: &str) -> bool {
    let Some(first) = value.as_bytes().first() else {
        return false;
    };
    first.is_ascii_lowercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        && value.split('_').all(|component| !component.is_empty())
}

fn validate_exact_envelope(value: &Value) -> Result<(), String> {
    let envelope = value
        .as_object()
        .ok_or_else(|| "envelope must be an object".to_owned())?;
    let required = ["schema", "type", "data"];
    if envelope.len() != required.len()
        || required
            .iter()
            .any(|property| !envelope.contains_key(*property))
    {
        return Err("envelope must contain exactly schema, type, and data".to_owned());
    }

    let schema = envelope
        .get("schema")
        .and_then(Value::as_str)
        .ok_or_else(|| "schema must be a string".to_owned())?;
    if schema != CLI_SCHEMA {
        return Err(format!("schema must equal {CLI_SCHEMA}"));
    }

    let record_type = envelope
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| "type must be a string".to_owned())?;
    if !is_lower_snake_case(record_type) {
        return Err("type must be non-empty lower_snake_case".to_owned());
    }

    if !envelope.get("data").is_some_and(Value::is_object) {
        return Err("data must be an object".to_owned());
    }
    Ok(())
}

fn assert_valid_envelope(label: &str, value: &Value) {
    validate_exact_envelope(value).unwrap_or_else(|error| panic!("{label}: {error}"));
}

#[test]
fn checked_in_schema_describes_only_the_exact_v1_envelope() {
    let schema = parse_json("checked-in CLI schema", SCHEMA_DOCUMENT);
    assert_eq!(schema["$schema"], JSON_SCHEMA_DRAFT);
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["required"], json!(["schema", "type", "data"]));
    assert_eq!(schema["additionalProperties"], false);

    let properties = schema["properties"]
        .as_object()
        .unwrap_or_else(|| panic!("schema properties must be an object"));
    assert_eq!(properties.len(), 3);
    for property in ["schema", "type", "data"] {
        assert!(
            properties.contains_key(property),
            "schema is missing {property}"
        );
    }
    assert_eq!(properties["schema"]["const"], CLI_SCHEMA);
    assert_eq!(properties["type"]["type"], "string");
    assert_eq!(properties["type"]["minLength"], 1);
    assert_eq!(properties["type"]["pattern"], TYPE_PATTERN);
    assert_eq!(properties["data"]["type"], "object");
    assert_eq!(properties["data"]["additionalProperties"], true);
}

#[test]
fn checked_in_compatibility_fixtures_match_the_exact_envelope() {
    let result = parse_json("finite-result fixture", RESULT_FIXTURE);
    assert_valid_envelope("finite-result fixture", &result);

    let records: Vec<_> = STREAM_FIXTURE.lines().collect();
    assert!(!records.is_empty(), "JSONL fixture must contain a record");
    for (index, line) in records.into_iter().enumerate() {
        assert!(!line.trim().is_empty(), "JSONL fixture line is empty");
        let record = parse_json(&format!("JSONL fixture line {}", index + 1), line);
        assert_valid_envelope(&format!("JSONL fixture line {}", index + 1), &record);
    }

    let known_records: Vec<_> = KNOWN_TYPES_FIXTURE.lines().collect();
    assert_eq!(known_records.len(), 69);
    let mut known_types = HashSet::with_capacity(known_records.len());
    for (index, line) in known_records.into_iter().enumerate() {
        assert!(!line.trim().is_empty(), "known-type fixture line is empty");
        let record = parse_json(&format!("known-type fixture line {}", index + 1), line);
        assert_valid_envelope(&format!("known-type fixture line {}", index + 1), &record);
        let record_type = record["type"]
            .as_str()
            .unwrap_or_else(|| panic!("known-type fixture line {} has no type", index + 1));
        assert!(
            known_types.insert(record_type.to_owned()),
            "known-type fixture repeats {record_type}"
        );
    }
}

#[test]
fn checked_in_stream_fixture_matches_demo_watch_startup_output() {
    let directory = TempDir::new().expect("temporary directory");
    let config = directory.path().join("config.toml").display().to_string();
    let init = Command::new(env!("CARGO_BIN_EXE_meshquill"))
        .args([
            "--config",
            &config,
            "--non-interactive",
            "init",
            "--name",
            "demo",
            "--demo",
            "--set-default",
        ])
        .output()
        .expect("run deterministic demo initialization");
    assert!(
        init.status.success(),
        "demo initialization failed: {}",
        String::from_utf8_lossy(&init.stderr)
    );

    let watch = Command::new(env!("CARGO_BIN_EXE_meshquill"))
        .args([
            "--config", &config, "--output", "jsonl", "watch", "--count", "2",
        ])
        .output()
        .expect("run deterministic demo watch");
    assert!(
        watch.status.success(),
        "demo watch failed: {}",
        String::from_utf8_lossy(&watch.stderr)
    );
    assert_eq!(watch.stdout, STREAM_FIXTURE.as_bytes());
}

#[test]
fn output_writer_json_and_jsonl_match_the_same_envelope_contract() {
    let result_data = json!({"value": 7, "nested": {"additive": true}});
    let mut result_writer = OutputWriter::new(OutputMode::Json, Vec::new());
    result_writer
        .result("contract_result", &result_data, "ignored")
        .unwrap_or_else(|error| panic!("could not render JSON result: {error}"));
    let result_bytes = result_writer.into_inner();
    let result_text = std::str::from_utf8(&result_bytes)
        .unwrap_or_else(|error| panic!("JSON result is not UTF-8: {error}"));
    assert_eq!(result_text.lines().count(), 1);
    let result = parse_json("OutputWriter JSON result", result_text);
    assert_valid_envelope("OutputWriter JSON result", &result);

    let mut stream_writer = OutputWriter::new(OutputMode::Jsonl, Vec::new());
    for sequence in [1, 2] {
        stream_writer
            .event("contract_event", &json!({"sequence": sequence}), "ignored")
            .unwrap_or_else(|error| panic!("could not render JSONL event: {error}"));
    }
    let stream_bytes = stream_writer.into_inner();
    let stream_text = std::str::from_utf8(&stream_bytes)
        .unwrap_or_else(|error| panic!("JSONL stream is not UTF-8: {error}"));
    let records: Vec<_> = stream_text.lines().collect();
    assert_eq!(records.len(), 2);
    for (index, line) in records.into_iter().enumerate() {
        let record = parse_json(&format!("OutputWriter JSONL line {}", index + 1), line);
        assert_valid_envelope(&format!("OutputWriter JSONL line {}", index + 1), &record);
    }
}

#[test]
fn exact_envelope_rejects_incompatible_shapes_and_allows_additive_data() {
    let incompatible = [
        (
            "wrong schema",
            json!({"schema": "meshquill.cli/v2", "type": "event", "data": {}}),
        ),
        (
            "missing property",
            json!({"schema": CLI_SCHEMA, "type": "event"}),
        ),
        ("scalar envelope", json!(7)),
        (
            "scalar data",
            json!({"schema": CLI_SCHEMA, "type": "event", "data": 7}),
        ),
        (
            "extra envelope property",
            json!({"schema": CLI_SCHEMA, "type": "event", "data": {}, "future": true}),
        ),
        (
            "empty type",
            json!({"schema": CLI_SCHEMA, "type": "", "data": {}}),
        ),
        (
            "non-snake-case type",
            json!({"schema": CLI_SCHEMA, "type": "FutureEvent", "data": {}}),
        ),
    ];
    for (label, value) in incompatible {
        assert!(
            validate_exact_envelope(&value).is_err(),
            "{label} unexpectedly matched the envelope contract"
        );
    }

    let future = json!({
        "schema": CLI_SCHEMA,
        "type": "future_record_v2",
        "data": {
            "new_field": true,
            "nested": {
                "deeper": [
                    {"any": "shape"},
                    42,
                    null
                ]
            }
        }
    });
    assert_valid_envelope("unknown future type with additive data", &future);
}
