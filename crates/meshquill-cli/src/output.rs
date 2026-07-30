//! Stable stdout contracts and process exit semantics.

use std::io::{self, Write};

use serde::Serialize;
use thiserror::Error;

use crate::args::OutputMode;

/// Version attached to every machine-readable CLI record.
pub const CLI_SCHEMA: &str = "meshquill.cli/v1";

/// Stable public exit statuses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ExitStatus {
    /// Operation completed successfully.
    Success = 0,
    /// Invalid command or missing required non-interactive input.
    Usage = 2,
    /// Configuration is missing, malformed or invalid.
    Configuration = 3,
    /// Device discovery failed or found no requested device.
    Discovery = 4,
    /// Transport connection failed or is owned by another application.
    Connection = 5,
    /// Device returned or emitted invalid/unexpected protocol data.
    Protocol = 6,
    /// An explicitly bounded operation timed out.
    Timeout = 7,
    /// Authentication or credential resolution failed.
    Authentication = 8,
    /// Operation was refused by policy, confirmation or the device.
    Denied = 9,
    /// Named profile, contact, channel or other target was not found.
    NotFound = 10,
    /// Configured Python hook failed under its selected policy.
    Hook = 11,
    /// MQTT validation or gateway operation failed.
    Mqtt = 12,
    /// User interruption such as Ctrl-C.
    Interrupted = 130,
}

impl ExitStatus {
    /// Convert to the standard-library process return type.
    #[must_use]
    pub fn process(self) -> std::process::ExitCode {
        std::process::ExitCode::from(self as u8)
    }
}

/// One versioned machine-readable output object.
#[derive(Debug, Serialize)]
pub struct Envelope<'a, T: Serialize> {
    /// Schema identifier.
    pub schema: &'static str,
    /// Stable record type, such as `contact` or `connection`.
    #[serde(rename = "type")]
    pub record_type: &'a str,
    /// Command/event payload.
    pub data: &'a T,
}

impl<'a, T: Serialize> Envelope<'a, T> {
    /// Wrap a value in the current schema.
    #[must_use]
    pub const fn new(record_type: &'a str, data: &'a T) -> Self {
        Self {
            schema: CLI_SCHEMA,
            record_type,
            data,
        }
    }
}

/// Output writer that keeps human, JSON and JSONL behavior explicit.
pub struct OutputWriter<W> {
    mode: OutputMode,
    writer: W,
}

impl<W: Write> OutputWriter<W> {
    /// Create a writer for one selected output contract.
    #[must_use]
    pub const fn new(mode: OutputMode, writer: W) -> Self {
        Self { mode, writer }
    }

    /// Selected mode.
    #[must_use]
    pub const fn mode(&self) -> OutputMode {
        self.mode
    }

    /// Emit one command result.
    ///
    /// Human text is used only in human mode.
    ///
    /// # Errors
    /// Returns [`OutputError::JsonlForSingleResult`] if JSONL mode is requested for
    /// a single-result command, or any I/O/serialization error from write or JSON
    /// encoding.
    pub fn result<T: Serialize>(
        &mut self,
        record_type: &str,
        data: &T,
        human: &str,
    ) -> Result<(), OutputError> {
        match self.mode {
            OutputMode::Human => {
                write_terminal_safe(&mut self.writer, human)?;
                self.writer.write_all(b"\n")?;
            }
            OutputMode::Json => {
                serde_json::to_writer(&mut self.writer, &Envelope::new(record_type, data))?;
                self.writer.write_all(b"\n")?;
            }
            OutputMode::Jsonl => {
                return Err(OutputError::JsonlForSingleResult);
            }
        }
        self.writer.flush()?;
        Ok(())
    }

    /// Emit one stream event.
    ///
    /// JSONL is the only machine-readable stream form.
    ///
    /// # Errors
    /// Returns [`OutputError::JsonForStream`] if JSON mode is requested for a stream,
    /// or any I/O/serialization error from write or JSON encoding.
    pub fn event<T: Serialize>(
        &mut self,
        record_type: &str,
        data: &T,
        human: &str,
    ) -> Result<(), OutputError> {
        match self.mode {
            OutputMode::Human => {
                write_terminal_safe(&mut self.writer, human)?;
                self.writer.write_all(b"\n")?;
            }
            OutputMode::Jsonl => {
                serde_json::to_writer(&mut self.writer, &Envelope::new(record_type, data))?;
                self.writer.write_all(b"\n")?;
            }
            OutputMode::Json => return Err(OutputError::JsonForStream),
        }
        self.writer.flush()?;
        Ok(())
    }

    /// Emit format-native bytes such as a shell completion script.
    ///
    /// Raw output is intentionally limited to human mode because it is not a
    /// versioned Meshquill machine record.
    ///
    /// # Errors
    /// Returns [`OutputError::MachineModeForRaw`] for JSON/JSONL modes or an
    /// I/O error from the underlying writer.
    pub fn raw(&mut self, bytes: &[u8]) -> Result<(), OutputError> {
        if self.mode != OutputMode::Human {
            return Err(OutputError::MachineModeForRaw);
        }
        self.writer.write_all(bytes)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Return the underlying writer.
    #[must_use]
    pub fn into_inner(self) -> W {
        self.writer
    }
}

fn write_terminal_safe(writer: &mut impl Write, value: &str) -> io::Result<()> {
    for character in value.chars() {
        if character == '\n' || character == '\t' || !character.is_control() {
            let mut bytes = [0_u8; 4];
            writer.write_all(character.encode_utf8(&mut bytes).as_bytes())?;
        } else {
            for escaped in character.escape_default() {
                let mut bytes = [0_u8; 4];
                writer.write_all(escaped.encode_utf8(&mut bytes).as_bytes())?;
            }
        }
    }
    Ok(())
}

/// Invalid output combination or write failure.
#[derive(Debug, Error)]
pub enum OutputError {
    /// A stream cannot be represented by one indefinitely open JSON value.
    #[error("streaming commands require --output jsonl (or human)")]
    JsonForStream,
    /// JSONL is reserved for streams so scripts can distinguish command shapes.
    #[error("single-result commands require --output json (or human)")]
    JsonlForSingleResult,
    /// Format-native artifacts cannot be wrapped in a machine record.
    #[error("this artifact requires --output human")]
    MachineModeForRaw,
    /// Stdout write failed.
    #[error("could not write command output: {0}")]
    Io(#[from] io::Error),
    /// JSON serialization failed before or during output.
    #[error("could not serialize command output: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use serde::Serialize;

    use super::{CLI_SCHEMA, OutputError, OutputMode, OutputWriter};

    #[derive(Serialize)]
    struct Fixture {
        value: u8,
    }

    fn utf8(bytes: Vec<u8>) -> String {
        String::from_utf8(bytes)
            .unwrap_or_else(|error| panic!("writer generated invalid UTF-8: {error}"))
    }

    #[test]
    fn json_result_is_one_versioned_value() {
        let mut writer = OutputWriter::new(OutputMode::Json, Vec::new());
        writer
            .result("fixture", &Fixture { value: 7 }, "ignored")
            .unwrap_or_else(|error| panic!("render failed: {error}"));
        let output = utf8(writer.into_inner());
        let parsed: serde_json::Value =
            serde_json::from_str(&output).unwrap_or_else(|error| panic!("invalid JSON: {error}"));
        assert_eq!(parsed["schema"], CLI_SCHEMA);
        assert_eq!(parsed["type"], "fixture");
        assert_eq!(parsed["data"]["value"], 7);
        assert_eq!(output.lines().count(), 1);
    }

    #[test]
    fn jsonl_event_is_one_object_per_call() {
        let mut writer = OutputWriter::new(OutputMode::Jsonl, Vec::new());
        writer
            .event("fixture", &Fixture { value: 1 }, "ignored")
            .unwrap_or_else(|error| panic!("first render failed: {error}"));
        writer
            .event("fixture", &Fixture { value: 2 }, "ignored")
            .unwrap_or_else(|error| panic!("second render failed: {error}"));
        let output = utf8(writer.into_inner());
        assert_eq!(output.lines().count(), 2);
        for line in output.lines() {
            let parsed: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|error| panic!("invalid JSONL object: {error}"));
            assert_eq!(parsed["schema"], CLI_SCHEMA);
        }
    }

    #[test]
    fn json_stream_is_rejected() {
        let mut writer = OutputWriter::new(OutputMode::Json, Vec::new());
        let result = writer.event("fixture", &Fixture { value: 1 }, "ignored");
        assert!(matches!(result, Err(OutputError::JsonForStream)));
    }

    #[test]
    fn human_mode_emits_no_terminal_codes() {
        let mut writer = OutputWriter::new(OutputMode::Human, Vec::new());
        writer
            .result("fixture", &Fixture { value: 1 }, "plain")
            .unwrap_or_else(|error| panic!("render failed: {error}"));
        assert_eq!(utf8(writer.into_inner()), "plain\n");
    }

    #[test]
    fn human_mode_escapes_untrusted_terminal_controls() {
        let mut writer = OutputWriter::new(OutputMode::Human, Vec::new());
        writer
            .result(
                "fixture",
                &Fixture { value: 1 },
                "safe\nname\t\u{1b}[31m\u{7}",
            )
            .unwrap_or_else(|error| panic!("render failed: {error}"));
        assert_eq!(utf8(writer.into_inner()), "safe\nname\t\\u{1b}[31m\\u{7}\n");
    }
}
