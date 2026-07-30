use std::{fmt, io, time::Duration};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::HookEventKind;

/// Stable top-level error category suitable for CLI exit-code mapping.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HookErrorCategory {
    /// Invalid runtime settings, script metadata, or script validation.
    Configuration,
    /// The configured deadline elapsed.
    Timeout,
    /// A `before_send` hook explicitly rejected a message.
    Rejected,
    /// The Python process could not be started or did not exit successfully.
    Process,
    /// The runner exchanged invalid or disallowed protocol data.
    Protocol,
}

/// Error returned by hook configuration, validation, or execution.
#[derive(Debug, Error)]
pub enum HookError {
    /// Invalid configuration or hook script.
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    /// Execution exceeded its configured timeout.
    #[error("hook execution exceeded its {timeout:?} timeout")]
    Timeout {
        /// Configured execution timeout.
        timeout: Duration,
    },
    /// A `before_send` hook rejected an outbound message.
    #[error(transparent)]
    Rejected(#[from] HookRejection),
    /// Subprocess lifecycle failure.
    #[error(transparent)]
    Process(#[from] ProcessError),
    /// JSON runner protocol failure.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
}

impl HookError {
    /// Returns the stable error category.
    #[must_use]
    pub const fn category(&self) -> HookErrorCategory {
        match self {
            Self::Configuration(_) => HookErrorCategory::Configuration,
            Self::Timeout { .. } => HookErrorCategory::Timeout,
            Self::Rejected(_) => HookErrorCategory::Rejected,
            Self::Process(_) => HookErrorCategory::Process,
            Self::Protocol(_) => HookErrorCategory::Protocol,
        }
    }

    pub(crate) fn rejected(reason: String) -> Self {
        Self::Rejected(HookRejection { reason })
    }
}

/// A hook rejection whose reason is intentionally redacted from logs and debug output.
#[derive(Error)]
#[error("outbound message rejected by hook")]
pub struct HookRejection {
    reason: String,
}

impl HookRejection {
    /// Returns the trusted hook's bounded, user-facing rejection reason.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl fmt::Debug for HookRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HookRejection")
            .field(
                "reason",
                &format_args!("<redacted:{} bytes>", self.reason.len()),
            )
            .finish()
    }
}

/// A safe validation failure reported by the Python bootstrap.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Error, PartialEq, Serialize)]
#[serde(tag = "kind", content = "handler", rename_all = "snake_case")]
pub enum ValidationIssue {
    /// The script could not be loaded or raised during module initialization.
    #[error("hook script could not be loaded")]
    LoadFailed,
    /// The script defines none of the supported handlers.
    #[error("hook script defines no supported handlers")]
    NoHandlers,
    /// A recognized handler attribute is not callable.
    #[error("hook handler {0} is not callable")]
    HandlerNotCallable(HookEventKind),
    /// A recognized handler does not accept exactly one positional argument.
    #[error("hook handler {0} must accept exactly one positional argument")]
    InvalidSignature(HookEventKind),
}

/// Invalid hook configuration or script metadata.
#[derive(Debug, Error)]
pub enum ConfigurationError {
    /// A byte limit or timeout is zero or exceeds the supported hard bound.
    #[error("invalid hook configuration limit: {field}")]
    InvalidLimit {
        /// Stable configuration field name.
        field: &'static str,
    },
    /// Concurrency is zero or exceeds the supported hard bound.
    #[error("invalid hook max_concurrency value")]
    InvalidConcurrency,
    /// The script path cannot be represented by the JSON protocol.
    #[error("hook script path must be valid UTF-8")]
    NonUtf8ScriptPath,
    /// An inherited environment-variable name is not valid for a subprocess.
    #[error("hook environment allow-list contains an invalid variable name")]
    InvalidEnvironmentName,
    /// The configured script does not exist.
    #[error("hook script does not exist")]
    ScriptNotFound,
    /// Script metadata could not be read.
    #[error("could not read hook script metadata ({kind:?})")]
    ScriptMetadata {
        /// Coarse operating-system error kind; paths and messages are not retained.
        kind: io::ErrorKind,
    },
    /// The configured path does not identify a regular file.
    #[error("hook script path is not a regular file")]
    ScriptNotRegularFile,
    /// The script exceeds the configured source-size cap.
    #[error("hook script is {actual} bytes; maximum is {maximum} bytes")]
    ScriptTooLarge {
        /// Observed file size.
        actual: u64,
        /// Configured maximum size.
        maximum: usize,
    },
    /// Python rejected the script or handler contract during validation.
    #[error(transparent)]
    Validation(#[from] ValidationIssue),
    /// `dispatch` was incorrectly used for the mutating `before_send` event.
    #[error("before_send must be invoked through HookRuntime::before_send")]
    BeforeSendRequiresDedicatedMethod,
}

/// Captured child stream identifier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    /// JSON protocol input.
    Stdin,
    /// Protocol standard output.
    Stdout,
    /// Hook diagnostics on standard error.
    Stderr,
}

/// Python subprocess lifecycle failure.
#[derive(Debug, Error)]
pub enum ProcessError {
    /// The executable could not be spawned.
    #[error("could not start configured Python executable ({kind:?})")]
    Spawn {
        /// Coarse operating-system error kind; executable paths and messages are not retained.
        kind: io::ErrorKind,
    },
    /// A pipe could not be acquired from the spawned process.
    #[error("Python subprocess did not expose its {stream:?} pipe")]
    MissingPipe {
        /// Missing pipe.
        stream: StreamKind,
    },
    /// A bounded subprocess pipe operation failed.
    #[error("Python subprocess {stream:?} I/O failed ({kind:?})")]
    StreamIo {
        /// Affected stream.
        stream: StreamKind,
        /// Coarse operating-system error kind; raw messages are not retained.
        kind: io::ErrorKind,
    },
    /// A stream-reader task stopped unexpectedly.
    #[error("Python subprocess {stream:?} reader stopped unexpectedly")]
    StreamTask {
        /// Affected stream.
        stream: StreamKind,
    },
    /// The internal concurrency coordinator was closed unexpectedly.
    #[error("hook subprocess coordinator stopped unexpectedly")]
    CoordinatorStopped,
    /// The child could not be waited on.
    #[error("could not wait for Python subprocess ({kind:?})")]
    Wait {
        /// Coarse operating-system error kind.
        kind: io::ErrorKind,
    },
    /// The child exited unsuccessfully.
    #[error(
        "Python subprocess exited unsuccessfully (code {code:?}, {stderr_bytes} stderr bytes discarded)"
    )]
    UnsuccessfulExit {
        /// Platform exit code, if available.
        code: Option<i32>,
        /// Number of bounded diagnostic bytes discarded.
        stderr_bytes: usize,
    },
}

/// Field that a `before_send` response may modify.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModificationField {
    /// Destination identifier.
    Destination,
    /// Message text.
    Text,
    /// Rejection reason.
    RejectionReason,
}

/// Stable reason a hook-supplied value was rejected.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ModificationIssue {
    /// The value was empty when a non-empty value is required.
    Empty,
    /// The UTF-8 encoded value exceeded its configured byte cap.
    TooLarge,
    /// The value contained a NUL code point.
    ContainsNul,
}

/// Versioned runner protocol failure.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// Serialized input exceeded its configured cap.
    #[error(
        "hook protocol input exceeded its {maximum}-byte cap (observed at least {observed_at_least} bytes)"
    )]
    InputTooLarge {
        /// Lower bound observed when bounded serialization stopped.
        observed_at_least: usize,
        /// Configured maximum.
        maximum: usize,
    },
    /// A subprocess output stream exceeded its configured cap.
    #[error("hook subprocess {stream:?} exceeded its {maximum}-byte cap")]
    OutputTooLarge {
        /// Stream that exceeded its cap.
        stream: StreamKind,
        /// Configured maximum.
        maximum: usize,
    },
    /// Rust could not encode a request without exposing the raw serialization error.
    #[error("hook protocol request could not be encoded")]
    RequestEncoding,
    /// Standard output was not exactly one valid protocol response.
    #[error("hook subprocess produced malformed protocol output")]
    MalformedOutput,
    /// The response schema did not match the supported schema.
    #[error("hook subprocess returned an unsupported protocol schema")]
    SchemaMismatch,
    /// The response status was not valid for the requested operation.
    #[error("hook subprocess returned an unexpected protocol response")]
    UnexpectedResponse,
    /// The hook raised while handling an event.
    #[error("hook handler failed")]
    HandlerFailed,
    /// The bootstrap rejected an invocation request or handler return value.
    #[error("hook bootstrap rejected protocol data")]
    RunnerRejected,
    /// A modified value failed Rust-side validation.
    #[error("hook modification for {field:?} is invalid ({issue:?})")]
    InvalidModification {
        /// Invalid field.
        field: ModificationField,
        /// Stable validation issue.
        issue: ModificationIssue,
    },
}
