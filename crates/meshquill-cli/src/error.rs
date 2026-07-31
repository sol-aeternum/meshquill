//! Typed command failures and stable exit-status mapping.

use std::fmt;

use meshquill_core::{CoreError, TransportError};
use meshquill_hooks::HookError;
use meshquill_store::StoreError;

use crate::{
    output::{ExitStatus, OutputError},
    transport::CliTransportBuildError,
};

/// A sanitized command failure suitable for a terminal diagnostic.
#[derive(Debug)]
pub(crate) struct CliError {
    status: ExitStatus,
    message: String,
    hint: Option<String>,
}

impl CliError {
    pub(crate) fn new(status: ExitStatus, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
            hint: None,
        }
    }

    pub(crate) fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub(crate) const fn status(&self) -> ExitStatus {
        self.status
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    pub(crate) fn unsupported(area: &'static str) -> Self {
        Self::new(
            ExitStatus::Denied,
            format!("{area} is not supported by this Meshquill build"),
        )
        .with_hint("No device change was attempted.")
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CliError {}

impl From<CoreError> for CliError {
    fn from(error: CoreError) -> Self {
        match error {
            CoreError::Timeout | CoreError::Transport(TransportError::Timeout) => Self::new(
                ExitStatus::Timeout,
                "the bounded device operation timed out",
            )
            .with_hint("Check the selected profile and try a longer configured timeout."),
            CoreError::Transport(
                TransportError::NotConnected
                | TransportError::Closed
                | TransportError::ReconnectUnsupported
                | TransportError::ReconnectFailed { .. },
            )
            | CoreError::Disconnected
            | CoreError::ActorStopped => Self::new(
                ExitStatus::Connection,
                "the companion connection is unavailable",
            )
            .with_hint("Check the profile endpoint and whether another application owns it."),
            CoreError::Transport(TransportError::Io(_)) => Self::new(
                ExitStatus::Connection,
                "transport I/O failed while communicating with the companion",
            )
            .with_hint("Check device presence, endpoint reachability, and OS access."),
            CoreError::Transport(TransportError::Backpressure { queue, capacity }) => Self::new(
                ExitStatus::Protocol,
                format!("bounded transport queue '{queue}' is full (capacity {capacity})"),
            ),
            CoreError::Transport(TransportError::PayloadTooLarge { maximum, actual }) => Self::new(
                ExitStatus::Usage,
                format!("payload is {actual} bytes; the transport maximum is {maximum}"),
            ),
            CoreError::Parse(_)
            | CoreError::ProtocolInvariant(_)
            | CoreError::InvalidUtf8 { .. } => Self::new(
                ExitStatus::Protocol,
                "the companion returned data that violates the supported protocol",
            )
            .with_hint("Run `meshquill doctor --connect` and check firmware compatibility."),
            CoreError::InvalidArgument { field, message } => {
                Self::new(ExitStatus::Usage, format!("invalid {field}: {message}"))
            }
            CoreError::DeviceRejected { operation, code } => {
                let code = code.map_or_else(|| "unspecified".to_owned(), |value| value.to_string());
                Self::new(
                    ExitStatus::Denied,
                    format!("the companion rejected {operation} (code {code})"),
                )
            }
            CoreError::FeatureDisabled { feature } => Self::new(
                ExitStatus::Denied,
                format!("the companion reports that {feature} is disabled"),
            ),
            CoreError::AuthenticationFailed => {
                Self::new(ExitStatus::Authentication, "remote authentication failed")
                    .with_hint("Verify the target and credential source before retrying.")
            }
        }
    }
}

impl From<StoreError> for CliError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::Parse { path, .. } => Self::new(
                ExitStatus::Configuration,
                format!("configuration at {} is malformed", path.display()),
            )
            .with_hint("Run `meshquill config repair` to create a backup and recover defaults."),
            StoreError::UnsupportedVersion { version } => Self::new(
                ExitStatus::Configuration,
                format!("configuration schema version {version} is unsupported"),
            )
            .with_hint("Upgrade Meshquill or restore a compatible configuration backup."),
            StoreError::Validation { field, message } => Self::new(
                ExitStatus::Configuration,
                format!("invalid configuration field {field}: {message}"),
            ),
            StoreError::MissingRuntimePath { .. } => Self::new(
                ExitStatus::Configuration,
                "the platform configuration directory could not be resolved",
            )
            .with_hint("Set --config (or MESHQUILL_CONFIG) to an explicit file path."),
            StoreError::LockTimeout { path } => Self::new(
                ExitStatus::Configuration,
                format!("another process is still updating {}", path.display()),
            )
            .with_hint("Wait for the other Meshquill process to finish, then retry."),
            StoreError::PromptRequired => Self::new(
                ExitStatus::Authentication,
                "a credential requires an interactive prompt",
            ),
            StoreError::SecretUnavailable { backend, .. } => Self::new(
                ExitStatus::Authentication,
                format!("credential resolution through {backend} failed"),
            ),
            StoreError::Io(_) | StoreError::AtomicRename { .. } | StoreError::Serde { .. } => {
                Self::new(
                    ExitStatus::Configuration,
                    "configuration could not be read or written safely",
                )
                .with_hint("Check the selected path and its directory permissions.")
            }
        }
    }
}

impl From<CliTransportBuildError> for CliError {
    fn from(error: CliTransportBuildError) -> Self {
        match error {
            CliTransportBuildError::UnknownMockScenario { scenario } => Self::new(
                ExitStatus::Configuration,
                format!("unknown explicit mock scenario '{scenario}'"),
            )
            .with_hint(
                "Use scenario `demo`, `ack-timeout`, `reconnect-demo`, `reconnect-fail`, or `send-disconnect`.",
            ),
            CliTransportBuildError::InvalidTransportConfig { transport, message } => Self::new(
                ExitStatus::Configuration,
                format!("invalid {transport} profile: {message}"),
            ),
            CliTransportBuildError::MockFixture(_) => Self::new(
                ExitStatus::Protocol,
                "the deterministic demo fixture could not be prepared",
            ),
        }
    }
}

impl From<OutputError> for CliError {
    fn from(error: OutputError) -> Self {
        match error {
            OutputError::JsonForStream
            | OutputError::JsonlForSingleResult
            | OutputError::MachineModeForRaw => Self::new(ExitStatus::Usage, error.to_string()),
            OutputError::Io(io_error) if io_error.kind() == std::io::ErrorKind::BrokenPipe => {
                Self::new(ExitStatus::Success, "stdout was closed")
            }
            OutputError::Io(_) => Self::new(ExitStatus::Protocol, "could not write command output"),
            OutputError::Json(_) => {
                Self::new(ExitStatus::Protocol, "could not encode command output")
            }
        }
    }
}

impl From<HookError> for CliError {
    fn from(error: HookError) -> Self {
        Self::new(ExitStatus::Hook, error.to_string())
    }
}
