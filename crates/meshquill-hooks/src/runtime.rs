use std::{
    collections::BTreeSet,
    fmt,
    io::{self, Write},
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    sync::Semaphore,
    task::JoinHandle,
    time::{Instant, sleep_until, timeout_at},
};
use tracing::{debug, warn};

use crate::{
    BeforeSendInput, BeforeSendOutcome, ConfigurationError, HookError, HookErrorCategory,
    HookEvent, HookEventKind, ModificationField, ModificationIssue, PROTOCOL_SCHEMA, ProcessError,
    ProtocolError, StreamKind, ValidationIssue,
    protocol::{PayloadRef, payload_ref},
};

const BOOTSTRAP: &str = include_str!("bootstrap.py");
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_SCRIPT_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_INPUT_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_OUTPUT_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_STDERR_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_DESTINATION_BYTES: usize = 4 * 1024;
const DEFAULT_MAX_TEXT_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_REJECTION_REASON_BYTES: usize = 4 * 1024;
const DEFAULT_MAX_CONCURRENCY: usize = 4;
const HARD_MAX_BYTES: usize = 64 * 1024 * 1024;
const HARD_MAX_CONCURRENCY: usize = 1024;
const SAFE_ENVIRONMENT: &[&str] = &[
    "PATH",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TZ",
    "TMPDIR",
    "SYSTEMROOT",
    "WINDIR",
];

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);

/// Policy applied when a hook operation fails.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailurePolicy {
    /// Continue the application operation and record the hook failure in its report.
    Open,
    /// Stop the application operation by returning the hook error.
    Closed,
}

/// Policy controlling which parent environment variables reach Python.
///
/// Fixed interpreter settings (`PYTHONIOENCODING`, `PYTHONUTF8`, and
/// `PYTHONDONTWRITEBYTECODE`) are always supplied. Python is launched in isolated mode, so
/// `PYTHON*` variables inherited from the parent cannot alter interpreter startup.
#[derive(Clone, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "mode", content = "variables", rename_all = "snake_case")]
pub enum EnvironmentPolicy {
    /// Clear the inherited environment completely.
    Clear,
    /// Inherit only a built-in allow-list of locale, time-zone, temporary-directory, and
    /// executable-search-path variables.
    #[default]
    SafeInherited,
    /// Inherit values for exactly the named variables.
    AllowList(BTreeSet<String>),
}

impl fmt::Debug for EnvironmentPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clear => formatter.write_str("Clear"),
            Self::SafeInherited => formatter.write_str("SafeInherited"),
            Self::AllowList(names) => formatter
                .debug_tuple("AllowList")
                .field(&format_args!("<{} variable names>", names.len()))
                .finish(),
        }
    }
}

/// Configuration for one trusted local Python hook script.
///
/// The default failure policy is fail-open for observational events and fail-closed for
/// `before_send`. Paths and hook-controlled data are redacted from [`fmt::Debug`].
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct HookConfig {
    /// Python executable to launch directly, without a shell.
    pub python_executable: PathBuf,
    /// Trusted local Python script.
    pub script: PathBuf,
    /// Complete operation timeout, including metadata checks, concurrency-queue wait, child
    /// execution, and pipe I/O.
    pub timeout: Duration,
    /// Maximum accepted script source size.
    pub max_script_bytes: usize,
    /// Maximum serialized runner request size.
    pub max_input_bytes: usize,
    /// Maximum protocol stdout size.
    pub max_output_bytes: usize,
    /// Maximum discarded diagnostic stderr size.
    pub max_stderr_bytes: usize,
    /// Maximum concurrently running subprocesses for this runtime.
    pub max_concurrency: usize,
    /// Maximum UTF-8 byte length of an original or modified destination.
    pub max_destination_bytes: usize,
    /// Maximum UTF-8 byte length of original or modified message text.
    pub max_text_bytes: usize,
    /// Maximum UTF-8 byte length of a rejection reason.
    pub max_rejection_reason_bytes: usize,
    /// Parent environment inheritance policy.
    pub environment: EnvironmentPolicy,
    /// Failure policy for all observational events.
    pub observational_failure: FailurePolicy,
    /// Failure policy for the mutating `before_send` event.
    pub before_send_failure: FailurePolicy,
}

impl HookConfig {
    /// Returns a configuration with conservative limits and `python3` as the executable.
    #[must_use]
    pub fn new(script: impl Into<PathBuf>) -> Self {
        Self {
            python_executable: PathBuf::from("python3"),
            script: script.into(),
            timeout: DEFAULT_TIMEOUT,
            max_script_bytes: DEFAULT_MAX_SCRIPT_BYTES,
            max_input_bytes: DEFAULT_MAX_INPUT_BYTES,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            max_stderr_bytes: DEFAULT_MAX_STDERR_BYTES,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            max_destination_bytes: DEFAULT_MAX_DESTINATION_BYTES,
            max_text_bytes: DEFAULT_MAX_TEXT_BYTES,
            max_rejection_reason_bytes: DEFAULT_MAX_REJECTION_REASON_BYTES,
            environment: EnvironmentPolicy::default(),
            observational_failure: FailurePolicy::Open,
            before_send_failure: FailurePolicy::Closed,
        }
    }
}

impl fmt::Debug for HookConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HookConfig")
            .field("python_executable", &"<redacted path>")
            .field("script", &"<redacted path>")
            .field("timeout", &self.timeout)
            .field("max_script_bytes", &self.max_script_bytes)
            .field("max_input_bytes", &self.max_input_bytes)
            .field("max_output_bytes", &self.max_output_bytes)
            .field("max_stderr_bytes", &self.max_stderr_bytes)
            .field("max_concurrency", &self.max_concurrency)
            .field("max_destination_bytes", &self.max_destination_bytes)
            .field("max_text_bytes", &self.max_text_bytes)
            .field(
                "max_rejection_reason_bytes",
                &self.max_rejection_reason_bytes,
            )
            .field("environment", &self.environment)
            .field("observational_failure", &self.observational_failure)
            .field("before_send_failure", &self.before_send_failure)
            .finish()
    }
}

/// Handlers discovered by an isolated validation subprocess.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HookValidation {
    /// Valid, callable handlers defined by the script.
    pub handlers: BTreeSet<HookEventKind>,
}

/// Result status for an observational hook dispatch.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum HookExecutionStatus {
    /// The handler completed; its return value, if any, was discarded.
    Completed,
    /// The script did not define the handler.
    Missing,
    /// Execution failed, but the configured policy allowed the application to continue.
    FailedOpen {
        /// Stable category of the discarded error.
        category: HookErrorCategory,
    },
}

/// Redacted report from an observational event dispatch.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct HookReport {
    /// Unique identifier included in the hook envelope.
    pub event_id: String,
    /// Event that was dispatched.
    pub event: HookEventKind,
    /// Dispatch result.
    pub status: HookExecutionStatus,
}

impl fmt::Debug for HookReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HookReport")
            .field("event_id", &"<redacted>")
            .field("event", &self.event)
            .field("status", &self.status)
            .finish()
    }
}

/// Failure-isolated runner for one trusted local Python hook script.
///
/// This type is cheap to clone. Clones share the configured concurrency bound. Construction does
/// not inspect the filesystem, resolve Python, or start a process.
#[derive(Clone)]
pub struct HookRuntime {
    inner: Arc<RuntimeInner>,
}

struct RuntimeInner {
    config: HookConfig,
    permits: Arc<Semaphore>,
    runtime_id: u64,
    next_event_id: AtomicU64,
}

impl fmt::Debug for HookRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HookRuntime")
            .field("config", &self.inner.config)
            .field("available_permits", &self.inner.permits.available_permits())
            .finish_non_exhaustive()
    }
}

impl HookRuntime {
    /// Constructs a runtime without filesystem access or Python startup.
    ///
    /// # Errors
    ///
    /// Returns [`HookError::Configuration`] when a cap, concurrency value, environment variable
    /// name, or JSON-incompatible script path is invalid.
    pub fn new(config: HookConfig) -> Result<Self, HookError> {
        validate_config(&config)?;
        let permits = Arc::new(Semaphore::new(config.max_concurrency));
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                config,
                permits,
                runtime_id: NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed),
                next_event_id: AtomicU64::new(1),
            }),
        })
    }

    /// Loads the script and validates every recognized handler in a fresh Python subprocess.
    ///
    /// A handler must be callable and accept exactly one positional argument. A script with no
    /// recognized handler is rejected.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration, timeout, process, or protocol error. Script exception text
    /// and standard error content are deliberately not retained.
    pub async fn validate(&self) -> Result<HookValidation, HookError> {
        let script = self.script_as_str()?;
        let request = ValidateRequest {
            schema: PROTOCOL_SCHEMA,
            operation: RunnerOperation::Validate,
            script,
        };
        let response = self.run_request(&request).await?;
        match response.body {
            RunnerResponseBody::Validated { handlers } => Ok(HookValidation {
                handlers: handlers.into_iter().collect(),
            }),
            RunnerResponseBody::Error { error } => Err(map_runner_error(&error, true)),
            RunnerResponseBody::Invoked { .. } | RunnerResponseBody::Missing => {
                Err(ProtocolError::UnexpectedResponse.into())
            }
        }
    }

    /// Dispatches one observational event in a fresh Python subprocess.
    ///
    /// Handler return values are discarded. Under [`FailurePolicy::Open`], failures become a
    /// [`HookExecutionStatus::FailedOpen`] report and a redacted warning; under
    /// [`FailurePolicy::Closed`], the error is returned. Use [`Self::before_send`] for the sole
    /// mutating event.
    ///
    /// # Errors
    ///
    /// Returns an error for `before_send`, or when execution fails under a closed policy.
    pub async fn dispatch(&self, event: HookEvent) -> Result<HookReport, HookError> {
        let event_kind = event.kind();
        if event_kind == HookEventKind::BeforeSend {
            return Err(ConfigurationError::BeforeSendRequiresDedicatedMethod.into());
        }
        let (event_id, envelope) = self.envelope(&event);
        match self.invoke(envelope).await {
            Ok(Invocation::Completed(_)) => Ok(HookReport {
                event_id,
                event: event_kind,
                status: HookExecutionStatus::Completed,
            }),
            Ok(Invocation::Missing) => Ok(HookReport {
                event_id,
                event: event_kind,
                status: HookExecutionStatus::Missing,
            }),
            Err(error) if self.inner.config.observational_failure == FailurePolicy::Open => {
                let category = error.category();
                warn!(
                    event = %event_kind,
                    category = ?category,
                    "observational hook failed open; details omitted"
                );
                Ok(HookReport {
                    event_id,
                    event: event_kind,
                    status: HookExecutionStatus::FailedOpen { category },
                })
            }
            Err(error) => Err(error),
        }
    }

    /// Invokes the mutating `before_send` hook in a fresh Python subprocess.
    ///
    /// A hook may allow the original message, replace its destination and/or text, or reject it.
    /// Modified values and rejection reasons are independently bounded and validated in Rust.
    /// A missing handler is equivalent to [`BeforeSendOutcome::Allow`]. Under a fail-open policy,
    /// execution failures also become `Allow`; the default policy is fail-closed.
    ///
    /// # Errors
    ///
    /// Returns a typed configuration, timeout, process, or protocol error when the configured
    /// policy is closed.
    pub async fn before_send(
        &self,
        input: BeforeSendInput,
    ) -> Result<BeforeSendOutcome, HookError> {
        let original = input.clone();
        if let Err(error) = self.validate_send_input(&input) {
            return self.apply_before_send_failure(error);
        }
        let event = HookEvent::BeforeSend(input);
        let (_, envelope) = self.envelope(&event);
        match self.invoke(envelope).await {
            Ok(Invocation::Missing) => Ok(BeforeSendOutcome::Allow),
            Ok(Invocation::Completed(Some(result))) => {
                match self.apply_before_send_result(&original, result) {
                    Ok(outcome) => Ok(outcome),
                    Err(error) => self.apply_before_send_failure(error),
                }
            }
            Ok(Invocation::Completed(None)) => {
                self.apply_before_send_failure(ProtocolError::UnexpectedResponse.into())
            }
            Err(error) => self.apply_before_send_failure(error),
        }
    }

    fn apply_before_send_failure(&self, error: HookError) -> Result<BeforeSendOutcome, HookError> {
        if self.inner.config.before_send_failure == FailurePolicy::Open {
            warn!(
                category = ?error.category(),
                "before_send hook failed open; details omitted"
            );
            Ok(BeforeSendOutcome::Allow)
        } else {
            Err(error)
        }
    }

    fn validate_send_input(&self, input: &BeforeSendInput) -> Result<(), HookError> {
        validate_modified_string(
            &input.destination,
            ModificationField::Destination,
            self.inner.config.max_destination_bytes,
            true,
        )?;
        validate_modified_string(
            &input.text,
            ModificationField::Text,
            self.inner.config.max_text_bytes,
            false,
        )
    }

    fn apply_before_send_result(
        &self,
        original: &BeforeSendInput,
        result: RawBeforeSendResult,
    ) -> Result<BeforeSendOutcome, HookError> {
        match result {
            RawBeforeSendResult::Allow => Ok(BeforeSendOutcome::Allow),
            RawBeforeSendResult::Modify { destination, text } => {
                if destination.is_none() && text.is_none() {
                    return Err(ProtocolError::UnexpectedResponse.into());
                }
                let destination = destination.unwrap_or_else(|| original.destination.clone());
                let text = text.unwrap_or_else(|| original.text.clone());
                validate_modified_string(
                    &destination,
                    ModificationField::Destination,
                    self.inner.config.max_destination_bytes,
                    true,
                )?;
                validate_modified_string(
                    &text,
                    ModificationField::Text,
                    self.inner.config.max_text_bytes,
                    false,
                )?;
                Ok(BeforeSendOutcome::Modify { destination, text })
            }
            RawBeforeSendResult::Reject { reason } => {
                validate_modified_string(
                    &reason,
                    ModificationField::RejectionReason,
                    self.inner.config.max_rejection_reason_bytes,
                    true,
                )?;
                Ok(BeforeSendOutcome::Reject { reason })
            }
        }
    }

    fn envelope<'a>(&self, event: &'a HookEvent) -> (String, EventEnvelope<'a>) {
        let timestamp = unix_timestamp_millis();
        let sequence = self.inner.next_event_id.fetch_add(1, Ordering::Relaxed);
        let event_id = format!("{timestamp}-{}-{sequence}", self.inner.runtime_id);
        (
            event_id.clone(),
            EventEnvelope {
                schema: PROTOCOL_SCHEMA,
                event_id,
                timestamp,
                event: event.kind(),
                payload: payload_ref(event),
            },
        )
    }

    async fn invoke(&self, envelope: EventEnvelope<'_>) -> Result<Invocation, HookError> {
        let script = self.script_as_str()?;
        let event = envelope.event;
        let request = InvokeRequest {
            schema: PROTOCOL_SCHEMA,
            operation: RunnerOperation::Invoke,
            script,
            envelope,
        };
        let response = self.run_request(&request).await?;
        match response.body {
            RunnerResponseBody::Invoked { result } => {
                if event == HookEventKind::BeforeSend {
                    Ok(Invocation::Completed(result))
                } else if result.is_none() {
                    Ok(Invocation::Completed(None))
                } else {
                    Err(ProtocolError::UnexpectedResponse.into())
                }
            }
            RunnerResponseBody::Missing => Ok(Invocation::Missing),
            RunnerResponseBody::Error { error } => Err(map_runner_error(&error, false)),
            RunnerResponseBody::Validated { .. } => Err(ProtocolError::UnexpectedResponse.into()),
        }
    }

    async fn run_request<T>(&self, request: &T) -> Result<RunnerResponse, HookError>
    where
        T: Serialize + ?Sized,
    {
        let deadline = Instant::now() + self.inner.config.timeout;
        match timeout_at(deadline, self.validate_script_file()).await {
            Ok(result) => result?,
            Err(_) => return Err(self.timeout_error()),
        }
        let input = encode_request(request, self.inner.config.max_input_bytes)?;
        if Instant::now() >= deadline {
            return Err(self.timeout_error());
        }

        let _permit = match timeout_at(deadline, self.inner.permits.clone().acquire_owned()).await {
            Ok(result) => result.map_err(|_| ProcessError::CoordinatorStopped)?,
            Err(_) => return Err(self.timeout_error()),
        };
        if Instant::now() >= deadline {
            return Err(self.timeout_error());
        }
        let mut child = self.spawn()?;
        if Instant::now() >= deadline {
            terminate_unconnected(&mut child).await;
            return Err(self.timeout_error());
        }
        let output = communicate(child, input, &self.inner.config, deadline).await?;
        if Instant::now() >= deadline {
            return Err(self.timeout_error());
        }
        let response: RunnerResponse =
            serde_json::from_slice(&output).map_err(|_| ProtocolError::MalformedOutput)?;
        if Instant::now() >= deadline {
            return Err(self.timeout_error());
        }
        if response.schema != PROTOCOL_SCHEMA {
            return Err(ProtocolError::SchemaMismatch.into());
        }
        Ok(response)
    }

    async fn validate_script_file(&self) -> Result<(), HookError> {
        let metadata = match tokio::fs::metadata(&self.inner.config.script).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(ConfigurationError::ScriptNotFound.into());
            }
            Err(error) => {
                return Err(ConfigurationError::ScriptMetadata { kind: error.kind() }.into());
            }
        };
        if !metadata.is_file() {
            return Err(ConfigurationError::ScriptNotRegularFile.into());
        }
        if metadata.len() > usize_to_u64(self.inner.config.max_script_bytes) {
            return Err(ConfigurationError::ScriptTooLarge {
                actual: metadata.len(),
                maximum: self.inner.config.max_script_bytes,
            }
            .into());
        }
        Ok(())
    }

    fn script_as_str(&self) -> Result<&str, HookError> {
        self.inner
            .config
            .script
            .to_str()
            .ok_or_else(|| ConfigurationError::NonUtf8ScriptPath.into())
    }

    fn spawn(&self) -> Result<Child, HookError> {
        let mut command = Command::new(&self.inner.config.python_executable);
        command
            .arg("-I")
            .arg("-B")
            .arg("-c")
            .arg(BOOTSTRAP)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .env_clear();
        for name in inherited_environment_names(&self.inner.config.environment) {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        command
            .env("PYTHONIOENCODING", "utf-8")
            .env("PYTHONUTF8", "1")
            .env("PYTHONDONTWRITEBYTECODE", "1");
        command
            .spawn()
            .map_err(|error| ProcessError::Spawn { kind: error.kind() }.into())
    }

    fn timeout_error(&self) -> HookError {
        HookError::Timeout {
            timeout: self.inner.config.timeout,
        }
    }
}

#[derive(Serialize)]
struct ValidateRequest<'a> {
    schema: &'static str,
    operation: RunnerOperation,
    script: &'a str,
}

#[derive(Serialize)]
struct InvokeRequest<'script, 'payload> {
    schema: &'static str,
    operation: RunnerOperation,
    script: &'script str,
    envelope: EventEnvelope<'payload>,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum RunnerOperation {
    Validate,
    Invoke,
}

#[derive(Serialize)]
struct EventEnvelope<'a> {
    schema: &'static str,
    event_id: String,
    timestamp: u64,
    event: HookEventKind,
    payload: PayloadRef<'a>,
}

#[derive(Deserialize)]
struct RunnerResponse {
    schema: String,
    #[serde(flatten)]
    body: RunnerResponseBody,
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum RunnerResponseBody {
    Validated {
        handlers: Vec<HookEventKind>,
    },
    Invoked {
        #[serde(default)]
        result: Option<RawBeforeSendResult>,
    },
    Missing,
    Error {
        error: RunnerFault,
    },
}

#[derive(Deserialize)]
struct RunnerFault {
    kind: RunnerFaultKind,
    handler: Option<HookEventKind>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RunnerFaultKind {
    LoadError,
    NoHandlers,
    NotCallable,
    InvalidSignature,
    HookException,
    InvalidResult,
    InvalidRequest,
    InternalError,
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum RawBeforeSendResult {
    Allow,
    Modify {
        #[serde(default)]
        destination: Option<String>,
        #[serde(default)]
        text: Option<String>,
    },
    Reject {
        reason: String,
    },
}

enum Invocation {
    Completed(Option<RawBeforeSendResult>),
    Missing,
}

enum BoundedRead {
    Complete(Vec<u8>),
    LimitExceeded,
}

struct ReaderTasks {
    stdout: JoinHandle<Result<BoundedRead, std::io::Error>>,
    stderr: JoinHandle<Result<BoundedRead, std::io::Error>>,
}

impl ReaderTasks {
    async fn terminate(&mut self, child: &mut Child, stdout_consumed: bool, stderr_consumed: bool) {
        self.stdout.abort();
        self.stderr.abort();
        let _ = child.start_kill();
        if !stdout_consumed {
            let _ = (&mut self.stdout).await;
        }
        if !stderr_consumed {
            let _ = (&mut self.stderr).await;
        }
        let _ = child.wait().await;
    }
}

struct BoundedJsonWriter {
    bytes: Vec<u8>,
    maximum: usize,
    observed_at_least: usize,
    exceeded: bool,
}

impl BoundedJsonWriter {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(maximum.min(8 * 1024)),
            maximum,
            observed_at_least: 0,
            exceeded: false,
        }
    }
}

impl Write for BoundedJsonWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let attempted = self.bytes.len().saturating_add(buffer.len());
        self.observed_at_least = self.observed_at_least.max(attempted);
        if attempted > self.maximum {
            self.exceeded = true;
            return Err(io::Error::other("hook JSON input limit exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn encode_request<T>(request: &T, maximum: usize) -> Result<Vec<u8>, HookError>
where
    T: Serialize + ?Sized,
{
    let mut writer = BoundedJsonWriter::new(maximum);
    match serde_json::to_writer(&mut writer, request) {
        Ok(()) => Ok(writer.bytes),
        Err(_) if writer.exceeded => Err(ProtocolError::InputTooLarge {
            observed_at_least: writer.observed_at_least,
            maximum,
        }
        .into()),
        Err(_) => Err(ProtocolError::RequestEncoding.into()),
    }
}

async fn communicate(
    mut child: Child,
    input: Vec<u8>,
    config: &HookConfig,
    deadline: Instant,
) -> Result<Vec<u8>, HookError> {
    let mut readers = start_communication(&mut child, &input, config, deadline).await?;

    let mut stdout_bytes = None;
    let mut stderr_bytes = None;
    let mut exit_status = None;
    loop {
        tokio::select! {
            stdout_result = &mut readers.stdout, if stdout_bytes.is_none() => {
                match unpack_reader(stdout_result, StreamKind::Stdout) {
                    Ok(BoundedRead::Complete(bytes)) => stdout_bytes = Some(bytes),
                    Ok(BoundedRead::LimitExceeded) => {
                        let stderr_consumed = stderr_bytes.is_some();
                        readers.terminate(&mut child, true, stderr_consumed).await;
                        return Err(ProtocolError::OutputTooLarge {
                            stream: StreamKind::Stdout,
                            maximum: config.max_output_bytes,
                        }.into());
                    }
                    Err(error) => {
                        let stderr_consumed = stderr_bytes.is_some();
                        readers.terminate(&mut child, true, stderr_consumed).await;
                        return Err(error);
                    }
                }
            }
            stderr_result = &mut readers.stderr, if stderr_bytes.is_none() => {
                match unpack_reader(stderr_result, StreamKind::Stderr) {
                    Ok(BoundedRead::Complete(bytes)) => stderr_bytes = Some(bytes),
                    Ok(BoundedRead::LimitExceeded) => {
                        let stdout_consumed = stdout_bytes.is_some();
                        readers.terminate(&mut child, stdout_consumed, true).await;
                        return Err(ProtocolError::OutputTooLarge {
                            stream: StreamKind::Stderr,
                            maximum: config.max_stderr_bytes,
                        }.into());
                    }
                    Err(error) => {
                        let stdout_consumed = stdout_bytes.is_some();
                        readers.terminate(&mut child, stdout_consumed, true).await;
                        return Err(error);
                    }
                }
            }
            status_result = child.wait(), if exit_status.is_none() => {
                match status_result {
                    Ok(status) => exit_status = Some(status),
                    Err(error) => {
                        let error = ProcessError::Wait { kind: error.kind() };
                        let stdout_consumed = stdout_bytes.is_some();
                        let stderr_consumed = stderr_bytes.is_some();
                        readers
                            .terminate(&mut child, stdout_consumed, stderr_consumed)
                            .await;
                        return Err(error.into());
                    }
                }
            }
            () = sleep_until(deadline) => {
                let stdout_consumed = stdout_bytes.is_some();
                let stderr_consumed = stderr_bytes.is_some();
                readers
                    .terminate(&mut child, stdout_consumed, stderr_consumed)
                    .await;
                return Err(HookError::Timeout { timeout: config.timeout });
            }
        }
        if stdout_bytes.is_some() && stderr_bytes.is_some() && exit_status.is_some() {
            break;
        }
    }

    finish_communication(stdout_bytes, stderr_bytes, exit_status)
}

async fn start_communication(
    child: &mut Child,
    input: &[u8],
    config: &HookConfig,
    deadline: Instant,
) -> Result<ReaderTasks, HookError> {
    let Some(mut stdin) = child.stdin.take() else {
        terminate_unconnected(child).await;
        return Err(ProcessError::MissingPipe {
            stream: StreamKind::Stdin,
        }
        .into());
    };
    let Some(stdout) = child.stdout.take() else {
        drop(stdin);
        terminate_unconnected(child).await;
        return Err(ProcessError::MissingPipe {
            stream: StreamKind::Stdout,
        }
        .into());
    };
    let Some(stderr) = child.stderr.take() else {
        drop(stdin);
        drop(stdout);
        terminate_unconnected(child).await;
        return Err(ProcessError::MissingPipe {
            stream: StreamKind::Stderr,
        }
        .into());
    };

    let mut readers = ReaderTasks {
        stdout: tokio::spawn(read_bounded(stdout, config.max_output_bytes)),
        stderr: tokio::spawn(read_bounded(stderr, config.max_stderr_bytes)),
    };
    let write_result = timeout_at(deadline, async {
        stdin.write_all(input).await?;
        stdin.shutdown().await
    })
    .await;
    drop(stdin);
    match write_result {
        Err(_) => {
            readers.terminate(child, false, false).await;
            return Err(HookError::Timeout {
                timeout: config.timeout,
            });
        }
        Ok(Err(error)) => {
            readers.terminate(child, false, false).await;
            return Err(ProcessError::StreamIo {
                stream: StreamKind::Stdin,
                kind: error.kind(),
            }
            .into());
        }
        Ok(Ok(())) => {}
    }
    Ok(readers)
}

fn finish_communication(
    stdout_bytes: Option<Vec<u8>>,
    stderr_bytes: Option<Vec<u8>>,
    exit_status: Option<std::process::ExitStatus>,
) -> Result<Vec<u8>, HookError> {
    let stdout_bytes = stdout_bytes.ok_or(ProcessError::StreamTask {
        stream: StreamKind::Stdout,
    })?;
    let stderr_bytes = stderr_bytes.ok_or(ProcessError::StreamTask {
        stream: StreamKind::Stderr,
    })?;
    let status = exit_status.ok_or(ProcessError::CoordinatorStopped)?;
    if !status.success() {
        return Err(ProcessError::UnsuccessfulExit {
            code: status.code(),
            stderr_bytes: stderr_bytes.len(),
        }
        .into());
    }
    if !stderr_bytes.is_empty() {
        debug!(
            stderr_bytes = stderr_bytes.len(),
            "hook subprocess stderr discarded"
        );
    }
    Ok(stdout_bytes)
}

async fn read_bounded(
    reader: impl AsyncRead + Unpin,
    maximum: usize,
) -> Result<BoundedRead, std::io::Error> {
    let limit = usize_to_u64(maximum).saturating_add(1);
    let mut bytes = Vec::with_capacity(maximum.min(8 * 1024).saturating_add(1));
    reader.take(limit).read_to_end(&mut bytes).await?;
    if bytes.len() > maximum {
        Ok(BoundedRead::LimitExceeded)
    } else {
        Ok(BoundedRead::Complete(bytes))
    }
}

fn unpack_reader(
    result: Result<Result<BoundedRead, std::io::Error>, tokio::task::JoinError>,
    stream: StreamKind,
) -> Result<BoundedRead, HookError> {
    match result {
        Ok(Ok(read)) => Ok(read),
        Ok(Err(error)) => Err(ProcessError::StreamIo {
            stream,
            kind: error.kind(),
        }
        .into()),
        Err(_) => Err(ProcessError::StreamTask { stream }.into()),
    }
}

async fn terminate_unconnected(child: &mut Child) {
    let _ = child.start_kill();
    let _ = child.wait().await;
}

fn map_runner_error(error: &RunnerFault, validation: bool) -> HookError {
    match error.kind {
        RunnerFaultKind::LoadError => {
            ConfigurationError::Validation(ValidationIssue::LoadFailed).into()
        }
        RunnerFaultKind::NoHandlers => {
            ConfigurationError::Validation(ValidationIssue::NoHandlers).into()
        }
        RunnerFaultKind::NotCallable => match error.handler {
            Some(handler) => {
                ConfigurationError::Validation(ValidationIssue::HandlerNotCallable(handler)).into()
            }
            None => ProtocolError::UnexpectedResponse.into(),
        },
        RunnerFaultKind::InvalidSignature => match error.handler {
            Some(handler) => {
                ConfigurationError::Validation(ValidationIssue::InvalidSignature(handler)).into()
            }
            None => ProtocolError::UnexpectedResponse.into(),
        },
        RunnerFaultKind::HookException if !validation => ProtocolError::HandlerFailed.into(),
        RunnerFaultKind::HookException
        | RunnerFaultKind::InvalidResult
        | RunnerFaultKind::InvalidRequest
        | RunnerFaultKind::InternalError
        | RunnerFaultKind::Unknown => ProtocolError::RunnerRejected.into(),
    }
}

fn validate_config(config: &HookConfig) -> Result<(), ConfigurationError> {
    if config.script.to_str().is_none() {
        return Err(ConfigurationError::NonUtf8ScriptPath);
    }
    validate_byte_limit("max_script_bytes", config.max_script_bytes)?;
    validate_byte_limit("max_input_bytes", config.max_input_bytes)?;
    validate_byte_limit("max_output_bytes", config.max_output_bytes)?;
    validate_byte_limit("max_stderr_bytes", config.max_stderr_bytes)?;
    validate_byte_limit("max_destination_bytes", config.max_destination_bytes)?;
    validate_byte_limit("max_text_bytes", config.max_text_bytes)?;
    validate_byte_limit(
        "max_rejection_reason_bytes",
        config.max_rejection_reason_bytes,
    )?;
    if config.timeout.is_zero() {
        return Err(ConfigurationError::InvalidLimit { field: "timeout" });
    }
    if config.max_concurrency == 0 || config.max_concurrency > HARD_MAX_CONCURRENCY {
        return Err(ConfigurationError::InvalidConcurrency);
    }
    if let EnvironmentPolicy::AllowList(names) = &config.environment
        && names
            .iter()
            .any(|name| name.is_empty() || name.contains('=') || name.contains('\0'))
    {
        return Err(ConfigurationError::InvalidEnvironmentName);
    }
    Ok(())
}

fn validate_byte_limit(field: &'static str, value: usize) -> Result<(), ConfigurationError> {
    if value == 0 || value > HARD_MAX_BYTES {
        Err(ConfigurationError::InvalidLimit { field })
    } else {
        Ok(())
    }
}

fn validate_modified_string(
    value: &str,
    field: ModificationField,
    maximum: usize,
    require_nonempty: bool,
) -> Result<(), HookError> {
    if value.len() > maximum {
        return Err(ProtocolError::InvalidModification {
            field,
            issue: ModificationIssue::TooLarge,
        }
        .into());
    }
    if require_nonempty && value.trim().is_empty() {
        return Err(ProtocolError::InvalidModification {
            field,
            issue: ModificationIssue::Empty,
        }
        .into());
    }
    if value.contains('\0') {
        return Err(ProtocolError::InvalidModification {
            field,
            issue: ModificationIssue::ContainsNul,
        }
        .into());
    }
    Ok(())
}

fn inherited_environment_names(policy: &EnvironmentPolicy) -> Vec<&str> {
    match policy {
        EnvironmentPolicy::Clear => Vec::new(),
        EnvironmentPolicy::SafeInherited => SAFE_ENVIRONMENT.to_vec(),
        EnvironmentPolicy::AllowList(names) => names.iter().map(String::as_str).collect(),
    }
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{future, process::Stdio, time::Duration};

    use super::*;

    fn spawn_test_process(stdin: Stdio, stdout: Stdio, stderr: Stdio) -> Child {
        let executable = std::env::current_exe().expect("current test executable");
        let mut command = Command::new(executable);
        command
            .arg("--list")
            .stdin(stdin)
            .stdout(stdout)
            .stderr(stderr)
            .kill_on_drop(true);
        command.spawn().expect("spawn test subprocess")
    }

    async fn assert_missing_pipe_cleanup(missing: StreamKind) {
        let (stdin, stdout, stderr) = match missing {
            StreamKind::Stdin => (Stdio::null(), Stdio::piped(), Stdio::piped()),
            StreamKind::Stdout => (Stdio::piped(), Stdio::null(), Stdio::piped()),
            StreamKind::Stderr => (Stdio::piped(), Stdio::piped(), Stdio::null()),
        };
        let mut child = spawn_test_process(stdin, stdout, stderr);
        let config = HookConfig::new("unused.py");
        let result = start_communication(
            &mut child,
            b"{}",
            &config,
            Instant::now() + Duration::from_secs(2),
        )
        .await;
        let Err(error) = result else {
            panic!("missing pipe must fail");
        };
        assert!(matches!(
            error,
            HookError::Process(ProcessError::MissingPipe { stream }) if stream == missing
        ));
        assert!(
            child.id().is_none(),
            "missing-pipe cleanup must wait for the child"
        );
    }

    #[tokio::test]
    async fn every_partial_pipe_acquisition_failure_reaps_the_child() {
        assert_missing_pipe_cleanup(StreamKind::Stdin).await;
        assert_missing_pipe_cleanup(StreamKind::Stdout).await;
        assert_missing_pipe_cleanup(StreamKind::Stderr).await;
    }

    #[tokio::test]
    async fn termination_awaits_aborted_reader_tasks_and_reaps_the_child() {
        let mut child = spawn_test_process(Stdio::null(), Stdio::null(), Stdio::null());
        let mut readers = ReaderTasks {
            stdout: tokio::spawn(future::pending()),
            stderr: tokio::spawn(future::pending()),
        };

        readers.terminate(&mut child, false, false).await;

        assert!(readers.stdout.is_finished());
        assert!(readers.stderr.is_finished());
        assert!(child.id().is_none());
    }
}
