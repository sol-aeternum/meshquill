use std::{io::Write, path::Path};

use meshquill_hooks::{
    AfterSendPayload, BeforeSendInput, BeforeSendOutcome, ContactChange, HookConfig, HookEvent,
    HookExecutionStatus, HookRuntime, OnAckPayload, OnConnectPayload, OnContactUpdatePayload,
    OnDisconnectPayload, OnErrorPayload, OnMessagePayload, OnTimeoutPayload, PROTOCOL_SCHEMA,
};
use meshquill_store::{Config as StoreConfig, HookFailurePolicy, LoadOutcome};
use serde::Serialize;

use crate::{
    args::{Cli, HooksCommand},
    config::{config_store, load_optional},
    error::CliError,
    output::{ExitStatus, OutputWriter},
};

#[derive(Debug, Serialize)]
struct HookStatusReport {
    protocol: &'static str,
    enabled: bool,
    configured: bool,
    observational_failure: HookFailurePolicy,
    before_send_failure: HookFailurePolicy,
}

#[derive(Debug, Serialize)]
struct HookTestReport {
    event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<HookExecutionStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    outcome: Option<String>,
    modified_destination: bool,
    modified_text: bool,
}

pub(crate) async fn hooks<W: Write>(
    cli: &Cli,
    args: &HooksCommand,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    match args {
        HooksCommand::Validate { path } => hooks_validate(cli, path.as_deref(), writer).await,
        HooksCommand::Test { event } => hooks_test(cli, event, writer).await,
        HooksCommand::Status => hooks_status(cli, writer),
    }
}

async fn hooks_validate<W: Write>(
    cli: &Cli,
    path: Option<&Path>,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let runtime = if let Some(path) = path {
        runtime_from_path(path)?
    } else {
        runtime_from_config(cli)?
    };
    let validation = runtime.validate().await?;
    let event_count = validation.handlers.len();
    let handlers = validation
        .handlers
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let human = format!("Validated {event_count} {PROTOCOL_SCHEMA} handler(s): {handlers}");
    writer
        .result("hook_validation", &validation, &human)
        .map_err(CliError::from)
}

async fn hooks_test<W: Write>(
    cli: &Cli,
    event: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let runtime = runtime_from_config(cli)?;
    let fixture = fixture_for_event(event)?;

    match fixture {
        TestFixture::Event { event, name } => {
            let report = runtime.dispatch(event).await?;
            let emitted = HookTestReport {
                event: name.to_owned(),
                event_id: Some(report.event_id),
                status: Some(report.status),
                outcome: None,
                modified_destination: false,
                modified_text: false,
            };
            let human = format!(
                "Dispatched hook event '{}' with status '{}'",
                emitted.event,
                match report.status {
                    HookExecutionStatus::Completed => "completed",
                    HookExecutionStatus::Missing => "missing",
                    HookExecutionStatus::FailedOpen { .. } => "failed-open",
                }
            );
            writer
                .result("hook_test", &emitted, &human)
                .map_err(CliError::from)
        }
        TestFixture::BeforeSend {
            input,
            destination,
            text,
        } => {
            let before_send = runtime.before_send(input).await?;
            let (outcome, modified_destination, modified_text) =
                analyze_before_send(destination, text, before_send);
            let emitted = HookTestReport {
                event: "before_send".to_owned(),
                event_id: None,
                status: None,
                outcome: Some(outcome.to_owned()),
                modified_destination,
                modified_text,
            };
            let human = format!("before_send hook decision: {outcome}");
            writer
                .result("hook_test", &emitted, &human)
                .map_err(CliError::from)
        }
    }
}

fn hooks_status<W: Write>(cli: &Cli, writer: &mut OutputWriter<W>) -> Result<(), CliError> {
    let store = config_store(cli)?;
    let report = match load_optional(&store)? {
        LoadOutcome::Missing => {
            let hook = StoreConfig::default().hook;
            HookStatusReport {
                protocol: PROTOCOL_SCHEMA,
                enabled: false,
                configured: false,
                observational_failure: hook.observational_failure,
                before_send_failure: hook.before_send_failure,
            }
        }
        LoadOutcome::Loaded(config) | LoadOutcome::NeedsMigration(config) => HookStatusReport {
            protocol: PROTOCOL_SCHEMA,
            enabled: config.hook.enabled,
            configured: config.hook.script.is_some(),
            observational_failure: config.hook.observational_failure,
            before_send_failure: config.hook.before_send_failure,
        },
    };
    let human = if !report.enabled {
        format!("Hook protocol {} is currently disabled.", report.protocol)
    } else if report.configured {
        format!(
            "Hook protocol {} is enabled with configured policies.",
            report.protocol
        )
    } else {
        format!(
            "Hook protocol {} is enabled but has no configured script.",
            report.protocol
        )
    };
    writer
        .result("hook_status", &report, &human)
        .map_err(CliError::from)
}

fn runtime_from_path(path: &Path) -> Result<HookRuntime, CliError> {
    let config = HookConfig::new(path);
    HookRuntime::new(config).map_err(CliError::from)
}

fn runtime_from_config(cli: &Cli) -> Result<HookRuntime, CliError> {
    let store = config_store(cli)?;
    let path = store.path().display().to_string();
    let config = match load_optional(&store)? {
        LoadOutcome::Missing => {
            return Err(CliError::new(
                ExitStatus::Configuration,
                format!("configuration is missing at {path}"),
            )
            .with_hint("Run `meshquill init` to create the configuration file."));
        }
        LoadOutcome::Loaded(config) | LoadOutcome::NeedsMigration(config) => config,
    };
    let runtime_config = config
        .hook
        .runtime_config()
        .map_err(CliError::from)?
        .ok_or_else(|| {
            CliError::new(
                ExitStatus::Configuration,
                "hooks are disabled; set [hook].enabled = true and [hook].script to a path",
            )
        })?;
    HookRuntime::new(runtime_config).map_err(CliError::from)
}

#[derive(Debug)]
enum TestFixture {
    Event {
        event: HookEvent,
        name: &'static str,
    },
    BeforeSend {
        input: BeforeSendInput,
        destination: &'static str,
        text: &'static str,
    },
}

fn fixture_for_event(input: &str) -> Result<TestFixture, CliError> {
    let normalized = input.to_ascii_lowercase();
    match normalized.as_str() {
        "on_connect" | "connect" => Ok(TestFixture::Event {
            name: "on_connect",
            event: HookEvent::OnConnect(OnConnectPayload {
                transport: "unit-test".to_owned(),
                peer: Some("meshquill-demo".to_owned()),
            }),
        }),
        "on_disconnect" | "disconnect" => Ok(TestFixture::Event {
            name: "on_disconnect",
            event: HookEvent::OnDisconnect(OnDisconnectPayload {
                transport: "unit-test".to_owned(),
                reason: Some("normal-shutdown".to_owned()),
            }),
        }),
        "on_message" | "message" => Ok(TestFixture::Event {
            name: "on_message",
            event: HookEvent::OnMessage(OnMessagePayload {
                source: "meshquill-demo".to_owned(),
                text: "hello from test".to_owned(),
                message_id: Some("msg-001".to_owned()),
            }),
        }),
        "before_send" | "before-send" => Ok(TestFixture::BeforeSend {
            destination: "inbound",
            text: "hello from test",
            input: BeforeSendInput {
                destination: "inbound".to_owned(),
                text: "hello from test".to_owned(),
            },
        }),
        "after_send" | "after-send" => Ok(TestFixture::Event {
            name: "after_send",
            event: HookEvent::AfterSend(AfterSendPayload {
                destination: "meshquill-demo".to_owned(),
                text: "hello from test".to_owned(),
                message_id: Some("msg-ack".to_owned()),
            }),
        }),
        "on_ack" | "ack" => Ok(TestFixture::Event {
            name: "on_ack",
            event: HookEvent::OnAck(OnAckPayload {
                message_id: "msg-001".to_owned(),
                source: Some("meshquill-demo".to_owned()),
                round_trip_ms: Some(42),
            }),
        }),
        "on_timeout" | "timeout" => Ok(TestFixture::Event {
            name: "on_timeout",
            event: HookEvent::OnTimeout(OnTimeoutPayload {
                operation: "test-event".to_owned(),
                message_id: Some("msg-001".to_owned()),
            }),
        }),
        "on_contact_update" | "contact_update" | "contact" => Ok(TestFixture::Event {
            name: "on_contact_update",
            event: HookEvent::OnContactUpdate(OnContactUpdatePayload {
                contact_id: "contact-id".to_owned(),
                display_name: Some("meshquill-contact".to_owned()),
                change: ContactChange::Updated,
            }),
        }),
        "on_error" | "error" => Ok(TestFixture::Event {
            name: "on_error",
            event: HookEvent::OnError(OnErrorPayload {
                operation: "test-operation".to_owned(),
                message: "sanitized test message".to_owned(),
            }),
        }),
        _ => Err(
            CliError::new(ExitStatus::Usage, format!("unsupported hook event '{input}'")).with_hint(
                "Supported events: on_connect, on_disconnect, on_message, before_send, after_send, on_ack, on_timeout, on_contact_update, on_error",
            ),
        ),
    }
}

fn analyze_before_send(
    destination: &str,
    text: &str,
    outcome: BeforeSendOutcome,
) -> (&'static str, bool, bool) {
    match outcome {
        BeforeSendOutcome::Allow => ("allow", false, false),
        BeforeSendOutcome::Modify {
            destination: new_destination,
            text: new_text,
        } => ("modify", new_destination != destination, new_text != text),
        BeforeSendOutcome::Reject { .. } => ("reject", false, false),
    }
}
