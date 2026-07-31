//! Application-level MQTT gateway commands.

use std::{
    collections::VecDeque,
    io::{self, IsTerminal, Read, Write},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use meshquill_core::{CoreError, Event, MAX_OPERATION_TIMEOUT, ManagedClient};
use meshquill_mqtt::{
    AcceptedCommand, CommandError, ConfigError as MqttConfigError, ConnectionStatus, GatewayError,
    GatewayHandle, GatewayNotice, GatewayRunner, MqttPassword, MqttProtocol, MqttQos, Publication,
    SendCommand,
};
use meshquill_store::{
    Config as StoreConfig, ConfigStore, LoadOutcome, MqttSettings, SecretRef, SystemSecretResolver,
};
use secrecy::SecretString;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::{
    args::{Cli, MqttCommand, MqttConfigureArgs, MqttProtocolChoice, MqttQosChoice},
    config::{config_store, load_optional, load_unmodified, select_profile},
    error::CliError,
    output::{ExitStatus, OutputWriter},
    runtime::{make_client, resolve_contact, watch_human, watch_record},
    workflow::{IncomingOrigin, OutgoingRecord, WorkflowServices},
};

const MQTT_SCHEMA: &str = "meshquill.mqtt/v1";
const CREDENTIAL_SERVICE: &str = "meshquill";
const MAX_PASSWORD_BYTES: usize = 4096;
const GATEWAY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PENDING_ACKS: usize = 256;

#[derive(Debug, Serialize)]
#[allow(clippy::struct_excessive_bools)]
struct MqttStatusReport {
    schema: &'static str,
    enabled: bool,
    configured: bool,
    host: String,
    port: u16,
    protocol: &'static str,
    qos: u8,
    tls: bool,
    custom_ca: bool,
    client_identity: bool,
    authentication: bool,
    topic_prefix: String,
    allow_send: bool,
    broker_state: &'static str,
}

#[derive(Debug, Serialize)]
#[allow(clippy::struct_excessive_bools)]
struct MqttConfigureReport {
    schema: &'static str,
    enabled: bool,
    host: String,
    port: u16,
    protocol: &'static str,
    qos: u8,
    tls: bool,
    custom_ca: bool,
    client_identity: bool,
    authentication: bool,
    credential_store: bool,
    topic_prefix: String,
    allow_send: bool,
}

#[derive(Debug, Serialize)]
struct MqttTestReport {
    schema: &'static str,
    host: String,
    port: u16,
    tls: bool,
    authenticated: bool,
    connected: bool,
}

#[derive(Debug, Serialize)]
struct BrokerStateReport {
    component: &'static str,
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct BrokerCommandReport {
    event_id: String,
    command: &'static str,
    queued: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct BrokerRejectionReport {
    accepted: bool,
    reason: &'static str,
}

struct PendingBrokerAck {
    record: OutgoingRecord,
    message_id: String,
    destination: String,
    ack_code: [u8; 4],
    deadline: Instant,
}

struct BrokerCommandOutcome {
    report: BrokerCommandReport,
    pending: Option<PendingBrokerAck>,
}

pub(crate) async fn mqtt<W: Write>(
    cli: &Cli,
    command: &MqttCommand,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    match command {
        MqttCommand::Configure(args) => mqtt_configure(cli, args, writer).await,
        MqttCommand::Test => mqtt_test(cli, writer).await,
        MqttCommand::Bridge => mqtt_bridge(cli, writer).await,
        MqttCommand::Status => mqtt_status(cli, writer),
    }
}

fn mqtt_status<W: Write>(cli: &Cli, writer: &mut OutputWriter<W>) -> Result<(), CliError> {
    let store = config_store(cli)?;
    let settings = match load_optional(&store)? {
        LoadOutcome::Missing => StoreConfig::default().mqtt,
        LoadOutcome::Loaded(config) | LoadOutcome::NeedsMigration(config) => config.mqtt,
    };
    let report = status_report(&settings);
    let human = if report.enabled {
        format!(
            "MQTT gateway enabled for {}:{} (TLS: {}, outbound sends: {}). Broker not probed.",
            report.host, report.port, report.tls, report.allow_send
        )
    } else {
        "MQTT gateway is disabled. Broker not probed.".to_owned()
    };
    writer
        .result("mqtt_status", &report, &human)
        .map_err(CliError::from)
}

async fn mqtt_configure<W: Write>(
    cli: &Cli,
    args: &MqttConfigureArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let (store, mut config) = load_config_for_mqtt_update(cli)?;

    let old_reference = config.mqtt.password.clone();
    let old_username = config.mqtt.gateway.username.clone();
    let mut gateway = config.mqtt.gateway.clone();
    gateway.host = args.host.trim().to_owned();
    gateway.port = args.port;
    gateway.protocol = protocol_from_choice(args.protocol);
    gateway.qos = qos_from_choice(args.qos);
    gateway.tls.enabled = !args.no_tls;
    gateway.tls.verify_server_certificate = true;
    gateway.tls.ca_path.clone_from(&args.ca_file);
    gateway
        .tls
        .client_certificate_path
        .clone_from(&args.client_certificate);
    gateway
        .tls
        .client_private_key_path
        .clone_from(&args.client_key);
    gateway.topic_prefix = args.topic_prefix.trim().to_owned();
    gateway.allow_send = args.allow_send;

    let password = if args.clear_auth {
        gateway.username = None;
        None
    } else if let Some(username) = &args.username {
        let username = username.trim().to_owned();
        if username.is_empty() {
            return Err(CliError::new(
                ExitStatus::Usage,
                "MQTT username must not be empty",
            ));
        }
        gateway.username = Some(username.clone());
        if args.password_stdin {
            Some(read_password_stdin()?)
        } else if old_username.as_deref() == Some(username.as_str()) {
            None
        } else {
            return Err(CliError::new(
                ExitStatus::Usage,
                "a new MQTT username requires --password-stdin",
            ));
        }
    } else {
        gateway.username.clone_from(&old_username);
        None
    };

    gateway
        .validate()
        .map_err(|error| mqtt_config_error(&error))?;

    let mut fresh_credential = None;
    let password_reference = if let Some(password) = password {
        let account = credential_account(store.path(), gateway.username.as_deref().unwrap_or(""))?;
        let reference = SecretRef::CredentialStore {
            service: CREDENTIAL_SERVICE.to_owned(),
            account: account.clone(),
        };
        store_credential(account, password).await?;
        fresh_credential = credential_target(&reference);
        Some(reference)
    } else if args.clear_auth {
        None
    } else {
        old_reference.clone()
    };

    config.mqtt = MqttSettings {
        enabled: true,
        gateway,
        password: password_reference,
    };
    if let Err(error) = config.validate() {
        cleanup_credential(fresh_credential).await;
        return Err(CliError::from(error));
    }
    if let Err(error) = store.save(&config) {
        cleanup_credential(fresh_credential).await;
        return Err(CliError::from(error));
    }

    if old_reference != config.mqtt.password {
        cleanup_credential(credential_target_opt(old_reference.as_ref())).await;
    }

    let report = configure_report(&config.mqtt);
    let human = format!(
        "Configured MQTT broker {}:{} (TLS: {}, authentication: {}, outbound sends: {}).",
        report.host, report.port, report.tls, report.authentication, report.allow_send
    );
    writer
        .result("mqtt_configuration", &report, &human)
        .map_err(CliError::from)
}

async fn mqtt_test<W: Write>(cli: &Cli, writer: &mut OutputWriter<W>) -> Result<(), CliError> {
    let settings = load_enabled_settings(cli)?;
    let password = resolve_password(cli, &settings).await?;
    let cancellation = CancellationToken::new();
    let (mut handle, runner) =
        GatewayRunner::connect(settings.gateway.clone(), password, cancellation.clone())
            .await
            .map_err(mqtt_gateway_error)?;
    let task = tokio::spawn(runner.run());

    let connected = tokio::time::timeout(cli.timeout, async {
        loop {
            match handle.recv_notice().await {
                Some(GatewayNotice::BrokerState(ConnectionStatus::Connected)) => return Ok(()),
                Some(
                    GatewayNotice::BrokerState(ConnectionStatus::Disconnected)
                    | GatewayNotice::Rejected(_)
                    | GatewayNotice::Command(_),
                ) => {}
                None => {
                    return Err(CliError::new(
                        ExitStatus::Mqtt,
                        "MQTT gateway stopped before the broker accepted the connection",
                    ));
                }
            }
        }
    })
    .await;
    handle.cancel();
    join_gateway(task).await?;
    match connected {
        Ok(result) => result?,
        Err(_) => {
            return Err(
                CliError::new(ExitStatus::Mqtt, "MQTT broker connection test timed out")
                    .with_hint("Check DNS, TCP reachability, TLS trust, and broker credentials."),
            );
        }
    }

    let report = MqttTestReport {
        schema: MQTT_SCHEMA,
        host: settings.gateway.host,
        port: settings.gateway.port,
        tls: settings.gateway.tls.enabled,
        authenticated: settings.gateway.username.is_some(),
        connected: true,
    };
    let transport = if report.tls { "TLS" } else { "plain TCP" };
    let authentication = if report.authenticated {
        "broker authentication"
    } else {
        "no broker authentication"
    };
    let human = format!(
        "Connected to MQTT broker {}:{} using {transport} and {authentication}.",
        report.host, report.port
    );
    writer
        .result("mqtt_test", &report, &human)
        .map_err(CliError::from)
}

async fn mqtt_bridge<W: Write>(cli: &Cli, writer: &mut OutputWriter<W>) -> Result<(), CliError> {
    let selected = select_profile(cli)?;
    if !selected.config.mqtt.enabled {
        return Err(mqtt_disabled());
    }
    let workflow = WorkflowServices::from_selected(&selected)?;
    let client = make_client(&selected)?;
    let settings = selected.config.mqtt.clone();
    let password = resolve_password(cli, &settings).await?;
    let cancellation = CancellationToken::new();
    let (mut gateway, runner) =
        GatewayRunner::connect(settings.gateway.clone(), password, cancellation.clone())
            .await
            .map_err(mqtt_gateway_error)?;
    let gateway_task = tokio::spawn(runner.run());

    let mut events = client.subscribe();
    let info = match client.connect().await {
        Ok(info) => info,
        Err(error) => {
            gateway.cancel();
            let _gateway_result = join_gateway(gateway_task).await;
            return shutdown_with_core_error(&client, error).await;
        }
    };
    if let Err(error) = workflow.connected(&info.name).await {
        gateway.cancel();
        let _ = client.shutdown().await;
        let _ = join_gateway(gateway_task).await;
        return Err(error);
    }

    let result = bridge_loop(&client, &workflow, &mut gateway, &mut events, writer).await;
    gateway.cancel();
    let disconnect_result = workflow
        .disconnected(Some(if result.is_ok() {
            "MQTT bridge completed"
        } else {
            "MQTT bridge stopped"
        }))
        .await;
    let core_shutdown = client.shutdown().await;
    let gateway_shutdown = join_gateway(gateway_task).await;
    if let Err(error) = result {
        Err(error)
    } else {
        disconnect_result?;
        core_shutdown.map_err(CliError::from)?;
        gateway_shutdown
    }
}

async fn bridge_loop<W: Write>(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    gateway: &mut GatewayHandle,
    events: &mut broadcast::Receiver<Event>,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let mut pending = VecDeque::new();
    let mut timeout_tick = tokio::time::interval(Duration::from_millis(250));
    timeout_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|_| CliError::new(
                    ExitStatus::Interrupted,
                    "could not install the interrupt handler",
                ))?;
                return Err(CliError::new(ExitStatus::Interrupted, "interrupted by user"));
            }
            _ = timeout_tick.tick() => {
                expire_pending_acks(workflow, &mut pending).await?;
            }
            event = events.recv() => {
                match event {
                    Ok(event) => publish_core_event(
                        workflow,
                        &mut pending,
                        gateway,
                        event,
                        writer,
                    ).await?,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let report = BrokerRejectionReport {
                            accepted: false,
                            reason: "local_event_consumer_lagged",
                        };
                        writer.event(
                            "mqtt_diagnostic",
                            &report,
                            &format!("MQTT bridge skipped {skipped} locally buffered event(s)."),
                        ).map_err(CliError::from)?;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(CliError::new(
                            ExitStatus::Connection,
                            "the device event stream closed",
                        ));
                    }
                }
            }
            notice = gateway.recv_notice() => {
                handle_gateway_notice(client, workflow, &mut pending, notice, writer).await?;
            }
        }
    }
}

async fn handle_gateway_notice<W: Write>(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    pending: &mut VecDeque<PendingBrokerAck>,
    notice: Option<GatewayNotice>,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    match notice {
        Some(GatewayNotice::BrokerState(status)) => {
            let report = BrokerStateReport {
                component: "mqtt_broker",
                status: connection_status(status),
            };
            writer
                .event(
                    "mqtt_connection",
                    &report,
                    &format!("MQTT broker is {}.", report.status),
                )
                .map_err(CliError::from)
        }
        Some(GatewayNotice::Rejected(error)) => {
            let report = BrokerRejectionReport {
                accepted: false,
                reason: rejection_code(&error),
            };
            writer
                .event(
                    "mqtt_command_rejected",
                    &report,
                    &format!("Rejected MQTT command: {}.", report.reason),
                )
                .map_err(CliError::from)
        }
        Some(GatewayNotice::Command(command)) => {
            let outcome = execute_command_once(client, workflow, command, pending.len()).await;
            if let Some(item) = outcome.pending {
                pending.push_back(item);
            }
            let report = outcome.report;
            let human = if report.queued {
                format!("Queued one MQTT {} request.", report.command)
            } else {
                format!(
                    "Did not queue MQTT {} request: {}.",
                    report.command,
                    report.reason.unwrap_or("unknown_failure")
                )
            };
            writer
                .event("mqtt_command", &report, &human)
                .map_err(CliError::from)
        }
        None => Err(CliError::new(
            ExitStatus::Mqtt,
            "MQTT gateway stopped unexpectedly",
        )),
    }
}

async fn publish_core_event<W: Write>(
    workflow: &WorkflowServices,
    pending: &mut VecDeque<PendingBrokerAck>,
    gateway: &GatewayHandle,
    event: Event,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    match &event {
        Event::Message(message) => {
            if workflow
                .incoming(message, IncomingOrigin::Live)
                .await?
                .is_none()
            {
                return Ok(());
            }
        }
        Event::Ack(ack) => {
            if let Some(position) = pending.iter().position(|item| item.ack_code == ack.code)
                && let Some(mut item) = pending.remove(position)
            {
                workflow
                    .acknowledged(
                        &mut item.record,
                        &item.message_id,
                        Some(&item.destination),
                        ack.trip_time_ms,
                        ack.code,
                    )
                    .await?;
            }
        }
        Event::ProtocolError(_) | Event::UnknownPacket { .. } | Event::LoginFailed { .. } => {
            workflow
                .error("mqtt_bridge", "the device emitted an error event")
                .await?;
        }
        _ => {}
    }
    if let Some(publication) = Publication::from_core_event(event.clone()) {
        gateway
            .publish(publication)
            .await
            .map_err(mqtt_gateway_error)?;
    }
    let record = watch_record(&event);
    let human = watch_human(&record);
    writer
        .event("meshcore_event", &record, &human)
        .map_err(CliError::from)
}

async fn expire_pending_acks(
    workflow: &WorkflowServices,
    pending: &mut VecDeque<PendingBrokerAck>,
) -> Result<(), CliError> {
    let now = Instant::now();
    let mut retained = VecDeque::with_capacity(pending.len());
    while let Some(mut item) = pending.pop_front() {
        if item.deadline <= now {
            workflow
                .timed_out(
                    &mut item.record,
                    "MQTT-originated send acknowledgement",
                    &item.message_id,
                )
                .await?;
        } else {
            retained.push_back(item);
        }
    }
    *pending = retained;
    Ok(())
}

async fn execute_command_once(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    accepted: AcceptedCommand,
    pending_count: usize,
) -> BrokerCommandOutcome {
    let event_id = accepted.event_id.to_string();
    match accepted.command {
        SendCommand::Direct { destination, text } => {
            execute_direct_command(client, workflow, event_id, destination, text, pending_count)
                .await
        }
        SendCommand::Channel { channel, text } => {
            execute_channel_command(client, workflow, event_id, channel, text).await
        }
    }
}

async fn execute_direct_command(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    event_id: String,
    destination: String,
    text: String,
    pending_count: usize,
) -> BrokerCommandOutcome {
    if pending_count >= MAX_PENDING_ACKS {
        return rejected_command(event_id, "direct_send", "local_pending_limit");
    }
    let prepared = match workflow.prepare_send(destination, text).await {
        Ok(prepared) => prepared,
        Err(error) => {
            tracing::warn!(error = %error, "MQTT direct send rejected by before-send workflow");
            return rejected_command(event_id, "direct_send", "before_send_rejected");
        }
    };
    let mut outgoing = match workflow
        .begin_outgoing(&prepared.destination, None, &prepared.text)
        .await
    {
        Ok(record) => record,
        Err(error) => {
            tracing::warn!(error = %error, "could not prepare MQTT direct-send history");
            return rejected_command(event_id, "direct_send", "history_failed");
        }
    };
    let Ok(contacts) = client.list_contacts(None).await else {
        record_failed_send(workflow, &mut outgoing).await;
        return rejected_command(event_id, "direct_send", "device_unavailable");
    };
    let Ok(contact) = resolve_contact(&contacts, &prepared.destination) else {
        record_failed_send(workflow, &mut outgoing).await;
        return rejected_command(
            event_id,
            "direct_send",
            "destination_not_found_or_ambiguous",
        );
    };
    let mut destination_prefix = [0_u8; 6];
    destination_prefix.copy_from_slice(&contact.public_key.as_bytes()[..6]);
    let Ok(tracking) = client
        .send_direct_text(&destination_prefix, 0, &prepared.text)
        .await
    else {
        record_failed_send(workflow, &mut outgoing).await;
        return rejected_command(event_id, "direct_send", "device_send_failed");
    };
    let message_id = outgoing.message_id().to_string();
    let workflow_result = workflow
        .sent(
            &mut outgoing,
            &contact.adv_name,
            &prepared.text,
            &message_id,
            Some(tracking.ack_code),
        )
        .await;
    let acknowledgement_timeout =
        Duration::from_millis(u64::from(tracking.timeout_ms)).min(MAX_OPERATION_TIMEOUT);
    let pending = PendingBrokerAck {
        record: outgoing,
        message_id,
        destination: contact.adv_name.clone(),
        ack_code: tracking.ack_code,
        deadline: Instant::now() + acknowledgement_timeout,
    };
    if let Err(error) = workflow_result {
        tracing::warn!(error = %error, "MQTT direct send queued but after-send workflow failed");
        return broker_command_outcome(
            event_id,
            "direct_send",
            true,
            Some("after_send_hook_failed"),
            Some(pending),
        );
    }
    broker_command_outcome(event_id, "direct_send", true, None, Some(pending))
}

async fn execute_channel_command(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    event_id: String,
    channel: u8,
    text: String,
) -> BrokerCommandOutcome {
    let prepared = match workflow.prepare_send(channel.to_string(), text).await {
        Ok(prepared) => prepared,
        Err(error) => {
            tracing::warn!(error = %error, "MQTT channel send rejected by before-send workflow");
            return rejected_command(event_id, "channel_send", "before_send_rejected");
        }
    };
    let Ok(prepared_channel) = prepared.destination.parse::<u8>() else {
        return rejected_command(event_id, "channel_send", "before_send_rejected");
    };
    let mut outgoing = match workflow
        .begin_outgoing(
            &prepared.destination,
            Some(prepared_channel),
            &prepared.text,
        )
        .await
    {
        Ok(record) => record,
        Err(error) => {
            tracing::warn!(error = %error, "could not prepare MQTT channel-send history");
            return rejected_command(event_id, "channel_send", "history_failed");
        }
    };
    if client
        .send_channel_message(prepared_channel, 0, &prepared.text)
        .await
        .is_err()
    {
        record_failed_send(workflow, &mut outgoing).await;
        return rejected_command(event_id, "channel_send", "device_send_failed");
    }
    let message_id = outgoing.message_id().to_string();
    if let Err(error) = workflow
        .sent(
            &mut outgoing,
            &prepared.destination,
            &prepared.text,
            &message_id,
            None,
        )
        .await
    {
        tracing::warn!(error = %error, "MQTT channel send queued but after-send workflow failed");
        return broker_command_outcome(
            event_id,
            "channel_send",
            true,
            Some("after_send_hook_failed"),
            None,
        );
    }
    broker_command_outcome(event_id, "channel_send", true, None, None)
}

fn rejected_command(
    event_id: String,
    command: &'static str,
    reason: &'static str,
) -> BrokerCommandOutcome {
    broker_command_outcome(event_id, command, false, Some(reason), None)
}

fn broker_command_outcome(
    event_id: String,
    command: &'static str,
    queued: bool,
    reason: Option<&'static str>,
    pending: Option<PendingBrokerAck>,
) -> BrokerCommandOutcome {
    BrokerCommandOutcome {
        report: BrokerCommandReport {
            event_id,
            command,
            queued,
            reason,
        },
        pending,
    }
}

async fn record_failed_send(workflow: &WorkflowServices, outgoing: &mut OutgoingRecord) {
    if let Err(error) = workflow.failed(outgoing).await {
        tracing::warn!(error = %error, "could not record failed MQTT send state");
    }
}

fn load_enabled_settings(cli: &Cli) -> Result<MqttSettings, CliError> {
    let store = config_store(cli)?;
    let settings = match load_optional(&store)? {
        LoadOutcome::Missing => return Err(mqtt_disabled()),
        LoadOutcome::Loaded(config) | LoadOutcome::NeedsMigration(config) => config.mqtt,
    };
    if !settings.enabled {
        return Err(mqtt_disabled());
    }
    Ok(settings)
}

async fn resolve_password(
    cli: &Cli,
    settings: &MqttSettings,
) -> Result<Option<MqttPassword>, CliError> {
    if settings.password.as_ref() == Some(&SecretRef::Prompt) {
        if cli.non_interactive || !io::stdin().is_terminal() {
            return Err(CliError::new(
                ExitStatus::Authentication,
                "MQTT password requires an interactive terminal",
            ));
        }
        let password = tokio::task::spawn_blocking(|| {
            rpassword::prompt_password("MQTT password: ").map_err(|_| {
                CliError::new(
                    ExitStatus::Authentication,
                    "could not read the MQTT password securely",
                )
            })
        })
        .await
        .map_err(|_| credential_worker_error())??;
        return MqttPassword::new(password)
            .map(Some)
            .map_err(|error| mqtt_config_error(&error));
    }

    let settings = settings.clone();
    tokio::task::spawn_blocking(move || settings.resolve_password(&SystemSecretResolver))
        .await
        .map_err(|_| credential_worker_error())?
        .map_err(CliError::from)
}

fn read_password_stdin() -> Result<SecretString, CliError> {
    let maximum = u64::try_from(MAX_PASSWORD_BYTES + 1).unwrap_or(u64::MAX);
    let mut input = io::stdin().lock().take(maximum);
    let mut bytes = Zeroizing::new(Vec::with_capacity(128));
    input.read_to_end(&mut bytes).map_err(|_| {
        CliError::new(
            ExitStatus::Authentication,
            "could not read the MQTT password from stdin",
        )
    })?;
    if bytes.len() > MAX_PASSWORD_BYTES {
        return Err(CliError::new(
            ExitStatus::Usage,
            format!("MQTT password exceeds the {MAX_PASSWORD_BYTES}-byte limit"),
        ));
    }
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    if bytes.is_empty() {
        return Err(CliError::new(
            ExitStatus::Usage,
            "MQTT password from stdin must not be empty",
        ));
    }
    let password = String::from_utf8(std::mem::take(&mut *bytes)).map_err(|_| {
        CliError::new(
            ExitStatus::Usage,
            "MQTT password from stdin must be valid UTF-8",
        )
    })?;
    if password.contains('\0') {
        return Err(CliError::new(
            ExitStatus::Usage,
            "MQTT password from stdin must not contain NUL",
        ));
    }
    Ok(SecretString::from(password))
}

async fn store_credential(account: String, password: SecretString) -> Result<(), CliError> {
    tokio::task::spawn_blocking(move || {
        SystemSecretResolver::set_credential(CREDENTIAL_SERVICE, &account, &password)
    })
    .await
    .map_err(|_| credential_worker_error())?
    .map_err(CliError::from)
}

async fn cleanup_credential(target: Option<(String, String)>) {
    let Some((service, account)) = target else {
        return;
    };
    let result = tokio::task::spawn_blocking(move || {
        SystemSecretResolver::delete_credential(&service, &account)
    })
    .await;
    if !matches!(result, Ok(Ok(()))) {
        tracing::warn!("an obsolete managed MQTT credential could not be removed");
    }
}

fn credential_account(path: &Path, username: &str) -> Result<String, CliError> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| credential_worker_error())?
        .as_nanos();
    let mut digest = Sha256::new();
    digest.update(path.as_os_str().as_encoded_bytes());
    digest.update([0]);
    digest.update(username.as_bytes());
    digest.update([0]);
    digest.update(std::process::id().to_le_bytes());
    digest.update(timestamp.to_le_bytes());
    let digest = digest.finalize();
    Ok(format!("mqtt-{}", hex::encode(&digest[..16])))
}

fn credential_target(reference: &SecretRef) -> Option<(String, String)> {
    credential_target_opt(Some(reference))
}

fn credential_target_opt(reference: Option<&SecretRef>) -> Option<(String, String)> {
    match reference {
        Some(SecretRef::CredentialStore { service, account }) => {
            Some((service.clone(), account.clone()))
        }
        Some(SecretRef::Environment { .. } | SecretRef::Prompt) | None => None,
    }
}

async fn join_gateway(
    mut task: tokio::task::JoinHandle<Result<(), GatewayError>>,
) -> Result<(), CliError> {
    match tokio::time::timeout(GATEWAY_SHUTDOWN_TIMEOUT, &mut task).await {
        Ok(Ok(Ok(()) | Err(GatewayError::Cancelled))) => Ok(()),
        Ok(Ok(Err(error))) => Err(mqtt_gateway_error(error)),
        Ok(Err(_)) => Err(CliError::new(
            ExitStatus::Mqtt,
            "MQTT gateway worker failed",
        )),
        Err(_) => {
            task.abort();
            let _abort_result = task.await;
            Err(CliError::new(
                ExitStatus::Mqtt,
                "MQTT gateway did not stop within its bounded shutdown timeout",
            ))
        }
    }
}

async fn shutdown_with_core_error<T>(
    client: &ManagedClient,
    error: CoreError,
) -> Result<T, CliError> {
    let _shutdown_result = client.shutdown().await;
    Err(CliError::from(error))
}

fn status_report(settings: &MqttSettings) -> MqttStatusReport {
    MqttStatusReport {
        schema: MQTT_SCHEMA,
        enabled: settings.enabled,
        configured: settings.enabled,
        host: settings.gateway.host.clone(),
        port: settings.gateway.port,
        protocol: protocol_name(settings.gateway.protocol),
        qos: qos_number(settings.gateway.qos),
        tls: settings.gateway.tls.enabled,
        custom_ca: settings.gateway.tls.ca_path.is_some(),
        client_identity: settings.gateway.tls.client_certificate_path.is_some()
            && settings.gateway.tls.client_private_key_path.is_some(),
        authentication: settings.gateway.username.is_some() && settings.password.is_some(),
        topic_prefix: settings.gateway.topic_prefix.clone(),
        allow_send: settings.gateway.allow_send,
        broker_state: "not_probed",
    }
}

fn load_config_for_mqtt_update(cli: &Cli) -> Result<(ConfigStore, StoreConfig), CliError> {
    let store = config_store(cli)?;
    let config = match load_unmodified(&store)? {
        LoadOutcome::Missing => StoreConfig::default(),
        LoadOutcome::Loaded(config) => config,
        LoadOutcome::NeedsMigration(_) => {
            return Err(CliError::new(
                ExitStatus::Configuration,
                "configuration must be migrated before changing MQTT settings",
            )
            .with_hint("Run `meshquill config migrate` first; it preserves a backup."));
        }
    };
    Ok((store, config))
}

fn configure_report(settings: &MqttSettings) -> MqttConfigureReport {
    MqttConfigureReport {
        schema: MQTT_SCHEMA,
        enabled: settings.enabled,
        host: settings.gateway.host.clone(),
        port: settings.gateway.port,
        protocol: protocol_name(settings.gateway.protocol),
        qos: qos_number(settings.gateway.qos),
        tls: settings.gateway.tls.enabled,
        custom_ca: settings.gateway.tls.ca_path.is_some(),
        client_identity: settings.gateway.tls.client_certificate_path.is_some()
            && settings.gateway.tls.client_private_key_path.is_some(),
        authentication: settings.gateway.username.is_some() && settings.password.is_some(),
        credential_store: matches!(settings.password, Some(SecretRef::CredentialStore { .. })),
        topic_prefix: settings.gateway.topic_prefix.clone(),
        allow_send: settings.gateway.allow_send,
    }
}

const fn protocol_from_choice(choice: MqttProtocolChoice) -> MqttProtocol {
    match choice {
        MqttProtocolChoice::V311 => MqttProtocol::V311,
        MqttProtocolChoice::V5 => MqttProtocol::V5,
    }
}

const fn qos_from_choice(choice: MqttQosChoice) -> MqttQos {
    match choice {
        MqttQosChoice::AtMostOnce => MqttQos::AtMostOnce,
        MqttQosChoice::AtLeastOnce => MqttQos::AtLeastOnce,
        MqttQosChoice::ExactlyOnce => MqttQos::ExactlyOnce,
    }
}

const fn protocol_name(protocol: MqttProtocol) -> &'static str {
    match protocol {
        MqttProtocol::V311 => "3.1.1",
        MqttProtocol::V5 => "5",
    }
}

const fn qos_number(qos: MqttQos) -> u8 {
    match qos {
        MqttQos::AtMostOnce => 0,
        MqttQos::AtLeastOnce => 1,
        MqttQos::ExactlyOnce => 2,
    }
}

const fn connection_status(status: ConnectionStatus) -> &'static str {
    match status {
        ConnectionStatus::Connected => "connected",
        ConnectionStatus::Disconnected => "disconnected",
    }
}

const fn rejection_code(error: &CommandError) -> &'static str {
    match error {
        CommandError::Config(_) | CommandError::Dedupe(_) => "invalid_gateway_policy",
        CommandError::SendDisabled => "outbound_send_disabled",
        CommandError::UnexpectedTopic => "unexpected_topic",
        CommandError::InvalidTopicEncoding => "invalid_topic_encoding",
        CommandError::EmptyPayload => "empty_payload",
        CommandError::PayloadTooLarge { .. } => "payload_too_large",
        CommandError::InvalidJson(_) => "invalid_json",
        CommandError::UnsupportedSchema => "unsupported_schema",
        CommandError::NilEventId => "nil_event_id",
        CommandError::InvalidOrigin => "invalid_origin",
        CommandError::LocalOriginLoop => "local_origin_loop",
        CommandError::DataMustBeObject => "data_must_be_object",
        CommandError::UnsupportedCommand => "unsupported_command",
        CommandError::InvalidCommandData(_) => "invalid_command_data",
        CommandError::InvalidDestination => "invalid_destination",
        CommandError::DestinationTooLong { .. } => "destination_too_long",
        CommandError::ChannelOutOfRange { .. } => "channel_out_of_range",
        CommandError::InvalidText => "invalid_text",
        CommandError::TextTooLong { .. } => "text_too_long",
        CommandError::DuplicateEvent => "duplicate_event",
    }
}

fn mqtt_config_error(error: &MqttConfigError) -> CliError {
    match error {
        MqttConfigError::InvalidField { field, reason } => CliError::new(
            ExitStatus::Mqtt,
            format!("invalid MQTT configuration field {field}: {reason}"),
        ),
        MqttConfigError::CredentialMismatch { .. } => CliError::new(
            ExitStatus::Mqtt,
            "MQTT username and password configuration do not match",
        ),
        MqttConfigError::TlsFile { field, .. } => CliError::new(
            ExitStatus::Mqtt,
            format!("configured MQTT TLS {field} file is unavailable or invalid"),
        ),
    }
}

fn mqtt_gateway_error(error: GatewayError) -> CliError {
    match error {
        GatewayError::Config(error) => mqtt_config_error(&error),
        GatewayError::TlsFile { .. }
        | GatewayError::TlsFileNotRegular { .. }
        | GatewayError::TlsFileTooLarge { .. }
        | GatewayError::Tls(_)
        | GatewayError::NoTrustRoots
        | GatewayError::IncompleteClientIdentity => CliError::new(
            ExitStatus::Mqtt,
            "MQTT TLS configuration could not be loaded safely",
        ),
        GatewayError::PublicationTooLarge { actual, maximum } => CliError::new(
            ExitStatus::Mqtt,
            format!("MQTT publication is {actual} bytes; maximum is {maximum}"),
        ),
        GatewayError::Cancelled => {
            CliError::new(ExitStatus::Interrupted, "MQTT gateway was cancelled")
        }
        GatewayError::BrokerOperationTimeout { .. } => CliError::new(
            ExitStatus::Mqtt,
            "MQTT broker operation exceeded its configured timeout",
        ),
        GatewayError::ClockBeforeUnixEpoch | GatewayError::TimestampOverflow => CliError::new(
            ExitStatus::Mqtt,
            "system clock cannot represent an MQTT event timestamp",
        ),
        GatewayError::Command(_)
        | GatewayError::Backoff(_)
        | GatewayError::Schema(_)
        | GatewayError::NonPublishableEvent
        | GatewayError::PublicationChannelClosed
        | GatewayError::NoticeChannelClosed
        | GatewayError::NetworkChannelClosed
        | GatewayError::NetworkWorkerStopped
        | GatewayError::WorkerJoin
        | GatewayError::MissingEventLoop
        | GatewayError::ProtocolPartsMismatch
        | GatewayError::Client(_) => CliError::new(
            ExitStatus::Mqtt,
            "MQTT gateway stopped because a bounded operation failed",
        ),
    }
}

fn mqtt_disabled() -> CliError {
    CliError::new(
        ExitStatus::Configuration,
        "MQTT gateway is disabled or unconfigured",
    )
    .with_hint("Run `meshquill mqtt configure --host HOST` first.")
}

fn credential_worker_error() -> CliError {
    CliError::new(
        ExitStatus::Authentication,
        "the MQTT credential operation failed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_never_serializes_secret_reference_details_or_tls_paths() {
        let mut settings = MqttSettings {
            enabled: true,
            password: Some(SecretRef::CredentialStore {
                service: "private-service".to_owned(),
                account: "private-account".to_owned(),
            }),
            ..MqttSettings::default()
        };
        settings.gateway.username = Some("alice".to_owned());
        settings.gateway.tls.ca_path = Some("/private/ca.pem".into());
        settings.gateway.tls.client_certificate_path = Some("/private/client.pem".into());
        settings.gateway.tls.client_private_key_path = Some("/private/key.pem".into());
        let encoded = serde_json::to_string(&status_report(&settings))
            .unwrap_or_else(|error| panic!("status encoding failed: {error}"));
        assert!(!encoded.contains("private"));
        assert!(!encoded.contains("alice"));
        assert!(encoded.contains("\"authentication\":true"));
        assert!(encoded.contains("\"client_identity\":true"));
    }

    #[test]
    fn rejection_codes_are_static_and_do_not_include_broker_payloads() {
        let error = CommandError::InvalidJson(
            serde_json::from_slice::<serde_json::Value>(b"secret payload")
                .expect_err("fixture must be malformed"),
        );
        assert_eq!(rejection_code(&error), "invalid_json");
        assert!(!rejection_code(&error).contains("secret"));
    }

    #[test]
    fn protocol_and_qos_choices_map_without_string_parsing() {
        assert_eq!(
            protocol_from_choice(MqttProtocolChoice::V5),
            MqttProtocol::V5
        );
        assert_eq!(
            qos_from_choice(MqttQosChoice::ExactlyOnce),
            MqttQos::ExactlyOnce
        );
        assert_eq!(protocol_name(MqttProtocol::V311), "3.1.1");
        assert_eq!(qos_number(MqttQos::AtMostOnce), 0);
    }
}
