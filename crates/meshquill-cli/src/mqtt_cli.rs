//! Application-level MQTT gateway commands.

use std::{
    collections::VecDeque,
    fs,
    io::{self, IsTerminal, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use meshquill_core::{CoreError, Event, MAX_OPERATION_TIMEOUT, ManagedClient};
use meshquill_mqtt::{
    AcceptedCommand, CommandError, CommandLimits, ConfigError as MqttConfigError, ConnectionStatus,
    GatewayError, GatewayHandle, GatewayNotice, GatewayRunner, MAX_MQTT_PASSWORD_BYTES,
    MqttPassword, MqttProtocol, MqttQos, Publication, SendCommand, validate_send_command,
};
use meshquill_store::{
    Config as StoreConfig, LoadOutcome, MqttSettings, SecretRef, SystemSecretResolver,
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
    config::{config_store, load_optional, load_unmodified_locked, select_profile},
    error::CliError,
    output::{ExitStatus, OutputWriter},
    runtime::{make_client, resolve_contact, watch_human, watch_record},
    workflow::{IncomingOrigin, OutgoingRecord, WorkflowServices},
};

const MQTT_SCHEMA: &str = "meshquill.mqtt/v1";
const CREDENTIAL_SERVICE: &str = "meshquill";
const GATEWAY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PENDING_ACKS: usize = 256;
const CONTACT_RESYNC_DEBOUNCE: Duration = Duration::from_millis(500);

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

#[derive(Debug, Serialize)]
struct SnapshotDiagnosticReport {
    publication: &'static str,
    published: bool,
    reason: &'static str,
}

#[derive(Default)]
struct ContactResyncDebounce {
    deadline: Option<Instant>,
}

impl ContactResyncDebounce {
    fn observe(&mut self, event: &Event, now: Instant) -> bool {
        let Event::UnknownPacket { code, .. } = event else {
            return false;
        };
        if !matches!(*code, 0x8a | 0x8f | 0x90) {
            return false;
        }

        // These firmware notifications are intentionally opaque. Their payload
        // layout is not stable enough to parse; the only safe response is a
        // bounded, authoritative directory query after a quiet period.
        self.deadline = Some(now + CONTACT_RESYNC_DEBOUNCE);
        true
    }

    const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    fn clear(&mut self) {
        self.deadline = None;
    }

    fn take_due(&mut self, now: Instant) -> bool {
        if self.deadline.is_some_and(|deadline| deadline <= now) {
            self.clear();
            true
        } else {
            false
        }
    }
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

enum AuthenticationUpdate {
    Clear,
    Replace {
        username: String,
        reference: SecretRef,
    },
    PreserveForUsername(String),
    Preserve,
}

async fn prepare_authentication(
    config_path: &Path,
    args: &MqttConfigureArgs,
) -> Result<(AuthenticationUpdate, Option<(String, String)>), CliError> {
    let requested_username = args.username.as_deref().map(str::trim).map(str::to_owned);
    if requested_username.as_deref() == Some("") {
        return Err(CliError::new(
            ExitStatus::Usage,
            "MQTT username must not be empty",
        ));
    }

    let mut fresh_credential = None;
    let authentication = if args.clear_auth {
        AuthenticationUpdate::Clear
    } else if let Some(username) = requested_username {
        if args.password_stdin {
            let password = read_password_stdin()?;
            let account = credential_account(config_path, &username)?;
            let reference = SecretRef::CredentialStore {
                service: CREDENTIAL_SERVICE.to_owned(),
                account: account.clone(),
            };
            store_credential(account, password).await?;
            fresh_credential = credential_target(&reference);
            AuthenticationUpdate::Replace {
                username,
                reference,
            }
        } else if let Some(name) = &args.password_env {
            AuthenticationUpdate::Replace {
                username,
                reference: SecretRef::environment(name.clone()).map_err(CliError::from)?,
            }
        } else {
            AuthenticationUpdate::PreserveForUsername(username)
        }
    } else {
        AuthenticationUpdate::Preserve
    };
    Ok((authentication, fresh_credential))
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
    let store = config_store(cli)?;
    let ca_path = canonical_tls_path(args.ca_file.as_deref(), "CA")?;
    let client_certificate_path =
        canonical_tls_path(args.client_certificate.as_deref(), "client certificate")?;
    let client_private_key_path =
        canonical_tls_path(args.client_key.as_deref(), "client private key")?;
    let (authentication, fresh_credential) = prepare_authentication(store.path(), args).await?;

    let update_result = (|| {
        let locked = store.lock_exclusive().map_err(CliError::from)?;
        let mut config = match load_unmodified_locked(&locked)? {
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
        let old_reference = config.mqtt.password.clone();
        let mut gateway = config.mqtt.gateway.clone();
        args.host.trim().clone_into(&mut gateway.host);
        gateway.port = args.port;
        gateway.protocol = protocol_from_choice(args.protocol);
        gateway.qos = qos_from_choice(args.qos);
        gateway.tls.enabled = !args.no_tls;
        gateway.tls.verify_server_certificate = true;
        gateway.tls.ca_path.clone_from(&ca_path);
        gateway
            .tls
            .client_certificate_path
            .clone_from(&client_certificate_path);
        gateway
            .tls
            .client_private_key_path
            .clone_from(&client_private_key_path);
        args.topic_prefix
            .trim()
            .clone_into(&mut gateway.topic_prefix);
        gateway.allow_send = args.allow_send;

        let password_reference = match &authentication {
            AuthenticationUpdate::Clear => {
                gateway.username = None;
                None
            }
            AuthenticationUpdate::Replace {
                username,
                reference,
            } => {
                gateway.username = Some(username.clone());
                Some(reference.clone())
            }
            AuthenticationUpdate::PreserveForUsername(username) => {
                if gateway.username.as_deref() != Some(username.as_str()) {
                    return Err(CliError::new(
                        ExitStatus::Usage,
                        "a new MQTT username requires --password-stdin or --password-env NAME",
                    ));
                }
                old_reference.clone()
            }
            AuthenticationUpdate::Preserve => old_reference.clone(),
        };

        gateway
            .validate()
            .map_err(|error| mqtt_config_error(&error))?;
        config.mqtt = MqttSettings {
            enabled: true,
            gateway,
            password: password_reference,
        };
        config.validate().map_err(CliError::from)?;
        locked.save(&config).map_err(CliError::from)?;
        Ok((config, old_reference))
    })();

    let (config, old_reference) = match update_result {
        Ok(result) => result,
        Err(error) => {
            cleanup_credential(fresh_credential).await;
            return Err(error);
        }
    };

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

fn canonical_tls_path(
    path: Option<&Path>,
    field: &'static str,
) -> Result<Option<PathBuf>, CliError> {
    path.map(|path| {
        fs::canonicalize(path).map_err(|_| {
            CliError::new(
                ExitStatus::Mqtt,
                format!("configured MQTT TLS {field} file is unavailable or invalid"),
            )
        })
    })
    .transpose()
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

    let command_readiness_required = settings.gateway.allow_send;
    let connected = tokio::time::timeout(cli.timeout, async {
        let mut broker_connected = false;
        loop {
            match handle.recv_notice().await {
                Some(GatewayNotice::BrokerState(ConnectionStatus::Connected)) => {
                    broker_connected = true;
                    if !command_readiness_required {
                        return Ok(());
                    }
                }
                Some(GatewayNotice::CommandReady) if broker_connected => return Ok(()),
                Some(
                    GatewayNotice::CommandReady
                    | GatewayNotice::BrokerState(ConnectionStatus::Disconnected)
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
    let command_limits = settings.gateway.command_limits;
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
        if workflow
            .disconnected(Some("MQTT bridge connection hook failed"))
            .await
            .is_err()
        {
            tracing::warn!("secondary MQTT disconnect hook failure; details omitted");
        }
        let _ = client.shutdown().await;
        let _ = join_gateway(gateway_task).await;
        return Err(error);
    }

    let result = match publish_full_device_snapshot(&client, &gateway, writer).await {
        Ok(()) => {
            bridge_loop(
                &client,
                &workflow,
                &mut gateway,
                &mut events,
                command_limits,
                writer,
            )
            .await
        }
        Err(error) => Err(error),
    };
    gateway.cancel();
    if let Err(error) = &result
        && !matches!(error.status(), ExitStatus::Hook | ExitStatus::Interrupted)
        && workflow
            .error("mqtt bridge", "MQTT bridge operation failed")
            .await
            .is_err()
    {
        tracing::warn!("secondary MQTT error hook failure; details omitted");
    }
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
        if disconnect_result.is_err() {
            tracing::warn!("secondary MQTT disconnect hook failure; details omitted");
        }
        if core_shutdown.is_err() {
            tracing::warn!("secondary MQTT client shutdown failure; details omitted");
        }
        if gateway_shutdown.is_err() {
            tracing::warn!("secondary MQTT gateway shutdown failure; details omitted");
        }
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
    command_limits: CommandLimits,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let mut pending = VecDeque::new();
    let mut contact_resync = ContactResyncDebounce::default();
    // A snapshot is refreshed only after an observed broker disconnect/connect pair.
    // This remains correct if the initial Connected notification is delayed or
    // absent and avoids treating a duplicate Connected notification as replay.
    let mut reconnect_pending = false;
    let mut timeout_tick = tokio::time::interval(Duration::from_millis(250));
    timeout_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let contact_resync_deadline = contact_resync.deadline();
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
            () = wait_for_contact_resync(contact_resync_deadline) => {
                if contact_resync.take_due(Instant::now()) {
                    publish_contact_snapshot(
                        client,
                        gateway,
                        writer,
                        "contact_resync_query_failed",
                    ).await?;
                }
            }
            event = events.recv() => {
                match event {
                    Ok(event) => {
                        let disconnected = matches!(&event, Event::Disconnected);
                        contact_resync.observe(&event, Instant::now());
                        publish_core_event(
                            workflow,
                            &mut pending,
                            gateway,
                            event,
                            writer,
                        ).await?;
                        if disconnected {
                            while let Some(mut item) = pending.pop_front() {
                                record_failed_send(workflow, &mut item.record).await;
                            }
                            return Err(CliError::new(
                                ExitStatus::Connection,
                                "the MeshCore companion disconnected while the MQTT bridge was running",
                            ).with_hint("Restart the bridge after restoring the device connection."));
                        }
                    }
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
                let reconnect_snapshot = broker_reconnect_requires_snapshot(
                    notice.as_ref(),
                    &mut reconnect_pending,
                );
                handle_gateway_notice(
                    client,
                    workflow,
                    &mut pending,
                    notice,
                    command_limits,
                    writer,
                ).await?;
                if reconnect_snapshot {
                    contact_resync.clear();
                    // Re-query current device state. Outgoing messages are never synthesized or
                    // replayed during broker reconnect synchronization.
                    publish_full_device_snapshot(client, gateway, writer).await?;
                }
            }
        }
    }
}

fn broker_reconnect_requires_snapshot(
    notice: Option<&GatewayNotice>,
    reconnect_pending: &mut bool,
) -> bool {
    match notice {
        Some(GatewayNotice::BrokerState(ConnectionStatus::Disconnected)) => {
            *reconnect_pending = true;
            false
        }
        Some(GatewayNotice::BrokerState(ConnectionStatus::Connected)) if *reconnect_pending => {
            *reconnect_pending = false;
            true
        }
        _ => false,
    }
}

async fn wait_for_contact_resync(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

async fn publish_full_device_snapshot<W: Write>(
    client: &ManagedClient,
    gateway: &GatewayHandle,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    for publication in collect_full_device_snapshot(client, writer).await? {
        gateway
            .publish(publication)
            .await
            .map_err(mqtt_gateway_error)?;
    }
    Ok(())
}

async fn collect_full_device_snapshot<W: Write>(
    client: &ManagedClient,
    writer: &mut OutputWriter<W>,
) -> Result<Vec<Publication>, CliError> {
    let mut publications = Vec::with_capacity(3);
    if let Some(contacts) =
        collect_contact_snapshot(client, writer, "contact_snapshot_query_failed").await?
    {
        publications.push(contacts);
    }

    // Managed core queries inherit the profile's validated finite request
    // timeout. A failed query emits a static diagnostic and contributes no fake
    // sample or placeholder value.
    match client.get_battery().await {
        Ok(info) => publications.push(Publication::battery(info)),
        Err(_) => write_snapshot_diagnostic(
            writer,
            "battery",
            "battery_snapshot_query_failed",
            "Skipped MQTT battery snapshot because the bounded device query failed.",
        )?,
    }
    match client.get_self_telemetry().await {
        Ok(response) => publications.push(Publication::raw_telemetry(response)),
        Err(_) => write_snapshot_diagnostic(
            writer,
            "raw_cayenne_lpp",
            "self_telemetry_snapshot_query_failed",
            "Skipped MQTT raw telemetry snapshot because the bounded device query failed.",
        )?,
    }
    Ok(publications)
}

async fn publish_contact_snapshot<W: Write>(
    client: &ManagedClient,
    gateway: &GatewayHandle,
    writer: &mut OutputWriter<W>,
    failure_reason: &'static str,
) -> Result<(), CliError> {
    if let Some(publication) = collect_contact_snapshot(client, writer, failure_reason).await? {
        gateway
            .publish(publication)
            .await
            .map_err(mqtt_gateway_error)?;
    }
    Ok(())
}

async fn collect_contact_snapshot<W: Write>(
    client: &ManagedClient,
    writer: &mut OutputWriter<W>,
    failure_reason: &'static str,
) -> Result<Option<Publication>, CliError> {
    let Ok(snapshot) = client.list_contacts_snapshot(None).await else {
        write_snapshot_diagnostic(
            writer,
            "contacts",
            failure_reason,
            "Skipped MQTT contact snapshot because no authoritative directory marker was available.",
        )?;
        return Ok(None);
    };
    Ok(Some(Publication::contacts(snapshot)))
}

fn write_snapshot_diagnostic<W: Write>(
    writer: &mut OutputWriter<W>,
    publication: &'static str,
    reason: &'static str,
    human: &'static str,
) -> Result<(), CliError> {
    let report = SnapshotDiagnosticReport {
        publication,
        published: false,
        reason,
    };
    writer
        .event("mqtt_diagnostic", &report, human)
        .map_err(CliError::from)
}

async fn handle_gateway_notice<W: Write>(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    pending: &mut VecDeque<PendingBrokerAck>,
    notice: Option<GatewayNotice>,
    command_limits: CommandLimits,
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
        Some(GatewayNotice::CommandReady) => Ok(()),
        Some(GatewayNotice::Command(command)) => {
            let outcome =
                execute_command_once(client, workflow, command, pending.len(), command_limits)
                    .await;
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
    command_limits: CommandLimits,
) -> BrokerCommandOutcome {
    let event_id = accepted.event_id.to_string();
    match accepted.command {
        SendCommand::Direct { destination, text } => {
            execute_direct_command(
                client,
                workflow,
                event_id,
                destination,
                text,
                pending_count,
                command_limits,
            )
            .await
        }
        SendCommand::Channel { channel, text } => {
            execute_channel_command(client, workflow, event_id, channel, text, command_limits).await
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
    command_limits: CommandLimits,
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
    if validate_send_command(
        &SendCommand::Direct {
            destination: prepared.destination.clone(),
            text: prepared.text.clone(),
        },
        command_limits,
    )
    .is_err()
    {
        return rejected_command(event_id, "direct_send", "before_send_out_of_bounds");
    }
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
            Some("post_send_workflow_failed"),
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
    command_limits: CommandLimits,
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
    if validate_send_command(
        &SendCommand::Channel {
            channel: prepared_channel,
            text: prepared.text.clone(),
        },
        command_limits,
    )
    .is_err()
    {
        return rejected_command(event_id, "channel_send", "before_send_out_of_bounds");
    }
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
            Some("post_send_workflow_failed"),
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
    let maximum = u64::try_from(MAX_MQTT_PASSWORD_BYTES + 1).unwrap_or(u64::MAX);
    let mut input = io::stdin().lock().take(maximum);
    let mut bytes = Zeroizing::new(Vec::with_capacity(128));
    input.read_to_end(&mut bytes).map_err(|_| {
        CliError::new(
            ExitStatus::Authentication,
            "could not read the MQTT password from stdin",
        )
    })?;
    if bytes.len() > MAX_MQTT_PASSWORD_BYTES {
        return Err(CliError::new(
            ExitStatus::Usage,
            format!("MQTT password exceeds the {MAX_MQTT_PASSWORD_BYTES}-byte limit"),
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
    let mut password = match String::from_utf8(std::mem::take(&mut *bytes)) {
        Ok(password) => Zeroizing::new(password),
        Err(error) => {
            let _invalid_password = Zeroizing::new(error.into_bytes());
            return Err(CliError::new(
                ExitStatus::Usage,
                "MQTT password from stdin must be valid UTF-8",
            ));
        }
    };
    if password.contains('\0') {
        return Err(CliError::new(
            ExitStatus::Usage,
            "MQTT password from stdin must not contain NUL",
        ));
    }
    Ok(SecretString::from(std::mem::take(&mut *password)))
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
        CommandError::RetainedCommand => "retained_command",
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
        GatewayError::CommandSubscriptionRejected => CliError::new(
            ExitStatus::Mqtt,
            "MQTT broker rejected the outbound-command subscription",
        )
        .with_hint("Check broker ACLs for the configured v1 outbound topic."),
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
    use crate::args::OutputMode;
    use meshquill_core::Client;
    use meshquill_mqtt::{ContactTypeData, TelemetryData};
    use meshquill_test_support::{ContactFixture, VirtualCompanion, make_contact_row};

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

    #[tokio::test]
    async fn startup_snapshot_collects_contacts_battery_and_raw_telemetry() {
        let companion = VirtualCompanion::new();
        let row = make_contact_row(&ContactFixture {
            public_key: [0xab; 32],
            contact_type: 3,
            route: 0x02,
            path: &[0x12, 0x34],
            adv_name: "sensor",
            last_advert: 7,
            adv_lat: 0.5,
            adv_lon: -0.25,
            lastmod: 8,
        })
        .expect("valid contact row");
        companion
            .set_contacts([row])
            .expect("configure contact fixture");
        let client = ManagedClient::spawn(
            Client::with_timeout(companion, Duration::from_secs(1)).expect("valid request timeout"),
        );
        client.connect().await.expect("connect virtual companion");
        let mut writer = OutputWriter::new(OutputMode::Jsonl, Vec::new());

        let publications = collect_full_device_snapshot(&client, &mut writer)
            .await
            .expect("collect startup snapshot");
        assert_eq!(publications.len(), 3);
        match &publications[0] {
            Publication::Contacts(data) => {
                assert_eq!(data.lastmod, 0);
                assert_eq!(data.contacts.len(), 1);
                assert_eq!(data.contacts[0].public_key, "ab".repeat(32));
                assert_eq!(data.contacts[0].out_path, "1234");
                assert_eq!(data.contacts[0].contact_type, ContactTypeData::Sensor);
            }
            other => panic!("expected contacts publication, got {other:?}"),
        }
        assert!(matches!(
            publications[1],
            Publication::Telemetry(TelemetryData::Battery { .. })
        ));
        match &publications[2] {
            Publication::Telemetry(TelemetryData::RawCayenneLpp {
                source_pubkey_prefix,
                payload,
            }) => {
                assert_eq!(source_pubkey_prefix.len(), 12);
                assert!(
                    source_pubkey_prefix
                        .bytes()
                        .all(|byte| { byte.is_ascii_digit() || matches!(byte, b'a'..=b'f') })
                );
                assert!(!payload.is_empty());
            }
            other => panic!("expected raw telemetry publication, got {other:?}"),
        }
        assert!(writer.into_inner().is_empty());
        client.shutdown().await.expect("shutdown virtual companion");
    }

    #[tokio::test]
    async fn failed_snapshot_queries_are_nonfatal_and_never_create_samples() {
        let client = ManagedClient::spawn(
            Client::with_timeout(VirtualCompanion::new(), Duration::from_millis(20))
                .expect("valid request timeout"),
        );
        let mut writer = OutputWriter::new(OutputMode::Jsonl, Vec::new());

        let publications = collect_full_device_snapshot(&client, &mut writer)
            .await
            .expect("query failures should remain nonfatal");
        assert!(publications.is_empty());

        let output = String::from_utf8(writer.into_inner()).expect("diagnostics are UTF-8");
        assert_eq!(output.lines().count(), 3);
        assert!(output.contains("\"published\":false"));
        assert!(!output.contains("\"published\":true"));
        assert!(!output.contains("public_key"));
        assert!(!output.contains("payload"));
        client.shutdown().await.expect("shutdown virtual companion");
    }

    #[test]
    fn opaque_contact_notifications_debounce_one_authoritative_resync() {
        let start = Instant::now();
        let mut debounce = ContactResyncDebounce::default();
        assert!(!debounce.observe(
            &Event::UnknownPacket {
                code: 0x89,
                payload: vec![0xff],
            },
            start,
        ));

        for (offset, code) in [(0, 0x8a), (100, 0x8f), (200, 0x90)] {
            assert!(debounce.observe(
                &Event::UnknownPacket {
                    code,
                    // Deliberately malformed/opaque: scheduling must not inspect it.
                    payload: vec![0xff, 0x00, 0x80],
                },
                start + Duration::from_millis(offset),
            ));
        }
        let deadline = debounce.deadline().expect("resync scheduled");
        assert_eq!(
            deadline,
            start + Duration::from_millis(200) + CONTACT_RESYNC_DEBOUNCE
        );
        assert!(!debounce.take_due(deadline - Duration::from_millis(1)));
        assert!(debounce.take_due(deadline));
        assert!(!debounce.take_due(deadline));
    }

    #[test]
    fn only_broker_disconnect_then_connect_requests_another_full_snapshot() {
        let mut reconnect_pending = false;
        assert!(!broker_reconnect_requires_snapshot(
            Some(&GatewayNotice::BrokerState(ConnectionStatus::Connected)),
            &mut reconnect_pending,
        ));
        assert!(!broker_reconnect_requires_snapshot(
            Some(&GatewayNotice::BrokerState(ConnectionStatus::Disconnected,)),
            &mut reconnect_pending,
        ));
        assert!(broker_reconnect_requires_snapshot(
            Some(&GatewayNotice::BrokerState(ConnectionStatus::Connected)),
            &mut reconnect_pending,
        ));
        assert!(!broker_reconnect_requires_snapshot(
            Some(&GatewayNotice::BrokerState(ConnectionStatus::Connected)),
            &mut reconnect_pending,
        ));
        assert!(!broker_reconnect_requires_snapshot(
            Some(&GatewayNotice::CommandReady),
            &mut reconnect_pending,
        ));
    }
}
