//! Async command dispatch and native CLI operations.

use std::{
    collections::HashSet,
    fs,
    io::{self, BufRead, IsTerminal, Read, Write},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc as std_mpsc,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::CommandFactory;
use meshquill_core::{
    Client, Contact, ContactRoute, ContactType, CoreError, DefaultFloodScope, DeviceInfo, Event,
    FloodScope, ManagedClient, Message, SelfInfo, domain::CommandTracking,
    protocol::MAX_INNER_PAYLOAD,
};
use meshquill_hooks::ContactChange;
use meshquill_store::{
    Config, DeviceProfile, HistoryDirection, HistoryEntry, LoadOutcome, TransportConfig,
};
use meshquill_transport::{
    DiscoveredDevice, discover_ble, discover_serial_async, manual_tcp_device,
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{broadcast, mpsc as tokio_mpsc};
use zeroize::Zeroizing;

use crate::{
    args::{
        ChannelCommand, Cli, Command, ConfigCommand, ContactCommand, ContactKind, ContactsArgs,
        DeviceCommand, DevicesArgs, HistoryCommand, InboxArgs, NetworkCommand, OutputMode,
        SendArgs, TransportChoice, WatchArgs, WatchEvent,
    },
    batch_cli,
    config::{
        SelectedProfile, config_store, initialize, load_optional, load_unmodified,
        load_unmodified_locked, select_profile,
    },
    error::CliError,
    hooks_cli,
    input::read_bounded_line,
    interrupt::InterruptWatcher,
    mqtt_cli,
    output::{ExitStatus, OutputWriter},
    profiles,
    reconnect::{ReconnectPolicy, reconnect_device, reconnect_trigger},
    remote_cli,
    transport::CliTransport,
    workflow::{
        CompanionSession, IncomingOrigin, OutgoingRecord, WorkflowServices, clear_history,
        history_store_for_selected, load_history,
    },
};

#[derive(Debug, Serialize)]
struct StatusReport {
    profile: String,
    config_path: String,
    transport: String,
    needs_migration: bool,
}

#[derive(Debug, Serialize)]
struct ConnectionReport {
    profile: String,
    transport: String,
    connected: bool,
    self_info: SelfInfo,
}

#[derive(Debug, Serialize)]
struct DeviceReport {
    profile: String,
    self_info: SelfInfo,
    device_info: DeviceInfo,
}

#[derive(Debug, Serialize)]
struct ContactView {
    name: String,
    public_key: String,
    kind: String,
    flags: u8,
    route: String,
    path: String,
    last_advert: u32,
    lastmod: u32,
}

#[derive(Debug, Serialize)]
struct ContactReport {
    profile: String,
    #[serde(flatten)]
    contact: ContactView,
}

#[derive(Debug, Serialize)]
struct ContactsReport {
    profile: String,
    /// Contact lists always come from a fresh device query; no cached path exists.
    refreshed: bool,
    /// Records use of the compatibility spelling that explicitly requests that query.
    refresh_requested: bool,
    contacts: Vec<ContactView>,
}

#[derive(Debug, Serialize)]
struct ContactUpdateReport {
    profile: String,
    public_key: String,
    name: String,
    favorite: bool,
}

#[derive(Debug, Serialize)]
struct ContactExportReport {
    profile: String,
    contact: String,
    public_key: String,
    uri: String,
}

#[derive(Debug, Serialize)]
struct ContactImportReport {
    profile: String,
    card_bytes: usize,
}

#[derive(Debug, Serialize)]
struct ContactForgetReport {
    profile: String,
    contact: String,
    public_key: String,
}

#[derive(Debug, Serialize)]
struct ContactPathReport {
    profile: String,
    contact: String,
    public_key: String,
    route: String,
    received_at: u32,
    path: String,
}

#[derive(Debug, Serialize)]
struct ContactPathDiscoveryReport {
    profile: String,
    contact: String,
    public_key: String,
    discovery: meshquill_core::PathDiscovery,
}

#[derive(Debug, Serialize)]
struct ContactPathResetReport {
    profile: String,
    contact: String,
    public_key: String,
    reset: bool,
}

#[derive(Debug, Serialize)]
struct ContactPathSetReport {
    profile: String,
    contact: String,
    public_key: String,
    hash_bytes: u8,
    hop_count: u8,
    path: String,
    updated: bool,
}

#[derive(Debug, Serialize)]
struct SendReport {
    destination: String,
    channel: Option<u8>,
    queued: bool,
    ack_code: Option<String>,
    acknowledged: bool,
    trip_time_ms: Option<u32>,
}

#[derive(Debug, Serialize)]
struct DeviceClockReport {
    profile: String,
    previous_time: u32,
    synced: bool,
    current_time: u32,
}

#[derive(Debug, Serialize)]
struct DeviceAdvertiseReport {
    profile: String,
    flood: bool,
}

#[derive(Debug, Serialize)]
struct DeviceRebootReport {
    profile: String,
    disconnected: bool,
}

#[derive(Debug, Serialize)]
struct DeviceTelemetryReport {
    profile: String,
    battery_level: u16,
    storage_used_kb: Option<u32>,
    storage_total_kb: Option<u32>,
    pubkey_prefix: String,
    payload: String,
}

#[derive(Debug, Serialize)]
struct InboxReport {
    profile: String,
    messages: Vec<Message>,
    drained: bool,
}

#[derive(Debug, Serialize)]
struct HistoryReport {
    profile: String,
    enabled: bool,
    storage: &'static str,
    path: String,
    entries: Vec<HistoryEntry>,
}

#[derive(Debug, Serialize)]
struct HistoryClearReport {
    profile: String,
    path: String,
    cleared: bool,
}

#[derive(Debug, Serialize)]
struct ConfigShowReport {
    path: String,
    needs_migration: bool,
    effective: meshquill_store::EffectiveConfig,
}

#[derive(Debug, Serialize)]
struct ConfigChangeReport {
    path: String,
    changed: bool,
    backup_path: Option<String>,
}

#[derive(Debug, Serialize)]
struct LegacyImportReport {
    source: String,
    config_path: String,
    profile: String,
    default: bool,
    transport: &'static str,
}

#[derive(Debug, Serialize)]
struct ChannelInfoView {
    idx: u8,
    name: String,
    secret_hash: Option<u8>,
}

#[derive(Debug, Serialize)]
struct ChannelsReport {
    profile: String,
    channels: Vec<ChannelInfoView>,
}

#[derive(Debug, Serialize)]
struct ChannelReport {
    profile: String,
    idx: u8,
    name: String,
    secret_hash: Option<u8>,
}

#[derive(Debug, Serialize)]
struct ChannelSetReport {
    profile: String,
    idx: u8,
    name: String,
    secret_set: bool,
}

#[derive(Debug, Serialize)]
struct ChannelRemoveReport {
    profile: String,
    idx: u8,
    previous_name: String,
    removed: bool,
}

#[derive(Debug, Serialize)]
struct NetworkScopeReport {
    profile: String,
    action: String,
    scope: Option<String>,
}

#[derive(Debug, Serialize)]
struct NetworkTraceReport {
    profile: String,
    target: String,
    public_key: String,
    discovery: meshquill_core::PathDiscovery,
}

#[derive(Debug, Serialize)]
struct NetworkDiscoveryNode {
    public_key_prefix: String,
    key_bytes: usize,
    node_type: u8,
    kind: &'static str,
    snr_qdb: i8,
    inbound_snr_qdb: i8,
    rssi_dbm: i8,
    path_len: u8,
}

#[derive(Debug, Serialize)]
struct NetworkDiscoveryReport {
    profile: String,
    filter: &'static str,
    scope: Option<String>,
    timeout_ms: u64,
    nodes: Vec<NetworkDiscoveryNode>,
}

static DISCOVERY_NONCE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize)]
struct ArtifactReport {
    files: Vec<String>,
}

/// Dispatch one parsed command using the selected stdout contract.
pub(crate) async fn dispatch<W: Write>(
    cli: &Cli,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    validate_output_shape(cli)?;
    if let Some(error) = unsupported_command(cli) {
        return Err(error);
    }

    match &cli.command {
        Command::Init(args) => {
            let report = initialize(cli, args).await?;
            let human = format!(
                "Created {} profile '{}' at {}{}",
                report.transport,
                report.profile,
                report.config_path,
                if report.default { " (default)" } else { "" }
            );
            writer
                .result("configuration_initialized", &report, &human)
                .map_err(CliError::from)
        }
        Command::Profiles(command) => profiles::profiles(cli, command, writer),
        Command::Devices(args) => devices(cli, args, writer).await,
        Command::Connect(args) if args.watch => watch_connection(cli, writer).await,
        Command::Connect(_) => connect(cli, writer).await,
        Command::Status => status(cli, writer),
        Command::Doctor(args) => doctor(cli, args.connect, args.repair, writer).await,
        Command::Device(DeviceCommand::Info) => device_info(cli, false, writer).await,
        Command::Device(DeviceCommand::Firmware) => device_info(cli, true, writer).await,
        Command::Device(DeviceCommand::Telemetry) => device_telemetry(cli, writer).await,
        Command::Device(DeviceCommand::Clock(args)) => device_clock(cli, args, writer).await,
        Command::Device(DeviceCommand::Advertise(args)) => {
            device_advertise(cli, args, writer).await
        }
        Command::Device(DeviceCommand::Reboot) => device_reboot(cli, writer).await,
        Command::Contacts(args) => contacts(cli, args, writer).await,
        Command::Channels(args) => channels(cli, args, writer).await,
        Command::Send(args) => send(cli, args, writer).await,
        Command::Inbox(args) => inbox(cli, args, writer).await,
        Command::History(command) => history(cli, command, writer).await,
        Command::Watch(args) => watch(cli, args, writer).await,
        Command::Chat(args) => chat(cli, args.destination.as_deref(), args.line, writer).await,
        Command::Network(command) => network(cli, command, writer).await,
        Command::Remote(command) => remote_cli::remote(cli, command, writer).await,
        Command::Sensor(command) => remote_cli::sensor(cli, command, writer).await,
        Command::Batch(command) => batch_cli::batch(cli, command, writer).await,
        Command::Hooks(args) => hooks_cli::hooks(cli, args, writer).await,
        Command::Mqtt(args) => mqtt_cli::mqtt(cli, args, writer).await,
        Command::Config(ConfigCommand::Show) => config_show(cli, writer),
        Command::Config(ConfigCommand::Migrate) => config_migrate(cli, writer),
        Command::Config(ConfigCommand::Repair) => config_repair(cli, writer),
        Command::Config(ConfigCommand::ImportLegacy { path }) => {
            config_import_legacy(cli, path.as_deref(), writer)
        }
        Command::Completions(args) => completions(args.shell, writer),
        Command::Manpages(args) => manpages(&args.directory, writer),
    }
}

fn validate_output_shape(cli: &Cli) -> Result<(), CliError> {
    let is_stream = matches!(
        &cli.command,
        Command::Watch(_) | Command::Chat(_) | Command::Mqtt(crate::args::MqttCommand::Bridge)
    ) || matches!(&cli.command, Command::Connect(args) if args.watch);
    match (is_stream, cli.output) {
        (true, OutputMode::Json) => Err(CliError::new(
            ExitStatus::Usage,
            "streaming commands require --output jsonl (or human)",
        )),
        (false, OutputMode::Jsonl) => Err(CliError::new(
            ExitStatus::Usage,
            "single-result commands require --output json (or human)",
        )),
        _ => Ok(()),
    }
}

fn unsupported_command(cli: &Cli) -> Option<CliError> {
    let area = match &cli.command {
        Command::Contacts(ContactsArgs {
            command: Some(ContactCommand::Pending(_)),
            ..
        }) => "contact mutation",
        _ => return None,
    };
    Some(CliError::unsupported(area))
}

fn status<W: Write>(cli: &Cli, writer: &mut OutputWriter<W>) -> Result<(), CliError> {
    let selected = select_profile(cli)?;
    let report = StatusReport {
        profile: selected.name,
        config_path: selected.path.display().to_string(),
        transport: describe_transport(&selected.profile.transport),
        needs_migration: selected.needs_migration,
    };
    let human = format!(
        "Profile: {}\nTransport: {}\nConfig: {}\nConnection: not probed",
        report.profile, report.transport, report.config_path
    );
    writer
        .result("status", &report, &human)
        .map_err(CliError::from)
}

// Audited direct-connect exceptions: connect, send, and inbox own established lifecycle flows;
// doctor performs a diagnostic probe; watch and chat own reconnect state machines. MQTT owns its
// separate gateway lifecycle in mqtt_cli, while batch reaches sessions through nested dispatch.
async fn connect<W: Write>(cli: &Cli, writer: &mut OutputWriter<W>) -> Result<(), CliError> {
    let selected = select_profile(cli)?;
    let workflow = WorkflowServices::from_selected(&selected)?;
    let client = make_client(&selected)?;
    let self_info = match client.connect().await {
        Ok(info) => info,
        Err(error) => return finish::<()>(&client, Err(error)).await,
    };
    activate_workflow(
        &client,
        &workflow,
        &self_info.name,
        "connection hook failed",
    )
    .await?;
    let report = ConnectionReport {
        profile: selected.name,
        transport: describe_transport(&selected.profile.transport),
        connected: true,
        self_info,
    };
    let human = format!(
        "Connected profile '{}' over {} as {}",
        report.profile, report.transport, report.self_info.name
    );
    finish_workflow(&client, &workflow, "command completed").await?;
    writer
        .result("connection", &report, &human)
        .map_err(CliError::from)
}

async fn device_info<W: Write>(
    cli: &Cli,
    firmware_only: bool,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let selected = select_profile(cli)?;
    let client = make_client(&selected)?;
    let operation_name = if firmware_only {
        "device firmware"
    } else {
        "device info"
    };
    let (session, self_info) = CompanionSession::connect(&selected, client, operation_name).await?;
    let operation = session
        .client()
        .query_device_info()
        .await
        .map_err(CliError::from);
    let device_info = session.finish(operation).await?;
    let report = DeviceReport {
        profile: selected.name,
        self_info,
        device_info,
    };
    let human = if firmware_only {
        format!(
            "Model: {}\nFirmware: {}\nBuild: {}\nProtocol: {}",
            report.device_info.model.as_deref().unwrap_or("unknown"),
            report
                .device_info
                .firmware_version
                .as_deref()
                .unwrap_or("unknown"),
            report
                .device_info
                .firmware_build
                .as_deref()
                .unwrap_or("unknown"),
            report.device_info.protocol_version
        )
    } else {
        format!(
            "Device: {}\nModel: {}\nFirmware: {}\nProtocol: {}",
            report.self_info.name,
            report.device_info.model.as_deref().unwrap_or("unknown"),
            report
                .device_info
                .firmware_version
                .as_deref()
                .unwrap_or("unknown"),
            report.device_info.protocol_version
        )
    };
    writer
        .result(
            if firmware_only { "firmware" } else { "device" },
            &report,
            &human,
        )
        .map_err(CliError::from)
}

async fn device_telemetry<W: Write>(
    cli: &Cli,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let selected = select_profile(cli)?;
    let client = make_client(&selected)?;
    let (session, _) = CompanionSession::connect(&selected, client, "device telemetry").await?;
    let operation = async {
        let battery = session
            .client()
            .get_battery()
            .await
            .map_err(CliError::from)?;
        let telemetry = session
            .client()
            .get_self_telemetry()
            .await
            .map_err(CliError::from)?;
        Ok((battery, telemetry))
    }
    .await;
    let (battery, telemetry) = session.finish(operation).await?;
    let report = DeviceTelemetryReport {
        profile: selected.name,
        battery_level: battery.level,
        storage_used_kb: battery.used_kb,
        storage_total_kb: battery.total_kb,
        pubkey_prefix: bytes_hex(&telemetry.pubkey_prefix),
        payload: bytes_hex(&telemetry.payload),
    };
    let human = format!(
        "Battery: {} mV\nStorage: {}/{} KiB\nTelemetry source {} with {} payload bytes",
        report.battery_level,
        report
            .storage_used_kb
            .map_or_else(|| "unknown".to_owned(), |value| value.to_string()),
        report
            .storage_total_kb
            .map_or_else(|| "unknown".to_owned(), |value| value.to_string()),
        report.pubkey_prefix,
        telemetry.payload.len()
    );
    writer
        .result("telemetry", &report, &human)
        .map_err(CliError::from)
}

async fn device_clock<W: Write>(
    cli: &Cli,
    args: &crate::args::DeviceClockArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let host_time = args.sync.then(bounded_unix_time).transpose()?;
    let selected = select_profile(cli)?;
    let client = make_client(&selected)?;
    let (session, _) = CompanionSession::connect(&selected, client, "device clock").await?;
    let operation = async {
        let previous_time = session.client().get_time().await.map_err(CliError::from)?;
        let Some(host_time) = host_time else {
            return Ok(DeviceClockReport {
                profile: selected.name,
                previous_time,
                synced: false,
                current_time: previous_time,
            });
        };
        session
            .client()
            .set_time(host_time)
            .await
            .map_err(CliError::from)?;
        let current_time = session.client().get_time().await.map_err(CliError::from)?;
        if current_time != host_time {
            return Err(CliError::from(CoreError::ProtocolInvariant(
                "device clock readback did not match requested host time",
            )));
        }
        Ok(DeviceClockReport {
            profile: selected.name,
            previous_time,
            synced: true,
            current_time,
        })
    }
    .await;
    let report = session.finish(operation).await?;
    let human = if report.synced {
        format!(
            "Device time was {}; synced to {}",
            report.previous_time, report.current_time
        )
    } else {
        format!("Device time is {}", report.current_time)
    };
    writer
        .result("device_clock", &report, &human)
        .map_err(CliError::from)
}

async fn device_advertise<W: Write>(
    cli: &Cli,
    args: &crate::args::DeviceAdvertiseArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let scope = match args.scope.as_deref() {
        Some(scope) => Some(parse_flood_scope(scope)?),
        None => None,
    };
    let selected = select_profile(cli)?;
    let client = make_client(&selected)?;
    let (session, _) = CompanionSession::connect(&selected, client, "device advertise").await?;
    let operation = async {
        if let Some((scope, _)) = &scope {
            session
                .client()
                .set_flood_scope(scope)
                .await
                .map_err(CliError::from)?;
        }
        let send = session
            .client()
            .send_self_advert(args.flood)
            .await
            .map_err(CliError::from);
        let restore = if scope.is_some() {
            session
                .client()
                .set_flood_scope(&FloodScope::Default)
                .await
                .map_err(CliError::from)
        } else {
            Ok(())
        };
        send?;
        restore?;
        Ok(DeviceAdvertiseReport {
            profile: selected.name,
            flood: args.flood,
        })
    }
    .await;
    let report = session.finish(operation).await?;
    let human = if args.flood {
        "Sent flood advertisement"
    } else {
        "Sent normal advertisement"
    };
    writer
        .result("advertise", &report, human)
        .map_err(CliError::from)
}

async fn device_reboot<W: Write>(cli: &Cli, writer: &mut OutputWriter<W>) -> Result<(), CliError> {
    confirm(cli, "reboot the local companion")?;
    let selected = select_profile(cli)?;
    let client = make_client(&selected)?;
    let (session, _) = CompanionSession::connect(&selected, client, "device reboot").await?;
    let operation = session.client().reboot().await.map_err(CliError::from);
    let report = session
        .finish(operation.map(|()| DeviceRebootReport {
            profile: selected.name,
            disconnected: true,
        }))
        .await?;
    let human = "Reboot command sent";
    writer
        .result("device_reboot", &report, human)
        .map_err(CliError::from)
}

async fn network<W: Write>(
    cli: &Cli,
    command: &NetworkCommand,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    match command {
        NetworkCommand::Discover(args) => network_discover(cli, args, writer).await,
        NetworkCommand::Trace(args) => network_trace(cli, args, writer).await,
        NetworkCommand::Scope(args) => network_scope(cli, args, writer).await,
    }
}

async fn network_discover<W: Write>(
    cli: &Cli,
    args: &crate::args::NetworkDiscoverArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let temporary_scope = args.scope.as_deref().map(parse_flood_scope).transpose()?;
    let tag = discovery_request_nonce()?;
    let selected = select_profile(cli)?;
    let client = make_client(&selected)?;
    let mut events = client.subscribe();
    let (session, _) = CompanionSession::connect(&selected, client, "network discover").await?;
    if let Some((scope, _)) = &temporary_scope {
        let scope_result = session
            .client()
            .set_flood_scope(scope)
            .await
            .map_err(CliError::from);
        if let Err(error) = scope_result {
            return session.finish(Err(error)).await;
        }
    }

    let filter = discovery_filter(args.kind);
    let operation = async {
        session
            .client()
            .send_node_discovery(filter, true, tag, None)
            .await
            .map_err(CliError::from)?;
        collect_node_discovery(&mut events, tag, cli.timeout).await
    }
    .await;
    let restore = if temporary_scope.is_some() {
        session
            .client()
            .set_flood_scope(&FloodScope::Default)
            .await
            .map_err(CliError::from)
    } else {
        Ok(())
    };
    let operation = match (operation, restore) {
        (Err(primary), Err(_)) => {
            tracing::warn!("secondary flood-scope restore failure; details omitted");
            Err(primary)
        }
        (Err(primary), Ok(())) => Err(primary),
        (Ok(_), Err(restore)) => Err(restore),
        (Ok(nodes), Ok(())) => Ok(nodes),
    };
    let nodes = session.finish(operation).await?;

    let timeout_ms = u64::try_from(cli.timeout.as_millis()).unwrap_or(u64::MAX);
    let report = NetworkDiscoveryReport {
        profile: selected.name,
        filter: discovery_filter_name(args.kind),
        scope: args.scope.clone(),
        timeout_ms,
        nodes,
    };
    let human = network_discovery_human(&report);
    writer
        .result("network_discovery", &report, &human)
        .map_err(CliError::from)
}

async fn collect_node_discovery(
    events: &mut broadcast::Receiver<Event>,
    tag: u32,
    duration: Duration,
) -> Result<Vec<NetworkDiscoveryNode>, CliError> {
    let deadline = tokio::time::Instant::now() + duration;
    let mut seen = HashSet::new();
    let mut nodes = Vec::new();
    loop {
        let event = match tokio::time::timeout_at(deadline, events.recv()).await {
            Err(_) => return Ok(nodes),
            Ok(Ok(event)) => event,
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                return Err(CliError::new(
                    ExitStatus::Protocol,
                    "node-discovery event buffering lagged; results may be incomplete",
                )
                .with_hint("Retry with fewer concurrent event consumers."));
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                return Err(CliError::new(
                    ExitStatus::Connection,
                    "the device event stream closed during node discovery",
                ));
            }
        };
        let Event::ControlData(data) = event else {
            continue;
        };
        let Some(response) = data.node_discovery_response().map_err(|_| {
            CliError::new(
                ExitStatus::Protocol,
                "the device emitted a malformed node-discovery response",
            )
        })?
        else {
            continue;
        };
        if response.tag != tag || !seen.insert(response.public_key.clone()) {
            continue;
        }
        nodes.push(NetworkDiscoveryNode {
            public_key_prefix: hex::encode(&response.public_key),
            key_bytes: response.public_key.len(),
            node_type: response.node_type,
            kind: discovery_node_type_name(response.node_type),
            snr_qdb: data.snr_qdb,
            inbound_snr_qdb: response.inbound_snr_qdb,
            rssi_dbm: data.rssi,
            path_len: data.path_len,
        });
    }
}

const fn discovery_filter(kind: Option<ContactKind>) -> u8 {
    match kind {
        None => u8::MAX,
        Some(ContactKind::Client) => 1 << 1,
        Some(ContactKind::Repeater) => 1 << 2,
        Some(ContactKind::Room) => 1 << 3,
        Some(ContactKind::Sensor) => 1 << 4,
    }
}

const fn discovery_filter_name(kind: Option<ContactKind>) -> &'static str {
    match kind {
        None => "all",
        Some(ContactKind::Client) => "client",
        Some(ContactKind::Repeater) => "repeater",
        Some(ContactKind::Room) => "room",
        Some(ContactKind::Sensor) => "sensor",
    }
}

const fn discovery_node_type_name(node_type: u8) -> &'static str {
    match node_type {
        0 => "client",
        1 => "repeater",
        2 => "room",
        3 => "sensor",
        _ => "unknown",
    }
}

fn discovery_request_nonce() -> Result<u32, CliError> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        CliError::new(
            ExitStatus::Protocol,
            "system time is unavailable for node-discovery correlation",
        )
    })?;
    let mut digest = Sha256::new();
    digest.update(elapsed.as_nanos().to_le_bytes());
    digest.update(std::process::id().to_le_bytes());
    digest.update(
        DISCOVERY_NONCE_COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .to_le_bytes(),
    );
    let bytes = digest.finalize();
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).max(1))
}

fn network_discovery_human(report: &NetworkDiscoveryReport) -> String {
    if report.nodes.is_empty() {
        return format!(
            "No {} nodes responded within {} ms.",
            report.filter, report.timeout_ms
        );
    }
    let rows = report
        .nodes
        .iter()
        .map(|node| {
            format!(
                "{}\ttype={} ({})\tsnr={} dB\tinbound_snr={} dB\trssi={} dBm\tpath_len={}",
                node.public_key_prefix,
                node.node_type,
                node.kind,
                format_qdb(node.snr_qdb),
                format_qdb(node.inbound_snr_qdb),
                node.rssi_dbm,
                node.path_len,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "Discovered {} {} node(s).\n{rows}",
        report.nodes.len(),
        report.filter
    )
}

fn format_qdb(value: i8) -> String {
    format!("{:.2}", f64::from(value) / 4.0)
}

async fn network_scope<W: Write>(
    cli: &Cli,
    args: &crate::args::NetworkScopeArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    if args.scope.is_none() && args.set_default {
        return Err(CliError::new(
            ExitStatus::Usage,
            "--set-default requires a scope value",
        ));
    }
    let parsed_scope = args.scope.as_deref().map(parse_flood_scope).transpose()?;
    let selected = select_profile(cli)?;
    let client = make_client(&selected)?;
    let (session, _) = CompanionSession::connect(&selected, client, "network scope").await?;
    let operation = async {
        let Some((scope, key_name)) = parsed_scope else {
            let configured = match session
                .client()
                .get_default_flood_scope()
                .await
                .map_err(CliError::from)?
            {
                DefaultFloodScope::Unconfigured => None,
                DefaultFloodScope::Configured(scope) => scope.name().map(str::to_owned),
            };
            return Ok(NetworkScopeReport {
                profile: selected.name,
                action: "query".to_owned(),
                scope: configured,
            });
        };

        if args.set_default {
            match (&scope, key_name.as_deref()) {
                (FloodScope::Default | FloodScope::Unscoped, _) => {
                    session
                        .client()
                        .clear_default_flood_scope()
                        .await
                        .map_err(CliError::from)?;
                    Ok(NetworkScopeReport {
                        profile: selected.name,
                        action: "clear_default".to_owned(),
                        scope: None,
                    })
                }
                (FloodScope::Key(key), Some(name)) => {
                    session
                        .client()
                        .set_default_flood_scope(name, *key)
                        .await
                        .map_err(CliError::from)?;
                    Ok(NetworkScopeReport {
                        profile: selected.name,
                        action: "set_default".to_owned(),
                        scope: Some(name.to_owned()),
                    })
                }
                (FloodScope::Key(_), None) => Err(CliError::from(CoreError::ProtocolInvariant(
                    "scope key name was lost",
                ))),
            }
        } else {
            session
                .client()
                .set_flood_scope(&scope)
                .await
                .map_err(CliError::from)?;
            Ok(NetworkScopeReport {
                profile: selected.name,
                action: "set".to_owned(),
                scope: scope_name_for_report(&scope, key_name),
            })
        }
    }
    .await;
    let report = session.finish(operation).await?;
    let human = match (report.action.as_str(), report.scope.as_deref()) {
        ("set_default", Some(scope)) => format!("Configured persistent scope '{scope}'"),
        ("set", Some(scope)) => format!("Set current flood scope to '{scope}'"),
        ("clear_default", _) => "Cleared persistent flood scope".to_owned(),
        ("query", Some(scope)) => format!("Configured default scope: {scope}"),
        ("query", None) => "No persistent flood scope is configured".to_owned(),
        _ => "Updated flood scope".to_owned(),
    };
    writer
        .result("network_scope", &report, &human)
        .map_err(CliError::from)
}

async fn network_trace<W: Write>(
    cli: &Cli,
    args: &crate::args::NetworkTraceArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    if args.target.contains(',') {
        return Err(CliError::new(
            ExitStatus::Usage,
            "network trace does not accept comma-separated paths",
        ));
    }
    let requested_hash_mode = match args.hash_bytes {
        None => None,
        Some(1) => Some(0),
        Some(2) => Some(1),
        Some(3) => Some(2),
        Some(4) => {
            return Err(CliError::new(
                ExitStatus::Usage,
                "4-byte path hashes are reserved by current companion firmware",
            )
            .with_hint("Use --hash-bytes 1, 2, or 3."));
        }
        Some(_) => {
            return Err(CliError::new(
                ExitStatus::Usage,
                "path hash bytes must be 1, 2, or 3",
            ));
        }
    };
    let selected = select_profile(cli)?;
    let client = make_client(&selected)?;
    let (session, _) = CompanionSession::connect(&selected, client, "network trace").await?;
    let operation = async {
        let contacts = session
            .client()
            .list_contacts(None)
            .await
            .map_err(CliError::from)?;
        let contact = resolve_contact(&contacts, &args.target)?.clone();
        let previous_hash_mode = if let Some(mode) = requested_hash_mode {
            let previous = session
                .client()
                .get_path_hash_mode()
                .await
                .map_err(CliError::from)?;
            if previous != mode {
                session
                    .client()
                    .set_path_hash_mode(mode)
                    .await
                    .map_err(CliError::from)?;
            }
            Some(previous)
        } else {
            None
        };
        let discovery = session
            .client()
            .discover_path(contact.public_key.as_bytes())
            .await
            .map_err(CliError::from);
        let restore = if let (Some(previous), Some(requested)) =
            (previous_hash_mode, requested_hash_mode)
            && previous != requested
        {
            session
                .client()
                .set_path_hash_mode(previous)
                .await
                .map_err(CliError::from)
        } else {
            Ok(())
        };
        let discovery = match (discovery, restore) {
            (Err(primary), Err(_)) => {
                tracing::warn!("secondary path-hash restore failure; details omitted");
                return Err(primary);
            }
            (Err(primary), Ok(())) => return Err(primary),
            (Ok(_), Err(restore)) => return Err(restore),
            (Ok(discovery), Ok(())) => discovery,
        };
        Ok(NetworkTraceReport {
            profile: selected.name,
            target: contact.adv_name.clone(),
            public_key: contact.public_key.to_hex(),
            discovery,
        })
    }
    .await;
    let report = session.finish(operation).await?;
    let human = format!("Discovered path for '{}'", report.target);
    writer
        .result("network_trace", &report, &human)
        .map_err(CliError::from)
}

async fn contacts<W: Write>(
    cli: &Cli,
    args: &ContactsArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let selected = select_profile(cli)?;
    let client = make_client(&selected)?;
    let (session, _) = CompanionSession::connect(&selected, client, "contacts").await?;
    let operation = async {
        let contacts = match &args.command {
            Some(ContactCommand::Import { .. }) => None,
            _ => Some(
                session
                    .client()
                    .list_contacts(None)
                    .await
                    .map_err(CliError::from)?,
            ),
        };
        match &args.command {
            Some(ContactCommand::Show { contact }) => {
                let contact = contact_view(resolve_contact(
                    contacts.as_deref().unwrap_or_default(),
                    contact,
                )?);
                let human = format!(
                    "{}\t{}\t{}\nRoute: {}\nPath: {}",
                    contact.name, contact.kind, contact.public_key, contact.route, contact.path
                );
                let report = ContactReport {
                    profile: selected.name.clone(),
                    contact,
                };
                writer
                    .result("contact", &report, &human)
                    .map_err(CliError::from)
            }
            Some(ContactCommand::Update(args)) => {
                contact_update(
                    &selected,
                    session.workflow(),
                    session.client(),
                    contacts.as_deref().unwrap_or_default(),
                    args,
                    writer,
                )
                .await
            }
            Some(ContactCommand::Forget { contact }) => {
                contact_forget(
                    cli,
                    &selected,
                    session.workflow(),
                    session.client(),
                    contacts.as_deref().unwrap_or_default(),
                    contact,
                    writer,
                )
                .await
            }
            Some(ContactCommand::Export { contact }) => {
                contact_export(
                    &selected,
                    session.client(),
                    contacts.as_deref().unwrap_or_default(),
                    contact,
                    writer,
                )
                .await
            }
            Some(ContactCommand::Import { uri }) => {
                contact_import(&selected, session.workflow(), session.client(), uri, writer).await
            }
            Some(ContactCommand::Path(command)) => {
                contact_path(
                    cli,
                    &selected,
                    session.workflow(),
                    session.client(),
                    contacts.as_deref().unwrap_or_default(),
                    command,
                    writer,
                )
                .await
            }
            None => contact_list(&selected, args, contacts.unwrap_or_default(), writer),
            Some(ContactCommand::Pending(_)) => Err(CliError::unsupported("pending contacts")),
        }
    }
    .await;
    session.finish(operation).await
}

fn contact_list<W: Write>(
    selected: &SelectedProfile,
    args: &ContactsArgs,
    contacts: Vec<Contact>,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let contacts = filter_contacts(contacts, args);
    let contacts: Vec<_> = contacts.iter().map(contact_view).collect();
    let mut human = if contacts.is_empty() {
        "No contacts matched.".to_owned()
    } else {
        contacts
            .iter()
            .map(|contact| {
                format!(
                    "{}\t{}\t{}",
                    contact.name,
                    contact.kind,
                    short_key(&contact.public_key)
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    if args.refresh {
        human = format!(
            "Contacts fetched fresh from the device (--refresh explicitly requested).\n{human}"
        );
    }
    let report = ContactsReport {
        profile: selected.name.clone(),
        refreshed: true,
        refresh_requested: args.refresh,
        contacts,
    };
    writer
        .result("contacts", &report, &human)
        .map_err(CliError::from)
}

async fn contact_update<W: Write>(
    selected: &crate::config::SelectedProfile,
    workflow: &WorkflowServices,
    client: &ManagedClient,
    contacts: &[Contact],
    args: &crate::args::ContactUpdateArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let contact = resolve_contact(contacts, &args.contact)?;
    let contact = client
        .get_contact(contact.public_key.as_bytes())
        .await
        .map_err(CliError::from)?;
    let (updated, changed) = apply_contact_update(contact, args);
    if !changed {
        return Err(CliError::new(
            ExitStatus::Usage,
            "no contact changes were supplied",
        ));
    }
    client
        .update_contact(&updated)
        .await
        .map_err(CliError::from)?;
    workflow
        .contact_updated(
            updated.public_key.to_hex(),
            Some(updated.adv_name.clone()),
            ContactChange::Updated,
        )
        .await?;
    let report = ContactUpdateReport {
        profile: selected.name.clone(),
        public_key: updated.public_key.to_hex(),
        name: updated.adv_name.clone(),
        favorite: updated.flags & 1 != 0,
    };
    let human = format!("Updated contact '{}'", report.name);
    writer
        .result("contact_update", &report, &human)
        .map_err(CliError::from)
}

async fn contact_forget<W: Write>(
    cli: &Cli,
    selected: &crate::config::SelectedProfile,
    workflow: &WorkflowServices,
    client: &ManagedClient,
    contacts: &[Contact],
    contact: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let contact = resolve_contact(contacts, contact)?;
    confirm(cli, &format!("remove contact {}", contact.adv_name))?;
    client
        .remove_contact(contact.public_key.as_bytes())
        .await
        .map_err(CliError::from)?;
    workflow
        .contact_updated(
            contact.public_key.to_hex(),
            Some(contact.adv_name.clone()),
            ContactChange::Removed,
        )
        .await?;
    let report = ContactForgetReport {
        profile: selected.name.clone(),
        contact: contact.adv_name.clone(),
        public_key: contact.public_key.to_hex(),
    };
    let human = format!("Removed contact '{}'", report.contact);
    writer
        .result("contact_remove", &report, &human)
        .map_err(CliError::from)
}

async fn contact_export<W: Write>(
    selected: &crate::config::SelectedProfile,
    client: &ManagedClient,
    contacts: &[Contact],
    contact: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let contact = resolve_contact(contacts, contact)?;
    let uri = client
        .export_contact(Some(contact.public_key.as_bytes()))
        .await
        .map_err(CliError::from)?;
    let report = ContactExportReport {
        profile: selected.name.clone(),
        contact: contact.adv_name.clone(),
        public_key: contact.public_key.to_hex(),
        uri: uri.uri,
    };
    let human = format!("Exported contact '{}'", report.contact);
    writer
        .result("contact_export", &report, &human)
        .map_err(CliError::from)
}

async fn contact_import<W: Write>(
    selected: &crate::config::SelectedProfile,
    workflow: &WorkflowServices,
    client: &ManagedClient,
    uri: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let card = parse_meshcore_uri(uri)?;
    client.import_contact(&card).await.map_err(CliError::from)?;
    let mut digest = Sha256::new();
    digest.update(&card);
    let contact_id = format!("import-{}", hex::encode(&digest.finalize()[..16]));
    workflow
        .contact_updated(contact_id, None, ContactChange::Added)
        .await?;
    let report = ContactImportReport {
        profile: selected.name.clone(),
        card_bytes: card.len(),
    };
    writer
        .result("contact_import", &report, "Imported contact")
        .map_err(CliError::from)
}

async fn contact_path<W: Write>(
    cli: &Cli,
    selected: &crate::config::SelectedProfile,
    workflow: &WorkflowServices,
    client: &ManagedClient,
    contacts: &[Contact],
    command: &crate::args::ContactPathCommand,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    match command {
        crate::args::ContactPathCommand::Show { contact } => {
            let contact = resolve_contact(contacts, contact)?;
            let path = client
                .get_advert_path(contact.public_key.as_bytes())
                .await
                .map_err(CliError::from)?;
            let report = ContactPathReport {
                profile: selected.name.clone(),
                contact: contact.adv_name.clone(),
                public_key: contact.public_key.to_hex(),
                route: contact_route_text(path.route),
                received_at: path.received_at,
                path: path.path.to_hex(),
            };
            let human = format!("Path for '{}' is {}", report.contact, report.path);
            writer
                .result("contact_path", &report, &human)
                .map_err(CliError::from)
        }
        crate::args::ContactPathCommand::Discover { contact } => {
            let contact = resolve_contact(contacts, contact)?;
            let discovery = client
                .discover_path(contact.public_key.as_bytes())
                .await
                .map_err(CliError::from)?;
            let report = ContactPathDiscoveryReport {
                profile: selected.name.clone(),
                contact: contact.adv_name.clone(),
                public_key: contact.public_key.to_hex(),
                discovery,
            };
            let human = format!("Discovered path for '{}'", report.contact);
            writer
                .result("contact_path_discovery", &report, &human)
                .map_err(CliError::from)
        }
        crate::args::ContactPathCommand::Reset { contact } => {
            let contact = resolve_contact(contacts, contact)?;
            confirm(cli, &format!("reset path for contact {}", contact.adv_name))?;
            client
                .reset_path(contact.public_key.as_bytes())
                .await
                .map_err(CliError::from)?;
            workflow
                .contact_updated(
                    contact.public_key.to_hex(),
                    Some(contact.adv_name.clone()),
                    ContactChange::Updated,
                )
                .await?;
            let report = ContactPathResetReport {
                profile: selected.name.clone(),
                contact: contact.adv_name.clone(),
                public_key: contact.public_key.to_hex(),
                reset: true,
            };
            let human = format!("Reset path for '{}'", report.contact);
            writer
                .result("contact_path_reset", &report, &human)
                .map_err(CliError::from)
        }
        crate::args::ContactPathCommand::Set { contact, path } => {
            contact_path_set(
                cli, selected, workflow, client, contacts, contact, path, writer,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn contact_path_set<W: Write>(
    cli: &Cli,
    selected: &crate::config::SelectedProfile,
    workflow: &WorkflowServices,
    client: &ManagedClient,
    contacts: &[Contact],
    contact: &str,
    path: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let listed = resolve_contact(contacts, contact)?;
    let mut updated = client
        .get_contact(listed.public_key.as_bytes())
        .await
        .map_err(CliError::from)?;
    let hash_mode = match client.get_path_hash_mode().await {
        Ok(value) if value <= 2 => value,
        Ok(_) => {
            return Err(CliError::new(
                ExitStatus::Protocol,
                "the device reported an unsupported contact path hash mode",
            ));
        }
        Err(error) => return Err(CliError::from(error)),
    };
    let path_bytes = parse_explicit_contact_path(path, hash_mode)?;
    confirm(
        cli,
        &format!("replace path for contact {}", updated.adv_name),
    )?;
    let width = usize::from(hash_mode) + 1;
    let hop_count = u8::try_from(path_bytes.len() / width)
        .map_err(|_| CliError::new(ExitStatus::Usage, "contact path contains too many hops"))?;
    updated.route = ContactRoute::Path {
        hash_mode,
        hop_count,
    };
    updated.out_path = meshquill_core::Path::try_from_bytes(&path_bytes).map_err(CliError::from)?;
    client
        .update_contact(&updated)
        .await
        .map_err(CliError::from)?;
    workflow
        .contact_updated(
            updated.public_key.to_hex(),
            Some(updated.adv_name.clone()),
            ContactChange::Updated,
        )
        .await?;
    let report = ContactPathSetReport {
        profile: selected.name.clone(),
        contact: updated.adv_name,
        public_key: updated.public_key.to_hex(),
        hash_bytes: hash_mode + 1,
        hop_count,
        path: hex::encode(path_bytes),
        updated: true,
    };
    let human = format!("Updated path for '{}'", terminal_safe(&report.contact));
    writer
        .result("contact_path_set", &report, &human)
        .map_err(CliError::from)
}

async fn channels<W: Write>(
    cli: &Cli,
    args: &ChannelCommand,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let set_secret = if let ChannelCommand::Set(command) = args {
        let secret_file = command.secret_file.as_deref().ok_or_else(|| {
            CliError::new(ExitStatus::Usage, "channel set requires --secret-file")
        })?;
        Some(read_channel_secret(secret_file)?)
    } else {
        None
    };

    let selected = select_profile(cli)?;
    let client = make_client(&selected)?;
    let (session, _) = CompanionSession::connect(&selected, client, "channels").await?;
    let operation = match args {
        ChannelCommand::List => channel_list(&selected, session.client(), writer).await,
        ChannelCommand::Show { channel } => {
            channel_show(&selected, session.client(), channel, writer).await
        }
        ChannelCommand::Set(command) => {
            let Some(secret) = set_secret else {
                return session
                    .finish(Err(CliError::from(CoreError::ProtocolInvariant(
                        "validated channel secret was unavailable",
                    ))))
                    .await;
            };
            channel_set(&selected, session.client(), command, secret, writer).await
        }
        ChannelCommand::Remove { channel } => {
            channel_remove(cli, &selected, session.client(), channel, writer).await
        }
    };
    session.finish(operation).await
}

async fn channel_list<W: Write>(
    selected: &SelectedProfile,
    client: &ManagedClient,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let mut channels = collect_channel_views(client)
        .await
        .map_err(CliError::from)?;
    channels.retain(|channel| !channel.name.is_empty());
    let human = if channels.is_empty() {
        "No configured channels.".to_owned()
    } else {
        channels
            .iter()
            .map(|channel| format!("{}\t{}", channel.idx, channel.name))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let report = ChannelsReport {
        profile: selected.name.clone(),
        channels,
    };
    writer
        .result("channels", &report, &human)
        .map_err(CliError::from)
}

async fn channel_show<W: Write>(
    selected: &SelectedProfile,
    client: &ManagedClient,
    query: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let channels = collect_channel_views(client)
        .await
        .map_err(CliError::from)?;
    let idx = resolve_channel_query(&channels, query)?;
    let Some(channel) = channels.iter().find(|value| value.idx == idx) else {
        return Err(CliError::from(CoreError::InvalidArgument {
            field: "channel",
            message: "channel index is out of range".to_owned(),
        }));
    };
    let report = ChannelReport {
        profile: selected.name.clone(),
        idx: channel.idx,
        name: channel.name.clone(),
        secret_hash: channel.secret_hash,
    };
    let display_name = if report.name.is_empty() {
        "(empty)"
    } else {
        &report.name
    };
    let human = format!("Channel {}: {display_name}", report.idx);
    writer
        .result("channel", &report, &human)
        .map_err(CliError::from)
}

async fn channel_set<W: Write>(
    selected: &SelectedProfile,
    client: &ManagedClient,
    command: &crate::args::ChannelSetArgs,
    secret: [u8; 16],
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let channels = collect_channel_views(client)
        .await
        .map_err(CliError::from)?;
    if !channels
        .iter()
        .any(|channel| channel.idx == command.channel)
    {
        return Err(CliError::new(
            ExitStatus::NotFound,
            format!("channel index '{}' is out of range", command.channel),
        ));
    }
    client
        .set_channel(command.channel, command.name.as_str(), secret)
        .await
        .map_err(CliError::from)?;
    let report = ChannelSetReport {
        profile: selected.name.clone(),
        idx: command.channel,
        name: command.name.clone(),
        secret_set: true,
    };
    let human = format!("Configured channel {}", command.channel);
    writer
        .result("channel_set", &report, &human)
        .map_err(CliError::from)
}

async fn channel_remove<W: Write>(
    cli: &Cli,
    selected: &SelectedProfile,
    client: &ManagedClient,
    query: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let channels = collect_channel_views(client)
        .await
        .map_err(CliError::from)?;
    let idx = resolve_channel_query(&channels, query)?;
    let previous_name = channels
        .iter()
        .find(|value| value.idx == idx)
        .map_or_else(String::new, |value| value.name.clone());
    if previous_name.is_empty() {
        return Err(CliError::new(
            ExitStatus::NotFound,
            format!("channel index '{idx}' is not configured"),
        ));
    }
    confirm(cli, &format!("remove channel {idx} ({previous_name})"))?;
    client.clear_channel(idx).await.map_err(CliError::from)?;
    let report = ChannelRemoveReport {
        profile: selected.name.clone(),
        idx,
        previous_name,
        removed: true,
    };
    let human = format!("Removed channel {idx}");
    writer
        .result("channel_remove", &report, &human)
        .map_err(CliError::from)
}

// Scope cleanup keeps every early device failure explicit and auditable.
#[allow(clippy::too_many_lines)]
async fn send<W: Write>(
    cli: &Cli,
    args: &SendArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    if args.channel && args.wait {
        return Err(CliError::new(
            ExitStatus::Usage,
            "--wait is available only for direct messages",
        ));
    }
    let selected = select_profile(cli)?;
    let workflow = WorkflowServices::from_selected(&selected)?;
    let prepared = workflow
        .prepare_send(args.destination.clone(), args.message.clone())
        .await?;
    let channel = if args.channel {
        Some(prepared.destination.parse::<u8>().map_err(|_| {
            CliError::new(
                ExitStatus::Usage,
                "channel destination must be a numeric channel index",
            )
            .with_hint("Named channel resolution is not available in the current core API.")
        })?)
    } else {
        None
    };
    let scope = match args.scope.as_deref() {
        Some(scope) => Some(parse_flood_scope(scope)?),
        None => None,
    };
    let client = make_client(&selected)?;
    let interrupt = InterruptWatcher::install().await;
    let connect_result = tokio::select! {
        result = client.connect() => result,
        () = interrupt.cancelled() => {
            client.cancel_pending_operations();
            let _ = client.shutdown().await;
            return Err(interrupt.error());
        }
    };
    let info = match connect_result {
        Ok(info) => info,
        Err(error) => return finish::<()>(&client, Err(error)).await,
    };
    activate_workflow(
        &client,
        &workflow,
        &info.name,
        "send connection hook failed",
    )
    .await?;
    if interrupt.token().is_cancelled() {
        cleanup_workflow(&client, &workflow, "interrupted").await;
        return Err(interrupt.error());
    }

    if let Some((scope, _)) = &scope
        && let Err(error) = client.set_flood_scope(scope).await
    {
        let cli_error = CliError::from(error);
        cleanup_send(&client, &workflow, true, "scope selection failed").await;
        return Err(cli_error);
    }
    if interrupt.token().is_cancelled() {
        cleanup_send(&client, &workflow, scope.is_some(), "interrupted").await;
        return Err(interrupt.error());
    }

    let mut outgoing = match workflow
        .begin_outgoing(&prepared.destination, channel, &prepared.text)
        .await
    {
        Ok(record) => record,
        Err(error) => {
            cleanup_send(&client, &workflow, scope.is_some(), "history setup failed").await;
            return Err(error);
        }
    };
    let workflow_message_id = outgoing.message_id().to_string();
    let mut post_acceptance_error: Option<(&'static str, CliError)> = None;
    if interrupt.token().is_cancelled() {
        if let Err(history_error) = workflow.failed(&mut outgoing).await {
            tracing::warn!(error = %history_error, "could not record interrupted send state");
        }
        cleanup_send(&client, &workflow, scope.is_some(), "interrupted").await;
        return Err(interrupt.error());
    }

    let report = {
        if let Some(channel) = channel {
            if interrupt.token().is_cancelled() {
                if let Err(history_error) = workflow.failed(&mut outgoing).await {
                    tracing::warn!(error = %history_error, "could not record interrupted send state");
                }
                cleanup_send(&client, &workflow, scope.is_some(), "interrupted").await;
                return Err(interrupt.error());
            }
            let result = client
                .send_channel_message(channel, 0, &prepared.text)
                .await;
            match result {
                Ok(()) => {
                    if let Err(error) = workflow
                        .sent(
                            &mut outgoing,
                            &prepared.destination,
                            &prepared.text,
                            &workflow_message_id,
                            None,
                        )
                        .await
                    {
                        post_acceptance_error = Some(("post-send workflow failed", error));
                    }
                    Ok(SendReport {
                        destination: prepared.destination.clone(),
                        channel: Some(channel),
                        queued: true,
                        ack_code: None,
                        acknowledged: false,
                        trip_time_ms: None,
                    })
                }
                Err(error) => {
                    if let Err(history_error) = workflow.failed(&mut outgoing).await {
                        tracing::warn!(error = %history_error, "could not record failed send state");
                    }
                    Err(CliError::from(error))
                }
            }
        } else {
            let contacts = match client.list_contacts(None).await {
                Ok(value) => value,
                Err(error) => {
                    if let Err(history_error) = workflow.failed(&mut outgoing).await {
                        tracing::warn!(error = %history_error, "could not record failed send state");
                    }
                    let cli_error = CliError::from(error);
                    cleanup_send(&client, &workflow, scope.is_some(), "contact list failed").await;
                    return Err(cli_error);
                }
            };
            let contact = match resolve_contact(&contacts, &prepared.destination) {
                Ok(contact) => contact,
                Err(error) => {
                    if let Err(history_error) = workflow.failed(&mut outgoing).await {
                        tracing::warn!(error = %history_error, "could not record failed send state");
                    }
                    cleanup_send(
                        &client,
                        &workflow,
                        scope.is_some(),
                        "contact resolution failed",
                    )
                    .await;
                    return Err(error);
                }
            };
            if interrupt.token().is_cancelled() {
                if let Err(history_error) = workflow.failed(&mut outgoing).await {
                    tracing::warn!(error = %history_error, "could not record interrupted send state");
                }
                cleanup_send(&client, &workflow, scope.is_some(), "interrupted").await;
                return Err(interrupt.error());
            }
            let prefix = &contact.public_key.as_bytes()[..6];
            let tracking = match client.send_direct_text(prefix, 0, &prepared.text).await {
                Ok(value) => value,
                Err(error) => {
                    if let Err(history_error) = workflow.failed(&mut outgoing).await {
                        tracing::warn!(error = %history_error, "could not record failed send state");
                    }
                    let cli_error = CliError::from(error);
                    cleanup_send(&client, &workflow, scope.is_some(), "send failed").await;
                    return Err(cli_error);
                }
            };
            if let Err(error) = workflow
                .sent(
                    &mut outgoing,
                    &contact.adv_name,
                    &prepared.text,
                    &workflow_message_id,
                    Some(tracking.ack_code),
                )
                .await
            {
                post_acceptance_error = Some(("post-send workflow failed", error));
            }
            let ack = if args.wait {
                let firmware_timeout = Duration::from_millis(u64::from(tracking.timeout_ms));
                let timeout = firmware_timeout.min(cli.timeout);
                let ack_result = tokio::select! {
                    result = client.wait_for_ack(tracking.ack_code, Some(timeout)) => result,
                    () = interrupt.cancelled() => {
                        client.cancel_pending_operations();
                        cleanup_send(
                            &client,
                            &workflow,
                            scope.is_some(),
                            "interrupted",
                        )
                        .await;
                        return Err(interrupt.error());
                    }
                };
                match ack_result {
                    Ok(value) => {
                        if let Err(error) = workflow
                            .acknowledged(
                                &mut outgoing,
                                &workflow_message_id,
                                Some(&contact.adv_name),
                                value.trip_time_ms,
                                tracking.ack_code,
                            )
                            .await
                        {
                            if post_acceptance_error.is_none() {
                                post_acceptance_error =
                                    Some(("acknowledgement workflow failed", error));
                            } else {
                                tracing::warn!(
                                    error = %error,
                                    "secondary acknowledgement workflow failure; message was already accepted"
                                );
                            }
                        }
                        Some(value)
                    }
                    Err(error) => {
                        let cli_error = CliError::from(error);
                        let workflow_result = if cli_error.status() == ExitStatus::Timeout {
                            workflow
                                .timed_out(
                                    &mut outgoing,
                                    "send acknowledgement",
                                    &workflow_message_id,
                                )
                                .await
                        } else {
                            workflow.failed(&mut outgoing).await
                        };
                        if let Err(history_error) = workflow_result {
                            tracing::warn!(error = %history_error, "could not record terminal send state");
                        }
                        cleanup_send(&client, &workflow, scope.is_some(), "send failed").await;
                        return Err(cli_error);
                    }
                }
            } else {
                None
            };
            Ok(SendReport {
                destination: contact.adv_name.clone(),
                channel: None,
                queued: true,
                ack_code: Some(bytes_hex(&tracking.ack_code)),
                acknowledged: ack.is_some(),
                trip_time_ms: ack.and_then(|value| value.trip_time_ms),
            })
        }
    };

    let report = match report {
        Ok(report) => report,
        Err(error) => {
            cleanup_send(&client, &workflow, scope.is_some(), "send failed").await;
            return Err(error);
        }
    };
    if interrupt.token().is_cancelled() && post_acceptance_error.is_none() {
        post_acceptance_error = Some(("post-send cleanup was interrupted", interrupt.error()));
    }
    if scope.is_some()
        && let Err(error) = client.set_flood_scope(&FloodScope::Default).await
    {
        let error = CliError::from(error);
        if post_acceptance_error.is_none() {
            post_acceptance_error = Some(("flood-scope cleanup failed", error));
        } else {
            tracing::warn!(error = %error, "secondary flood-scope cleanup failure");
        }
    }

    if let Err(error) = finish_workflow(&client, &workflow, "command completed").await {
        if post_acceptance_error.is_none() {
            post_acceptance_error = Some(("disconnect cleanup failed", error));
        } else {
            tracing::warn!(error = %error, "secondary send cleanup failure");
        }
    }
    let human = if report.acknowledged {
        format!(
            "Sent to {} and received acknowledgement",
            report.destination
        )
    } else {
        format!("Queued message to {}", report.destination)
    };
    writer
        .result("send", &report, &human)
        .map_err(CliError::from)?;
    if let Some((stage, error)) = post_acceptance_error {
        return Err(accepted_delivery_error(stage, &error));
    }
    Ok(())
}

fn accepted_delivery_error(stage: &str, error: &CliError) -> CliError {
    CliError::new(
        error.status(),
        format!(
            "the device already accepted the message, but {stage}; do not retry automatically"
        ),
    )
    .with_hint("The emitted send result is authoritative: queued=true means radio transmission was accepted.")
}

async fn cleanup_send(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    reset_scope: bool,
    reason: &str,
) {
    if reset_scope && client.set_flood_scope(&FloodScope::Default).await.is_err() {
        tracing::warn!("secondary flood-scope cleanup failure; details omitted");
    }
    cleanup_workflow(client, workflow, reason).await;
}

async fn cleanup_workflow(client: &ManagedClient, workflow: &WorkflowServices, reason: &str) {
    if workflow.disconnected(Some(reason)).await.is_err() {
        tracing::warn!("secondary disconnect hook failure; details omitted");
    }
    if client.shutdown().await.is_err() {
        tracing::warn!("secondary client shutdown failure; details omitted");
    }
}

async fn activate_workflow(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    peer: &str,
    failure_reason: &str,
) -> Result<(), CliError> {
    if let Err(primary) = workflow.connected(peer).await {
        if workflow.disconnected(Some(failure_reason)).await.is_err() {
            tracing::warn!("secondary disconnect hook failure; details omitted");
        }
        if client.shutdown().await.is_err() {
            tracing::warn!("secondary client shutdown failure; details omitted");
        }
        return Err(primary);
    }
    Ok(())
}

async fn finish_workflow(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    reason: &str,
) -> Result<(), CliError> {
    let disconnect_result = workflow.disconnected(Some(reason)).await;
    let shutdown_result = finish(client, Ok(())).await;
    match (disconnect_result, shutdown_result) {
        (Err(primary), Err(_)) => {
            tracing::warn!("secondary client shutdown failure; details omitted");
            Err(primary)
        }
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), result) => result,
    }
}

async fn inbox<W: Write>(
    cli: &Cli,
    args: &InboxArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let selected = select_profile(cli)?;
    let workflow = WorkflowServices::from_selected(&selected)?;
    let client = make_client(&selected)?;
    let info = match client.connect().await {
        Ok(info) => info,
        Err(error) => return finish::<()>(&client, Err(error)).await,
    };
    activate_workflow(
        &client,
        &workflow,
        &info.name,
        "inbox connection hook failed",
    )
    .await?;
    let mut messages = Vec::new();
    let mut drained = false;
    while args.limit.is_none_or(|limit| messages.len() < limit) {
        match client.sync_next_message().await {
            Ok(Some(message)) => match workflow.incoming(&message, IncomingOrigin::Queue).await {
                Ok(Some(_)) => messages.push(message),
                Ok(None) => {}
                Err(error) => {
                    cleanup_workflow(&client, &workflow, "incoming workflow failed").await;
                    return Err(error);
                }
            },
            Ok(None) => {
                drained = true;
                break;
            }
            Err(error) => {
                let cli_error = CliError::from(error);
                let _ = workflow.error("inbox", cli_error.message()).await;
                cleanup_workflow(&client, &workflow, "inbox failed").await;
                return Err(cli_error);
            }
        }
    }
    finish_workflow(&client, &workflow, "command completed").await?;
    let human = if messages.is_empty() {
        "Inbox is empty.".to_owned()
    } else {
        messages
            .iter()
            .map(|message| format!("{}\t{}", message_source(message), message.text))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let report = InboxReport {
        profile: selected.name,
        messages,
        drained,
    };
    writer
        .result("inbox", &report, &human)
        .map_err(CliError::from)
}

async fn history<W: Write>(
    cli: &Cli,
    command: &HistoryCommand,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let selected = select_profile(cli)?;
    let store = history_store_for_selected(&selected, true)?.ok_or_else(|| {
        CliError::new(
            ExitStatus::Configuration,
            "could not resolve the selected profile history store",
        )
    })?;
    let path = store.path().display().to_string();

    match command {
        HistoryCommand::List { limit } => {
            let mut entries = load_history(&store).await?;
            if let Some(limit) = limit {
                let first = entries.len().saturating_sub(*limit);
                entries = entries.split_off(first);
            }
            let human = if entries.is_empty() {
                format!(
                    "No retained history for '{}'. Persistence is {}.",
                    selected.name,
                    if selected.config.history.enabled {
                        "enabled"
                    } else {
                        "disabled"
                    }
                )
            } else {
                entries
                    .iter()
                    .map(|entry| {
                        format!(
                            "{}\t{:?}\t{:?}\t{}\t{}",
                            entry.recorded_at_unix_ms,
                            entry.direction,
                            entry.status,
                            terminal_safe(&entry.peer),
                            terminal_safe(&entry.text)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            let report = HistoryReport {
                profile: selected.name,
                enabled: selected.config.history.enabled,
                storage: "plaintext_opt_in",
                path,
                entries,
            };
            writer
                .result("history", &report, &human)
                .map_err(CliError::from)
        }
        HistoryCommand::Clear => {
            confirm(cli, "delete the selected profile's local message history")?;
            clear_history(&store).await?;
            let report = HistoryClearReport {
                profile: selected.name,
                path,
                cleared: true,
            };
            writer
                .result("history_cleared", &report, "Cleared local message history.")
                .map_err(CliError::from)
        }
    }
}

fn config_show<W: Write>(cli: &Cli, writer: &mut OutputWriter<W>) -> Result<(), CliError> {
    let store = config_store(cli)?;
    let path = store.path().display().to_string();
    let (config, needs_migration) = match load_optional(&store)? {
        LoadOutcome::Missing => {
            return Err(CliError::new(
                ExitStatus::Configuration,
                format!("configuration is missing at {path}"),
            )
            .with_hint("Run `meshquill init`."));
        }
        LoadOutcome::Loaded(config) => (config, false),
        LoadOutcome::NeedsMigration(config) => (config, true),
    };
    let human = config.to_effective_toml().map_err(CliError::from)?;
    let report = ConfigShowReport {
        path,
        needs_migration,
        effective: config.effective_config(),
    };
    writer
        .result("configuration", &report, human.trim_end())
        .map_err(CliError::from)
}

fn config_migrate<W: Write>(cli: &Cli, writer: &mut OutputWriter<W>) -> Result<(), CliError> {
    let store = config_store(cli)?;
    let locked = store.lock_exclusive().map_err(CliError::from)?;
    let (changed, backup_path) = match load_unmodified_locked(&locked)? {
        LoadOutcome::Missing => {
            return Err(CliError::new(
                ExitStatus::Configuration,
                format!("configuration is missing at {}", locked.path().display()),
            )
            .with_hint("Run `meshquill init`."));
        }
        LoadOutcome::Loaded(_) => (false, None),
        LoadOutcome::NeedsMigration(config) => {
            let backup = locked.backup().map_err(CliError::from)?;
            locked.save(&config).map_err(CliError::from)?;
            (true, Some(backup.display().to_string()))
        }
    };
    let report = ConfigChangeReport {
        path: locked.path().display().to_string(),
        changed,
        backup_path,
    };
    let human = if changed {
        format!(
            "Migrated configuration; backup: {}",
            report.backup_path.as_deref().unwrap_or("unavailable")
        )
    } else {
        "Configuration is already current.".to_owned()
    };
    writer
        .result("configuration_migration", &report, &human)
        .map_err(CliError::from)
}

fn config_repair<W: Write>(cli: &Cli, writer: &mut OutputWriter<W>) -> Result<(), CliError> {
    confirm(cli, "replace the selected configuration with safe defaults")?;
    let store = config_store(cli)?;
    let outcome = store.repair().map_err(CliError::from)?;
    let report = ConfigChangeReport {
        path: store.path().display().to_string(),
        changed: true,
        backup_path: outcome
            .backup_path
            .as_ref()
            .map(|path| path.display().to_string()),
    };
    let human = report.backup_path.as_ref().map_or_else(
        || "Created a clean default configuration.".to_owned(),
        |backup| format!("Repaired configuration; backup: {backup}"),
    );
    writer
        .result("configuration_repair", &report, &human)
        .map_err(CliError::from)
}

fn config_import_legacy<W: Write>(
    cli: &Cli,
    requested_path: Option<&Path>,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    const IMPORTED_PROFILE: &str = "legacy";

    let source = legacy_source_path(requested_path)?;
    let address = read_legacy_address(&source)?;

    let store = config_store(cli)?;
    let locked = store.lock_exclusive().map_err(CliError::from)?;
    let mut config = match load_unmodified_locked(&locked)? {
        LoadOutcome::Missing => Config::default(),
        LoadOutcome::Loaded(config) => config,
        LoadOutcome::NeedsMigration(_) => {
            return Err(CliError::new(
                ExitStatus::Configuration,
                "migrate the Meshquill configuration before importing a legacy device",
            )
            .with_hint("Run `meshquill config migrate` first."));
        }
    };
    if config.device_profiles.contains_key(IMPORTED_PROFILE) {
        return Err(CliError::new(
            ExitStatus::Denied,
            "profile 'legacy' already exists; legacy import never overwrites it",
        ));
    }
    config.device_profiles.insert(
        IMPORTED_PROFILE.to_owned(),
        DeviceProfile {
            transport: TransportConfig::Ble {
                id: address,
                name: None,
            },
            transport_overrides: None,
            secret: None,
        },
    );
    let make_default = config.default_profile.is_none();
    if make_default {
        config.default_profile = Some(IMPORTED_PROFILE.to_owned());
    }
    locked.save(&config).map_err(CliError::from)?;

    let report = LegacyImportReport {
        source: source.display().to_string(),
        config_path: locked.path().display().to_string(),
        profile: IMPORTED_PROFILE.to_owned(),
        default: make_default,
        transport: "ble",
    };
    let human = format!(
        "Imported legacy BLE selection as profile '{}'.",
        report.profile
    );
    writer
        .result("legacy_configuration_import", &report, &human)
        .map_err(CliError::from)
}

fn legacy_source_path(requested_path: Option<&Path>) -> Result<std::path::PathBuf, CliError> {
    if let Some(path) = requested_path {
        return Ok(path.to_path_buf());
    }
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .ok_or_else(|| {
            CliError::new(
                ExitStatus::Configuration,
                "the legacy meshcore-cli home directory could not be resolved",
            )
            .with_hint("Pass the path to its `default_address` file explicitly.")
        })?;
    Ok(std::path::PathBuf::from(home)
        .join(".config")
        .join("meshcore")
        .join("default_address"))
}

fn read_legacy_address(source: &Path) -> Result<String, CliError> {
    const MAX_LEGACY_ADDRESS_BYTES: u64 = 512;

    let file = fs::File::open(source).map_err(|_| {
        CliError::new(
            ExitStatus::Configuration,
            format!(
                "legacy meshcore-cli selection was not found at {}",
                source.display()
            ),
        )
        .with_hint("Pass the path to the old `default_address` file.")
    })?;
    let metadata = file.metadata().map_err(|_| {
        CliError::new(
            ExitStatus::Configuration,
            "legacy meshcore-cli selection could not be inspected",
        )
    })?;
    if !metadata.is_file() || metadata.len() > MAX_LEGACY_ADDRESS_BYTES {
        return Err(CliError::new(
            ExitStatus::Configuration,
            "legacy meshcore-cli selection must be a small regular file",
        ));
    }
    let mut raw = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(512));
    file.take(MAX_LEGACY_ADDRESS_BYTES + 1)
        .read_to_end(&mut raw)
        .map_err(|_| {
            CliError::new(
                ExitStatus::Configuration,
                "legacy meshcore-cli selection could not be read",
            )
        })?;
    if raw.len() > usize::try_from(MAX_LEGACY_ADDRESS_BYTES).unwrap_or(512) {
        return Err(CliError::new(
            ExitStatus::Configuration,
            "legacy meshcore-cli selection must be a small regular file",
        ));
    }
    let address = std::str::from_utf8(&raw)
        .map_err(|_| {
            CliError::new(
                ExitStatus::Configuration,
                "legacy meshcore-cli selection is not valid UTF-8",
            )
        })?
        .trim();
    if address.is_empty() || address.len() > 128 || address.chars().any(char::is_control) {
        return Err(CliError::new(
            ExitStatus::Configuration,
            "legacy meshcore-cli BLE identifier is empty or invalid",
        ));
    }
    Ok(address.to_owned())
}

pub(crate) fn make_client(selected: &SelectedProfile) -> Result<ManagedClient, CliError> {
    tracing::debug!(profile = %selected.name, "creating bounded CLI client");
    let transport = CliTransport::from_profile(&selected.profile, selected.connect_timeout())?;
    let client =
        Client::with_timeout(transport, selected.request_timeout()).map_err(CliError::from)?;
    Ok(ManagedClient::spawn(client))
}

async fn finish<T>(client: &ManagedClient, operation: Result<T, CoreError>) -> Result<T, CliError> {
    let shutdown = client.shutdown().await;
    match operation {
        Ok(value) => {
            shutdown.map_err(CliError::from)?;
            Ok(value)
        }
        Err(error) => {
            let _ = shutdown;
            Err(CliError::from(error))
        }
    }
}

async fn collect_channel_views(client: &ManagedClient) -> Result<Vec<ChannelInfoView>, CoreError> {
    let device_info = client.query_device_info().await?;
    let mut channels = Vec::new();
    for idx in 0..device_info.max_channels.unwrap_or_default() {
        let channel = client.get_channel(idx).await?;
        channels.push(ChannelInfoView {
            idx: channel.idx,
            name: channel.name,
            secret_hash: channel.secret_hash,
        });
    }
    Ok(channels)
}

fn resolve_channel_query(channels: &[ChannelInfoView], query: &str) -> Result<u8, CliError> {
    if let Ok(idx) = query.parse::<u8>() {
        if channels.iter().any(|channel| channel.idx == idx) {
            return Ok(idx);
        }
        return Err(CliError::new(
            ExitStatus::NotFound,
            format!("channel index '{idx}' is out of range"),
        ));
    }
    let matches: Vec<_> = channels
        .iter()
        .filter(|channel| channel.name == query)
        .collect();
    match matches.as_slice() {
        [channel] => Ok(channel.idx),
        [] => Err(CliError::new(
            ExitStatus::NotFound,
            format!("channel name '{query}' was not found"),
        )),
        [_, ..] => Err(CliError::new(
            ExitStatus::Usage,
            format!("channel name '{query}' is ambiguous"),
        )
        .with_hint("Use an exact numeric channel index instead.")),
    }
}

fn apply_contact_update(
    mut contact: Contact,
    args: &crate::args::ContactUpdateArgs,
) -> (Contact, bool) {
    let mut changed = false;
    if let Some(name) = &args.name
        && contact.adv_name != *name
    {
        contact.adv_name.clone_from(name);
        changed = true;
    }
    if let Some(favorite) = args.favorite {
        let flags = (contact.flags & 0b1111_1110) | u8::from(favorite);
        if contact.flags != flags {
            contact.flags = flags;
            changed = true;
        }
    }
    (contact, changed)
}

fn parse_meshcore_uri(uri: &str) -> Result<Vec<u8>, CliError> {
    const MESHCORE_PREFIX: &str = "meshcore://";
    if !uri.starts_with(MESHCORE_PREFIX) {
        return Err(CliError::new(
            ExitStatus::Usage,
            "contact URI must begin with 'meshcore://'",
        ));
    }
    if !uri[..MESHCORE_PREFIX.len()]
        .chars()
        .all(|value| !value.is_ascii_uppercase())
    {
        return Err(CliError::new(
            ExitStatus::Usage,
            "contact URI prefix must be lowercase",
        ));
    }
    let hex_bytes = &uri[MESHCORE_PREFIX.len()..];
    if hex_bytes.is_empty() {
        return Err(CliError::new(
            ExitStatus::Usage,
            "contact URI must include at least 98 bytes of hex card data",
        ));
    }
    if !hex_bytes.len().is_multiple_of(2)
        || !hex_bytes
            .chars()
            .all(|value| value.is_ascii_hexdigit() && !value.is_ascii_uppercase())
    {
        return Err(CliError::new(
            ExitStatus::Usage,
            "contact URI must contain lowercase hexadecimal payload bytes",
        ));
    }
    let max_card_bytes = MAX_INNER_PAYLOAD - 1;
    if hex_bytes.len() > max_card_bytes.saturating_mul(2) {
        return Err(CliError::new(
            ExitStatus::Usage,
            format!("contact URI card data must not exceed {max_card_bytes} bytes"),
        ));
    }
    let card = hex::decode(hex_bytes).map_err(|error| {
        CliError::new(
            ExitStatus::Usage,
            format!("contact URI is not valid hex ({error})"),
        )
    })?;
    if card.len() < 98 {
        return Err(CliError::new(
            ExitStatus::Usage,
            format!(
                "contact URI must include at least 98 bytes (was {})",
                card.len()
            ),
        ));
    }
    Ok(card)
}

fn parse_explicit_contact_path(raw: &str, hash_mode: u8) -> Result<Vec<u8>, CliError> {
    const MAX_CONTACT_PATH_BYTES: usize = 64;
    if raw.len() > MAX_CONTACT_PATH_BYTES.saturating_mul(5) {
        return Err(CliError::new(
            ExitStatus::Usage,
            "explicit contact path is too long",
        ));
    }
    let mut bytes = Vec::new();
    for token in raw.split(',') {
        let token = token.trim();
        let token = token
            .strip_prefix("0x")
            .or_else(|| token.strip_prefix("0X"))
            .unwrap_or(token);
        if token.is_empty() || token.len() > 2 {
            return Err(CliError::new(
                ExitStatus::Usage,
                "contact path must contain comma-separated hexadecimal bytes",
            ));
        }
        let byte = u8::from_str_radix(token, 16).map_err(|_| {
            CliError::new(
                ExitStatus::Usage,
                "contact path must contain comma-separated hexadecimal bytes",
            )
        })?;
        bytes.push(byte);
        if bytes.len() > MAX_CONTACT_PATH_BYTES {
            return Err(CliError::new(
                ExitStatus::Usage,
                "contact path exceeds the firmware's 64-byte limit",
            ));
        }
    }
    let hash_width = usize::from(hash_mode) + 1;
    if bytes.is_empty() || !bytes.len().is_multiple_of(hash_width) {
        return Err(CliError::new(
            ExitStatus::Usage,
            format!(
                "contact path byte count must be a non-zero multiple of the device's {hash_width}-byte hash width"
            ),
        ));
    }
    if bytes.len() / hash_width > 63 {
        return Err(CliError::new(
            ExitStatus::Usage,
            "contact path exceeds the firmware's 63-hop limit",
        ));
    }
    Ok(bytes)
}

fn bounded_unix_time() -> Result<u32, CliError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CliError::new(ExitStatus::Protocol, "system time is invalid"))?;
    u32::try_from(elapsed.as_secs()).map_err(|_| {
        CliError::new(
            ExitStatus::Usage,
            "system timestamp does not fit into the device 32-bit clock field",
        )
    })
}

fn read_channel_secret(path: &Path) -> Result<[u8; 16], CliError> {
    const CHANNEL_SECRET_BYTES: u64 = 16;
    let file = fs::File::open(path).map_err(|error| {
        CliError::new(
            ExitStatus::Usage,
            format!(
                "unable to read channel secret file '{}': {error}",
                path.display()
            ),
        )
    })?;
    let metadata = file.metadata().map_err(|error| {
        CliError::new(
            ExitStatus::Usage,
            format!(
                "unable to inspect channel secret file '{}': {error}",
                path.display()
            ),
        )
    })?;
    if !metadata.is_file() || metadata.len() != CHANNEL_SECRET_BYTES {
        return Err(CliError::new(
            ExitStatus::Usage,
            "channel secret file must be exactly 16 bytes",
        ));
    }
    let mut bytes = Zeroizing::new(Vec::with_capacity(17));
    file.take(17).read_to_end(&mut bytes).map_err(|error| {
        CliError::new(
            ExitStatus::Usage,
            format!(
                "unable to read channel secret file '{}': {error}",
                path.display()
            ),
        )
    })?;
    if bytes.len() != 16 {
        return Err(CliError::new(
            ExitStatus::Usage,
            "channel secret file must be exactly 16 bytes",
        ));
    }
    let mut secret = [0_u8; 16];
    secret.copy_from_slice(&bytes);
    Ok(secret)
}

fn contact_route_text(route: ContactRoute) -> String {
    match route {
        ContactRoute::Flood => "flood".to_owned(),
        ContactRoute::Path {
            hash_mode,
            hop_count,
        } => format!("path(hash_mode={hash_mode},hops={hop_count})"),
    }
}

fn filter_contacts(contacts: Vec<Contact>, args: &ContactsArgs) -> Vec<Contact> {
    let search = args.search.as_ref().map(|value| value.to_ascii_lowercase());
    contacts
        .into_iter()
        .filter(|contact| {
            args.kind
                .is_none_or(|kind| contact_kind_matches(contact.contact_type, kind))
        })
        .filter(|contact| {
            search.as_ref().is_none_or(|needle| {
                contact.adv_name.to_ascii_lowercase().contains(needle)
                    || contact.public_key.to_hex().starts_with(needle)
            })
        })
        .collect()
}

pub(crate) fn resolve_contact<'a>(
    contacts: &'a [Contact],
    query: &str,
) -> Result<&'a Contact, CliError> {
    let exact: Vec<_> = contacts
        .iter()
        .filter(|contact| contact.adv_name == query)
        .collect();
    match exact.as_slice() {
        [contact] => return Ok(contact),
        [_, ..] => {
            return Err(CliError::new(
                ExitStatus::Usage,
                format!("contact name '{query}' is not unique"),
            )
            .with_hint("Use a unique public-key prefix."));
        }
        [] => {}
    }
    if query.is_empty() || !query.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        let error = CliError::new(
            ExitStatus::NotFound,
            format!("contact '{query}' was not found"),
        );
        return Err(match contact_name_suggestion(contacts, query) {
            Some(suggestion) => error.with_hint(format!(
                "Did you mean '{suggestion}'? Contact names are case-sensitive; no contact was selected."
            )),
            None => error
                .with_hint("Use an exact case-sensitive name or a unique hexadecimal key prefix."),
        });
    }
    let prefix = query.to_ascii_lowercase();
    let matches: Vec<_> = contacts
        .iter()
        .filter(|contact| contact.public_key.to_hex().starts_with(&prefix))
        .collect();
    match matches.as_slice() {
        [contact] => Ok(contact),
        [] => {
            let error = CliError::new(
                ExitStatus::NotFound,
                format!("contact key prefix '{query}' was not found"),
            );
            Err(match contact_name_suggestion(contacts, query) {
                Some(suggestion) => error.with_hint(format!(
                    "Did you mean the contact '{suggestion}'? No contact was selected."
                )),
                None => error.with_hint(
                    "Use an exact case-sensitive name or a unique hexadecimal key prefix.",
                ),
            })
        }
        [_, ..] => Err(CliError::new(
            ExitStatus::Usage,
            format!("contact key prefix '{query}' is ambiguous"),
        )
        .with_hint("Provide more hexadecimal key characters.")),
    }
}

fn contact_view(contact: &Contact) -> ContactView {
    ContactView {
        name: contact.adv_name.clone(),
        public_key: contact.public_key.to_hex(),
        kind: contact_type_name(contact.contact_type).to_owned(),
        flags: contact.flags,
        route: match contact.route {
            ContactRoute::Flood => "flood".to_owned(),
            ContactRoute::Path {
                hash_mode,
                hop_count,
            } => format!("path(hash_mode={hash_mode},hops={hop_count})"),
        },
        path: contact.out_path.to_hex(),
        last_advert: contact.last_advert,
        lastmod: contact.lastmod,
    }
}

fn contact_kind_matches(contact_type: ContactType, kind: ContactKind) -> bool {
    matches!(
        (contact_type, kind),
        (ContactType::Chat, ContactKind::Client)
            | (ContactType::Repeater, ContactKind::Repeater)
            | (ContactType::Room, ContactKind::Room)
            | (ContactType::Sensor, ContactKind::Sensor)
    )
}

fn contact_type_name(contact_type: ContactType) -> &'static str {
    match contact_type {
        ContactType::Chat => "client",
        ContactType::Repeater => "repeater",
        ContactType::Room => "room",
        ContactType::Sensor => "sensor",
        ContactType::Unknown(_) => "unknown",
    }
}

fn describe_transport(transport: &TransportConfig) -> String {
    match transport {
        TransportConfig::Ble { id, .. } => format!("BLE {id}"),
        TransportConfig::Serial { port, baud } => format!("serial {port} at {baud} baud"),
        TransportConfig::Tcp { host, port } => format!("TCP {host}:{port}"),
        TransportConfig::Mock { scenario } => format!("explicit mock ({scenario})"),
    }
}

fn short_key(key: &str) -> &str {
    key.get(..12).unwrap_or(key)
}

fn parse_flood_scope(value: &str) -> Result<(FloodScope, Option<String>), CliError> {
    if value.eq_ignore_ascii_case("default")
        || value == "0"
        || value.eq_ignore_ascii_case("none")
        || value.is_empty()
    {
        return Ok((FloodScope::Default, None));
    }
    if value.eq_ignore_ascii_case("unscoped") || value == "*" {
        return Ok((FloodScope::Unscoped, None));
    }
    let normalized = if value.starts_with('#') {
        value.to_owned()
    } else {
        format!("#{value}")
    };
    if normalized.len() == 1 {
        return Err(CliError::new(
            ExitStatus::Usage,
            "flood scope name cannot be empty",
        ));
    }
    if normalized
        .as_bytes()
        .iter()
        .any(|value| *value == 0 || value.is_ascii_control())
    {
        return Err(CliError::new(
            ExitStatus::Usage,
            "flood scope name cannot contain NUL or control characters",
        ));
    }
    let key_length = normalized.len();
    if !(1..=30).contains(&key_length) {
        return Err(CliError::new(
            ExitStatus::Usage,
            format!("flood scope name must be 1..=30 bytes, got {key_length}"),
        ));
    }
    let hash = Sha256::digest(normalized.as_bytes());
    let mut key = [0_u8; 16];
    key.copy_from_slice(&hash[..16]);
    Ok((FloodScope::Key(key), Some(normalized)))
}

fn scope_name_for_report(scope: &FloodScope, key_name: Option<String>) -> Option<String> {
    match scope {
        FloodScope::Default => Some("default".to_owned()),
        FloodScope::Unscoped => Some("unscoped".to_owned()),
        FloodScope::Key(_) => key_name,
    }
}

fn bytes_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        let _result = write!(output, "{byte:02x}");
    }
    output
}

fn message_source(message: &Message) -> String {
    match &message.source {
        meshquill_core::domain::MessageSource::Direct { pubkey_prefix } => {
            format!("direct:{pubkey_prefix}")
        }
        meshquill_core::domain::MessageSource::Channel { channel_idx } => {
            format!("channel:{channel_idx}")
        }
    }
}

fn terminal_safe(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_control() {
            escaped.extend(character.escape_default());
        } else {
            escaped.push(character);
        }
    }
    escaped
}

pub(crate) fn confirm(cli: &Cli, operation: &str) -> Result<(), CliError> {
    if cli.yes {
        return Ok(());
    }
    if cli.non_interactive || !io::stdin().is_terminal() {
        return Err(CliError::new(
            ExitStatus::Denied,
            format!("confirmation is required to {operation}"),
        )
        .with_hint("Review the operation and rerun it with --yes."));
    }
    let mut stderr = io::stderr().lock();
    write!(stderr, "Confirm: {operation}? [y/N] ")
        .and_then(|()| stderr.flush())
        .map_err(|_| CliError::new(ExitStatus::Protocol, "could not write confirmation prompt"))?;
    drop(stderr);
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let answer = read_bounded_line(&mut input, "confirmation input")?.unwrap_or_default();
    if matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        Ok(())
    } else {
        Err(CliError::new(
            ExitStatus::Denied,
            "operation was not confirmed",
        ))
    }
}

#[derive(Debug, Serialize)]
struct ProviderDiagnostic {
    provider: &'static str,
    status: &'static str,
    detail: String,
}

#[derive(Debug, Serialize)]
struct DiscoveryReport {
    devices: Vec<DiscoveredDevice>,
    mock_profiles: Vec<String>,
    diagnostics: Vec<ProviderDiagnostic>,
}

#[derive(Default)]
struct DiscoveryCollection {
    devices: Vec<DiscoveredDevice>,
    mock_profiles: Vec<String>,
    diagnostics: Vec<ProviderDiagnostic>,
    provider_successes: usize,
}

impl DiscoveryCollection {
    fn add_physical(
        &mut self,
        provider: &'static str,
        noun: &'static str,
        result: Result<Vec<DiscoveredDevice>, meshquill_transport::DiscoveryError>,
    ) {
        match result {
            Ok(mut found) => {
                self.provider_successes = self.provider_successes.saturating_add(1);
                self.diagnostics.push(ProviderDiagnostic {
                    provider,
                    status: "ok",
                    detail: format!("{} {noun}(s) observed", found.len()),
                });
                self.devices.append(&mut found);
            }
            Err(error) => self.diagnostics.push(ProviderDiagnostic {
                provider,
                status: "error",
                detail: error.to_string(),
            }),
        }
    }
}

async fn devices<W: Write>(
    cli: &Cli,
    args: &DevicesArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let mut collection = DiscoveryCollection::default();
    discover_physical(args, &mut collection).await;
    discover_configured(cli, args, &mut collection)?;

    for diagnostic in collection
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.status == "error")
    {
        emit_diagnostic(
            cli,
            &format!("{} discovery: {}", diagnostic.provider, diagnostic.detail),
        );
    }
    collection
        .devices
        .sort_by(|left, right| left.id.cmp(&right.id));
    collection.mock_profiles.sort();
    let human = discovery_human(&collection.devices, &collection.mock_profiles);
    let found_any = !collection.devices.is_empty() || !collection.mock_profiles.is_empty();
    let provider_successes = collection.provider_successes;
    let report = DiscoveryReport {
        devices: collection.devices,
        mock_profiles: collection.mock_profiles,
        diagnostics: collection.diagnostics,
    };
    writer
        .result("devices", &report, &human)
        .map_err(CliError::from)?;
    validate_discovery_result(provider_successes, found_any)
}

async fn discover_physical(args: &DevicesArgs, collection: &mut DiscoveryCollection) {
    let want_ble = args
        .transport
        .is_none_or(|choice| choice == TransportChoice::Ble);
    let want_serial = args
        .transport
        .is_none_or(|choice| choice == TransportChoice::Serial);
    let ble_future = async {
        if want_ble {
            Some(discover_ble(args.scan_timeout).await)
        } else {
            None
        }
    };
    let serial_future = async {
        if want_serial {
            Some(discover_serial_async().await)
        } else {
            None
        }
    };
    let (ble_result, serial_result) = tokio::join!(ble_future, serial_future);
    if let Some(result) = ble_result {
        collection.add_physical("ble", "compatible device", result);
    }
    if let Some(result) = serial_result {
        collection.add_physical("serial", "candidate port", result);
    }
}

fn discover_configured(
    cli: &Cli,
    args: &DevicesArgs,
    collection: &mut DiscoveryCollection,
) -> Result<(), CliError> {
    let want_tcp = args
        .transport
        .is_none_or(|choice| choice == TransportChoice::Tcp);
    let want_mock = args.transport == Some(TransportChoice::Mock);
    if want_tcp || want_mock {
        let store = config_store(cli)?;
        match load_unmodified(&store) {
            Ok(LoadOutcome::Loaded(config) | LoadOutcome::NeedsMigration(config)) => {
                collection.provider_successes = collection.provider_successes.saturating_add(1);
                for (name, profile) in config.device_profiles {
                    match profile.transport {
                        TransportConfig::Tcp { host, port } if want_tcp => {
                            match manual_tcp_device(host, port) {
                                Ok(mut device) => {
                                    device.notes.push(format!(
                                        "Configured by profile '{name}'; no reachability claim was made."
                                    ));
                                    collection.devices.push(device);
                                }
                                Err(error) => collection.diagnostics.push(ProviderDiagnostic {
                                    provider: "configured_tcp",
                                    status: "error",
                                    detail: error.to_string(),
                                }),
                            }
                        }
                        TransportConfig::Mock { scenario } if want_mock => {
                            collection.mock_profiles.push(format!("{name}:{scenario}"));
                        }
                        _ => {}
                    }
                }
                collection.diagnostics.push(ProviderDiagnostic {
                    provider: if want_mock {
                        "configured_mock"
                    } else {
                        "configured_tcp"
                    },
                    status: "ok",
                    detail: "configuration inspected without opening endpoints".to_owned(),
                });
            }
            Ok(LoadOutcome::Missing) => collection.diagnostics.push(ProviderDiagnostic {
                provider: "configuration",
                status: "error",
                detail: format!("no configuration at {}", store.path().display()),
            }),
            Err(error) => collection.diagnostics.push(ProviderDiagnostic {
                provider: "configuration",
                status: "error",
                detail: error.message().to_owned(),
            }),
        }
    }
    Ok(())
}

fn validate_discovery_result(provider_successes: usize, found_any: bool) -> Result<(), CliError> {
    if provider_successes == 0 {
        return Err(CliError::new(
            ExitStatus::Discovery,
            "every requested discovery provider failed",
        ));
    }
    if !found_any {
        return Err(CliError::new(
            ExitStatus::Discovery,
            "no requested devices or configured endpoints were found",
        )
        .with_hint("Connect a companion, broaden --transport, or initialize a profile."));
    }
    Ok(())
}

fn discovery_human(devices: &[DiscoveredDevice], mock_profiles: &[String]) -> String {
    let mut lines: Vec<String> = devices
        .iter()
        .map(|device| {
            format!(
                "{}\t{}\t{}",
                transport_kind_name(device.transport),
                device.display_name,
                device.id
            )
        })
        .collect();
    lines.extend(
        mock_profiles
            .iter()
            .map(|profile| format!("mock\t{profile}\texplicit demo profile")),
    );
    if lines.is_empty() {
        lines.push("No devices found.".to_owned());
    }
    lines.join("\n")
}

fn transport_kind_name(kind: meshquill_core::TransportKind) -> &'static str {
    match kind {
        meshquill_core::TransportKind::Ble => "ble",
        meshquill_core::TransportKind::Serial => "serial",
        meshquill_core::TransportKind::Tcp => "tcp",
        meshquill_core::TransportKind::Scripted => "mock",
        meshquill_core::TransportKind::Unknown => "unknown",
    }
}

#[derive(Debug, Serialize)]
struct DoctorCheck {
    name: &'static str,
    status: &'static str,
    detail: String,
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    healthy: bool,
    repaired: bool,
    checks: Vec<DoctorCheck>,
}

struct ConfigDiagnosis {
    healthy: bool,
    repaired: bool,
    checks: Vec<DoctorCheck>,
}

const OLDEST_KNOWN_DEVICE_INFO_LEVEL: u8 = 3;
const NEWEST_KNOWN_DEVICE_INFO_LEVEL: u8 = 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FirmwareCompatibility {
    Legacy,
    Known,
    Newer,
}

async fn doctor<W: Write>(
    cli: &Cli,
    connect_requested: bool,
    repair_requested: bool,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let mut diagnosis = diagnose_configuration(cli, repair_requested)?;
    diagnosis.checks.extend(provider_checks(cli.timeout).await);
    let (connection_healthy, connection_checks) = connection_checks(cli, connect_requested).await;
    diagnosis.checks.extend(connection_checks);
    let healthy = diagnosis.healthy && connection_healthy;
    let human = diagnosis
        .checks
        .iter()
        .map(|check| format!("{}\t{}\t{}", check.status, check.name, check.detail))
        .collect::<Vec<_>>()
        .join("\n");
    let report = DoctorReport {
        healthy,
        repaired: diagnosis.repaired,
        checks: diagnosis.checks,
    };
    writer
        .result("doctor", &report, &human)
        .map_err(CliError::from)?;
    if !healthy {
        return Err(CliError::new(
            if diagnosis.healthy {
                ExitStatus::Connection
            } else {
                ExitStatus::Configuration
            },
            "doctor found one or more blocking problems",
        ));
    }
    Ok(())
}

fn diagnose_configuration(cli: &Cli, repair_requested: bool) -> Result<ConfigDiagnosis, CliError> {
    let store = config_store(cli)?;
    let mut checks = Vec::new();
    let config_result = load_unmodified(&store);
    let mut configuration_healthy = matches!(
        config_result,
        Ok(LoadOutcome::Loaded(_) | LoadOutcome::NeedsMigration(_))
    );
    match &config_result {
        Ok(LoadOutcome::Loaded(_)) => checks.push(DoctorCheck {
            name: "configuration",
            status: "ok",
            detail: format!(
                "current configuration loaded from {}",
                store.path().display()
            ),
        }),
        Ok(LoadOutcome::NeedsMigration(_)) => checks.push(DoctorCheck {
            name: "configuration",
            status: "warning",
            detail: "configuration is valid but needs `meshquill config migrate`".to_owned(),
        }),
        Ok(LoadOutcome::Missing) => checks.push(DoctorCheck {
            name: "configuration",
            status: "error",
            detail: format!("configuration is missing at {}", store.path().display()),
        }),
        Err(error) => checks.push(DoctorCheck {
            name: "configuration",
            status: "error",
            detail: error.message().to_owned(),
        }),
    }

    let mut repaired = false;
    if repair_requested && !configuration_healthy {
        confirm(cli, "back up and repair the selected configuration")?;
        let outcome = store.repair().map_err(CliError::from)?;
        repaired = true;
        configuration_healthy = true;
        checks.push(DoctorCheck {
            name: "configuration_repair",
            status: "ok",
            detail: outcome.backup_path.map_or_else(
                || "safe defaults created; no source file existed".to_owned(),
                |path| {
                    format!(
                        "safe defaults created; source backed up to {}",
                        path.display()
                    )
                },
            ),
        });
    } else if repair_requested {
        checks.push(DoctorCheck {
            name: "configuration_repair",
            status: "ok",
            detail: "no repair was needed; valid configuration was left unchanged".to_owned(),
        });
    }
    if configuration_healthy && let Err(error) = load_optional(&store) {
        configuration_healthy = false;
        checks.push(DoctorCheck {
            name: "runtime_overrides",
            status: "error",
            detail: error.message().to_owned(),
        });
    }
    Ok(ConfigDiagnosis {
        healthy: configuration_healthy,
        repaired,
        checks,
    })
}

async fn provider_checks(timeout: Duration) -> Vec<DoctorCheck> {
    let (serial_result, ble_result) = tokio::join!(discover_serial_async(), discover_ble(timeout));
    let serial = match serial_result {
        Ok(devices) => DoctorCheck {
            name: "serial_provider",
            status: "ok",
            detail: format!(
                "provider enumerated {} candidate(s); OS access is checked only on connect",
                devices.len()
            ),
        },
        Err(error) => DoctorCheck {
            name: "serial_provider",
            status: "warning",
            detail: error.to_string(),
        },
    };
    let ble = match ble_result {
        Ok(devices) => DoctorCheck {
            name: "ble_provider",
            status: "ok",
            detail: format!(
                "bounded scan observed {} compatible device(s); no permission state was inferred",
                devices.len()
            ),
        },
        Err(error) => DoctorCheck {
            name: "ble_provider",
            status: "warning",
            detail: error.to_string(),
        },
    };
    vec![serial, ble]
}

async fn connection_checks(cli: &Cli, connect_requested: bool) -> (bool, Vec<DoctorCheck>) {
    if !connect_requested {
        return (true, Vec::new());
    }
    match select_profile(cli)
        .and_then(|selected| make_client(&selected).map(|client| (selected, client)))
    {
        Ok((selected, client)) => {
            let self_info = match client.connect().await {
                Ok(info) => info,
                Err(error) => {
                    let error = CliError::from(error);
                    let _ = client.shutdown().await;
                    return (false, vec![failed_handshake_check(&error)]);
                }
            };
            let handshake = DoctorCheck {
                name: "handshake",
                status: "ok",
                detail: format!(
                    "APP_START completed for profile '{}' as {}",
                    selected.name, self_info.name
                ),
            };
            let device_info = client.query_device_info().await;
            let shutdown = client.shutdown().await;
            match device_info {
                Err(error) => {
                    let error = CliError::from(error);
                    (
                        false,
                        vec![handshake, failed_firmware_compatibility_check(&error)],
                    )
                }
                Ok(info) => {
                    let compatibility = firmware_compatibility_check(&info);
                    match shutdown {
                        Ok(()) => (true, vec![handshake, compatibility]),
                        Err(error) => {
                            let error = CliError::from(error);
                            (
                                false,
                                vec![
                                    handshake,
                                    compatibility,
                                    DoctorCheck {
                                        name: "connection_shutdown",
                                        status: "error",
                                        detail: format_cli_error(
                                            "graceful connection shutdown failed",
                                            &error,
                                        ),
                                    },
                                ],
                            )
                        }
                    }
                }
            }
        }
        Err(error) => (false, vec![failed_handshake_check(&error)]),
    }
}

fn failed_handshake_check(error: &CliError) -> DoctorCheck {
    DoctorCheck {
        name: "handshake",
        status: "error",
        detail: format_cli_error("APP_START failed", error),
    }
}

const fn classify_firmware_compatibility(protocol_level: u8) -> FirmwareCompatibility {
    match protocol_level {
        ..OLDEST_KNOWN_DEVICE_INFO_LEVEL => FirmwareCompatibility::Legacy,
        OLDEST_KNOWN_DEVICE_INFO_LEVEL..=NEWEST_KNOWN_DEVICE_INFO_LEVEL => {
            FirmwareCompatibility::Known
        }
        _ => FirmwareCompatibility::Newer,
    }
}

fn firmware_compatibility_check(info: &DeviceInfo) -> DoctorCheck {
    let protocol_level = info.protocol_version;
    let identity = format!(
        "firmware '{}', model '{}'",
        info.firmware_version.as_deref().unwrap_or("unknown"),
        info.model.as_deref().unwrap_or("unknown")
    );
    match classify_firmware_compatibility(protocol_level) {
        FirmwareCompatibility::Legacy => DoctorCheck {
            name: "firmware_compatibility",
            status: "warning",
            detail: format!(
                "DEVICE_INFO protocol level {protocol_level} is legacy; only reduced capability information is available ({identity})"
            ),
        },
        FirmwareCompatibility::Known => DoctorCheck {
            name: "firmware_compatibility",
            status: "ok",
            detail: format!(
                "DEVICE_INFO protocol level {protocol_level} has a known layout ({identity})"
            ),
        },
        FirmwareCompatibility::Newer => DoctorCheck {
            name: "firmware_compatibility",
            status: "warning",
            detail: format!(
                "DEVICE_INFO protocol level {protocol_level} is newer than known level {NEWEST_KNOWN_DEVICE_INFO_LEVEL}; known fields were read from {identity}, extension fields were ignored, and full compatibility is not claimed"
            ),
        },
    }
}

fn failed_firmware_compatibility_check(error: &CliError) -> DoctorCheck {
    DoctorCheck {
        name: "firmware_compatibility",
        status: "error",
        detail: format_cli_error("DEVICE_QUERY failed", error),
    }
}

fn format_cli_error(context: &str, error: &CliError) -> String {
    match error.hint() {
        Some(hint) => format!("{context}: {}; hint: {hint}", error.message()),
        None => format!("{context}: {}", error.message()),
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct WatchRecord {
    event: &'static str,
    data: Value,
}

async fn watch<W: Write>(
    cli: &Cli,
    args: &WatchArgs,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    watch_events(cli, &args.events, args.count, writer).await
}

async fn watch_connection<W: Write>(
    cli: &Cli,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    watch_events(cli, &[WatchEvent::Connection], None, writer).await
}

#[allow(clippy::too_many_lines)]
async fn watch_events<W: Write>(
    cli: &Cli,
    filters: &[WatchEvent],
    count: Option<usize>,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let selected = select_profile(cli)?;
    let reconnect_policy =
        ReconnectPolicy::new(selected.retry_timeout(), selected.connect_timeout());
    let workflow = WorkflowServices::from_selected(&selected)?;
    let client = make_client(&selected)?;
    let mut receiver = client.subscribe();
    let interrupt = InterruptWatcher::install().await;
    let connect_result = tokio::select! {
        result = client.connect() => result,
        () = interrupt.cancelled() => {
            client.cancel_pending_operations();
            let _ = client.shutdown().await;
            return Err(interrupt.error());
        }
    };
    let info = match connect_result {
        Ok(info) => info,
        Err(error) => return finish::<()>(&client, Err(error)).await,
    };
    activate_workflow(
        &client,
        &workflow,
        &info.name,
        "watch connection hook failed",
    )
    .await?;
    let mut device_name = info.name;
    let mut workflow_connected = true;
    let mut awaiting_reconnect_confirmation = false;
    let mut emitted = 0_usize;
    while count.is_none_or(|limit| emitted < limit) {
        tokio::select! {
            () = interrupt.cancelled() => {
                if workflow_connected {
                    cleanup_workflow(&client, &workflow, "interrupted").await;
                } else {
                    let _ = client.shutdown().await;
                }
                return Err(interrupt.error());
            }
            event = receiver.recv() => {
                match event {
                    Ok(event) => {
                        if awaiting_reconnect_confirmation
                            && matches!(&event, Event::Disconnected)
                        {
                            continue;
                        }
                        if awaiting_reconnect_confirmation && matches!(&event, Event::Connected) {
                            awaiting_reconnect_confirmation = false;
                        }
                        let disconnected = matches!(&event, Event::Disconnected);
                        let mut suppress_event = false;
                        let workflow_result = match &event {
                            Event::Message(message) => match workflow
                                .incoming(message, IncomingOrigin::Live)
                                .await
                            {
                                Ok(Some(_)) => Ok(()),
                                Ok(None) => {
                                    suppress_event = true;
                                    Ok(())
                                }
                                Err(error) => Err(error),
                            },
                            Event::Disconnected if workflow_connected => {
                                workflow_connected = false;
                                workflow.disconnected(Some("device event")).await
                            }
                            Event::Connected if !workflow_connected => {
                                let result = activate_workflow(
                                    &client,
                                    &workflow,
                                    &device_name,
                                    "watch reconnect hook failed",
                                )
                                .await;
                                if result.is_ok() {
                                    workflow_connected = true;
                                }
                                result
                            }
                            Event::ProtocolError(_) | Event::UnknownPacket { .. } | Event::LoginFailed { .. } => {
                                workflow.error("watch", "the device emitted an error event").await
                            }
                            _ => Ok(()),
                        };
                        if let Err(error) = workflow_result {
                            if workflow_connected {
                                cleanup_workflow(&client, &workflow, "watch workflow failed").await;
                            } else {
                                let _ = client.shutdown().await;
                            }
                            return Err(error);
                        }
                        if !suppress_event && event_matches(&event, filters) {
                            let record = watch_record(&event);
                            let human = watch_human(&record);
                            if let Err(error) = writer.event("event", &record, &human).map_err(CliError::from) {
                                if workflow_connected {
                                    cleanup_workflow(&client, &workflow, "watch output failed").await;
                                } else {
                                    let _ = client.shutdown().await;
                                }
                                return Err(error);
                            }
                            emitted = emitted.saturating_add(1);
                        }
                        if disconnected && count.is_none_or(|limit| emitted < limit) {
                            match reconnect_device(
                                &client,
                                reconnect_policy,
                                interrupt.token(),
                            )
                            .await
                            {
                                Ok(reconnected) => {
                                    device_name = reconnected.name;
                                    activate_workflow(
                                        &client,
                                        &workflow,
                                        &device_name,
                                        "watch reconnect hook failed",
                                    )
                                    .await?;
                                    workflow_connected = true;
                                    awaiting_reconnect_confirmation = true;
                                }
                                Err(error) => {
                                    let _ = client.shutdown().await;
                                    return if interrupt.token().is_cancelled() {
                                        Err(interrupt.error())
                                    } else {
                                        Err(error)
                                    };
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        awaiting_reconnect_confirmation = false;
                        emit_diagnostic(cli, &format!(
                            "event consumer lagged; {skipped} bounded event(s) were skipped"
                        ));
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        if workflow_connected {
                            cleanup_workflow(&client, &workflow, "event stream closed").await;
                        } else {
                            let _ = client.shutdown().await;
                        }
                        return Err(CliError::new(
                            ExitStatus::Connection,
                            "the device event stream closed",
                        ));
                    }
                }
            }
        }
    }
    if workflow_connected {
        finish_workflow(&client, &workflow, "command completed").await
    } else {
        finish(&client, Ok(())).await
    }
}

fn event_matches(event: &Event, filters: &[WatchEvent]) -> bool {
    if filters.is_empty() {
        return true;
    }
    filters.iter().any(|filter| match filter {
        WatchEvent::Message => matches!(event, Event::Message(_)),
        WatchEvent::Ack => matches!(event, Event::Ack(_) | Event::MessageSent { .. }),
        WatchEvent::Contact => matches!(event, Event::Contacts { .. }),
        WatchEvent::Connection => matches!(event, Event::Connected | Event::Disconnected),
        WatchEvent::Telemetry => matches!(
            event,
            Event::Battery { .. }
                | Event::DeviceStats(_)
                | Event::Telemetry(_)
                | Event::RemoteStatus(_)
        ),
        WatchEvent::Error => matches!(
            event,
            Event::ProtocolError(_) | Event::UnknownPacket { .. } | Event::LoginFailed { .. }
        ),
    })
}

// Keep the exhaustive redaction policy in one auditable match as the event surface evolves.
#[allow(clippy::too_many_lines)]
pub(crate) fn watch_record(event: &Event) -> WatchRecord {
    match event {
        Event::Connected => WatchRecord {
            event: "connected",
            data: json!({}),
        },
        Event::Disconnected => WatchRecord {
            event: "disconnected",
            data: json!({}),
        },
        Event::Message(message) => WatchRecord {
            event: "message",
            data: json!({ "message": message }),
        },
        Event::Ack(ack) => WatchRecord {
            event: "ack",
            data: json!({
                "code": bytes_hex(&ack.code),
                "trip_time_ms": ack.trip_time_ms,
            }),
        },
        Event::MessageSent {
            destination_type,
            ack_code,
            suggested_timeout_ms,
        } => WatchRecord {
            event: "message_sent",
            data: json!({
                "destination_type": destination_type,
                "ack_code": bytes_hex(ack_code),
                "suggested_timeout_ms": suggested_timeout_ms,
            }),
        },
        Event::Contacts { contacts, lastmod } => WatchRecord {
            event: "contacts",
            data: json!({ "count": contacts.len(), "lastmod": lastmod }),
        },
        Event::SelfInfo(info) => WatchRecord {
            event: "self_info",
            data: json!({
                "name": info.name,
                "public_key": info.public_key.to_hex(),
            }),
        },
        Event::DeviceInfo(info) => WatchRecord {
            event: "device_info",
            data: json!({
                "protocol_version": info.protocol_version,
                "model": info.model,
                "firmware_version": info.firmware_version,
            }),
        },
        Event::Battery {
            level,
            used_kb,
            total_kb,
        } => WatchRecord {
            event: "battery",
            data: json!({ "level": level, "used_kb": used_kb, "total_kb": total_kb }),
        },
        Event::ProtocolError(message) => WatchRecord {
            event: "protocol_error",
            data: json!({ "message": message }),
        },
        Event::InboxEmpty => WatchRecord {
            event: "inbox_empty",
            data: json!({}),
        },
        Event::MessagesWaiting => WatchRecord {
            event: "messages_waiting",
            data: json!({}),
        },
        Event::ChannelInfo { idx, name, .. } => WatchRecord {
            event: "channel_info",
            data: json!({ "idx": idx, "name": name }),
        },
        Event::CurrentTime(value) => WatchRecord {
            event: "current_time",
            data: json!({ "timestamp": value }),
        },
        Event::UnknownPacket { code, payload } => WatchRecord {
            event: "unknown_packet",
            data: json!({ "code": code, "payload_bytes": payload.len() }),
        },
        Event::ContactUri(_) => WatchRecord {
            event: "contact_uri",
            data: json!({ "redacted": true }),
        },
        Event::TuningParams(_) => generic_watch_record("tuning_params"),
        Event::CustomVariables(_) => generic_watch_record("custom_variables"),
        Event::AdvertPath(_) => generic_watch_record("advert_path"),
        Event::DeviceStats(_) => generic_watch_record("device_stats"),
        Event::AutoAddConfig(_) => generic_watch_record("auto_add_config"),
        Event::AllowedRepeatFrequencies(values) => WatchRecord {
            event: "allowed_repeat_frequencies",
            data: json!({ "count": values.len() }),
        },
        Event::DefaultFloodScope(_) => generic_watch_record("default_flood_scope"),
        Event::LoginSucceeded(session) => WatchRecord {
            event: "login_succeeded",
            data: json!({
                "permissions": session.permissions,
                "acl_permissions": session.acl_permissions,
                "firmware_version_level": session.firmware_version_level,
                "server_timestamp": session.server_timestamp,
            }),
        },
        Event::LoginFailed { .. } => WatchRecord {
            event: "login_failed",
            data: json!({ "target": "redacted" }),
        },
        Event::RemoteStatus(status) => WatchRecord {
            event: "remote_status",
            data: json!({
                "battery_mv": status.battery_mv,
                "uptime_seconds": status.uptime_seconds,
                "tx_queue_length": status.tx_queue_length,
            }),
        },
        Event::Telemetry(response) => WatchRecord {
            event: "telemetry",
            data: json!({ "payload_bytes": response.payload.len() }),
        },
        Event::BinaryResponse(response) => WatchRecord {
            event: "binary_response",
            data: json!({ "payload_bytes": response.payload.len() }),
        },
        Event::ControlData(data) => WatchRecord {
            event: "control_data",
            data: json!({
                "snr_qdb": data.snr_qdb,
                "rssi_dbm": data.rssi,
                "path_len": data.path_len,
                "payload_bytes": data.payload.len(),
            }),
        },
        Event::PathDiscovery(path) => WatchRecord {
            event: "path_discovery",
            data: json!({
                "outbound_path_bytes": path.outbound_path.as_bytes().len(),
                "inbound_path_bytes": path.inbound_path.as_bytes().len(),
            }),
        },
        Event::Signature(signature) => WatchRecord {
            event: "signature",
            data: json!({ "signature_bytes": signature.as_bytes().len() }),
        },
    }
}

fn generic_watch_record(event: &'static str) -> WatchRecord {
    WatchRecord {
        event,
        data: json!({ "available": true }),
    }
}

pub(crate) fn watch_human(record: &WatchRecord) -> String {
    match record.event {
        "message" => record
            .data
            .get("message")
            .and_then(|message| message.get("text"))
            .and_then(Value::as_str)
            .map_or_else(|| "message".to_owned(), |text| format!("message\t{text}")),
        "protocol_error" => record
            .data
            .get("message")
            .and_then(Value::as_str)
            .map_or_else(
                || "protocol error".to_owned(),
                |message| format!("error\t{message}"),
            ),
        other => other.replace('_', " "),
    }
}

#[derive(Debug, Serialize)]
struct ChatRecord {
    state: &'static str,
    destination: String,
    draft_retained: bool,
}

#[derive(Debug, Serialize)]
struct ChatIncomingRecord {
    state: &'static str,
    source: String,
    text: String,
    message_id: String,
}

#[derive(Debug, Serialize)]
struct ChatHelpRecord {
    state: &'static str,
    destination: String,
    commands: &'static [&'static str],
}

#[derive(Debug, Serialize)]
struct ChatContactSummary {
    name: String,
    public_key_prefix: String,
    kind: &'static str,
}

#[derive(Debug, Serialize)]
struct ChatContactsRecord {
    state: &'static str,
    destination: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<String>,
    contacts: Vec<ChatContactSummary>,
}

#[derive(Debug, Serialize)]
struct ChatHistoryRecord {
    state: &'static str,
    destination: String,
    enabled: bool,
    storage: &'static str,
    entries: Vec<HistoryEntry>,
}

#[derive(Debug, Serialize)]
struct ChatCommandErrorRecord {
    state: &'static str,
    destination: String,
    command: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    suggestion: Option<String>,
}

#[derive(Clone)]
struct ChatTarget {
    destination: String,
    channel: Option<u8>,
    direct_prefix: Option<[u8; 6]>,
}

#[derive(Clone)]
struct RetainedChatDraft {
    target: ChatTarget,
    text: String,
}

#[derive(Debug, Eq, PartialEq)]
enum ParsedChatLine {
    Empty,
    Message(String),
    Help,
    Contacts {
        query: Option<String>,
    },
    To {
        query: String,
    },
    Channel {
        channel: u8,
    },
    History {
        limit: usize,
    },
    SendRetained,
    DiscardRetained,
    Quit,
    CommandError {
        command: String,
        message: String,
        suggestion: Option<String>,
    },
}

const CHAT_HISTORY_DEFAULT: usize = 20;
const CHAT_HISTORY_MAX: usize = 100;
const CHAT_COMMAND_NAMES: &[&str] = &[
    "/help",
    "/contacts",
    "/to",
    "/channel",
    "/history",
    "/send",
    "/discard",
    "/quit",
];
const CHAT_HELP_COMMANDS: &[&str] = &[
    "/help",
    "/contacts [query]",
    "/to <contact>",
    "/channel <0..255>",
    "/history [N]",
    "/send",
    "/discard",
    "/quit",
    "//text",
];

fn parse_chat_line(line: &str) -> ParsedChatLine {
    if line.trim().is_empty() {
        return ParsedChatLine::Empty;
    }
    if let Some(literal) = line.strip_prefix("//") {
        return ParsedChatLine::Message(format!("/{literal}"));
    }
    if !line.starts_with('/') {
        return ParsedChatLine::Message(line.to_owned());
    }

    let command_end = line.find(char::is_whitespace).unwrap_or(line.len());
    let command = &line[..command_end];
    let arguments = line[command_end..].trim();
    match command {
        "/help" => no_argument_chat_command(arguments, command, ParsedChatLine::Help),
        "/contacts" => ParsedChatLine::Contacts {
            query: (!arguments.is_empty()).then(|| arguments.to_owned()),
        },
        "/to" => {
            if arguments.is_empty() {
                chat_parse_error(command, "expected an exact contact name or key prefix")
            } else {
                ParsedChatLine::To {
                    query: arguments.to_owned(),
                }
            }
        }
        "/channel" => match one_chat_argument(arguments) {
            Some(value) => match value.parse::<u8>() {
                Ok(channel) => ParsedChatLine::Channel { channel },
                Err(_) => chat_parse_error(command, "expected one channel index from 0 to 255"),
            },
            None => chat_parse_error(command, "expected one channel index from 0 to 255"),
        },
        "/history" => {
            if arguments.is_empty() {
                ParsedChatLine::History {
                    limit: CHAT_HISTORY_DEFAULT,
                }
            } else {
                match one_chat_argument(arguments).and_then(|value| value.parse::<usize>().ok()) {
                    Some(0) | None => {
                        chat_parse_error(command, "expected one positive history entry count")
                    }
                    Some(limit) => ParsedChatLine::History {
                        limit: limit.min(CHAT_HISTORY_MAX),
                    },
                }
            }
        }
        "/send" => no_argument_chat_command(arguments, command, ParsedChatLine::SendRetained),
        "/discard" => no_argument_chat_command(arguments, command, ParsedChatLine::DiscardRetained),
        "/quit" => no_argument_chat_command(arguments, command, ParsedChatLine::Quit),
        _ => ParsedChatLine::CommandError {
            command: command.to_owned(),
            message: format!("unknown chat command '{command}'"),
            suggestion: closest_suggestion(command, CHAT_COMMAND_NAMES.iter().copied())
                .map(str::to_owned),
        },
    }
}

fn one_chat_argument(arguments: &str) -> Option<&str> {
    let mut values = arguments.split_whitespace();
    let value = values.next()?;
    values.next().is_none().then_some(value)
}

fn no_argument_chat_command(
    arguments: &str,
    command: &str,
    parsed: ParsedChatLine,
) -> ParsedChatLine {
    if arguments.is_empty() {
        parsed
    } else {
        chat_parse_error(command, "this command does not accept arguments")
    }
}

fn chat_parse_error(command: &str, message: &str) -> ParsedChatLine {
    ParsedChatLine::CommandError {
        command: command.to_owned(),
        message: message.to_owned(),
        suggestion: None,
    }
}

fn edit_distance(left: &str, right: &str) -> usize {
    let right: Vec<char> = right.chars().collect();
    let mut previous: Vec<usize> = (0..=right.len()).collect();
    let mut current = vec![0; right.len() + 1];
    for (left_index, left_char) in left.chars().enumerate() {
        current[0] = left_index + 1;
        for (right_index, right_char) in right.iter().enumerate() {
            let substitution = previous[right_index] + usize::from(left_char != *right_char);
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(substitution);
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[right.len()]
}

fn closest_suggestion<'a>(
    query: &str,
    candidates: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    if query.is_empty() {
        return None;
    }
    let normalized_query = query.to_lowercase();
    let maximum_distance = normalized_query.chars().count().div_ceil(3).clamp(1, 3);
    candidates
        .into_iter()
        .map(|candidate| {
            (
                candidate,
                edit_distance(&normalized_query, &candidate.to_lowercase()),
            )
        })
        .filter(|(_, distance)| *distance <= maximum_distance)
        .min_by(|(left, left_distance), (right, right_distance)| {
            left_distance
                .cmp(right_distance)
                .then_with(|| left.to_lowercase().cmp(&right.to_lowercase()))
                .then_with(|| left.cmp(right))
        })
        .map(|(candidate, _)| candidate)
}

fn contact_name_suggestion<'a>(contacts: &'a [Contact], query: &str) -> Option<&'a str> {
    closest_suggestion(
        query,
        contacts.iter().map(|contact| contact.adv_name.as_str()),
    )
}

const CHAT_INPUT_CAPACITY: usize = 1;

struct ChatInput {
    receiver: tokio_mpsc::Receiver<Result<Option<String>, CliError>>,
    resume: std_mpsc::Sender<()>,
}

impl ChatInput {
    fn spawn() -> Result<Self, CliError> {
        let (line_tx, receiver) = tokio_mpsc::channel(CHAT_INPUT_CAPACITY);
        let (resume, resume_rx) = std_mpsc::channel();
        thread::Builder::new()
            .name("meshquill-chat-input".to_owned())
            .spawn(move || {
                let stdin = io::stdin();
                let mut input = stdin.lock();
                loop {
                    let result = read_chat_line(&mut input);
                    let terminal = !matches!(&result, Ok(Some(_)));
                    if line_tx.blocking_send(result).is_err()
                        || terminal
                        || resume_rx.recv().is_err()
                    {
                        break;
                    }
                }
            })
            .map_err(|_| {
                CliError::new(
                    ExitStatus::Protocol,
                    "could not start the bounded line chat input reader",
                )
            })?;
        Ok(Self { receiver, resume })
    }

    async fn next(&mut self) -> Result<Option<String>, CliError> {
        self.receiver.recv().await.ok_or_else(|| {
            CliError::new(
                ExitStatus::Protocol,
                "the line chat input reader stopped unexpectedly",
            )
        })?
    }

    fn resume(&self) -> Result<(), CliError> {
        self.resume.send(()).map_err(|_| {
            CliError::new(
                ExitStatus::Protocol,
                "the line chat input reader stopped unexpectedly",
            )
        })
    }
}

enum ChatLineFlow {
    Continue,
    Quit,
    SessionReconnected(usize),
}

#[derive(Clone, Copy)]
struct ChatSessionControl<'a> {
    ack_timeout: Duration,
    reconnect_policy: ReconnectPolicy,
    interrupt: &'a InterruptWatcher,
    workflow_connected: &'a AtomicBool,
}

enum ChatAckOutcome {
    Acknowledged,
    TimedOut,
    Reconnected,
}

impl ChatAckOutcome {
    const fn output(self) -> (&'static str, &'static str) {
        match self {
            Self::Acknowledged => ("acknowledged", "acknowledged"),
            Self::TimedOut => (
                "timed_out",
                "Acknowledgement timed out; chat remains open and the message was not retransmitted.",
            ),
            Self::Reconnected => (
                "reconnected",
                "Reconnected after acknowledgement tracking failed; the sent message was not retained for resend.",
            ),
        }
    }
}

async fn chat<W: Write>(
    cli: &Cli,
    destination: Option<&str>,
    line_requested: bool,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    if !line_requested {
        emit_diagnostic(cli, "using the portable line chat interface");
    }
    let destination = chat_destination(cli, destination)?;
    let selected = select_profile(cli)?;
    let reconnect_policy =
        ReconnectPolicy::new(selected.retry_timeout(), selected.connect_timeout());
    let workflow = WorkflowServices::from_selected(&selected)?;
    let client = make_client(&selected)?;
    let mut events = client.subscribe();
    let interrupt = InterruptWatcher::install().await;
    let connect_result = tokio::select! {
        result = client.connect() => result,
        () = interrupt.cancelled() => {
            client.cancel_pending_operations();
            let _ = client.shutdown().await;
            return Err(interrupt.error());
        }
    };
    let info = match connect_result {
        Ok(info) => info,
        Err(error) => return finish::<()>(&client, Err(error)).await,
    };
    activate_workflow(
        &client,
        &workflow,
        &info.name,
        "chat connection hook failed",
    )
    .await?;
    let workflow_connected = AtomicBool::new(true);
    let control = ChatSessionControl {
        ack_timeout: cli.timeout,
        reconnect_policy,
        interrupt: &interrupt,
        workflow_connected: &workflow_connected,
    };
    let (target, target_reconnected) = match prepare_chat_target_with_reconnect(
        &client,
        &workflow,
        destination,
        control,
        "chat target lookup transport failed",
    )
    .await
    {
        Ok(value) => value,
        Err(error) => {
            cleanup_chat_workflow(
                &client,
                &workflow,
                &workflow_connected,
                "chat target failed",
            )
            .await;
            return Err(error);
        }
    };
    if let Err(error) = emit_chat_connected(&target, writer) {
        cleanup_chat_workflow(
            &client,
            &workflow,
            &workflow_connected,
            "chat output failed",
        )
        .await;
        return Err(error);
    }
    if target_reconnected
        && let Err(error) = emit_chat_state(
            &target,
            "reconnected",
            false,
            "Reconnected while resolving the chat target; no message was sent or replayed.",
            writer,
        )
    {
        cleanup_chat_workflow(
            &client,
            &workflow,
            &workflow_connected,
            "chat output failed",
        )
        .await;
        return Err(error);
    }
    let result = run_chat_lines(&client, &workflow, &target, control, &mut events, writer).await;
    match result {
        Ok(()) => finish_workflow(&client, &workflow, "chat completed").await,
        Err(error) => {
            cleanup_chat_workflow(&client, &workflow, &workflow_connected, "chat failed").await;
            Err(error)
        }
    }
}

async fn cleanup_chat_workflow(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    workflow_connected: &AtomicBool,
    reason: &str,
) {
    if workflow_connected.load(Ordering::Acquire) {
        cleanup_workflow(client, workflow, reason).await;
    } else if client.shutdown().await.is_err() {
        tracing::warn!("secondary client shutdown failure; details omitted");
    }
}

fn chat_destination(cli: &Cli, destination: Option<&str>) -> Result<String, CliError> {
    match destination {
        Some(value) if !value.trim().is_empty() => Ok(value.trim().to_owned()),
        _ if cli.non_interactive || !io::stdin().is_terminal() => Err(CliError::new(
            ExitStatus::Usage,
            "line chat requires a destination when input is non-interactive",
        )),
        _ => prompt_chat_destination(),
    }
}

async fn prepare_chat_target_with_reconnect(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    destination: String,
    control: ChatSessionControl<'_>,
    disconnect_reason: &str,
) -> Result<(ChatTarget, bool), CliError> {
    if let Ok(channel) = destination.parse::<u8>() {
        return Ok((chat_channel_target(channel), false));
    }
    let (contacts, reconnected) =
        list_chat_contacts_with_reconnect(client, workflow, control, disconnect_reason).await?;
    resolve_contact(&contacts, &destination)
        .map(chat_contact_target)
        .map(|target| (target, reconnected))
}

async fn list_chat_contacts_with_reconnect(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    control: ChatSessionControl<'_>,
    disconnect_reason: &str,
) -> Result<(Vec<Contact>, bool), CliError> {
    let first = tokio::select! {
        result = client.list_contacts(None) => result,
        () = control.interrupt.cancelled() => {
            client.cancel_pending_operations();
            return Err(control.interrupt.error());
        }
    };
    match first {
        Ok(contacts) => Ok((contacts, false)),
        Err(error) if reconnect_trigger(&error) => {
            reconnect_chat_session(client, workflow, control, disconnect_reason).await?;
            let contacts = tokio::select! {
                result = client.list_contacts(None) => result.map_err(CliError::from)?,
                () = control.interrupt.cancelled() => {
                    client.cancel_pending_operations();
                    return Err(control.interrupt.error());
                }
            };
            Ok((contacts, true))
        }
        Err(error) => Err(CliError::from(error)),
    }
}

async fn reconnect_chat_session(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    control: ChatSessionControl<'_>,
    disconnect_reason: &str,
) -> Result<SelfInfo, CliError> {
    if control.workflow_connected.swap(false, Ordering::AcqRel) {
        workflow.disconnected(Some(disconnect_reason)).await?;
    }
    let info =
        reconnect_device(client, control.reconnect_policy, control.interrupt.token()).await?;
    activate_workflow(client, workflow, &info.name, "chat reconnect hook failed").await?;
    control.workflow_connected.store(true, Ordering::Release);
    Ok(info)
}

fn chat_contact_target(contact: &Contact) -> ChatTarget {
    let mut prefix = [0_u8; 6];
    prefix.copy_from_slice(&contact.public_key.as_bytes()[..6]);
    ChatTarget {
        destination: contact.adv_name.clone(),
        channel: None,
        direct_prefix: Some(prefix),
    }
}

fn chat_channel_target(channel: u8) -> ChatTarget {
    ChatTarget {
        destination: channel.to_string(),
        channel: Some(channel),
        direct_prefix: None,
    }
}

fn emit_chat_connected<W: Write>(
    target: &ChatTarget,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let connected = ChatRecord {
        state: "connected",
        destination: target.destination.clone(),
        draft_retained: false,
    };
    writer
        .event(
            "chat",
            &connected,
            &format!(
                "Chatting with {}; /help lists commands and /quit exits.",
                target.destination
            ),
        )
        .map_err(CliError::from)
}

#[allow(clippy::too_many_lines)]
async fn run_chat_lines<W: Write>(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    initial_target: &ChatTarget,
    control: ChatSessionControl<'_>,
    events: &mut broadcast::Receiver<Event>,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let interrupt = control.interrupt;
    drain_chat_inbox_with_reconnect(client, workflow, initial_target, control, events, writer)
        .await?;
    let mut input = ChatInput::spawn()?;
    let mut target = initial_target.clone();
    let mut retained_draft: Option<RetainedChatDraft> = None;
    let mut pending_reconnect_confirmations = 0_usize;
    loop {
        tokio::select! {
            biased;
            () = interrupt.cancelled() => return Err(interrupt.error()),
            event = events.recv() => {
                match event {
                    Ok(Event::Disconnected) if pending_reconnect_confirmations > 0 => {}
                    Ok(Event::Connected) if pending_reconnect_confirmations > 0 => {
                        pending_reconnect_confirmations = pending_reconnect_confirmations.saturating_sub(1);
                    }
                    Ok(Event::Message(message)) => {
                        emit_chat_incoming(
                            workflow,
                            message,
                            IncomingOrigin::Live,
                            writer,
                        )
                        .await?;
                    }
                    Ok(Event::Disconnected) => {
                        reconnect_chat_session(
                            client,
                            workflow,
                            control,
                            "chat device event",
                        )
                        .await
                        .map_err(|error| {
                            if interrupt.token().is_cancelled() {
                                interrupt.error()
                            } else {
                                error
                            }
                        })?;
                        pending_reconnect_confirmations = pending_reconnect_confirmations.saturating_add(1);
                        emit_chat_state(
                            &target,
                            "reconnected",
                            retained_draft.is_some(),
                            "Reconnected to the companion; no message was replayed.",
                            writer,
                        )?;
                    }
                    Ok(
                        Event::ProtocolError(_)
                        | Event::UnknownPacket { .. }
                        | Event::LoginFailed { .. },
                    ) => {
                        workflow
                            .error("chat", "the device emitted an error event")
                            .await?;
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        pending_reconnect_confirmations = 0;
                        emit_chat_state(
                            &target,
                            "lagged",
                            retained_draft.is_some(),
                            &format!("Chat event consumer skipped {skipped} bounded event(s)."),
                            writer,
                        )?;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(CliError::new(
                            ExitStatus::Connection,
                            "the device event stream closed during chat",
                        ));
                    }
                }
            }
            line = input.next() => {
                let Some(line) = line? else {
                    return Ok(());
                };
                match handle_chat_line(
                    client,
                    workflow,
                    &mut target,
                    line,
                    control,
                    &mut retained_draft,
                    writer,
                )
                .await?
                {
                    ChatLineFlow::Quit => return Ok(()),
                    ChatLineFlow::Continue => {
                        if interrupt.token().is_cancelled() {
                            return Err(interrupt.error());
                        }
                        input.resume()?;
                    }
                    ChatLineFlow::SessionReconnected(count) => {
                        pending_reconnect_confirmations = pending_reconnect_confirmations.saturating_add(count);
                        if interrupt.token().is_cancelled() {
                            return Err(interrupt.error());
                        }
                        input.resume()?;
                    }
                }
            }
        }
    }
}

async fn drain_chat_inbox_with_reconnect<W: Write>(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    target: &ChatTarget,
    control: ChatSessionControl<'_>,
    events: &mut broadcast::Receiver<Event>,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    match poll_chat_incoming(client, workflow, events, control, writer).await {
        Ok(()) => {}
        Err(error) if error.status() == ExitStatus::Connection => {
            reconnect_chat_session(
                client,
                workflow,
                control,
                "initial chat inbox transport failed",
            )
            .await?;
            emit_chat_state(
                target,
                "reconnected",
                false,
                "Reconnected while loading the queued inbox; no message was sent or replayed.",
                writer,
            )?;
            poll_chat_incoming(client, workflow, events, control, writer).await?;
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn handle_chat_line<W: Write>(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    target: &mut ChatTarget,
    line: String,
    control: ChatSessionControl<'_>,
    retained_draft: &mut Option<RetainedChatDraft>,
    writer: &mut OutputWriter<W>,
) -> Result<ChatLineFlow, CliError> {
    match parse_chat_line(&line) {
        ParsedChatLine::Empty => Ok(ChatLineFlow::Continue),
        ParsedChatLine::Quit => Ok(ChatLineFlow::Quit),
        ParsedChatLine::Help => {
            emit_chat_help(target, writer)?;
            Ok(ChatLineFlow::Continue)
        }
        ParsedChatLine::Contacts { query } => {
            let (contacts, reconnected) = list_chat_contacts_with_reconnect(
                client,
                workflow,
                control,
                "chat contact listing transport failed",
            )
            .await?;
            if reconnected {
                emit_chat_state(
                    target,
                    "reconnected",
                    retained_draft.is_some(),
                    "Reconnected while listing contacts; no message was sent or replayed.",
                    writer,
                )?;
            }
            emit_chat_contacts(contacts, target, query, writer)?;
            Ok(if reconnected {
                ChatLineFlow::SessionReconnected(1)
            } else {
                ChatLineFlow::Continue
            })
        }
        ParsedChatLine::To { query } => {
            let (contacts, reconnected) = list_chat_contacts_with_reconnect(
                client,
                workflow,
                control,
                "chat destination lookup transport failed",
            )
            .await?;
            if reconnected {
                emit_chat_state(
                    target,
                    "reconnected",
                    retained_draft.is_some(),
                    "Reconnected while resolving the destination; no message was sent or replayed.",
                    writer,
                )?;
            }
            match resolve_contact(&contacts, &query) {
                Ok(contact) => {
                    *target = chat_contact_target(contact);
                    emit_chat_destination_changed(target, retained_draft.as_ref(), writer)?;
                }
                Err(error) => {
                    emit_chat_command_error(
                        target,
                        "/to",
                        error.message(),
                        contact_name_suggestion(&contacts, &query),
                        writer,
                    )?;
                }
            }
            Ok(if reconnected {
                ChatLineFlow::SessionReconnected(1)
            } else {
                ChatLineFlow::Continue
            })
        }
        ParsedChatLine::Channel { channel } => {
            *target = chat_channel_target(channel);
            emit_chat_destination_changed(target, retained_draft.as_ref(), writer)?;
            Ok(ChatLineFlow::Continue)
        }
        ParsedChatLine::History { limit } => {
            emit_chat_history(workflow, target, limit, writer).await?;
            Ok(ChatLineFlow::Continue)
        }
        ParsedChatLine::SendRetained => {
            let Some(draft) = retained_draft.take() else {
                emit_chat_command_error(
                    target,
                    "/send",
                    "there is no retained failed-send draft",
                    None,
                    writer,
                )?;
                return Ok(ChatLineFlow::Continue);
            };
            submit_chat_draft(client, workflow, draft, control, retained_draft, writer).await
        }
        ParsedChatLine::DiscardRetained => {
            let Some(discarded) = retained_draft.take() else {
                emit_chat_command_error(
                    target,
                    "/discard",
                    "there is no retained failed-send draft",
                    None,
                    writer,
                )?;
                return Ok(ChatLineFlow::Continue);
            };
            emit_chat_state(
                target,
                "draft_discarded",
                false,
                &format!(
                    "Discarded the retained draft addressed to {}.",
                    discarded.target.destination
                ),
                writer,
            )?;
            Ok(ChatLineFlow::Continue)
        }
        ParsedChatLine::Message(text) => {
            if let Some(draft) = retained_draft.as_ref() {
                emit_chat_command_error(
                    target,
                    "message",
                    &format!(
                        "a failed-send draft for {} is retained; use /send or /discard before composing another message",
                        draft.target.destination
                    ),
                    None,
                    writer,
                )?;
                return Ok(ChatLineFlow::Continue);
            }
            let prepared = workflow
                .prepare_send(target.destination.clone(), text)
                .await?;
            let (send_target, target_reconnected) = prepare_chat_target_with_reconnect(
                client,
                workflow,
                prepared.destination,
                control,
                "chat send target lookup transport failed",
            )
            .await?;
            if target_reconnected {
                emit_chat_state(
                    target,
                    "reconnected",
                    false,
                    "Reconnected while resolving the unsent draft; its text was retained and no message was replayed.",
                    writer,
                )?;
            }
            let draft = RetainedChatDraft {
                target: send_target,
                text: prepared.text,
            };
            let flow =
                submit_chat_draft(client, workflow, draft, control, retained_draft, writer).await?;
            Ok(match flow {
                ChatLineFlow::Continue if target_reconnected => ChatLineFlow::SessionReconnected(1),
                ChatLineFlow::SessionReconnected(count) if target_reconnected => {
                    ChatLineFlow::SessionReconnected(count.saturating_add(1))
                }
                flow => flow,
            })
        }
        ParsedChatLine::CommandError {
            command,
            message,
            suggestion,
        } => {
            emit_chat_command_error(target, &command, &message, suggestion.as_deref(), writer)?;
            Ok(ChatLineFlow::Continue)
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn submit_chat_draft<W: Write>(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    draft: RetainedChatDraft,
    control: ChatSessionControl<'_>,
    retained_draft: &mut Option<RetainedChatDraft>,
    writer: &mut OutputWriter<W>,
) -> Result<ChatLineFlow, CliError> {
    let mut outgoing = workflow
        .begin_outgoing(&draft.target.destination, draft.target.channel, &draft.text)
        .await?;
    let message_id = outgoing.message_id().to_string();
    let send_result = send_chat_message(client, &draft.target, &draft.text).await;
    let mut session_reconnected = false;
    match send_result {
        Ok(tracking) => {
            let mut post_acceptance_error = workflow
                .sent(
                    &mut outgoing,
                    &draft.target.destination,
                    &draft.text,
                    &message_id,
                    tracking.as_ref().map(|value| value.ack_code),
                )
                .await
                .err()
                .map(|error| ("post-send workflow failed", error));
            emit_chat_state(
                &draft.target,
                "sent",
                retained_draft.is_some(),
                &format!("Sent to {}.", draft.target.destination),
                writer,
            )?;
            if control.interrupt.token().is_cancelled() {
                return Err(accepted_delivery_error(
                    "chat was interrupted after delivery was accepted",
                    &control.interrupt.error(),
                ));
            }
            if let Some(tracking) = tracking {
                let (outcome, acknowledgement_workflow_error) = wait_for_chat_ack(
                    client,
                    workflow,
                    &draft.target,
                    &mut outgoing,
                    &message_id,
                    tracking,
                    control,
                )
                .await?;
                if let Some(error) = acknowledgement_workflow_error {
                    if post_acceptance_error.is_none() {
                        post_acceptance_error = Some(("acknowledgement workflow failed", error));
                    } else {
                        tracing::warn!(
                            error = %error,
                            "secondary acknowledgement workflow failure; message was already accepted"
                        );
                    }
                }
                session_reconnected = matches!(&outcome, ChatAckOutcome::Reconnected);
                let (state, human) = outcome.output();
                emit_chat_state(
                    &draft.target,
                    state,
                    retained_draft.is_some(),
                    &format!("{}: {human}", draft.target.destination),
                    writer,
                )?;
            }
            if let Some((stage, error)) = post_acceptance_error {
                return Err(accepted_delivery_error(stage, &error));
            }
        }
        Err(error) if reconnect_trigger(&error) => {
            if let Err(history_error) = workflow.failed(&mut outgoing).await {
                tracing::warn!(error = %history_error, "could not record failed chat send");
            }
            if control.interrupt.token().is_cancelled() {
                return Err(control.interrupt.error());
            }
            reconnect_chat_session(client, workflow, control, "chat transport failed").await?;
            session_reconnected = true;
            let failed_target = draft.target.clone();
            *retained_draft = Some(draft);
            emit_chat_state(
                &failed_target,
                "reconnected",
                true,
                &format!(
                    "Reconnected; delivery to {} was not confirmed. Its draft is retained, and /send deliberately submits it again.",
                    failed_target.destination
                ),
                writer,
            )?;
        }
        Err(error) => {
            if let Err(history_error) = workflow.failed(&mut outgoing).await {
                tracing::warn!(error = %history_error, "could not record failed chat send");
            }
            return Err(CliError::from(error));
        }
    }
    if session_reconnected {
        Ok(ChatLineFlow::SessionReconnected(1))
    } else {
        Ok(ChatLineFlow::Continue)
    }
}

async fn wait_for_chat_ack(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    target: &ChatTarget,
    outgoing: &mut OutgoingRecord,
    message_id: &str,
    tracking: CommandTracking,
    control: ChatSessionControl<'_>,
) -> Result<(ChatAckOutcome, Option<CliError>), CliError> {
    let firmware_timeout = Duration::from_millis(u64::from(tracking.timeout_ms));
    let timeout = firmware_timeout.min(control.ack_timeout);
    let ack_result = tokio::select! {
        result = client.wait_for_ack(tracking.ack_code, Some(timeout)) => result,
        () = control.interrupt.cancelled() => {
            client.cancel_pending_operations();
            return Err(control.interrupt.error());
        },
    };
    match ack_result {
        Ok(ack) => {
            let workflow_error = workflow
                .acknowledged(
                    outgoing,
                    message_id,
                    Some(&target.destination),
                    ack.trip_time_ms,
                    tracking.ack_code,
                )
                .await
                .err();
            Ok((ChatAckOutcome::Acknowledged, workflow_error))
        }
        Err(error) => {
            let reconnectable = reconnect_trigger(&error);
            let cli_error = CliError::from(error);
            if cli_error.status() == ExitStatus::Timeout {
                if let Err(workflow_error) = workflow
                    .timed_out(outgoing, "chat acknowledgement", message_id)
                    .await
                {
                    tracing::warn!(error = %workflow_error, "could not record timed-out chat acknowledgement");
                }
                return Ok((ChatAckOutcome::TimedOut, None));
            }

            if let Err(workflow_error) = workflow.failed(outgoing).await {
                tracing::warn!(error = %workflow_error, "could not record failed chat acknowledgement");
            }
            if !reconnectable {
                return Err(cli_error);
            }

            reconnect_chat_session(
                client,
                workflow,
                control,
                "chat acknowledgement transport failed",
            )
            .await?;
            Ok((ChatAckOutcome::Reconnected, None))
        }
    }
}

async fn poll_chat_incoming<W: Write>(
    client: &ManagedClient,
    workflow: &WorkflowServices,
    events: &mut broadcast::Receiver<Event>,
    control: ChatSessionControl<'_>,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let mut connection_active = true;
    loop {
        let message = tokio::select! {
            result = client.sync_next_message() => result.map_err(CliError::from)?,
            () = control.interrupt.cancelled() => {
                client.cancel_pending_operations();
                return Err(control.interrupt.error());
            }
        };
        let drained = message.is_none();
        loop {
            match events.try_recv() {
                Ok(Event::Message(message)) => {
                    emit_chat_incoming(workflow, message, IncomingOrigin::Live, writer).await?;
                }
                Ok(Event::Disconnected) => connection_active = false,
                Ok(Event::Connected) => connection_active = true,
                Ok(
                    Event::ProtocolError(_)
                    | Event::UnknownPacket { .. }
                    | Event::LoginFailed { .. },
                ) => {
                    workflow
                        .error("chat", "the device emitted an error event")
                        .await?;
                }
                Ok(_) => {}
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    return Err(CliError::new(
                        ExitStatus::Protocol,
                        "initial chat event buffering lagged while draining the queued inbox",
                    )
                    .with_hint("Retry after reducing the companion's queued-message backlog."));
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    return Err(CliError::new(
                        ExitStatus::Connection,
                        "the device event stream closed during chat setup",
                    ));
                }
            }
        }
        if let Some(message) = message {
            emit_chat_incoming(workflow, message, IncomingOrigin::Queue, writer).await?;
        }
        if !connection_active {
            return Err(CliError::new(
                ExitStatus::Connection,
                "the companion disconnected while chat was loading the queued inbox",
            ));
        }
        if drained {
            return Ok(());
        }
    }
}

async fn emit_chat_incoming<W: Write>(
    workflow: &WorkflowServices,
    message: Message,
    origin: IncomingOrigin,
    writer: &mut OutputWriter<W>,
) -> Result<bool, CliError> {
    let Some(message_id) = workflow.incoming(&message, origin).await? else {
        return Ok(false);
    };
    let source = message_source(&message);
    let human = format!(
        "{}: {}",
        terminal_safe(&source),
        terminal_safe(&message.text)
    );
    let record = ChatIncomingRecord {
        state: "incoming",
        source,
        text: message.text,
        message_id,
    };
    writer
        .event("chat", &record, &human)
        .map_err(CliError::from)?;
    Ok(true)
}

fn read_chat_line(input: &mut impl BufRead) -> Result<Option<String>, CliError> {
    read_bounded_line(input, "line chat input")
}

async fn send_chat_message(
    client: &ManagedClient,
    target: &ChatTarget,
    text: &str,
) -> Result<Option<CommandTracking>, CoreError> {
    if let Some(channel) = target.channel {
        client
            .send_channel_message(channel, 0, text)
            .await
            .map(|()| None)
    } else {
        client
            .send_direct_text(
                target
                    .direct_prefix
                    .as_ref()
                    .map_or(&[], |prefix| &prefix[..]),
                0,
                text,
            )
            .await
            .map(Some)
    }
}

fn emit_chat_state<W: Write>(
    target: &ChatTarget,
    state: &'static str,
    draft_retained: bool,
    human: &str,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let record = ChatRecord {
        state,
        destination: target.destination.clone(),
        draft_retained,
    };
    writer.event("chat", &record, human).map_err(CliError::from)
}

fn emit_chat_help<W: Write>(
    target: &ChatTarget,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let record = ChatHelpRecord {
        state: "help",
        destination: target.destination.clone(),
        commands: CHAT_HELP_COMMANDS,
    };
    let human = std::iter::once(format!("Chat commands (target: {}):", target.destination))
        .chain(
            CHAT_HELP_COMMANDS
                .iter()
                .map(|command| format!("{command}\t{}", chat_help_description(command))),
        )
        .collect::<Vec<_>>()
        .join("\n");
    writer
        .event("chat", &record, &human)
        .map_err(CliError::from)
}

fn chat_help_description(command: &str) -> &'static str {
    match command {
        "/help" => "show this command list",
        "/contacts [query]" => "list contacts, optionally filtered by name",
        "/to <contact>" => "change to an exact contact name or unique key prefix",
        "/channel <0..255>" => "change to a channel index",
        "/history [N]" => "show up to 100 retained messages for the current target",
        "/send" => "deliberately retry the retained failed-send draft",
        "/discard" => "discard the retained failed-send draft without transmitting it",
        "/quit" => "exit chat",
        "//text" => "send a message beginning with a literal slash",
        _ => "",
    }
}

fn emit_chat_contacts<W: Write>(
    contacts: Vec<Contact>,
    target: &ChatTarget,
    query: Option<String>,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let needle = query.as_ref().map(|value| value.to_lowercase());
    let mut contacts: Vec<_> = contacts
        .into_iter()
        .filter(|contact| {
            needle.as_ref().is_none_or(|needle| {
                contact.adv_name.to_lowercase().contains(needle)
                    || contact.public_key.to_hex().contains(needle)
            })
        })
        .map(|contact| ChatContactSummary {
            name: contact.adv_name,
            public_key_prefix: hex::encode(&contact.public_key.as_bytes()[..6]),
            kind: contact_type_name(contact.contact_type),
        })
        .collect();
    contacts.sort_by(|left, right| {
        left.name
            .to_lowercase()
            .cmp(&right.name.to_lowercase())
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.public_key_prefix.cmp(&right.public_key_prefix))
    });
    let human = if contacts.is_empty() {
        match &query {
            Some(query) => format!(
                "No contacts matched '{query}'. Current target: {}.",
                target.destination
            ),
            None => format!(
                "No contacts are available. Current target: {}.",
                target.destination
            ),
        }
    } else {
        std::iter::once(format!(
            "Contacts (current target: {}):",
            target.destination
        ))
        .chain(contacts.iter().map(|contact| {
            format!(
                "{}\t{}\t{}",
                contact.name, contact.public_key_prefix, contact.kind
            )
        }))
        .collect::<Vec<_>>()
        .join("\n")
    };
    let record = ChatContactsRecord {
        state: "contacts",
        destination: target.destination.clone(),
        query,
        contacts,
    };
    writer
        .event("chat", &record, &human)
        .map_err(CliError::from)
}

fn emit_chat_destination_changed<W: Write>(
    target: &ChatTarget,
    retained_draft: Option<&RetainedChatDraft>,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let record = ChatRecord {
        state: "destination_changed",
        destination: target.destination.clone(),
        draft_retained: retained_draft.is_some(),
    };
    let human = retained_draft.map_or_else(
        || format!("Chat target changed to {}.", target.destination),
        |draft| {
            format!(
                "Chat target changed to {}. The retained draft remains addressed to {}.",
                target.destination, draft.target.destination
            )
        },
    );
    writer
        .event("chat", &record, &human)
        .map_err(CliError::from)
}

async fn emit_chat_history<W: Write>(
    workflow: &WorkflowServices,
    target: &ChatTarget,
    limit: usize,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let loaded = workflow.load_history().await?;
    let enabled = loaded.is_some();
    let mut entries: Vec<_> = loaded
        .unwrap_or_default()
        .into_iter()
        .filter(|entry| chat_history_entry_matches(entry, target))
        .collect();
    let first = entries.len().saturating_sub(limit);
    entries = entries.split_off(first);
    let human = if !enabled {
        format!(
            "History is disabled for {} (plaintext opt-in); no entries were loaded.",
            target.destination
        )
    } else if entries.is_empty() {
        format!("No retained history for {}.", target.destination)
    } else {
        std::iter::once(format!("History for {}:", target.destination))
            .chain(entries.iter().map(|entry| {
                format!(
                    "{}\t{:?}\t{:?}\t{}",
                    entry.recorded_at_unix_ms,
                    entry.direction,
                    entry.status,
                    terminal_safe(&entry.text)
                )
            }))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let record = ChatHistoryRecord {
        state: "history",
        destination: target.destination.clone(),
        enabled,
        storage: "plaintext_opt_in",
        entries,
    };
    writer
        .event("chat", &record, &human)
        .map_err(CliError::from)
}

fn chat_history_entry_matches(entry: &HistoryEntry, target: &ChatTarget) -> bool {
    match target.channel {
        Some(channel) => entry.channel == Some(channel),
        None if entry.channel.is_none() => match entry.direction {
            HistoryDirection::Outgoing => entry.peer == target.destination,
            HistoryDirection::Incoming => target
                .direct_prefix
                .as_ref()
                .is_some_and(|prefix| entry.peer == format!("direct:{}", hex::encode(prefix))),
        },
        None => false,
    }
}

fn emit_chat_command_error<W: Write>(
    target: &ChatTarget,
    command: &str,
    message: &str,
    suggestion: Option<&str>,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let record = ChatCommandErrorRecord {
        state: "command_error",
        destination: target.destination.clone(),
        command: command.to_owned(),
        message: message.to_owned(),
        suggestion: suggestion.map(str::to_owned),
    };
    let human = suggestion.map_or_else(
        || {
            format!(
                "{command}: {message} Current target: {}.",
                target.destination
            )
        },
        |suggestion| {
            format!(
                "{command}: {message} Did you mean '{suggestion}'? Current target: {}.",
                target.destination
            )
        },
    );
    writer
        .event("chat", &record, &human)
        .map_err(CliError::from)
}

fn prompt_chat_destination() -> Result<String, CliError> {
    let mut stderr = io::stderr().lock();
    write!(stderr, "Chat destination: ")
        .and_then(|()| stderr.flush())
        .map_err(|_| CliError::new(ExitStatus::Protocol, "could not write chat prompt"))?;
    drop(stderr);
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let destination = read_bounded_line(&mut input, "chat destination")?.unwrap_or_default();
    let destination = destination.trim().to_owned();
    if destination.is_empty() {
        return Err(CliError::new(
            ExitStatus::Usage,
            "chat destination must not be empty",
        ));
    }
    Ok(destination)
}

fn completions<W: Write>(
    shell: clap_complete::Shell,
    writer: &mut OutputWriter<W>,
) -> Result<(), CliError> {
    let mut command = Cli::command();
    let mut bytes = Vec::new();
    clap_complete::generate(shell, &mut command, "meshquill", &mut bytes);
    writer.raw(&bytes).map_err(CliError::from)
}

fn manpages<W: Write>(directory: &Path, writer: &mut OutputWriter<W>) -> Result<(), CliError> {
    fs::create_dir_all(directory).map_err(artifact_io_error)?;
    let command = Cli::command();
    let mut files = Vec::new();
    write_manpage_tree(&command, "meshquill", directory, &mut files)?;
    let human = format!(
        "Generated {} man page(s) in {}",
        files.len(),
        directory.display()
    );
    let report = ArtifactReport { files };
    writer
        .result("manpages", &report, &human)
        .map_err(CliError::from)
}

fn write_manpage_tree(
    command: &clap::Command,
    page_name: &str,
    directory: &Path,
    files: &mut Vec<String>,
) -> Result<(), CliError> {
    let mut page = command
        .clone()
        .name(page_name.to_owned())
        .version(env!("CARGO_PKG_VERSION"));
    page = page.bin_name(page_name.replace('-', " "));
    let mut bytes = Vec::new();
    clap_mangen::Man::new(page)
        .render(&mut bytes)
        .map_err(artifact_io_error)?;
    let path = directory.join(format!("{page_name}.1"));
    fs::write(&path, bytes).map_err(artifact_io_error)?;
    files.push(path.display().to_string());
    for subcommand in command.get_subcommands() {
        let child_name = format!("{page_name}-{}", subcommand.get_name());
        write_manpage_tree(subcommand, &child_name, directory, files)?;
    }
    Ok(())
}

fn artifact_io_error(_error: io::Error) -> CliError {
    CliError::new(
        ExitStatus::Configuration,
        "could not create the requested local artifact",
    )
    .with_hint("Check the destination directory and its permissions.")
}

fn emit_diagnostic(cli: &Cli, message: &str) {
    if cli.quiet {
        return;
    }
    let mut stderr = io::stderr().lock();
    let _result = writeln!(stderr, "diagnostic: {message}");
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;

    use tempfile::NamedTempFile;

    use super::{
        ChannelInfoView, ContactRoute, FirmwareCompatibility, FloodScope, MAX_INNER_PAYLOAD,
        ParsedChatLine, apply_contact_update, chat_channel_target, chat_contact_target,
        chat_history_entry_matches, classify_firmware_compatibility, closest_suggestion,
        edit_distance, format_cli_error, parse_chat_line, parse_flood_scope, parse_meshcore_uri,
        read_channel_secret, resolve_channel_query, resolve_contact,
    };
    use crate::{args::ContactUpdateArgs, error::CliError, output::ExitStatus};
    use meshquill_core::{Contact, ContactType, Path, PublicKey};
    use meshquill_store::{HistoryDirection, HistoryEntry, HistoryStatus};
    use sha2::{Digest, Sha256};

    fn must<T, E: Display>(result: Result<T, E>, context: &str) -> T {
        match result {
            Ok(value) => value,
            Err(error) => panic!("{context}: {error}"),
        }
    }

    fn expect_cli_error<T>(result: Result<T, CliError>, context: &str) -> CliError {
        match result {
            Ok(_) => panic!("{context}"),
            Err(error) => error,
        }
    }

    #[test]
    fn direct_connect_call_sites_remain_audited() {
        let needle = concat!("client.", "connect()");
        assert_eq!(include_str!("runtime.rs").matches(needle).count(), 6);
        assert_eq!(include_str!("remote_cli.rs").matches(needle).count(), 0);
    }

    #[test]
    fn line_chat_parser_handles_commands_literal_slashes_and_limits() {
        assert_eq!(parse_chat_line(""), ParsedChatLine::Empty);
        assert_eq!(parse_chat_line("   "), ParsedChatLine::Empty);
        assert_eq!(
            parse_chat_line("//help"),
            ParsedChatLine::Message("/help".to_owned())
        );
        assert_eq!(parse_chat_line("/help"), ParsedChatLine::Help);
        assert_eq!(
            parse_chat_line("/contacts ALi"),
            ParsedChatLine::Contacts {
                query: Some("ALi".to_owned())
            }
        );
        assert_eq!(
            parse_chat_line("/to Alice Smith"),
            ParsedChatLine::To {
                query: "Alice Smith".to_owned()
            }
        );
        assert_eq!(
            parse_chat_line("/channel 255"),
            ParsedChatLine::Channel { channel: 255 }
        );
        assert_eq!(
            parse_chat_line("/history"),
            ParsedChatLine::History { limit: 20 }
        );
        assert_eq!(
            parse_chat_line("/history 999"),
            ParsedChatLine::History { limit: 100 }
        );
        assert_eq!(parse_chat_line("/send"), ParsedChatLine::SendRetained);
        assert_eq!(parse_chat_line("/discard"), ParsedChatLine::DiscardRetained);
        assert_eq!(parse_chat_line("/quit"), ParsedChatLine::Quit);
    }

    #[test]
    fn line_chat_parser_rejects_malformed_commands_without_turning_them_into_messages() {
        for line in [
            "/help now",
            "/to",
            "/channel 256",
            "/history 0",
            "/discard now",
        ] {
            assert!(
                matches!(parse_chat_line(line), ParsedChatLine::CommandError { .. }),
                "{line} was not rejected"
            );
        }
        assert_eq!(edit_distance("kitten", "sitting"), 3);
        assert_eq!(
            closest_suggestion("/hlep", ["/quit", "/help"]),
            Some("/help")
        );
        assert!(matches!(
            parse_chat_line("/hlep"),
            ParsedChatLine::CommandError {
                suggestion: Some(suggestion),
                ..
            } if suggestion == "/help"
        ));
    }

    #[test]
    fn contact_resolution_suggests_case_mistakes_without_selecting_them() {
        let contacts = [Contact {
            public_key: must(
                PublicKey::try_from_bytes(&[0xaa; 32]),
                "valid public key rejected",
            ),
            contact_type: ContactType::Chat,
            flags: 0,
            route: ContactRoute::Flood,
            out_path: must(Path::try_from_bytes(&[]), "empty flood path rejected"),
            adv_name: "Alice".to_owned(),
            last_advert: 0,
            adv_lat: 0.0,
            adv_lon: 0.0,
            lastmod: 0,
        }];
        let error = expect_cli_error(
            resolve_contact(&contacts, "alice"),
            "case-insensitive contact match was silently accepted",
        );
        assert_eq!(error.status(), ExitStatus::NotFound);
        assert!(error.hint().is_some_and(|hint| hint.contains("Alice")));
    }

    #[test]
    fn line_chat_history_matches_only_the_current_conversation() {
        let contact = Contact {
            public_key: must(
                PublicKey::try_from_bytes(&[0x22; 32]),
                "valid public key rejected",
            ),
            contact_type: ContactType::Chat,
            flags: 0,
            route: ContactRoute::Flood,
            out_path: must(Path::try_from_bytes(&[]), "empty flood path rejected"),
            adv_name: "Alice".to_owned(),
            last_advert: 0,
            adv_lat: 0.0,
            adv_lon: 0.0,
            lastmod: 0,
        };
        let direct = chat_contact_target(&contact);
        let channel = chat_channel_target(7);
        let outgoing = must(
            HistoryEntry::new(
                HistoryDirection::Outgoing,
                "Alice",
                None,
                "outgoing",
                HistoryStatus::Pending,
                None,
            ),
            "outgoing history entry",
        );
        let incoming = must(
            HistoryEntry::new(
                HistoryDirection::Incoming,
                "direct:222222222222",
                None,
                "incoming",
                HistoryStatus::Received,
                None,
            ),
            "incoming history entry",
        );
        let other_direct = must(
            HistoryEntry::new(
                HistoryDirection::Incoming,
                "direct:aaaaaaaaaaaa",
                None,
                "other",
                HistoryStatus::Received,
                None,
            ),
            "other direct history entry",
        );
        let channel_entry = must(
            HistoryEntry::new(
                HistoryDirection::Outgoing,
                "7",
                Some(7),
                "channel",
                HistoryStatus::Pending,
                None,
            ),
            "channel history entry",
        );

        assert!(chat_history_entry_matches(&outgoing, &direct));
        assert!(chat_history_entry_matches(&incoming, &direct));
        assert!(!chat_history_entry_matches(&other_direct, &direct));
        assert!(!chat_history_entry_matches(&channel_entry, &direct));
        assert!(chat_history_entry_matches(&channel_entry, &channel));
        assert!(!chat_history_entry_matches(&outgoing, &channel));
    }

    #[test]
    fn firmware_compatibility_classifies_documented_layout_boundaries() {
        assert_eq!(
            classify_firmware_compatibility(2),
            FirmwareCompatibility::Legacy
        );
        assert_eq!(
            classify_firmware_compatibility(3),
            FirmwareCompatibility::Known
        );
        assert_eq!(
            classify_firmware_compatibility(10),
            FirmwareCompatibility::Known
        );
        assert_eq!(
            classify_firmware_compatibility(11),
            FirmwareCompatibility::Newer
        );
    }

    #[test]
    fn doctor_check_error_detail_preserves_cli_hint() {
        let error = CliError::new(ExitStatus::Connection, "connection unavailable")
            .with_hint("Check endpoint ownership and OS permissions.");
        assert_eq!(
            format_cli_error("DEVICE_QUERY failed", &error),
            "DEVICE_QUERY failed: connection unavailable; hint: Check endpoint ownership and OS permissions."
        );
    }

    #[test]
    fn parse_flood_scope_accepts_default_and_unscoped_case_insensitive() {
        assert!(matches!(
            parse_flood_scope("default"),
            Ok((FloodScope::Default, None))
        ));
        assert!(matches!(
            parse_flood_scope("UNSCOPED"),
            Ok((FloodScope::Unscoped, None))
        ));
        assert!(matches!(
            parse_flood_scope("*"),
            Ok((FloodScope::Unscoped, None))
        ));
    }

    #[test]
    fn parse_flood_scope_trims_scope_prefix() {
        let (scope, key_name) = must(
            parse_flood_scope("#field-team"),
            "scoped scope was rejected",
        );
        assert!(matches!(scope, FloodScope::Key(_)));
        assert_eq!(key_name.as_deref(), Some("#field-team"));

        let (scope, key_name) = must(
            parse_flood_scope("#default"),
            "explicitly named default scope was rejected",
        );
        assert!(matches!(scope, FloodScope::Key(_)));
        assert_eq!(key_name.as_deref(), Some("#default"));
    }

    #[test]
    fn parse_flood_scope_rejects_control_chars_and_nul() {
        let error = expect_cli_error(
            parse_flood_scope("bad\nname"),
            "control character in scope was accepted",
        );
        assert_eq!(error.status(), ExitStatus::Usage);
        let error = expect_cli_error(
            parse_flood_scope("bad\0name"),
            "NUL character in scope was accepted",
        );
        assert_eq!(error.status(), ExitStatus::Usage);
    }

    #[test]
    fn parse_flood_scope_rejects_overlong_value() {
        let too_long = "a".repeat(31);
        let error = expect_cli_error(parse_flood_scope(&too_long), "overlong scope was accepted");
        assert_eq!(error.status(), ExitStatus::Usage);
        assert!(error.message().contains("1..=30"));
    }

    #[test]
    fn parse_flood_scope_determines_hash_deterministically() {
        let (_, first) = must(parse_flood_scope("demo"), "first parse was rejected");
        let (_, second) = must(parse_flood_scope("demo"), "second parse was rejected");
        assert_eq!(first, second);

        let digest = Sha256::digest("#demo".as_bytes());
        let mut expected = [0_u8; 16];
        expected.copy_from_slice(&digest[..16]);
        let (scope, _) = must(
            parse_flood_scope("demo"),
            "deterministic parse was rejected",
        );
        assert_eq!(scope, FloodScope::Key(expected));
    }

    #[test]
    fn parse_meshcore_uri_accepts_valid_lowercase_payload() {
        let payload = "11".repeat(98);
        let uri = format!("meshcore://{payload}");
        let card = parse_meshcore_uri(&uri)
            .unwrap_or_else(|error| panic!("valid contact URI rejected: {error}"));
        assert_eq!(card.len(), 98);
    }

    #[test]
    fn parse_meshcore_uri_rejects_bad_prefix() {
        let error = expect_cli_error(
            parse_meshcore_uri("http://11"),
            "invalid URI prefix was accepted",
        );
        assert_eq!(error.status(), ExitStatus::Usage);
        assert!(error.message().contains("meshcore://"));
    }

    #[test]
    fn parse_meshcore_uri_rejects_uppercase_payload() {
        let uri = format!("meshcore://{}", "AA".repeat(100));
        let error = expect_cli_error(
            parse_meshcore_uri(&uri),
            "uppercase URI payload was accepted",
        );
        assert_eq!(error.status(), ExitStatus::Usage);
        assert!(error.message().contains("lowercase"));
    }

    #[test]
    fn parse_meshcore_uri_rejects_short_payload() {
        let uri = "meshcore://abcd".to_owned();
        let error = expect_cli_error(
            parse_meshcore_uri(&uri),
            "undersized URI payload was accepted",
        );
        assert_eq!(error.status(), ExitStatus::Usage);
        assert!(error.message().contains("at least 98 bytes"));
    }

    #[test]
    fn parse_meshcore_uri_rejects_oversized_payload_before_decode() {
        let uri = format!("meshcore://{}", "11".repeat(MAX_INNER_PAYLOAD));
        let error = expect_cli_error(
            parse_meshcore_uri(&uri),
            "oversized URI payload was accepted",
        );
        assert_eq!(error.status(), ExitStatus::Usage);
        assert!(error.message().contains("must not exceed"));
    }

    #[test]
    fn apply_contact_update_reports_no_change() {
        let contact = Contact {
            public_key: must(
                PublicKey::try_from_bytes(&[0xaa; 32]),
                "valid public key rejected",
            ),
            contact_type: ContactType::Chat,
            flags: 0x10,
            route: ContactRoute::Flood,
            out_path: must(Path::try_from_bytes(&[]), "empty flood path rejected"),
            adv_name: "existing".to_owned(),
            last_advert: 0,
            adv_lat: 0.0,
            adv_lon: 0.0,
            lastmod: 0,
        };
        let args = ContactUpdateArgs {
            contact: "existing".to_owned(),
            name: Some("existing".to_owned()),
            favorite: Some(false),
        };
        let (_, changed) = apply_contact_update(contact, &args);
        assert!(!changed);
    }

    #[test]
    fn apply_contact_update_changes_name_and_favorite() {
        let contact = Contact {
            public_key: must(
                PublicKey::try_from_bytes(&[0xaa; 32]),
                "valid public key rejected",
            ),
            contact_type: ContactType::Chat,
            flags: 0x10,
            route: ContactRoute::Flood,
            out_path: must(Path::try_from_bytes(&[]), "empty flood path rejected"),
            adv_name: "existing".to_owned(),
            last_advert: 0,
            adv_lat: 0.0,
            adv_lon: 0.0,
            lastmod: 0,
        };
        let args = ContactUpdateArgs {
            contact: "existing".to_owned(),
            name: Some("renamed".to_owned()),
            favorite: Some(true),
        };
        let (updated, changed) = apply_contact_update(contact, &args);
        assert!(changed);
        assert_eq!(updated.adv_name, "renamed");
        assert_eq!(updated.flags, 0x11);
    }

    #[test]
    fn resolve_channel_query_prefers_exact_index_then_unique_name() {
        let channels = vec![
            ChannelInfoView {
                idx: 1,
                name: "alpha".to_owned(),
                secret_hash: None,
            },
            ChannelInfoView {
                idx: 2,
                name: "beta".to_owned(),
                secret_hash: Some(9),
            },
            ChannelInfoView {
                idx: 3,
                name: "gamma".to_owned(),
                secret_hash: None,
            },
        ];
        assert_eq!(
            must(resolve_channel_query(&channels, "2"), "channel 2 missing"),
            2
        );
        assert_eq!(
            must(
                resolve_channel_query(&channels, "beta"),
                "channel beta missing"
            ),
            2
        );
        let ambiguous = vec![
            ChannelInfoView {
                idx: 1,
                name: "alpha".to_owned(),
                secret_hash: None,
            },
            ChannelInfoView {
                idx: 2,
                name: "dup".to_owned(),
                secret_hash: None,
            },
            ChannelInfoView {
                idx: 3,
                name: "dup".to_owned(),
                secret_hash: None,
            },
        ];
        let error = expect_cli_error(
            resolve_channel_query(&ambiguous, "dup"),
            "ambiguous channel was accepted",
        );
        assert_eq!(error.status(), ExitStatus::Usage);
        let error = expect_cli_error(
            resolve_channel_query(&channels, "missing"),
            "missing channel was accepted",
        );
        assert_eq!(error.status(), ExitStatus::NotFound);
    }

    #[test]
    fn read_channel_secret_requires_16_bytes() {
        let mut path16 = NamedTempFile::new()
            .unwrap_or_else(|error| panic!("unable to create temp file: {error}"));
        std::io::Write::write_all(&mut path16, &[0u8; 16])
            .unwrap_or_else(|error| panic!("failed to write valid channel secret: {error}"));
        let value = read_channel_secret(path16.path())
            .unwrap_or_else(|error| panic!("valid secret rejected: {error}"));
        assert_eq!(value, [0u8; 16]);

        let mut path15 = NamedTempFile::new()
            .unwrap_or_else(|error| panic!("unable to create temp file: {error}"));
        std::io::Write::write_all(&mut path15, &[0u8; 15])
            .unwrap_or_else(|error| panic!("failed to write invalid secret: {error}"));
        assert!(read_channel_secret(path15.path()).is_err());
    }
}
