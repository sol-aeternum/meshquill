//! Configuration lookup, profile selection, and first-run setup.

use std::{
    collections::HashMap,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use meshquill_store::{
    Config, ConfigStore, DeviceProfile, HistoryStore, LoadOutcome, LockedConfigStore, Platform,
    ProfileSelectionError, TransportConfig, history_paths, normalized_config_path_digest,
    resolve_default_data_dir, resolve_profile, validate_identifier,
};
use meshquill_transport::{DiscoveredDevice, TransportTarget, discover_ble, discover_serial_async};
use serde::Serialize;

use crate::{
    args::{Cli, InitArgs},
    error::CliError,
    input::read_bounded_line,
    output::ExitStatus,
};

const OVERRIDE_NAMES: &[&str] = &[
    "MESHQUILL_DEFAULT_PROFILE",
    "MESHQUILL_TIMEOUT_CONNECT_MS",
    "MESHQUILL_TIMEOUT_REQUEST_MS",
    "MESHQUILL_TIMEOUT_RETRY_MS",
    "MESHQUILL_HISTORY_ENABLED",
    "MESHQUILL_HISTORY_MAX_MESSAGES",
    "MESHQUILL_HOOK_ENABLED",
    "MESHQUILL_HOOK_SCRIPT",
    "MESHQUILL_MQTT_ENABLED",
    "MESHQUILL_MQTT_BROKER",
    "MESHQUILL_MQTT_PORT",
    "MESHQUILL_MQTT_TOPIC_PREFIX",
    "MESHQUILL_QUEUES_INBOUND",
    "MESHQUILL_QUEUES_OUTBOUND",
    "MESHQUILL_QUEUES_EVENT",
];

#[derive(Debug)]
pub(crate) struct SelectedProfile {
    pub(crate) config: Config,
    pub(crate) name: String,
    pub(crate) profile: DeviceProfile,
    pub(crate) path: PathBuf,
    pub(crate) needs_migration: bool,
    pub(crate) data_dir: Option<PathBuf>,
    pub(crate) namespaced_history: bool,
}

impl SelectedProfile {
    pub(crate) fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.config.timeout.connect_timeout_ms)
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        let milliseconds = self
            .profile
            .transport_overrides
            .as_ref()
            .and_then(|overrides| overrides.request_timeout_ms)
            .unwrap_or(self.config.timeout.request_timeout_ms);
        Duration::from_millis(milliseconds)
    }

    pub(crate) fn retry_timeout(&self) -> Duration {
        Duration::from_millis(self.config.timeout.retry_timeout_ms)
    }

    pub(crate) fn history_store(&self) -> Result<HistoryStore, CliError> {
        history_store_for(
            self.data_dir.as_deref(),
            self.namespaced_history,
            &self.path,
            &self.name,
            self.config.history.max_messages,
        )
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct InitReport {
    pub(crate) profile: String,
    pub(crate) config_path: String,
    pub(crate) default: bool,
    pub(crate) transport: &'static str,
}

pub(crate) fn config_store(cli: &Cli) -> Result<ConfigStore, CliError> {
    if let Some(path) = &cli.config {
        return Ok(ConfigStore::new(path));
    }
    ConfigStore::from_default_path(current_platform(), "meshquill").map_err(CliError::from)
}

pub(crate) fn documented_overrides() -> HashMap<String, String> {
    OVERRIDE_NAMES
        .iter()
        .filter_map(|name| {
            std::env::var(name)
                .ok()
                .map(|value| ((*name).to_owned(), value))
        })
        .collect()
}

pub(crate) fn load_optional(store: &ConfigStore) -> Result<LoadOutcome, CliError> {
    store
        .load_with_overrides(&documented_overrides())
        .map_err(CliError::from)
}

pub(crate) fn load_unmodified(store: &ConfigStore) -> Result<LoadOutcome, CliError> {
    store
        .load_with_overrides(&HashMap::new())
        .map_err(CliError::from)
}

pub(crate) fn load_unmodified_locked(
    store: &LockedConfigStore<'_>,
) -> Result<LoadOutcome, CliError> {
    store
        .load_with_overrides(&HashMap::new())
        .map_err(CliError::from)
}

pub(crate) fn select_profile(cli: &Cli) -> Result<SelectedProfile, CliError> {
    let store = config_store(cli)?;
    let path = store.path().to_path_buf();
    let (config, needs_migration) = match load_optional(&store)? {
        LoadOutcome::Missing => {
            return Err(CliError::new(
                ExitStatus::Configuration,
                format!("configuration is missing at {}", path.display()),
            )
            .with_hint("Run `meshquill init` or select an existing file with --config."));
        }
        LoadOutcome::Loaded(config) => (config, false),
        LoadOutcome::NeedsMigration(config) => (config, true),
    };
    let resolved = resolve_profile(&config, cli.profile.as_deref()).map_err(selection_error)?;
    let name = resolved.name.to_owned();
    let profile = resolved.profile.clone();
    let namespaced_history = uses_namespaced_history(cli, &path)?;
    Ok(SelectedProfile {
        config,
        name,
        profile,
        path,
        needs_migration,
        data_dir: cli.data_dir.clone(),
        namespaced_history,
    })
}

fn selection_error(error: ProfileSelectionError) -> CliError {
    match error {
        ProfileSelectionError::NoneConfigured => CliError::new(
            ExitStatus::Configuration,
            "no device profiles are configured",
        )
        .with_hint("Run `meshquill init` to create a profile."),
        ProfileSelectionError::Ambiguous { profiles } => CliError::new(
            ExitStatus::Configuration,
            format!(
                "multiple device profiles are configured without a default: {}",
                profiles.join(", ")
            ),
        )
        .with_hint("Run `meshquill profiles set-default NAME` or pass --profile NAME."),
        ProfileSelectionError::NotFound { name } => CliError::new(
            ExitStatus::NotFound,
            format!("device profile '{name}' was not found"),
        )
        .with_hint("Run `meshquill profiles list` to list configured profiles."),
    }
}

pub(crate) fn history_store_for_cli(
    cli: &Cli,
    config_path: &Path,
    profile: &str,
    max_messages: u32,
) -> Result<HistoryStore, CliError> {
    history_store_for(
        cli.data_dir.as_deref(),
        uses_namespaced_history(cli, config_path)?,
        config_path,
        profile,
        max_messages,
    )
}

fn history_store_for(
    data_dir: Option<&Path>,
    explicit_config: bool,
    config_path: &Path,
    profile: &str,
    max_messages: u32,
) -> Result<HistoryStore, CliError> {
    let data_dir = match data_dir {
        Some(path) => absolute_path(path)?,
        None => resolve_default_data_dir(current_platform(), "meshquill").map_err(|_| {
            CliError::new(
                ExitStatus::Configuration,
                "the platform data directory could not be resolved",
            )
            .with_hint("Set --data-dir (or MESHQUILL_DATA_DIR) to an explicit directory.")
        })?,
    };
    let config_path = absolute_path(config_path)?;
    let paths =
        history_paths(&data_dir, &config_path, explicit_config, profile).map_err(CliError::from)?;
    HistoryStore::with_legacy(paths.canonical, paths.legacy, max_messages).map_err(CliError::from)
}

fn uses_namespaced_history(cli: &Cli, config_path: &Path) -> Result<bool, CliError> {
    if cli.config.is_none() {
        return Ok(false);
    }
    let selected = absolute_path(config_path)?;
    let default = match ConfigStore::from_default_path(current_platform(), "meshquill") {
        Ok(store) => absolute_path(store.path())?,
        Err(_) => return Ok(true),
    };
    Ok(normalized_config_path_digest(&selected) != normalized_config_path_digest(&default))
}

fn absolute_path(path: &Path) -> Result<PathBuf, CliError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    std::env::current_dir()
        .map(|directory| directory.join(path))
        .map_err(|_| {
            CliError::new(
                ExitStatus::Configuration,
                "the selected configuration path could not be normalized",
            )
        })
}

pub(crate) async fn initialize(cli: &Cli, args: &InitArgs) -> Result<InitReport, CliError> {
    let (name, transport) = collect_init_inputs(cli, args).await?;
    let store = config_store(cli)?;
    let locked = store.lock_exclusive().map_err(CliError::from)?;
    let mut config = match locked
        .load_with_overrides(&HashMap::new())
        .map_err(CliError::from)?
    {
        LoadOutcome::Missing => Config::default(),
        LoadOutcome::Loaded(config) => config,
        LoadOutcome::NeedsMigration(_) => {
            return Err(CliError::new(
                ExitStatus::Configuration,
                "configuration must be migrated before adding a profile",
            )
            .with_hint("Run `meshquill config migrate` first; it preserves a backup."));
        }
    };
    if config.device_profiles.contains_key(&name) {
        return Err(CliError::new(
            ExitStatus::Denied,
            format!("profile '{name}' already exists"),
        )
        .with_hint("Choose another name; init never overwrites an existing profile."));
    }
    let transport_name = transport_label(&transport);
    config.device_profiles.insert(
        name.clone(),
        DeviceProfile {
            transport,
            transport_overrides: None,
            secret: None,
        },
    );
    let make_default = args.set_default || config.default_profile.is_none();
    if make_default {
        config.default_profile = Some(name.clone());
    }
    locked.save(&config).map_err(CliError::from)?;
    Ok(InitReport {
        profile: name,
        config_path: locked.path().display().to_string(),
        default: make_default,
        transport: transport_name,
    })
}

async fn collect_init_inputs(
    cli: &Cli,
    args: &InitArgs,
) -> Result<(String, TransportConfig), CliError> {
    let transport_count = usize::from(args.ble.is_some())
        + usize::from(args.serial.is_some())
        + usize::from(args.tcp.is_some())
        + usize::from(args.demo);
    if transport_count > 1 {
        return Err(CliError::new(
            ExitStatus::Usage,
            "init requires exactly one transport",
        ));
    }
    if cli.non_interactive && (args.name.is_none() || transport_count != 1) {
        return Err(CliError::new(
            ExitStatus::Usage,
            "non-interactive init requires --name and exactly one transport option",
        )
        .with_hint("Choose one of --ble, --serial, --tcp, or --demo."));
    }

    let needs_prompt = args.name.is_none() || transport_count == 0;
    if needs_prompt && !io::stdin().is_terminal() {
        return Err(CliError::new(
            ExitStatus::Usage,
            "the init wizard requires an interactive terminal",
        )
        .with_hint("Pass --non-interactive, --name, and exactly one transport option."));
    }

    let name = match &args.name {
        Some(name) => name.trim().to_owned(),
        None => prompt("Profile name")?,
    };
    if name.is_empty() {
        return Err(CliError::new(
            ExitStatus::Usage,
            "profile name must not be empty",
        ));
    }
    if !validate_identifier(&name) {
        return Err(CliError::new(
            ExitStatus::Usage,
            "profile name must be at most 64 ASCII bytes, start with a letter or underscore, and contain only letters, digits, underscores, or hyphens",
        ));
    }

    let transport = if transport_count == 1 {
        transport_from_options(
            args.ble.as_deref(),
            args.serial.as_deref(),
            args.tcp.as_deref(),
            args.demo,
        )?
    } else {
        prompt_transport().await?
    };
    Ok((name, transport))
}

/// Map exactly one transport option to the reusable persisted representation.
pub(crate) fn transport_from_options(
    ble: Option<&str>,
    serial: Option<&str>,
    tcp: Option<&str>,
    demo: bool,
) -> Result<TransportConfig, CliError> {
    let count = usize::from(ble.is_some())
        + usize::from(serial.is_some())
        + usize::from(tcp.is_some())
        + usize::from(demo);
    if count != 1 {
        return Err(CliError::new(
            ExitStatus::Usage,
            "exactly one of --ble, --serial, --tcp, or --demo is required",
        ));
    }
    if let Some(id) = ble {
        return Ok(TransportConfig::Ble {
            id: required_value("BLE identifier", id)?,
            name: None,
        });
    }
    if let Some(port) = serial {
        return Ok(TransportConfig::Serial {
            port: required_value("serial port", port)?,
            baud: 115_200,
        });
    }
    if let Some(endpoint) = tcp {
        return tcp_transport(endpoint);
    }
    Ok(TransportConfig::Mock {
        scenario: "demo".to_owned(),
    })
}

async fn prompt_transport() -> Result<TransportConfig, CliError> {
    match prompt("Transport (ble/serial/tcp/demo)")?
        .to_ascii_lowercase()
        .as_str()
    {
        "ble" => prompt_discovered_target(GuidedTransport::Ble).await,
        "serial" => prompt_discovered_target(GuidedTransport::Serial).await,
        "tcp" => tcp_transport(&prompt("TCP endpoint (HOST:PORT)")?),
        "demo" => Ok(TransportConfig::Mock {
            scenario: "demo".to_owned(),
        }),
        _ => Err(CliError::new(
            ExitStatus::Usage,
            "transport must be ble, serial, tcp, or demo",
        )),
    }
}

#[derive(Clone, Copy)]
enum GuidedTransport {
    Ble,
    Serial,
}

impl GuidedTransport {
    const fn label(self) -> &'static str {
        match self {
            Self::Ble => "BLE",
            Self::Serial => "serial",
        }
    }

    const fn manual_label(self) -> &'static str {
        match self {
            Self::Ble => "BLE identifier or address",
            Self::Serial => "serial port",
        }
    }
}

async fn prompt_discovered_target(kind: GuidedTransport) -> Result<TransportConfig, CliError> {
    const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);
    let result = match kind {
        GuidedTransport::Ble => discover_ble(DISCOVERY_TIMEOUT).await,
        GuidedTransport::Serial => {
            let Ok(result) = tokio::time::timeout(DISCOVERY_TIMEOUT, discover_serial_async()).await
            else {
                let mut stderr = io::stderr().lock();
                writeln!(
                    stderr,
                    "{} discovery timed out; enter a target manually.",
                    kind.label()
                )
                .map_err(|_| {
                    CliError::new(
                        ExitStatus::Protocol,
                        "could not write discovery diagnostics",
                    )
                })?;
                return manual_discovered_target(kind);
            };
            result
        }
    };
    let mut devices = match result {
        Ok(devices) => devices,
        Err(error) => {
            let mut stderr = io::stderr().lock();
            writeln!(
                stderr,
                "{} discovery failed ({error}); enter a target manually.",
                kind.label()
            )
            .map_err(|_| {
                CliError::new(
                    ExitStatus::Protocol,
                    "could not write discovery diagnostics",
                )
            })?;
            return manual_discovered_target(kind);
        }
    };
    devices.sort_by(|left, right| left.id.cmp(&right.id));
    if devices.is_empty() {
        let mut stderr = io::stderr().lock();
        writeln!(
            stderr,
            "No {} candidates were discovered; enter a target manually.",
            kind.label()
        )
        .map_err(|_| {
            CliError::new(
                ExitStatus::Protocol,
                "could not write discovery diagnostics",
            )
        })?;
        return manual_discovered_target(kind);
    }

    {
        let mut stderr = io::stderr().lock();
        writeln!(stderr, "Discovered {} candidates:", kind.label()).map_err(|_| {
            CliError::new(ExitStatus::Protocol, "could not write discovery candidates")
        })?;
        for (index, device) in devices.iter().enumerate() {
            writeln!(
                stderr,
                "  {}. {} ({})",
                index.saturating_add(1),
                device.display_name,
                device.id
            )
            .map_err(|_| {
                CliError::new(ExitStatus::Protocol, "could not write discovery candidates")
            })?;
        }
    }
    let selection = prompt(&format!(
        "Select {} candidate number or enter {}",
        kind.label(),
        kind.manual_label()
    ))?;
    map_target_choice(kind, &devices, &selection)
}

fn manual_discovered_target(kind: GuidedTransport) -> Result<TransportConfig, CliError> {
    let selection = prompt(kind.manual_label())?;
    map_target_choice(kind, &[], &selection)
}

fn map_target_choice(
    kind: GuidedTransport,
    devices: &[DiscoveredDevice],
    selection: &str,
) -> Result<TransportConfig, CliError> {
    let selection = required_value(kind.manual_label(), selection)?;
    if !devices.is_empty()
        && let Ok(index) = selection.parse::<usize>()
    {
        if index == 0 || index > devices.len() {
            return Err(CliError::new(
                ExitStatus::Usage,
                format!("candidate number must be between 1 and {}", devices.len()),
            ));
        }
        return target_to_transport(kind, &devices[index - 1].target);
    }
    match kind {
        GuidedTransport::Ble => transport_from_options(Some(&selection), None, None, false),
        GuidedTransport::Serial => transport_from_options(None, Some(&selection), None, false),
    }
}

fn target_to_transport(
    kind: GuidedTransport,
    target: &TransportTarget,
) -> Result<TransportConfig, CliError> {
    match (kind, target) {
        (GuidedTransport::Ble, TransportTarget::Ble { selector }) => {
            transport_from_options(Some(selector), None, None, false)
        }
        (GuidedTransport::Serial, TransportTarget::Serial { port, baud }) => {
            Ok(TransportConfig::Serial {
                port: required_value("serial port", port)?,
                baud: *baud,
            })
        }
        _ => Err(CliError::new(
            ExitStatus::Protocol,
            "discovery returned a target for the wrong transport",
        )),
    }
}

fn prompt(label: &str) -> Result<String, CliError> {
    let mut stderr = io::stderr().lock();
    write!(stderr, "{label}: ")
        .and_then(|()| stderr.flush())
        .map_err(|_| CliError::new(ExitStatus::Protocol, "could not write the prompt"))?;
    drop(stderr);
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let value = read_bounded_line(&mut input, "interactive setup input")?.unwrap_or_default();
    Ok(value.trim().to_owned())
}

fn required_value(field: &'static str, value: &str) -> Result<String, CliError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CliError::new(
            ExitStatus::Usage,
            format!("{field} must not be empty"),
        ));
    }
    Ok(value.to_owned())
}

fn tcp_transport(endpoint: &str) -> Result<TransportConfig, CliError> {
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| CliError::new(ExitStatus::Usage, "TCP endpoint must use HOST:PORT form"))?;
    let host = host.trim().trim_start_matches('[').trim_end_matches(']');
    let host = required_value("TCP host", host)?;
    let port = port.parse::<u16>().map_err(|_| {
        CliError::new(
            ExitStatus::Usage,
            "TCP port must be an integer from 1 to 65535",
        )
    })?;
    if port == 0 {
        return Err(CliError::new(
            ExitStatus::Usage,
            "TCP port must be an integer from 1 to 65535",
        ));
    }
    Ok(TransportConfig::Tcp { host, port })
}

pub(crate) fn transport_label(transport: &TransportConfig) -> &'static str {
    match transport {
        TransportConfig::Ble { .. } => "ble",
        TransportConfig::Serial { .. } => "serial",
        TransportConfig::Tcp { .. } => "tcp",
        TransportConfig::Mock { .. } => "mock",
    }
}

const fn current_platform() -> Platform {
    #[cfg(target_os = "windows")]
    {
        Platform::Windows
    }
    #[cfg(target_os = "macos")]
    {
        Platform::Macos
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        Platform::Linux
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn discovered(target: TransportTarget) -> DiscoveredDevice {
        let transport = target.kind();
        DiscoveredDevice {
            id: "candidate:one".to_owned(),
            display_name: "Candidate one".to_owned(),
            transport,
            target,
            address: None,
            port: None,
            rssi: None,
            notes: Vec::new(),
        }
    }

    #[test]
    fn target_choice_maps_indexed_and_manual_reusable_targets() {
        let ble = discovered(TransportTarget::Ble {
            selector: "AA:BB:CC:DD:EE:FF".to_owned(),
        });
        assert!(matches!(
            map_target_choice(GuidedTransport::Ble, &[ble], "1"),
            Ok(TransportConfig::Ble { id, .. }) if id == "AA:BB:CC:DD:EE:FF"
        ));
        assert!(matches!(
            map_target_choice(GuidedTransport::Ble, &[], "manual-selector"),
            Ok(TransportConfig::Ble { id, .. }) if id == "manual-selector"
        ));

        let serial = discovered(TransportTarget::Serial {
            port: "/dev/ttyUSB7".to_owned(),
            baud: 9_600,
        });
        assert!(matches!(
            map_target_choice(GuidedTransport::Serial, &[serial], "1"),
            Ok(TransportConfig::Serial { port, baud })
                if port == "/dev/ttyUSB7" && baud == 9_600
        ));
        assert!(matches!(
            map_target_choice(GuidedTransport::Serial, &[], "COM7"),
            Ok(TransportConfig::Serial { port, baud }) if port == "COM7" && baud == 115_200
        ));
    }

    #[test]
    fn target_choice_rejects_bad_indices_and_cross_transport_records() {
        let serial = discovered(TransportTarget::Serial {
            port: "/dev/ttyUSB0".to_owned(),
            baud: 115_200,
        });
        assert!(
            map_target_choice(GuidedTransport::Serial, std::slice::from_ref(&serial), "2").is_err()
        );
        assert!(map_target_choice(GuidedTransport::Ble, &[serial], "1").is_err());
    }
}
