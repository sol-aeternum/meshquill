//! Configuration lookup, profile selection, and first-run setup.

use std::{
    collections::HashMap,
    io::{self, IsTerminal, Write},
    path::PathBuf,
    time::Duration,
};

use meshquill_store::{Config, ConfigStore, DeviceProfile, LoadOutcome, Platform, TransportConfig};
use serde::Serialize;

use crate::{
    args::{Cli, InitArgs},
    error::CliError,
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
    let name = cli
        .profile
        .clone()
        .or_else(|| config.default_profile.clone())
        .ok_or_else(|| {
            CliError::new(ExitStatus::Configuration, "no device profile is selected")
                .with_hint("Pass --profile NAME or set default_profile in the configuration.")
        })?;
    let profile = config.device_profiles.get(&name).cloned().ok_or_else(|| {
        CliError::new(
            ExitStatus::NotFound,
            format!("device profile '{name}' was not found"),
        )
        .with_hint("Run `meshquill config show` to list configured profiles.")
    })?;
    Ok(SelectedProfile {
        config,
        name,
        profile,
        path,
        needs_migration,
    })
}

pub(crate) fn initialize(cli: &Cli, args: &InitArgs) -> Result<InitReport, CliError> {
    let (name, transport) = collect_init_inputs(cli, args)?;
    let store = config_store(cli)?;
    let mut config = match load_unmodified(&store)? {
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
    store.save(&config).map_err(CliError::from)?;
    Ok(InitReport {
        profile: name,
        config_path: store.path().display().to_string(),
        default: make_default,
        transport: transport_name,
    })
}

fn collect_init_inputs(cli: &Cli, args: &InitArgs) -> Result<(String, TransportConfig), CliError> {
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
    if !valid_profile_name(&name) {
        return Err(CliError::new(
            ExitStatus::Usage,
            "profile name must start with a letter or underscore and contain only ASCII letters, digits, or underscores",
        ));
    }

    let transport = if let Some(id) = &args.ble {
        TransportConfig::Ble {
            id: required_value("BLE identifier", id)?,
            name: None,
        }
    } else if let Some(port) = &args.serial {
        TransportConfig::Serial {
            port: required_value("serial port", port)?,
            baud: 115_200,
        }
    } else if let Some(endpoint) = &args.tcp {
        tcp_transport(endpoint)?
    } else if args.demo {
        TransportConfig::Mock {
            scenario: "demo".to_owned(),
        }
    } else {
        prompt_transport()?
    };
    Ok((name, transport))
}

fn prompt_transport() -> Result<TransportConfig, CliError> {
    match prompt("Transport (ble/serial/tcp/demo)")?
        .to_ascii_lowercase()
        .as_str()
    {
        "ble" => Ok(TransportConfig::Ble {
            id: required_value("BLE identifier", &prompt("BLE identifier or address")?)?,
            name: None,
        }),
        "serial" => Ok(TransportConfig::Serial {
            port: required_value("serial port", &prompt("Serial port")?)?,
            baud: 115_200,
        }),
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

fn prompt(label: &str) -> Result<String, CliError> {
    let mut stderr = io::stderr().lock();
    write!(stderr, "{label}: ")
        .and_then(|()| stderr.flush())
        .map_err(|_| CliError::new(ExitStatus::Protocol, "could not write the prompt"))?;
    drop(stderr);
    let mut value = String::new();
    io::stdin()
        .read_line(&mut value)
        .map_err(|_| CliError::new(ExitStatus::Protocol, "could not read interactive input"))?;
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

fn transport_label(transport: &TransportConfig) -> &'static str {
    match transport {
        TransportConfig::Ble { .. } => "ble",
        TransportConfig::Serial { .. } => "serial",
        TransportConfig::Tcp { .. } => "tcp",
        TransportConfig::Mock { .. } => "mock",
    }
}

fn valid_profile_name(name: &str) -> bool {
    name.chars()
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
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
