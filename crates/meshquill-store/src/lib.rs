//! Persisted Meshquill configuration with migration, validation, atomic replacement,
//! and secret-safe effective rendering.

use meshquill_hooks::{EnvironmentPolicy, FailurePolicy, HookConfig, HookRuntime};
use meshquill_mqtt::{MqttConfig, MqttPassword};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::Builder as TempFileBuilder;
use thiserror::Error;
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

/// Active serialized schema version.
pub const CONFIG_VERSION: u8 = 1;

/// Config file name used by [`ConfigStore`].
pub const CONFIG_FILE_NAME: &str = "config.toml";

/// Directory containing explicitly enabled plaintext message-history files.
pub const HISTORY_DIR_NAME: &str = "history";

/// Current JSONL record version for optional local history.
pub const HISTORY_FORMAT_VERSION: u8 = 1;

/// Supported runtime platform for config path lookup.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Platform {
    /// Linux platform variant.
    Linux,
    /// macOS platform variant.
    Macos,
    /// Windows platform variant.
    Windows,
}

/// Environment payload used by [`resolve_platform_config_path`].
#[derive(Clone, Debug, Default)]
pub struct PathEnvironment {
    /// Home directory override, used for Linux/macOS fallback path detection.
    pub home: Option<PathBuf>,
    /// XDG config directory override used by Linux resolution.
    pub xdg_config_home: Option<PathBuf>,
    /// `AppData` directory override used by Windows resolution.
    pub app_data: Option<PathBuf>,
}

/// Resolve a platform config directory without reading process state.
///
/// # Errors
/// Returns [`StoreError::MissingRuntimePath`] when platform-specific data is unavailable.
pub fn resolve_platform_config_dir(
    platform: Platform,
    app_name: &str,
    env: &PathEnvironment,
) -> Result<PathBuf, StoreError> {
    match platform {
        Platform::Linux => {
            let base = if let Some(dir) = &env.xdg_config_home {
                dir.clone()
            } else {
                env.home
                    .as_ref()
                    .map(|home| home.join(".config"))
                    .ok_or_else(|| StoreError::MissingRuntimePath {
                        platform,
                        context: "missing HOME/XDG_CONFIG_HOME".to_string(),
                    })?
            };
            Ok(base.join(app_name))
        }
        Platform::Macos => env
            .home
            .as_ref()
            .map(|home| {
                home.join("Library")
                    .join("Application Support")
                    .join(app_name)
            })
            .ok_or_else(|| StoreError::MissingRuntimePath {
                platform,
                context: "missing HOME".to_string(),
            }),
        Platform::Windows => env
            .app_data
            .as_ref()
            .map(|base| base.join(app_name))
            .ok_or_else(|| StoreError::MissingRuntimePath {
                platform,
                context: "missing APPDATA/LOCALAPPDATA".to_string(),
            }),
    }
}

/// Resolve a config file path without mutating process state.
///
/// # Errors
/// Returns [`StoreError::MissingRuntimePath`] when config-directory resolution fails.
pub fn resolve_platform_config_path(
    platform: Platform,
    app_name: &str,
    env: &PathEnvironment,
) -> Result<PathBuf, StoreError> {
    Ok(resolve_platform_config_dir(platform, app_name, env)?.join(CONFIG_FILE_NAME))
}

fn current_process_env() -> PathEnvironment {
    fn env_path(key: &str) -> Option<PathBuf> {
        std::env::var_os(key).map(PathBuf::from)
    }

    PathEnvironment {
        home: env_path("HOME").or_else(|| env_path("USERPROFILE")),
        xdg_config_home: env_path("XDG_CONFIG_HOME"),
        app_data: env_path("APPDATA").or_else(|| env_path("LOCALAPPDATA")),
    }
}

/// The result of loading from disk.
#[derive(Debug)]
pub enum LoadOutcome {
    /// No config file was found.
    Missing,
    /// Current version config loaded.
    Loaded(Config),
    /// Legacy config loaded and migrated to v1.
    NeedsMigration(Config),
}

/// Result of an explicit repair, including the recoverable source backup.
#[derive(Debug)]
pub struct RepairOutcome {
    /// Fresh validated configuration written to the selected path.
    pub config: Config,
    /// Backup of the previous file, or `None` when no file existed.
    pub backup_path: Option<PathBuf>,
}

/// Load/store/repair operations for a selected config path.
pub struct ConfigStore {
    path: PathBuf,
}

impl ConfigStore {
    /// Use a caller-selected path.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Resolve to a default platform path.
    ///
    /// # Errors
    /// Returns [`StoreError::MissingRuntimePath`] when path resolution fails.
    pub fn from_default_path(platform: Platform, app_name: &str) -> Result<Self, StoreError> {
        let path = resolve_platform_config_path(platform, app_name, &current_process_env())?;
        Ok(Self::new(path))
    }

    /// Return the configured file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Load config and apply optional `MESHQUILL_*` overrides.
    ///
    /// # Errors
    /// Returns parsing, versioning, I/O, or validation errors while loading.
    pub fn load_with_overrides(
        &self,
        env_overrides: &HashMap<String, String>,
    ) -> Result<LoadOutcome, StoreError> {
        if !self.path.exists() {
            return Ok(LoadOutcome::Missing);
        }

        let mut raw = String::new();
        OpenOptions::new()
            .read(true)
            .open(&self.path)
            .and_then(|mut f| f.read_to_string(&mut raw))
            .map_err(StoreError::Io)?;

        let value: toml::Value = toml::from_str(&raw).map_err(|err| StoreError::Parse {
            path: self.path.clone(),
            message: err.to_string(),
        })?;

        let (mut config, needs_migration) =
            match value.get("version").and_then(toml::Value::as_integer) {
                Some(v) if v == i64::from(CONFIG_VERSION) => {
                    (Config::from_value(value, self.path.clone())?, false)
                }
                Some(_) => {
                    return Err(StoreError::UnsupportedVersion {
                        version: value
                            .get("version")
                            .and_then(toml::Value::as_integer)
                            .and_then(|value| u8::try_from(value).ok())
                            .unwrap_or(0),
                    });
                }
                None => (Config::from_legacy_value(value, self.path.clone())?, true),
            };

        apply_env_overrides(&mut config, env_overrides)?;
        config.validate()?;

        if needs_migration {
            return Ok(LoadOutcome::NeedsMigration(config));
        }

        Ok(LoadOutcome::Loaded(config))
    }

    /// Save config atomically with directory-local temp file + sync + rename.
    ///
    /// # Errors
    /// Returns validation, I/O, serialization, or atomic replace errors.
    pub fn save(&self, config: &Config) -> Result<(), StoreError> {
        config.validate()?;
        if let Some(parent) = self.path.parent() {
            let parent_existed = parent.exists();
            fs::create_dir_all(parent).map_err(StoreError::Io)?;
            #[cfg(unix)]
            if !parent_existed {
                fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
                    .map_err(StoreError::Io)?;
            }
        }

        let text = config.to_toml_string().map_err(|err| StoreError::Serde {
            message: err.to_string(),
        })?;

        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let mut temp = TempFileBuilder::new()
            .prefix(".meshquill-config-")
            .suffix(".tmp")
            .tempfile_in(parent)
            .map_err(StoreError::Io)?;

        #[cfg(unix)]
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(StoreError::Io)?;

        temp.write_all(text.as_bytes()).map_err(StoreError::Io)?;
        temp.flush().map_err(StoreError::Io)?;
        temp.as_file().sync_all().map_err(StoreError::Io)?;
        temp.persist(&self.path)
            .map_err(|err| StoreError::AtomicRename {
                path: self.path.clone(),
                message: err.error.to_string(),
            })?;

        #[cfg(unix)]
        OpenOptions::new()
            .read(true)
            .open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(StoreError::Io)?;

        Ok(())
    }

    /// Backup existing config and replace with defaults.
    ///
    /// # Errors
    /// Returns I/O, serialization, validation, or atomic replace errors.
    pub fn repair(&self) -> Result<RepairOutcome, StoreError> {
        let backup_path = if self.path.exists() {
            Some(self.backup()?)
        } else {
            None
        };

        let config = Config::default();
        self.save(&config)?;
        Ok(RepairOutcome {
            config,
            backup_path,
        })
    }

    /// Choose a collision-resistant backup path without creating it.
    #[must_use]
    pub fn backup_path(&self) -> PathBuf {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let file_name = self
            .path
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or(CONFIG_FILE_NAME);

        let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_nanos(),
            Err(_) => 0,
        };

        parent.join(format!("{file_name}.{now}.{}.bak", std::process::id()))
    }

    /// Create backup of the current config file.
    ///
    /// # Errors
    /// Returns I/O errors when backup creation fails.
    pub fn backup(&self) -> Result<PathBuf, StoreError> {
        if !self.path.exists() {
            return Ok(self.backup_path());
        }

        let path = self.backup_path();
        self.create_backup(&path)?;
        Ok(path)
    }

    fn create_backup(&self, path: &Path) -> Result<(), StoreError> {
        let mut source = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .map_err(StoreError::Io)?;
        let mut backup = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .map_err(StoreError::Io)?;
        #[cfg(unix)]
        backup
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(StoreError::Io)?;
        io::copy(&mut source, &mut backup).map_err(StoreError::Io)?;
        backup.flush().map_err(StoreError::Io)?;
        backup.sync_all().map_err(StoreError::Io)?;
        Ok(())
    }
}

/// Versioned settings document.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Schema version.
    pub version: u8,
    /// Optional default profile key.
    pub default_profile: Option<String>,
    /// Device profile map.
    #[serde(default)]
    pub device_profiles: BTreeMap<String, DeviceProfile>,
    /// Timeout policy.
    #[serde(default)]
    pub timeout: TimeoutSettings,
    /// History retention settings.
    #[serde(default)]
    pub history: HistorySettings,
    /// Hook policy.
    #[serde(default)]
    pub hook: HookSettings,
    /// MQTT integration settings.
    #[serde(default)]
    pub mqtt: MqttSettings,
    /// Queue sizing.
    #[serde(default)]
    pub queues: QueueSettings,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: CONFIG_VERSION,
            default_profile: None,
            device_profiles: BTreeMap::new(),
            timeout: TimeoutSettings::default(),
            history: HistorySettings::default(),
            hook: HookSettings::default(),
            mqtt: MqttSettings::default(),
            queues: QueueSettings::default(),
        }
    }
}

impl Config {
    fn from_value(value: toml::Value, path: PathBuf) -> Result<Self, StoreError> {
        toml::Value::try_into::<Config>(value).map_err(|err| StoreError::Parse {
            path,
            message: err.to_string(),
        })
    }

    fn from_legacy_value(value: toml::Value, path: PathBuf) -> Result<Self, StoreError> {
        let legacy: LegacyConfig = value.try_into().map_err(|err| StoreError::Parse {
            path,
            message: err.to_string(),
        })?;
        Ok(Config::from(legacy))
    }

    fn to_toml_string(&self) -> Result<String, toml::ser::Error> {
        let mut out = toml::to_string_pretty(self)?;
        if !out.ends_with('\n') {
            out.push('\n');
        }
        Ok(out)
    }

    /// Convert to a serializable structure with secrets redacted.
    #[must_use]
    pub fn effective_config(&self) -> EffectiveConfig {
        let mut profiles = BTreeMap::new();
        for (name, profile) in &self.device_profiles {
            profiles.insert(
                name.clone(),
                EffectiveDeviceProfile {
                    transport: profile.transport.clone(),
                    transport_overrides: profile.transport_overrides.clone(),
                    secret: redact_secret(profile.secret.as_ref()),
                },
            );
        }

        EffectiveConfig {
            version: self.version,
            default_profile: self.default_profile.clone(),
            device_profiles: profiles,
            timeout: self.timeout.clone(),
            history: self.history.clone(),
            hook: self.hook.clone(),
            mqtt: EffectiveMqttSettings {
                enabled: self.mqtt.enabled,
                gateway: self.mqtt.gateway.clone(),
                password: redact_secret(self.mqtt.password.as_ref()),
            },
            queues: self.queues.clone(),
        }
    }

    /// Serialize effective config as TOML without writing secret values.
    ///
    /// # Errors
    /// Returns [`StoreError::Serde`] on serialization failures.
    pub fn to_effective_toml(&self) -> Result<String, StoreError> {
        toml::to_string_pretty(&self.effective_config()).map_err(|err| StoreError::Serde {
            message: err.to_string(),
        })
    }

    /// Validate all configurable fields and nested validation constraints.
    ///
    /// # Errors
    /// Returns [`StoreError::Validation`] when any field is invalid.
    pub fn validate(&self) -> Result<(), StoreError> {
        for (name, profile) in &self.device_profiles {
            if !validate_identifier(name) {
                return Err(StoreError::Validation {
                    field: "device_profiles".to_string(),
                    message: format!("invalid profile name: {name}"),
                });
            }
            profile.validate()?;
        }

        if let Some(default_profile) = &self.default_profile {
            if !validate_identifier(default_profile) {
                return Err(StoreError::Validation {
                    field: "default_profile".to_string(),
                    message: "invalid profile name".to_string(),
                });
            }
            if !self.device_profiles.contains_key(default_profile) {
                return Err(StoreError::Validation {
                    field: "default_profile".to_string(),
                    message: "default profile missing from device_profiles".to_string(),
                });
            }
        }

        self.timeout.validate()?;
        self.history.validate()?;
        self.hook.validate()?;
        self.mqtt.validate()?;
        self.queues.validate()?;

        Ok(())
    }
}

fn redact_secret(secret: Option<&SecretRef>) -> Option<SecretStatus> {
    secret.map(|_| SecretStatus::redacted())
}

/// Per-profile transport and local knobs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceProfile {
    /// Transport selection.
    pub transport: TransportConfig,
    #[serde(default)]
    /// Transport-specific override values.
    pub transport_overrides: Option<TransportOverrides>,
    /// Secret location for profile-specific credential.
    #[serde(default)]
    pub secret: Option<SecretRef>,
}

impl DeviceProfile {
    fn validate(&self) -> Result<(), StoreError> {
        self.transport.validate()?;
        if let Some(secret) = &self.secret {
            secret.validate("device_profiles.secret")?;
        }
        Ok(())
    }
}

/// Transport variant for a profile.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum TransportConfig {
    /// BLE transport.
    Ble {
        /// BLE adapter identifier.
        id: String,
        #[serde(default)]
        /// Optional BLE display name.
        name: Option<String>,
    },
    /// USB serial transport.
    Serial {
        /// Device path or port path for serial transport.
        port: String,
        #[serde(default = "default_serial_baud")]
        /// Serial baud rate.
        baud: u32,
    },
    /// TCP transport.
    Tcp {
        /// Hostname or IP address.
        host: String,
        /// TCP port.
        port: u16,
    },
    /// Deterministic simulator transport.
    Mock {
        /// Simulation scenario name.
        scenario: String,
    },
}

fn default_serial_baud() -> u32 {
    115_200
}

fn default_companion_tcp_port() -> u16 {
    5_000
}

impl TransportConfig {
    fn validate(&self) -> Result<(), StoreError> {
        match self {
            TransportConfig::Ble { id, .. } => {
                if id.trim().is_empty() {
                    return Err(StoreError::Validation {
                        field: "transport.ble.id".to_string(),
                        message: "required".to_string(),
                    });
                }
            }
            TransportConfig::Serial { port, baud } => {
                if port.trim().is_empty() {
                    return Err(StoreError::Validation {
                        field: "transport.serial.port".to_string(),
                        message: "required".to_string(),
                    });
                }
                if *baud == 0 {
                    return Err(StoreError::Validation {
                        field: "transport.serial.baud".to_string(),
                        message: "must be non-zero".to_string(),
                    });
                }
            }
            TransportConfig::Tcp { host, port } => {
                if host.trim().is_empty() {
                    return Err(StoreError::Validation {
                        field: "transport.tcp.host".to_string(),
                        message: "required".to_string(),
                    });
                }
                if *port == 0 {
                    return Err(StoreError::Validation {
                        field: "transport.tcp.port".to_string(),
                        message: "must be non-zero".to_string(),
                    });
                }
            }
            TransportConfig::Mock { scenario } => {
                if scenario.trim().is_empty() {
                    return Err(StoreError::Validation {
                        field: "transport.mock.scenario".to_string(),
                        message: "required".to_string(),
                    });
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Optional transport-level overrides.
pub struct TransportOverrides {
    /// Optional override for request timeout.
    #[serde(default)]
    pub request_timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Timeout knobs used by transport and request flow.
pub struct TimeoutSettings {
    /// Timeout for initial network connection attempts.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Timeout for request/response cycles.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Timeout for retry wait/backoff behavior.
    #[serde(default = "default_retry_timeout_ms")]
    pub retry_timeout_ms: u64,
}

fn default_connect_timeout_ms() -> u64 {
    5_000
}

fn default_request_timeout_ms() -> u64 {
    3_000
}

fn default_retry_timeout_ms() -> u64 {
    1_000
}

impl Default for TimeoutSettings {
    fn default() -> Self {
        Self {
            connect_timeout_ms: default_connect_timeout_ms(),
            request_timeout_ms: default_request_timeout_ms(),
            retry_timeout_ms: default_retry_timeout_ms(),
        }
    }
}

impl TimeoutSettings {
    fn validate(&self) -> Result<(), StoreError> {
        if self.connect_timeout_ms == 0
            || self.request_timeout_ms == 0
            || self.retry_timeout_ms == 0
        {
            return Err(StoreError::Validation {
                field: "timeout".to_string(),
                message: "all timeout values must be positive".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// History retention controls for persisted message history.
pub struct HistorySettings {
    /// Whether command/response history is persisted.
    #[serde(default = "default_history_enabled")]
    pub enabled: bool,
    /// Maximum number of history entries to retain.
    #[serde(default = "default_history_max_messages")]
    pub max_messages: u32,
}

fn default_history_enabled() -> bool {
    false
}

fn default_history_max_messages() -> u32 {
    256
}

impl Default for HistorySettings {
    fn default() -> Self {
        Self {
            enabled: default_history_enabled(),
            max_messages: default_history_max_messages(),
        }
    }
}

impl HistorySettings {
    fn validate(&self) -> Result<(), StoreError> {
        if self.max_messages == 0 || self.max_messages > 100_000 {
            return Err(StoreError::Validation {
                field: "history.max_messages".to_string(),
                message: "must be between 1 and 100000".to_string(),
            });
        }
        Ok(())
    }
}

/// Direction of a persisted message-history record.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryDirection {
    /// Message received from the mesh.
    Incoming,
    /// Message submitted to the companion for transmission.
    Outgoing,
}

/// Delivery state retained for one optional local-history record.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryStatus {
    /// The companion accepted the message for transmission.
    Pending,
    /// A matching acknowledgement arrived.
    Acknowledged,
    /// The acknowledgement deadline expired.
    TimedOut,
    /// Sending failed before acknowledgement.
    Failed,
    /// The message was received from the mesh.
    Received,
}

/// One versioned plaintext JSONL entry in the opt-in local message history.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryEntry {
    /// On-disk record schema version.
    pub version: u8,
    /// Stable event identifier used to update delivery state without duplicating an entry.
    pub id: Uuid,
    /// Host time in Unix milliseconds when the entry was first recorded.
    pub recorded_at_unix_ms: u64,
    /// Incoming or outgoing direction.
    pub direction: HistoryDirection,
    /// Contact name, key prefix, or other user-visible destination/source label.
    pub peer: String,
    /// Channel index for channel messages.
    pub channel: Option<u8>,
    /// Plaintext message body. History is disabled by default because this is sensitive.
    pub text: String,
    /// Current delivery state.
    pub status: HistoryStatus,
    /// Optional non-secret companion acknowledgement correlation code.
    pub acknowledgement: Option<[u8; 4]>,
}

impl HistoryEntry {
    /// Construct a validated record with a new sortable identifier and current host time.
    ///
    /// # Errors
    /// Returns [`StoreError::Validation`] for empty, NUL-containing, or oversized fields, an
    /// invalid direction/status pairing, or a host clock before the Unix epoch.
    pub fn new(
        direction: HistoryDirection,
        peer: impl Into<String>,
        channel: Option<u8>,
        text: impl Into<String>,
        status: HistoryStatus,
        acknowledgement: Option<[u8; 4]>,
    ) -> Result<Self, StoreError> {
        let recorded_at_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| StoreError::Validation {
                field: "history.recorded_at_unix_ms".to_owned(),
                message: "host clock is before the Unix epoch".to_owned(),
            })?
            .as_millis()
            .try_into()
            .map_err(|_| StoreError::Validation {
                field: "history.recorded_at_unix_ms".to_owned(),
                message: "host timestamp is not representable".to_owned(),
            })?;
        let entry = Self {
            version: HISTORY_FORMAT_VERSION,
            id: Uuid::now_v7(),
            recorded_at_unix_ms,
            direction,
            peer: peer.into(),
            channel,
            text: text.into(),
            status,
            acknowledgement,
        };
        entry.validate()?;
        Ok(entry)
    }

    fn validate(&self) -> Result<(), StoreError> {
        if self.version != HISTORY_FORMAT_VERSION {
            return Err(StoreError::Validation {
                field: "history.version".to_owned(),
                message: "unsupported history entry version".to_owned(),
            });
        }
        validate_history_text(&self.peer, 256, "history.peer")?;
        validate_history_text(&self.text, 1_024, "history.text")?;
        let status_matches_direction = matches!(
            (self.direction, self.status),
            (HistoryDirection::Incoming, HistoryStatus::Received)
                | (
                    HistoryDirection::Outgoing,
                    HistoryStatus::Pending
                        | HistoryStatus::Acknowledged
                        | HistoryStatus::TimedOut
                        | HistoryStatus::Failed
                )
        );
        if !status_matches_direction {
            return Err(StoreError::Validation {
                field: "history.status".to_owned(),
                message: "status does not match message direction".to_owned(),
            });
        }
        if self.status == HistoryStatus::Acknowledged && self.acknowledgement.is_none() {
            return Err(StoreError::Validation {
                field: "history.acknowledgement".to_owned(),
                message: "acknowledged messages require a correlation code".to_owned(),
            });
        }
        Ok(())
    }
}

impl fmt::Debug for HistoryEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoryEntry")
            .field("version", &self.version)
            .field("id", &self.id)
            .field("recorded_at_unix_ms", &self.recorded_at_unix_ms)
            .field("direction", &self.direction)
            .field("peer", &"<redacted>")
            .field("channel", &self.channel)
            .field("text", &"<redacted>")
            .field("status", &self.status)
            .field(
                "acknowledgement",
                &self.acknowledgement.map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Atomic, bounded storage for explicitly enabled plaintext history.
pub struct HistoryStore {
    path: PathBuf,
    max_messages: usize,
}

impl HistoryStore {
    /// Build a history path beside a selected config file using a validated profile identifier.
    ///
    /// # Errors
    /// Returns [`StoreError::Validation`] for unsafe profile names or retention outside 1..=100000.
    pub fn for_config(
        config_path: &Path,
        profile: &str,
        max_messages: u32,
    ) -> Result<Self, StoreError> {
        if !validate_identifier(profile) {
            return Err(StoreError::Validation {
                field: "history.profile".to_owned(),
                message: "profile must be a safe identifier".to_owned(),
            });
        }
        let parent = config_path.parent().unwrap_or_else(|| Path::new("."));
        Self::new(
            parent
                .join(HISTORY_DIR_NAME)
                .join(format!("{profile}.jsonl")),
            max_messages,
        )
    }

    /// Use an explicit history file path and bounded retention count.
    ///
    /// # Errors
    /// Returns [`StoreError::Validation`] when retention is outside 1..=100000.
    pub fn new(path: impl Into<PathBuf>, max_messages: u32) -> Result<Self, StoreError> {
        if !(1..=100_000).contains(&max_messages) {
            return Err(StoreError::Validation {
                field: "history.max_messages".to_owned(),
                message: "must be between 1 and 100000".to_owned(),
            });
        }
        Ok(Self {
            path: path.into(),
            max_messages: usize::try_from(max_messages).map_err(|_| StoreError::Validation {
                field: "history.max_messages".to_owned(),
                message: "retention is not representable on this platform".to_owned(),
            })?,
        })
    }

    /// Return the selected plaintext JSONL path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Load all retained records with strict per-line and total-file bounds.
    ///
    /// # Errors
    /// Returns I/O, parse, or validation errors without including message contents.
    pub fn load(&self) -> Result<Vec<HistoryEntry>, StoreError> {
        const MAX_ENTRY_BYTES: u64 = 4_096;
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let maximum = u64::try_from(self.max_messages)
            .unwrap_or(u64::MAX)
            .saturating_mul(MAX_ENTRY_BYTES);
        let metadata = fs::metadata(&self.path).map_err(StoreError::Io)?;
        if metadata.len() > maximum {
            return Err(StoreError::Validation {
                field: "history.file".to_owned(),
                message: "history file exceeds its configured retention bound".to_owned(),
            });
        }
        let raw = fs::read_to_string(&self.path).map_err(StoreError::Io)?;
        let mut entries = Vec::new();
        for line in raw.lines() {
            if line.len() > usize::try_from(MAX_ENTRY_BYTES).unwrap_or(usize::MAX) {
                return Err(StoreError::Validation {
                    field: "history.entry".to_owned(),
                    message: "history entry exceeds its byte bound".to_owned(),
                });
            }
            let entry: HistoryEntry =
                serde_json::from_str(line).map_err(|error| StoreError::Parse {
                    path: self.path.clone(),
                    message: error.to_string(),
                })?;
            entry.validate()?;
            entries.push(entry);
            if entries.len() > self.max_messages {
                return Err(StoreError::Validation {
                    field: "history.file".to_owned(),
                    message: "history file contains more entries than configured".to_owned(),
                });
            }
        }
        Ok(entries)
    }

    /// Insert a record or replace the matching event identifier, retaining newest entries only.
    ///
    /// # Errors
    /// Returns validation, I/O, serialization, or atomic replacement errors.
    pub fn upsert(&self, entry: &HistoryEntry) -> Result<(), StoreError> {
        entry.validate()?;
        let mut entries = self.load()?;
        if let Some(existing) = entries.iter_mut().find(|existing| existing.id == entry.id) {
            *existing = entry.clone();
        } else {
            entries.push(entry.clone());
        }
        let excess = entries.len().saturating_sub(self.max_messages);
        if excess > 0 {
            entries.drain(..excess);
        }
        self.persist(&entries)
    }

    /// Remove the selected profile history file. Missing files are already clear.
    ///
    /// # Errors
    /// Returns an I/O error if an existing file cannot be removed.
    pub fn clear(&self) -> Result<(), StoreError> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StoreError::Io(error)),
        }
    }

    fn persist(&self, entries: &[HistoryEntry]) -> Result<(), StoreError> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(StoreError::Io)?;
        #[cfg(unix)]
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).map_err(StoreError::Io)?;
        let mut temp = TempFileBuilder::new()
            .prefix(".meshquill-history-")
            .suffix(".tmp")
            .tempfile_in(parent)
            .map_err(StoreError::Io)?;
        #[cfg(unix)]
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(StoreError::Io)?;
        for entry in entries {
            serde_json::to_writer(&mut temp, entry).map_err(|error| StoreError::Serde {
                message: error.to_string(),
            })?;
            temp.write_all(b"\n").map_err(StoreError::Io)?;
        }
        temp.flush().map_err(StoreError::Io)?;
        temp.as_file().sync_all().map_err(StoreError::Io)?;
        temp.persist(&self.path)
            .map_err(|error| StoreError::AtomicRename {
                path: self.path.clone(),
                message: error.error.to_string(),
            })?;
        #[cfg(unix)]
        OpenOptions::new()
            .read(true)
            .open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(StoreError::Io)?;
        Ok(())
    }
}

fn validate_history_text(value: &str, maximum: usize, field: &str) -> Result<(), StoreError> {
    if value.is_empty() || value.len() > maximum || value.as_bytes().contains(&0) {
        return Err(StoreError::Validation {
            field: field.to_owned(),
            message: format!("must contain 1 to {maximum} UTF-8 bytes without NUL"),
        });
    }
    Ok(())
}

/// Persisted hook failure policy.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HookFailurePolicy {
    /// Continue the application operation after a redacted hook failure.
    #[default]
    Open,
    /// Stop the application operation when the hook fails.
    Closed,
}

impl From<HookFailurePolicy> for FailurePolicy {
    fn from(value: HookFailurePolicy) -> Self {
        match value {
            HookFailurePolicy::Open => Self::Open,
            HookFailurePolicy::Closed => Self::Closed,
        }
    }
}

/// Persisted hook environment inheritance policy.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", content = "variables", rename_all = "snake_case")]
pub enum HookEnvironmentPolicy {
    /// Inherit no parent environment variables.
    Clear,
    /// Inherit only the runtime's conservative built-in allow-list.
    #[default]
    SafeInherited,
    /// Inherit exactly the named variables.
    AllowList(BTreeSet<String>),
}

impl From<HookEnvironmentPolicy> for EnvironmentPolicy {
    fn from(value: HookEnvironmentPolicy) -> Self {
        match value {
            HookEnvironmentPolicy::Clear => Self::Clear,
            HookEnvironmentPolicy::SafeInherited => Self::SafeInherited,
            HookEnvironmentPolicy::AllowList(names) => Self::AllowList(names),
        }
    }
}

/// Hook execution behavior for one trusted local Python script.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HookSettings {
    /// Whether trusted local Python hook execution is enabled.
    #[serde(default = "default_hook_enabled")]
    pub enabled: bool,
    /// Python script invoked directly by the isolated runner, never through a shell.
    #[serde(default)]
    pub script: Option<PathBuf>,
    /// Python executable invoked directly.
    #[serde(default = "default_python_executable")]
    pub python_executable: PathBuf,
    /// Complete hook operation timeout in milliseconds.
    #[serde(default = "default_hook_timeout_ms")]
    pub timeout_ms: u64,
    /// Maximum number of concurrently running hook subprocesses.
    #[serde(default = "default_hook_max_concurrency")]
    pub max_concurrency: usize,
    /// Parent environment inheritance policy.
    #[serde(default)]
    pub environment: HookEnvironmentPolicy,
    /// Failure policy for observational hooks.
    #[serde(default)]
    pub observational_failure: HookFailurePolicy,
    /// Failure policy for the mutating `before_send` hook.
    #[serde(default = "default_before_send_failure")]
    pub before_send_failure: HookFailurePolicy,
}

fn default_hook_enabled() -> bool {
    false
}

fn default_hook_timeout_ms() -> u64 {
    5_000
}

fn default_hook_max_concurrency() -> usize {
    4
}

fn default_python_executable() -> PathBuf {
    PathBuf::from("python3")
}

fn default_before_send_failure() -> HookFailurePolicy {
    HookFailurePolicy::Closed
}

impl Default for HookSettings {
    fn default() -> Self {
        Self {
            enabled: default_hook_enabled(),
            script: None,
            python_executable: default_python_executable(),
            timeout_ms: default_hook_timeout_ms(),
            max_concurrency: default_hook_max_concurrency(),
            environment: HookEnvironmentPolicy::default(),
            observational_failure: HookFailurePolicy::Open,
            before_send_failure: default_before_send_failure(),
        }
    }
}

impl HookSettings {
    /// Build the side-effect-free runtime configuration when hooks are enabled.
    ///
    /// # Errors
    /// Returns [`StoreError::Validation`] when a required path or runtime bound is invalid.
    pub fn runtime_config(&self) -> Result<Option<HookConfig>, StoreError> {
        if !self.enabled {
            return Ok(None);
        }
        let script = self.script.clone().ok_or_else(|| StoreError::Validation {
            field: "hook.script".to_string(),
            message: "required when hook.enabled is true".to_string(),
        })?;
        let mut config = HookConfig::new(script);
        config.python_executable.clone_from(&self.python_executable);
        config.timeout = std::time::Duration::from_millis(self.timeout_ms);
        config.max_concurrency = self.max_concurrency;
        config.environment = self.environment.clone().into();
        config.observational_failure = self.observational_failure.into();
        config.before_send_failure = self.before_send_failure.into();
        HookRuntime::new(config.clone()).map_err(|error| StoreError::Validation {
            field: "hook".to_string(),
            message: error.to_string(),
        })?;
        Ok(Some(config))
    }

    fn validate(&self) -> Result<(), StoreError> {
        if self.enabled && self.script.is_none() {
            return Err(StoreError::Validation {
                field: "hook.script".to_string(),
                message: "required when hook.enabled is true".to_string(),
            });
        }
        if self.python_executable.as_os_str().is_empty() {
            return Err(StoreError::Validation {
                field: "hook.python_executable".to_string(),
                message: "must not be empty".to_string(),
            });
        }
        if self.timeout_ms == 0 || self.max_concurrency == 0 {
            return Err(StoreError::Validation {
                field: "hook".to_string(),
                message: "timeout and concurrency must be positive".to_string(),
            });
        }
        if self.enabled {
            let _ = self.runtime_config()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// MQTT integration settings.
pub struct MqttSettings {
    /// Whether MQTT integration is enabled.
    #[serde(default = "default_mqtt_enabled")]
    pub enabled: bool,
    /// Complete non-secret application-gateway configuration.
    #[serde(default)]
    pub gateway: MqttConfig,
    /// Optional secret reference for password lookup.
    #[serde(default)]
    pub password: Option<SecretRef>,
}

fn default_mqtt_enabled() -> bool {
    false
}

impl Default for MqttSettings {
    fn default() -> Self {
        Self {
            enabled: default_mqtt_enabled(),
            gateway: MqttConfig::default(),
            password: None,
        }
    }
}

impl MqttSettings {
    /// Resolve an optional password reference into the MQTT crate's runtime-only secret type.
    ///
    /// This performs credential-store or environment access only when called by an operation
    /// that needs broker authentication.
    ///
    /// # Errors
    /// Returns a redacted store or MQTT validation error.
    pub fn resolve_password(
        &self,
        resolver: &impl SecretResolver,
    ) -> Result<Option<MqttPassword>, StoreError> {
        self.password
            .as_ref()
            .map(|reference| {
                let value = resolver.resolve(reference)?;
                MqttPassword::new(value.expose_secret().to_owned()).map_err(|error| {
                    StoreError::Validation {
                        field: "mqtt.password".to_string(),
                        message: error.to_string(),
                    }
                })
            })
            .transpose()
    }

    fn validate(&self) -> Result<(), StoreError> {
        self.gateway
            .validate()
            .map_err(|error| StoreError::Validation {
                field: "mqtt.gateway".to_string(),
                message: error.to_string(),
            })?;
        if let Some(password) = &self.password {
            password.validate("mqtt.password")?;
        }
        if self.gateway.username.is_some() != self.password.is_some() {
            return Err(StoreError::Validation {
                field: "mqtt.credentials".to_string(),
                message: "username and password reference must be configured together".to_string(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
/// Queue sizing controls for internal channels.
pub struct QueueSettings {
    /// Maximum inbound queue capacity.
    #[serde(default = "default_inbound_capacity")]
    pub inbound_capacity: u32,
    /// Maximum outbound queue capacity.
    #[serde(default = "default_outbound_capacity")]
    pub outbound_capacity: u32,
    /// Maximum event queue capacity.
    #[serde(default = "default_event_capacity")]
    pub event_capacity: u32,
}

fn default_inbound_capacity() -> u32 {
    64
}

fn default_outbound_capacity() -> u32 {
    64
}

fn default_event_capacity() -> u32 {
    128
}

impl Default for QueueSettings {
    fn default() -> Self {
        Self {
            inbound_capacity: default_inbound_capacity(),
            outbound_capacity: default_outbound_capacity(),
            event_capacity: default_event_capacity(),
        }
    }
}

impl QueueSettings {
    fn validate(&self) -> Result<(), StoreError> {
        let max = 1_000_000_u32;
        if self.inbound_capacity == 0
            || self.outbound_capacity == 0
            || self.event_capacity == 0
            || self.inbound_capacity > max
            || self.outbound_capacity > max
            || self.event_capacity > max
        {
            return Err(StoreError::Validation {
                field: "queues".to_string(),
                message: "invalid queue capacity".to_string(),
            });
        }
        Ok(())
    }
}

/// Secret location references.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SecretRef {
    /// Resolve via OS credential store.
    CredentialStore {
        /// Credential service name.
        service: String,
        /// Credential account name.
        account: String,
    },
    /// Resolve via environment variable.
    Environment {
        /// Environment variable name used to resolve secret material.
        name: String,
    },
    /// Resolve interactively.
    Prompt,
}

impl SecretRef {
    fn validate(&self, field: &str) -> Result<(), StoreError> {
        match self {
            Self::CredentialStore { service, account } => {
                if !validate_credential_label(service) || !validate_credential_label(account) {
                    return Err(StoreError::Validation {
                        field: field.to_string(),
                        message: "credential service and account must be non-empty, bounded, and contain no control characters"
                            .to_string(),
                    });
                }
            }
            Self::Environment { name } => {
                if !validate_env_name(name) {
                    return Err(StoreError::Validation {
                        field: field.to_string(),
                        message: format!("invalid environment variable: {name}"),
                    });
                }
            }
            Self::Prompt => {}
        }
        Ok(())
    }
}

impl fmt::Display for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CredentialStore { .. } => write!(f, "secret[credential-store]"),
            Self::Environment { name } => write!(f, "secret[env:{name}]"),
            Self::Prompt => write!(f, "secret[prompt]"),
        }
    }
}

impl fmt::Debug for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CredentialStore { .. } => {
                write!(
                    f,
                    "SecretRef::CredentialStore{{service:[redacted],account:[redacted]}}"
                )
            }
            Self::Environment { name } => write!(f, "SecretRef::Environment({name})"),
            Self::Prompt => write!(f, "SecretRef::Prompt"),
        }
    }
}

/// Runtime abstraction for resolving secret material.
pub trait SecretResolver {
    /// Resolve a reference only at the point an operation actually needs it.
    ///
    /// # Errors
    /// Returns an error when the secret cannot be resolved from the selected backend.
    fn resolve(&self, reference: &SecretRef) -> Result<SecretString, StoreError>;
}

/// Resolver backed by the platform credential store and process environment.
///
/// Native credential-store calls can block and should be invoked outside an async
/// executor worker thread. Prompt references are deliberately returned to the CLI
/// instead of reading a terminal from this persistence layer.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemSecretResolver;

impl SystemSecretResolver {
    /// Store or replace a password in the platform credential store.
    ///
    /// # Errors
    /// Returns validation or platform credential-store errors. The secret is never
    /// included in the returned error.
    pub fn set_credential(
        service: &str,
        account: &str,
        secret: &SecretString,
    ) -> Result<(), StoreError> {
        validate_credential_target(service, account)?;
        let entry = keyring::v1::Entry::new(service, account)
            .map_err(|error| credential_error("initialize", &error))?;
        entry
            .set_password(secret.expose_secret())
            .map_err(|error| credential_error("store", &error))
    }

    /// Delete a password from the platform credential store.
    ///
    /// # Errors
    /// Returns validation or platform credential-store errors.
    pub fn delete_credential(service: &str, account: &str) -> Result<(), StoreError> {
        validate_credential_target(service, account)?;
        let entry = keyring::v1::Entry::new(service, account)
            .map_err(|error| credential_error("initialize", &error))?;
        entry
            .delete_credential()
            .map_err(|error| credential_error("delete", &error))
    }
}

impl SecretResolver for SystemSecretResolver {
    fn resolve(&self, reference: &SecretRef) -> Result<SecretString, StoreError> {
        reference.validate("secret")?;
        match reference {
            SecretRef::CredentialStore { service, account } => {
                let entry = keyring::v1::Entry::new(service, account)
                    .map_err(|error| credential_error("initialize", &error))?;
                entry
                    .get_password()
                    .map(SecretString::from)
                    .map_err(|error| credential_error("read", &error))
            }
            SecretRef::Environment { name } => {
                std::env::var(name).map(SecretString::from).map_err(|_| {
                    StoreError::SecretUnavailable {
                        backend: "environment",
                        message: format!("{name} is not set or is not valid Unicode"),
                    }
                })
            }
            SecretRef::Prompt => Err(StoreError::PromptRequired),
        }
    }
}

fn validate_credential_target(service: &str, account: &str) -> Result<(), StoreError> {
    SecretRef::CredentialStore {
        service: service.to_string(),
        account: account.to_string(),
    }
    .validate("credential_store")
}

fn credential_error(operation: &'static str, error: &keyring::v1::Error) -> StoreError {
    StoreError::SecretUnavailable {
        backend: "credential_store",
        message: format!("could not {operation} credential: {error}"),
    }
}

#[derive(Clone, Debug, Serialize)]
/// Secret metadata safe for logging.
pub struct SecretStatus {
    kind: &'static str,
}

impl SecretStatus {
    fn redacted() -> Self {
        Self { kind: "redacted" }
    }
}

/// Effective config safe for logging and diagnostics.
#[derive(Clone, Debug, Serialize)]
/// Redacted effective configuration used for diagnostics and display.
pub struct EffectiveConfig {
    /// Schema version.
    pub version: u8,
    /// Selected default profile key.
    pub default_profile: Option<String>,
    /// Device profiles without secret material.
    pub device_profiles: BTreeMap<String, EffectiveDeviceProfile>,
    /// Timeout policy.
    pub timeout: TimeoutSettings,
    /// History policy.
    pub history: HistorySettings,
    /// Hook policy.
    pub hook: HookSettings,
    /// MQTT policy with secrets redacted.
    pub mqtt: EffectiveMqttSettings,
    /// Queue sizing policy.
    pub queues: QueueSettings,
}

#[derive(Clone, Debug, Serialize)]
/// Effective profile view with redacted secret metadata.
pub struct EffectiveDeviceProfile {
    /// Transport configuration.
    pub transport: TransportConfig,
    /// Transport overrides.
    pub transport_overrides: Option<TransportOverrides>,
    /// Redacted secret status.
    pub secret: Option<SecretStatus>,
}

#[derive(Clone, Debug, Serialize)]
/// Effective MQTT settings with redacted secret values.
pub struct EffectiveMqttSettings {
    /// Whether MQTT is enabled.
    pub enabled: bool,
    /// Complete non-secret gateway settings.
    pub gateway: MqttConfig,
    /// Redacted password status.
    pub password: Option<SecretStatus>,
}

#[derive(Debug, Deserialize)]
struct LegacyConfig {
    #[serde(default)]
    default_profile: Option<String>,
    #[serde(default)]
    devices: BTreeMap<String, LegacyDeviceProfile>,
}

#[derive(Debug, Deserialize)]
struct LegacyDeviceProfile {
    #[serde(flatten)]
    transport: LegacyTransport,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "transport", rename_all = "lowercase")]
enum LegacyTransport {
    Ble {
        id: String,
        name: Option<String>,
    },
    Serial {
        port: String,
        #[serde(default = "default_serial_baud")]
        baud: u32,
    },
    Tcp {
        host: String,
        #[serde(default = "default_companion_tcp_port")]
        port: u16,
    },
    Mock {
        #[serde(default)]
        scenario: String,
    },
}

impl From<LegacyConfig> for Config {
    fn from(value: LegacyConfig) -> Self {
        let mut device_profiles = BTreeMap::new();

        for (name, legacy) in value.devices {
            let transport = match legacy.transport {
                LegacyTransport::Ble { id, name } => TransportConfig::Ble { id, name },
                LegacyTransport::Serial { port, baud } => TransportConfig::Serial { port, baud },
                LegacyTransport::Tcp { host, port } => TransportConfig::Tcp { host, port },
                LegacyTransport::Mock { scenario } => TransportConfig::Mock { scenario },
            };

            device_profiles.insert(
                name,
                DeviceProfile {
                    transport,
                    transport_overrides: None,
                    secret: None,
                },
            );
        }

        let default_profile = value
            .default_profile
            .or_else(|| device_profiles.keys().next().cloned());

        Self {
            version: CONFIG_VERSION,
            default_profile,
            device_profiles,
            timeout: TimeoutSettings::default(),
            history: HistorySettings::default(),
            hook: HookSettings::default(),
            mqtt: MqttSettings::default(),
            queues: QueueSettings::default(),
        }
    }
}

fn apply_env_overrides(
    config: &mut Config,
    env: &HashMap<String, String>,
) -> Result<(), StoreError> {
    for (key, value) in env {
        if !key.starts_with("MESHQUILL_") {
            continue;
        }

        match key.as_str() {
            "MESHQUILL_DEFAULT_PROFILE" => {
                config.default_profile = Some(value.clone());
            }
            "MESHQUILL_TIMEOUT_CONNECT_MS" => {
                config.timeout.connect_timeout_ms =
                    parse_u64(value, "MESHQUILL_TIMEOUT_CONNECT_MS")?;
            }
            "MESHQUILL_TIMEOUT_REQUEST_MS" => {
                config.timeout.request_timeout_ms =
                    parse_u64(value, "MESHQUILL_TIMEOUT_REQUEST_MS")?;
            }
            "MESHQUILL_TIMEOUT_RETRY_MS" => {
                config.timeout.retry_timeout_ms = parse_u64(value, "MESHQUILL_TIMEOUT_RETRY_MS")?;
            }
            "MESHQUILL_HISTORY_ENABLED" => {
                config.history.enabled = parse_bool(value, "MESHQUILL_HISTORY_ENABLED")?;
            }
            "MESHQUILL_HISTORY_MAX_MESSAGES" => {
                config.history.max_messages = parse_u32(value, "MESHQUILL_HISTORY_MAX_MESSAGES")?;
            }
            "MESHQUILL_HOOK_ENABLED" => {
                config.hook.enabled = parse_bool(value, "MESHQUILL_HOOK_ENABLED")?;
            }
            "MESHQUILL_HOOK_SCRIPT" => {
                config.hook.script = Some(PathBuf::from(value));
            }
            "MESHQUILL_MQTT_ENABLED" => {
                config.mqtt.enabled = parse_bool(value, "MESHQUILL_MQTT_ENABLED")?;
            }
            "MESHQUILL_MQTT_BROKER" => {
                config.mqtt.gateway.host.clone_from(value);
            }
            "MESHQUILL_MQTT_PORT" => {
                config.mqtt.gateway.port = parse_u16(value, "MESHQUILL_MQTT_PORT")?;
            }
            "MESHQUILL_MQTT_TOPIC_PREFIX" => {
                config.mqtt.gateway.topic_prefix.clone_from(value);
            }
            "MESHQUILL_QUEUES_INBOUND" => {
                config.queues.inbound_capacity = parse_u32(value, "MESHQUILL_QUEUES_INBOUND")?;
            }
            "MESHQUILL_QUEUES_OUTBOUND" => {
                config.queues.outbound_capacity = parse_u32(value, "MESHQUILL_QUEUES_OUTBOUND")?;
            }
            "MESHQUILL_QUEUES_EVENT" => {
                config.queues.event_capacity = parse_u32(value, "MESHQUILL_QUEUES_EVENT")?;
            }
            _ => {}
        }
    }

    Ok(())
}

fn parse_u64(raw: &str, field: &str) -> Result<u64, StoreError> {
    raw.parse::<u64>().map_err(|_| StoreError::Validation {
        field: field.to_string(),
        message: "invalid unsigned integer".to_string(),
    })
}

fn parse_u32(raw: &str, field: &str) -> Result<u32, StoreError> {
    raw.parse::<u32>().map_err(|_| StoreError::Validation {
        field: field.to_string(),
        message: "invalid unsigned integer".to_string(),
    })
}

fn parse_u16(raw: &str, field: &str) -> Result<u16, StoreError> {
    raw.parse::<u16>().map_err(|_| StoreError::Validation {
        field: field.to_string(),
        message: "invalid port".to_string(),
    })
}

fn parse_bool(raw: &str, field: &str) -> Result<bool, StoreError> {
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(StoreError::Validation {
            field: field.to_string(),
            message: "invalid bool".to_string(),
        }),
    }
}

fn validate_identifier(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }

    let mut chars = name.chars();
    let first = chars.next();
    if !matches!(first, Some(ch) if ch.is_ascii_alphabetic() || ch == '_' || ch == '-') {
        return false;
    }

    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

fn validate_env_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 64 {
        return false;
    }

    name.chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn validate_credential_label(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 255 && !value.chars().any(char::is_control)
}

#[derive(Debug, Error)]
/// Errors produced while loading, saving, validating, or repairing configuration.
pub enum StoreError {
    /// I/O failure from filesystem operations.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Missing runtime path and platform context for path resolution.
    #[error("missing runtime path for {platform:?}: {context}")]
    MissingRuntimePath {
        /// Platform whose path resolution failed.
        platform: Platform,
        /// Failure context for troubleshooting.
        context: String,
    },

    /// Failed encoding or decoding with serde.
    #[error("serde encode/decode error: {message}")]
    Serde {
        /// Serialization error details.
        message: String,
    },

    /// Failed to parse configuration at a specific path.
    #[error("malformed config at {path:?}: {message}")]
    Parse {
        /// Input path being parsed.
        path: PathBuf,
        /// Parsing error details.
        message: String,
    },

    /// Unknown or unsupported file schema version.
    #[error("unsupported config version: {version}")]
    UnsupportedVersion {
        /// Unsupported schema version value.
        version: u8,
    },

    /// A schema or rule validation failure.
    #[error("validation error in {field}: {message}")]
    Validation {
        /// Failing field name.
        field: String,
        /// Validation message.
        message: String,
    },

    /// Secret lookup operation failed without exposing secret material.
    #[error("secret resolution failed via {backend}: {message}")]
    SecretUnavailable {
        /// Non-secret backend identifier.
        backend: &'static str,
        /// Actionable backend failure without secret material.
        message: String,
    },

    /// The caller must securely prompt for a secret.
    #[error("secret requires an interactive prompt")]
    PromptRequired,

    /// Final atomic replace step failed during save/repair.
    #[error("atomic rename failed for {path:?}: {message}")]
    AtomicRename {
        /// Output path for the attempted replacement.
        path: PathBuf,
        /// Reason for atomic rename failure.
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    fn assert_ok<T, E: std::fmt::Display>(result: Result<T, E>, message: &str) -> T {
        match result {
            Ok(value) => value,
            Err(err) => {
                panic!("{message}: {err}");
            }
        }
    }

    #[test]
    fn roundtrip_serialization() {
        let mut profiles = BTreeMap::new();
        profiles.insert(
            "desk".to_string(),
            DeviceProfile {
                transport: TransportConfig::Serial {
                    port: "/dev/ttyUSB0".to_string(),
                    baud: 115_200,
                },
                transport_overrides: None,
                secret: Some(SecretRef::Environment {
                    name: "MESHQUILL_DEVICE_SECRET".to_string(),
                }),
            },
        );

        let config = Config {
            version: CONFIG_VERSION,
            default_profile: Some("desk".to_string()),
            device_profiles: profiles,
            timeout: TimeoutSettings::default(),
            history: HistorySettings::default(),
            hook: HookSettings::default(),
            mqtt: MqttSettings::default(),
            queues: QueueSettings::default(),
        };

        let text = assert_ok(config.to_toml_string(), "serialize config");
        let parsed: Config = assert_ok(toml::from_str(&text), "parse config");
        assert_eq!(config, parsed);
    }

    #[test]
    fn malformed_recovery_writes_backup_and_defaults() {
        let dir = assert_ok(TempDir::new(), "temp dir");
        let store = ConfigStore::new(dir.path().join("config.toml"));

        {
            let mut file = assert_ok(File::create(store.path()), "create file");
            assert_ok(file.write_all(b"not valid toml"), "write malformed");
        }

        assert!(store.load_with_overrides(&HashMap::new()).is_err());
        let repaired = assert_ok(store.repair(), "repair");
        assert_eq!(repaired.config.version, CONFIG_VERSION);
        assert!(repaired.backup_path.is_some_and(|path| path.exists()));
        assert!(store.path().exists());
    }

    #[test]
    fn migration_from_legacy_version_zero() {
        let dir = assert_ok(TempDir::new(), "temp dir");
        let store = ConfigStore::new(dir.path().join("config.toml"));

        let legacy = [
            "default_profile = \"desk\"\n",
            "[devices.desk]\n",
            "transport = \"serial\"\n",
            "port = \"/dev/ttyUSB0\"\n",
            "baud = 9600\n",
        ]
        .concat();

        let mut file = assert_ok(File::create(store.path()), "create legacy");
        assert_ok(file.write_all(legacy.as_bytes()), "write legacy");

        let loaded = match store.load_with_overrides(&HashMap::new()) {
            Ok(LoadOutcome::NeedsMigration(config)) => config,
            Ok(LoadOutcome::Loaded(_)) => {
                panic!("expected migration outcome");
            }
            Ok(LoadOutcome::Missing) => {
                panic!("file was written but reported missing");
            }
            Err(err) => {
                panic!("load failed {err}");
            }
        };

        assert_eq!(loaded.version, CONFIG_VERSION);
        assert!(loaded.device_profiles.contains_key("desk"));
    }

    #[test]
    fn env_override_injection() {
        let dir = assert_ok(TempDir::new(), "temp dir");
        let store = ConfigStore::new(dir.path().join("config.toml"));

        let config = Config {
            version: CONFIG_VERSION,
            default_profile: Some("desk".to_string()),
            device_profiles: [(
                "desk".to_string(),
                DeviceProfile {
                    transport: TransportConfig::Tcp {
                        host: "localhost".to_string(),
                        port: 1883,
                    },
                    transport_overrides: None,
                    secret: None,
                },
            )]
            .into_iter()
            .collect(),
            timeout: TimeoutSettings::default(),
            history: HistorySettings::default(),
            hook: HookSettings::default(),
            mqtt: MqttSettings::default(),
            queues: QueueSettings::default(),
        };
        assert_ok(store.save(&config), "save config");

        let overrides = [
            ("MESHQUILL_MQTT_PORT".to_string(), "1889".to_string()),
            ("MESHQUILL_DEFAULT_PROFILE".to_string(), "desk".to_string()),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let loaded = match store.load_with_overrides(&overrides) {
            Ok(LoadOutcome::Loaded(config)) => config,
            Ok(_) => {
                panic!("expected loaded");
            }
            Err(err) => {
                panic!("load failed {err}");
            }
        };
        assert_eq!(loaded.mqtt.gateway.port, 1889);
    }

    #[test]
    fn validation_rejects_bad_identifiers_ports_topic_prefixes() {
        let mut bad = Config::default();
        bad.device_profiles.insert(
            "bad name".to_string(),
            DeviceProfile {
                transport: TransportConfig::Tcp {
                    host: "localhost".to_string(),
                    port: 1883,
                },
                transport_overrides: None,
                secret: None,
            },
        );
        bad.default_profile = Some("bad name".to_string());

        assert!(matches!(
            bad.validate(),
            Err(StoreError::Validation { field, .. }) if field == "device_profiles"
        ));

        let mut bad2 = Config::default();
        bad2.mqtt.gateway.topic_prefix = "invalid/#".to_string();
        assert!(matches!(
            bad2.validate(),
            Err(StoreError::Validation { .. })
        ));

        bad2.mqtt.gateway.topic_prefix = "meshquill".to_string();
        bad2.queues.inbound_capacity = 0;
        assert!(matches!(
            bad2.validate(),
            Err(StoreError::Validation { .. })
        ));
    }

    #[test]
    fn atomic_replace_and_unix_mode() {
        let dir = assert_ok(TempDir::new(), "temp dir");
        let store = ConfigStore::new(dir.path().join("config.toml"));
        assert_ok(store.save(&Config::default()), "save config");

        let mut temp_left = false;
        for entry in assert_ok(dir.path().read_dir(), "read config dir").flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.contains(".meshquill") && name.contains(".tmp") {
                temp_left = true;
            }
        }
        assert!(!temp_left);

        #[cfg(unix)]
        {
            let mode = assert_ok(
                std::fs::metadata(store.path()).map(|metadata| metadata.permissions().mode()),
                "metadata",
            );
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    struct TestResolver;

    impl SecretResolver for TestResolver {
        fn resolve(&self, reference: &SecretRef) -> Result<SecretString, StoreError> {
            match reference {
                SecretRef::Environment { name } => {
                    if name == "MESHQUILL_DEVICE_SECRET" {
                        Ok(SecretString::from("plain-text-password".to_string()))
                    } else {
                        Err(StoreError::SecretUnavailable {
                            backend: "test",
                            message: name.clone(),
                        })
                    }
                }
                SecretRef::CredentialStore { .. } => Err(StoreError::SecretUnavailable {
                    backend: "test",
                    message: "credential store unavailable".to_string(),
                }),
                SecretRef::Prompt => Ok(SecretString::from("prompt-value".to_string())),
            }
        }
    }

    #[test]
    fn redacts_effective_serialization() {
        let mut profiles = BTreeMap::new();
        profiles.insert(
            "desk".to_string(),
            DeviceProfile {
                transport: TransportConfig::Mock {
                    scenario: "default".to_string(),
                },
                transport_overrides: None,
                secret: Some(SecretRef::Environment {
                    name: "MESHQUILL_DEVICE_SECRET".to_string(),
                }),
            },
        );

        let config = Config {
            device_profiles: profiles,
            ..Config::default()
        };
        let resolver = TestResolver;
        assert!(
            resolver
                .resolve(&SecretRef::Environment {
                    name: "MESHQUILL_DEVICE_SECRET".to_string(),
                })
                .is_ok()
        );
        let effective = assert_ok(config.to_effective_toml(), "effective config");
        assert!(effective.contains("redacted"));
        assert!(!effective.contains("plain-text-password"));
        let shown = format!(
            "{:?}",
            SecretRef::CredentialStore {
                service: "svc".to_string(),
                account: "acc".to_string(),
            }
        );
        assert!(shown.contains("[redacted]"));
    }

    #[test]
    fn hook_settings_map_to_the_direct_bounded_runtime_without_a_shell() {
        let settings = HookSettings {
            enabled: true,
            script: Some(PathBuf::from("hooks/on_message.py")),
            python_executable: PathBuf::from("python-custom"),
            timeout_ms: 1_250,
            max_concurrency: 2,
            environment: HookEnvironmentPolicy::Clear,
            observational_failure: HookFailurePolicy::Closed,
            before_send_failure: HookFailurePolicy::Open,
        };

        let runtime = assert_ok(settings.runtime_config(), "map hook settings")
            .unwrap_or_else(|| panic!("enabled hook must produce runtime config"));
        assert_eq!(runtime.script, PathBuf::from("hooks/on_message.py"));
        assert_eq!(runtime.python_executable, PathBuf::from("python-custom"));
        assert_eq!(runtime.timeout, std::time::Duration::from_millis(1_250));
        assert_eq!(runtime.max_concurrency, 2);
        assert_eq!(runtime.environment, EnvironmentPolicy::Clear);
        assert_eq!(runtime.observational_failure, FailurePolicy::Closed);
        assert_eq!(runtime.before_send_failure, FailurePolicy::Open);
    }

    #[test]
    fn mqtt_persistence_keeps_password_reference_only_and_maps_runtime_secret() {
        let mut config = Config::default();
        config.mqtt.enabled = true;
        config.mqtt.gateway.username = Some("gateway-user".to_string());
        config.mqtt.password = Some(SecretRef::Environment {
            name: "MESHQUILL_DEVICE_SECRET".to_string(),
        });
        assert_ok(config.validate(), "validate MQTT settings");

        let serialized = assert_ok(config.to_toml_string(), "serialize MQTT settings");
        assert!(serialized.contains("MESHQUILL_DEVICE_SECRET"));
        assert!(!serialized.contains("plain-text-password"));

        let effective = assert_ok(config.to_effective_toml(), "render effective MQTT settings");
        assert!(effective.contains("redacted"));
        assert!(!effective.contains("MESHQUILL_DEVICE_SECRET"));
        assert!(!effective.contains("plain-text-password"));

        let password = assert_ok(
            config.mqtt.resolve_password(&TestResolver),
            "resolve MQTT password",
        )
        .unwrap_or_else(|| panic!("password reference must resolve"));
        assert_eq!(format!("{password:?}"), "MqttPassword([REDACTED])");
    }

    #[test]
    fn mqtt_username_and_password_reference_must_be_configured_together() {
        let mut config = Config::default();
        config.mqtt.gateway.username = Some("gateway-user".to_string());
        assert!(matches!(
            config.validate(),
            Err(StoreError::Validation { field, .. }) if field == "mqtt.credentials"
        ));
    }

    #[test]
    fn opt_in_history_is_atomic_bounded_private_and_state_updatable() {
        let dir = assert_ok(TempDir::new(), "temp dir");
        let config_path = dir.path().join("config.toml");
        let store = assert_ok(
            HistoryStore::for_config(&config_path, "field", 2),
            "history store",
        );
        let mut first = assert_ok(
            HistoryEntry::new(
                HistoryDirection::Incoming,
                "alice",
                None,
                "first-sensitive-message",
                HistoryStatus::Received,
                None,
            ),
            "first history entry",
        );
        first.id = Uuid::from_u128(1);
        let mut second = assert_ok(
            HistoryEntry::new(
                HistoryDirection::Outgoing,
                "bob",
                None,
                "second-sensitive-message",
                HistoryStatus::Pending,
                Some([1, 2, 3, 4]),
            ),
            "second history entry",
        );
        second.id = Uuid::from_u128(2);
        assert_ok(store.upsert(&first), "persist first entry");
        assert_ok(store.upsert(&second), "persist second entry");
        second.status = HistoryStatus::Acknowledged;
        assert_ok(store.upsert(&second), "update second entry");
        let mut third = assert_ok(
            HistoryEntry::new(
                HistoryDirection::Incoming,
                "carol",
                Some(1),
                "third-sensitive-message",
                HistoryStatus::Received,
                None,
            ),
            "third history entry",
        );
        third.id = Uuid::from_u128(3);
        assert_ok(store.upsert(&third), "persist third entry");

        let loaded = assert_ok(store.load(), "load retained history");
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].id, Uuid::from_u128(2));
        assert_eq!(loaded[0].status, HistoryStatus::Acknowledged);
        assert_eq!(loaded[1].id, Uuid::from_u128(3));
        let raw = assert_ok(fs::read_to_string(store.path()), "read plaintext history");
        assert!(raw.contains("second-sensitive-message"));
        assert!(!format!("{second:?}").contains("second-sensitive-message"));

        #[cfg(unix)]
        {
            let mode = assert_ok(
                fs::metadata(store.path()).map(|metadata| metadata.permissions().mode()),
                "history metadata",
            );
            assert_eq!(mode & 0o777, 0o600);
        }
        assert_ok(store.clear(), "clear history");
        assert!(!store.path().exists());
    }

    #[test]
    fn history_rejects_unsafe_paths_fields_and_direction_state_pairs() {
        let config_path = PathBuf::from("/tmp/config.toml");
        assert!(matches!(
            HistoryStore::for_config(&config_path, "../escape", 10),
            Err(StoreError::Validation { field, .. }) if field == "history.profile"
        ));
        assert!(HistoryStore::new("history.jsonl", 0).is_err());
        assert!(
            HistoryEntry::new(
                HistoryDirection::Incoming,
                "peer",
                None,
                "message",
                HistoryStatus::Pending,
                None,
            )
            .is_err()
        );
        assert!(
            HistoryEntry::new(
                HistoryDirection::Outgoing,
                "peer\0name",
                None,
                "message",
                HistoryStatus::Failed,
                None,
            )
            .is_err()
        );
        assert!(
            HistoryEntry::new(
                HistoryDirection::Outgoing,
                "peer",
                None,
                "message",
                HistoryStatus::Acknowledged,
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn cross_platform_path_resolution_is_pure() {
        let linux = PathEnvironment {
            home: Some(PathBuf::from("/home/me")),
            xdg_config_home: Some(PathBuf::from("/tmp/xdg")),
            app_data: None,
        };
        assert_eq!(
            assert_ok(
                resolve_platform_config_path(Platform::Linux, "meshquill", &linux),
                "linux path"
            ),
            PathBuf::from("/tmp/xdg/meshquill/config.toml")
        );

        let mac = PathEnvironment {
            home: Some(PathBuf::from("/Users/me")),
            xdg_config_home: None,
            app_data: None,
        };
        assert_eq!(
            assert_ok(
                resolve_platform_config_path(Platform::Macos, "meshquill", &mac),
                "mac path"
            ),
            PathBuf::from("/Users/me/Library/Application Support/meshquill/config.toml")
        );

        let win = PathEnvironment {
            home: Some(PathBuf::from("C:/Users/me")),
            xdg_config_home: None,
            app_data: Some(PathBuf::from("C:/Users/me/AppData/Roaming")),
        };
        assert_eq!(
            assert_ok(
                resolve_platform_config_path(Platform::Windows, "Meshquill", &win),
                "windows path"
            ),
            PathBuf::from("C:/Users/me/AppData/Roaming/Meshquill/config.toml")
        );
    }

    #[test]
    fn system_resolver_defers_prompt_to_interactive_caller() {
        let resolver = SystemSecretResolver;
        assert!(matches!(
            resolver.resolve(&SecretRef::Prompt),
            Err(StoreError::PromptRequired)
        ));
    }

    #[test]
    fn credential_targets_are_validated_before_backend_access() {
        let secret = SecretString::from("fixture-secret".to_string());
        assert!(matches!(
            SystemSecretResolver::set_credential("meshquill", "bad\naccount", &secret),
            Err(StoreError::Validation { field, .. }) if field == "credential_store"
        ));

        let mut config = Config::default();
        config.mqtt.password = Some(SecretRef::CredentialStore {
            service: " ".to_string(),
            account: "mqtt".to_string(),
        });
        assert!(matches!(
            config.validate(),
            Err(StoreError::Validation { field, .. }) if field == "mqtt.password"
        ));
    }
}
