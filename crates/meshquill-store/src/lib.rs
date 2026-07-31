//! Persisted Meshquill configuration with migration, validation, atomic replacement,
//! and secret-safe effective rendering.

use fs4::TryLockError;
use meshquill_hooks::{EnvironmentPolicy, FailurePolicy, HookConfig, HookRuntime};
use meshquill_mqtt::{MqttConfig, MqttPassword};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::Builder as TempFileBuilder;
use thiserror::Error;
use uuid::Uuid;

#[cfg(unix)]
use std::os::unix::{ffi::OsStrExt as _, fs::PermissionsExt};

#[cfg(windows)]
use std::os::windows::ffi::OsStrExt as _;

/// Active serialized schema version.
pub const CONFIG_VERSION: u8 = 1;

/// Config file name used by [`ConfigStore`].
pub const CONFIG_FILE_NAME: &str = "config.toml";

/// Largest configuration document read from disk (one MiB).
pub const MAX_CONFIG_FILE_BYTES: u64 = 1024 * 1024;

/// Largest caller-controlled operation timeout persisted in configuration (24 hours).
pub const MAX_OPERATION_TIMEOUT_MS: u64 = 24 * 60 * 60 * 1000;

/// Directory containing explicitly enabled plaintext message-history files.
pub const HISTORY_DIR_NAME: &str = "history";

/// Current JSONL record version for optional local history.
pub const HISTORY_FORMAT_VERSION: u8 = 1;

/// Largest safe profile identifier, in ASCII bytes.
pub const MAX_PROFILE_IDENTIFIER_BYTES: usize = 64;

const MAX_HISTORY_MESSAGES: usize = 100_000;
const MAX_HISTORY_ENTRY_BYTES: u64 = 8_192;
const FILE_LOCK_TIMEOUT: Duration = Duration::from_secs(10);

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
    /// XDG data directory override used by Linux data resolution.
    pub xdg_data_home: Option<PathBuf>,
    /// Roaming `AppData` directory override used by Windows config resolution.
    pub app_data: Option<PathBuf>,
    /// Local `AppData` directory override preferred by Windows data resolution.
    pub local_app_data: Option<PathBuf>,
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
    let base = match platform {
        Platform::Linux => {
            if let Some(dir) = &env.xdg_config_home {
                dir.clone()
            } else {
                env.home
                    .as_ref()
                    .map(|home| home.join(".config"))
                    .ok_or_else(|| StoreError::MissingRuntimePath {
                        platform,
                        context: "missing HOME/XDG_CONFIG_HOME".to_string(),
                    })?
            }
        }
        Platform::Macos => env
            .home
            .clone()
            .ok_or_else(|| StoreError::MissingRuntimePath {
                platform,
                context: "missing HOME".to_string(),
            })?,
        Platform::Windows => env
            .app_data
            .as_ref()
            .or(env.local_app_data.as_ref())
            .cloned()
            .ok_or_else(|| StoreError::MissingRuntimePath {
                platform,
                context: "missing APPDATA/LOCALAPPDATA".to_string(),
            })?,
    };
    let base = validated_platform_base(platform, base, "configuration")?;
    let directory = match platform {
        Platform::Macos => base.join("Library").join("Application Support"),
        Platform::Linux | Platform::Windows => base,
    };
    Ok(directory.join(app_name))
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

/// Resolve a platform data directory without reading process state.
///
/// Linux uses `XDG_DATA_HOME` or `~/.local/share`, macOS uses Application Support,
/// and Windows prefers `LOCALAPPDATA` before falling back to `APPDATA`.
///
/// # Errors
/// Returns [`StoreError::MissingRuntimePath`] when platform-specific data is unavailable.
pub fn resolve_platform_data_dir(
    platform: Platform,
    app_name: &str,
    env: &PathEnvironment,
) -> Result<PathBuf, StoreError> {
    let base = match platform {
        Platform::Linux => {
            if let Some(dir) = &env.xdg_data_home {
                dir.clone()
            } else {
                env.home
                    .as_ref()
                    .map(|home| home.join(".local").join("share"))
                    .ok_or_else(|| StoreError::MissingRuntimePath {
                        platform,
                        context: "missing HOME/XDG_DATA_HOME".to_owned(),
                    })?
            }
        }
        Platform::Macos => env
            .home
            .clone()
            .ok_or_else(|| StoreError::MissingRuntimePath {
                platform,
                context: "missing HOME".to_owned(),
            })?,
        Platform::Windows => env
            .local_app_data
            .as_ref()
            .or(env.app_data.as_ref())
            .cloned()
            .ok_or_else(|| StoreError::MissingRuntimePath {
                platform,
                context: "missing LOCALAPPDATA/APPDATA".to_owned(),
            })?,
    };
    let base = validated_platform_base(platform, base, "data")?;
    let directory = match platform {
        Platform::Macos => base.join("Library").join("Application Support"),
        Platform::Linux | Platform::Windows => base,
    };
    Ok(directory.join(app_name))
}

fn validated_platform_base(
    platform: Platform,
    base: PathBuf,
    purpose: &str,
) -> Result<PathBuf, StoreError> {
    if base.as_os_str().is_empty() || !path_is_absolute_for(platform, &base) {
        return Err(StoreError::MissingRuntimePath {
            platform,
            context: format!("{purpose} base directory must be absolute"),
        });
    }
    Ok(base)
}

fn path_is_absolute_for(platform: Platform, path: &Path) -> bool {
    let bytes = path.as_os_str().as_encoded_bytes();
    match platform {
        Platform::Linux | Platform::Macos => bytes.starts_with(b"/"),
        Platform::Windows => {
            bytes.starts_with(b"\\\\")
                || bytes.starts_with(b"//")
                || (bytes.len() >= 3
                    && bytes[0].is_ascii_alphabetic()
                    && bytes[1] == b':'
                    && matches!(bytes[2], b'/' | b'\\'))
        }
    }
}

/// Resolve the current process's default platform data directory.
///
/// # Errors
/// Returns [`StoreError::MissingRuntimePath`] when path resolution fails.
pub fn resolve_default_data_dir(platform: Platform, app_name: &str) -> Result<PathBuf, StoreError> {
    resolve_platform_data_dir(platform, app_name, &current_process_env())
}

fn current_process_env() -> PathEnvironment {
    fn env_path(key: &str) -> Option<PathBuf> {
        std::env::var_os(key)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    }

    PathEnvironment {
        home: env_path("HOME").or_else(|| env_path("USERPROFILE")),
        xdg_config_home: env_path("XDG_CONFIG_HOME"),
        xdg_data_home: env_path("XDG_DATA_HOME"),
        app_data: env_path("APPDATA"),
        local_app_data: env_path("LOCALAPPDATA"),
    }
}

#[derive(Debug)]
struct AdvisoryFileLock {
    _file: fs::File,
}

fn sidecar_lock_path(target: &Path) -> PathBuf {
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let mut name = target
        .file_name()
        .unwrap_or_else(|| OsStr::new("meshquill"))
        .to_os_string();
    name.push(".lock");
    parent.join(name)
}

fn acquire_target_locks<'a>(
    targets: impl IntoIterator<Item = &'a Path>,
) -> Result<Vec<AdvisoryFileLock>, StoreError> {
    let paths = targets
        .into_iter()
        .map(sidecar_lock_path)
        .collect::<BTreeSet<_>>();
    paths
        .into_iter()
        .map(|path| acquire_lock_path(&path))
        .collect()
}

fn acquire_lock_path(path: &Path) -> Result<AdvisoryFileLock, StoreError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    #[cfg(unix)]
    let parent_existed = parent.exists();
    fs::create_dir_all(parent).map_err(StoreError::Io)?;
    #[cfg(unix)]
    if !parent_existed {
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).map_err(StoreError::Io)?;
    }

    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(StoreError::Io)?;
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(StoreError::Io)?;

    let started = Instant::now();
    loop {
        match fs4::FileExt::try_lock(&file) {
            Ok(()) => return Ok(AdvisoryFileLock { _file: file }),
            Err(TryLockError::WouldBlock) if started.elapsed() < FILE_LOCK_TIMEOUT => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(TryLockError::WouldBlock) => {
                return Err(StoreError::LockTimeout {
                    path: path.to_path_buf(),
                });
            }
            Err(TryLockError::Error(error)) => return Err(StoreError::Io(error)),
        }
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

/// Exclusive cross-process transaction guard for one configuration file.
pub struct LockedConfigStore<'a> {
    store: &'a ConfigStore,
    _locks: Vec<AdvisoryFileLock>,
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

    /// Acquire the cross-process transaction lock for this configuration file.
    ///
    /// The sidecar lock is retained on disk so all processes synchronize on one stable inode.
    ///
    /// # Errors
    /// Returns an I/O error or [`StoreError::LockTimeout`] when the lock cannot be acquired.
    pub fn lock_exclusive(&self) -> Result<LockedConfigStore<'_>, StoreError> {
        let locks = acquire_target_locks([self.path.as_path()])?;
        Ok(LockedConfigStore {
            store: self,
            _locks: locks,
        })
    }

    /// Load config and apply optional `MESHQUILL_*` overrides.
    ///
    /// # Errors
    /// Returns parsing, versioning, I/O, or validation errors while loading.
    pub fn load_with_overrides(
        &self,
        env_overrides: &HashMap<String, String>,
    ) -> Result<LoadOutcome, StoreError> {
        self.load_unlocked(env_overrides)
    }

    fn load_unlocked(
        &self,
        env_overrides: &HashMap<String, String>,
    ) -> Result<LoadOutcome, StoreError> {
        if !self.path.exists() {
            return Ok(LoadOutcome::Missing);
        }

        let file = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .map_err(StoreError::Io)?;
        let mut raw = String::new();
        file.take(MAX_CONFIG_FILE_BYTES.saturating_add(1))
            .read_to_string(&mut raw)
            .map_err(StoreError::Io)?;
        if u64::try_from(raw.len()).unwrap_or(u64::MAX) > MAX_CONFIG_FILE_BYTES {
            return Err(StoreError::Validation {
                field: "config.file".to_owned(),
                message: format!(
                    "configuration exceeds the {MAX_CONFIG_FILE_BYTES}-byte input bound"
                ),
            });
        }

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
        self.lock_exclusive()?.save(config)
    }

    fn save_unlocked(&self, config: &Config) -> Result<(), StoreError> {
        config.validate()?;
        if let Some(parent) = self.path.parent() {
            #[cfg(unix)]
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
        self.lock_exclusive()?.repair()
    }

    fn repair_unlocked(&self) -> Result<RepairOutcome, StoreError> {
        let backup_path = if self.path.exists() {
            Some(self.backup_unlocked()?)
        } else {
            None
        };

        let config = Config::default();
        self.save_unlocked(&config)?;
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
        self.lock_exclusive()?.backup()
    }

    fn backup_unlocked(&self) -> Result<PathBuf, StoreError> {
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

impl LockedConfigStore<'_> {
    /// Return the locked configuration path.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.store.path()
    }

    /// Load the configuration while retaining the transaction lock.
    ///
    /// # Errors
    /// Returns parsing, versioning, I/O, or validation errors while loading.
    pub fn load_with_overrides(
        &self,
        env_overrides: &HashMap<String, String>,
    ) -> Result<LoadOutcome, StoreError> {
        self.store.load_unlocked(env_overrides)
    }

    /// Save the configuration while retaining the transaction lock.
    ///
    /// # Errors
    /// Returns validation, I/O, serialization, or atomic replace errors.
    pub fn save(&self, config: &Config) -> Result<(), StoreError> {
        self.store.save_unlocked(config)
    }

    /// Create a backup while retaining the transaction lock.
    ///
    /// # Errors
    /// Returns I/O errors when backup creation fails.
    pub fn backup(&self) -> Result<PathBuf, StoreError> {
        self.store.backup_unlocked()
    }

    /// Repair the configuration while retaining the transaction lock.
    ///
    /// # Errors
    /// Returns I/O, serialization, validation, or atomic replace errors.
    pub fn repair(&self) -> Result<RepairOutcome, StoreError> {
        self.store.repair_unlocked()
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

/// A stored device profile selected by explicit name, configured default, or sole entry.
#[derive(Clone, Copy, Debug)]
pub struct ResolvedProfile<'a> {
    /// Exact key from [`Config::device_profiles`].
    pub name: &'a str,
    /// Profile stored under [`Self::name`].
    pub profile: &'a DeviceProfile,
}

/// Typed profile-selection failures shared by native and Python clients.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProfileSelectionError {
    /// No device profiles are configured.
    #[error("no device profiles are configured")]
    NoneConfigured,
    /// More than one profile exists and no default or explicit name selected one.
    #[error("multiple device profiles are configured but no default profile is selected")]
    Ambiguous {
        /// Configured profile names in deterministic order.
        profiles: Vec<String>,
    },
    /// An explicit profile name was not present.
    #[error("device profile '{name}' was not found")]
    NotFound {
        /// Requested profile name.
        name: String,
    },
}

/// Resolve a device profile in strict precedence order: explicit, default, then sole profile.
///
/// # Errors
/// Returns a typed [`ProfileSelectionError`] when no profile exists, an explicit profile is
/// missing, or multiple profiles exist without a default.
pub fn resolve_profile<'a>(
    config: &'a Config,
    explicit: Option<&str>,
) -> Result<ResolvedProfile<'a>, ProfileSelectionError> {
    if let Some(name) = explicit {
        return config
            .device_profiles
            .get_key_value(name)
            .map(|(name, profile)| ResolvedProfile { name, profile })
            .ok_or_else(|| ProfileSelectionError::NotFound {
                name: name.to_owned(),
            });
    }

    if let Some(name) = &config.default_profile {
        return config
            .device_profiles
            .get_key_value(name)
            .map(|(name, profile)| ResolvedProfile { name, profile })
            .ok_or_else(|| ProfileSelectionError::NotFound { name: name.clone() });
    }

    match config.device_profiles.len() {
        0 => Err(ProfileSelectionError::NoneConfigured),
        1 => {
            let Some((name, profile)) = config.device_profiles.first_key_value() else {
                return Err(ProfileSelectionError::NoneConfigured);
            };
            Ok(ResolvedProfile { name, profile })
        }
        _ => Err(ProfileSelectionError::Ambiguous {
            profiles: config.device_profiles.keys().cloned().collect(),
        }),
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
        if let Some(overrides) = &self.transport_overrides {
            overrides.validate()?;
        }
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

impl TransportOverrides {
    fn validate(&self) -> Result<(), StoreError> {
        if self
            .request_timeout_ms
            .is_some_and(|value| !(1..=MAX_OPERATION_TIMEOUT_MS).contains(&value))
        {
            return Err(StoreError::Validation {
                field: "device_profiles.transport_overrides.request_timeout_ms".to_owned(),
                message: "must be between 1 and 86400000".to_owned(),
            });
        }
        Ok(())
    }
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
        if !(1..=MAX_OPERATION_TIMEOUT_MS).contains(&self.connect_timeout_ms)
            || !(1..=MAX_OPERATION_TIMEOUT_MS).contains(&self.request_timeout_ms)
            || !(1..=MAX_OPERATION_TIMEOUT_MS).contains(&self.retry_timeout_ms)
        {
            return Err(StoreError::Validation {
                field: "timeout".to_string(),
                message: "all timeout values must be between 1 and 86400000 milliseconds"
                    .to_string(),
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

/// Local direction of a persisted message-history record.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryDirection {
    /// Incoming message observation recorded locally.
    Incoming,
    /// Local outgoing attempt.
    Outgoing,
}

/// Local workflow state retained for one optional history record, not wire-delivery truth.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryStatus {
    /// No terminal local result has been recorded for the outgoing attempt.
    Pending,
    /// A matching acknowledgement was observed locally.
    Acknowledged,
    /// The local acknowledgement deadline expired.
    TimedOut,
    /// A local failure occurred; the wire outcome may be ambiguous.
    Failed,
    /// An incoming message observation was recorded locally.
    Received,
}

/// One versioned plaintext JSONL entry in the opt-in local message history.
///
/// Its timestamp is local-host record time. Sender timestamp, route, SNR, and signature are not
/// retained.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryEntry {
    /// On-disk record schema version.
    pub version: u8,
    /// Stable local record ID used for status updates, not protocol or event identity.
    pub id: Uuid,
    /// Local-host time in Unix milliseconds when the entry was first recorded.
    pub recorded_at_unix_ms: u64,
    /// Incoming or outgoing direction.
    pub direction: HistoryDirection,
    /// Contact name, key prefix, or other user-visible destination/source label.
    pub peer: String,
    /// Channel index for channel messages.
    pub channel: Option<u8>,
    /// Plaintext message body. History is disabled by default because this is sensitive.
    pub text: String,
    /// Current local workflow state, not wire-delivery truth.
    pub status: HistoryStatus,
    /// Optional non-secret companion acknowledgement correlation code.
    pub acknowledgement: Option<[u8; 4]>,
}

impl HistoryEntry {
    /// Construct a validated record with a new sortable local ID and current local-host time.
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

/// Canonical and adjacent-legacy paths for one profile's plaintext history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryPaths {
    /// Canonical path under the selected data root.
    pub canonical: PathBuf,
    /// Previous config-adjacent path retained for one-way reconciliation.
    pub legacy: PathBuf,
}

/// Return a deterministic SHA-256 digest of a lexically normalized config path.
///
/// This is a namespace key, not a security boundary. Relative paths remain relative so callers
/// that need working-directory independence should supply an absolute path.
#[must_use]
pub fn normalized_config_path_digest(config_path: &Path) -> String {
    use std::path::Component;

    let mut prefix: Option<OsString> = None;
    let mut rooted = false;
    let mut parts: Vec<OsString> = Vec::new();
    for component in config_path.components() {
        match component {
            Component::Prefix(value) => {
                prefix = Some(value.as_os_str().to_os_string());
            }
            Component::RootDir => rooted = true,
            Component::CurDir => {}
            Component::ParentDir => {
                if parts
                    .last()
                    .is_some_and(|part| part.as_os_str() != OsStr::new(".."))
                {
                    parts.pop();
                } else if !rooted {
                    parts.push(OsString::from(".."));
                }
            }
            Component::Normal(value) => parts.push(value.to_os_string()),
        }
    }

    let mut hasher = Sha256::new();
    if let Some(prefix) = prefix {
        hasher.update(b"prefix\0");
        hash_os_str(&mut hasher, &prefix);
        hasher.update(b"\0");
    }
    hasher.update(if rooted {
        b"root\0".as_slice()
    } else {
        b"relative\0".as_slice()
    });
    for part in parts {
        hash_os_str(&mut hasher, &part);
    }
    format!("{:x}", hasher.finalize())
}

fn hash_os_str(hasher: &mut Sha256, value: &OsStr) {
    #[cfg(unix)]
    {
        let bytes = value.as_bytes();
        hasher.update(b"unix\0");
        hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(bytes);
    }

    #[cfg(windows)]
    {
        let units = value.encode_wide().collect::<Vec<_>>();
        hasher.update(b"windows\0");
        hasher.update(u64::try_from(units.len()).unwrap_or(u64::MAX).to_le_bytes());
        for unit in units {
            hasher.update(unit.to_le_bytes());
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let bytes = value.as_encoded_bytes();
        hasher.update(b"platform\0");
        hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(bytes);
    }
}

/// Construct canonical data-root and adjacent legacy history paths.
///
/// An explicitly selected config receives a digest namespace so unrelated config files cannot
/// collide. The platform-default config stores history directly below the application's history
/// directory.
///
/// # Errors
/// Returns [`StoreError::Validation`] when `profile` is not a safe identifier.
pub fn history_paths(
    data_dir: &Path,
    config_path: &Path,
    explicit_config: bool,
    profile: &str,
) -> Result<HistoryPaths, StoreError> {
    if !validate_identifier(profile) {
        return Err(StoreError::Validation {
            field: "history.profile".to_owned(),
            message: "profile must be a safe identifier".to_owned(),
        });
    }
    let canonical_parent = if explicit_config {
        data_dir
            .join(HISTORY_DIR_NAME)
            .join(normalized_config_path_digest(config_path))
    } else {
        data_dir.join(HISTORY_DIR_NAME)
    };
    let legacy_parent = config_path.parent().unwrap_or_else(|| Path::new("."));
    Ok(HistoryPaths {
        canonical: canonical_parent.join(format!("{profile}.jsonl")),
        legacy: legacy_parent
            .join(HISTORY_DIR_NAME)
            .join(format!("{profile}.jsonl")),
    })
}

/// Atomic, bounded storage for explicitly enabled plaintext history.
pub struct HistoryStore {
    path: PathBuf,
    legacy_path: Option<PathBuf>,
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
        Self::with_optional_legacy(path.into(), None, max_messages)
    }

    /// Use canonical and adjacent-legacy paths with bounded, one-way reconciliation.
    ///
    /// # Errors
    /// Returns [`StoreError::Validation`] when retention is outside 1..=100000.
    pub fn with_legacy(
        canonical_path: impl Into<PathBuf>,
        legacy_path: impl Into<PathBuf>,
        max_messages: u32,
    ) -> Result<Self, StoreError> {
        let canonical_path = canonical_path.into();
        let legacy_path = legacy_path.into();
        let legacy_path = (legacy_path != canonical_path).then_some(legacy_path);
        Self::with_optional_legacy(canonical_path, legacy_path, max_messages)
    }

    fn with_optional_legacy(
        path: PathBuf,
        legacy_path: Option<PathBuf>,
        max_messages: u32,
    ) -> Result<Self, StoreError> {
        if !(1..=100_000).contains(&max_messages) {
            return Err(StoreError::Validation {
                field: "history.max_messages".to_owned(),
                message: "must be between 1 and 100000".to_owned(),
            });
        }
        Ok(Self {
            path,
            legacy_path,
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

    /// Return the adjacent legacy path, when it differs from the canonical path.
    #[must_use]
    pub fn legacy_path(&self) -> Option<&Path> {
        self.legacy_path.as_deref()
    }

    /// Load all retained records with strict per-line and total-file bounds.
    ///
    /// When an adjacent legacy file exists, both files are read within their independent bounds,
    /// duplicate UUIDs are resolved in favor of the canonical file, and the canonical replacement
    /// is made durable before the legacy file is removed.
    ///
    /// # Errors
    /// Returns I/O, parse, or validation errors without including message contents.
    pub fn load(&self) -> Result<Vec<HistoryEntry>, StoreError> {
        let _locks = acquire_target_locks(self.lock_targets())?;
        self.load_unlocked()
    }

    fn load_unlocked(&self) -> Result<Vec<HistoryEntry>, StoreError> {
        let canonical = Self::load_path(&self.path)?;
        let canonical_len = canonical.len();
        let Some(legacy_path) = self
            .legacy_path
            .as_ref()
            .filter(|legacy_path| legacy_path.exists())
        else {
            let entries = self.merge_entries(Vec::new(), canonical);
            if entries.len() != canonical_len {
                self.persist(&entries)?;
            }
            return Ok(entries);
        };
        let legacy = Self::load_path(legacy_path)?;
        let entries = self.merge_entries(legacy, canonical);
        self.persist(&entries)?;
        remove_file_if_exists(legacy_path)?;
        Ok(entries)
    }

    fn load_path(path: &Path) -> Result<Vec<HistoryEntry>, StoreError> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let maximum = u64::try_from(MAX_HISTORY_MESSAGES)
            .unwrap_or(u64::MAX)
            .saturating_mul(MAX_HISTORY_ENTRY_BYTES.saturating_add(2));
        let metadata = fs::metadata(path).map_err(StoreError::Io)?;
        if metadata.len() > maximum {
            return Err(StoreError::Validation {
                field: "history.file".to_owned(),
                message: "history file exceeds the global input bound".to_owned(),
            });
        }
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(StoreError::Io)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        loop {
            let mut line = String::new();
            let read = Read::by_ref(&mut reader)
                .take(MAX_HISTORY_ENTRY_BYTES.saturating_add(3))
                .read_line(&mut line)
                .map_err(StoreError::Io)?;
            if read == 0 {
                break;
            }
            let line = line.strip_suffix('\n').unwrap_or(&line);
            let line = line.strip_suffix('\r').unwrap_or(line);
            if u64::try_from(line.len()).unwrap_or(u64::MAX) > MAX_HISTORY_ENTRY_BYTES {
                return Err(StoreError::Validation {
                    field: "history.entry".to_owned(),
                    message: "history entry exceeds its byte bound".to_owned(),
                });
            }
            let entry: HistoryEntry =
                serde_json::from_str(line).map_err(|error| StoreError::Parse {
                    path: path.to_path_buf(),
                    message: error.to_string(),
                })?;
            entry.validate()?;
            entries.push(entry);
            if entries.len() > MAX_HISTORY_MESSAGES {
                return Err(StoreError::Validation {
                    field: "history.file".to_owned(),
                    message: "history file contains more entries than the global input bound"
                        .to_owned(),
                });
            }
        }
        Ok(entries)
    }

    fn merge_entries(
        &self,
        lower_priority: Vec<HistoryEntry>,
        higher_priority: Vec<HistoryEntry>,
    ) -> Vec<HistoryEntry> {
        let mut by_id = BTreeMap::new();
        for entry in lower_priority.into_iter().chain(higher_priority) {
            by_id.insert(entry.id, entry);
        }
        let mut entries: Vec<_> = by_id.into_values().collect();
        entries.sort_by(|left, right| {
            (left.recorded_at_unix_ms, left.id).cmp(&(right.recorded_at_unix_ms, right.id))
        });
        let excess = entries.len().saturating_sub(self.max_messages);
        if excess > 0 {
            entries.drain(..excess);
        }
        entries
    }

    /// Insert a record or replace the matching local record ID, retaining newest entries only.
    ///
    /// # Errors
    /// Returns validation, I/O, serialization, or atomic replacement errors.
    pub fn upsert(&self, entry: &HistoryEntry) -> Result<(), StoreError> {
        entry.validate()?;
        let _locks = acquire_target_locks(self.lock_targets())?;
        let mut entries = self.load_unlocked()?;
        if let Some(existing) = entries.iter_mut().find(|existing| existing.id == entry.id) {
            *existing = entry.clone();
        } else {
            entries.push(entry.clone());
        }
        entries.sort_by(|left, right| {
            (left.recorded_at_unix_ms, left.id).cmp(&(right.recorded_at_unix_ms, right.id))
        });
        let excess = entries.len().saturating_sub(self.max_messages);
        entries.drain(..excess);
        self.persist(&entries)
    }

    /// Copy retained history to an unused canonical store without removing this store.
    ///
    /// Any retained destination file is rejected instead of merging unrelated conversations.
    ///
    /// # Errors
    /// Returns validation, I/O, parse, serialization, or atomic replacement errors.
    pub fn copy_to(&self, destination: &Self) -> Result<(), StoreError> {
        let _locks = acquire_target_locks(
            self.lock_targets()
                .into_iter()
                .chain(destination.lock_targets()),
        )?;
        if destination.any_path_exists() {
            return Err(StoreError::Validation {
                field: "history.destination".to_owned(),
                message: "retained destination history must be cleared before copying".to_owned(),
            });
        }
        let source_entries = self.load_unlocked()?;
        if source_entries.is_empty() && !self.path.exists() {
            return Ok(());
        }
        destination.persist(&source_entries)
    }

    /// Move retained history to an unused destination while an owner update succeeds.
    ///
    /// Source and destination locks remain held while `update_owner` runs. Any retained
    /// destination file is rejected instead of merging unrelated conversations. When the owner
    /// update fails, a newly written destination is removed and the source remains authoritative.
    ///
    /// # Errors
    /// Returns validation, I/O, parsing, serialization, lock, or owner-update errors.
    pub fn move_to_with(
        &self,
        destination: &Self,
        update_owner: impl FnOnce() -> Result<(), StoreError>,
    ) -> Result<bool, StoreError> {
        let _locks = acquire_target_locks(
            self.lock_targets()
                .into_iter()
                .chain(destination.lock_targets()),
        )?;
        if destination.any_path_exists() {
            return Err(StoreError::Validation {
                field: "history.destination".to_owned(),
                message: "retained destination history must be cleared before profile rename"
                    .to_owned(),
            });
        }

        let source_present = self.any_path_exists();
        let source_entries = self.load_unlocked()?;
        if source_present {
            destination.persist(&source_entries)?;
        }
        if let Err(error) = update_owner() {
            if source_present {
                let _cleanup_result = destination.clear_unlocked();
            }
            return Err(error);
        }
        self.clear_unlocked()?;
        Ok(source_present)
    }

    /// Remove the selected profile history file. Missing files are already clear.
    ///
    /// # Errors
    /// Returns an I/O error if an existing file cannot be removed.
    pub fn clear(&self) -> Result<(), StoreError> {
        let _locks = acquire_target_locks(self.lock_targets())?;
        self.clear_unlocked()
    }

    fn clear_unlocked(&self) -> Result<(), StoreError> {
        let canonical_result = remove_file_if_exists(&self.path);
        let legacy_result = self
            .legacy_path
            .as_deref()
            .map_or(Ok(()), remove_file_if_exists);
        match (canonical_result, legacy_result) {
            (Err(error), _) | (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn lock_targets(&self) -> Vec<&Path> {
        let mut targets = vec![self.path.as_path()];
        if let Some(legacy) = self.legacy_path.as_deref() {
            targets.push(legacy);
        }
        targets
    }

    fn any_path_exists(&self) -> bool {
        self.path.exists() || self.legacy_path.as_deref().is_some_and(Path::exists)
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
            let encoded = serde_json::to_vec(entry).map_err(|error| StoreError::Serde {
                message: error.to_string(),
            })?;
            if u64::try_from(encoded.len()).unwrap_or(u64::MAX) > MAX_HISTORY_ENTRY_BYTES {
                return Err(StoreError::Validation {
                    field: "history.entry".to_owned(),
                    message: "serialized history entry exceeds its byte bound".to_owned(),
                });
            }
            temp.write_all(&encoded).map_err(StoreError::Io)?;
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

fn remove_file_if_exists(path: &Path) -> Result<(), StoreError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StoreError::Io(error)),
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
        if !(1..=MAX_OPERATION_TIMEOUT_MS).contains(&self.timeout_ms) || self.max_concurrency == 0 {
            return Err(StoreError::Validation {
                field: "hook".to_string(),
                message: "timeout must be between 1 and 86400000 milliseconds and concurrency must be positive"
                    .to_string(),
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
    /// Construct a validated environment-backed secret reference.
    ///
    /// # Errors
    /// Returns [`StoreError::Validation`] when `name` is not a bounded environment identifier.
    pub fn environment(name: impl Into<String>) -> Result<Self, StoreError> {
        let reference = Self::Environment { name: name.into() };
        reference.validate("secret.environment")?;
        Ok(reference)
    }

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

/// Return whether a string is safe for use as a persisted profile identifier.
///
/// Identifiers contain at most [`MAX_PROFILE_IDENTIFIER_BYTES`] ASCII bytes, start with an
/// ASCII letter or underscore, and contain only ASCII letters, digits, underscores, or hyphens.
#[must_use]
pub fn validate_identifier(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_PROFILE_IDENTIFIER_BYTES {
        return false;
    }

    let mut chars = name.chars();
    let first = chars.next();
    if !matches!(first, Some(ch) if ch.is_ascii_alphabetic() || ch == '_') {
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

    /// A cross-process configuration or history transaction stayed busy past the deadline.
    #[error("timed out waiting for the file lock at {path:?}")]
    LockTimeout {
        /// Stable sidecar lock path.
        path: PathBuf,
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
    fn config_file_input_bound_is_inclusive() {
        let dir = assert_ok(TempDir::new(), "temp dir");
        let store = ConfigStore::new(dir.path().join("config.toml"));
        let maximum = usize::try_from(MAX_CONFIG_FILE_BYTES).expect("config bound fits usize");
        let mut raw = "version = 1\n".to_owned();
        raw.push_str(&"#".repeat(maximum.saturating_sub(raw.len())));
        assert_eq!(raw.len(), maximum);
        assert_ok(fs::write(store.path(), &raw), "write exact-bound config");
        assert!(matches!(
            store.load_with_overrides(&HashMap::new()),
            Ok(LoadOutcome::Loaded(_))
        ));

        raw.push('#');
        assert_ok(fs::write(store.path(), raw), "write oversized config");
        assert!(matches!(
            store.load_with_overrides(&HashMap::new()),
            Err(StoreError::Validation { field, .. }) if field == "config.file"
        ));
    }

    #[test]
    fn persisted_timeout_bounds_are_strict() {
        let mut config = Config {
            timeout: TimeoutSettings {
                connect_timeout_ms: MAX_OPERATION_TIMEOUT_MS,
                request_timeout_ms: MAX_OPERATION_TIMEOUT_MS,
                retry_timeout_ms: MAX_OPERATION_TIMEOUT_MS,
            },
            hook: HookSettings {
                timeout_ms: MAX_OPERATION_TIMEOUT_MS,
                ..HookSettings::default()
            },
            ..Config::default()
        };
        config.device_profiles.insert(
            "bounded".to_owned(),
            DeviceProfile {
                transport: TransportConfig::Mock {
                    scenario: "demo".to_owned(),
                },
                transport_overrides: Some(TransportOverrides {
                    request_timeout_ms: Some(MAX_OPERATION_TIMEOUT_MS),
                }),
                secret: None,
            },
        );
        assert!(config.validate().is_ok());

        config.timeout.connect_timeout_ms = u64::MAX;
        assert!(config.validate().is_err());
        config.timeout.connect_timeout_ms = MAX_OPERATION_TIMEOUT_MS;
        config.hook.timeout_ms = MAX_OPERATION_TIMEOUT_MS.saturating_add(1);
        assert!(config.validate().is_err());
        config.hook.timeout_ms = MAX_OPERATION_TIMEOUT_MS;
        config
            .device_profiles
            .get_mut("bounded")
            .expect("bounded profile")
            .transport_overrides = Some(TransportOverrides {
            request_timeout_ms: Some(0),
        });
        assert!(config.validate().is_err());
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
        assert!(!validate_identifier("-leading-option"));
        assert!(validate_identifier("field-unit_2"));
        assert!(validate_identifier(&format!("a{}", "b".repeat(63))));
        assert!(!validate_identifier(&format!("a{}", "b".repeat(64))));
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
    fn resolved_mqtt_passwords_share_the_source_independent_bound() {
        struct OversizedResolver;

        impl SecretResolver for OversizedResolver {
            fn resolve(&self, _reference: &SecretRef) -> Result<SecretString, StoreError> {
                Ok(SecretString::from("x".repeat(
                    meshquill_mqtt::MAX_MQTT_PASSWORD_BYTES.saturating_add(1),
                )))
            }
        }

        let settings = MqttSettings {
            password: Some(SecretRef::Environment {
                name: "MESHQUILL_MQTT_PASSWORD".to_owned(),
            }),
            ..MqttSettings::default()
        };
        assert!(matches!(
            settings.resolve_password(&OversizedResolver),
            Err(StoreError::Validation { field, .. }) if field == "mqtt.password"
        ));
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
    fn maximally_escaped_valid_history_fields_roundtrip_within_the_line_bound() {
        let dir = assert_ok(TempDir::new(), "temp dir");
        let store = assert_ok(
            HistoryStore::new(dir.path().join("history.jsonl"), 1),
            "history store",
        );
        let entry = assert_ok(
            HistoryEntry::new(
                HistoryDirection::Incoming,
                "\u{001f}".repeat(256),
                Some(u8::MAX),
                "\u{001f}".repeat(1_024),
                HistoryStatus::Received,
                Some([u8::MAX; 4]),
            ),
            "maximally escaped history entry",
        );
        let encoded = assert_ok(serde_json::to_vec(&entry), "serialize history entry");
        assert!(
            encoded.len() > 4_096,
            "fixture must cover the former line bound"
        );
        assert!(
            u64::try_from(encoded.len()).unwrap_or(u64::MAX) <= MAX_HISTORY_ENTRY_BYTES,
            "valid entry must fit the persisted line bound"
        );

        assert_ok(store.upsert(&entry), "persist escaped history entry");
        assert_eq!(
            assert_ok(store.load(), "reload escaped history entry"),
            vec![entry]
        );
    }

    #[test]
    fn lowering_history_retention_prunes_and_rewrites_newest_entries() {
        let dir = assert_ok(TempDir::new(), "temp dir");
        let path = dir.path().join("history.jsonl");
        let original = assert_ok(HistoryStore::new(&path, 3), "original history store");
        for (id, timestamp) in [(1, 10), (2, 20), (3, 30)] {
            let mut entry = assert_ok(
                HistoryEntry::new(
                    HistoryDirection::Incoming,
                    "peer",
                    None,
                    format!("message-{id}"),
                    HistoryStatus::Received,
                    None,
                ),
                "history entry",
            );
            entry.id = Uuid::from_u128(id);
            entry.recorded_at_unix_ms = timestamp;
            assert_ok(original.upsert(&entry), "original upsert");
        }

        let reduced = assert_ok(HistoryStore::new(&path, 2), "reduced history store");
        let loaded = assert_ok(reduced.load(), "load with reduced retention");
        assert_eq!(
            loaded.iter().map(|entry| entry.id).collect::<Vec<_>>(),
            vec![Uuid::from_u128(2), Uuid::from_u128(3)]
        );
        assert_eq!(
            assert_ok(fs::read_to_string(&path), "read pruned history")
                .lines()
                .count(),
            2
        );
    }

    #[test]
    fn concurrent_history_upserts_do_not_lose_distinct_records() {
        use std::sync::{Arc, Barrier};

        let dir = assert_ok(TempDir::new(), "temp dir");
        let store = Arc::new(assert_ok(
            HistoryStore::new(dir.path().join("history.jsonl"), 32),
            "history store",
        ));
        let barrier = Arc::new(Barrier::new(8));
        let workers = (0_u128..8)
            .map(|id| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut entry = HistoryEntry::new(
                        HistoryDirection::Incoming,
                        "peer",
                        None,
                        format!("message-{id}"),
                        HistoryStatus::Received,
                        None,
                    )
                    .expect("history entry");
                    entry.id = Uuid::from_u128(id.saturating_add(1));
                    entry.recorded_at_unix_ms = u64::try_from(id).expect("small id");
                    barrier.wait();
                    store.upsert(&entry).expect("concurrent upsert");
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("history worker");
        }
        assert_eq!(assert_ok(store.load(), "load concurrent history").len(), 8);
    }

    #[test]
    fn history_copy_and_profile_move_refuse_retained_destination_without_mutation() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let dir = assert_ok(TempDir::new(), "temp dir");
        let source = assert_ok(
            HistoryStore::new(dir.path().join("old.jsonl"), 10),
            "source history",
        );
        let destination = assert_ok(
            HistoryStore::new(dir.path().join("new.jsonl"), 10),
            "destination history",
        );
        for (store, id, text) in [(&source, 1, "source"), (&destination, 2, "destination")] {
            let mut entry = assert_ok(
                HistoryEntry::new(
                    HistoryDirection::Incoming,
                    "peer",
                    None,
                    text,
                    HistoryStatus::Received,
                    None,
                ),
                "history entry",
            );
            entry.id = Uuid::from_u128(id);
            assert_ok(store.upsert(&entry), "seed history");
        }
        let source_before = assert_ok(fs::read(source.path()), "source bytes");
        let destination_before = assert_ok(fs::read(destination.path()), "destination bytes");
        let copy_error = source
            .copy_to(&destination)
            .expect_err("copy to retained destination must be refused");
        assert!(matches!(
            copy_error,
            StoreError::Validation { field, .. } if field == "history.destination"
        ));
        assert_eq!(
            assert_ok(fs::read(source.path()), "source after refused copy"),
            source_before
        );
        assert_eq!(
            assert_ok(
                fs::read(destination.path()),
                "destination after refused copy"
            ),
            destination_before
        );

        let owner_updated = AtomicBool::new(false);
        let error = source
            .move_to_with(&destination, || {
                owner_updated.store(true, Ordering::SeqCst);
                Ok(())
            })
            .expect_err("retained destination must be refused");
        assert!(matches!(
            error,
            StoreError::Validation { field, .. } if field == "history.destination"
        ));
        assert!(!owner_updated.load(Ordering::SeqCst));
        assert_eq!(
            assert_ok(fs::read(source.path()), "source after"),
            source_before
        );
        assert_eq!(
            assert_ok(fs::read(destination.path()), "destination after"),
            destination_before
        );
    }

    #[test]
    fn config_transactions_preserve_concurrent_distinct_profile_updates() {
        use std::sync::{Arc, Barrier};

        let dir = assert_ok(TempDir::new(), "temp dir");
        let path = dir.path().join("config.toml");
        assert_ok(
            ConfigStore::new(&path).save(&Config::default()),
            "seed config",
        );
        let barrier = Arc::new(Barrier::new(8));
        let workers = (0..8)
            .map(|index| {
                let path = path.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    let store = ConfigStore::new(path);
                    let locked = store.lock_exclusive().expect("config lock");
                    let LoadOutcome::Loaded(mut config) = locked
                        .load_with_overrides(&HashMap::new())
                        .expect("load config")
                    else {
                        panic!("expected current config");
                    };
                    config.device_profiles.insert(
                        format!("profile_{index}"),
                        DeviceProfile {
                            transport: TransportConfig::Mock {
                                scenario: "default".to_owned(),
                            },
                            transport_overrides: None,
                            secret: None,
                        },
                    );
                    locked.save(&config).expect("save config");
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().expect("config worker");
        }
        let LoadOutcome::Loaded(loaded) = assert_ok(
            ConfigStore::new(&path).load_with_overrides(&HashMap::new()),
            "load final config",
        ) else {
            panic!("expected current config");
        };
        assert_eq!(loaded.device_profiles.len(), 8);
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
    fn profile_resolution_uses_explicit_default_then_sole_profile() {
        fn profile(scenario: &str) -> DeviceProfile {
            DeviceProfile {
                transport: TransportConfig::Mock {
                    scenario: scenario.to_owned(),
                },
                transport_overrides: None,
                secret: None,
            }
        }

        let mut config = Config::default();
        assert_eq!(
            resolve_profile(&config, None).expect_err("empty selection must fail"),
            ProfileSelectionError::NoneConfigured
        );

        config
            .device_profiles
            .insert("sole".to_owned(), profile("one"));
        assert_eq!(
            resolve_profile(&config, None)
                .expect("sole profile selection")
                .name,
            "sole"
        );

        config
            .device_profiles
            .insert("other".to_owned(), profile("two"));
        assert_eq!(
            resolve_profile(&config, None).expect_err("ambiguous selection must fail"),
            ProfileSelectionError::Ambiguous {
                profiles: vec!["other".to_owned(), "sole".to_owned()]
            }
        );

        config.default_profile = Some("sole".to_owned());
        assert_eq!(
            resolve_profile(&config, None)
                .expect("default selection")
                .name,
            "sole"
        );
        assert_eq!(
            resolve_profile(&config, Some("other"))
                .expect("explicit selection")
                .name,
            "other"
        );
        assert_eq!(
            resolve_profile(&config, Some("missing"))
                .expect_err("missing explicit profile must fail"),
            ProfileSelectionError::NotFound {
                name: "missing".to_owned()
            }
        );
    }

    #[test]
    fn history_paths_use_data_root_and_stable_explicit_config_namespace() {
        let data = Path::new("/var/data/meshquill");
        let config = Path::new("/etc/meshquill/./nested/../field.toml");
        let normalized = Path::new("/etc/meshquill/field.toml");
        assert_eq!(
            normalized_config_path_digest(config),
            normalized_config_path_digest(normalized)
        );

        let default_paths = assert_ok(
            history_paths(data, config, false, "field"),
            "default history paths",
        );
        assert_eq!(
            default_paths.canonical,
            data.join("history").join("field.jsonl")
        );
        assert_eq!(
            default_paths.legacy,
            Path::new("/etc/meshquill/./nested/..")
                .join("history")
                .join("field.jsonl")
        );

        let explicit_paths = assert_ok(
            history_paths(data, normalized, true, "field"),
            "explicit history paths",
        );
        assert_eq!(
            explicit_paths.canonical.parent().and_then(Path::parent),
            Some(data.join("history").as_path())
        );
        assert_eq!(
            explicit_paths
                .canonical
                .parent()
                .and_then(Path::file_name)
                .and_then(|value| value.to_str())
                .map(str::len),
            Some(64)
        );
    }

    #[cfg(unix)]
    #[test]
    fn config_path_digest_distinguishes_non_utf8_components() {
        use std::os::unix::ffi::OsStringExt;

        let first = PathBuf::from(OsString::from_vec(b"/tmp/config-\x80.toml".to_vec()));
        let second = PathBuf::from(OsString::from_vec(b"/tmp/config-\x81.toml".to_vec()));
        assert_ne!(
            normalized_config_path_digest(&first),
            normalized_config_path_digest(&second)
        );
    }

    #[cfg(windows)]
    #[test]
    fn config_path_digest_distinguishes_unpaired_utf16_components() {
        use std::os::windows::ffi::OsStringExt;

        let first = PathBuf::from(OsString::from_wide(&[
            u16::from(b'C'),
            u16::from(b':'),
            u16::from(b'\\'),
            0xd800,
        ]));
        let second = PathBuf::from(OsString::from_wide(&[
            u16::from(b'C'),
            u16::from(b':'),
            u16::from(b'\\'),
            0xd801,
        ]));
        assert_ne!(
            normalized_config_path_digest(&first),
            normalized_config_path_digest(&second)
        );
    }

    #[test]
    fn canonical_history_reconciles_legacy_deduplicates_and_is_idempotent() {
        fn entry(id: u128, timestamp: u64, text: &str) -> HistoryEntry {
            let mut entry = assert_ok(
                HistoryEntry::new(
                    HistoryDirection::Incoming,
                    "peer",
                    None,
                    text,
                    HistoryStatus::Received,
                    None,
                ),
                "history entry",
            );
            entry.id = Uuid::from_u128(id);
            entry.recorded_at_unix_ms = timestamp;
            entry
        }

        let dir = assert_ok(TempDir::new(), "temp dir");
        let canonical_path = dir.path().join("data/history/field.jsonl");
        let legacy_path = dir.path().join("config/history/field.jsonl");
        let canonical = assert_ok(HistoryStore::new(&canonical_path, 3), "canonical store");
        let legacy = assert_ok(HistoryStore::new(&legacy_path, 3), "legacy store");
        assert_ok(
            canonical.upsert(&entry(2, 20, "canonical wins")),
            "canonical duplicate",
        );
        assert_ok(
            canonical.upsert(&entry(4, 40, "newest")),
            "canonical newest",
        );
        assert_ok(legacy.upsert(&entry(1, 10, "oldest")), "legacy oldest");
        assert_ok(
            legacy.upsert(&entry(2, 20, "legacy loses")),
            "legacy duplicate",
        );
        assert_ok(legacy.upsert(&entry(3, 30, "middle")), "legacy middle");

        let store = assert_ok(
            HistoryStore::with_legacy(&canonical_path, &legacy_path, 3),
            "reconciling store",
        );
        let entries = assert_ok(store.load(), "reconciled load");
        assert_eq!(
            entries.iter().map(|entry| entry.id).collect::<Vec<_>>(),
            vec![Uuid::from_u128(2), Uuid::from_u128(3), Uuid::from_u128(4)]
        );
        assert_eq!(entries[0].text, "canonical wins");
        assert!(!legacy_path.exists());
        let first_bytes = assert_ok(fs::read(&canonical_path), "canonical bytes");
        assert_eq!(assert_ok(store.load(), "idempotent load"), entries);
        assert_eq!(
            assert_ok(fs::read(&canonical_path), "canonical bytes again"),
            first_bytes
        );
    }

    #[test]
    fn failed_canonical_history_write_retains_legacy_and_clear_removes_both() {
        let dir = assert_ok(TempDir::new(), "temp dir");
        let blocked_parent = dir.path().join("blocked");
        assert_ok(
            fs::write(&blocked_parent, b"not a directory"),
            "blocking file",
        );
        let canonical_path = blocked_parent.join("field.jsonl");
        let legacy_path = dir.path().join("legacy/field.jsonl");
        let legacy = assert_ok(HistoryStore::new(&legacy_path, 3), "legacy store");
        let entry = assert_ok(
            HistoryEntry::new(
                HistoryDirection::Incoming,
                "peer",
                None,
                "retained",
                HistoryStatus::Received,
                None,
            ),
            "history entry",
        );
        assert_ok(legacy.upsert(&entry), "legacy write");

        let blocked = assert_ok(
            HistoryStore::with_legacy(&canonical_path, &legacy_path, 3),
            "blocked store",
        );
        assert!(blocked.load().is_err());
        assert!(legacy_path.exists());

        let canonical_path = dir.path().join("canonical/field.jsonl");
        let both = assert_ok(
            HistoryStore::with_legacy(&canonical_path, &legacy_path, 3),
            "clear store",
        );
        assert_ok(
            HistoryStore::new(&canonical_path, 3).and_then(|store| store.upsert(&entry)),
            "canonical write",
        );
        assert_ok(both.clear(), "clear both paths");
        assert!(!canonical_path.exists());
        assert!(!legacy_path.exists());
    }

    #[test]
    fn cross_platform_path_resolution_is_pure() {
        let linux = PathEnvironment {
            home: Some(PathBuf::from("/home/me")),
            xdg_config_home: Some(PathBuf::from("/tmp/xdg")),
            xdg_data_home: None,
            app_data: None,
            local_app_data: None,
        };
        assert_eq!(
            assert_ok(
                resolve_platform_config_path(Platform::Linux, "meshquill", &linux),
                "linux path"
            ),
            PathBuf::from("/tmp/xdg/meshquill/config.toml")
        );
        assert_eq!(
            assert_ok(
                resolve_platform_data_dir(Platform::Linux, "meshquill", &linux),
                "linux data path"
            ),
            PathBuf::from("/home/me/.local/share/meshquill")
        );

        let mac = PathEnvironment {
            home: Some(PathBuf::from("/Users/me")),
            xdg_config_home: None,
            xdg_data_home: None,
            app_data: None,
            local_app_data: None,
        };
        assert_eq!(
            assert_ok(
                resolve_platform_config_path(Platform::Macos, "meshquill", &mac),
                "mac path"
            ),
            PathBuf::from("/Users/me/Library/Application Support/meshquill/config.toml")
        );
        assert_eq!(
            assert_ok(
                resolve_platform_data_dir(Platform::Macos, "meshquill", &mac),
                "mac data path"
            ),
            PathBuf::from("/Users/me/Library/Application Support/meshquill")
        );

        let win = PathEnvironment {
            home: Some(PathBuf::from("C:/Users/me")),
            xdg_config_home: None,
            xdg_data_home: None,
            app_data: Some(PathBuf::from("C:/Users/me/AppData/Roaming")),
            local_app_data: Some(PathBuf::from("C:/Users/me/AppData/Local")),
        };
        assert_eq!(
            assert_ok(
                resolve_platform_config_path(Platform::Windows, "Meshquill", &win),
                "windows path"
            ),
            PathBuf::from("C:/Users/me/AppData/Roaming/Meshquill/config.toml")
        );
        assert_eq!(
            assert_ok(
                resolve_platform_data_dir(Platform::Windows, "Meshquill", &win),
                "windows data path"
            ),
            PathBuf::from("C:/Users/me/AppData/Local/Meshquill")
        );

        let local_only_windows = PathEnvironment {
            local_app_data: Some(PathBuf::from("C:/Users/me/AppData/Local")),
            ..PathEnvironment::default()
        };
        assert_eq!(
            assert_ok(
                resolve_platform_config_path(Platform::Windows, "Meshquill", &local_only_windows,),
                "windows config fallback path",
            ),
            PathBuf::from("C:/Users/me/AppData/Local/Meshquill/config.toml")
        );

        for invalid in [PathBuf::new(), PathBuf::from("relative/data")] {
            let invalid_linux = PathEnvironment {
                xdg_data_home: Some(invalid),
                ..PathEnvironment::default()
            };
            assert!(matches!(
                resolve_platform_data_dir(Platform::Linux, "meshquill", &invalid_linux),
                Err(StoreError::MissingRuntimePath { .. })
            ));
        }
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
