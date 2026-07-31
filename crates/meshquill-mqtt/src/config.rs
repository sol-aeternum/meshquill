use std::fmt;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::Zeroizing;

/// Largest MQTT application payload accepted by configuration validation.
pub const MAX_CONFIGURED_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Hard MQTT v1 destination bound; installations may configure a smaller value.
pub const MAX_COMMAND_DESTINATION_BYTES: usize = 128;

/// Hard MQTT v1 command-text bound; installations may configure a smaller value.
pub const MAX_COMMAND_TEXT_BYTES: usize = 1024;

/// Largest configured TLS PEM file read by the gateway (one MiB).
pub const MAX_TLS_FILE_BYTES: usize = 1024 * 1024;

/// Largest runtime MQTT password accepted from any credential source.
pub const MAX_MQTT_PASSWORD_BYTES: usize = 4_096;

/// Largest allowed reconnect delay (one hour).
pub const MAX_RECONNECT_DELAY_MS: u64 = 60 * 60 * 1000;

/// Largest allowed timeout for queueing one broker operation (one minute).
pub const MAX_BROKER_OPERATION_TIMEOUT_MS: u64 = 60 * 1000;

const MAX_HOST_BYTES: usize = 253;
const MAX_CLIENT_ID_BYTES: usize = 128;
const MAX_USERNAME_BYTES: usize = 128;
const MAX_ORIGIN_BYTES: usize = 128;
const MAX_TOPIC_PREFIX_BYTES: usize = 512;
const MAX_CHANNEL_CAPACITY: usize = 4096;
pub(crate) const MAX_DEDUPE_CAPACITY: usize = 100_000;
pub(crate) const MAX_DEDUPE_TTL_SECS: u64 = 7 * 24 * 60 * 60;
const MAX_TLS_FILE_BYTES_U64: u64 = 1024 * 1024;

/// MQTT protocol dialect used for the broker connection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum MqttProtocol {
    /// MQTT 3.1.1 (the protocol level called v4 by `rumqttc`).
    #[default]
    #[serde(rename = "3.1.1")]
    V311,
    /// MQTT 5.0.
    #[serde(rename = "5")]
    V5,
}

/// MQTT quality-of-service level used for gateway publications and subscriptions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MqttQos {
    /// At most once delivery (`QoS` 0).
    AtMostOnce,
    /// At least once delivery (`QoS` 1).
    #[default]
    AtLeastOnce,
    /// Exactly once delivery (`QoS` 2).
    ExactlyOnce,
}

impl MqttQos {
    /// Converts this setting to the MQTT 3.1.1 `rumqttc` `QoS` type.
    #[must_use]
    pub fn as_v311(self) -> rumqttc::QoS {
        match self {
            Self::AtMostOnce => rumqttc::QoS::AtMostOnce,
            Self::AtLeastOnce => rumqttc::QoS::AtLeastOnce,
            Self::ExactlyOnce => rumqttc::QoS::ExactlyOnce,
        }
    }

    /// Converts this setting to the MQTT 5 `rumqttc` `QoS` type.
    #[must_use]
    pub fn as_v5(self) -> rumqttc::v5::mqttbytes::QoS {
        match self {
            Self::AtMostOnce => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
            Self::AtLeastOnce => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
            Self::ExactlyOnce => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
        }
    }
}

/// TLS settings for the broker connection.
///
/// Certificate validation cannot be disabled. Plain TCP requires the explicit
/// `enabled = false` opt-out instead.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
    /// Whether to use TLS. This is enabled by default.
    pub enabled: bool,
    /// Must remain true; configurations that disable verification are rejected.
    pub verify_server_certificate: bool,
    /// Optional PEM bundle replacing the system trust roots.
    pub ca_path: Option<PathBuf>,
    /// Optional PEM client certificate chain for mutual TLS.
    pub client_certificate_path: Option<PathBuf>,
    /// Optional PEM private key paired with `client_certificate_path`.
    pub client_private_key_path: Option<PathBuf>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            verify_server_certificate: true,
            ca_path: None,
            client_certificate_path: None,
            client_private_key_path: None,
        }
    }
}

/// Broker session behavior shared across MQTT protocol versions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SessionConfig {
    /// Start with a clean broker session.
    pub clean: bool,
    /// MQTT 5 session expiry interval. MQTT 3.1.1 configurations must leave this unset.
    pub expiry_secs: Option<u32>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            clean: true,
            expiry_secs: None,
        }
    }
}

/// Bounded exponential reconnect timing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReconnectConfig {
    /// Delay after the first connection failure.
    pub initial_delay_ms: u64,
    /// Maximum delay between retries.
    pub max_delay_ms: u64,
    /// Integer growth factor applied after each failure.
    pub multiplier: u32,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay_ms: 250,
            max_delay_ms: 30_000,
            multiplier: 2,
        }
    }
}

/// Bounds for the in-memory command event-ID cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DedupeConfig {
    /// Maximum number of event IDs retained.
    pub capacity: usize,
    /// Time after which a retained event ID expires.
    pub ttl_secs: u64,
}

impl Default for DedupeConfig {
    fn default() -> Self {
        Self {
            capacity: 4096,
            ttl_secs: 15 * 60,
        }
    }
}

/// Application limits applied to allowlisted outbound send commands.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CommandLimits {
    /// Maximum UTF-8 byte length of a direct-message destination.
    pub max_destination_bytes: usize,
    /// Largest channel index accepted from MQTT.
    pub max_channel: u8,
    /// Maximum UTF-8 byte length of message text.
    pub max_text_bytes: usize,
}

impl Default for CommandLimits {
    fn default() -> Self {
        Self {
            max_destination_bytes: 128,
            max_channel: 7,
            max_text_bytes: 1024,
        }
    }
}

/// Serializable gateway configuration. Password material is deliberately absent.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MqttConfig {
    /// Broker host name or IP address, without a URI scheme.
    pub host: String,
    /// Broker TCP port.
    pub port: u16,
    /// Stable MQTT client identifier.
    pub client_id: String,
    /// MQTT protocol dialect.
    pub protocol: MqttProtocol,
    /// TLS and certificate settings.
    pub tls: TlsConfig,
    /// Optional broker username. The password is passed separately at runtime.
    pub username: Option<String>,
    /// `QoS` used for publications and the optional send subscription.
    pub qos: MqttQos,
    /// MQTT keep-alive interval in seconds.
    pub keep_alive_secs: u64,
    /// Maximum time allowed to queue one publish or subscribe request.
    pub broker_operation_timeout_ms: u64,
    /// Broker session behavior.
    pub session: SessionConfig,
    /// MQTT topic namespace owned by this gateway deployment.
    pub topic_prefix: String,
    /// Stable local origin included in events and used for loop prevention.
    pub origin: String,
    /// Explicit opt-in for subscribing to outbound send requests.
    pub allow_send: bool,
    /// Reconnect delay policy.
    pub reconnect: ReconnectConfig,
    /// Maximum JSON payload size accepted or published by the gateway.
    pub max_payload_bytes: usize,
    /// Event-ID deduplication policy.
    pub dedupe: DedupeConfig,
    /// Validation bounds for outbound commands.
    pub command_limits: CommandLimits,
    /// Capacity of each gateway-owned async channel.
    pub channel_capacity: usize,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_owned(),
            port: 8883,
            client_id: "meshquill-mqtt".to_owned(),
            protocol: MqttProtocol::V311,
            tls: TlsConfig::default(),
            username: None,
            qos: MqttQos::AtLeastOnce,
            keep_alive_secs: 30,
            broker_operation_timeout_ms: 5000,
            session: SessionConfig::default(),
            topic_prefix: "meshquill".to_owned(),
            origin: "meshquill-mqtt".to_owned(),
            allow_send: false,
            reconnect: ReconnectConfig::default(),
            max_payload_bytes: 64 * 1024,
            dedupe: DedupeConfig::default(),
            command_limits: CommandLimits::default(),
            channel_capacity: 64,
        }
    }
}

impl MqttConfig {
    /// Validates all non-secret configuration, including configured PEM paths.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for unsafe, malformed, or unsupported settings.
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_host(&self.host)?;
        validate_nonempty_text("client_id", &self.client_id, MAX_CLIENT_ID_BYTES)?;
        validate_nonempty_text("origin", &self.origin, MAX_ORIGIN_BYTES)?;
        validate_topic_prefix(&self.topic_prefix)?;

        if self.port == 0 {
            return Err(ConfigError::invalid("port", "must be non-zero"));
        }
        if let Some(username) = &self.username {
            validate_nonempty_text("username", username, MAX_USERNAME_BYTES)?;
        }
        if !(1..=65_535).contains(&self.keep_alive_secs) {
            return Err(ConfigError::invalid(
                "keep_alive_secs",
                "must be between 1 and 65535",
            ));
        }
        if !(1..=MAX_BROKER_OPERATION_TIMEOUT_MS).contains(&self.broker_operation_timeout_ms) {
            return Err(ConfigError::invalid(
                "broker_operation_timeout_ms",
                "must be between 1 and 60000",
            ));
        }
        if self.protocol == MqttProtocol::V311 && self.session.expiry_secs.is_some() {
            return Err(ConfigError::invalid(
                "session.expiry_secs",
                "is only supported by MQTT 5",
            ));
        }
        if self.allow_send && !self.session.clean {
            return Err(ConfigError::invalid(
                "session.clean",
                "must be true when allow_send is enabled because command deduplication is process-local",
            ));
        }
        validate_reconnect(self.reconnect)?;
        validate_dedupe(self.dedupe)?;
        validate_command_limits(self.command_limits, self.max_payload_bytes)?;
        if !(1..=MAX_CONFIGURED_PAYLOAD_BYTES).contains(&self.max_payload_bytes) {
            return Err(ConfigError::invalid(
                "max_payload_bytes",
                "must be between 1 and 1048576",
            ));
        }
        if !(1..=MAX_CHANNEL_CAPACITY).contains(&self.channel_capacity) {
            return Err(ConfigError::invalid(
                "channel_capacity",
                "must be between 1 and 4096",
            ));
        }
        validate_tls(&self.tls)
    }

    /// Validates the relationship between the optional username and runtime password.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::CredentialMismatch`] unless username and password are
    /// either both present or both absent.
    pub fn validate_credentials(&self, password: Option<&MqttPassword>) -> Result<(), ConfigError> {
        match (self.username.as_ref(), password) {
            (Some(_), Some(_)) | (None, None) => Ok(()),
            (Some(_), None) => Err(ConfigError::CredentialMismatch {
                reason: "username configured without a runtime password",
            }),
            (None, Some(_)) => Err(ConfigError::CredentialMismatch {
                reason: "runtime password supplied without a username",
            }),
        }
    }
}

/// Runtime-only MQTT password with redacted formatting and no serialization support.
pub struct MqttPassword(SecretString);

impl MqttPassword {
    /// Wraps a non-empty, bounded password for runtime use.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when the supplied password is empty, contains NUL, or exceeds
    /// [`MAX_MQTT_PASSWORD_BYTES`] UTF-8 bytes.
    pub fn new(password: impl Into<String>) -> Result<Self, ConfigError> {
        let mut password = Zeroizing::new(password.into());
        if password.is_empty() {
            return Err(ConfigError::invalid("password", "must not be empty"));
        }
        if password.len() > MAX_MQTT_PASSWORD_BYTES {
            return Err(ConfigError::invalid(
                "password",
                "must not exceed 4096 UTF-8 bytes",
            ));
        }
        if password.as_bytes().contains(&0) {
            return Err(ConfigError::invalid("password", "must not contain NUL"));
        }
        Ok(Self(SecretString::from(std::mem::take(&mut *password))))
    }

    pub(crate) fn expose(&self) -> &str {
        self.0.expose_secret()
    }
}

impl fmt::Debug for MqttPassword {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MqttPassword([REDACTED])")
    }
}

/// MQTT gateway configuration error.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A scalar or string field failed validation.
    #[error("invalid MQTT configuration field `{field}`: {reason}")]
    InvalidField {
        /// Configuration field name.
        field: &'static str,
        /// Static validation explanation.
        reason: &'static str,
    },
    /// Username/password presence did not match.
    #[error("invalid MQTT credentials: {reason}")]
    CredentialMismatch {
        /// Static mismatch explanation.
        reason: &'static str,
    },
    /// A TLS file was inaccessible or malformed.
    #[error("invalid TLS {field} file `{path}`: {reason}")]
    TlsFile {
        /// TLS field name.
        field: &'static str,
        /// Configured filesystem path.
        path: PathBuf,
        /// Safe diagnostic that never contains file contents.
        reason: String,
    },
}

impl ConfigError {
    fn invalid(field: &'static str, reason: &'static str) -> Self {
        Self::InvalidField { field, reason }
    }
}

pub(crate) fn validate_topic_prefix(prefix: &str) -> Result<(), ConfigError> {
    if prefix.is_empty() {
        return Err(ConfigError::invalid("topic_prefix", "must not be empty"));
    }
    if prefix.len() > MAX_TOPIC_PREFIX_BYTES {
        return Err(ConfigError::invalid(
            "topic_prefix",
            "exceeds 512 UTF-8 bytes",
        ));
    }
    if prefix.starts_with('/') || prefix.ends_with('/') || prefix.contains("//") {
        return Err(ConfigError::invalid(
            "topic_prefix",
            "must contain non-empty topic levels",
        ));
    }
    if prefix
        .chars()
        .any(|character| matches!(character, '+' | '#' | '\0') || character.is_control())
    {
        return Err(ConfigError::invalid(
            "topic_prefix",
            "contains an MQTT wildcard, NUL, or control character",
        ));
    }
    Ok(())
}

fn validate_host(host: &str) -> Result<(), ConfigError> {
    if host.is_empty() {
        return Err(ConfigError::invalid("host", "must not be empty"));
    }
    if host.len() > MAX_HOST_BYTES {
        return Err(ConfigError::invalid("host", "exceeds 253 UTF-8 bytes"));
    }
    if host.contains("//")
        || host.contains('/')
        || host
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(ConfigError::invalid(
            "host",
            "must be a host name or IP address without a URI scheme",
        ));
    }
    Ok(())
}

fn validate_nonempty_text(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), ConfigError> {
    if value.is_empty() || value.trim() != value {
        return Err(ConfigError::invalid(
            field,
            "must be non-empty and have no surrounding whitespace",
        ));
    }
    if value.len() > max_bytes {
        return Err(ConfigError::invalid(field, "exceeds its UTF-8 byte limit"));
    }
    if value.chars().any(char::is_control) {
        return Err(ConfigError::invalid(field, "contains a control character"));
    }
    Ok(())
}

fn validate_reconnect(config: ReconnectConfig) -> Result<(), ConfigError> {
    if config.initial_delay_ms == 0 {
        return Err(ConfigError::invalid(
            "reconnect.initial_delay_ms",
            "must be non-zero",
        ));
    }
    if config.max_delay_ms < config.initial_delay_ms {
        return Err(ConfigError::invalid(
            "reconnect.max_delay_ms",
            "must be at least the initial delay",
        ));
    }
    if config.max_delay_ms > MAX_RECONNECT_DELAY_MS {
        return Err(ConfigError::invalid(
            "reconnect.max_delay_ms",
            "must not exceed 3600000",
        ));
    }
    if config.multiplier < 2 {
        return Err(ConfigError::invalid(
            "reconnect.multiplier",
            "must be at least 2",
        ));
    }
    Ok(())
}

fn validate_dedupe(config: DedupeConfig) -> Result<(), ConfigError> {
    if !(1..=MAX_DEDUPE_CAPACITY).contains(&config.capacity) {
        return Err(ConfigError::invalid(
            "dedupe.capacity",
            "must be between 1 and 100000",
        ));
    }
    if !(1..=MAX_DEDUPE_TTL_SECS).contains(&config.ttl_secs) {
        return Err(ConfigError::invalid(
            "dedupe.ttl_secs",
            "must be between 1 and 604800",
        ));
    }
    Ok(())
}

fn validate_command_limits(
    limits: CommandLimits,
    max_payload_bytes: usize,
) -> Result<(), ConfigError> {
    if limits.max_destination_bytes == 0
        || limits.max_destination_bytes > MAX_COMMAND_DESTINATION_BYTES
    {
        return Err(ConfigError::invalid(
            "command_limits.max_destination_bytes",
            "must be between 1 and 128",
        ));
    }
    if limits.max_text_bytes == 0
        || limits.max_text_bytes > max_payload_bytes
        || limits.max_text_bytes > MAX_COMMAND_TEXT_BYTES
    {
        return Err(ConfigError::invalid(
            "command_limits.max_text_bytes",
            "must be non-zero and no larger than 1024 or max_payload_bytes",
        ));
    }
    Ok(())
}

fn validate_tls(config: &TlsConfig) -> Result<(), ConfigError> {
    if !config.verify_server_certificate {
        return Err(ConfigError::invalid(
            "tls.verify_server_certificate",
            "certificate validation cannot be disabled",
        ));
    }

    let any_paths = config.ca_path.is_some()
        || config.client_certificate_path.is_some()
        || config.client_private_key_path.is_some();
    if !config.enabled && any_paths {
        return Err(ConfigError::invalid(
            "tls",
            "certificate paths require TLS to be enabled",
        ));
    }
    if config.client_certificate_path.is_some() != config.client_private_key_path.is_some() {
        return Err(ConfigError::invalid(
            "tls.client_certificate_path",
            "client certificate and private key paths must be supplied together",
        ));
    }

    if let Some(path) = &config.ca_path {
        validate_pem_file("CA certificate", path, &[b"-----BEGIN CERTIFICATE-----"])?;
    }
    if let Some(path) = &config.client_certificate_path {
        validate_pem_file(
            "client certificate",
            path,
            &[b"-----BEGIN CERTIFICATE-----"],
        )?;
    }
    if let Some(path) = &config.client_private_key_path {
        validate_pem_file(
            "client private key",
            path,
            &[
                b"-----BEGIN PRIVATE KEY-----",
                b"-----BEGIN RSA PRIVATE KEY-----",
                b"-----BEGIN EC PRIVATE KEY-----",
            ],
        )?;
    }
    Ok(())
}

fn validate_pem_file(
    field: &'static str,
    path: &Path,
    markers: &[&[u8]],
) -> Result<(), ConfigError> {
    let metadata = fs::metadata(path).map_err(|error| ConfigError::TlsFile {
        field,
        path: path.to_path_buf(),
        reason: error.to_string(),
    })?;
    if !metadata.is_file() {
        return Err(ConfigError::TlsFile {
            field,
            path: path.to_path_buf(),
            reason: "path is not a regular file".to_owned(),
        });
    }
    if metadata.len() > MAX_TLS_FILE_BYTES_U64 {
        return Err(ConfigError::TlsFile {
            field,
            path: path.to_path_buf(),
            reason: "file exceeds the one MiB TLS file limit".to_owned(),
        });
    }
    let file = fs::File::open(path).map_err(|error| ConfigError::TlsFile {
        field,
        path: path.to_path_buf(),
        reason: error.to_string(),
    })?;
    let mut contents = Zeroizing::new(Vec::new());
    file.take(MAX_TLS_FILE_BYTES_U64 + 1)
        .read_to_end(&mut contents)
        .map_err(|error| ConfigError::TlsFile {
            field,
            path: path.to_path_buf(),
            reason: error.to_string(),
        })?;
    if contents.len() > MAX_TLS_FILE_BYTES {
        return Err(ConfigError::TlsFile {
            field,
            path: path.to_path_buf(),
            reason: "file exceeds the one MiB TLS file limit".to_owned(),
        });
    }
    if !markers.iter().any(|marker| {
        contents
            .windows(marker.len())
            .any(|window| window == *marker)
    }) {
        return Err(ConfigError::TlsFile {
            field,
            path: path.to_path_buf(),
            reason: "file does not contain the expected PEM block".to_owned(),
        });
    }
    Ok(())
}

/// Converts milliseconds to a duration without exposing integer arithmetic to callers.
#[must_use]
pub(crate) const fn milliseconds(value: u64) -> Duration {
    Duration::from_millis(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_secure_and_valid() {
        let config = MqttConfig::default();
        assert!(config.tls.enabled);
        assert!(config.tls.verify_server_certificate);
        assert!(!config.allow_send);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn invalid_scalar_and_protocol_combinations_are_rejected() {
        let mut config = MqttConfig {
            port: 0,
            ..MqttConfig::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidField { field: "port", .. })
        ));

        config.port = 8883;
        config.session.expiry_secs = Some(60);
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidField {
                field: "session.expiry_secs",
                ..
            })
        ));

        config.protocol = MqttProtocol::V5;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn configured_command_limits_cannot_expand_the_v1_wire_contract() {
        let mut config = MqttConfig::default();
        config.command_limits.max_destination_bytes = MAX_COMMAND_DESTINATION_BYTES + 1;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidField {
                field: "command_limits.max_destination_bytes",
                ..
            })
        ));

        config.command_limits.max_destination_bytes = MAX_COMMAND_DESTINATION_BYTES;
        config.command_limits.max_text_bytes = MAX_COMMAND_TEXT_BYTES + 1;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidField {
                field: "command_limits.max_text_bytes",
                ..
            })
        ));
    }

    #[test]
    fn unsafe_topics_and_tls_verification_are_rejected() {
        let mut config = MqttConfig {
            topic_prefix: "meshquill/#".to_owned(),
            ..MqttConfig::default()
        };
        assert!(config.validate().is_err());

        config.topic_prefix = "meshquill".to_owned();
        config.tls.verify_server_certificate = false;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidField {
                field: "tls.verify_server_certificate",
                ..
            })
        ));
    }

    #[test]
    fn mutual_tls_paths_must_be_paired() {
        let config = MqttConfig {
            tls: TlsConfig {
                client_certificate_path: Some(PathBuf::from("client.pem")),
                ..TlsConfig::default()
            },
            ..MqttConfig::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidField {
                field: "tls.client_certificate_path",
                ..
            })
        ));
    }

    #[test]
    fn configured_tls_files_must_exist_and_contain_pem() {
        let missing = PathBuf::from("this-mqtt-test-certificate-does-not-exist.pem");
        let config = MqttConfig {
            tls: TlsConfig {
                ca_path: Some(missing),
                ..TlsConfig::default()
            },
            ..MqttConfig::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::TlsFile { .. })
        ));
    }

    #[test]
    fn oversized_tls_file_is_rejected_before_reading_contents() {
        let path = std::env::temp_dir().join(format!(
            "meshquill-mqtt-oversized-{}.pem",
            uuid::Uuid::now_v7()
        ));
        let file = fs::File::create(&path).expect("create sparse TLS fixture");
        file.set_len(u64::try_from(MAX_TLS_FILE_BYTES + 1).expect("limit fits u64"))
            .expect("size sparse TLS fixture");
        drop(file);

        let config = MqttConfig {
            tls: TlsConfig {
                ca_path: Some(path.clone()),
                ..TlsConfig::default()
            },
            ..MqttConfig::default()
        };
        let validation = config.validate();
        fs::remove_file(path).expect("remove sparse TLS fixture");
        assert!(matches!(validation, Err(ConfigError::TlsFile { .. })));
    }

    #[test]
    fn operation_timeout_and_reconnect_delay_have_hard_caps() {
        let mut config = MqttConfig {
            broker_operation_timeout_ms: MAX_BROKER_OPERATION_TIMEOUT_MS + 1,
            ..MqttConfig::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidField {
                field: "broker_operation_timeout_ms",
                ..
            })
        ));

        config.broker_operation_timeout_ms = 5000;
        config.reconnect.max_delay_ms = MAX_RECONNECT_DELAY_MS + 1;
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidField {
                field: "reconnect.max_delay_ms",
                ..
            })
        ));
    }

    #[test]
    fn inbound_sends_require_a_clean_broker_session() {
        let config = MqttConfig {
            allow_send: true,
            session: SessionConfig {
                clean: false,
                expiry_secs: None,
            },
            ..MqttConfig::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidField {
                field: "session.clean",
                ..
            })
        ));

        let safe = MqttConfig {
            session: SessionConfig {
                clean: true,
                expiry_secs: None,
            },
            ..config
        };
        assert!(safe.validate().is_ok());
    }

    #[test]
    fn credentials_must_be_supplied_as_a_pair() {
        let mut config = MqttConfig::default();
        let password = MqttPassword::new("correct horse").expect("non-empty password");
        assert!(config.validate_credentials(Some(&password)).is_err());

        config.username = Some("gateway".to_owned());
        assert!(config.validate_credentials(None).is_err());
        assert!(config.validate_credentials(Some(&password)).is_ok());
    }

    #[test]
    fn password_debug_is_redacted() {
        let password = MqttPassword::new("extremely-secret").expect("non-empty password");
        let debug = format!("{password:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("extremely-secret"));

        let config_json = serde_json::to_string(&MqttConfig::default()).expect("serialize config");
        assert!(!config_json.contains("password"));
    }

    #[test]
    fn password_bound_applies_to_every_constructor_caller() {
        assert!(MqttPassword::new("x".repeat(MAX_MQTT_PASSWORD_BYTES)).is_ok());
        assert!(matches!(
            MqttPassword::new("x".repeat(MAX_MQTT_PASSWORD_BYTES + 1)),
            Err(ConfigError::InvalidField {
                field: "password",
                ..
            })
        ));
        assert!(matches!(
            MqttPassword::new("contains\0nul"),
            Err(ConfigError::InvalidField {
                field: "password",
                ..
            })
        ));
    }

    #[test]
    fn qos_mapping_covers_both_protocols() {
        assert_eq!(MqttQos::AtMostOnce.as_v311(), rumqttc::QoS::AtMostOnce);
        assert_eq!(MqttQos::AtLeastOnce.as_v311(), rumqttc::QoS::AtLeastOnce);
        assert_eq!(MqttQos::ExactlyOnce.as_v311(), rumqttc::QoS::ExactlyOnce);
        assert_eq!(
            MqttQos::AtMostOnce.as_v5(),
            rumqttc::v5::mqttbytes::QoS::AtMostOnce
        );
        assert_eq!(
            MqttQos::AtLeastOnce.as_v5(),
            rumqttc::v5::mqttbytes::QoS::AtLeastOnce
        );
        assert_eq!(
            MqttQos::ExactlyOnce.as_v5(),
            rumqttc::v5::mqttbytes::QoS::ExactlyOnce
        );
    }
}
