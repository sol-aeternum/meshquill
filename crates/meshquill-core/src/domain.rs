use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::error::{CoreError, PacketDisplay, ParseError};

/// A validated 32-byte public key.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct PublicKey([u8; 32]);

impl PublicKey {
    /// Creates a key from an exact 32-byte slice.
    ///
    /// # Errors
    ///
    /// Returns `CoreError::ProtocolInvariant` when the input is not exactly 32 bytes.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, CoreError> {
        let raw: [u8; 32] = bytes
            .try_into()
            .map_err(|_| CoreError::ProtocolInvariant("public key must be exactly 32 bytes"))?;
        Ok(Self(raw))
    }

    /// Parses a hexadecimal key representation.
    ///
    /// # Errors
    ///
    /// Returns `CoreError::ProtocolInvariant` when the input is not valid hex or has a
    /// non-32-byte decoded size.
    pub fn from_hex(hex: &str) -> Result<Self, CoreError> {
        let bytes =
            hex::decode(hex).map_err(|_| CoreError::ProtocolInvariant("invalid public key hex"))?;
        Self::try_from_bytes(&bytes)
    }

    /// Returns raw key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns lowercase hex for logging and serialization.
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PublicKey(<redacted>)")
    }
}

/// Zeroizing 64-byte device private-key material.
///
/// This type deliberately does not implement serialization. Callers must explicitly borrow the
/// bytes and choose a secure destination when performing a privileged export.
#[derive(Clone, Eq, PartialEq)]
pub struct PrivateKeyMaterial([u8; 64]);

impl PrivateKeyMaterial {
    /// Creates private-key material from an exact 64-byte slice.
    ///
    /// # Errors
    ///
    /// Returns `CoreError::ProtocolInvariant` when the input is not exactly 64 bytes.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, CoreError> {
        let raw: [u8; 64] = bytes
            .try_into()
            .map_err(|_| CoreError::ProtocolInvariant("private key must be exactly 64 bytes"))?;
        Ok(Self(raw))
    }

    /// Explicitly exposes the key bytes to a privileged caller.
    #[must_use]
    pub fn expose_secret(&self) -> &[u8; 64] {
        &self.0
    }
}

impl fmt::Debug for PrivateKeyMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PrivateKeyMaterial(<redacted>)")
    }
}

impl Zeroize for PrivateKeyMaterial {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

impl Drop for PrivateKeyMaterial {
    fn drop(&mut self) {
        self.zeroize();
    }
}

impl ZeroizeOnDrop for PrivateKeyMaterial {}

/// A path or destination fragment used by direct messaging.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Path {
    bytes: Vec<u8>,
}

impl Path {
    /// Maximum allowed path bytes in known firmware payload layouts.
    pub const MAX_BYTES: usize = 128;

    /// Creates a path with bounds checks.
    ///
    /// # Errors
    ///
    /// Returns `CoreError::ProtocolInvariant` when the input exceeds `MAX_BYTES`.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, CoreError> {
        if bytes.len() > Self::MAX_BYTES {
            return Err(CoreError::ProtocolInvariant("path exceeds maximum bytes"));
        }
        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }

    /// Returns the path bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Human-readable path bytes (hex).
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex::encode(&self.bytes)
    }
}

impl fmt::Debug for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Path(len={}, bytes=<redacted>)", self.bytes.len())
    }
}

/// Scope selection for commands that are default- or explicit-scoped.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Scope {
    /// Optional human-readable scope name.
    pub name: Option<String>,
    /// Scope secret bytes used for authenticated scope operations.
    #[serde(skip_serializing, default)]
    pub key: [u8; 16],
}

impl Scope {
    /// Creates a new scope reference.
    #[must_use]
    pub fn new(name: Option<String>, key: [u8; 16]) -> Self {
        Self { name, key }
    }

    /// Name if present.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

impl fmt::Debug for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scope")
            .field("name", &"<redacted>")
            .field("key", &"<redacted>")
            .finish()
    }
}

/// Flood scope applied to routing behavior.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum FloodScope {
    /// Use firmware default flood scope.
    Default,
    /// Disable scoping so the flood applies globally.
    Unscoped,
    /// Use a fixed 16-byte flood scope key.
    Key([u8; 16]),
}

impl fmt::Debug for FloodScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => write!(f, "FloodScope::Default"),
            Self::Unscoped => write!(f, "FloodScope::Unscoped"),
            Self::Key(_) => write!(f, "FloodScope::Key(<redacted>)"),
        }
    }
}

/// Device battery telemetry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BatteryInfo {
    /// Remaining battery level.
    pub level: u16,
    /// Used storage in kilobytes.
    pub used_kb: Option<u32>,
    /// Total storage in kilobytes.
    pub total_kb: Option<u32>,
}

/// Radio tuning and coding configuration values.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RadioParams {
    /// Center frequency in kHz.
    pub frequency_khz: u32,
    /// Channel bandwidth in Hz.
    pub bandwidth_hz: u32,
    /// Spreading factor setting.
    pub spreading_factor: u8,
    /// Coding rate setting.
    pub coding_rate: u8,
    /// Optional repeat enablement flag.
    pub repeat: Option<bool>,
}

/// Packet timing and scheduling parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TuningParams {
    /// Receive delay in milliseconds.
    pub rx_delay: u32,
    /// Airtime multiplier used by firmware scheduling.
    pub airtime_factor: u32,
}

/// Key/value custom variable entry.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CustomVariable {
    /// Variable key.
    pub key: String,
    /// Variable value.
    pub value: String,
}

impl fmt::Debug for CustomVariable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomVariable")
            .field("key_bytes", &self.key.len())
            .field("value_bytes", &self.value.len())
            .finish()
    }
}

/// Parsed custom variable payload.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CustomVariables {
    /// Raw payload bytes from the companion.
    pub raw: Vec<u8>,
    /// Parsed key-value pairs.
    pub entries: Vec<CustomVariable>,
}

/// Parsed route advertisement observation.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct AdvertPath {
    /// Unix-style seconds timestamp when path was received.
    pub received_at: u32,
    /// Route advertised for the path.
    pub route: ContactRoute,
    /// Observed route path bytes.
    pub path: Path,
}

/// Family of known telemetry stats message kinds.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StatsType {
    /// Baseline system stats.
    Core,
    /// Radio subsystem stats.
    Radio,
    /// Packet counters.
    Packets,
}

/// Statistics payloads by subsystem.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DeviceStats {
    /// Basic device counters.
    Core {
        /// Battery voltage in millivolts.
        battery_mv: u16,
        /// Seconds the device has been online.
        uptime_seconds: u32,
        /// Reported error count.
        errors: u16,
        /// Current outbound queue length.
        queue_length: u8,
    },
    /// Radio transport statistics.
    Radio {
        /// Reported noise floor.
        noise_floor: i16,
        /// Last received signal strength indicator.
        last_rssi: i8,
        /// Last signal to noise ratio.
        last_snr: f32,
        /// Accumulated transmit airtime in seconds.
        tx_airtime_seconds: u32,
        /// Accumulated receive airtime in seconds.
        rx_airtime_seconds: u32,
    },
    /// Packet-level counters.
    Packets {
        /// Packets received.
        recv: u32,
        /// Packets sent.
        sent: u32,
        /// Flood packets received.
        flood_recv: u32,
        /// Flood packets sent.
        flood_sent: u32,
        /// Direct packets received.
        direct_recv: u32,
        /// Direct packets sent.
        direct_sent: u32,
        /// Optional receive error count.
        recv_errors: Option<u32>,
    },
}

/// Successful authenticated session metadata returned by a remote node.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct LoginSession {
    /// Remote permission bits; bit zero denotes administrative access.
    pub permissions: u8,
    /// Six-byte remote public-key prefix used to match the response.
    pub pubkey_prefix: [u8; 6],
    /// Remote server timestamp on newer firmware.
    pub server_timestamp: Option<u32>,
    /// Remote ACL permission bits on newer firmware.
    pub acl_permissions: Option<u8>,
    /// Remote firmware feature-level marker on newer firmware.
    pub firmware_version_level: Option<u8>,
}

impl LoginSession {
    /// Whether the remote permission bits grant administrative access.
    #[must_use]
    pub const fn is_admin(&self) -> bool {
        self.permissions & 1 == 1
    }
}

impl fmt::Debug for LoginSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoginSession")
            .field("permissions", &self.permissions)
            .field("pubkey_prefix", &"<redacted>")
            .field("server_timestamp", &self.server_timestamp)
            .field("acl_permissions", &self.acl_permissions)
            .field("firmware_version_level", &self.firmware_version_level)
            .finish()
    }
}

/// Repeater/server status counters returned by a remote status request.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoteStatus {
    /// Six-byte source public-key prefix.
    pub pubkey_prefix: [u8; 6],
    /// Battery voltage in millivolts.
    pub battery_mv: u16,
    /// Current transmit queue length.
    pub tx_queue_length: u16,
    /// Radio noise floor.
    pub noise_floor: i16,
    /// Last received signal strength.
    pub last_rssi: i16,
    /// Total received packet count.
    pub packets_received: u32,
    /// Total sent packet count.
    pub packets_sent: u32,
    /// Total transmit airtime in seconds.
    pub tx_airtime_seconds: u32,
    /// Total uptime in seconds.
    pub uptime_seconds: u32,
    /// Sent flood-packet count.
    pub sent_flood: u32,
    /// Sent direct-packet count.
    pub sent_direct: u32,
    /// Received flood-packet count.
    pub received_flood: u32,
    /// Received direct-packet count.
    pub received_direct: u32,
    /// Full/error event count.
    pub error_events: u16,
    /// Last signal-to-noise ratio.
    pub last_snr: f32,
    /// Duplicate direct-packet count.
    pub direct_duplicates: u16,
    /// Duplicate flood-packet count.
    pub flood_duplicates: u16,
    /// Total receive airtime in seconds.
    pub rx_airtime_seconds: u32,
    /// Receive error count added by newer firmware.
    pub receive_errors: Option<u32>,
}

impl fmt::Debug for RemoteStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteStatus")
            .field("pubkey_prefix", &"<redacted>")
            .field("battery_mv", &self.battery_mv)
            .field("tx_queue_length", &self.tx_queue_length)
            .field("noise_floor", &self.noise_floor)
            .field("last_rssi", &self.last_rssi)
            .field("packets_received", &self.packets_received)
            .field("packets_sent", &self.packets_sent)
            .field("tx_airtime_seconds", &self.tx_airtime_seconds)
            .field("uptime_seconds", &self.uptime_seconds)
            .field("sent_flood", &self.sent_flood)
            .field("sent_direct", &self.sent_direct)
            .field("received_flood", &self.received_flood)
            .field("received_direct", &self.received_direct)
            .field("error_events", &self.error_events)
            .field("last_snr", &self.last_snr)
            .field("direct_duplicates", &self.direct_duplicates)
            .field("flood_duplicates", &self.flood_duplicates)
            .field("rx_airtime_seconds", &self.rx_airtime_seconds)
            .field("receive_errors", &self.receive_errors)
            .finish()
    }
}

/// Bounded telemetry bytes returned by a local or remote sensor request.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TelemetryResponse {
    /// Six-byte source public-key prefix.
    pub pubkey_prefix: [u8; 6],
    /// Cayenne-LPP-compatible telemetry payload bytes.
    pub payload: Vec<u8>,
}

impl fmt::Debug for TelemetryResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TelemetryResponse(pubkey_prefix=<redacted>, payload_len={})",
            self.payload.len()
        )
    }
}

/// Correlated response bytes returned by a binary request.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct BinaryResponse {
    /// Four-byte response correlation tag.
    pub tag: [u8; 4],
    /// Bounded response payload.
    pub payload: Vec<u8>,
}

impl fmt::Debug for BinaryResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BinaryResponse(tag=<redacted>, payload_len={})",
            self.payload.len()
        )
    }
}

/// Received control-plane bytes and their radio metadata.
///
/// The control payload is deliberately omitted from debug output because individual control
/// families can contain public keys or other identifiers.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControlData {
    /// Received signal-to-noise ratio in signed quarter-decibel units.
    pub snr_qdb: i8,
    /// Received signal strength in dBm.
    pub rssi: i8,
    /// Path length reported by the companion.
    pub path_len: u8,
    /// Bounded raw control payload.
    pub payload: Vec<u8>,
}

impl ControlData {
    /// Strictly decodes a node-discovery response when this is control family `0x9x`.
    ///
    /// Other control families return `Ok(None)`. A matching node-discovery response must contain
    /// the type byte, signed inbound SNR, a non-zero little-endian correlation tag, and exactly an
    /// 8-byte public-key prefix or a 32-byte full public key.
    ///
    /// # Errors
    ///
    /// Returns [`ParseError`] when a node-discovery response is truncated, has a partial or
    /// trailing key width, or contains a zero correlation tag.
    pub fn node_discovery_response(&self) -> Result<Option<NodeDiscoveryResponse>, ParseError> {
        const PREFIX_RESPONSE_BYTES: usize = 1 + 1 + 4 + 8;
        const FULL_RESPONSE_BYTES: usize = 1 + 1 + 4 + 32;

        let Some(control_type) = self.payload.first().copied() else {
            return Err(ParseError::InvalidPacketLength {
                code: PacketDisplay::Raw(0x8e),
                minimum: 1,
                actual: 0,
            });
        };
        if control_type & 0xf0 != 0x90 {
            return Ok(None);
        }

        if self.payload.len() < PREFIX_RESPONSE_BYTES {
            return Err(ParseError::InvalidPacketLength {
                code: PacketDisplay::Raw(0x8e),
                minimum: PREFIX_RESPONSE_BYTES,
                actual: self.payload.len(),
            });
        }
        if !matches!(
            self.payload.len(),
            PREFIX_RESPONSE_BYTES | FULL_RESPONSE_BYTES
        ) {
            return Err(ParseError::Malformed {
                reason: "node-discovery response public key must be exactly 8 or 32 bytes",
            });
        }

        let tag = u32::from_le_bytes([
            self.payload[2],
            self.payload[3],
            self.payload[4],
            self.payload[5],
        ]);
        if tag == 0 {
            return Err(ParseError::Malformed {
                reason: "node-discovery response tag must be non-zero",
            });
        }

        Ok(Some(NodeDiscoveryResponse {
            node_type: control_type & 0x0f,
            inbound_snr_qdb: i8::from_le_bytes([self.payload[1]]),
            tag,
            public_key: self.payload[6..].to_vec(),
        }))
    }
}

impl fmt::Debug for ControlData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlData")
            .field("snr_qdb", &self.snr_qdb)
            .field("rssi", &self.rssi)
            .field("path_len", &self.path_len)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

/// Strictly decoded node-discovery response from a control-data event.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeDiscoveryResponse {
    /// Node advertisement type encoded in the low nibble of the control type.
    pub node_type: u8,
    /// Signal-to-noise ratio observed by the responding node, in signed quarter-decibel units.
    pub inbound_snr_qdb: i8,
    /// Non-zero request correlation tag reflected by the responder.
    pub tag: u32,
    /// Exact 8-byte public-key prefix or 32-byte full public key.
    pub public_key: Vec<u8>,
}

impl fmt::Debug for NodeDiscoveryResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeDiscoveryResponse")
            .field("node_type", &self.node_type)
            .field("inbound_snr_qdb", &self.inbound_snr_qdb)
            .field("tag", &"<redacted>")
            .field("public_key_len", &self.public_key.len())
            .finish()
    }
}

/// Route pair returned by path discovery.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PathDiscovery {
    /// Six-byte contact public-key prefix.
    pub pubkey_prefix: [u8; 6],
    /// Outbound route descriptor.
    pub outbound_route: ContactRoute,
    /// Outbound path hashes.
    pub outbound_path: Path,
    /// Inbound route descriptor.
    pub inbound_route: ContactRoute,
    /// Inbound path hashes.
    pub inbound_path: Path,
}

impl fmt::Debug for PathDiscovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PathDiscovery(pubkey_prefix=<redacted>, outbound_route={:?}, outbound_path_len={}, inbound_route={:?}, inbound_path_len={})",
            self.outbound_route,
            self.outbound_path.as_bytes().len(),
            self.inbound_route,
            self.inbound_path.as_bytes().len()
        )
    }
}

/// Exact 64-byte Ed25519 signature returned by the companion signing flow.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct Signature {
    bytes: Vec<u8>,
}

impl Signature {
    /// Creates a signature from exactly 64 bytes.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::ProtocolInvariant`] for any other byte length.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, CoreError> {
        if bytes.len() != 64 {
            return Err(CoreError::ProtocolInvariant(
                "signature must be exactly 64 bytes",
            ));
        }
        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }

    /// Borrows signature bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Signature(len={})", self.bytes.len())
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Self::try_from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}

/// Auto-add contact configuration parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AutoAddConfig {
    /// Configuration mode selector.
    pub config: u8,
    /// Optional max hop count.
    pub max_hops: Option<u8>,
}

/// Frequency span used for repeat filtering.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FrequencyRange {
    /// Inclusive lower frequency in kHz.
    pub lower_khz: u32,
    /// Inclusive upper frequency in kHz.
    pub upper_khz: u32,
}

/// Named wrapper for configured default flood scope.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum DefaultFloodScope {
    /// Scope state not yet provided.
    Unconfigured,
    /// Scope state has been provisioned.
    Configured(Scope),
}

/// Contact URI payload with optional companion card bytes.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ContactUri {
    /// URI value.
    pub uri: String,
    /// Companion card payload bytes.
    pub card: Vec<u8>,
}

impl fmt::Debug for ContactUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContactUri(card_len={})", self.card.len())
    }
}

impl fmt::Debug for CustomVariables {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CustomVariables(raw_len={}, entry_count={})",
            self.raw.len(),
            self.entries.len()
        )
    }
}

impl fmt::Debug for AdvertPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AdvertPath(received_at={}, route={:?}, path_len={})",
            self.received_at,
            self.route,
            self.path.as_bytes().len()
        )
    }
}

impl fmt::Debug for DefaultFloodScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unconfigured => write!(f, "DefaultFloodScope::Unconfigured"),
            Self::Configured(scope) => write!(f, "DefaultFloodScope::Configured({scope:?})"),
        }
    }
}

/// Contact class from device address book payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ContactType {
    /// Standard direct chat contact.
    Chat,
    /// Mesh relay/repeater contact.
    Repeater,
    /// Group room contact.
    Room,
    /// Sensor/contact type that publishes telemetry.
    Sensor,
    /// Unknown scope-specific contact type.
    Unknown(u8),
}

impl ContactType {
    /// Converts a firmware contact class byte.
    #[must_use]
    pub fn from_u8(raw: u8) -> Self {
        match raw {
            0 => Self::Chat,
            1 => Self::Repeater,
            2 => Self::Room,
            3 => Self::Sensor,
            other => Self::Unknown(other),
        }
    }
}

/// A resolved contact row.
#[derive(Clone, Serialize, Deserialize)]
pub struct Contact {
    /// Public key identifying the contact.
    pub public_key: PublicKey,
    /// Contact type classifier.
    pub contact_type: ContactType,
    /// Firmware bit flags attached to the contact record.
    pub flags: u8,
    /// Routing strategy used for this contact.
    pub route: ContactRoute,
    /// Advertised path prefix for the contact.
    pub out_path: Path,
    /// Display name advertised by contact.
    pub adv_name: String,
    /// Last modification counter from contact directory.
    pub last_advert: u32,
    /// Latitude in radians from firmware payload scaling.
    pub adv_lat: f64,
    /// Longitude in radians from firmware payload scaling.
    pub adv_lon: f64,
    /// Last modification timestamp for list diffing.
    pub lastmod: u32,
}

impl fmt::Debug for Contact {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Contact")
            .field("public_key", &"<redacted>")
            .field("contact_type", &self.contact_type)
            .field("flags", &self.flags)
            .field("route", &self.route)
            .field("out_path", &"<redacted>")
            .field("adv_name", &"<redacted>")
            .field("last_advert", &self.last_advert)
            .field("adv_lat", &"<redacted>")
            .field("adv_lon", &"<redacted>")
            .field("lastmod", &self.lastmod)
            .finish()
    }
}

/// Route advertised for a saved contact.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ContactRoute {
    /// Firmware will flood messages for this contact.
    Flood,
    /// Firmware has a concrete path to the contact.
    Path {
        /// Path hash mode encoded in the high two bits of the route byte.
        hash_mode: u8,
        /// Number of hops encoded in the low six bits of the route byte.
        hop_count: u8,
    },
}

/// Session metadata returned by `APP_START`.
#[derive(Clone, Serialize, Deserialize)]
pub struct SelfInfo {
    /// Advertising mode for outbound identity packets.
    pub advertising_type: u8,
    /// Current transmit power in dBm units.
    pub tx_power: u8,
    /// Maximum supported transmit power in dBm units.
    pub max_tx_power: u8,
    /// Public key of this device.
    pub public_key: PublicKey,
    /// Last reported latitude from firmware.
    pub adv_lat: f64,
    /// Last reported longitude from firmware.
    pub adv_lon: f64,
    /// Number of acknowledgements the firmware can send in one burst.
    pub multi_acks: u8,
    /// Advertising location policy bitmask.
    pub advert_loc_policy: u8,
    /// Environmental telemetry mode bits.
    pub telemetry_mode_env: u8,
    /// Location telemetry mode bits.
    pub telemetry_mode_loc: u8,
    /// Base telemetry mode bits.
    pub telemetry_mode_base: u8,
    /// Whether manual contact additions are enabled.
    pub manual_add_contacts: bool,
    /// Current radio center frequency in MHz.
    pub radio_frequency_mhz: f64,
    /// Current radio channel bandwidth in kHz.
    pub radio_bandwidth_khz: f64,
    /// Current `LoRa` spreading factor value.
    pub radio_spreading_factor: u8,
    /// Current coding rate selector.
    pub radio_coding_rate: u8,
    /// Device short name.
    pub name: String,
}

impl fmt::Debug for SelfInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SelfInfo")
            .field("advertising_type", &self.advertising_type)
            .field("tx_power", &self.tx_power)
            .field("max_tx_power", &self.max_tx_power)
            .field("public_key", &"<redacted>")
            .field("adv_lat", &"<redacted>")
            .field("adv_lon", &"<redacted>")
            .field("multi_acks", &self.multi_acks)
            .field("advert_loc_policy", &self.advert_loc_policy)
            .field("telemetry_mode_env", &self.telemetry_mode_env)
            .field("telemetry_mode_loc", &self.telemetry_mode_loc)
            .field("telemetry_mode_base", &self.telemetry_mode_base)
            .field("manual_add_contacts", &self.manual_add_contacts)
            .field("radio_frequency_mhz", &self.radio_frequency_mhz)
            .field("radio_bandwidth_khz", &self.radio_bandwidth_khz)
            .field("radio_spreading_factor", &self.radio_spreading_factor)
            .field("radio_coding_rate", &self.radio_coding_rate)
            .field("name", &"<redacted>")
            .finish()
    }
}

/// Firmware/build metadata returned by `DEVICE_QUERY`.
#[derive(Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    /// Firmware protocol version.
    pub protocol_version: u8,
    /// Max supported contact rows, if known.
    pub max_contacts: Option<u16>,
    /// Max supported channel count, if known.
    pub max_channels: Option<u8>,
    /// BLE pin from firmware, if reported.
    #[serde(skip_serializing, default)]
    pub ble_pin: Option<u32>,
    /// Firmware build string, if known.
    pub firmware_build: Option<String>,
    /// Device model string, if known.
    pub model: Option<String>,
    /// Firmware version string, if known.
    pub firmware_version: Option<String>,
    /// Repeat mode enablement flag from firmware.
    pub repeat_enabled: Option<bool>,
    /// Path hash mode preference.
    pub path_hash_mode: Option<u8>,
}

impl fmt::Debug for DeviceInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceInfo")
            .field("protocol_version", &self.protocol_version)
            .field("max_contacts", &self.max_contacts)
            .field("max_channels", &self.max_channels)
            .field("ble_pin", &"<redacted>")
            .field("firmware_build", &self.firmware_build)
            .field("model", &self.model)
            .field("firmware_version", &self.firmware_version)
            .field("repeat_enabled", &self.repeat_enabled)
            .field("path_hash_mode", &self.path_hash_mode)
            .finish()
    }
}

/// Status attached to parsed messages in the event stream.
#[derive(Clone, Serialize, Deserialize)]
pub enum MessageStatus {
    /// Message was received from the companion device.
    Received,
    /// Transport accepted and waiting for firmware send confirmation.
    Queued,
    /// Firmware returned a `msg sent` packet.
    Sent {
        /// Timeout hint from firmware.
        suggested_timeout_ms: Option<u32>,
    },
    /// Delivery ack was observed.
    Acked,
    /// A request failed and carried a text reason.
    Failed(String),
}

impl fmt::Debug for MessageStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Received => write!(f, "Received"),
            Self::Queued => write!(f, "Queued"),
            Self::Sent {
                suggested_timeout_ms,
            } => f
                .debug_struct("Sent")
                .field("suggested_timeout_ms", suggested_timeout_ms)
                .finish(),
            Self::Acked => write!(f, "Acked"),
            Self::Failed(_) => write!(f, "Failed(<redacted>)"),
        }
    }
}

/// Message origin when consumed by host clients.
#[derive(Clone, Serialize, Deserialize)]
pub enum MessageSource {
    /// Direct contact delivery with destination pubkey prefix.
    Direct {
        /// Prefix string in hexadecimal.
        pubkey_prefix: String,
    },
    /// Broadcast-like channel message.
    Channel {
        /// Channel index.
        channel_idx: u8,
    },
}

impl fmt::Debug for MessageSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct { .. } => write!(f, "Direct {{ pubkey_prefix: <redacted> }}"),
            Self::Channel { channel_idx } => f
                .debug_struct("Channel")
                .field("channel_idx", channel_idx)
                .finish(),
        }
    }
}

/// A parsed inbound message packet.
#[derive(Clone, Serialize, Deserialize)]
pub struct Message {
    /// Where the message originated.
    pub source: MessageSource,
    /// Routing path metadata.
    pub route: MessageRoute,
    /// Message type discriminator from payload.
    pub txt_type: u8,
    /// Sender timestamp encoded in firmware wire format.
    pub sender_timestamp: u32,
    /// Optional signature bytes for authenticated direct text.
    pub signature: Option<[u8; 4]>,
    /// Payload text.
    pub text: String,
    /// Optional signal-to-noise ratio.
    pub snr: Option<f32>,
    /// Delivery state for host consumption.
    pub status: MessageStatus,
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Message")
            .field("source", &self.source)
            .field("route", &self.route)
            .field("txt_type", &self.txt_type)
            .field("sender_timestamp", &self.sender_timestamp)
            .field("signature", &self.signature.map(|_| "<redacted>"))
            .field("text", &"<redacted>")
            .field("snr", &self.snr)
            .field("status", &self.status)
            .finish()
    }
}

/// Route descriptor included with an inbound message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MessageRoute {
    /// The firmware marks the message as direct with route byte `0xff`.
    Direct,
    /// The firmware supplied a path descriptor.
    Path {
        /// Path hash mode encoded in the high two bits of the route byte.
        hash_mode: u8,
        /// Number of hops encoded in the low six bits of the route byte.
        hop_count: u8,
    },
}

/// Firmware ACK payload.
#[derive(Clone, Serialize, Deserialize)]
pub struct Ack {
    /// Four-byte acknowledgement code.
    pub code: [u8; 4],
    /// Optional round-trip time estimate in milliseconds.
    pub trip_time_ms: Option<u32>,
}

impl fmt::Debug for Ack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Ack(code={}, trip_time_ms={:?})",
            hex::encode(self.code),
            self.trip_time_ms
        )
    }
}

/// Application-level events emitted from the reader loop.
#[derive(Clone, Serialize, Deserialize)]
pub enum Event {
    /// Companion transport is connected.
    Connected,
    /// Companion transport disconnected.
    Disconnected,
    /// Initial batch of contacts was received.
    Contacts {
        /// Contact entries included in the batch.
        contacts: Vec<Contact>,
        /// Last modified sequence reported by firmware.
        lastmod: u32,
    },
    /// Self metadata was received.
    SelfInfo(SelfInfo),
    /// Device metadata was received.
    DeviceInfo(DeviceInfo),
    /// Message from companion payload.
    Message(Message),
    /// Channel info metadata.
    ChannelInfo {
        /// Channel index.
        idx: u8,
        /// Channel display name.
        name: String,
        /// Optional precomputed secret hash for redaction.
        secret_hash: Option<u8>,
    },
    /// Firmware ACK packet converted into event form.
    Ack(Ack),
    /// Firmware emitted a message-sent status.
    MessageSent {
        /// Destination type from firmware.
        destination_type: u8,
        /// ACK tracking code for the sent message.
        ack_code: [u8; 4],
        /// Suggested client timeout for completion.
        suggested_timeout_ms: u32,
    },
    /// Timestamp sync event.
    CurrentTime(u32),
    /// Battery metadata event.
    Battery {
        /// Remaining battery level.
        level: u16,
        /// Optional used storage in kilobytes.
        used_kb: Option<u32>,
        /// Optional total storage in kilobytes.
        total_kb: Option<u32>,
    },
    /// Contact URI event.
    ContactUri(ContactUri),
    /// Tuning parameter response.
    TuningParams(TuningParams),
    /// Custom variable payload response.
    CustomVariables(CustomVariables),
    /// Advert path payload response.
    AdvertPath(AdvertPath),
    /// Device statistics payload.
    DeviceStats(DeviceStats),
    /// Auto-add configuration payload.
    AutoAddConfig(AutoAddConfig),
    /// Allowed repeat frequency list.
    AllowedRepeatFrequencies(Vec<FrequencyRange>),
    /// Default flood scope payload.
    DefaultFloodScope(DefaultFloodScope),
    /// Remote login completed successfully.
    LoginSucceeded(LoginSession),
    /// Remote login was rejected.
    LoginFailed {
        /// Six-byte remote public-key prefix.
        pubkey_prefix: [u8; 6],
    },
    /// Remote status response.
    RemoteStatus(RemoteStatus),
    /// Local or remote telemetry response.
    Telemetry(TelemetryResponse),
    /// Correlated raw binary response.
    BinaryResponse(BinaryResponse),
    /// Received control-plane data and radio metadata.
    ControlData(ControlData),
    /// Discovered inbound and outbound paths.
    PathDiscovery(PathDiscovery),
    /// Device-generated signature.
    Signature(Signature),
    /// Companion queue is currently empty.
    InboxEmpty,
    /// Additional messages are queued for polling.
    MessagesWaiting,
    /// Protocol-level error payload converted to event.
    ProtocolError(String),
    /// Non-standard payload for compatibility diagnostics.
    UnknownPacket {
        /// Packet code that was not modelled.
        code: u8,
        /// Raw payload bytes.
        payload: Vec<u8>,
    },
}

impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connected => write!(f, "Event::Connected"),
            Self::Disconnected => write!(f, "Event::Disconnected"),
            Self::Contacts { contacts, lastmod } => write!(
                f,
                "Event::Contacts(count={}, lastmod={lastmod})",
                contacts.len()
            ),
            Self::SelfInfo(_) => write!(f, "Event::SelfInfo(<redacted>)"),
            Self::DeviceInfo(info) => write!(f, "Event::DeviceInfo({info:?})"),
            Self::Message(message) => write!(f, "Event::Message({message:?})"),
            Self::ChannelInfo { idx, .. } => {
                write!(
                    f,
                    "Event::ChannelInfo(idx={idx}, name=<redacted>, secret=<redacted>)"
                )
            }
            Self::Ack(ack) => write!(f, "Event::Ack({ack:?})"),
            Self::MessageSent {
                destination_type,
                suggested_timeout_ms,
                ..
            } => write!(
                f,
                "Event::MessageSent(destination_type={destination_type}, ack_code=<redacted>, suggested_timeout_ms={suggested_timeout_ms})"
            ),
            Self::CurrentTime(timestamp) => write!(f, "Event::CurrentTime({timestamp})"),
            Self::Battery {
                level,
                used_kb,
                total_kb,
            } => write!(
                f,
                "Event::Battery(level={level}, used_kb={used_kb:?}, total_kb={total_kb:?})"
            ),
            Self::ContactUri(uri) => write!(f, "Event::ContactUri({uri:?})"),
            Self::TuningParams(params) => write!(f, "Event::TuningParams({params:?})"),
            Self::CustomVariables(vars) => write!(f, "Event::CustomVariables({vars:?})"),
            Self::AdvertPath(path) => write!(f, "Event::AdvertPath({path:?})"),
            Self::DeviceStats(stats) => write!(f, "Event::DeviceStats({stats:?})"),
            Self::AutoAddConfig(config) => write!(f, "Event::AutoAddConfig({config:?})"),
            Self::AllowedRepeatFrequencies(ranges) => {
                write!(f, "Event::AllowedRepeatFrequencies(count={})", ranges.len())
            }
            Self::DefaultFloodScope(scope) => write!(f, "Event::DefaultFloodScope({scope:?})"),
            Self::LoginSucceeded(session) => write!(f, "Event::LoginSucceeded({session:?})"),
            Self::LoginFailed { .. } => {
                write!(f, "Event::LoginFailed(pubkey_prefix=<redacted>)")
            }
            Self::RemoteStatus(status) => write!(f, "Event::RemoteStatus({status:?})"),
            Self::Telemetry(response) => write!(f, "Event::Telemetry({response:?})"),
            Self::BinaryResponse(response) => write!(f, "Event::BinaryResponse({response:?})"),
            Self::ControlData(data) => write!(f, "Event::ControlData({data:?})"),
            Self::PathDiscovery(path) => write!(f, "Event::PathDiscovery({path:?})"),
            Self::Signature(signature) => write!(f, "Event::Signature({signature:?})"),
            Self::InboxEmpty => write!(f, "Event::InboxEmpty"),
            Self::MessagesWaiting => write!(f, "Event::MessagesWaiting"),
            Self::ProtocolError(reason) => {
                write!(f, "Event::ProtocolError(reason_len={})", reason.len())
            }
            Self::UnknownPacket { code, payload } => write!(
                f,
                "Event::UnknownPacket(code={code:#04x}, payload_len={})",
                payload.len()
            ),
        }
    }
}

/// Channel metadata used by the `get channel` command.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ChannelInfo {
    /// Channel index.
    pub idx: u8,
    /// Human-readable channel name.
    pub name: String,
    /// Optional short hash of the secret for redaction.
    pub secret_hash: Option<u8>,
    #[serde(skip)]
    secret: SecretBytes,
}

impl ChannelInfo {
    /// Creates a helper value with an optional channel secret payload.
    #[must_use]
    pub fn with_secret(idx: u8, name: String, secret: Option<[u8; 16]>) -> Self {
        let secret_hash = secret
            .as_ref()
            .map(|value| Sha256::digest(value).first().copied().unwrap_or_default());
        Self {
            idx,
            name,
            secret_hash,
            secret: SecretBytes::from(secret),
        }
    }

    /// Borrows zeroized secret bytes if available.
    #[must_use]
    pub fn secret(&self) -> Option<&[u8; 16]> {
        self.secret.0.as_ref()
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
struct SecretBytes(Option<[u8; 16]>);

impl SecretBytes {
    fn from(value: Option<[u8; 16]>) -> Self {
        Self(value)
    }
}

impl Zeroize for SecretBytes {
    fn zeroize(&mut self) {
        if let Some(bytes) = self.0.as_mut() {
            bytes.zeroize();
        }
        self.0 = None;
    }
}

impl Drop for SecretBytes {
    fn drop(&mut self) {
        self.zeroize();
    }
}

impl ZeroizeOnDrop for SecretBytes {}

impl fmt::Debug for ChannelInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ChannelInfo(idx={}, name=<redacted>, secret_hash={}, secret=<redacted>)",
            self.idx,
            self.secret_hash
                .map_or_else(|| "none".to_owned(), |value| format!("{value:02x}"))
        )
    }
}

/// Outstanding ack tracker state used by the client.
#[derive(Clone, Debug)]
pub struct CommandTracking {
    /// ACK code expected by firmware for this command.
    pub ack_code: [u8; 4],
    /// Suggested timeout in milliseconds from firmware.
    pub timeout_ms: u32,
}
