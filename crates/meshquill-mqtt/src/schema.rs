use meshquill_core::{
    Ack, BatteryInfo, Contact, ContactRoute, ContactSnapshot, ContactType, DeviceStats, Event,
    Message, MessageRoute, MessageStatus, TelemetryResponse,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::config::{MAX_COMMAND_DESTINATION_BYTES, MAX_COMMAND_TEXT_BYTES};
use crate::topics::SCHEMA_VERSION;

/// Event type names defined by the `meshquill.mqtt/v1` schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    /// An incoming `MeshCore` text message.
    IncomingMessage,
    /// A `MeshCore` acknowledgement.
    Ack,
    /// A `MeshCore` or MQTT broker connection state transition.
    ConnectionState,
    /// A contact directory snapshot.
    Contacts,
    /// Device or contact telemetry.
    Telemetry,
    /// Allowlisted request to send a direct message.
    SendDirect,
    /// Allowlisted request to send a channel message.
    SendChannel,
}

/// Versioned JSON event envelope shared by publications and outbound commands.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventEnvelope {
    /// Exact schema identifier. It must equal `meshquill.mqtt/v1`.
    pub schema: String,
    /// Non-nil producer-assigned ID used as a bounded, process-local deduplication key.
    #[serde(deserialize_with = "deserialize_canonical_uuid")]
    pub event_id: Uuid,
    /// Stable identifier of the producing application instance.
    pub origin: String,
    /// Milliseconds since the Unix epoch.
    pub timestamp: u64,
    /// Versioned event discriminator.
    #[serde(rename = "type")]
    pub kind: EventKind,
    /// Event-specific JSON object.
    pub data: Value,
}

impl EventEnvelope {
    /// Creates and validates an envelope from already serialized event data.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError`] for a nil ID, invalid origin, or data that does
    /// not match the selected v1 discriminator.
    pub fn new(
        event_id: Uuid,
        origin: impl Into<String>,
        timestamp: u64,
        kind: EventKind,
        data: Value,
    ) -> Result<Self, SchemaError> {
        let envelope = Self {
            schema: SCHEMA_VERSION.to_owned(),
            event_id,
            origin: origin.into(),
            timestamp,
            kind,
            data,
        };
        envelope.validate()?;
        Ok(envelope)
    }

    /// Converts a typed gateway publication into a validated envelope.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError`] if publication serialization or envelope validation fails.
    pub fn from_publication(
        event_id: Uuid,
        origin: impl Into<String>,
        timestamp: u64,
        publication: &Publication,
    ) -> Result<Self, SchemaError> {
        Self::new(
            event_id,
            origin,
            timestamp,
            publication.kind(),
            publication.data_value()?,
        )
    }

    /// Validates fixed envelope invariants and discriminator-specific data.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError`] when any v1 contract invariant is violated.
    pub fn validate(&self) -> Result<(), SchemaError> {
        if self.schema != SCHEMA_VERSION {
            return Err(SchemaError::UnsupportedSchema(self.schema.clone()));
        }
        if self.event_id.is_nil() {
            return Err(SchemaError::NilEventId);
        }
        validate_origin(&self.origin)?;
        if !self.data.is_object() {
            return Err(SchemaError::DataMustBeObject);
        }
        validate_event_data(self.kind, &self.data)
    }

    /// Serializes the envelope as compact UTF-8 JSON.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::Json`] if serialization fails.
    pub fn encode(&self) -> Result<Vec<u8>, SchemaError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(SchemaError::Json)
    }

    /// Parses and validates a compact or pretty-printed JSON envelope.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError`] for malformed JSON or a schema invariant violation.
    pub fn decode(payload: &[u8]) -> Result<Self, SchemaError> {
        let envelope: Self = serde_json::from_slice(payload).map_err(SchemaError::Json)?;
        envelope.validate()?;
        Ok(envelope)
    }
}

/// Source connection described by a connection-state event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionComponent {
    /// The local `MeshCore` companion/application connection.
    MeshCore,
    /// The gateway's MQTT broker connection.
    MqttBroker,
}

/// Connected/disconnected state encoded in a connection event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionStatus {
    /// The component is connected.
    Connected,
    /// The component is disconnected.
    Disconnected,
}

/// Data published with a connection-state event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionStateData {
    /// Connection being described.
    pub component: ConnectionComponent,
    /// Current state.
    pub status: ConnectionStatus,
    /// Optional non-sensitive, stable reason code.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Stable MQTT v1 representation of a contact class.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContactTypeData {
    /// Standard direct chat contact.
    Chat,
    /// Mesh relay/repeater contact.
    Repeater,
    /// Group room contact.
    Room,
    /// Sensor contact.
    Sensor,
    /// Firmware contact class not known to this version.
    Unknown {
        /// Original firmware class byte.
        code: u8,
    },
}

impl From<ContactType> for ContactTypeData {
    fn from(value: ContactType) -> Self {
        match value {
            ContactType::Chat => Self::Chat,
            ContactType::Repeater => Self::Repeater,
            ContactType::Room => Self::Room,
            ContactType::Sensor => Self::Sensor,
            ContactType::Unknown(code) => Self::Unknown { code },
        }
    }
}

/// Stable MQTT v1 representation of a saved contact route.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContactRouteData {
    /// Firmware-selected flooding.
    Flood,
    /// Concrete route descriptor.
    Path {
        /// Path hash mode.
        hash_mode: u8,
        /// Route hop count.
        hop_count: u8,
    },
}

impl From<ContactRoute> for ContactRouteData {
    fn from(value: ContactRoute) -> Self {
        match value {
            ContactRoute::Flood => Self::Flood,
            ContactRoute::Path {
                hash_mode,
                hop_count,
            } => Self::Path {
                hash_mode,
                hop_count,
            },
        }
    }
}

/// Stable MQTT v1 contact row.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContactData {
    /// Full 32-byte public key as lowercase hexadecimal.
    pub public_key: String,
    /// Contact class.
    pub contact_type: ContactTypeData,
    /// Firmware bit flags.
    pub flags: u8,
    /// Contact route descriptor.
    pub route: ContactRouteData,
    /// Advertised route path bytes as lowercase hexadecimal.
    pub out_path: String,
    /// Advertised display name.
    pub adv_name: String,
    /// Last advert counter.
    pub last_advert: u32,
    /// Advertised latitude in radians.
    pub adv_lat: f64,
    /// Advertised longitude in radians.
    pub adv_lon: f64,
    /// Per-row last-modified counter.
    pub lastmod: u32,
}

impl From<&Contact> for ContactData {
    fn from(value: &Contact) -> Self {
        Self {
            public_key: value.public_key.to_hex(),
            contact_type: value.contact_type.into(),
            flags: value.flags,
            route: value.route.into(),
            out_path: value.out_path.to_hex(),
            adv_name: value.adv_name.clone(),
            last_advert: value.last_advert,
            adv_lat: value.adv_lat,
            adv_lon: value.adv_lon,
            lastmod: value.lastmod,
        }
    }
}

/// Data published with a contact snapshot event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContactsData {
    /// Contact rows in this snapshot.
    pub contacts: Vec<ContactData>,
    /// `MeshCore` last-modified sequence for the snapshot.
    pub lastmod: u32,
}

impl From<ContactSnapshot> for ContactsData {
    fn from(value: ContactSnapshot) -> Self {
        Self {
            contacts: value.contacts.iter().map(ContactData::from).collect(),
            lastmod: value.lastmod,
        }
    }
}

/// Stable MQTT v1 representation of an incoming message source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MessageSourceData {
    /// Direct message carrying a six-byte public-key prefix.
    Direct {
        /// Source public-key prefix as lowercase hexadecimal.
        pubkey_prefix: String,
    },
    /// Channel message.
    Channel {
        /// Numeric channel index.
        channel_idx: u8,
    },
}

/// Stable MQTT v1 representation of inbound route metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MessageRouteData {
    /// Direct route marker.
    Direct,
    /// Concrete route descriptor.
    Path {
        /// Path hash mode.
        hash_mode: u8,
        /// Route hop count.
        hop_count: u8,
    },
}

impl From<MessageRoute> for MessageRouteData {
    fn from(value: MessageRoute) -> Self {
        match value {
            MessageRoute::Direct => Self::Direct,
            MessageRoute::Path {
                hash_mode,
                hop_count,
            } => Self::Path {
                hash_mode,
                hop_count,
            },
        }
    }
}

/// Stable MQTT v1 delivery state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MessageStatusData {
    /// Received from the companion.
    Received,
    /// Accepted for outbound delivery.
    Queued,
    /// Firmware reported the message sent.
    Sent {
        /// Optional firmware timeout hint.
        #[serde(skip_serializing_if = "Option::is_none")]
        suggested_timeout_ms: Option<u32>,
    },
    /// Delivery acknowledgement observed.
    Acked,
    /// Delivery failed.
    Failed {
        /// Failure reason supplied by the core event.
        reason: String,
    },
}

/// Stable MQTT v1 incoming-message payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IncomingMessageData {
    /// Logical message source.
    pub source: MessageSourceData,
    /// Route descriptor.
    pub route: MessageRouteData,
    /// Firmware text type.
    pub txt_type: u8,
    /// Sender timestamp.
    pub sender_timestamp: u32,
    /// Optional four-byte signature as lowercase hexadecimal.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    /// UTF-8 message text.
    pub text: String,
    /// Optional signal-to-noise ratio.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snr: Option<f32>,
    /// Delivery state.
    pub status: MessageStatusData,
}

impl From<Message> for IncomingMessageData {
    fn from(value: Message) -> Self {
        let source = match value.source {
            meshquill_core::domain::MessageSource::Direct { pubkey_prefix } => {
                MessageSourceData::Direct {
                    pubkey_prefix: pubkey_prefix.to_ascii_lowercase(),
                }
            }
            meshquill_core::domain::MessageSource::Channel { channel_idx } => {
                MessageSourceData::Channel { channel_idx }
            }
        };
        let status = match value.status {
            MessageStatus::Received => MessageStatusData::Received,
            MessageStatus::Queued => MessageStatusData::Queued,
            MessageStatus::Sent {
                suggested_timeout_ms,
            } => MessageStatusData::Sent {
                suggested_timeout_ms,
            },
            MessageStatus::Acked => MessageStatusData::Acked,
            MessageStatus::Failed(reason) => MessageStatusData::Failed { reason },
        };
        Self {
            source,
            route: value.route.into(),
            txt_type: value.txt_type,
            sender_timestamp: value.sender_timestamp,
            signature: value.signature.map(|bytes| lowercase_hex(&bytes)),
            text: value.text,
            snr: value.snr,
            status,
        }
    }
}

/// Stable MQTT v1 acknowledgement payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AckData {
    /// Four-byte acknowledgement code as lowercase hexadecimal.
    pub code: String,
    /// Optional round-trip estimate in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trip_time_ms: Option<u32>,
}

impl From<Ack> for AckData {
    fn from(value: Ack) -> Self {
        Self {
            code: lowercase_hex(&value.code),
            trip_time_ms: value.trip_time_ms,
        }
    }
}

/// Closed set of telemetry payloads defined by MQTT v1.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TelemetryData {
    /// Battery and optional storage readings.
    Battery {
        /// Battery level reported by firmware.
        level: u16,
        /// Used storage in kilobytes.
        #[serde(skip_serializing_if = "Option::is_none")]
        used_kb: Option<u32>,
        /// Total storage in kilobytes.
        #[serde(skip_serializing_if = "Option::is_none")]
        total_kb: Option<u32>,
    },
    /// Core device counters.
    StatsCore {
        /// Battery voltage in millivolts.
        battery_mv: u16,
        /// Device uptime in seconds.
        uptime_seconds: u32,
        /// Reported error count.
        errors: u16,
        /// Current outbound queue length.
        queue_length: u8,
    },
    /// Radio subsystem counters.
    StatsRadio {
        /// Radio noise floor.
        noise_floor: i16,
        /// Last received signal strength.
        last_rssi: i8,
        /// Last signal-to-noise ratio.
        last_snr: f32,
        /// Transmit airtime in seconds.
        tx_airtime_seconds: u32,
        /// Receive airtime in seconds.
        rx_airtime_seconds: u32,
    },
    /// Packet counters.
    StatsPackets {
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
        #[serde(skip_serializing_if = "Option::is_none")]
        recv_errors: Option<u32>,
    },
    /// Unparsed Cayenne-LPP-compatible bytes.
    RawCayenneLpp {
        /// Six-byte source public-key prefix as lowercase hexadecimal.
        source_pubkey_prefix: String,
        /// Raw telemetry bytes as lowercase hexadecimal.
        payload: String,
    },
}

impl From<BatteryInfo> for TelemetryData {
    fn from(value: BatteryInfo) -> Self {
        Self::Battery {
            level: value.level,
            used_kb: value.used_kb,
            total_kb: value.total_kb,
        }
    }
}

impl From<DeviceStats> for TelemetryData {
    fn from(value: DeviceStats) -> Self {
        match value {
            DeviceStats::Core {
                battery_mv,
                uptime_seconds,
                errors,
                queue_length,
            } => Self::StatsCore {
                battery_mv,
                uptime_seconds,
                errors,
                queue_length,
            },
            DeviceStats::Radio {
                noise_floor,
                last_rssi,
                last_snr,
                tx_airtime_seconds,
                rx_airtime_seconds,
            } => Self::StatsRadio {
                noise_floor,
                last_rssi,
                last_snr,
                tx_airtime_seconds,
                rx_airtime_seconds,
            },
            DeviceStats::Packets {
                recv,
                sent,
                flood_recv,
                flood_sent,
                direct_recv,
                direct_sent,
                recv_errors,
            } => Self::StatsPackets {
                recv,
                sent,
                flood_recv,
                flood_sent,
                direct_recv,
                direct_sent,
                recv_errors,
            },
        }
    }
}

impl From<TelemetryResponse> for TelemetryData {
    fn from(value: TelemetryResponse) -> Self {
        Self::RawCayenneLpp {
            source_pubkey_prefix: lowercase_hex(&value.pubkey_prefix),
            payload: lowercase_hex(&value.payload),
        }
    }
}

/// Typed application events accepted for MQTT publication.
#[derive(Clone, Debug, PartialEq)]
pub enum Publication {
    /// Incoming `MeshCore` message.
    IncomingMessage(IncomingMessageData),
    /// `MeshCore` acknowledgement.
    Ack(AckData),
    /// Connection transition.
    ConnectionState(ConnectionStateData),
    /// Contact snapshot.
    Contacts(ContactsData),
    /// Typed telemetry.
    Telemetry(TelemetryData),
}

impl Publication {
    /// Creates a contact publication from a complete core snapshot.
    #[must_use]
    pub fn contacts(snapshot: ContactSnapshot) -> Self {
        Self::Contacts(snapshot.into())
    }

    /// Creates a battery telemetry publication.
    #[must_use]
    pub fn battery(info: BatteryInfo) -> Self {
        Self::Telemetry(info.into())
    }

    /// Creates an unparsed Cayenne-LPP telemetry publication.
    #[must_use]
    pub fn raw_telemetry(response: TelemetryResponse) -> Self {
        Self::Telemetry(response.into())
    }

    /// Returns the fixed v1 discriminator for this publication.
    #[must_use]
    pub const fn kind(&self) -> EventKind {
        match self {
            Self::IncomingMessage(_) => EventKind::IncomingMessage,
            Self::Ack(_) => EventKind::Ack,
            Self::ConnectionState(_) => EventKind::ConnectionState,
            Self::Contacts(_) => EventKind::Contacts,
            Self::Telemetry(_) => EventKind::Telemetry,
        }
    }

    /// Converts the subset of core events represented by the MQTT v1 schema.
    ///
    /// Events outside the publication allowlist return `None`.
    #[must_use]
    pub fn from_core_event(event: Event) -> Option<Self> {
        match event {
            Event::Connected => Some(Self::ConnectionState(ConnectionStateData {
                component: ConnectionComponent::MeshCore,
                status: ConnectionStatus::Connected,
                reason: None,
            })),
            Event::Disconnected => Some(Self::ConnectionState(ConnectionStateData {
                component: ConnectionComponent::MeshCore,
                status: ConnectionStatus::Disconnected,
                reason: None,
            })),
            Event::Contacts { contacts, lastmod } => {
                Some(Self::contacts(ContactSnapshot { contacts, lastmod }))
            }
            Event::Message(message) => Some(Self::IncomingMessage(message.into())),
            Event::Ack(ack) => Some(Self::Ack(ack.into())),
            Event::Battery {
                level,
                used_kb,
                total_kb,
            } => Some(Self::battery(BatteryInfo {
                level,
                used_kb,
                total_kb,
            })),
            Event::DeviceStats(stats) => Some(Self::Telemetry(stats.into())),
            Event::Telemetry(response) => Some(Self::raw_telemetry(response)),
            Event::SelfInfo(_)
            | Event::DeviceInfo(_)
            | Event::ChannelInfo { .. }
            | Event::MessageSent { .. }
            | Event::CurrentTime(_)
            | Event::InboxEmpty
            | Event::MessagesWaiting
            | Event::ProtocolError(_)
            | Event::UnknownPacket { .. }
            | Event::ContactUri(_)
            | Event::TuningParams(_)
            | Event::CustomVariables(_)
            | Event::AdvertPath(_)
            | Event::AutoAddConfig(_)
            | Event::AllowedRepeatFrequencies(_)
            | Event::DefaultFloodScope(_)
            | Event::LoginSucceeded(_)
            | Event::LoginFailed { .. }
            | Event::RemoteStatus(_)
            | Event::BinaryResponse(_)
            | Event::ControlData(_)
            | Event::PathDiscovery(_)
            | Event::Signature(_) => None,
        }
    }

    pub(crate) fn data_value(&self) -> Result<Value, SchemaError> {
        match self {
            Self::IncomingMessage(data) => serde_json::to_value(data),
            Self::Ack(data) => serde_json::to_value(data),
            Self::ConnectionState(data) => serde_json::to_value(data),
            Self::Contacts(data) => serde_json::to_value(data),
            Self::Telemetry(data) => serde_json::to_value(data),
        }
        .map_err(SchemaError::Json)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SendDirectData {
    destination: String,
    text: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SendChannelData {
    #[serde(rename = "channel")]
    _channel: u8,
    text: String,
}

fn validate_event_data(kind: EventKind, data: &Value) -> Result<(), SchemaError> {
    match kind {
        EventKind::IncomingMessage => {
            let value: IncomingMessageData = deserialize_data(data)?;
            if let MessageSourceData::Direct { pubkey_prefix } = &value.source
                && !is_lower_hex_exact(pubkey_prefix, 6)
            {
                return Err(SchemaError::InvalidWireData(
                    "incoming message public-key prefix must be six lowercase-hex bytes",
                ));
            }
            if value
                .signature
                .as_deref()
                .is_some_and(|signature| !is_lower_hex_exact(signature, 4))
            {
                return Err(SchemaError::InvalidWireData(
                    "incoming message signature must be four lowercase-hex bytes",
                ));
            }
            Ok(())
        }
        EventKind::Ack => {
            let value: AckData = deserialize_data(data)?;
            if !is_lower_hex_exact(&value.code, 4) {
                return Err(SchemaError::InvalidWireData(
                    "acknowledgement code must be four lowercase-hex bytes",
                ));
            }
            Ok(())
        }
        EventKind::ConnectionState => deserialize_data::<ConnectionStateData>(data).map(drop),
        EventKind::Contacts => {
            let value: ContactsData = deserialize_data(data)?;
            for contact in value.contacts {
                if !is_lower_hex_exact(&contact.public_key, 32) {
                    return Err(SchemaError::InvalidWireData(
                        "contact public key must be 32 lowercase-hex bytes",
                    ));
                }
                if !is_lower_hex_bounded(&contact.out_path, 128) {
                    return Err(SchemaError::InvalidWireData(
                        "contact path must be at most 128 lowercase-hex bytes",
                    ));
                }
            }
            Ok(())
        }
        EventKind::Telemetry => {
            let value: TelemetryData = deserialize_data(data)?;
            if let TelemetryData::RawCayenneLpp {
                source_pubkey_prefix,
                payload,
            } = value
            {
                if !is_lower_hex_exact(&source_pubkey_prefix, 6) {
                    return Err(SchemaError::InvalidWireData(
                        "telemetry source must be six lowercase-hex bytes",
                    ));
                }
                if !is_lower_hex_bounded(&payload, usize::MAX) {
                    return Err(SchemaError::InvalidWireData(
                        "telemetry payload must be lowercase hexadecimal",
                    ));
                }
            }
            Ok(())
        }
        EventKind::SendDirect => {
            let value: SendDirectData = deserialize_data(data)?;
            if value.destination.is_empty()
                || value.destination.len() > MAX_COMMAND_DESTINATION_BYTES
                || value.destination.trim() != value.destination
                || value.destination.chars().any(char::is_control)
                || value.text.is_empty()
                || value.text.len() > MAX_COMMAND_TEXT_BYTES
                || value.text.contains('\0')
            {
                return Err(SchemaError::InvalidWireData(
                    "direct command destination or text is invalid",
                ));
            }
            Ok(())
        }
        EventKind::SendChannel => {
            let value: SendChannelData = deserialize_data(data)?;
            if value.text.is_empty()
                || value.text.len() > MAX_COMMAND_TEXT_BYTES
                || value.text.contains('\0')
            {
                return Err(SchemaError::InvalidWireData(
                    "channel command text is invalid",
                ));
            }
            Ok(())
        }
    }
}

fn deserialize_data<T: DeserializeOwned>(data: &Value) -> Result<T, SchemaError> {
    serde_json::from_value(data.clone()).map_err(SchemaError::InvalidEventData)
}

fn is_lower_hex_exact(value: &str, bytes: usize) -> bool {
    value.len() == bytes.saturating_mul(2) && value.bytes().all(is_lower_hex_digit)
}

fn is_lower_hex_bounded(value: &str, maximum_bytes: usize) -> bool {
    value.len().is_multiple_of(2)
        && value.len() / 2 <= maximum_bytes
        && value.bytes().all(is_lower_hex_digit)
}

const fn is_lower_hex_digit(byte: u8) -> bool {
    byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

/// Versioned envelope validation or JSON encoding error.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// JSON parsing or serialization failed.
    #[error("invalid MQTT event JSON: {0}")]
    Json(serde_json::Error),
    /// The schema identifier was not v1.
    #[error("unsupported MQTT schema `{0}`")]
    UnsupportedSchema(String),
    /// Event IDs must be non-nil UUIDs.
    #[error("MQTT event_id must not be nil")]
    NilEventId,
    /// Origin identifiers are bounded and may not contain control characters.
    #[error("invalid MQTT event origin")]
    InvalidOrigin,
    /// The v1 data member must always be a JSON object.
    #[error("MQTT event data must be a JSON object")]
    DataMustBeObject,
    /// Data did not deserialize as the selected discriminator's DTO.
    #[error("invalid MQTT event data: {0}")]
    InvalidEventData(serde_json::Error),
    /// Data used an invalid stable wire encoding.
    #[error("invalid MQTT v1 wire data: {0}")]
    InvalidWireData(&'static str),
}

pub(crate) fn validate_origin(origin: &str) -> Result<(), SchemaError> {
    if origin.is_empty()
        || origin.len() > 128
        || origin.trim() != origin
        || origin.chars().any(char::is_control)
    {
        return Err(SchemaError::InvalidOrigin);
    }
    Ok(())
}

pub(crate) fn deserialize_canonical_uuid<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    let event_id = Uuid::parse_str(&value).map_err(serde::de::Error::custom)?;
    let canonical = event_id.hyphenated().to_string();
    if value.len() != canonical.len() || !value.eq_ignore_ascii_case(&canonical) {
        return Err(serde::de::Error::custom(
            "UUID must use canonical hyphenated form",
        ));
    }
    Ok(event_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use meshquill_core::domain::MessageSource;
    use meshquill_core::{Path, PublicKey};

    #[test]
    fn schema_roundtrip_preserves_required_fields() {
        let event_id =
            Uuid::parse_str("018f0f65-9b50-7cc2-a6e9-3b8b3a7f3101").expect("valid UUID fixture");
        let envelope = EventEnvelope::new(
            event_id,
            "test-origin",
            1_725_000_000_123,
            EventKind::Telemetry,
            serde_json::json!({"kind": "battery", "level": 87}),
        )
        .expect("valid envelope");

        let encoded = envelope.encode().expect("encode envelope");
        let decoded = EventEnvelope::decode(&encoded).expect("decode envelope");
        assert_eq!(decoded, envelope);
        assert_eq!(decoded.schema, "meshquill.mqtt/v1");

        let json: Value = serde_json::from_slice(&encoded).expect("JSON value");
        assert_eq!(json["type"], "telemetry");
        assert_eq!(json["data"]["kind"], "battery");
    }

    #[test]
    fn event_ids_require_canonical_hyphenated_uuid_spelling() {
        let canonical = "018f0f65-9b50-7cc2-a6e9-3b8b3a7f3101";
        let envelope = |event_id: &str| {
            serde_json::to_vec(&serde_json::json!({
                "schema": SCHEMA_VERSION,
                "event_id": event_id,
                "origin": "remote",
                "timestamp": 42,
                "type": "ack",
                "data": {"code": "01020304"}
            }))
            .expect("serialize UUID fixture")
        };

        assert!(EventEnvelope::decode(&envelope(canonical)).is_ok());
        assert!(EventEnvelope::decode(&envelope(&canonical.to_uppercase())).is_ok());
        for noncanonical in [
            "018f0f659b507cc2a6e93b8b3a7f3101",
            "{018f0f65-9b50-7cc2-a6e9-3b8b3a7f3101}",
            "urn:uuid:018f0f65-9b50-7cc2-a6e9-3b8b3a7f3101",
        ] {
            assert!(matches!(
                EventEnvelope::decode(&envelope(noncanonical)),
                Err(SchemaError::Json(_))
            ));
        }
    }

    #[test]
    fn wrong_schema_nil_id_scalar_and_mismatched_data_are_rejected() {
        let mut envelope = EventEnvelope::new(
            Uuid::now_v7(),
            "remote",
            42,
            EventKind::Ack,
            serde_json::json!({"code": "01020304"}),
        )
        .expect("valid fixture");
        envelope.schema = "meshquill.mqtt/v2".to_owned();
        assert!(matches!(
            envelope.validate(),
            Err(SchemaError::UnsupportedSchema(_))
        ));

        envelope.schema = SCHEMA_VERSION.to_owned();
        envelope.event_id = Uuid::nil();
        assert!(matches!(envelope.validate(), Err(SchemaError::NilEventId)));

        envelope.event_id = Uuid::now_v7();
        envelope.data = Value::String("not an object".to_owned());
        assert!(matches!(
            envelope.validate(),
            Err(SchemaError::DataMustBeObject)
        ));

        envelope.data = serde_json::json!({"code": [1, 2, 3, 4]});
        assert!(matches!(
            envelope.validate(),
            Err(SchemaError::InvalidEventData(_))
        ));
    }

    #[test]
    fn envelopes_enforce_utf8_byte_bounds_beyond_json_schema_codepoint_caps() {
        let origin = EventEnvelope::new(
            Uuid::now_v7(),
            "é".repeat(65),
            42,
            EventKind::Ack,
            serde_json::json!({"code": "01020304"}),
        );
        assert!(matches!(origin, Err(SchemaError::InvalidOrigin)));

        let direct = EventEnvelope::new(
            Uuid::now_v7(),
            "remote",
            42,
            EventKind::SendDirect,
            serde_json::json!({
                "destination": "é".repeat(65),
                "text": "hello"
            }),
        );
        assert!(matches!(direct, Err(SchemaError::InvalidWireData(_))));

        let channel = EventEnvelope::new(
            Uuid::now_v7(),
            "remote",
            42,
            EventKind::SendChannel,
            serde_json::json!({
                "channel": 0,
                "text": "é".repeat(513)
            }),
        );
        assert!(matches!(channel, Err(SchemaError::InvalidWireData(_))));
    }

    #[test]
    fn core_wire_dtos_use_stable_lowercase_hex() {
        let message = Message {
            observation_id: None,
            source: MessageSource::Direct {
                pubkey_prefix: "A1B2C3D4E5F6".to_owned(),
            },
            route: MessageRoute::Path {
                hash_mode: 2,
                hop_count: 3,
            },
            txt_type: 1,
            sender_timestamp: 77,
            signature: Some([0xab, 0xcd, 0x01, 0xef]),
            text: "hello".to_owned(),
            snr: Some(4.5),
            status: MessageStatus::Received,
        };
        let message = Publication::from_core_event(Event::Message(message))
            .expect("message is publishable")
            .data_value()
            .expect("serialize message");
        assert_eq!(message["source"]["pubkey_prefix"], "a1b2c3d4e5f6");
        assert_eq!(message["signature"], "abcd01ef");
        assert_eq!(message["route"]["kind"], "path");

        let contact = Contact {
            public_key: PublicKey::try_from_bytes(&[0xab; 32]).expect("valid public key"),
            contact_type: ContactType::Unknown(9),
            flags: 4,
            route: ContactRoute::Path {
                hash_mode: 1,
                hop_count: 2,
            },
            out_path: Path::try_from_bytes(&[0xcd, 0xef]).expect("valid path"),
            adv_name: "sensor".to_owned(),
            last_advert: 8,
            adv_lat: 0.5,
            adv_lon: -0.25,
            lastmod: 9,
        };
        let contacts = Publication::contacts(ContactSnapshot {
            contacts: vec![contact],
            lastmod: 0x1234_5678,
        })
        .data_value()
        .expect("serialize contacts");
        assert_eq!(contacts["contacts"][0]["public_key"], "ab".repeat(32));
        assert_eq!(contacts["contacts"][0]["out_path"], "cdef");
        assert_eq!(contacts["lastmod"], 0x1234_5678_u32);

        let ack = Publication::from_core_event(Event::Ack(Ack {
            code: [0xde, 0xad, 0xbe, 0xef],
            trip_time_ms: Some(50),
        }))
        .expect("ack is publishable")
        .data_value()
        .expect("serialize ack");
        assert_eq!(ack["code"], "deadbeef");
    }

    #[test]
    fn all_core_telemetry_variants_are_typed() {
        let cases = [
            (
                Event::Battery {
                    level: 91,
                    used_kb: Some(10),
                    total_kb: Some(100),
                },
                "battery",
            ),
            (
                Event::DeviceStats(DeviceStats::Core {
                    battery_mv: 3_700,
                    uptime_seconds: 12_345,
                    errors: 4,
                    queue_length: 2,
                }),
                "stats_core",
            ),
            (
                Event::DeviceStats(DeviceStats::Radio {
                    noise_floor: -100,
                    last_rssi: -80,
                    last_snr: 3.5,
                    tx_airtime_seconds: 4,
                    rx_airtime_seconds: 5,
                }),
                "stats_radio",
            ),
            (
                Event::DeviceStats(DeviceStats::Packets {
                    recv: 1,
                    sent: 2,
                    flood_recv: 3,
                    flood_sent: 4,
                    direct_recv: 5,
                    direct_sent: 6,
                    recv_errors: Some(7),
                }),
                "stats_packets",
            ),
            (
                Event::Telemetry(TelemetryResponse {
                    pubkey_prefix: [0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6],
                    payload: vec![0x01, 0xab, 0xff],
                }),
                "raw_cayenne_lpp",
            ),
        ];

        for (event, expected_kind) in cases {
            let publication = Publication::from_core_event(event).expect("telemetry publishable");
            assert_eq!(publication.kind(), EventKind::Telemetry);
            let data = publication.data_value().expect("serialize telemetry");
            assert_eq!(data["kind"], expected_kind);
            assert!(data.get("values").is_none());
            if expected_kind == "raw_cayenne_lpp" {
                assert_eq!(data["source_pubkey_prefix"], "a1b2c3d4e5f6");
                assert_eq!(data["payload"], "01abff");
            }
        }
    }

    #[test]
    fn unsupported_core_events_are_not_publishable() {
        let event = Event::ContactUri(meshquill_core::ContactUri {
            uri: "mesh://example".to_owned(),
            card: vec![1, 2, 3],
        });
        assert!(Publication::from_core_event(event).is_none());
    }
}
