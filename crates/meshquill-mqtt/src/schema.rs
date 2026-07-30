use std::collections::BTreeMap;

use meshquill_core::{Ack, Contact, DeviceStats, Event, Message};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

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
    /// Globally unique event identity used for bounded deduplication.
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
    /// Returns [`SchemaError`] for a nil ID, invalid origin, or non-object data.
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

    /// Validates schema invariants independently of deserialization.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError`] when any fixed envelope invariant is violated.
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
        Ok(())
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

/// Data published with a contact snapshot event.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContactsData {
    /// Contact rows in this snapshot.
    pub contacts: Vec<Contact>,
    /// `MeshCore` last-modified sequence for the snapshot.
    pub lastmod: u32,
}

/// Structured telemetry values for a device or contact.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryData {
    /// Optional source identifier, such as a contact name or public-key prefix.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Named telemetry values. Values remain JSON so protocol extensions do not
    /// require arbitrary command execution support.
    pub values: BTreeMap<String, Value>,
}

/// Typed application events accepted for MQTT publication.
#[derive(Clone, Debug)]
pub enum Publication {
    /// Incoming `MeshCore` message.
    IncomingMessage(Message),
    /// `MeshCore` acknowledgement.
    Ack(Ack),
    /// Connection transition.
    ConnectionState(ConnectionStateData),
    /// Contact snapshot.
    Contacts(ContactsData),
    /// Telemetry values.
    Telemetry(TelemetryData),
}

impl Publication {
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
                Some(Self::Contacts(ContactsData { contacts, lastmod }))
            }
            Event::Message(message) => Some(Self::IncomingMessage(message)),
            Event::Ack(ack) => Some(Self::Ack(ack)),
            Event::Battery {
                level,
                used_kb,
                total_kb,
            } => {
                let mut values = BTreeMap::new();
                values.insert("battery_level".to_owned(), Value::from(level));
                if let Some(used_kb) = used_kb {
                    values.insert("used_kb".to_owned(), Value::from(used_kb));
                }
                if let Some(total_kb) = total_kb {
                    values.insert("total_kb".to_owned(), Value::from(total_kb));
                }
                Some(Self::telemetry_from_values(values))
            }
            Event::DeviceStats(stats) => Some(Self::device_stats_telemetry(&stats)),
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
            | Event::Telemetry(_)
            | Event::BinaryResponse(_)
            | Event::ControlData(_)
            | Event::PathDiscovery(_)
            | Event::Signature(_) => None,
        }
    }

    fn telemetry_from_values(values: BTreeMap<String, Value>) -> Self {
        Self::Telemetry(TelemetryData {
            source: None,
            values,
        })
    }

    fn device_stats_telemetry(stats: &DeviceStats) -> Self {
        let mut values = BTreeMap::new();
        match stats {
            DeviceStats::Core {
                battery_mv,
                uptime_seconds,
                errors,
                queue_length,
            } => {
                values.insert("stats_type".to_owned(), Value::from("core"));
                values.insert("battery_mv".to_owned(), Value::from(*battery_mv));
                values.insert("uptime_seconds".to_owned(), Value::from(*uptime_seconds));
                values.insert("errors".to_owned(), Value::from(*errors));
                values.insert("queue_length".to_owned(), Value::from(*queue_length));
            }
            DeviceStats::Radio {
                noise_floor,
                last_rssi,
                last_snr,
                tx_airtime_seconds,
                rx_airtime_seconds,
            } => {
                values.insert("stats_type".to_owned(), Value::from("radio"));
                values.insert("noise_floor".to_owned(), Value::from(*noise_floor));
                values.insert("last_rssi".to_owned(), Value::from(*last_rssi));
                values.insert("last_snr".to_owned(), Value::from(*last_snr));
                values.insert(
                    "tx_airtime_seconds".to_owned(),
                    Value::from(*tx_airtime_seconds),
                );
                values.insert(
                    "rx_airtime_seconds".to_owned(),
                    Value::from(*rx_airtime_seconds),
                );
            }
            DeviceStats::Packets {
                recv,
                sent,
                flood_recv,
                flood_sent,
                direct_recv,
                direct_sent,
                recv_errors,
            } => {
                values.insert("stats_type".to_owned(), Value::from("packets"));
                values.insert("recv".to_owned(), Value::from(*recv));
                values.insert("sent".to_owned(), Value::from(*sent));
                values.insert("flood_recv".to_owned(), Value::from(*flood_recv));
                values.insert("flood_sent".to_owned(), Value::from(*flood_sent));
                values.insert("direct_recv".to_owned(), Value::from(*direct_recv));
                values.insert("direct_sent".to_owned(), Value::from(*direct_sent));
                if let Some(recv_errors) = recv_errors {
                    values.insert("recv_errors".to_owned(), Value::from(*recv_errors));
                }
            }
        }

        Self::telemetry_from_values(values)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_roundtrip_preserves_required_fields() {
        let event_id =
            Uuid::parse_str("018f0f65-9b50-7cc2-a6e9-3b8b3a7f3101").expect("valid UUID fixture");
        let envelope = EventEnvelope::new(
            event_id,
            "test-origin",
            1_725_000_000_123,
            EventKind::Telemetry,
            serde_json::json!({"values": {"battery": 87}}),
        )
        .expect("valid envelope");

        let encoded = envelope.encode().expect("encode envelope");
        let decoded = EventEnvelope::decode(&encoded).expect("decode envelope");
        assert_eq!(decoded, envelope);
        assert_eq!(decoded.schema, "meshquill.mqtt/v1");

        let json: Value = serde_json::from_slice(&encoded).expect("JSON value");
        assert_eq!(json["type"], "telemetry");
        assert!(json.get("event_id").is_some());
        assert!(json.get("origin").is_some());
        assert!(json.get("timestamp").is_some());
        assert!(json.get("data").is_some());
    }

    #[test]
    fn wrong_schema_nil_id_and_scalar_data_are_rejected() {
        let mut envelope = EventEnvelope::new(
            Uuid::now_v7(),
            "remote",
            42,
            EventKind::Ack,
            serde_json::json!({}),
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
    }

    #[test]
    fn core_battery_event_becomes_telemetry() {
        let publication = Publication::from_core_event(Event::Battery {
            level: 91,
            used_kb: Some(10),
            total_kb: Some(100),
        })
        .expect("battery is publishable");
        assert_eq!(publication.kind(), EventKind::Telemetry);
        let data = publication.data_value().expect("serialize telemetry");
        assert_eq!(data["values"]["battery_level"], 91);
    }

    #[test]
    fn core_device_stats_event_becomes_telemetry() {
        let publication = Publication::from_core_event(Event::DeviceStats(DeviceStats::Core {
            battery_mv: 3_700,
            uptime_seconds: 12_345,
            errors: 4,
            queue_length: 2,
        }))
        .expect("device-stats are publishable");

        assert_eq!(publication.kind(), EventKind::Telemetry);
        let data = publication.data_value().expect("serialize telemetry");
        assert_eq!(data["source"], Value::Null);
        assert_eq!(data["values"]["stats_type"], "core");
        assert_eq!(data["values"]["battery_mv"], 3_700);
        assert_eq!(data["values"]["queue_length"], 2);
    }

    #[test]
    fn core_new_events_are_not_publishable() {
        let unpublished_events = vec![
            Event::ContactUri(meshquill_core::ContactUri {
                uri: "mesh://example".to_owned(),
                card: vec![1, 2, 3],
            }),
            Event::TuningParams(meshquill_core::TuningParams {
                rx_delay: 250,
                airtime_factor: 2,
            }),
            Event::CustomVariables(meshquill_core::CustomVariables {
                raw: vec![4, 5, 6],
                entries: vec![meshquill_core::CustomVariable {
                    key: "k".to_owned(),
                    value: "v".to_owned(),
                }],
            }),
            Event::AdvertPath(meshquill_core::AdvertPath {
                received_at: 123,
                route: meshquill_core::ContactRoute::Path {
                    hash_mode: 1,
                    hop_count: 1,
                },
                path: meshquill_core::Path::try_from_bytes(&[0x01, 0x02])
                    .expect("path bytes valid"),
            }),
            Event::AutoAddConfig(meshquill_core::AutoAddConfig {
                config: 1,
                max_hops: Some(3),
            }),
            Event::AllowedRepeatFrequencies(vec![
                meshquill_core::FrequencyRange {
                    lower_khz: 915,
                    upper_khz: 916,
                },
                meshquill_core::FrequencyRange {
                    lower_khz: 1_000,
                    upper_khz: 1_001,
                },
            ]),
            Event::DefaultFloodScope(meshquill_core::DefaultFloodScope::Unconfigured),
        ];

        for event in unpublished_events {
            assert!(Publication::from_core_event(event).is_none());
        }
    }
}
