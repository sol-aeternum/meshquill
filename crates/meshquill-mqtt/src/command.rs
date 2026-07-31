use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

use crate::config::{CommandLimits, MAX_DEDUPE_CAPACITY, MAX_DEDUPE_TTL_SECS, MqttConfig};
use crate::schema::{deserialize_canonical_uuid, validate_origin};
use crate::topics::{SCHEMA_VERSION, TopicSet};

/// An allowlisted application command accepted from MQTT.
#[derive(Clone, Eq, PartialEq)]
pub enum SendCommand {
    /// Send text to a contact name, key, or other caller-resolved direct destination.
    Direct {
        /// Validated destination selector.
        destination: String,
        /// Validated UTF-8 text.
        text: String,
    },
    /// Send text to a numeric `MeshCore` channel.
    Channel {
        /// Validated channel index.
        channel: u8,
        /// Validated UTF-8 text.
        text: String,
    },
}

impl fmt::Debug for SendCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct { destination, text } => formatter
                .debug_struct("Direct")
                .field("destination_bytes", &destination.len())
                .field("text_bytes", &text.len())
                .finish(),
            Self::Channel { channel, text } => formatter
                .debug_struct("Channel")
                .field("channel", channel)
                .field("text_bytes", &text.len())
                .finish(),
        }
    }
}

/// Metadata and command delivered to the application after all gateway checks pass.
#[derive(Clone, Eq, PartialEq)]
pub struct AcceptedCommand {
    /// Source envelope event ID.
    pub event_id: Uuid,
    /// Remote origin that requested the command.
    pub origin: String,
    /// Source envelope timestamp in Unix milliseconds.
    pub timestamp: u64,
    /// Strictly allowlisted send operation.
    pub command: SendCommand,
}

impl fmt::Debug for AcceptedCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AcceptedCommand")
            .field("event_id", &self.event_id)
            .field("origin_bytes", &self.origin.len())
            .field("timestamp", &self.timestamp)
            .field("command", &self.command)
            .finish()
    }
}

/// Result of checking an event ID against the bounded cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DedupeDecision {
    /// The ID was new and has been retained.
    Fresh,
    /// The ID was already retained and has not expired.
    Duplicate,
}

/// Bounded, TTL-based event-ID cache used to reject MQTT redelivery and loops.
#[derive(Debug)]
pub struct EventIdDedupe {
    capacity: usize,
    ttl: Duration,
    entries: HashMap<Uuid, Instant>,
    order: VecDeque<(Uuid, Instant)>,
}

impl EventIdDedupe {
    /// Creates a cache with explicit finite bounds.
    ///
    /// # Errors
    ///
    /// Returns [`DedupeError`] for a zero or excessive capacity or TTL.
    pub fn new(capacity: usize, ttl: Duration) -> Result<Self, DedupeError> {
        if capacity == 0 {
            return Err(DedupeError::ZeroCapacity);
        }
        if ttl.is_zero() {
            return Err(DedupeError::ZeroTtl);
        }
        if capacity > MAX_DEDUPE_CAPACITY {
            return Err(DedupeError::CapacityExceedsLimit {
                configured: capacity,
                maximum: MAX_DEDUPE_CAPACITY,
            });
        }
        let maximum_ttl = Duration::from_secs(MAX_DEDUPE_TTL_SECS);
        if ttl > maximum_ttl {
            return Err(DedupeError::TtlExceedsLimit {
                configured: ttl,
                maximum: maximum_ttl,
            });
        }
        Ok(Self {
            capacity,
            ttl,
            entries: HashMap::with_capacity(capacity.min(4096)),
            order: VecDeque::with_capacity(capacity.min(4096)),
        })
    }

    /// Checks and retains an event ID using the current monotonic time.
    pub fn check_and_insert(&mut self, event_id: Uuid) -> DedupeDecision {
        self.check_and_insert_at(event_id, Instant::now())
    }

    /// Deterministic variant for tests and callers with an existing monotonic timestamp.
    pub fn check_and_insert_at(&mut self, event_id: Uuid, now: Instant) -> DedupeDecision {
        self.evict_expired(now);
        if self.entries.get(&event_id).is_some_and(|seen| {
            now.checked_duration_since(*seen)
                .is_none_or(|elapsed| elapsed < self.ttl)
        }) {
            return DedupeDecision::Duplicate;
        }

        self.entries.insert(event_id, now);
        self.order.push_back((event_id, now));
        self.enforce_capacity();
        DedupeDecision::Fresh
    }

    /// Number of IDs currently retained.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn evict_expired(&mut self, now: Instant) {
        while let Some((event_id, seen)) = self.order.front().copied() {
            let expired = now
                .checked_duration_since(seen)
                .is_some_and(|elapsed| elapsed >= self.ttl);
            if !expired {
                break;
            }
            self.order.pop_front();
            if self.entries.get(&event_id) == Some(&seen) {
                self.entries.remove(&event_id);
            }
        }
    }

    fn enforce_capacity(&mut self) {
        while self.entries.len() > self.capacity {
            let Some((event_id, seen)) = self.order.pop_front() else {
                break;
            };
            if self.entries.get(&event_id) == Some(&seen) {
                self.entries.remove(&event_id);
            }
        }
    }
}

/// Invalid event-ID cache bounds.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DedupeError {
    /// A bounded cache cannot have zero slots.
    #[error("MQTT dedupe capacity must be non-zero")]
    ZeroCapacity,
    /// Entries must have a finite positive lifetime.
    #[error("MQTT dedupe TTL must be non-zero")]
    ZeroTtl,
    /// The requested capacity exceeds the crate-wide retention bound.
    #[error("MQTT dedupe capacity {configured} exceeds the {maximum} entry hard limit")]
    CapacityExceedsLimit {
        /// Requested maximum entry count.
        configured: usize,
        /// Crate-wide hard maximum entry count.
        maximum: usize,
    },
    /// The requested TTL exceeds the crate-wide retention bound.
    #[error("MQTT dedupe TTL {configured:?} exceeds the {maximum:?} hard limit")]
    TtlExceedsLimit {
        /// Requested entry lifetime.
        configured: Duration,
        /// Crate-wide hard maximum entry lifetime.
        maximum: Duration,
    },
}

/// Stateful parser and security gate for the single outbound send topic.
#[derive(Debug)]
pub struct CommandProcessor {
    allow_send: bool,
    outbound_topic: String,
    local_origin: String,
    max_payload_bytes: usize,
    limits: CommandLimits,
    dedupe: EventIdDedupe,
}

impl CommandProcessor {
    /// Builds a processor from an already validated gateway configuration.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError`] if topic or dedupe construction fails.
    pub fn new(config: &MqttConfig) -> Result<Self, CommandError> {
        config.validate().map_err(CommandError::Config)?;
        let topics = TopicSet::new(&config.topic_prefix).map_err(CommandError::Config)?;
        let dedupe = EventIdDedupe::new(
            config.dedupe.capacity,
            Duration::from_secs(config.dedupe.ttl_secs),
        )?;
        Ok(Self {
            allow_send: config.allow_send,
            outbound_topic: topics.outbound_send().to_owned(),
            local_origin: config.origin.clone(),
            max_payload_bytes: config.max_payload_bytes,
            limits: config.command_limits,
            dedupe,
        })
    }

    /// Returns the exact subscription topic only when send support was explicitly enabled.
    #[must_use]
    pub fn subscription_topic(&self) -> Option<&str> {
        self.allow_send.then_some(self.outbound_topic.as_str())
    }

    /// Parses, validates, loop-checks, and deduplicates one broker publication.
    ///
    /// # Errors
    ///
    /// Returns [`CommandError`] for disabled input, a wrong topic, malformed or oversized
    /// JSON, an unsupported command, a loop, a duplicate, or command-specific bounds.
    pub fn process(
        &mut self,
        topic: &str,
        payload: &[u8],
    ) -> Result<AcceptedCommand, CommandError> {
        self.process_at(topic, payload, Instant::now())
    }

    /// Deterministic parser variant for TTL and replay tests.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::process`].
    pub fn process_at(
        &mut self,
        topic: &str,
        payload: &[u8],
        now: Instant,
    ) -> Result<AcceptedCommand, CommandError> {
        if !self.allow_send {
            return Err(CommandError::SendDisabled);
        }
        if topic != self.outbound_topic {
            return Err(CommandError::UnexpectedTopic);
        }
        if payload.is_empty() {
            return Err(CommandError::EmptyPayload);
        }
        if payload.len() > self.max_payload_bytes {
            return Err(CommandError::PayloadTooLarge {
                actual: payload.len(),
                maximum: self.max_payload_bytes,
            });
        }

        let envelope: CommandEnvelope =
            serde_json::from_slice(payload).map_err(CommandError::InvalidJson)?;
        if envelope.schema != SCHEMA_VERSION {
            return Err(CommandError::UnsupportedSchema);
        }
        if envelope.event_id.is_nil() {
            return Err(CommandError::NilEventId);
        }
        validate_origin(&envelope.origin).map_err(|_| CommandError::InvalidOrigin)?;
        if envelope.origin == self.local_origin {
            return Err(CommandError::LocalOriginLoop);
        }
        if !envelope.data.is_object() {
            return Err(CommandError::DataMustBeObject);
        }

        let command = match envelope.kind.as_str() {
            "send_direct" => {
                let data: DirectData = serde_json::from_value(envelope.data)
                    .map_err(CommandError::InvalidCommandData)?;
                SendCommand::Direct {
                    destination: data.destination,
                    text: data.text,
                }
            }
            "send_channel" => {
                let data: ChannelData = serde_json::from_value(envelope.data)
                    .map_err(CommandError::InvalidCommandData)?;
                SendCommand::Channel {
                    channel: data.channel,
                    text: data.text,
                }
            }
            _ => return Err(CommandError::UnsupportedCommand),
        };
        validate_send_command(&command, self.limits)?;

        if self.dedupe.check_and_insert_at(envelope.event_id, now) == DedupeDecision::Duplicate {
            return Err(CommandError::DuplicateEvent);
        }

        Ok(AcceptedCommand {
            event_id: envelope.event_id,
            origin: envelope.origin,
            timestamp: envelope.timestamp,
            command,
        })
    }
}

/// Revalidate a send command against the configured MQTT application limits.
///
/// Callers must use this again after any trusted local hook modifies an accepted command and
/// before performing radio I/O.
///
/// # Errors
/// Returns [`CommandError`] when the destination, channel, or text violates `limits`.
pub fn validate_send_command(
    command: &SendCommand,
    limits: CommandLimits,
) -> Result<(), CommandError> {
    match command {
        SendCommand::Direct { destination, text } => {
            validate_destination(destination, limits.max_destination_bytes)?;
            validate_text(text, limits.max_text_bytes)
        }
        SendCommand::Channel { channel, text } => {
            if *channel > limits.max_channel {
                return Err(CommandError::ChannelOutOfRange {
                    channel: *channel,
                    maximum: limits.max_channel,
                });
            }
            validate_text(text, limits.max_text_bytes)
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandEnvelope {
    schema: String,
    #[serde(deserialize_with = "deserialize_canonical_uuid")]
    event_id: Uuid,
    origin: String,
    timestamp: u64,
    #[serde(rename = "type")]
    kind: String,
    data: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectData {
    destination: String,
    text: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelData {
    channel: u8,
    text: String,
}

fn validate_destination(destination: &str, maximum: usize) -> Result<(), CommandError> {
    if destination.is_empty() || destination.trim() != destination {
        return Err(CommandError::InvalidDestination);
    }
    if destination.len() > maximum {
        return Err(CommandError::DestinationTooLong {
            actual: destination.len(),
            maximum,
        });
    }
    if destination.chars().any(char::is_control) {
        return Err(CommandError::InvalidDestination);
    }
    Ok(())
}

fn validate_text(text: &str, maximum: usize) -> Result<(), CommandError> {
    if text.is_empty() || text.contains('\0') {
        return Err(CommandError::InvalidText);
    }
    if text.len() > maximum {
        return Err(CommandError::TextTooLong {
            actual: text.len(),
            maximum,
        });
    }
    Ok(())
}

/// Security or validation rejection for an inbound MQTT command.
#[derive(Debug, Error)]
pub enum CommandError {
    /// Construction received invalid shared configuration.
    #[error(transparent)]
    Config(#[from] crate::config::ConfigError),
    /// Construction received invalid dedupe bounds.
    #[error(transparent)]
    Dedupe(#[from] DedupeError),
    /// Command input is disabled unless explicitly opted in.
    #[error("MQTT outbound send commands are disabled")]
    SendDisabled,
    /// The publication did not use the one exact v1 outbound topic.
    #[error("MQTT command arrived on an unexpected topic")]
    UnexpectedTopic,
    /// MQTT topics must be valid UTF-8.
    #[error("MQTT command topic is not valid UTF-8")]
    InvalidTopicEncoding,
    /// Retained publications are replayable across fresh sessions and are never commands.
    #[error("retained MQTT publications are not accepted as commands")]
    RetainedCommand,
    /// Empty application payloads are invalid.
    #[error("MQTT command payload is empty")]
    EmptyPayload,
    /// The payload exceeded the configured byte bound.
    #[error("MQTT command payload is {actual} bytes; maximum is {maximum}")]
    PayloadTooLarge {
        /// Actual payload bytes.
        actual: usize,
        /// Configured maximum.
        maximum: usize,
    },
    /// The JSON envelope was malformed or contained unknown fields.
    #[error("invalid MQTT command envelope: {0}")]
    InvalidJson(serde_json::Error),
    /// Only the exact v1 schema is accepted.
    #[error("unsupported MQTT command schema")]
    UnsupportedSchema,
    /// Nil UUIDs do not provide replay protection.
    #[error("MQTT command event_id must not be nil")]
    NilEventId,
    /// Origin was empty, too large, or contained controls.
    #[error("invalid MQTT command origin")]
    InvalidOrigin,
    /// The gateway refuses its own reflected events.
    #[error("MQTT command origin matches the local gateway")]
    LocalOriginLoop,
    /// Command data must be a JSON object.
    #[error("MQTT command data must be a JSON object")]
    DataMustBeObject,
    /// Administrative and arbitrary command names are not allowlisted.
    #[error("MQTT command type is not allowlisted")]
    UnsupportedCommand,
    /// Allowlisted command data was malformed or contained unknown fields.
    #[error("invalid MQTT command data: {0}")]
    InvalidCommandData(serde_json::Error),
    /// A direct-message destination was empty or unsafe.
    #[error("invalid MQTT direct-message destination")]
    InvalidDestination,
    /// A direct destination exceeded its byte bound.
    #[error("MQTT destination is {actual} bytes; maximum is {maximum}")]
    DestinationTooLong {
        /// Actual destination bytes.
        actual: usize,
        /// Configured maximum.
        maximum: usize,
    },
    /// A channel index exceeded the configured application bound.
    #[error("MQTT channel {channel} exceeds maximum {maximum}")]
    ChannelOutOfRange {
        /// Requested channel.
        channel: u8,
        /// Configured maximum.
        maximum: u8,
    },
    /// Text was empty or contained a NUL byte.
    #[error("invalid MQTT message text")]
    InvalidText,
    /// Text exceeded its UTF-8 byte bound.
    #[error("MQTT message text is {actual} bytes; maximum is {maximum}")]
    TextTooLong {
        /// Actual text bytes.
        actual: usize,
        /// Configured maximum.
        maximum: usize,
    },
    /// The event ID is still present in the bounded replay cache.
    #[error("duplicate MQTT command event_id")]
    DuplicateEvent,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_config() -> MqttConfig {
        MqttConfig {
            allow_send: true,
            tls: crate::config::TlsConfig {
                enabled: false,
                ..crate::config::TlsConfig::default()
            },
            ..MqttConfig::default()
        }
    }

    fn command_payload(event_id: Uuid, origin: &str, kind: &str, data: &Value) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "schema": SCHEMA_VERSION,
            "event_id": event_id,
            "origin": origin,
            "timestamp": 1_725_000_000_000_u64,
            "type": kind,
            "data": data,
        }))
        .expect("serialize command fixture")
    }

    #[test]
    fn command_subscription_requires_explicit_opt_in() {
        let disabled = CommandProcessor::new(&MqttConfig::default()).expect("valid processor");
        assert_eq!(disabled.subscription_topic(), None);

        let mut disabled = disabled;
        assert!(matches!(
            disabled.process("any", b"{}"),
            Err(CommandError::SendDisabled)
        ));

        let enabled = CommandProcessor::new(&enabled_config()).expect("valid processor");
        assert_eq!(
            enabled.subscription_topic(),
            Some("meshquill/meshquill.mqtt/v1/outbound/send")
        );
    }

    #[test]
    fn command_event_ids_require_canonical_hyphenated_uuid_spelling() {
        let config = enabled_config();
        let topic = TopicSet::new(&config.topic_prefix)
            .expect("valid topics")
            .outbound_send()
            .to_owned();
        let payload = |event_id: &str| {
            serde_json::to_vec(&serde_json::json!({
                "schema": SCHEMA_VERSION,
                "event_id": event_id,
                "origin": "remote",
                "timestamp": 42,
                "type": "send_channel",
                "data": {"channel": 1, "text": "hello"}
            }))
            .expect("serialize UUID fixture")
        };
        let canonical = "018f0f65-9b50-7cc2-a6e9-3b8b3a7f3101";

        for accepted in [canonical.to_owned(), canonical.to_uppercase()] {
            let mut processor = CommandProcessor::new(&config).expect("valid processor");
            assert!(processor.process(&topic, &payload(&accepted)).is_ok());
        }
        for noncanonical in [
            "018f0f659b507cc2a6e93b8b3a7f3101",
            "{018f0f65-9b50-7cc2-a6e9-3b8b3a7f3101}",
            "urn:uuid:018f0f65-9b50-7cc2-a6e9-3b8b3a7f3101",
        ] {
            let mut processor = CommandProcessor::new(&config).expect("valid processor");
            assert!(matches!(
                processor.process(&topic, &payload(noncanonical)),
                Err(CommandError::InvalidJson(_))
            ));
        }
    }

    #[test]
    fn only_direct_and_channel_commands_are_allowlisted() {
        let config = enabled_config();
        let topic = TopicSet::new(&config.topic_prefix)
            .expect("valid topics")
            .outbound_send()
            .to_owned();
        let mut processor = CommandProcessor::new(&config).expect("valid processor");

        let direct = command_payload(
            Uuid::now_v7(),
            "remote",
            "send_direct",
            &serde_json::json!({"destination": "alice", "text": "hello"}),
        );
        assert!(matches!(
            processor.process(&topic, &direct),
            Ok(AcceptedCommand {
                command: SendCommand::Direct { .. },
                ..
            })
        ));

        let channel = command_payload(
            Uuid::now_v7(),
            "remote",
            "send_channel",
            &serde_json::json!({"channel": 3, "text": "hello room"}),
        );
        assert!(matches!(
            processor.process(&topic, &channel),
            Ok(AcceptedCommand {
                command: SendCommand::Channel { channel: 3, .. },
                ..
            })
        ));

        let admin = command_payload(
            Uuid::now_v7(),
            "remote",
            "set_radio_config",
            &serde_json::json!({"power": 99}),
        );
        assert!(matches!(
            processor.process(&topic, &admin),
            Err(CommandError::UnsupportedCommand)
        ));
    }

    #[test]
    fn exact_topic_payload_and_command_limits_are_enforced() {
        let mut config = enabled_config();
        config.max_payload_bytes = 512;
        config.command_limits = CommandLimits {
            max_destination_bytes: 5,
            max_channel: 2,
            max_text_bytes: 8,
        };
        let topic = TopicSet::new(&config.topic_prefix)
            .expect("valid topics")
            .outbound_send()
            .to_owned();
        let mut processor = CommandProcessor::new(&config).expect("valid processor");

        let direct = command_payload(
            Uuid::now_v7(),
            "remote",
            "send_direct",
            &serde_json::json!({"destination": "longer", "text": "hello"}),
        );
        assert!(matches!(
            processor.process("meshquill/wrong", &direct),
            Err(CommandError::UnexpectedTopic)
        ));
        assert!(matches!(
            processor.process(&topic, &vec![b'x'; 513]),
            Err(CommandError::PayloadTooLarge { .. })
        ));
        assert!(matches!(
            processor.process(&topic, &direct),
            Err(CommandError::DestinationTooLong { .. })
        ));

        let channel = command_payload(
            Uuid::now_v7(),
            "remote",
            "send_channel",
            &serde_json::json!({"channel": 3, "text": "hello"}),
        );
        assert!(matches!(
            processor.process(&topic, &channel),
            Err(CommandError::ChannelOutOfRange { .. })
        ));

        let text = command_payload(
            Uuid::now_v7(),
            "remote",
            "send_direct",
            &serde_json::json!({"destination": "alice", "text": "too long!"}),
        );
        assert!(matches!(
            processor.process(&topic, &text),
            Err(CommandError::TextTooLong { .. })
        ));
    }

    #[test]
    fn local_origin_and_duplicate_ids_are_rejected() {
        let config = enabled_config();
        let topic = TopicSet::new(&config.topic_prefix)
            .expect("valid topics")
            .outbound_send()
            .to_owned();
        let mut processor = CommandProcessor::new(&config).expect("valid processor");
        let event_id = Uuid::now_v7();

        let local = command_payload(
            event_id,
            &config.origin,
            "send_direct",
            &serde_json::json!({"destination": "alice", "text": "hello"}),
        );
        assert!(matches!(
            processor.process(&topic, &local),
            Err(CommandError::LocalOriginLoop)
        ));

        let remote = command_payload(
            event_id,
            "remote",
            "send_direct",
            &serde_json::json!({"destination": "alice", "text": "hello"}),
        );
        assert!(processor.process(&topic, &remote).is_ok());
        assert!(matches!(
            processor.process(&topic, &remote),
            Err(CommandError::DuplicateEvent)
        ));
    }

    #[test]
    fn dedupe_is_bounded_and_expires_entries() {
        let start = Instant::now();
        let mut dedupe = EventIdDedupe::new(2, Duration::from_secs(5)).expect("valid cache");
        let first = Uuid::now_v7();
        let second = Uuid::now_v7();
        let third = Uuid::now_v7();

        assert_eq!(
            dedupe.check_and_insert_at(first, start),
            DedupeDecision::Fresh
        );
        assert_eq!(
            dedupe.check_and_insert_at(first, start + Duration::from_secs(1)),
            DedupeDecision::Duplicate
        );
        assert_eq!(
            dedupe.check_and_insert_at(second, start + Duration::from_secs(1)),
            DedupeDecision::Fresh
        );
        assert_eq!(
            dedupe.check_and_insert_at(third, start + Duration::from_secs(2)),
            DedupeDecision::Fresh
        );
        assert_eq!(dedupe.len(), 2);
        assert_eq!(
            dedupe.check_and_insert_at(first, start + Duration::from_secs(3)),
            DedupeDecision::Fresh
        );
        assert_eq!(dedupe.len(), 2);
        assert_eq!(
            dedupe.check_and_insert_at(first, start + Duration::from_secs(9)),
            DedupeDecision::Fresh
        );
    }

    #[test]
    fn dedupe_constructor_rejects_bounds_above_the_config_limits() {
        let capacity_result = EventIdDedupe::new(
            MAX_DEDUPE_CAPACITY + 1,
            Duration::from_secs(MAX_DEDUPE_TTL_SECS),
        );
        assert!(matches!(
            capacity_result,
            Err(DedupeError::CapacityExceedsLimit {
                configured,
                maximum,
            }) if configured == MAX_DEDUPE_CAPACITY + 1 && maximum == MAX_DEDUPE_CAPACITY
        ));

        let ttl_result = EventIdDedupe::new(
            MAX_DEDUPE_CAPACITY,
            Duration::from_secs(MAX_DEDUPE_TTL_SECS + 1),
        );
        assert!(matches!(
            ttl_result,
            Err(DedupeError::TtlExceedsLimit {
                configured,
                maximum,
            }) if configured == Duration::from_secs(MAX_DEDUPE_TTL_SECS + 1)
                && maximum == Duration::from_secs(MAX_DEDUPE_TTL_SECS)
        ));
    }

    #[test]
    fn command_debug_redacts_destination_text_and_origin() {
        const DESTINATION: &str = "SENTINEL_DESTINATION_DO_NOT_LOG";
        const DIRECT_TEXT: &str = "SENTINEL_DIRECT_TEXT_DO_NOT_LOG";
        const CHANNEL_TEXT: &str = "SENTINEL_CHANNEL_TEXT_DO_NOT_LOG";
        const ORIGIN: &str = "SENTINEL_ORIGIN_DO_NOT_LOG";

        let event_id = Uuid::now_v7();
        let direct = crate::runner::GatewayNotice::Command(AcceptedCommand {
            event_id,
            origin: ORIGIN.to_owned(),
            timestamp: 1_725_000_000_000,
            command: SendCommand::Direct {
                destination: DESTINATION.to_owned(),
                text: DIRECT_TEXT.to_owned(),
            },
        });
        let channel = SendCommand::Channel {
            channel: 3,
            text: CHANNEL_TEXT.to_owned(),
        };
        let debug = format!("{direct:?} {channel:?}");

        for sentinel in [DESTINATION, DIRECT_TEXT, CHANNEL_TEXT, ORIGIN] {
            assert!(!debug.contains(sentinel));
        }
        assert!(debug.contains("Command(AcceptedCommand"));
        assert!(debug.contains(&event_id.to_string()));
        assert!(debug.contains("Direct"));
        assert!(debug.contains("Channel"));
        assert!(debug.contains("destination_bytes"));
        assert!(debug.contains("text_bytes"));
        assert!(debug.contains("origin_bytes"));
        assert!(debug.contains("channel: 3"));
    }

    #[test]
    fn unknown_data_fields_are_rejected() {
        let config = enabled_config();
        let topic = TopicSet::new(&config.topic_prefix)
            .expect("valid topics")
            .outbound_send()
            .to_owned();
        let mut processor = CommandProcessor::new(&config).expect("valid processor");
        let payload = command_payload(
            Uuid::now_v7(),
            "remote",
            "send_direct",
            &serde_json::json!({
                "destination": "alice",
                "text": "hello",
                "admin": true
            }),
        );
        assert!(matches!(
            processor.process(&topic, &payload),
            Err(CommandError::InvalidCommandData(_))
        ));
    }

    #[test]
    fn dedupe_config_shape_is_used() {
        let config = MqttConfig {
            allow_send: true,
            dedupe: crate::config::DedupeConfig {
                capacity: 1,
                ttl_secs: 1,
            },
            ..MqttConfig::default()
        };
        assert!(CommandProcessor::new(&config).is_ok());
    }
}
