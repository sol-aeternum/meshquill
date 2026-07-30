#![warn(missing_docs, unreachable_pub)]

//! Versioned application-level MQTT event gateway for Meshquill.
//!
//! This crate bridges application events to a broker. It deliberately does not
//! implement [`meshquill_core::transport::Transport`] and is never a `MeshCore`
//! radio/companion transport.

mod backoff;
mod command;
mod config;
mod runner;
mod schema;
mod topics;

pub use backoff::{BackoffError, ExponentialBackoff};
pub use command::{
    AcceptedCommand, CommandError, CommandProcessor, DedupeDecision, DedupeError, EventIdDedupe,
    SendCommand,
};
pub use config::{
    CommandLimits, ConfigError, DedupeConfig, MAX_BROKER_OPERATION_TIMEOUT_MS,
    MAX_CONFIGURED_PAYLOAD_BYTES, MAX_RECONNECT_DELAY_MS, MAX_TLS_FILE_BYTES, MqttConfig,
    MqttPassword, MqttProtocol, MqttQos, ReconnectConfig, SessionConfig, TlsConfig,
};
pub use runner::{GatewayError, GatewayHandle, GatewayNotice, GatewayPublisher, GatewayRunner};
pub use schema::{
    ConnectionComponent, ConnectionStateData, ConnectionStatus, ContactsData, EventEnvelope,
    EventKind, Publication, SchemaError, TelemetryData,
};
pub use topics::{SCHEMA_VERSION, TopicSet};
