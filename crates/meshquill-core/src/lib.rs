#![warn(missing_docs, unreachable_pub)]

//! Meshquill core protocol and client primitives.

/// Client abstraction for companion transport sessions.
pub mod client;
/// Domain model types for public protocol objects and events.
pub mod domain;
/// Shared protocol and transport errors.
pub mod error;
/// Serial/TCP frame codecs and codec error handling.
pub mod framing;
/// Single-owner asynchronous actor handle for serialized client operations.
pub mod managed;
/// Companion command and packet encoding, parsing, and typed events.
pub mod protocol;
/// Remote request/response helper payloads.
pub mod remote;
/// Transport trait definitions and in-process scripted transport implementation.
pub mod transport;

pub use client::Client;
pub use domain::{
    Ack, AdvertPath, AutoAddConfig, BatteryInfo, BinaryResponse, Contact, ContactRoute,
    ContactType, ContactUri, ControlData, CustomVariable, CustomVariables, DefaultFloodScope,
    DeviceInfo, DeviceStats, Event, FloodScope, FrequencyRange, LoginSession, Message,
    MessageRoute, MessageStatus, NodeDiscoveryResponse, Path, PathDiscovery, PrivateKeyMaterial,
    PublicKey, RadioParams, RemoteStatus, Scope, SelfInfo, Signature, StatsType, TelemetryResponse,
    TuningParams,
};
pub use error::{CoreError, ParseError, TransportError};
pub use framing::{InboundFrame, OutboundFrame, OuterDecoder, OuterEncoder};
pub use managed::{MANAGED_CLIENT_COMMAND_CAPACITY, ManagedClient};
pub use protocol::{Command, CommandCode, CommandError, Packet, PacketCode};
pub use transport::{ReconnectableTransport, Transport, TransportKind};
