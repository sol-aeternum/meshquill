use std::fmt;
use thiserror::Error;

/// Core-level errors for parser, transport, and client operations.
#[derive(Debug, Error)]
pub enum CoreError {
    /// A transport-level error from the active connection implementation.
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),

    /// A parser/codec error while handling inbound bytes.
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),

    /// A timeout was hit while waiting for a response.
    #[error("operation timed out")]
    Timeout,

    /// A protocol invariant was violated in the client state machine.
    #[error("protocol invariant violation: {0}")]
    ProtocolInvariant(&'static str),

    /// Firmware explicitly rejected an otherwise well-formed operation.
    #[error("device rejected {operation} (code {code:?})")]
    DeviceRejected {
        /// Stable operation label without user-provided or secret data.
        operation: &'static str,
        /// Optional firmware error code.
        code: Option<u8>,
    },

    /// Firmware was compiled without the requested optional feature.
    #[error("device feature is disabled: {feature}")]
    FeatureDisabled {
        /// Stable feature label.
        feature: &'static str,
    },

    /// A remote peer rejected authentication without echoing credential data.
    #[error("remote authentication failed")]
    AuthenticationFailed,

    /// A caller supplied a value that cannot be represented safely on the wire.
    #[error("invalid argument {field}: {message}")]
    InvalidArgument {
        /// Public argument name.
        field: &'static str,
        /// Validation failure detail.
        message: String,
    },

    /// A UTF-8 payload could not be decoded.
    #[error("invalid utf8 payload in {field}")]
    InvalidUtf8 {
        /// Logical field that failed UTF-8 decoding.
        field: &'static str,
    },

    /// The transport disconnected.
    #[error("transport disconnected")]
    Disconnected,

    /// The managed client actor is no longer available to accept commands.
    ///
    /// This error intentionally carries no command data so payloads are not exposed through
    /// diagnostics.
    #[error("managed client actor stopped")]
    ActorStopped,
}

impl From<std::io::Error> for CoreError {
    fn from(error: std::io::Error) -> Self {
        Self::Transport(TransportError::Io(error))
    }
}

/// Protocol parsing errors.
#[derive(Debug, Error)]
pub enum ParseError {
    /// Packet code is unknown.
    #[error("unknown packet code {code:#04x}")]
    UnknownPacketCode {
        /// Raw packet code.
        code: u8,
    },

    /// Packet payload length was too short for expected shape.
    #[error("invalid packet length for {code:?}: expected at least {minimum}, got {actual}")]
    InvalidPacketLength {
        /// Encoded packet code for the failing packet.
        code: PacketDisplay,
        /// Minimum valid byte length.
        minimum: usize,
        /// Actual byte length observed.
        actual: usize,
    },

    /// Packet payload length exceeded configured boundaries.
    #[error("oversized packet payload: {actual} > {maximum}")]
    OversizedPacketPayload {
        /// Observed payload length.
        actual: usize,
        /// Maximum supported payload length.
        maximum: usize,
    },

    /// Packet payload contains bytes that are not valid UTF-8 where text is required.
    #[error("invalid UTF-8 payload for {context}")]
    InvalidUtf8Payload {
        /// Parsing context for the malformed payload.
        context: &'static str,
    },

    /// Packet payload was structurally malformed.
    #[error("malformed packet: {reason}")]
    Malformed {
        /// Human-readable reason for the malformed packet.
        reason: &'static str,
    },
}

/// Transport errors.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Generic transport I/O failure.
    #[error("transport i/o failure: {0}")]
    Io(#[from] std::io::Error),

    /// Transport is not connected.
    #[error("not connected")]
    NotConnected,

    /// Reconnect failed.
    #[error("reconnect failed: {message}")]
    ReconnectFailed {
        /// Provider-specific reconnect failure reason.
        message: &'static str,
    },

    /// Reconnect disabled by transport implementation.
    #[error("reconnect not supported")]
    ReconnectUnsupported,

    /// A timeout occurred in transport operation.
    #[error("transport timeout")]
    Timeout,

    /// A bounded transport queue cannot accept another logical packet.
    #[error("transport queue '{queue}' is full (capacity {capacity})")]
    Backpressure {
        /// Queue that rejected the packet.
        queue: &'static str,
        /// Configured queue capacity.
        capacity: usize,
    },

    /// A logical packet exceeds the transport protocol's payload limit.
    #[error("transport payload is too large: {actual} bytes (maximum {maximum})")]
    PayloadTooLarge {
        /// Maximum supported logical packet size.
        maximum: usize,
        /// Observed logical packet size.
        actual: usize,
    },

    /// Transport already closed.
    #[error("closed")]
    Closed,
}

/// Packet code display helper for parse errors.
#[derive(Clone, Copy, Debug)]
pub enum PacketDisplay {
    /// Hexadecimal raw packet code.
    Raw(u8),
}

impl fmt::Display for PacketDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PacketDisplay::Raw(code) => write!(f, "{code:#04x}"),
        }
    }
}
