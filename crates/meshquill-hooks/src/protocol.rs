use std::fmt;

use serde::{Deserialize, Serialize};

struct Redacted(usize);

impl fmt::Debug for Redacted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "<redacted:{} bytes>", self.0)
    }
}

struct OptionalRedacted(Option<usize>);

impl fmt::Debug for OptionalRedacted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(length) => write!(formatter, "Some(<redacted:{length} bytes>)"),
            None => formatter.write_str("None"),
        }
    }
}

macro_rules! redacted_debug {
    ($name:ident { $($field:ident $(: $kind:ident)?),* $(,)? }) => {
        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = formatter.debug_struct(stringify!($name));
                $(redacted_debug!(@field debug, self, $field $(: $kind)?);)*
                debug.finish()
            }
        }
    };
    (@field $debug:ident, $self:ident, $field:ident) => {
        $debug.field(stringify!($field), &Redacted($self.$field.len()))
    };
    (@field $debug:ident, $self:ident, $field:ident: optional) => {
        $debug.field(
            stringify!($field),
            &OptionalRedacted($self.$field.as_ref().map(String::len)),
        )
    };
}

/// Schema identifier used by hook event envelopes and runner messages.
pub const PROTOCOL_SCHEMA: &str = "meshquill.hook/v1";

/// A supported hook event name.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HookEventKind {
    /// A companion connection became available.
    OnConnect,
    /// A companion connection ended.
    OnDisconnect,
    /// An inbound message was received.
    OnMessage,
    /// An outbound message is about to be sent.
    BeforeSend,
    /// An outbound message was submitted.
    AfterSend,
    /// An acknowledgement was received.
    OnAck,
    /// An operation timed out.
    OnTimeout,
    /// A contact was added, changed, or removed.
    OnContactUpdate,
    /// A non-fatal application error occurred.
    OnError,
}

impl HookEventKind {
    pub(crate) const fn handler_name(self) -> &'static str {
        match self {
            Self::OnConnect => "on_connect",
            Self::OnDisconnect => "on_disconnect",
            Self::OnMessage => "on_message",
            Self::BeforeSend => "before_send",
            Self::AfterSend => "after_send",
            Self::OnAck => "on_ack",
            Self::OnTimeout => "on_timeout",
            Self::OnContactUpdate => "on_contact_update",
            Self::OnError => "on_error",
        }
    }
}

impl fmt::Display for HookEventKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.handler_name())
    }
}

/// Details about a newly established connection.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnConnectPayload {
    /// Non-secret transport description, such as `serial` or `tcp`.
    pub transport: String,
    /// Optional peer or device label.
    pub peer: Option<String>,
}

redacted_debug!(OnConnectPayload {
    transport,
    peer: optional
});

/// Details about a connection that ended.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnDisconnectPayload {
    /// Transport that disconnected.
    pub transport: String,
    /// Optional human-readable reason.
    pub reason: Option<String>,
}

redacted_debug!(OnDisconnectPayload {
    transport,
    reason: optional
});

/// Details about an inbound message.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnMessagePayload {
    /// Sender identifier as exposed by the caller.
    pub source: String,
    /// Message text.
    pub text: String,
    /// Optional caller-assigned message identifier.
    pub message_id: Option<String>,
}

redacted_debug!(OnMessagePayload {
    source,
    text,
    message_id: optional
});

/// Input to the mutating `before_send` hook.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct BeforeSendInput {
    /// Destination identifier.
    pub destination: String,
    /// Outbound message text.
    pub text: String,
}

redacted_debug!(BeforeSendInput { destination, text });

/// Decision returned by a `before_send` hook.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum BeforeSendOutcome {
    /// Send the original destination and text unchanged.
    Allow,
    /// Send bounded, validated replacement values.
    Modify {
        /// Final destination after applying the hook response.
        destination: String,
        /// Final text after applying the hook response.
        text: String,
    },
    /// Do not send the message.
    Reject {
        /// Bounded human-readable rejection reason supplied by trusted local code.
        reason: String,
    },
}

impl BeforeSendOutcome {
    /// Converts a rejection outcome into the typed rejected error category.
    ///
    /// Allow and modify outcomes are returned unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`crate::HookError::Rejected`] when this value is [`Self::Reject`].
    pub fn require_allowed(self) -> Result<Self, crate::HookError> {
        match self {
            Self::Reject { reason } => Err(crate::HookError::rejected(reason)),
            allowed => Ok(allowed),
        }
    }
}

impl fmt::Debug for BeforeSendOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow => formatter.write_str("Allow"),
            Self::Modify { destination, text } => formatter
                .debug_struct("Modify")
                .field("destination", &Redacted(destination.len()))
                .field("text", &Redacted(text.len()))
                .finish(),
            Self::Reject { reason } => formatter
                .debug_struct("Reject")
                .field("reason", &Redacted(reason.len()))
                .finish(),
        }
    }
}

/// Details about a submitted outbound message.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct AfterSendPayload {
    /// Destination identifier.
    pub destination: String,
    /// Submitted message text.
    pub text: String,
    /// Optional caller-assigned message identifier.
    pub message_id: Option<String>,
}

redacted_debug!(AfterSendPayload {
    destination,
    text,
    message_id: optional
});

/// Details about a received acknowledgement.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnAckPayload {
    /// Identifier of the acknowledged message.
    pub message_id: String,
    /// Optional acknowledgement source.
    pub source: Option<String>,
    /// Optional measured round-trip time.
    pub round_trip_ms: Option<u64>,
}

impl fmt::Debug for OnAckPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OnAckPayload")
            .field("message_id", &Redacted(self.message_id.len()))
            .field(
                "source",
                &OptionalRedacted(self.source.as_ref().map(String::len)),
            )
            .field("round_trip_ms", &self.round_trip_ms)
            .finish()
    }
}

/// Details about an operation timeout.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnTimeoutPayload {
    /// Name of the operation that timed out.
    pub operation: String,
    /// Optional related message identifier.
    pub message_id: Option<String>,
}

redacted_debug!(OnTimeoutPayload {
    operation,
    message_id: optional
});

/// Kind of contact directory change.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContactChange {
    /// A contact was added.
    Added,
    /// A contact was updated.
    Updated,
    /// A contact was removed.
    Removed,
}

/// Details about a contact directory change.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnContactUpdatePayload {
    /// Stable contact identifier.
    pub contact_id: String,
    /// Optional display name after the change.
    pub display_name: Option<String>,
    /// Kind of directory change.
    pub change: ContactChange,
}

impl fmt::Debug for OnContactUpdatePayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OnContactUpdatePayload")
            .field("contact_id", &Redacted(self.contact_id.len()))
            .field(
                "display_name",
                &OptionalRedacted(self.display_name.as_ref().map(String::len)),
            )
            .field("change", &self.change)
            .finish()
    }
}

/// Sanitized application error details passed to an observational hook.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct OnErrorPayload {
    /// Operation that encountered the error.
    pub operation: String,
    /// Human-readable, caller-sanitized error summary.
    pub message: String,
}

redacted_debug!(OnErrorPayload { operation, message });

/// A typed hook event and its event-specific payload.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event", content = "payload", rename_all = "snake_case")]
pub enum HookEvent {
    /// A companion connection became available.
    OnConnect(OnConnectPayload),
    /// A companion connection ended.
    OnDisconnect(OnDisconnectPayload),
    /// An inbound message was received.
    OnMessage(OnMessagePayload),
    /// An outbound message is about to be sent.
    BeforeSend(BeforeSendInput),
    /// An outbound message was submitted.
    AfterSend(AfterSendPayload),
    /// An acknowledgement was received.
    OnAck(OnAckPayload),
    /// An operation timed out.
    OnTimeout(OnTimeoutPayload),
    /// A contact was added, changed, or removed.
    OnContactUpdate(OnContactUpdatePayload),
    /// A non-fatal application error occurred.
    OnError(OnErrorPayload),
}

impl HookEvent {
    /// Returns the stable event discriminator.
    #[must_use]
    pub const fn kind(&self) -> HookEventKind {
        match self {
            Self::OnConnect(_) => HookEventKind::OnConnect,
            Self::OnDisconnect(_) => HookEventKind::OnDisconnect,
            Self::OnMessage(_) => HookEventKind::OnMessage,
            Self::BeforeSend(_) => HookEventKind::BeforeSend,
            Self::AfterSend(_) => HookEventKind::AfterSend,
            Self::OnAck(_) => HookEventKind::OnAck,
            Self::OnTimeout(_) => HookEventKind::OnTimeout,
            Self::OnContactUpdate(_) => HookEventKind::OnContactUpdate,
            Self::OnError(_) => HookEventKind::OnError,
        }
    }
}

impl fmt::Debug for HookEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HookEvent")
            .field("kind", &self.kind())
            .field("payload", &"<redacted>")
            .finish()
    }
}

#[derive(Serialize)]
#[serde(untagged)]
pub(crate) enum PayloadRef<'a> {
    OnConnect(&'a OnConnectPayload),
    OnDisconnect(&'a OnDisconnectPayload),
    OnMessage(&'a OnMessagePayload),
    BeforeSend(&'a BeforeSendInput),
    AfterSend(&'a AfterSendPayload),
    OnAck(&'a OnAckPayload),
    OnTimeout(&'a OnTimeoutPayload),
    OnContactUpdate(&'a OnContactUpdatePayload),
    OnError(&'a OnErrorPayload),
}

pub(crate) const fn payload_ref(event: &HookEvent) -> PayloadRef<'_> {
    match event {
        HookEvent::OnConnect(payload) => PayloadRef::OnConnect(payload),
        HookEvent::OnDisconnect(payload) => PayloadRef::OnDisconnect(payload),
        HookEvent::OnMessage(payload) => PayloadRef::OnMessage(payload),
        HookEvent::BeforeSend(payload) => PayloadRef::BeforeSend(payload),
        HookEvent::AfterSend(payload) => PayloadRef::AfterSend(payload),
        HookEvent::OnAck(payload) => PayloadRef::OnAck(payload),
        HookEvent::OnTimeout(payload) => PayloadRef::OnTimeout(payload),
        HookEvent::OnContactUpdate(payload) => PayloadRef::OnContactUpdate(payload),
        HookEvent::OnError(payload) => PayloadRef::OnError(payload),
    }
}
