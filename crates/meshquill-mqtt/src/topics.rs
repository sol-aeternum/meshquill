use crate::config::{ConfigError, validate_topic_prefix};
use crate::schema::EventKind;

/// Version identifier included in every MQTT topic owned by this gateway.
pub const SCHEMA_VERSION: &str = "meshquill.mqtt/v1";

/// Validated, fully expanded MQTT topics used by the gateway.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicSet {
    incoming_message: String,
    ack: String,
    connection_state: String,
    contacts: String,
    telemetry: String,
    outbound_send: String,
}

impl TopicSet {
    /// Constructs the fixed v1 topic set below a validated prefix.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when the prefix is not a safe MQTT topic namespace.
    pub fn new(prefix: &str) -> Result<Self, ConfigError> {
        validate_topic_prefix(prefix)?;
        let root = format!("{prefix}/{SCHEMA_VERSION}");
        Ok(Self {
            incoming_message: format!("{root}/events/incoming_message"),
            ack: format!("{root}/events/ack"),
            connection_state: format!("{root}/events/connection_state"),
            contacts: format!("{root}/events/contacts"),
            telemetry: format!("{root}/events/telemetry"),
            outbound_send: format!("{root}/outbound/send"),
        })
    }

    /// Topic for incoming `MeshCore` messages.
    #[must_use]
    pub fn incoming_message(&self) -> &str {
        &self.incoming_message
    }

    /// Topic for `MeshCore` acknowledgements.
    #[must_use]
    pub fn ack(&self) -> &str {
        &self.ack
    }

    /// Topic for connection state changes.
    #[must_use]
    pub fn connection_state(&self) -> &str {
        &self.connection_state
    }

    /// Topic for contact snapshots.
    #[must_use]
    pub fn contacts(&self) -> &str {
        &self.contacts
    }

    /// Topic for telemetry.
    #[must_use]
    pub fn telemetry(&self) -> &str {
        &self.telemetry
    }

    /// The only topic to which command-enabled gateways subscribe.
    #[must_use]
    pub fn outbound_send(&self) -> &str {
        &self.outbound_send
    }

    /// Resolves a publishable event kind to its fixed topic.
    #[must_use]
    pub fn for_event(&self, kind: EventKind) -> Option<&str> {
        match kind {
            EventKind::IncomingMessage => Some(self.incoming_message()),
            EventKind::Ack => Some(self.ack()),
            EventKind::ConnectionState => Some(self.connection_state()),
            EventKind::Contacts => Some(self.contacts()),
            EventKind::Telemetry => Some(self.telemetry()),
            EventKind::SendDirect | EventKind::SendChannel => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topics_are_fixed_and_versioned() {
        let topics = TopicSet::new("site/device").expect("valid prefix");
        assert_eq!(
            topics.incoming_message(),
            "site/device/meshquill.mqtt/v1/events/incoming_message"
        );
        assert_eq!(
            topics.outbound_send(),
            "site/device/meshquill.mqtt/v1/outbound/send"
        );
        assert_eq!(
            topics.for_event(EventKind::Telemetry),
            Some("site/device/meshquill.mqtt/v1/events/telemetry")
        );
        assert_eq!(topics.for_event(EventKind::SendDirect), None);
    }

    #[test]
    fn wildcard_prefix_is_rejected() {
        assert!(TopicSet::new("site/+").is_err());
        assert!(TopicSet::new("site//device").is_err());
    }
}
