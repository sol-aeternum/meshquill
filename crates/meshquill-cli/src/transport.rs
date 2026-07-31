//! Transport selector and constructor helpers for CLI runtime.

use std::time::Duration;

use async_trait::async_trait;
use meshquill_core::TransportError;
use meshquill_core::transport::{ReconnectableTransport, Transport, TransportKind};
use meshquill_store::{DeviceProfile, TransportConfig};
use meshquill_transport::{BleTransport, SerialTransport, TcpTransport};
use thiserror::Error;

use meshquill_test_support::{
    ContactFixture, VirtualCompanion, VirtualCompanionCapacities, VirtualCompanionError,
    VirtualCompanionIdleReadMode, make_contact_row, make_direct_message_packet,
};

use crate::reconnect::DEVICE_RECONNECT_ATTEMPTS;

const DEMO_CONTACT_KEY: [u8; 32] = [0x22; 32];
const RECONNECT_DEMO_MESSAGE: &str = "Live direct message after deterministic reconnect";

/// CLI transport wrapper selecting and delegating concrete transport implementations.
#[derive(Debug)]
pub(crate) enum CliTransport {
    /// Bluetooth Low Energy transport selected from profile configuration.
    Ble(BleTransport),
    /// Serial transport selected from profile configuration.
    Serial(SerialTransport),
    /// Framed TCP transport selected from profile configuration.
    Tcp(TcpTransport),
    /// Deterministic in-memory transport used only for explicit mock profiles.
    VirtualCompanion(VirtualCompanion),
}

/// Build errors for transport creation and mock-fixture seeding.
#[derive(Debug, Error)]
pub(crate) enum CliTransportBuildError {
    /// The transport profile could not be mapped to a concrete target.
    #[error("invalid {transport} transport configuration: {message}")]
    InvalidTransportConfig {
        /// Transport variant name for diagnostics.
        transport: &'static str,
        /// Concrete validation message.
        message: String,
    },
    /// The mock profile scenario is unknown.
    #[error(
        "unsupported mock scenario {scenario}; expected one of: demo, ack-timeout, reconnect-demo, reconnect-fail, send-disconnect"
    )]
    UnknownMockScenario {
        /// Scenario string provided by profile configuration.
        scenario: String,
    },
    /// The mock transport fixture could not be created.
    #[error("mock transport fixture failed: {0}")]
    MockFixture(#[from] VirtualCompanionError),
}

impl CliTransport {
    /// Build a transport from a stored profile and connect timeout override.
    pub(crate) fn from_profile(
        profile: &DeviceProfile,
        connect_timeout: Duration,
    ) -> Result<Self, CliTransportBuildError> {
        match &profile.transport {
            TransportConfig::Ble { id, .. } => {
                Ok(Self::Ble(BleTransport::new(id, connect_timeout).map_err(
                    |error| CliTransportBuildError::InvalidTransportConfig {
                        transport: "ble",
                        message: error.to_string(),
                    },
                )?))
            }
            TransportConfig::Serial { port, baud } => Ok(Self::Serial(
                SerialTransport::new(port, *baud, connect_timeout).map_err(|error| {
                    CliTransportBuildError::InvalidTransportConfig {
                        transport: "serial",
                        message: error.to_string(),
                    }
                })?,
            )),
            TransportConfig::Tcp { host, port } => Ok(Self::Tcp(
                TcpTransport::new(host, *port, connect_timeout).map_err(|error| {
                    CliTransportBuildError::InvalidTransportConfig {
                        transport: "tcp",
                        message: error.to_string(),
                    }
                })?,
            )),
            TransportConfig::Mock { scenario } => Ok(Self::VirtualCompanion(
                Self::seeded_virtual_companion(scenario)?,
            )),
        }
    }

    fn seeded_virtual_companion(
        scenario: &str,
    ) -> Result<VirtualCompanion, CliTransportBuildError> {
        let emit_send_txt_ack = match scenario {
            "demo" | "reconnect-demo" | "reconnect-fail" | "send-disconnect" => true,
            "ack-timeout" => false,
            scenario => {
                return Err(CliTransportBuildError::UnknownMockScenario {
                    scenario: scenario.to_string(),
                });
            }
        };

        let companion =
            VirtualCompanion::with_capacities(VirtualCompanionCapacities::new(256, 128, 4, 4));
        companion.set_contacts([make_contact_row(&ContactFixture {
            public_key: DEMO_CONTACT_KEY,
            contact_type: 0,
            route: u8::MAX,
            path: &[],
            adv_name: "Alice",
            last_advert: 1,
            adv_lat: -34.9285,
            adv_lon: 138.6007,
            lastmod: 1,
        })?])?;
        companion.set_remote_session(DEMO_CONTACT_KEY, true);
        companion.push_sync_message(make_direct_message_packet(
            u8::MAX,
            "Demo direct packet for deterministic CLI tests",
        )?)?;
        companion.configure_send_txt_ack([0x12, 0x34, 0x56, 0x78], 1_000, emit_send_txt_ack);
        match scenario {
            "reconnect-demo" => {
                companion.disconnect_on_next_idle_read();
                companion.fail_next_reconnects(2);
                companion.set_next_reconnect_push(make_direct_message_packet(
                    u8::MAX,
                    RECONNECT_DEMO_MESSAGE,
                )?)?;
            }
            "reconnect-fail" => {
                companion.disconnect_on_next_idle_read();
                companion.fail_next_reconnects(DEVICE_RECONNECT_ATTEMPTS);
            }
            "send-disconnect" => {
                companion.disconnect_before_next_direct_send();
            }
            "ack-timeout" => {
                companion.set_idle_read_mode(VirtualCompanionIdleReadMode::Pending);
            }
            "demo" => {}
            _ => unreachable!("scenario was validated before fixture construction"),
        }
        Ok(companion)
    }
}

#[async_trait]
impl Transport for CliTransport {
    fn kind(&self) -> TransportKind {
        match self {
            Self::Ble(transport) => transport.kind(),
            Self::Serial(transport) => transport.kind(),
            Self::Tcp(transport) => transport.kind(),
            Self::VirtualCompanion(transport) => transport.kind(),
        }
    }

    async fn connect(&mut self) -> Result<(), TransportError> {
        match self {
            Self::Ble(transport) => transport.connect().await,
            Self::Serial(transport) => transport.connect().await,
            Self::Tcp(transport) => transport.connect().await,
            Self::VirtualCompanion(transport) => transport.connect().await,
        }
    }

    async fn disconnect(&mut self) -> Result<(), TransportError> {
        match self {
            Self::Ble(transport) => transport.disconnect().await,
            Self::Serial(transport) => transport.disconnect().await,
            Self::Tcp(transport) => transport.disconnect().await,
            Self::VirtualCompanion(transport) => transport.disconnect().await,
        }
    }

    async fn write(&mut self, payload: &[u8]) -> Result<(), TransportError> {
        match self {
            Self::Ble(transport) => transport.write(payload).await,
            Self::Serial(transport) => transport.write(payload).await,
            Self::Tcp(transport) => transport.write(payload).await,
            Self::VirtualCompanion(transport) => transport.write(payload).await,
        }
    }

    async fn read(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        match self {
            Self::Ble(transport) => transport.read().await,
            Self::Serial(transport) => transport.read().await,
            Self::Tcp(transport) => transport.read().await,
            Self::VirtualCompanion(transport) => transport.read().await,
        }
    }
}

#[async_trait]
impl ReconnectableTransport for CliTransport {
    async fn reconnect(&mut self) -> Result<(), TransportError> {
        match self {
            Self::Ble(transport) => transport.reconnect().await,
            Self::Serial(transport) => transport.reconnect().await,
            Self::Tcp(transport) => transport.reconnect().await,
            Self::VirtualCompanion(transport) => transport.reconnect().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meshquill_core::{Client, CoreError};

    fn mock_profile(scenario: &str) -> DeviceProfile {
        DeviceProfile {
            transport: TransportConfig::Mock {
                scenario: scenario.to_string(),
            },
            transport_overrides: None,
            secret: None,
        }
    }

    #[test]
    fn mock_profile_accepts_only_known_scenarios() {
        let unknown =
            CliTransport::from_profile(&mock_profile("unknown"), Duration::from_millis(10))
                .expect_err("unknown mock scenario should fail");
        assert!(matches!(
            unknown,
            CliTransportBuildError::UnknownMockScenario { .. }
        ));

        for scenario in [
            "ack-timeout",
            "reconnect-demo",
            "reconnect-fail",
            "send-disconnect",
        ] {
            let transport =
                CliTransport::from_profile(&mock_profile(scenario), Duration::from_millis(10))
                    .unwrap_or_else(|error| {
                        panic!("{scenario} scenario should construct: {error}")
                    });
            assert!(matches!(transport, CliTransport::VirtualCompanion(_)));
        }
    }

    #[test]
    fn mock_profile_is_rejected_in_demo_and_ack_scenarios_without_errors() {
        let demo = CliTransport::from_profile(&mock_profile("demo"), Duration::from_millis(10))
            .expect("demo scenario should construct");
        assert!(matches!(demo, CliTransport::VirtualCompanion(_)));
    }

    #[tokio::test]
    async fn ack_timeout_queues_no_ack_and_keeps_the_idle_read_pending() {
        let transport =
            CliTransport::from_profile(&mock_profile("ack-timeout"), Duration::from_millis(10))
                .expect("ack-timeout scenario should construct");
        let mut client = Client::new(transport);
        let _ = client
            .connect()
            .await
            .expect("mock handshake should succeed");
        let tracking = client
            .send_direct_text(&[0x22; 6], 0, "no acknowledgement")
            .await
            .expect("mock send should return MSG_SENT");

        let idle = tokio::time::timeout(Duration::from_millis(10), client.next_event()).await;
        assert!(idle.is_err(), "idle read returned an unexpected ACK");
        let ack = client
            .wait_for_ack(tracking.ack_code, Some(Duration::from_millis(10)))
            .await;
        assert!(matches!(ack, Err(CoreError::Timeout)));
    }

    #[tokio::test]
    async fn demo_scenario_retains_immediate_idle_timeout() {
        let mut transport =
            CliTransport::from_profile(&mock_profile("demo"), Duration::from_millis(10))
                .expect("demo scenario should construct");
        transport
            .connect()
            .await
            .expect("mock connect should succeed");

        let idle = tokio::time::timeout(Duration::from_millis(20), transport.read())
            .await
            .expect("demo idle read remained pending");
        assert!(matches!(idle, Err(TransportError::Timeout)));
    }
}
