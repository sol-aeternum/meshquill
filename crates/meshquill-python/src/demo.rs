use std::time::Duration;

use async_trait::async_trait;
use meshquill_core::{ReconnectableTransport, Transport, TransportError, TransportKind};
use meshquill_test_support::{
    ContactFixture, VirtualCompanion, VirtualCompanionCapacities, VirtualCompanionError,
    make_channel_message_packet, make_contact_row,
};

const DEMO_IDLE_DELAY: Duration = Duration::from_millis(100);

pub(crate) struct DemoTransport {
    companion: VirtualCompanion,
}

impl DemoTransport {
    pub(crate) fn seeded() -> Result<Self, VirtualCompanionError> {
        let companion =
            VirtualCompanion::with_capacities(VirtualCompanionCapacities::new(512, 128, 16, 16));
        companion.set_contacts([
            make_contact_row(&ContactFixture {
                public_key: [0x22; 32],
                contact_type: 0,
                route: u8::MAX,
                path: &[],
                adv_name: "Alice",
                last_advert: 1,
                adv_lat: -34.9285,
                adv_lon: 138.6007,
                lastmod: 1,
            })?,
            make_contact_row(&ContactFixture {
                public_key: [0x33; 32],
                contact_type: 0,
                route: u8::MAX,
                path: &[],
                adv_name: "Bob",
                last_advert: 2,
                adv_lat: -33.8688,
                adv_lon: 151.2093,
                lastmod: 2,
            })?,
        ])?;
        companion.set_sync_messages([make_channel_message_packet(
            1,
            "Hello from the deterministic Meshquill demo companion",
        )?])?;
        companion.configure_send_txt_ack([0x12, 0x34, 0x56, 0x78], 1_000, true);
        Ok(Self { companion })
    }
}

#[async_trait]
impl Transport for DemoTransport {
    fn kind(&self) -> TransportKind {
        TransportKind::Scripted
    }

    async fn connect(&mut self) -> Result<(), TransportError> {
        self.companion.connect().await
    }

    async fn disconnect(&mut self) -> Result<(), TransportError> {
        self.companion.disconnect().await
    }

    async fn write(&mut self, payload: &[u8]) -> Result<(), TransportError> {
        self.companion.write(payload).await
    }

    async fn read(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        match self.companion.read().await {
            Err(TransportError::Timeout) => {
                tokio::time::sleep(DEMO_IDLE_DELAY).await;
                Err(TransportError::Timeout)
            }
            result => result,
        }
    }
}

impl ReconnectableTransport for DemoTransport {}

#[cfg(test)]
mod tests {
    use meshquill_core::{Client, ManagedClient};

    use super::*;

    #[tokio::test]
    async fn seeded_demo_completes_a_managed_handshake() {
        let transport = DemoTransport::seeded().expect("seeded demo transport");
        let client = Client::with_timeout(transport, Duration::from_secs(1));
        let managed = ManagedClient::spawn(client);

        let info = managed.connect().await.expect("demo handshake");
        assert_eq!(info.public_key.as_bytes(), &[1_u8; 32]);
        let contacts = managed.list_contacts(None).await.expect("demo contacts");
        assert_eq!(contacts.len(), 2);
        managed.shutdown().await.expect("demo shutdown");
    }
}
