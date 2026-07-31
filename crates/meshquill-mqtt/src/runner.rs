use std::fmt;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rumqttc::tokio_rustls::rustls::{ClientConfig, RootCertStore};
use rumqttc::{AsyncClient, EventLoop, MqttOptions, Packet, Transport};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::backoff::{BackoffError, ExponentialBackoff};
use crate::command::{AcceptedCommand, CommandError, CommandProcessor};
use crate::config::{
    ConfigError, MAX_TLS_FILE_BYTES, MqttConfig, MqttPassword, MqttProtocol, TlsConfig,
};
use crate::schema::{
    ConnectionComponent, ConnectionStateData, ConnectionStatus, EventEnvelope, EventKind,
    Publication, SchemaError,
};
use crate::topics::TopicSet;

/// A command or safe rejection emitted by the gateway's bounded inbound channel.
#[derive(Debug)]
pub enum GatewayNotice {
    /// Current MQTT broker connection state for local status reporting.
    BrokerState(ConnectionStatus),
    /// The broker granted the exact outbound-command subscription.
    CommandReady,
    /// A fully validated allowlisted send request.
    Command(AcceptedCommand),
    /// An inbound broker publication was safely rejected.
    Rejected(CommandError),
}

/// Cloneable bounded publisher for application events.
#[derive(Clone)]
pub struct GatewayPublisher {
    sender: mpsc::Sender<PreparedPublication>,
    origin: Arc<str>,
    max_payload_bytes: usize,
    cancellation: CancellationToken,
}

impl fmt::Debug for GatewayPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayPublisher")
            .field("origin", &self.origin)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .finish_non_exhaustive()
    }
}

impl GatewayPublisher {
    /// Builds and queues a v1 event with a `UUIDv7` ID and the current Unix timestamp.
    ///
    /// The call applies the configured payload bound before the event enters an async
    /// queue, so an oversized publication is never reported as broker success.
    ///
    /// # Errors
    ///
    /// Returns [`GatewayError`] for clock, schema, size, cancellation, or channel errors.
    pub async fn publish(&self, publication: Publication) -> Result<Uuid, GatewayError> {
        let timestamp = unix_timestamp_millis()?;
        self.publish_with_metadata(publication, Uuid::now_v7(), timestamp)
            .await
    }

    /// Deterministic publication helper with an explicit event ID and timestamp.
    ///
    /// This is useful to write broker integration tests without weakening or faking
    /// the MQTT connection path.
    ///
    /// # Errors
    ///
    /// Returns [`GatewayError`] for schema, size, cancellation, or channel errors.
    pub async fn publish_with_metadata(
        &self,
        publication: Publication,
        event_id: Uuid,
        timestamp: u64,
    ) -> Result<Uuid, GatewayError> {
        let prepared = prepare_publication(
            &publication,
            event_id,
            &self.origin,
            timestamp,
            self.max_payload_bytes,
        )?;
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(GatewayError::Cancelled),
            result = self.sender.send(prepared) => {
                result.map_err(|_| GatewayError::PublicationChannelClosed)?;
                Ok(event_id)
            }
        }
    }
}

/// Application handle paired with a running [`GatewayRunner`].
pub struct GatewayHandle {
    publisher: GatewayPublisher,
    notices: mpsc::Receiver<GatewayNotice>,
    cancellation: CancellationToken,
}

impl fmt::Debug for GatewayHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayHandle")
            .field("publisher", &self.publisher)
            .finish_non_exhaustive()
    }
}

impl GatewayHandle {
    /// Returns a cloneable application event publisher.
    #[must_use]
    pub fn publisher(&self) -> GatewayPublisher {
        self.publisher.clone()
    }

    /// Convenience wrapper around [`GatewayPublisher::publish`].
    ///
    /// # Errors
    ///
    /// Returns [`GatewayError`] when the event cannot be validated or queued.
    pub async fn publish(&self, publication: Publication) -> Result<Uuid, GatewayError> {
        self.publisher.publish(publication).await
    }

    /// Receives the next accepted command or safe rejection.
    pub async fn recv_notice(&mut self) -> Option<GatewayNotice> {
        self.notices.recv().await
    }

    /// Requests cooperative shutdown of the runner and all its network work.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Returns a clone of the cooperative cancellation token.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

impl Drop for GatewayHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

/// MQTT network runner. It is an application gateway, never a `MeshCore` radio transport.
pub struct GatewayRunner {
    config: MqttConfig,
    topics: TopicSet,
    client: ProtocolClient,
    event_loop: Option<Box<ProtocolEventLoop>>,
    publications: mpsc::Receiver<PreparedPublication>,
    notices: mpsc::Sender<GatewayNotice>,
    processor: CommandProcessor,
    cancellation: CancellationToken,
}

impl GatewayRunner {
    /// Validates configuration, loads rustls trust material, and creates a bounded runner.
    ///
    /// The optional password is runtime-only and must match `config.username`. Neither
    /// the password nor the resulting `rumqttc` options are exposed through `Debug` or
    /// serialization by this API.
    ///
    /// # Errors
    ///
    /// Returns [`GatewayError`] for configuration, credential, certificate, or TLS errors.
    pub async fn connect(
        config: MqttConfig,
        password: Option<MqttPassword>,
        cancellation: CancellationToken,
    ) -> Result<(GatewayHandle, Self), GatewayError> {
        config.validate()?;
        config.validate_credentials(password.as_ref())?;
        let transport = build_transport(&config.tls).await?;
        let (client, event_loop) = build_protocol_session(&config, password.as_ref(), transport);
        Self::from_protocol_parts(config, client, event_loop, cancellation)
    }

    /// Creates a runner around caller-provided MQTT 3.1.1 client/event-loop parts.
    ///
    /// This helper preserves the production parser, bounded channels, and cancellation
    /// behavior. It is intended for deterministic tests that need to inspect a real
    /// `rumqttc` session without claiming a broker connection succeeded.
    ///
    /// # Errors
    ///
    /// Returns [`GatewayError`] if the gateway configuration is invalid.
    pub fn from_v311_parts(
        config: MqttConfig,
        client: AsyncClient,
        event_loop: EventLoop,
        cancellation: CancellationToken,
    ) -> Result<(GatewayHandle, Self), GatewayError> {
        config.validate()?;
        if config.protocol != MqttProtocol::V311 {
            return Err(GatewayError::ProtocolPartsMismatch);
        }
        Self::from_protocol_parts(
            config,
            ProtocolClient::V311(client),
            ProtocolEventLoop::V311(Box::new(event_loop)),
            cancellation,
        )
    }

    /// Creates a runner around caller-provided MQTT 5 client/event-loop parts.
    ///
    /// # Errors
    ///
    /// Returns [`GatewayError`] if the configuration is invalid or selects MQTT 3.1.1.
    pub fn from_v5_parts(
        config: MqttConfig,
        client: rumqttc::v5::AsyncClient,
        event_loop: rumqttc::v5::EventLoop,
        cancellation: CancellationToken,
    ) -> Result<(GatewayHandle, Self), GatewayError> {
        config.validate()?;
        if config.protocol != MqttProtocol::V5 {
            return Err(GatewayError::ProtocolPartsMismatch);
        }
        Self::from_protocol_parts(
            config,
            ProtocolClient::V5(client),
            ProtocolEventLoop::V5(Box::new(event_loop)),
            cancellation,
        )
    }

    /// Runs MQTT polling, reconnects with bounded backoff, and bridges bounded channels.
    ///
    /// Invalid broker commands become [`GatewayNotice::Rejected`] and do not terminate
    /// the connection. Cancellation is cooperative and returns success.
    ///
    /// # Errors
    ///
    /// Returns [`GatewayError`] only for local channel/task failures or client requests
    /// that cannot be queued. Broker connection errors are retried until cancellation.
    pub async fn run(mut self) -> Result<(), GatewayError> {
        let event_loop = self
            .event_loop
            .take()
            .ok_or(GatewayError::MissingEventLoop)?;
        let (network_sender, network_receiver) = mpsc::channel(self.config.channel_capacity);
        let worker_cancellation = self.cancellation.clone();
        let reconnect = self.config.reconnect;
        let worker: JoinHandle<Result<(), GatewayError>> = tokio::spawn(async move {
            poll_event_loop(event_loop, network_sender, worker_cancellation, reconnect).await
        });

        let processing_result = self.process(network_receiver).await;
        self.cancellation.cancel();
        let worker_result = worker.await.map_err(|_| GatewayError::WorkerJoin)?;
        match processing_result {
            Ok(()) | Err(GatewayError::Cancelled) => worker_result,
            Err(error) => Err(error),
        }
    }

    fn from_protocol_parts(
        config: MqttConfig,
        client: ProtocolClient,
        event_loop: ProtocolEventLoop,
        cancellation: CancellationToken,
    ) -> Result<(GatewayHandle, Self), GatewayError> {
        let topics = TopicSet::new(&config.topic_prefix)?;
        let processor = CommandProcessor::new(&config)?;
        let (publication_sender, publications) = mpsc::channel(config.channel_capacity);
        let (notices, notice_receiver) = mpsc::channel(config.channel_capacity);
        let publisher = GatewayPublisher {
            sender: publication_sender,
            origin: Arc::from(config.origin.as_str()),
            max_payload_bytes: config.max_payload_bytes,
            cancellation: cancellation.clone(),
        };
        let handle = GatewayHandle {
            publisher,
            notices: notice_receiver,
            cancellation: cancellation.clone(),
        };
        let runner = Self {
            config,
            topics,
            client,
            event_loop: Some(Box::new(event_loop)),
            publications,
            notices,
            processor,
            cancellation,
        };
        Ok((handle, runner))
    }

    async fn process(
        &mut self,
        mut network: mpsc::Receiver<NetworkNotice>,
    ) -> Result<(), GatewayError> {
        let mut publication_input_open = true;
        loop {
            tokio::select! {
                biased;
                () = self.cancellation.cancelled() => return Ok(()),
                publication = self.publications.recv(), if publication_input_open => {
                    match publication {
                        Some(publication) => self.publish_prepared(publication).await?,
                        None => publication_input_open = false,
                    }
                }
                notice = network.recv() => {
                    let Some(notice) = notice else {
                        return Err(GatewayError::NetworkWorkerStopped);
                    };
                    self.handle_network_notice(notice).await?;
                }
            }
        }
    }

    async fn handle_network_notice(&mut self, notice: NetworkNotice) -> Result<(), GatewayError> {
        match notice {
            NetworkNotice::Connected => {
                self.send_notice(GatewayNotice::BrokerState(ConnectionStatus::Connected))
                    .await?;
                if let Some(topic) = self.processor.subscription_topic() {
                    self.client
                        .subscribe(
                            topic.to_owned(),
                            self.config.qos,
                            self.config.broker_operation_timeout_ms,
                            &self.cancellation,
                        )
                        .await?;
                }
                self.publish_connection_state(ConnectionStatus::Connected)
                    .await
            }
            NetworkNotice::Disconnected => {
                self.send_notice(GatewayNotice::BrokerState(ConnectionStatus::Disconnected))
                    .await?;
                self.publish_connection_state(ConnectionStatus::Disconnected)
                    .await
            }
            NetworkNotice::CommandReady => self.send_notice(GatewayNotice::CommandReady).await,
            NetworkNotice::CommandSubscriptionRejected => {
                Err(GatewayError::CommandSubscriptionRejected)
            }
            NetworkNotice::Publish {
                topic,
                payload,
                retain,
            } => {
                if retain {
                    self.try_send_rejection(CommandError::RetainedCommand)?;
                    return Ok(());
                }
                let Ok(topic) = std::str::from_utf8(&topic) else {
                    self.try_send_rejection(CommandError::InvalidTopicEncoding)?;
                    return Ok(());
                };
                match self.processor.process(topic, &payload) {
                    Ok(command) => {
                        self.send_notice(GatewayNotice::Command(command)).await?;
                    }
                    Err(error) => self.try_send_rejection(error)?,
                }
                Ok(())
            }
        }
    }

    async fn publish_connection_state(
        &mut self,
        status: ConnectionStatus,
    ) -> Result<(), GatewayError> {
        let publication = Publication::ConnectionState(ConnectionStateData {
            component: ConnectionComponent::MqttBroker,
            status,
            reason: None,
        });
        let prepared = prepare_publication(
            &publication,
            Uuid::now_v7(),
            &self.config.origin,
            unix_timestamp_millis()?,
            self.config.max_payload_bytes,
        )?;
        self.publish_prepared(prepared).await
    }

    async fn publish_prepared(
        &mut self,
        publication: PreparedPublication,
    ) -> Result<(), GatewayError> {
        let topic = self
            .topics
            .for_event(publication.kind)
            .ok_or(GatewayError::NonPublishableEvent)?;
        self.client
            .publish(
                topic.to_owned(),
                self.config.qos,
                publication.payload,
                self.config.broker_operation_timeout_ms,
                &self.cancellation,
            )
            .await
    }

    async fn send_notice(&mut self, notice: GatewayNotice) -> Result<(), GatewayError> {
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Ok(()),
            result = self.notices.send(notice) => {
                result.map_err(|_| GatewayError::NoticeChannelClosed)
            }
        }
    }

    fn try_send_rejection(&self, error: CommandError) -> Result<(), GatewayError> {
        match self.notices.try_send(GatewayNotice::Rejected(error)) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(GatewayError::NoticeChannelClosed),
        }
    }
}

#[derive(Debug)]
struct PreparedPublication {
    kind: EventKind,
    payload: Vec<u8>,
}

fn prepare_publication(
    publication: &Publication,
    event_id: Uuid,
    origin: &str,
    timestamp: u64,
    max_payload_bytes: usize,
) -> Result<PreparedPublication, GatewayError> {
    let envelope = EventEnvelope::from_publication(event_id, origin, timestamp, publication)?;
    let payload = envelope.encode()?;
    if payload.len() > max_payload_bytes {
        return Err(GatewayError::PublicationTooLarge {
            actual: payload.len(),
            maximum: max_payload_bytes,
        });
    }
    Ok(PreparedPublication {
        kind: publication.kind(),
        payload,
    })
}

#[derive(Clone, Debug)]
enum NetworkNotice {
    Connected,
    Disconnected,
    CommandReady,
    CommandSubscriptionRejected,
    Publish {
        topic: Vec<u8>,
        payload: Vec<u8>,
        retain: bool,
    },
}

enum ProtocolClient {
    V311(AsyncClient),
    V5(rumqttc::v5::AsyncClient),
}

impl ProtocolClient {
    async fn publish(
        &self,
        topic: String,
        qos: crate::config::MqttQos,
        payload: Vec<u8>,
        timeout_ms: u64,
        cancellation: &CancellationToken,
    ) -> Result<(), GatewayError> {
        match self {
            Self::V311(client) => {
                await_broker_operation(
                    client.publish(topic, qos.as_v311(), false, payload),
                    cancellation,
                    timeout_ms,
                )
                .await
            }
            Self::V5(client) => {
                await_broker_operation(
                    client.publish(topic, qos.as_v5(), false, payload),
                    cancellation,
                    timeout_ms,
                )
                .await
            }
        }
    }

    async fn subscribe(
        &self,
        topic: String,
        qos: crate::config::MqttQos,
        timeout_ms: u64,
        cancellation: &CancellationToken,
    ) -> Result<(), GatewayError> {
        match self {
            Self::V311(client) => {
                await_broker_operation(
                    client.subscribe(topic, qos.as_v311()),
                    cancellation,
                    timeout_ms,
                )
                .await
            }
            Self::V5(client) => {
                await_broker_operation(
                    client.subscribe(topic, qos.as_v5()),
                    cancellation,
                    timeout_ms,
                )
                .await
            }
        }
    }
}

async fn await_broker_operation<F, E>(
    operation: F,
    cancellation: &CancellationToken,
    timeout_ms: u64,
) -> Result<(), GatewayError>
where
    F: Future<Output = Result<(), E>>,
    E: fmt::Display,
{
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(GatewayError::Cancelled),
        result = tokio::time::timeout(Duration::from_millis(timeout_ms), operation) => {
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(GatewayError::Client(error.to_string())),
                Err(_) => Err(GatewayError::BrokerOperationTimeout { timeout_ms }),
            }
        }
    }
}

enum ProtocolEventLoop {
    V311(Box<EventLoop>),
    V5(Box<rumqttc::v5::EventLoop>),
}

impl ProtocolEventLoop {
    async fn poll(&mut self) -> Result<Option<NetworkNotice>, ()> {
        match self {
            Self::V311(event_loop) => {
                let event = event_loop.poll().await.map_err(|_| ())?;
                Ok(classify_v311_event(event))
            }
            Self::V5(event_loop) => {
                let event = event_loop.poll().await.map_err(|_| ())?;
                Ok(classify_v5_event(event))
            }
        }
    }
}

fn classify_v311_event(event: rumqttc::Event) -> Option<NetworkNotice> {
    match event {
        rumqttc::Event::Incoming(Packet::ConnAck(_)) => Some(NetworkNotice::Connected),
        rumqttc::Event::Incoming(Packet::Publish(publication)) => Some(NetworkNotice::Publish {
            topic: publication.topic.into_bytes(),
            payload: publication.payload.to_vec(),
            retain: publication.retain,
        }),
        rumqttc::Event::Incoming(Packet::SubAck(ack)) => Some(
            if matches!(
                ack.return_codes.as_slice(),
                [rumqttc::SubscribeReasonCode::Success(_)]
            ) {
                NetworkNotice::CommandReady
            } else {
                NetworkNotice::CommandSubscriptionRejected
            },
        ),
        rumqttc::Event::Incoming(_) | rumqttc::Event::Outgoing(_) => None,
    }
}

fn classify_v5_event(event: rumqttc::v5::Event) -> Option<NetworkNotice> {
    use rumqttc::v5::mqttbytes::v5::Packet as V5Packet;

    match event {
        rumqttc::v5::Event::Incoming(V5Packet::ConnAck(_)) => Some(NetworkNotice::Connected),
        rumqttc::v5::Event::Incoming(V5Packet::Publish(publication)) => {
            Some(NetworkNotice::Publish {
                topic: publication.topic.to_vec(),
                payload: publication.payload.to_vec(),
                retain: publication.retain,
            })
        }
        rumqttc::v5::Event::Incoming(V5Packet::SubAck(ack)) => Some(
            if matches!(
                ack.return_codes.as_slice(),
                [rumqttc::v5::mqttbytes::v5::SubscribeReasonCode::Success(_)]
            ) {
                NetworkNotice::CommandReady
            } else {
                NetworkNotice::CommandSubscriptionRejected
            },
        ),
        rumqttc::v5::Event::Incoming(_) | rumqttc::v5::Event::Outgoing(_) => None,
    }
}

async fn poll_event_loop(
    mut event_loop: Box<ProtocolEventLoop>,
    sender: mpsc::Sender<NetworkNotice>,
    cancellation: CancellationToken,
    reconnect: crate::config::ReconnectConfig,
) -> Result<(), GatewayError> {
    let mut backoff = ExponentialBackoff::new(reconnect)?;
    let mut connected = false;
    loop {
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(()),
            result = event_loop.poll() => result,
        };
        match result {
            Ok(Some(NetworkNotice::Connected)) => {
                connected = true;
                backoff.reset();
                send_network_notice(&sender, NetworkNotice::Connected, &cancellation).await?;
            }
            Ok(Some(notice)) => {
                send_network_notice(&sender, notice, &cancellation).await?;
            }
            Ok(None) => {}
            Err(()) => {
                if connected {
                    connected = false;
                    send_network_notice(&sender, NetworkNotice::Disconnected, &cancellation)
                        .await?;
                }
                let delay = backoff.next_delay();
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Ok(()),
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
}

async fn send_network_notice(
    sender: &mpsc::Sender<NetworkNotice>,
    notice: NetworkNotice,
    cancellation: &CancellationToken,
) -> Result<(), GatewayError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Ok(()),
        result = sender.send(notice) => {
            result.map_err(|_| GatewayError::NetworkChannelClosed)
        }
    }
}

fn build_protocol_session(
    config: &MqttConfig,
    password: Option<&MqttPassword>,
    transport: Transport,
) -> (ProtocolClient, ProtocolEventLoop) {
    match config.protocol {
        MqttProtocol::V311 => {
            let mut options = MqttOptions::new(&config.client_id, &config.host, config.port);
            options
                .set_keep_alive(Duration::from_secs(config.keep_alive_secs))
                .set_clean_session(config.session.clean)
                .set_max_packet_size(packet_size_bound(config), packet_size_bound(config))
                .set_transport(transport);
            if let (Some(username), Some(password)) = (&config.username, password) {
                options.set_credentials(username, password.expose());
            }
            let (client, event_loop) = AsyncClient::new(options, config.channel_capacity);
            (
                ProtocolClient::V311(client),
                ProtocolEventLoop::V311(Box::new(event_loop)),
            )
        }
        MqttProtocol::V5 => {
            let mut options =
                rumqttc::v5::MqttOptions::new(&config.client_id, &config.host, config.port);
            options
                .set_keep_alive(Duration::from_secs(config.keep_alive_secs))
                .set_clean_start(config.session.clean)
                .set_session_expiry_interval(config.session.expiry_secs)
                .set_max_packet_size(Some(
                    u32::try_from(packet_size_bound(config)).unwrap_or(u32::MAX),
                ))
                .set_transport(transport);
            if let (Some(username), Some(password)) = (&config.username, password) {
                options.set_credentials(username, password.expose());
            }
            let (client, event_loop) =
                rumqttc::v5::AsyncClient::new(options, config.channel_capacity);
            (
                ProtocolClient::V5(client),
                ProtocolEventLoop::V5(Box::new(event_loop)),
            )
        }
    }
}

const fn packet_size_bound(config: &MqttConfig) -> usize {
    config.max_payload_bytes + 2048
}

async fn build_transport(config: &TlsConfig) -> Result<Transport, GatewayError> {
    if !config.enabled {
        return Ok(Transport::tcp());
    }

    let roots = load_root_certificates(config.ca_path.as_deref()).await?;
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let client_config = match (
        config.client_certificate_path.as_deref(),
        config.client_private_key_path.as_deref(),
    ) {
        (Some(certificate_path), Some(private_key_path)) => {
            let certificates = load_certificate_chain(certificate_path).await?;
            let private_key = load_private_key(private_key_path).await?;
            builder
                .with_client_auth_cert(certificates, private_key)
                .map_err(|error| GatewayError::Tls(error.to_string()))?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => return Err(GatewayError::IncompleteClientIdentity),
    };
    Ok(Transport::tls_with_config(client_config.into()))
}

async fn load_root_certificates(path: Option<&Path>) -> Result<RootCertStore, GatewayError> {
    let mut roots = RootCertStore::empty();
    if let Some(path) = path {
        for certificate in load_certificate_chain(path).await? {
            roots
                .add(certificate)
                .map_err(|error| GatewayError::Tls(error.to_string()))?;
        }
    } else {
        let native = rustls_native_certs::load_native_certs();
        for certificate in native.certs {
            roots
                .add(certificate)
                .map_err(|error| GatewayError::Tls(error.to_string()))?;
        }
    }
    if roots.is_empty() {
        return Err(GatewayError::NoTrustRoots);
    }
    Ok(roots)
}

async fn load_certificate_chain(path: &Path) -> Result<Vec<CertificateDer<'static>>, GatewayError> {
    let bytes = read_bounded_tls_file(path).await?;
    let certificates = CertificateDer::pem_slice_iter(bytes.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| GatewayError::Tls(error.to_string()))?;
    if certificates.is_empty() {
        return Err(GatewayError::Tls(
            "PEM certificate chain is empty".to_owned(),
        ));
    }
    Ok(certificates)
}

async fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, GatewayError> {
    let bytes = read_bounded_tls_file(path).await?;
    PrivateKeyDer::from_pem_slice(bytes.as_slice())
        .map_err(|error| GatewayError::Tls(error.to_string()))
}

async fn read_bounded_tls_file(path: &Path) -> Result<Zeroizing<Vec<u8>>, GatewayError> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|source| GatewayError::TlsFile {
            path: path.to_path_buf(),
            source,
        })?;
    let metadata = file
        .metadata()
        .await
        .map_err(|source| GatewayError::TlsFile {
            path: path.to_path_buf(),
            source,
        })?;
    if !metadata.is_file() {
        return Err(GatewayError::TlsFileNotRegular {
            path: path.to_path_buf(),
        });
    }

    let maximum = u64::try_from(MAX_TLS_FILE_BYTES)
        .map_err(|_| GatewayError::Tls("TLS file limit exceeds the platform range".to_owned()))?;
    if metadata.len() > maximum {
        return Err(GatewayError::TlsFileTooLarge {
            path: path.to_path_buf(),
            maximum: MAX_TLS_FILE_BYTES,
        });
    }

    let mut bytes = Zeroizing::new(Vec::new());
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)
        .await
        .map_err(|source| GatewayError::TlsFile {
            path: path.to_path_buf(),
            source,
        })?;
    if bytes.len() > MAX_TLS_FILE_BYTES {
        return Err(GatewayError::TlsFileTooLarge {
            path: path.to_path_buf(),
            maximum: MAX_TLS_FILE_BYTES,
        });
    }
    Ok(bytes)
}

fn unix_timestamp_millis() -> Result<u64, GatewayError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GatewayError::ClockBeforeUnixEpoch)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| GatewayError::TimestampOverflow)
}

/// MQTT gateway construction or runtime error.
#[derive(Debug, Error)]
pub enum GatewayError {
    /// Invalid gateway configuration.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// Command processor construction failed.
    #[error(transparent)]
    Command(#[from] CommandError),
    /// Reconnect backoff construction failed.
    #[error(transparent)]
    Backoff(#[from] BackoffError),
    /// Event schema validation or serialization failed.
    #[error(transparent)]
    Schema(#[from] SchemaError),
    /// TLS file access failed. File contents are never included.
    #[error("failed to read TLS file `{path}`: {source}")]
    TlsFile {
        /// Configured file path.
        path: std::path::PathBuf,
        /// Filesystem failure.
        #[source]
        source: std::io::Error,
    },
    /// A configured TLS path changed to a non-file after validation.
    #[error("TLS path `{path}` is not a regular file")]
    TlsFileNotRegular {
        /// Configured file path.
        path: std::path::PathBuf,
    },
    /// A TLS file exceeded the hard read bound.
    #[error("TLS file `{path}` exceeds the {maximum}-byte limit")]
    TlsFileTooLarge {
        /// Configured file path.
        path: std::path::PathBuf,
        /// Hard maximum in bytes.
        maximum: usize,
    },
    /// TLS certificate or key parsing failed.
    #[error("failed to configure MQTT TLS: {0}")]
    Tls(String),
    /// TLS validation cannot operate without any roots.
    #[error("no usable TLS trust roots were found")]
    NoTrustRoots,
    /// Mutual TLS paths must remain paired after validation.
    #[error("MQTT client certificate and private key must be provided together")]
    IncompleteClientIdentity,
    /// The supplied concrete MQTT parts did not match the configured protocol.
    #[error("provided rumqttc parts do not match the configured MQTT protocol")]
    ProtocolPartsMismatch,
    /// A publication exceeded the application byte bound.
    #[error("MQTT publication is {actual} bytes; maximum is {maximum}")]
    PublicationTooLarge {
        /// Encoded JSON size.
        actual: usize,
        /// Configured maximum.
        maximum: usize,
    },
    /// Only incoming application event kinds can be published.
    #[error("MQTT event kind is not publishable")]
    NonPublishableEvent,
    /// The publishing side was canceled before queueing.
    #[error("MQTT gateway was cancelled")]
    Cancelled,
    /// The runner no longer receives application publications.
    #[error("MQTT publication channel is closed")]
    PublicationChannelClosed,
    /// The application stopped receiving gateway notices.
    #[error("MQTT gateway notice channel is closed")]
    NoticeChannelClosed,
    /// The network worker's bounded channel closed unexpectedly.
    #[error("MQTT network channel is closed")]
    NetworkChannelClosed,
    /// The network worker stopped without cancellation.
    #[error("MQTT network worker stopped unexpectedly")]
    NetworkWorkerStopped,
    /// The broker refused the exact allowlisted command subscription.
    #[error("MQTT broker rejected the outbound-command subscription")]
    CommandSubscriptionRejected,
    /// A runner invariant lost its owned event loop.
    #[error("MQTT runner has no event loop")]
    MissingEventLoop,
    /// A `rumqttc` request could not be queued.
    #[error("MQTT client request failed: {0}")]
    Client(String),
    /// A bounded broker operation did not enter the `rumqttc` queue in time.
    #[error("MQTT broker operation timed out after {timeout_ms} ms")]
    BrokerOperationTimeout {
        /// Configured operation timeout.
        timeout_ms: u64,
    },
    /// The network task failed to join. Panics are reported, never propagated.
    #[error("MQTT network worker failed")]
    WorkerJoin,
    /// The system clock predates the Unix epoch.
    #[error("system clock is before the Unix epoch")]
    ClockBeforeUnixEpoch,
    /// The current Unix timestamp does not fit the v1 representation.
    #[error("system timestamp exceeds the MQTT v1 range")]
    TimestampOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rumqttc::v5::mqttbytes::v5::{
        Packet as V5Packet, Publish as V5Publish, SubAck as V5SubAck,
        SubscribeReasonCode as V5SubscribeReasonCode,
    };

    fn offline_parts(
        mut config: MqttConfig,
    ) -> (MqttConfig, AsyncClient, EventLoop, CancellationToken) {
        config.tls.enabled = false;
        config.port = 9;
        let mut options = MqttOptions::new(&config.client_id, &config.host, config.port);
        options.set_keep_alive(Duration::from_secs(config.keep_alive_secs));
        let (client, event_loop) = AsyncClient::new(options, config.channel_capacity);
        (config, client, event_loop, CancellationToken::new())
    }

    #[tokio::test]
    async fn publisher_rejects_oversized_json_before_queueing() {
        let (config, client, event_loop, cancellation) = offline_parts(MqttConfig {
            max_payload_bytes: 128,
            command_limits: crate::config::CommandLimits {
                max_text_bytes: 64,
                ..crate::config::CommandLimits::default()
            },
            ..MqttConfig::default()
        });
        let (handle, _runner) =
            GatewayRunner::from_v311_parts(config, client, event_loop, cancellation)
                .expect("valid offline runner");
        let publication = Publication::Telemetry(crate::schema::TelemetryData::RawCayenneLpp {
            source_pubkey_prefix: "001122334455".to_owned(),
            payload: "aa".repeat(256),
        });
        assert!(matches!(
            handle.publish(publication).await,
            Err(GatewayError::PublicationTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn cancellation_stops_an_offline_runner_without_a_panic() {
        let (config, client, event_loop, cancellation) = offline_parts(MqttConfig::default());
        let (handle, runner) =
            GatewayRunner::from_v311_parts(config, client, event_loop, cancellation)
                .expect("valid offline runner");
        let task = tokio::spawn(runner.run());
        handle.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("runner should stop promptly")
            .expect("runner task should join");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn broker_operation_wait_is_cancellation_and_timeout_bounded() {
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let cancellation_result = await_broker_operation(
            std::future::pending::<Result<(), std::io::Error>>(),
            &cancelled,
            60_000,
        )
        .await;
        assert!(matches!(cancellation_result, Err(GatewayError::Cancelled)));

        let timeout_result = await_broker_operation(
            std::future::pending::<Result<(), std::io::Error>>(),
            &CancellationToken::new(),
            1,
        )
        .await;
        assert!(matches!(
            timeout_result,
            Err(GatewayError::BrokerOperationTimeout { timeout_ms: 1 })
        ));
    }

    #[tokio::test]
    async fn runtime_tls_reader_rejects_an_oversized_file() {
        let path = std::env::temp_dir().join(format!(
            "meshquill-mqtt-runtime-oversized-{}.pem",
            Uuid::now_v7()
        ));
        let file = std::fs::File::create(&path).expect("create sparse TLS fixture");
        file.set_len(u64::try_from(MAX_TLS_FILE_BYTES + 1).expect("limit fits u64"))
            .expect("size sparse TLS fixture");
        drop(file);

        let result = read_bounded_tls_file(&path).await;
        std::fs::remove_file(path).expect("remove sparse TLS fixture");
        assert!(matches!(result, Err(GatewayError::TlsFileTooLarge { .. })));
    }

    #[test]
    fn protocol_part_mismatch_is_rejected() {
        let (mut config, client, event_loop, cancellation) = offline_parts(MqttConfig::default());
        config.protocol = MqttProtocol::V5;
        assert!(matches!(
            GatewayRunner::from_v311_parts(config, client, event_loop, cancellation),
            Err(GatewayError::ProtocolPartsMismatch)
        ));
    }

    #[test]
    fn mqtt_v5_parts_build_the_same_bounded_runner_api() {
        let config = MqttConfig {
            protocol: MqttProtocol::V5,
            tls: TlsConfig {
                enabled: false,
                ..TlsConfig::default()
            },
            ..MqttConfig::default()
        };
        let options = rumqttc::v5::MqttOptions::new(&config.client_id, &config.host, config.port);
        let (client, event_loop) = rumqttc::v5::AsyncClient::new(options, config.channel_capacity);
        let result =
            GatewayRunner::from_v5_parts(config, client, event_loop, CancellationToken::new());
        assert!(result.is_ok());
    }

    #[test]
    fn protocol_classifiers_preserve_retain_and_subscription_outcomes() {
        let mut publication = rumqttc::Publish::new(
            "meshquill/v1/out/send",
            rumqttc::QoS::AtLeastOnce,
            b"{}".to_vec(),
        );
        publication.retain = true;
        assert!(matches!(
            classify_v311_event(rumqttc::Event::Incoming(Packet::Publish(publication))),
            Some(NetworkNotice::Publish { retain: true, .. })
        ));
        let accepted = rumqttc::SubAck::new(
            1,
            vec![rumqttc::SubscribeReasonCode::Success(
                rumqttc::QoS::AtLeastOnce,
            )],
        );
        assert!(matches!(
            classify_v311_event(rumqttc::Event::Incoming(Packet::SubAck(accepted))),
            Some(NetworkNotice::CommandReady)
        ));
        let rejected = rumqttc::SubAck::new(1, vec![rumqttc::SubscribeReasonCode::Failure]);
        assert!(matches!(
            classify_v311_event(rumqttc::Event::Incoming(Packet::SubAck(rejected))),
            Some(NetworkNotice::CommandSubscriptionRejected)
        ));

        let mut publication = V5Publish::new(
            "meshquill/v1/out/send",
            rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
            b"{}".to_vec(),
            None,
        );
        publication.retain = true;
        assert!(matches!(
            classify_v5_event(rumqttc::v5::Event::Incoming(V5Packet::Publish(publication))),
            Some(NetworkNotice::Publish { retain: true, .. })
        ));
        let accepted = V5SubAck {
            pkid: 1,
            return_codes: vec![V5SubscribeReasonCode::Success(
                rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
            )],
            properties: None,
        };
        assert!(matches!(
            classify_v5_event(rumqttc::v5::Event::Incoming(V5Packet::SubAck(accepted))),
            Some(NetworkNotice::CommandReady)
        ));
        let rejected = V5SubAck {
            pkid: 1,
            return_codes: vec![V5SubscribeReasonCode::NotAuthorized],
            properties: None,
        };
        assert!(matches!(
            classify_v5_event(rumqttc::v5::Event::Incoming(V5Packet::SubAck(rejected))),
            Some(NetworkNotice::CommandSubscriptionRejected)
        ));
    }

    #[tokio::test]
    async fn retained_command_is_rejected_before_dedupe_and_fresh_copy_is_accepted() {
        let config = MqttConfig {
            allow_send: true,
            tls: TlsConfig {
                enabled: false,
                ..TlsConfig::default()
            },
            ..MqttConfig::default()
        };
        let topic = TopicSet::new(&config.topic_prefix)
            .expect("topic set")
            .outbound_send()
            .to_owned();
        let payload = serde_json::to_vec(&serde_json::json!({
            "schema": crate::topics::SCHEMA_VERSION,
            "event_id": Uuid::now_v7(),
            "origin": "remote-test",
            "timestamp": 1_725_000_000_000_u64,
            "type": "send_direct",
            "data": {"destination": "alice", "text": "hello"}
        }))
        .expect("command JSON");
        let (config, client, event_loop, cancellation) = offline_parts(config);
        let (mut handle, mut runner) =
            GatewayRunner::from_v311_parts(config, client, event_loop, cancellation)
                .expect("offline runner");

        runner
            .handle_network_notice(NetworkNotice::Publish {
                topic: topic.as_bytes().to_vec(),
                payload: payload.clone(),
                retain: true,
            })
            .await
            .expect("retained rejection");
        assert!(matches!(
            handle.recv_notice().await,
            Some(GatewayNotice::Rejected(CommandError::RetainedCommand))
        ));

        runner
            .handle_network_notice(NetworkNotice::Publish {
                topic: topic.into_bytes(),
                payload,
                retain: false,
            })
            .await
            .expect("fresh command");
        assert!(matches!(
            handle.recv_notice().await,
            Some(GatewayNotice::Command(_))
        ));

        assert!(matches!(
            runner
                .handle_network_notice(NetworkNotice::CommandSubscriptionRejected)
                .await,
            Err(GatewayError::CommandSubscriptionRejected)
        ));
    }
}
