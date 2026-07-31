//! Ignored real-broker round-trip coverage for the MQTT gateway.

use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use meshquill_mqtt::{
    AcceptedCommand, ConnectionStatus, EventEnvelope, EventKind, GatewayHandle, GatewayNotice,
    GatewayRunner, MqttConfig, MqttPassword, MqttProtocol, Publication, SCHEMA_VERSION,
    SendCommand, TelemetryData, TlsConfig, TopicSet,
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

struct BrokerTestSettings {
    host: String,
    port: u16,
}

impl BrokerTestSettings {
    fn from_env() -> TestResult<Self> {
        let host = std::env::var("MESHQUILL_MQTT_TEST_HOST").map_err(|_| {
            io::Error::other(
                "set MESHQUILL_MQTT_TEST_HOST to run the ignored Mosquitto integration test",
            )
        })?;
        let port = std::env::var("MESHQUILL_MQTT_TEST_PORT")
            .unwrap_or_else(|_| "1883".to_owned())
            .parse()?;
        Ok(Self { host, port })
    }
}

struct SecuredBrokerTestSettings {
    broker: BrokerTestSettings,
    wrong_host: String,
    ca_path: PathBuf,
    wrong_ca_path: PathBuf,
    client_certificate_path: PathBuf,
    client_private_key_path: PathBuf,
    username: String,
    password: String,
}

impl SecuredBrokerTestSettings {
    fn from_env() -> TestResult<Self> {
        Ok(Self {
            broker: BrokerTestSettings {
                host: required_env("MESHQUILL_MQTT_SECURE_TEST_HOST")?,
                port: required_env("MESHQUILL_MQTT_SECURE_TEST_PORT")?.parse()?,
            },
            wrong_host: required_env("MESHQUILL_MQTT_SECURE_TEST_WRONG_HOST")?,
            ca_path: required_env("MESHQUILL_MQTT_SECURE_TEST_CA")?.into(),
            wrong_ca_path: required_env("MESHQUILL_MQTT_SECURE_TEST_WRONG_CA")?.into(),
            client_certificate_path: required_env("MESHQUILL_MQTT_SECURE_TEST_CLIENT_CERT")?.into(),
            client_private_key_path: required_env("MESHQUILL_MQTT_SECURE_TEST_CLIENT_KEY")?.into(),
            username: required_env("MESHQUILL_MQTT_SECURE_TEST_USERNAME")?,
            password: required_env("MESHQUILL_MQTT_SECURE_TEST_PASSWORD")?,
        })
    }
}

fn required_env(name: &'static str) -> TestResult<String> {
    std::env::var(name)
        .map_err(|_| io::Error::other(format!("set {name} to run this Mosquitto test")).into())
}

enum ObserverNotice {
    Subscribed,
    Publish { topic: String, payload: Vec<u8> },
}

async fn observer_loop(
    mut event_loop: rumqttc::EventLoop,
    sender: mpsc::Sender<ObserverNotice>,
    cancellation: CancellationToken,
) {
    loop {
        let event = tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            result = event_loop.poll() => match result {
                Ok(event) => event,
                Err(_) => return,
            }
        };
        let notice = match event {
            Event::Incoming(Packet::SubAck(_)) => Some(ObserverNotice::Subscribed),
            Event::Incoming(Packet::Publish(publication)) => Some(ObserverNotice::Publish {
                topic: publication.topic,
                payload: publication.payload.to_vec(),
            }),
            Event::Incoming(_) | Event::Outgoing(_) => None,
        };
        if let Some(notice) = notice
            && sender.send(notice).await.is_err()
        {
            return;
        }
    }
}

async fn receive_observer(
    receiver: &mut mpsc::Receiver<ObserverNotice>,
) -> TestResult<ObserverNotice> {
    tokio::time::timeout(Duration::from_secs(10), receiver.recv())
        .await?
        .ok_or_else(|| io::Error::other("Mosquitto observer stopped before the expected packet"))
        .map_err(Into::into)
}

async fn wait_for_event(
    receiver: &mut mpsc::Receiver<ObserverNotice>,
    topic: &str,
    kind: EventKind,
) -> TestResult<EventEnvelope> {
    loop {
        if let ObserverNotice::Publish {
            topic: actual,
            payload,
        } = receive_observer(receiver).await?
            && actual == topic
        {
            let envelope = EventEnvelope::decode(&payload)?;
            if envelope.kind == kind {
                return Ok(envelope);
            }
        }
    }
}

async fn wait_for_gateway_command(handle: &mut GatewayHandle) -> TestResult<AcceptedCommand> {
    loop {
        let notice = tokio::time::timeout(Duration::from_secs(10), handle.recv_notice())
            .await?
            .ok_or_else(|| io::Error::other("gateway stopped before accepting the command"))?;
        match notice {
            GatewayNotice::Command(command) => return Ok(command),
            GatewayNotice::BrokerState(_) => {}
            GatewayNotice::Rejected(error) => {
                return Err(io::Error::other(format!(
                    "gateway rejected the integration command: {error}"
                ))
                .into());
            }
        }
    }
}

async fn run_roundtrip(protocol: MqttProtocol) -> TestResult {
    let broker = BrokerTestSettings::from_env()?;
    let namespace = format!("meshquill-test/{}", Uuid::now_v7());
    let topics = TopicSet::new(&namespace)?;
    let observer_id = format!("meshquill-observer-{}", Uuid::now_v7());
    let observer_options = MqttOptions::new(observer_id, &broker.host, broker.port);
    let (observer, observer_event_loop) = AsyncClient::new(observer_options, 16);
    let observer_filter = format!("{namespace}/{SCHEMA_VERSION}/#");
    observer
        .subscribe(observer_filter, QoS::AtLeastOnce)
        .await?;
    let observer_cancellation = CancellationToken::new();
    let (observer_sender, mut observer_receiver) = mpsc::channel(32);
    let observer_task = tokio::spawn(observer_loop(
        observer_event_loop,
        observer_sender,
        observer_cancellation.clone(),
    ));
    while !matches!(
        receive_observer(&mut observer_receiver).await?,
        ObserverNotice::Subscribed
    ) {}

    let gateway_cancellation = CancellationToken::new();
    let config = MqttConfig {
        host: broker.host,
        port: broker.port,
        client_id: format!("meshquill-gateway-{}", Uuid::now_v7()),
        protocol,
        tls: TlsConfig {
            enabled: false,
            ..TlsConfig::default()
        },
        topic_prefix: namespace,
        origin: format!("gateway-{}", Uuid::now_v7()),
        allow_send: true,
        ..MqttConfig::default()
    };
    let (mut handle, runner) =
        GatewayRunner::connect(config, None, gateway_cancellation.clone()).await?;
    let runner_task = tokio::spawn(runner.run());
    wait_for_event(
        &mut observer_receiver,
        topics.connection_state(),
        EventKind::ConnectionState,
    )
    .await?;

    let command_id = Uuid::now_v7();
    let command = EventEnvelope::new(
        command_id,
        "mosquitto-test-sender",
        1_725_000_000_000,
        EventKind::SendDirect,
        serde_json::json!({"destination": "alice", "text": "broker roundtrip"}),
    )?;
    observer
        .publish(
            topics.outbound_send(),
            QoS::AtLeastOnce,
            false,
            command.encode()?,
        )
        .await?;
    let accepted = wait_for_gateway_command(&mut handle).await?;
    assert!(matches!(
        accepted,
        AcceptedCommand {
            event_id,
            command: SendCommand::Direct { .. },
            ..
        } if event_id == command_id
    ));

    let telemetry_id = handle
        .publisher()
        .publish(Publication::Telemetry(TelemetryData {
            source: Some("integration-test".to_owned()),
            values: [("battery".to_owned(), serde_json::json!(88))]
                .into_iter()
                .collect(),
        }))
        .await?;
    let telemetry = wait_for_event(
        &mut observer_receiver,
        topics.telemetry(),
        EventKind::Telemetry,
    )
    .await?;
    assert_eq!(telemetry.event_id, telemetry_id);

    handle.cancel();
    tokio::time::timeout(Duration::from_secs(10), runner_task).await???;
    observer_cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(10), observer_task).await??;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real Mosquitto broker configured through MESHQUILL_MQTT_TEST_HOST"]
async fn mosquitto_v311_roundtrip_uses_real_subscription_and_publication() -> TestResult {
    run_roundtrip(MqttProtocol::V311).await
}

#[tokio::test]
#[ignore = "requires a real Mosquitto broker configured through MESHQUILL_MQTT_TEST_HOST"]
async fn mosquitto_v5_roundtrip_uses_real_subscription_and_publication() -> TestResult {
    run_roundtrip(MqttProtocol::V5).await
}

fn secure_config(settings: &SecuredBrokerTestSettings, host: String) -> MqttConfig {
    MqttConfig {
        host,
        port: settings.broker.port,
        client_id: format!("meshquill-secure-test-{}", Uuid::now_v7()),
        tls: TlsConfig {
            enabled: true,
            verify_server_certificate: true,
            ca_path: Some(settings.ca_path.clone()),
            client_certificate_path: Some(settings.client_certificate_path.clone()),
            client_private_key_path: Some(settings.client_private_key_path.clone()),
        },
        username: Some(settings.username.clone()),
        topic_prefix: format!("meshquill-secure-test/{}", Uuid::now_v7()),
        origin: format!("secure-gateway-{}", Uuid::now_v7()),
        ..MqttConfig::default()
    }
}

async fn gateway_connects_within(
    config: MqttConfig,
    password: MqttPassword,
    limit: Duration,
) -> TestResult<bool> {
    let cancellation = CancellationToken::new();
    let (mut handle, runner) =
        GatewayRunner::connect(config, Some(password), cancellation.clone()).await?;
    let runner_task = tokio::spawn(runner.run());
    let connected = tokio::time::timeout(limit, async {
        while let Some(notice) = handle.recv_notice().await {
            if matches!(
                notice,
                GatewayNotice::BrokerState(ConnectionStatus::Connected)
            ) {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    handle.cancel();
    tokio::time::timeout(Duration::from_secs(10), runner_task).await???;
    Ok(connected)
}

#[tokio::test]
#[ignore = "requires the secured broker configured through MESHQUILL_MQTT_SECURE_TEST_*"]
async fn mosquitto_tls_auth_and_mtls_enforce_all_peer_checks() -> TestResult {
    let settings = SecuredBrokerTestSettings::from_env()?;
    let connection_limit = Duration::from_secs(3);

    assert!(
        gateway_connects_within(
            secure_config(&settings, settings.broker.host.clone()),
            MqttPassword::new(settings.password.clone())?,
            connection_limit,
        )
        .await?,
        "the valid CA, server name, username/password, and client identity must connect"
    );

    let mut wrong_trust = secure_config(&settings, settings.broker.host.clone());
    wrong_trust.tls.ca_path = Some(settings.wrong_ca_path.clone());
    assert!(
        !gateway_connects_within(
            wrong_trust,
            MqttPassword::new(settings.password.clone())?,
            connection_limit,
        )
        .await?,
        "a client trusting a different CA must not connect"
    );

    assert!(
        !gateway_connects_within(
            secure_config(&settings, settings.broker.host.clone()),
            MqttPassword::new("definitely-the-wrong-password")?,
            connection_limit,
        )
        .await?,
        "invalid broker credentials must not connect"
    );

    let mut missing_client_identity = secure_config(&settings, settings.broker.host.clone());
    missing_client_identity.tls.client_certificate_path = None;
    missing_client_identity.tls.client_private_key_path = None;
    assert!(
        !gateway_connects_within(
            missing_client_identity,
            MqttPassword::new(settings.password.clone())?,
            connection_limit,
        )
        .await?,
        "a broker requiring mTLS must reject a missing client identity"
    );

    assert!(
        !gateway_connects_within(
            secure_config(&settings, settings.wrong_host.clone()),
            MqttPassword::new(settings.password)?,
            connection_limit,
        )
        .await?,
        "the server certificate must not validate for a different host name"
    );
    Ok(())
}
