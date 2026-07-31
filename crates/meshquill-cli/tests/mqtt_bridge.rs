//! Ignored real-broker MQTT bridge integration test.

use std::{
    error::Error,
    fs, io,
    process::{Command, Output, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use meshquill_mqtt::{EventEnvelope, EventKind, SCHEMA_VERSION, TopicSet};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use tempfile::TempDir;
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
                "set MESHQUILL_MQTT_TEST_HOST to run this ignored Mosquitto integration test",
            )
        })?;
        let port = std::env::var("MESHQUILL_MQTT_TEST_PORT")
            .ok()
            .or_else(|| std::env::var("PORT").ok())
            .unwrap_or_else(|| "1883".to_owned())
            .parse()?;
        Ok(Self { host, port })
    }
}

struct BridgeProcess {
    child: std::process::Child,
}

impl Drop for BridgeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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
            Event::Incoming(Packet::SubAck(ack))
                if matches!(
                    ack.return_codes.as_slice(),
                    [rumqttc::SubscribeReasonCode::Success(_)]
                ) =>
            {
                Some(ObserverNotice::Subscribed)
            }
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
        .ok_or_else(|| io::Error::other("MQTT observer stopped before the expected packet"))
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

struct StartupPublications {
    connection: EventEnvelope,
    contacts: EventEnvelope,
    battery: EventEnvelope,
    raw_telemetry: EventEnvelope,
}

async fn wait_for_startup_publications(
    receiver: &mut mpsc::Receiver<ObserverNotice>,
    topics: &TopicSet,
) -> TestResult<StartupPublications> {
    let mut connection = None;
    let mut contacts = None;
    let mut battery = None;
    let mut raw_telemetry = None;

    // The gateway may accept application snapshot publications before its network worker reports
    // CONNACK. Startup ordering is therefore deliberately unspecified; retain every required
    // publication instead of discarding snapshots while waiting for connection state first.
    while connection.is_none() || contacts.is_none() || battery.is_none() || raw_telemetry.is_none()
    {
        let ObserverNotice::Publish {
            topic: actual,
            payload,
        } = receive_observer(receiver).await?
        else {
            continue;
        };
        let envelope = EventEnvelope::decode(&payload)?;
        if actual == topics.connection_state() && envelope.kind == EventKind::ConnectionState {
            connection = Some(envelope);
        } else if actual == topics.contacts() && envelope.kind == EventKind::Contacts {
            contacts = Some(envelope);
        } else if actual == topics.telemetry() && envelope.kind == EventKind::Telemetry {
            match envelope.data["kind"].as_str() {
                Some("battery") => battery = Some(envelope),
                Some("raw_cayenne_lpp") => raw_telemetry = Some(envelope),
                _ => {}
            }
        }
    }

    Ok(StartupPublications {
        connection: connection
            .ok_or_else(|| io::Error::other("startup connection publication missing"))?,
        contacts: contacts
            .ok_or_else(|| io::Error::other("startup contacts publication missing"))?,
        battery: battery.ok_or_else(|| io::Error::other("startup battery publication missing"))?,
        raw_telemetry: raw_telemetry
            .ok_or_else(|| io::Error::other("startup raw telemetry publication missing"))?,
    })
}

fn invoke(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_meshquill"))
        .args(arguments)
        .output()
        .unwrap_or_else(|error| panic!("failed to run meshquill: {error}"))
}

fn init_demo(config: &str) {
    let output = invoke(&[
        "--config",
        config,
        "--non-interactive",
        "init",
        "--name",
        "demo",
        "--demo",
        "--set-default",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));
}

fn config_path(directory: &TempDir) -> String {
    directory.path().join("config.toml").display().to_string()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn required_env(name: &str) -> TestResult<String> {
    std::env::var(name)
        .map_err(|_| io::Error::other(format!("set {name} to run this integration test")).into())
}

fn start_bridge(config: &str) -> TestResult<BridgeProcess> {
    let child = Command::new(env!("CARGO_BIN_EXE_meshquill"))
        .args([
            "--config",
            config,
            "--non-interactive",
            "--output",
            "jsonl",
            "mqtt",
            "bridge",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| {
            io::Error::other(format!("failed to spawn meshquill mqtt bridge: {error}"))
        })?;
    Ok(BridgeProcess { child })
}

#[tokio::test]
#[ignore = "requires a reachable real MQTT broker configured through MESHQUILL_MQTT_TEST_HOST"]
async fn mqtt_bridge_with_real_broker_roundtrip_send_direct_to_demo_destination() -> TestResult {
    let settings = BrokerTestSettings::from_env()?;

    let topic_prefix = format!("meshquill-test/{}", Uuid::now_v7());
    let topics = TopicSet::new(&topic_prefix).expect("validated prefix");

    let directory = TempDir::new().expect("temporary directory");
    let config = config_path(&directory);

    init_demo(&config);
    let output = invoke(&[
        "--config",
        &config,
        "--non-interactive",
        "mqtt",
        "configure",
        "--host",
        &settings.host,
        "--port",
        &settings.port.to_string(),
        "--no-tls",
        "--topic-prefix",
        &topic_prefix,
        "--allow-send",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", stderr(&output));

    let observer_client_id = format!("meshquill-observer-{}", Uuid::now_v7());
    let observer_options = MqttOptions::new(observer_client_id, &settings.host, settings.port);
    let (observer_client, observer_event_loop) = AsyncClient::new(observer_options, 16);
    observer_client
        .subscribe(
            format!("{topic_prefix}/{SCHEMA_VERSION}/#"),
            QoS::AtLeastOnce,
        )
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

    let _bridge = start_bridge(&config)?;
    let startup = wait_for_startup_publications(&mut observer_receiver, &topics).await?;
    assert_eq!(startup.connection.data["status"], "connected");
    assert!(startup.contacts.data["lastmod"].is_u64());
    assert!(startup.contacts.data["contacts"].is_array());
    assert_eq!(startup.battery.data["kind"], "battery");
    assert_eq!(startup.raw_telemetry.data["kind"], "raw_cayenne_lpp");
    assert!(startup.raw_telemetry.data["source_pubkey_prefix"].is_string());
    assert!(startup.raw_telemetry.data["payload"].is_string());

    let command_id = Uuid::now_v7();
    let command = EventEnvelope::new(
        command_id,
        format!("int-test-{command_id}"),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .try_into()?,
        EventKind::SendDirect,
        serde_json::json!({
            "destination": "Alice",
            "text": "integration test",
        }),
    )?;
    observer_client
        .publish(
            topics.outbound_send(),
            QoS::AtLeastOnce,
            false,
            command.encode()?,
        )
        .await?;

    let ack = wait_for_event(&mut observer_receiver, topics.ack(), EventKind::Ack).await?;
    assert!(ack.data.get("code").is_some());

    observer_cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(10), observer_task).await??;
    Ok(())
}

#[test]
#[ignore = "requires the secured broker configured through MESHQUILL_MQTT_SECURE_TEST_*"]
fn persisted_cli_tls_mtls_and_environment_secret_connect_in_a_fresh_process() -> TestResult {
    let host = required_env("MESHQUILL_MQTT_SECURE_TEST_HOST")?;
    let port = required_env("MESHQUILL_MQTT_SECURE_TEST_PORT")?;
    let ca = required_env("MESHQUILL_MQTT_SECURE_TEST_CA")?;
    let client_certificate = required_env("MESHQUILL_MQTT_SECURE_TEST_CLIENT_CERT")?;
    let client_key = required_env("MESHQUILL_MQTT_SECURE_TEST_CLIENT_KEY")?;
    let username = required_env("MESHQUILL_MQTT_SECURE_TEST_USERNAME")?;
    let password = required_env("MESHQUILL_MQTT_SECURE_TEST_PASSWORD")?;

    let directory = TempDir::new()?;
    let config = config_path(&directory);
    init_demo(&config);

    let configured = invoke(&[
        "--config",
        &config,
        "--non-interactive",
        "--output",
        "json",
        "mqtt",
        "configure",
        "--host",
        &host,
        "--port",
        &port,
        "--ca-file",
        &ca,
        "--client-certificate",
        &client_certificate,
        "--client-key",
        &client_key,
        "--username",
        &username,
        "--password-env",
        "MESHQUILL_MQTT_SECURE_TEST_PASSWORD",
    ]);
    assert_eq!(configured.status.code(), Some(0), "{}", stderr(&configured));

    let persisted = fs::read_to_string(&config)?;
    assert!(persisted.contains("MESHQUILL_MQTT_SECURE_TEST_PASSWORD"));
    assert!(persisted.contains("environment"));
    assert!(!persisted.contains(&password));

    let tested = invoke(&[
        "--config",
        &config,
        "--non-interactive",
        "--output",
        "json",
        "mqtt",
        "test",
    ]);
    assert_eq!(tested.status.code(), Some(0), "{}", stderr(&tested));
    let report: serde_json::Value = serde_json::from_slice(&tested.stdout)?;
    assert_eq!(report["type"], "mqtt_test");
    assert_eq!(report["data"]["connected"], true);
    assert_eq!(report["data"]["tls"], true);
    assert_eq!(report["data"]["authenticated"], true);

    for exposed in [
        configured.stdout.as_slice(),
        configured.stderr.as_slice(),
        tested.stdout.as_slice(),
        tested.stderr.as_slice(),
    ] {
        assert!(!String::from_utf8_lossy(exposed).contains(&password));
    }
    Ok(())
}
