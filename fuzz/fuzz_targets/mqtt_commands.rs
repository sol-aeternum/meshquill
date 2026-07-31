#![no_main]

use libfuzzer_sys::fuzz_target;
use meshquill_mqtt::{CommandProcessor, MqttConfig, SessionConfig, TlsConfig};

fuzz_target!(|payload: &[u8]| {
    let config = MqttConfig {
        tls: TlsConfig {
            enabled: false,
            ..TlsConfig::default()
        },
        session: SessionConfig {
            clean: true,
            expiry_secs: None,
        },
        allow_send: true,
        ..MqttConfig::default()
    };
    let mut processor = CommandProcessor::new(&config).expect("fuzz configuration must be valid");
    let topic = processor
        .subscription_topic()
        .expect("send-enabled processor must expose its exact topic")
        .to_owned();

    let _ = processor.process(&topic, payload);
});
