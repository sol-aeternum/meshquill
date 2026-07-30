# Optional MQTT application gateway

MeshCore does not currently define a normative companion MQTT protocol. Meshquill therefore treats
MQTT as an optional application gateway attached to one foreground client, never as a radio
transport. A broker introduces a LAN or internet dependency; BLE, serial and TCP device use do not
require one.

## Configure and test

TLS with system certificate validation is the default:

```console
printf '%s\n' "$MQTT_PASSWORD" | meshquill mqtt configure \
  --host broker.example.net --port 8883 --protocol 5 --qos 1 \
  --username mesh-user --password-stdin --topic-prefix field/device-1
meshquill mqtt test
meshquill mqtt status
```

Use `--ca-file` for a private CA, and both `--client-certificate` and `--client-key` for mutual TLS.
Certificate verification cannot be disabled while TLS is on. `--no-tls` is an explicit plain-TCP
choice intended for a secured local test broker. Passwords are read from a terminal or stdin and
stored through the OS credential store; they are not accepted in argv or emitted by `config show`.

For a disposable local Mosquitto broker:

```console
meshquill --profile demo mqtt configure \
  --host 127.0.0.1 --port 1883 --no-tls \
  --topic-prefix meshquill-demo --allow-send
meshquill --profile demo mqtt test
meshquill --profile demo --output jsonl mqtt bridge
```

The bridge is a foreground streaming command and requires `--output jsonl` for machine output.
Ctrl-C stops both the MeshCore client and broker session. Broker reconnect uses bounded exponential
backoff. The application never retransmits a radio message merely because the device reconnects.

## Safe outbound commands

Outbound broker commands are disabled by default. `--allow-send` subscribes to one exact topic and
allows only direct and channel text sends. It does not allow login, device settings, remote CLI,
contact deletion, reboot or arbitrary administration.

Example direct send (replace the UUID, timestamp and origin for every command):

```console
mosquitto_pub -h 127.0.0.1 -p 1883 -q 1 \
  -t 'meshquill-demo/meshquill.mqtt/v1/outbound/send' \
  -m '{"schema":"meshquill.mqtt/v1","event_id":"018f8f4c-9e5d-7d62-8bb8-94d547f2979b","origin":"operator-console","timestamp":1785384000000,"type":"send_direct","data":{"destination":"Alice","text":"hello"}}'
```

For a channel, use `"type":"send_channel"` and
`"data":{"channel":0,"text":"hello"}`. The gateway rejects its own origin, nil or duplicate
event IDs, unknown fields, malformed/oversized payloads, invalid destinations, channels above 7,
and any non-allowlisted type. The default duplicate cache retains 4096 IDs for 15 minutes.

## Topics and payloads

See [the v1 schema reference](reference/mqtt-schema-v1.md). Under a configured prefix `P`, the
bridge publishes:

- `P/meshquill.mqtt/v1/events/incoming_message`
- `P/meshquill.mqtt/v1/events/ack`
- `P/meshquill.mqtt/v1/events/connection_state`
- `P/meshquill.mqtt/v1/events/contacts`
- `P/meshquill.mqtt/v1/events/telemetry`

It subscribes only to `P/meshquill.mqtt/v1/outbound/send`, and only after outbound sends are
explicitly enabled. MQTT QoS improves broker delivery semantics; it is not a MeshCore radio ACK.

## Validation evidence

Unit tests cover TLS configuration, credentials, topic validation, schemas, payload bounds,
allowlisting, loop prevention, duplicate expiry, both MQTT protocol versions and reconnect bounds.
The ignored integration suites run against a disposable Eclipse Mosquitto 2 broker in CI; the
release-candidate host run and exact command are recorded in [STATUS.md](../STATUS.md).
