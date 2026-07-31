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
choice intended for a secured local test broker. `--password-stdin` stores the supplied password in
the OS credential store. `--password-env NAME` stores only the validated environment-variable name
and resolves its value at runtime; the password is never written to TOML or argv. Reusing the same
username without either option preserves its existing reference, while `--clear-auth` removes the
authentication configuration and any managed credential. Secrets are never emitted by
`config show`. Passwords from stdin, environment variables, the credential store, or an interactive
prompt share the same 1–4096 UTF-8-byte bound and reject NUL. TLS file arguments are canonicalized
to absolute paths before the configuration transaction is saved, so a later working directory
cannot change which trust or identity material is loaded.

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
backoff. The first broker connection publishes a full contacts/battery/telemetry snapshot; the
first successful connection after each observed broker disconnect publishes one fresh full
snapshot. The application never retransmits a radio message during that synchronization. A
MeshCore companion disconnect is terminal for the bridge: pending local ACK records are marked
failed, hooks are balanced once, and the command exits with connection status and asks the operator
to restart after restoring the device.
MQTT 3.1.1 and MQTT 5 are both supported; protocol selection changes the broker session, not the
versioned application payload schema.

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
and any non-allowlisted type. Retained publications are rejected before parsing or duplicate-cache
insertion, so a broker cannot replay a retained send into a fresh gateway. A trusted local
`before_send` hook may modify a command, but the gateway applies the configured destination,
channel, and text bounds again before radio I/O. The default duplicate cache retains 4096 IDs for
15 minutes.

Deduplication is deliberately process-local. A configuration with `allow_send = true` therefore
must use a clean broker session; validation rejects a persistent session that could redeliver old
commands after the process cache is lost. Broker credentials and mTLS identify the client, but the
broker's topic ACL remains a security boundary: grant publish access to the outbound topic only to
the operators that are allowed to cause radio sends.

## Topics and payloads

See [the v1 schema reference](reference/mqtt-schema-v1.md). Under a configured prefix `P`, the
bridge publishes:

- `P/meshquill.mqtt/v1/events/incoming_message`
- `P/meshquill.mqtt/v1/events/ack`
- `P/meshquill.mqtt/v1/events/connection_state`
- `P/meshquill.mqtt/v1/events/contacts`
- `P/meshquill.mqtt/v1/events/telemetry`

It subscribes only to `P/meshquill.mqtt/v1/outbound/send`, and only after outbound sends are
explicitly enabled. `mqtt test` does not report send-capable readiness until the broker returns one
successful SUBACK for that exact subscription; rejection, an empty response, or a mismatched
multi-topic response is terminal and includes an ACL hint. MQTT QoS improves broker delivery
semantics; it is not a MeshCore radio ACK.

## Validation evidence

Unit tests cover TLS configuration, credentials, topic validation, schemas, payload bounds,
allowlisting, loop prevention, duplicate expiry, both MQTT protocol versions and reconnect bounds.
The ignored CI integration suite is designed to use digest-pinned disposable Eclipse Mosquitto 2
brokers and assert real publish/subscribe command round trips over MQTT 3.1.1 and MQTT 5, the
CLI-to-radio allowlist bridge, and a secured broker requiring a private CA, username/password and
client certificate. The secured case is designed to assert that unrelated trust roots, a wrong
password, a missing client identity and a mismatched server name do not reach `Connected`.

Broker-restart behavior is covered by the maintained client's reconnect state machine and bounded
unit tests, but this RC does not claim a live container-restart endurance test. [Live status](https://github.com/sol-aeternum/meshquill/blob/main/STATUS.md)
records which exact release candidate executed the real-broker suite; this page describes the
maintained validation design and is not evidence by itself.
