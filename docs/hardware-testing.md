# Hardware verification

Physical results are kept separate from deterministic host-side tests. A row is added only after a
real device completes the documented smoke suite; discovery alone is not a protocol pass.

## Current matrix

| Date | Device | Firmware | Transport | Host | Result |
| --- | --- | --- | --- | --- | --- |
| 2026-07-30 | No device available | — | BLE/serial | Linux development host | Not run: no USB companion exposed and Bluetooth D-Bus initialization failed. |

TCP loopback and the virtual companion are software tests recorded in the
[live repository status](https://github.com/sol-aeternum/meshquill/blob/main/STATUS.md), not in this
physical matrix.

## Required smoke record

For each device/transport combination record the exact model, firmware build/version, host OS,
adapter/driver where relevant, connection profile with secrets removed, and commands used. At
minimum exercise handshake/info, contacts, direct and channel send, inbox/watch, ACK success and
timeout, clean disconnect, reconnect without retransmission, malformed-input recovery, and Ctrl-C.
Attach only redacted logs and note whether another application held the connection during conflict
testing.
