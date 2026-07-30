# Remote administration

Remote administration targets repeater, room-server, or sensor contacts through the locally
connected companion. This RC has deterministic virtual-companion coverage but no recorded physical
hardware verification. Do not use it for safety-critical or destructive administration; check
[current status](../STATUS.md) and [hardware verification](hardware-testing.md).

## Prerequisites and command classes

Confirm the local profile and target contact first:

```console
$ meshquill status
$ meshquill contacts --kind repeater
$ meshquill contacts show Repeater
$ meshquill doctor --connect
```

Contact resolution uses an exact case-sensitive name or unique hexadecimal public-key prefix. Each
remote command opens the selected local transport. Commands that need authentication ask the
companion whether a remote session currently exists and fail with a `remote login` hint if it does
not.

| Operation | Login required by Meshquill |
| --- | --- |
| `remote regions CONTACT` | No; anonymous bounded request |
| `remote owner CONTACT` | No; anonymous bounded request |
| `remote clock CONTACT` | No; anonymous read |
| `remote clock CONTACT --sync` | Yes |
| `remote status CONTACT` | Yes |
| `remote neighbours CONTACT ...` | Yes |
| `remote run CONTACT COMMAND` | Yes |
| `sensor telemetry`, `sensor summary`, `sensor acl` | Yes |
| `remote logout CONTACT` | Yes |

The remote firmware can still deny a request because of its role, permissions, ACL, feature set, or
session lifetime.

## Log in without exposing the password

Do not put a remote password in a command argument. In an interactive terminal:

```console
$ meshquill remote login Repeater
```

Meshquill first tries the operating-system credential store, then prompts securely if no stored
credential is available. Save a credential only after a successful login:

```console
$ meshquill remote login Repeater --save
```

For automation, make the stdin read explicit:

```console
$ meshquill --non-interactive remote login Repeater --password-stdin < protected-password-file
```

Add `--save` only if the runner has an available and appropriate OS credential store. Stdin
input must be at most 1024 bytes. Meshquill then removes one trailing LF or CRLF; the resulting
password must be non-empty UTF-8 and contain no NUL. Password values are not included in result
records or authentication errors.

Stored credential accounts are scoped to the configuration path, profile, and contact public key.
Headless Linux sessions in particular may lack an unlocked credential backend; use
`--password-stdin` rather than falling back to plaintext TOML.

## Read remote state

Anonymous metadata reads:

```console
$ meshquill remote regions Repeater
$ meshquill remote owner Repeater
$ meshquill remote clock Repeater
```

Authenticated reads:

```console
$ meshquill remote status Repeater
$ meshquill remote neighbours Repeater
$ meshquill remote neighbours Repeater --count 50 --offset 0 --order strongest --prefix-length 8
```

Neighbour order is `newest`, `oldest`, `strongest`, or `weakest`; `count` is an unsigned byte,
`offset` is an unsigned 16-bit value, and public-key prefix length must be from 1 through 32.

Synchronizing the remote clock requires a session:

```console
$ meshquill remote clock Repeater --sync
```

The result reports the clock value read before the synchronization command and that the command was
queued. It does not claim a post-write read-back.

## Query sensors

All current sensor operations require an authenticated remote session:

```console
$ meshquill sensor telemetry SensorNode
$ meshquill sensor summary SensorNode
$ meshquill sensor summary SensorNode --start-secs-ago 86400 --end-secs-ago 0
$ meshquill sensor acl SensorNode
```

For `summary`, `end-secs-ago` must not be greater than `start-secs-ago`. Firmware permissions and
compiled features determine whether telemetry, summaries, and ACL data are available.

## Run one CommonCLI command

`remote run` forwards exactly one quoted command string after verifying an authenticated session.
Only these complete, trimmed commands are on the conservative read-only allowlist:

```text
?  help  info  status  stats  uptime  ver  version  clock
```

For example:

```console
$ meshquill remote run Repeater "status"
```

The match is exact and case-insensitive. `clock sync`, `get ...`, a command with arguments, shell
punctuation, and every unknown command are outside the allowlist, even if the firmware might treat
one as harmless.

Anything outside that list requires both an explicit mutation marker and confirmation:

```console
$ meshquill --yes remote run Repeater "reboot" --destructive
```

Without `--destructive`, Meshquill refuses before connecting. Without interactive confirmation or
global `--yes`, it refuses before sending. These flags express intent; they do not make a command
safe, validate it against the target firmware, or prove that the remote node executed it. A success
result means the companion queued it and returned acknowledgement correlation metadata.

Avoid combining multiple firmware commands in one string. Meshquill does not provide or validate a
remote shell language.

## End sessions and delete credentials

End the companion's authenticated remote session:

```console
$ meshquill remote logout Repeater
```

Logout does not delete the locally stored credential. Delete that separately; this is a confirmed
local destructive action:

```console
$ meshquill --yes remote credentials-forget Repeater
```

Deleting the stored credential does not itself log out the remote session. Conversely, logout does
not remove the credential.

## Diagnose failures

- Exit 8 or `no authenticated remote session` means login or credential resolution failed, or the
  remote session expired. Log in again with the same profile and contact.
- A malformed/unsupported remote payload points to firmware compatibility; run
  `meshquill doctor --connect` and compare the supported protocol baseline in
  [protocol coverage](protocol-coverage.md).
- A denied response can be firmware permissions, a disabled feature, Meshquill's command policy, or
  missing confirmation.
- A queued command without observed effect is not proof of execution. Query state again with a typed
  read where one exists.
- Transport and OS recovery steps are in [troubleshooting](troubleshooting.md).
