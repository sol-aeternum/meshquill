# Current status

Last updated: 2026-07-31

## RC2 candidate summary

The current checkout is the `0.1.0-rc.2` release candidate. Its Rust workspace, native CLI,
deterministic companion, transports, hooks, MQTT gateway, installed-wheel Python SDK, four parser
fuzz targets, and local Linux x86-64 package have completed their applicable local gates. The exact
CI Rust test command passed 338 tests; the four deliberately ignored real-broker cases also passed
separately against disposable digest-pinned Mosquitto brokers.

RC2 is **not published yet**. Source candidate
`52702397834916fad2cdd148a9dad03dc283578f` is pushed and passed its exact GitHub CI and
supply-chain workflows, but it has no tag, GitHub release, or public assets. Recording that evidence
in this file creates the final tag candidate; that resulting exact commit must pass both workflows
before an annotated `v0.1.0-rc.2` tag may trigger the five-target native and wheel matrix. A
successful tagged workflow creates a private draft pre-classified as a prerelease, not a public
download. This file must be updated with the immutable tag, asset, and public-download evidence as
those boundaries complete.

No physical MeshCore companion was available, so no BLE, serial, or over-the-air radio result is
claimed. No crates.io or PyPI credential is available; those independent registry uploads remain
unpublished and do not block a public GitHub prerelease.

## Publication baseline

- Public source repository: <https://github.com/sol-aeternum/meshquill>.
- The pushed annotated `v0.1.0-rc.1` tag points to
  `5c6c1233143ae95337fe8e064d78b42727e7daf8`.
- RC1's successful tag workflow assembled 16 assets: four `.tar.gz` archives, one `.zip`, five
  sibling `.sha256` files, five wheels, and `SHA256SUMS`. The associated GitHub release remains a
  maintainer-visible draft (`isDraft: true`, `publishedAt: null`), so those assets are not a public
  RC1 delivery.
- No `v0.1.0-rc.2` tag or release exists yet. No `meshquill` crates.io package or
  `meshquill-sdk` PyPI release is claimed.

## Verification environment

| Item | Recorded evidence |
| --- | --- |
| Host | CachyOS Linux x86-64, kernel 7.1.5 |
| Rust | 1.97.1 stable; MSRV check with 1.88.0; fuzzing with `nightly-2026-07-30` |
| Python | Installed-wheel checks on CPython 3.9.25 and 3.14.6 |
| Broker | Docker 29.6.2; Eclipse Mosquitto 2 image pinned by digest `sha256:6f8d8a947c506f8a2290ec65cd4bd2bc7cb4d43fb5f6271f861cb013e2ef9797` |
| Hardware | None available; no USB companion or physical-radio path was exercised |
| Upstream baseline | MeshCore companion firmware v1.16.0, `meshcore_py` v2.3.8, and `meshcore-cli` main `56b246b4`, pinned in [research](docs/research.md) |

TCP loopbacks and the deterministic companion are software evidence, not physical-device evidence.
The physical matrix remains in [hardware testing](docs/hardware-testing.md).

## Fresh local RC2 evidence

| Gate | Result |
| --- | --- |
| Rust formatting | Passed: `cargo fmt --all -- --check` |
| Rust compile/lint | Passed locked, offline workspace checking and warnings-denied Clippy across all targets and all features |
| Rust tests | Passed outside the restricted network namespace: 338 passed and four real-broker cases intentionally ignored by the ordinary `cargo test --workspace --all-targets --locked` suite; all five TCP loopbacks passed |
| Rust documentation | Passed with `RUSTDOCFLAGS='-D warnings'`, all features, no dependencies |
| MSRV | Passed locked workspace all-target/all-feature checking with Rust 1.88.0 |
| Python 3.14 | The installed `cp39-abi3` wheel passed exact-version/import checks, strict Twine metadata, licence inventory, `pip check`, Ruff format/check, strict Mypy, two-module stubtest, generated-API drift, 28 Pytest cases, quickstart, and streaming |
| Python 3.9 | The same installed wheel passed exact-version/import checks, licence inventory, `pip check`, quickstart, and streaming in a fresh environment |
| MQTT | MQTT 3.1.1, MQTT 5, allowlisted broker-to-demo direct send, and private-CA TLS/auth/mTLS enforcement all passed; the negative cases rejected wrong trust, password, client identity, and server name |
| Fuzzing | All four sanitizer targets completed 20-second gates without a crash: `protocol_packet` 2,640,848 runs, `outer_frames` 774,764, `remote_payloads` 1,698,222, and `mqtt_commands` 99,305 |
| Dependencies/licences | `cargo audit --no-fetch` and `cargo deny check` passed for root and fuzz lockfiles; Pip Audit reported no known vulnerabilities for the exact Python requirements |
| Workflows/docs | Actionlint 1.7.12 passed after the exact 16-asset draft allowlist was added; the shell package script passed `bash -n`; Lychee 0.24.2 validated 145 source Markdown links. PowerShell release packaging awaits the tagged Windows artifact workflow because `pwsh` is unavailable locally |
| Native package | `scripts/package-release.sh x86_64-unknown-linux-gnu v0.1.0-rc.2` produced a verified archive and sibling checksum; clean extraction found both licences, the CLI schema and fixtures, four completions, and 77 man pages, then passed version, init, info, contacts, ACKed send, inbox, and JSONL watch |
| Remote source matrix | Passed for pushed source candidate `52702397834916fad2cdd148a9dad03dc283578f`: [CI run 30593438470](https://github.com/sol-aeternum/meshquill/actions/runs/30593438470) and [supply-chain run 30593438464](https://github.com/sol-aeternum/meshquill/actions/runs/30593438464) both completed successfully. The evidence-only status commit still requires its own exact-SHA rerun before tagging |
| Tagged artifact matrix | **Pending:** no RC2 tag exists; no RC2 draft or public assets are claimed |

The local archive was built on a rolling Linux host and is local smoke evidence only. Release CI
builds Linux archives on Ubuntu 22.04 and rejects a referenced glibc version newer than 2.35; Linux
wheels are built in a pinned manylinux environment and audited for actual `manylinux_2_28`
compatibility. Local artifacts must not be substituted for the five CI-built native archives or five
CI-built wheels.

## Required acceptance scenarios

Commands below use an isolated configuration and either the installed RC2 binary, the extracted
RC2 archive, the installed wheel, or an exact named test. Hardware-only evidence is called out.

| # | Scenario | RC2 result and reproducible evidence |
| ---: | --- | --- |
| 1 | Clean native installation | Passed locally with locked, offline `cargo install --path crates/meshquill-cli --root TEMP`; the installed binary reports `0.1.0-rc.2` |
| 2 | First-run wizard/profile | Passed through non-interactive process tests, an interactive PTY test, and extracted-archive `init`; v1 profiles are written atomically |
| 3 | Discovery | Passed deterministic mock and host serial enumeration tests; bounded BLE failure remains actionable when host D-Bus is unavailable. No physical target was present |
| 4 | Device information | Passed in human and JSON modes against the deterministic companion; the clean-room archive returned protocol 10 and virtual firmware/model data |
| 5 | Contacts | Passed deterministic list/show/search/unique-prefix flows and the clean-room archive returned `Alice` |
| 6 | Direct send | Passed clean-room `send Alice 'clean-room RC2' --wait` with one companion acceptance and matching acknowledgement |
| 7 | Receive/watch | Passed bounded inbox and JSONL watch; the extracted archive emitted exactly the deterministic `self_info` and `connected` startup events |
| 8 | ACK success/timeout | Passed deterministic ACK success and stable exit 7 on finite timeout; line chat emits a nonfatal timeout state and remains usable |
| 9 | Reconnect without duplicate | Passed explicit reconnect, cancelled-caller, bounded watch/chat reconnect, retained-push, and no-mutation-replay tests; each queued sync response uses exactly one command |
| 10 | Interactive chat | Passed PTY and process coverage for live incoming display, contacts, target/channel switching, conversation history, three-attempt reconnect, SIGINT, and destination-bound unconfirmed draft `/send` or `/discard` handling |
| 11 | Human/JSON/JSONL | Passed process tests plus checked-in schema/fixtures; finite output is one `meshquill.cli/v1` value and streams are one envelope per line |
| 12 | Non-interactive failure | Passed: incomplete non-interactive init writes no stdout, emits corrective stderr, and exits 2 |
| 13 | Python install/quick start | Passed from the local `cp39-abi3` wheel on CPython 3.9 and 3.14; `examples/quickstart.py` completed an ACKed demo send |
| 14 | Python streaming | Passed on both interpreters with independent retained/live streams through `examples/streaming.py` |
| 15 | Hooks/on-message | Passed validation and configured `on_message` dispatch under `meshquill.hook/v1`; partial-pipe, timeout, and child-reaping cases are bounded |
| 16 | MQTT broker round trip | Passed all three ignored MQTT-crate broker tests plus the CLI bridge test against disposable digest-pinned Mosquitto listeners |
| 17 | TLS/auth validation | Passed positive private-CA TLS plus negative wrong-CA, wrong-password, missing-client-certificate, and wrong-host checks; secrets stayed runtime-only/redacted |
| 18 | Configuration migration | Passed atomic versionless-to-v1 migration with preserved default and same-directory backup, plus repair and one-MiB input-bound tests |
| 19 | Malformed frames/no panic | Passed exhaustive known-packet truncation, malformed/oversized/property tests and four sanitizer fuzz targets |
| 20 | Generated-artifact install | Passed local RC2 checksum, clean extraction, inventory/licence/schema/completion/manpage checks, version, init, info, contacts, ACKed send, inbox, and JSONL watch. Cross-platform RC2 CI artifacts remain pending |

Host-level SIGINT process tests verify the documented status 130 and `interrupted by user`
diagnostic while connecting, waiting for an ACK, waiting for line input, and watching. They verify
bounded user-visible cancellation and no automatic replay; they do not claim physical transport
disconnect instrumentation.

## Known limitations and remaining release gates

- Live/queued incoming-message correlation is a bounded, connection-local heuristic because the
  companion protocol supplies no globally unique occurrence ID. A distinct identical message on
  the opposite observation path inside five seconds can collide; identical same-path occurrences
  remain distinct.
- An ambiguous chat write is retained as an unconfirmed destination-bound draft and is never
  replayed automatically. Explicit `/send` can duplicate delivery if the original radio write
  succeeded but its response was lost; `/discard` avoids that risk.
- History is opt-in bounded plaintext. Hook programs are explicitly trusted local code. MQTT sends
  are disabled by default and broker ACLs remain a security boundary.
- Release archives and wheels use SHA-256 integrity checks but are unsigned; macOS and Windows
  binaries are not code-signed.
- RC2 still requires: committing and pushing this evidence-only status update, successful CI and
  supply-chain runs for that resulting exact SHA, an immutable annotated tag, successful
  five-target artifact jobs, inspection of all 16 draft assets, publication as a GitHub prerelease,
  and a fresh public checksum/download smoke. Physical hardware and registry publication remain
  separately recorded limitations.

## Historical RC1 evidence

RC1's pre-tag source commit `0661982538693b6a35c5c177f754c4416bd36d03` passed
[CI run 30560917595](https://github.com/sol-aeternum/meshquill/actions/runs/30560917595) and
[supply-chain run 30560916669](https://github.com/sol-aeternum/meshquill/actions/runs/30560916669)
on 2026-07-30. Its local evidence used Rust 1.97.1/MSRV 1.88, CPython 3.9.25 and 3.14.6,
28 installed-wheel Python tests on each interpreter, two fuzz targets, two real-broker cases, and
`scripts/package-release.sh x86_64-unknown-linux-gnu v0.1.0-rc.1`. The final tagged RC1 commit is
`5c6c1233143ae95337fe8e064d78b42727e7daf8`; its five native archives and five wheels were assembled
successfully but remain only in the private draft described above. RC1 is historical evidence, not
the availability or quality claim for the current RC2 source.
