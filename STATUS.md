# Current status

Last updated: 2026-07-31

## RC4 candidate summary

The current checkout is the `0.1.0-rc.4` release candidate. It carries RC3's product behavior
unchanged and corrects one release-only orchestration defect: cross-platform installed-wheel jobs
exercise the SDK test module, while their prerequisite full CI continues to build the native CLI
and run the complete Python and CLI-schema integration suite.

The immutable annotated `v0.1.0-rc.3` tag peels to
`b72135fc0edf390fe89811587c7c5363fe8d368b`. Its
[release workflow](https://github.com/sol-aeternum/meshquill/actions/runs/30615078718) passed tag
validation, the full CI and supply-chain gates, all five native archive jobs, and every wheel build,
metadata, install, import, version, and SDK check on all five targets. Each wheel job then invoked a
CLI schema integration test without building the expected debug CLI, so draft assembly was skipped.
No RC3 GitHub release, release asset, registry package, or public prerelease was created, and the
tag was not moved or replaced.

RC4 is an unpublished candidate. Its immutable source snapshot intentionally does not assert a
future tag or publication state: consult this file on `main` for live state. Before tagging, the
exact candidate commit must pass fresh CI and supply-chain workflows. A successful tag workflow
creates a private draft prerelease; making that draft public remains a separate maintainer decision
after all 16 assets are inspected.

No physical MeshCore companion was available, so no BLE, serial, or over-the-air radio result is
claimed. No crates.io or PyPI credential is available; those independent registry uploads remain
unpublished and do not block a public GitHub prerelease.

## Publication baseline

- Public source repository: <https://github.com/sol-aeternum/meshquill>.
- The pushed annotated `v0.1.0-rc.2` tag peels to
  `0f33edbec475819ea7737c8fd03808312237f468`. Its successful
  [release workflow](https://github.com/sol-aeternum/meshquill/actions/runs/30594223032) assembled
  16 assets: four `.tar.gz` archives, one `.zip`, five sibling `.sha256` files, five wheels, and
  `SHA256SUMS`.
- RC2 remains a maintainer-visible draft (`isDraft: true`, `isPrerelease: true`,
  `publishedAt: null`) named “superseded; do not publish.” Its packaged documentation captured
  pre-tag status and was therefore rejected as a public candidate; the immutable tag and assets
  were not replaced.
- The pushed annotated `v0.1.0-rc.1` tag points to
  `5c6c1233143ae95337fe8e064d78b42727e7daf8`; its 16-asset release also remains a private draft.
- RC3 has the immutable tag and failed workflow recorded above, but no GitHub release or release
  assets. No `v0.1.0-rc.4` tag or release exists at candidate preparation time. No `meshquill`
  crates.io package or `meshquill-sdk` PyPI release is claimed.

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

## Fresh local RC4 delta evidence

RC4 changes only release orchestration and versioned metadata; the broader unchanged product
behavior remains covered by the historical RC3 evidence below.

| Gate | Fresh RC4 result |
| --- | --- |
| Version and metadata | Passed the Rust `0.1.0-rc.4`, Python `0.1.0rc4`, changelog, exact internal dependency, locked workspace, and locked fuzz metadata contracts |
| Rust formatting/compile/lint | Passed formatting, locked all-target/all-feature checking, warnings-denied Clippy, and warnings-denied all-feature Rustdoc with Rust 1.97.1 |
| Rust tests/MSRV | Passed 395 ordinary workspace/all-target tests; the five real-broker tests were the only ignored cases and all five TCP loopback tests passed. Locked all-target/all-feature checking passed with Rust 1.88.0 |
| Installed Python wheel | A fresh local `cp39-abi3` RC4 wheel passed strict metadata and licence inventory, dependency and exact-version checks, Ruff, Mypy, two-module stubtest, generated-API drift, the corrected 28-test release SDK scope, the complete 37-test CI suite, and both examples on CPython 3.14. The same wheel passed import/version/dependency checks and both examples on CPython 3.9 |
| Dependencies/licences | `cargo audit --no-fetch` and `cargo deny check` passed for root and fuzz lockfiles; a current online Pip Audit reported no known vulnerabilities in the exact Python requirements |
| Workflows/docs | Actionlint 1.7.12, shell package syntax, and `git diff --check` passed; Lychee 0.24.2 validated all 165 tracked source Markdown links |
| Native package | The RC4 shell package produced a verified archive and sibling checksum. Safe clean extraction found 130 regular files, no links or unsafe paths, both licences, 26 docs, one example, nine schemas/fixtures, four completions, and 83 man pages. The extracted binary reported RC4 and passed init, device info, contacts, ACKed send, inbox, and bounded JSONL watch |
| Remote source matrix | Passed for pushed source candidate `24f8f5745d12e7e05526d33713e60a540d8f6723`: [CI run 30620791102](https://github.com/sol-aeternum/meshquill/actions/runs/30620791102) and [supply-chain run 30620791160](https://github.com/sol-aeternum/meshquill/actions/runs/30620791160) both completed successfully. This evidence-only status commit must receive its own exact-SHA CI and supply-chain success before tagging |

The local archive was built on a rolling Linux host and is local smoke evidence only; it is not a
substitute for any tagged CI artifact. Tagged CI must build and validate all five native targets,
enforce the Linux glibc ceiling, build and audit all five wheels, and assemble the exact 16-asset
private draft.

## Historical local RC3 evidence

RC3's product evidence remains applicable to the unchanged implementation carried by RC4, but it
is not substituted for RC4's fresh version, workflow, source, and artifact gates.

| Gate | Historical RC3 result |
| --- | --- |
| Version and metadata | Passed the Rust `0.1.0-rc.3`, Python `0.1.0rc3`, changelog, locked workspace metadata, locked fuzz metadata, shell syntax, and `git diff --check` contracts |
| Rust formatting/compile/lint | Passed `cargo fmt`; locked workspace checking and warnings-denied Clippy across all targets and features |
| Rust tests | Passed 395 ordinary workspace/all-target tests; the five real-broker tests were the only ignored cases and passed separately. This includes 82 core, 73 CLI process, 70 CLI library, 34 deterministic-companion, and five TCP loopback tests |
| Rust documentation/MSRV | Passed warnings-denied all-feature Rustdoc and locked all-target/all-feature checking with Rust 1.88.0 |
| Python 3.14 | The installed local `cp39-abi3` wheel passed exact-version/import checks, strict Twine metadata, licence inventory, `pip check`, Ruff format/check, strict Mypy, two-module stubtest, generated-API drift, 37 Pytest cases, quickstart, and streaming |
| Python 3.9 | The same installed wheel passed exact-version/import checks, licence inventory, `pip check`, quickstart, and streaming in a fresh environment |
| MQTT | All five real-broker cases passed against disposable digest-pinned Mosquitto listeners: MQTT 3.1.1, MQTT 5, allowlisted CLI bridge send, private-CA TLS/auth/mTLS positive and negative checks, and a fresh-process persisted TLS/mTLS/environment-secret connection |
| Fuzzing | All four sanitizer targets completed fresh 20-second unsandboxed gates without a crash: `protocol_packet`, `outer_frames`, `remote_payloads`, and `mqtt_commands` |
| Dependencies/licences | `cargo audit --no-fetch` and `cargo deny check` passed for root and fuzz lockfiles; Pip Audit reported no known vulnerabilities for the exact Python requirements |
| Workflows/docs | Actionlint 1.7.12 passed; the shell package script passed `bash -n`; Lychee 0.24.2 validated 162 source Markdown links. PowerShell packaging remains delegated to tagged Windows CI because `pwsh` is unavailable locally |
| Remote source matrix | Passed for final tagged commit `b72135fc0edf390fe89811587c7c5363fe8d368b`: [CI run 30613920036](https://github.com/sol-aeternum/meshquill/actions/runs/30613920036) and [supply-chain run 30613919813](https://github.com/sol-aeternum/meshquill/actions/runs/30613919813) both completed successfully |
| Source install/interactive use | A locked offline `cargo install` reported RC3. Real PTYs exercised both guided profile/transport prompts and destination-prompted line chat; chat displayed the queued message, sent one reply, observed its ACK, and exited via `/quit` |
| Public hook example | An isolated copy of `examples/hooks/basic.py` validated and dispatched `on_message`, producing exactly one `meshquill.hook/v1` metadata record beside the copied script |
| Native package | The RC3 shell package produced one archive and sibling checksum. Safe clean extraction found 130 regular files, no symlinks, both licences, 26 docs, one example, nine schemas/fixtures, four completions, and 83 man pages; the extracted binary passed version, init, info, contacts, ACKed send, inbox, and bounded JSONL watch |

The local archive was built on a rolling Linux host and is local smoke evidence only. Its highest
referenced glibc symbol is 2.39, so it is deliberately not a distributable release artifact. Tagged
CI must still build all five native targets, enforce Linux glibc 2.35 or older, build and audit five
platform wheels, and assemble the exact 16-asset private draft.

## Historical local RC3 acceptance scenarios

Commands used isolated configuration/data directories and either the locked source-installed RC3
binary, the clean extracted RC3 archive, the installed RC3 wheel, or exact named tests. Hardware-only
evidence is called out rather than simulated.

| # | Scenario | Historical RC3 result and reproducible evidence |
| ---: | --- | --- |
| 1 | Clean native installation | Passed locked, offline `cargo install --path crates/meshquill-cli --root TEMP`; the installed binary reports `0.1.0-rc.3` |
| 2 | First-run wizard/profile | Passed through a real PTY: `Profile name` = `wizard`, `Transport` = `demo`; the private atomic config selected the new profile by default |
| 3 | Discovery | Passed deterministic mock and host serial-enumeration coverage; bounded BLE host failure remains actionable. No physical target was present, so no radio discovery is claimed |
| 4 | Device information | Passed from the clean archive; the deterministic companion returned protocol 10 plus virtual firmware/model information |
| 5 | Contacts | Passed deterministic list/show/search/prefix coverage; the clean archive returned `Alice` |
| 6 | Direct send | Passed clean-room `send Alice 'clean-room RC3' --wait` with one companion acceptance and the matching acknowledgement |
| 7 | Receive/watch | Passed bounded inbox; the clean archive emitted exactly the deterministic `self_info` and `connected` JSONL startup events |
| 8 | ACK success/timeout | Passed deterministic ACK success and stable exit 7 finite timeout coverage; line chat keeps timeout nonfatal and never retransmits automatically |
| 9 | Reconnect without duplicate | Passed explicit reconnect, cancellation, watch/chat reconnect, late-sync-response reconciliation, and no-mutation-replay coverage; valid asynchronous notifications are published before the one sync response, and cancellation cannot issue a second command before reconciliation |
| 10 | Interactive chat | Passed a real PTY destination prompt, queued incoming display, outgoing reply, matching ACK, and `/quit`; process coverage additionally checks switching, history, reconnect, SIGINT, and explicit unconfirmed-draft handling |
| 11 | Human/JSON/JSONL | Passed output/process tests and checked-in strict CLI schema/fixtures; finite output remains one value and streams one envelope per line |
| 12 | Non-interactive failure | Passed: incomplete non-interactive init writes no stdout, emits corrective stderr, exits 2, and does not create configuration |
| 13 | Python install/quick start | Passed from one local `cp39-abi3` wheel on CPython 3.9 and 3.14; `examples/quickstart.py` completed an ACKed demo send |
| 14 | Python streaming | Passed on both interpreters with independent retained/live streams through `examples/streaming.py` |
| 15 | Hooks/on-message | Passed the exact public `examples/hooks/basic.py`: validation plus configured `on_message` dispatch wrote one metadata-only record; bounded failure/child-reaping cases pass tests |
| 16 | MQTT broker round trip | Passed MQTT 3.1.1, MQTT 5, and the CLI outbound bridge against disposable digest-pinned Mosquitto listeners |
| 17 | TLS/auth validation | Passed private-CA TLS/auth/mTLS and fresh-process persisted configuration; wrong CA, password, client identity, and server name were rejected while secrets remained runtime-only/redacted |
| 18 | Configuration migration | Passed atomic versionless-to-v1 migration with preserved default and same-directory backup, repair, one-MiB input bounds, and locked cross-process mutation coverage |
| 19 | Malformed frames/no panic | Passed exhaustive packet truncation, malformed/oversized/property tests and all four fresh sanitizer fuzz targets |
| 20 | Generated-artifact install | Passed checksum, safe clean extraction, exact 130-file inventory, licence/schema/completion/manpage checks, version, init, info, contacts, ACKed send, inbox, and bounded JSONL watch |

## Historical RC2 evidence

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
| Native package | `scripts/package-release.sh x86_64-unknown-linux-gnu v0.1.0-rc.2 dist` produced a verified archive and sibling checksum; clean extraction found both licences, the CLI schema and fixtures, four completions, and 77 man pages, then passed version, init, info, contacts, ACKed send, inbox, and JSONL watch |
| Remote source matrix | Passed for final tagged commit `0f33edbec475819ea7737c8fd03808312237f468`: [CI run 30593867355](https://github.com/sol-aeternum/meshquill/actions/runs/30593867355) and [supply-chain run 30593867327](https://github.com/sol-aeternum/meshquill/actions/runs/30593867327) both completed successfully |
| Tagged artifact matrix | Passed privately in [release run 30594223032](https://github.com/sol-aeternum/meshquill/actions/runs/30594223032): 16 draft assets were assembled and checksummed. The draft was not published and was superseded after packaged documentation proved stale |

The local archive was built on a rolling Linux host and is local smoke evidence only. Release CI
builds Linux archives on Ubuntu 22.04 and rejects a referenced glibc version newer than 2.35; Linux
wheels are built in a pinned manylinux environment and audited for actual `manylinux_2_28`
compatibility. Local artifacts must not be substituted for the five CI-built native archives or five
CI-built wheels.

## Historical RC2 acceptance scenarios

Commands below use an isolated configuration and either the installed RC2 binary, the extracted
RC2 archive, the installed wheel, or an exact named test. Hardware-only evidence is called out.

| # | Scenario | Historical RC2 result and reproducible evidence |
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
| 20 | Generated-artifact install | Passed local RC2 checksum, clean extraction, inventory/licence/schema/completion/manpage checks, version, init, info, contacts, ACKed send, inbox, and JSONL watch. Cross-platform artifacts subsequently passed the private tagged release run recorded above |

Host-level SIGINT process tests verify the documented status 130 and `interrupted by user`
diagnostic while connecting, waiting for an ACK, waiting for line input, and watching. They verify
bounded user-visible cancellation and no automatic replay; they do not claim physical transport
disconnect instrumentation.

## Known limitations and remaining RC4 release gates

- The companion protocol supplies no globally unique, durable message ID. Meshquill preserves every
  separately decoded occurrence and coalesces only return/event clones carrying the same ephemeral
  client observation ID; it does not claim cross-process retransmission identity.
- An ambiguous chat write is retained as an unconfirmed destination-bound draft and is never
  replayed automatically. Explicit `/send` can duplicate delivery if the original radio write
  succeeded but its response was lost; `/discard` avoids that risk.
- History is opt-in bounded plaintext. Hook programs are explicitly trusted local code. MQTT sends
  are disabled by default and broker ACLs remain a security boundary.
- Release archives and wheels use SHA-256 integrity checks but are unsigned; macOS and Windows
  binaries are not code-signed.
- RC4 still requires: successful CI and supply-chain runs for the exact final source SHA, an
  immutable annotated tag, successful
  five-target artifact jobs, inspection of all 16 private-draft assets and packaged documentation,
  explicit approval before public GitHub prerelease publication, and a fresh public
  checksum/download smoke. Physical hardware and registry publication remain separately recorded
  limitations.

## Historical RC1 evidence

RC1's pre-tag source commit `0661982538693b6a35c5c177f754c4416bd36d03` passed
[CI run 30560917595](https://github.com/sol-aeternum/meshquill/actions/runs/30560917595) and
[supply-chain run 30560916669](https://github.com/sol-aeternum/meshquill/actions/runs/30560916669)
on 2026-07-30. Its local evidence used Rust 1.97.1/MSRV 1.88, CPython 3.9.25 and 3.14.6,
28 installed-wheel Python tests on each interpreter, two fuzz targets, two real-broker cases, and
`scripts/package-release.sh x86_64-unknown-linux-gnu v0.1.0-rc.1 dist`. The final tagged RC1 commit is
`5c6c1233143ae95337fe8e064d78b42727e7daf8`; its five native archives and five wheels were assembled
successfully but remain only in the private draft described above. RC1 is historical evidence, not
the availability or quality claim for the current RC4 source.
