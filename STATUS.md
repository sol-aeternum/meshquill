# Current status

Last updated: 2026-07-30

## Release-candidate summary

The `v0.1.0-rc.1` source tree is implemented and locally release-gated on Linux x86-64. The native
CLI, Rust libraries, deterministic companion, transports, hooks, MQTT gateway, and installed-wheel
Python SDK have all completed their applicable local tests. A local native archive and checksum
were generated, extracted, and exercised from an isolated configuration.

This is **not yet a cross-platform-verified release**. No physical MeshCore device was available.
The public [GitHub repository](https://github.com/sol-aeternum/meshquill) has been created, and its
Linux ARM64, macOS, and Windows Actions matrix is the remaining pre-tag publication gate. No GitHub
release, crates.io package, or PyPI project is claimed to exist yet.

## Verification environment

| Item | Recorded evidence |
| --- | --- |
| Host | CachyOS Linux x86-64, kernel 7.1.5 |
| Rust | 1.97.1 stable; MSRV check with 1.88.0; fuzzing with `nightly-2026-07-30` |
| Python | Installed-wheel tests on CPython 3.9.25 and 3.14.6 |
| Broker | Docker 29.6.2 and digest-pinned Eclipse Mosquitto |
| Hardware | None available; no USB companion, and host Bluetooth D-Bus could not initialize |
| Upstream baseline | MeshCore companion firmware v1.16.0, `meshcore_py` v2.3.8, and `meshcore-cli` main `56b246b4`, pinned in [research](docs/research.md) |

TCP loopbacks and the virtual companion are software evidence, not physical-radio evidence. The
physical matrix remains in [hardware testing](docs/hardware-testing.md).

## Quality-gate evidence

| Gate | Result |
| --- | --- |
| Rust formatting | Passed: `cargo fmt --all -- --check` |
| Rust compile/lint | Passed: locked, offline workspace `check` and warnings-denied Clippy across all targets and all features |
| Rust tests | Passed outside the restricted network namespace: 284 passed, 2 real-broker tests intentionally ignored in the ordinary suite; all five TCP loopback tests passed |
| Rust documentation | Passed with `RUSTDOCFLAGS='-D warnings'`, all features, no dependencies |
| MSRV | Passed locked workspace all-target/all-feature check with Rust 1.88.0 |
| Python | Ruff format/check, strict Mypy for Python 3.9, two-module `stubtest`, generated API drift check, and 28 installed-wheel tests on each of Python 3.9.25 and 3.14.6 passed |
| Python wheel | Local `cp39-abi3` Linux x86-64 wheel passed strict Twine metadata, licence inventory, `pip check`, imports, examples, and tests. Its honest local tag requires the build host's glibc 2.38; it is not substituted for a release wheel |
| MQTT | Both ignored integration tests passed separately against a disposable digest-pinned Mosquitto: publication/subscription and allowlisted broker-to-radio direct send |
| Parsers | Both cargo-fuzz targets ran with sanitizers for 20 seconds on the pinned nightly without a crash; truncation, malformed, oversized, and property-style tests also passed |
| Dependencies/licences | `cargo audit 0.22.2` passed for root and fuzz lockfiles against the refreshed RustSec database; `cargo deny 0.19.4 check` passed for both manifests |
| Workflows/docs | actionlint 1.7.12 passed; generated Python API check passed; 128 source Markdown links passed with Lychee 0.24.2; shell release script passed `bash -n` |
| Native package | Local x86-64 GNU archive checksum, 129-file inventory, licences, docs, four completions, 77 man pages, version, init, info, and ACKed send passed |
| Remote platform matrix | **Pending:** the pinned workflow covers Linux x86-64/ARM64, macOS Intel/ARM64, and Windows x86-64; its pushed-commit result must be recorded before the tag is pushed |

The local archive was built on the rolling host and references glibc 2.39. It is only local smoke
evidence. Release CI builds Linux archives on Ubuntu 22.04 and rejects a referenced glibc version
newer than 2.35; Linux wheels are built in a manylinux container and audited for actual
`manylinux_2_28` compatibility before upload. Those CI artifacts have not yet been produced.

## Required acceptance scenarios

Commands below assume `meshquill` is the freshly installed binary and use an isolated config. Test
names and complete broker setup are also encoded in the pinned workflows.

| # | Scenario | Result and reproducible evidence |
| ---: | --- | --- |
| 1 | Clean native installation | Passed: `cargo install --path crates/meshquill-cli --root /tmp/meshquill-cargo-install --locked --offline`; the installed binary reported `0.1.0-rc.1` |
| 2 | First-run wizard/profile | Passed in a real PTY: `meshquill --config /tmp/demo.toml init`, answers `field` and `demo`; a default v1 profile was written |
| 3 | Discovery | Passed: `devices --transport mock` returned the explicit profile; serial enumeration returned host candidates; bounded BLE discovery failed cleanly with actionable D-Bus guidance. No physical target was present |
| 4 | Device information | Passed: `meshquill --config /tmp/demo.toml device info` returned virtual model, firmware, and protocol 10 in human and JSON modes |
| 5 | Contacts | Passed: `contacts` returned deterministic contact `Alice` and its typed JSON record |
| 6 | Direct send | Passed: `send Alice 'clean install direct' --wait` reported companion acceptance and a matching acknowledgement |
| 7 | Receive/watch | Passed: `inbox --limit 1` returned the queued direct message; `--output jsonl watch --count 2` emitted bounded `self_info` and `connected` records |
| 8 | ACK success/timeout | Passed: demo direct send acknowledged; the `ack-timeout` fixture returned stable exit 7 for finite `send --wait`, while line chat emitted `connected,incoming,sent,timed_out` and stayed usable |
| 9 | Reconnect without duplicate | Passed: `cargo test -p meshquill-core explicit_reconnect_handshakes_without_replaying_prior_send` and `cancelling_caller_does_not_cancel_or_duplicate_started_send` |
| 10 | Interactive chat | Passed in a PTY: direct line chat displayed incoming text, then `sent` and `acknowledged`; `/quit` closed cleanly. Piped direct, timeout, and numeric-channel JSONL cases also passed |
| 11 | Human/JSON/JSONL | Passed with installed binary and process tests; finite JSON used one `meshquill.cli/v1` envelope and streams used one envelope per line |
| 12 | Non-interactive failure | Passed: incomplete `--non-interactive init` emitted no stdout, gave the corrective error on stderr, and exited 2 |
| 13 | Python install/quick start | Passed from the locally built wheel in fresh CPython 3.9 and 3.14 environments; `examples/quickstart.py` completed an ACKed demo send |
| 14 | Python streaming | Passed on both supported interpreters: `examples/streaming.py` exercised independent retained/live streams |
| 15 | Hooks/on-message | Passed: `hooks validate examples/hooks/basic.py` found `on_message` and `before_send`; configured `hooks test on_message` completed under `meshquill.hook/v1` |
| 16 | MQTT broker round trip | Passed against disposable Mosquitto using the two exact ignored tests in the `mqtt-mosquitto` [CI job](.github/workflows/ci.yml): event publication/subscription and an explicitly permitted outbound direct send |
| 17 | TLS/auth validation | Passed: missing CA configuration failed before writing config with exit 12; five TLS tests and the username/password pairing test passed; passwords remained runtime-only/redacted |
| 18 | Configuration migration | Passed: a versionless serial profile migrated atomically to v1, preserved default `desk`, and created a readable same-directory backup |
| 19 | Malformed frames/no panic | Passed exhaustive known-packet truncation, malformed/oversized frame tests, strict fixtures, and both sanitizer fuzz targets |
| 20 | Generated-artifact install | Passed locally: `scripts/package-release.sh x86_64-unknown-linux-gnu v0.1.0-rc.1 DIST`, checksum verification, clean extraction, inventory checks, init/info/ACK smoke. Cross-platform CI artifacts remain pending |

An explicit host-level SIGINT test also verified that an indefinite `watch` disconnects cleanly,
prints `interrupted by user`, and exits with the documented status 130.

## Clean-room and review record

The local archive was followed using only the public installation/getting-started instructions and
an isolated configuration. The flow reached the first acknowledged message without developer
configuration. Review found and fixed three documentation problems: discovery now precedes the
wizard in the README, the doctor example actually includes `--connect`, and the demo watch example
uses the two deterministic startup events instead of waiting for a nonexistent live message.

Maintainer, security, CLI-consistency, documentation, and artifact reviews are represented by the
full warnings-denied build/test run, dependency and licence gates, no-unsafe/no-placeholder scan,
stable output/exit process tests, help/manpage generation, link/API drift checks, and the extracted
archive smoke. Hook code remains explicitly trusted, history remains opt-in plaintext, MQTT sends
remain disabled by default, and ambiguous device writes are never replayed.

## Name and publication state

The name was available when checked on 2026-07-30: GitHub search returned no conflicting
repository, `cargo search` returned no `meshquill` package, and PyPI returned an exact 404 for
`meshquill-sdk`. The project now lives at <https://github.com/sol-aeternum/meshquill>.

GitHub authentication for `sol-aeternum` is active and the remote is configured. The quality and
release workflows, registry publication, and prerelease publication remain gated in the order
documented by the [release runbook](docs/release.md). Physical validation is a separately recorded
hardware limitation, not a publication credential problem.
