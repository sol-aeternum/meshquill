# Contributing

Meshquill is an independent interoperability project. Contributions must not include private keys,
passwords, personal messages, unpublished firmware, or captures that identify people without their
permission.

## Development setup

Install the Rust version in `rust-toolchain.toml` plus `clippy` and `rustfmt`. On Linux, BLE and
serial builds also need D-Bus and udev development packages. Then run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Python, MQTT, documentation, audit and licence gates are described in `docs/release.md` and are
required when the affected surface changes.

## Change expectations

- Derive wire behavior from the pinned current upstream source and include a golden or behavioral
  test. Do not infer packet layouts from memory.
- Preserve bounded inputs, queues and output. External data must not cause a panic.
- Never auto-retry a command after an ambiguous write.
- Keep machine-readable schemas backward compatible or introduce a new schema identifier.
- Update `docs/protocol-coverage.md`, `docs/capability-matrix.md`, and public examples with behavior.
- Record physical hardware evidence precisely; simulated results are labeled as such.

By contributing, you agree that your contribution is licensed under Apache-2.0 OR MIT, at the
recipient's option.
