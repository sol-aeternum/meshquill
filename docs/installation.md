# Installation

## Release archives

Consult [live status](https://github.com/sol-aeternum/meshquill/blob/main/STATUS.md) for authoritative
`v0.1.0-rc.3` tag, artifact, and public-prerelease availability. A source checkout or packaged copy
of this page does not itself prove publication. If the release is not yet public, use the source
installation below; the asset names and download commands apply once status records publication.

When run for the `v0.1.0-rc.3` tag, the release workflow builds:

| Target | Archive | Build/runtime note |
| --- | --- | --- |
| Linux x86-64 GNU | `.tar.gz` | built on Ubuntu 22.04; requires glibc 2.35+, D-Bus (and its systemd dependency) and libudev |
| Linux ARM64 GNU | `.tar.gz` | built on Ubuntu 22.04 ARM; requires glibc 2.35+, D-Bus and libudev |
| macOS Intel | `.tar.gz` | built and smoke-tested on the macOS 15 Intel runner |
| macOS Apple Silicon | `.tar.gz` | built and smoke-tested on the macOS 15 ARM runner |
| Windows x86-64 MSVC | `.zip` | built and smoke-tested on the Windows 2025 runner |

The macOS and Windows artifacts are not code-signed in this RC. Their older-OS minimums have not
been independently hardware-tested. Each archive has a sibling `.sha256`; the release also contains
`SHA256SUMS` covering native archives and Python wheels.

After the prerelease is published, download both the archive and its sibling `.sha256` asset, then
verify and install a Unix archive:

```console
sha256sum --check meshquill-v0.1.0-rc.3-x86_64-unknown-linux-gnu.tar.gz.sha256
# macOS: shasum -a 256 -c meshquill-v0.1.0-rc.3-x86_64-apple-darwin.tar.gz.sha256
tar -xzf meshquill-v0.1.0-rc.3-x86_64-unknown-linux-gnu.tar.gz
cd meshquill-v0.1.0-rc.3-x86_64-unknown-linux-gnu
./bin/meshquill --version
mkdir -p "$HOME/.local/bin"
install -m 0755 bin/meshquill "$HOME/.local/bin/meshquill"
```

The archive also contains `share/man/man1` and `share/completions`. Copy those directories into a
location used by your shell/package manager, or generate them at any time:

```console
mkdir -p ~/.local/share/bash-completion/completions ~/.local/share/man/man1
meshquill completions bash > ~/.local/share/bash-completion/completions/meshquill
meshquill manpages ~/.local/share/man/man1
```

Add `$HOME/.local/bin` to `PATH` if your shell does not already do so. Zsh users can place
`_meshquill` in a directory on `fpath`; Fish uses
`~/.config/fish/completions/meshquill.fish`. PowerShell users can source `_meshquill.ps1` from
their profile.

On Windows, compare `(Get-FileHash -Algorithm SHA256 ARCHIVE.zip).Hash.ToLower()` with the first
field in `ARCHIVE.zip.sha256`, expand the zip, run `bin\meshquill.exe --version`, then put its `bin`
directory on `PATH`.

## Install with Cargo

Rust 1.88 or newer is supported. Install the current source checkout with:

```console
cargo install --path crates/meshquill-cli --locked
```

When [live status](https://github.com/sol-aeternum/meshquill/blob/main/STATUS.md) records that the
`v0.1.0-rc.3` tag exists, the tagged-source command is:

```console
cargo install --locked --git https://github.com/sol-aeternum/meshquill \
  --tag v0.1.0-rc.3 meshquill
```

Do not assume the unqualified crates.io `cargo install meshquill` route exists unless status records
that separate registry publication.

Linux build dependencies:

```console
# Debian/Ubuntu
sudo apt-get install build-essential pkg-config libdbus-1-dev libudev-dev

# Fedora
sudo dnf install gcc pkgconf-pkg-config dbus-devel systemd-devel
```

At runtime, Bluetooth discovery uses the platform Bluetooth service and serial discovery uses OS
device enumeration. See [troubleshooting](troubleshooting.md) for group membership, udev and
Bluetooth-service checks.

## Python wheel

For the currently available source checkout, use the source-build procedure in the
[Python SDK guide](python-sdk.md#install). After live status records a published RC3 GitHub
prerelease, download the wheel matching your platform and install that file directly:

```console
python -m pip install ./meshquill_sdk-0.1.0rc3-*.whl
python -c "import meshcore_sdk; print(meshcore_sdk.__version__)"
```

That wheel uses PyO3's `abi3-py39` interface and supports CPython 3.9+. Only if status separately
records the `0.1.0rc3` PyPI publication should the exact index install be used:

```console
python -m pip install --pre "meshquill-sdk==0.1.0rc3"
```

See the [Python SDK guide](python-sdk.md) for source builds and a complete smoke test.

## Isolated installation smoke test

To avoid relying on developer configuration, pass an isolated config and create the explicit demo
profile:

```console
tmp_config="$(mktemp -d)/config.toml"
meshquill --config "$tmp_config" --non-interactive \
  init --name demo --demo --set-default
meshquill --config "$tmp_config" device info
meshquill --config "$tmp_config" send Alice 'clean install' --wait
```

This verifies packaging and the deterministic client flow. It does not verify a physical radio.
