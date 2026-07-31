# `v0.1.0-rc.3` release runbook

This runbook is tied to the current [workspace manifest](../Cargo.toml),
[Python package metadata](../crates/meshquill-python/pyproject.toml), [CI workflow](../.github/workflows/ci.yml),
[supply-chain workflow](../.github/workflows/supply-chain.yml), and
[tag release workflow](../.github/workflows/release.yml). Run commands from the repository root in
a Bash shell unless a command says otherwise.

The release has two version spellings:

- Rust crates and the Git tag use `0.1.0-rc.3` and `v0.1.0-rc.3`.
- The PyPI distribution uses the PEP 440 version `0.1.0rc3`; its distribution name is
  `meshquill-sdk` and its import name is `meshcore_sdk`.

The tag workflow builds and tests artifacts and creates a **draft prerelease** on GitHub. It does not
publish to crates.io or PyPI, and it has no PyPI trusted-publisher job. Registry publication is a
separate, manual maintainer action.

This runbook describes the complete RC3 procedure, not current availability. Consult
[live status](https://github.com/sol-aeternum/meshquill/blob/main/STATUS.md) for the authoritative
state of the tag, draft, assets, registries, and public prerelease. A source checkout or packaged
copy of this runbook does not prove that any step has completed. A successful workflow draft is
private release staging, not a public downloadable delivery.

## 1. Credentials and release authority

Use a maintainer workstation with a clean checkout. GitHub authority is required for the GitHub
release; registry credentials are independently optional and gate only their respective uploads:

- Git/GitHub authority to push the tag, inspect Actions, edit the draft, and publish the GitHub
  release. Confirm the intended account and repository with `gh auth status` and `gh repo view`.
- A crates.io token authorized to publish new crates for the seven names listed below. Configure it
  through Cargo's credential-provider mechanism or an interactive `cargo login`; do not pass it with
  `--token` or place it in this repository. Revoke or narrow the initial token after the crate names
  exist.
- A PyPI API token authorized to create and publish `meshquill-sdk`, supplied to Maturin through
  `MATURIN_PYPI_TOKEN` from a secret manager. The first upload may require a short-lived
  account-scoped token because the project does not yet exist; replace it with a project-scoped token
  afterward. Do not put it in a command argument, file, release note, shell history, or Actions log;
  unset it immediately after upload.

The workflow's `GITHUB_TOKEN` has `contents: write` only in the draft-assembly job. No workflow job
has `id-token: write`, a crates.io token, or a PyPI token. Do not add registry credentials merely to
run this release; the implemented boundary is manual registry publication.

## 2. Freeze and pre-tag gates

Set the release constants and require the release commit to be the clean, pushed tip of `main`:

```bash
set -euo pipefail
export MESHQUILL_VERSION=0.1.0-rc.3
export MESHQUILL_TAG="v${MESHQUILL_VERSION}"
export MESHQUILL_SHA="$(git rev-parse HEAD)"

git fetch origin main --tags
test "$(git branch --show-current)" = main
test -z "$(git status --porcelain=v1)"
test "$(git rev-parse origin/main)" = "$MESHQUILL_SHA"
```

Verify the exact version contract enforced by the tag workflow:

```bash
test "$(sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -1)" = "$MESHQUILL_VERSION"
test "$(sed -n 's/^version = "\([^"]*\)"/\1/p' crates/meshquill-python/pyproject.toml | head -1)" = 0.1.0rc3
grep -Fqx "## [${MESHQUILL_VERSION}] - 2026-07-31" CHANGELOG.md
cargo metadata --locked --no-deps --format-version 1 >/dev/null
cargo metadata --locked --manifest-path fuzz/Cargo.toml --no-deps --format-version 1 >/dev/null
git diff --check
```

Before tagging, confirm that [CHANGELOG.md](../CHANGELOG.md), the
[live repository status](https://github.com/sol-aeternum/meshquill/blob/main/STATUS.md), the
[capability matrix](capability-matrix.md), and the [physical-hardware matrix](hardware-testing.md)
describe the same release commit. In particular, host-side or virtual-companion evidence must not be
reported as physical-radio evidence.

The authoritative pre-tag quality gates are successful `CI` and `Supply chain` runs for
`$MESHQUILL_SHA`. Inspect the exact runs, not merely the latest runs on the branch:

```bash
test "$(gh run list --workflow ci.yml --commit "$MESHQUILL_SHA" --limit 1 \
  --json conclusion --jq '.[0].conclusion')" = success
test "$(gh run list --workflow supply-chain.yml --commit "$MESHQUILL_SHA" --limit 1 \
  --json conclusion --jq '.[0].conclusion')" = success
```

Both conclusions must be `success`. Together they cover:

- Rust 1.97.1 formatting, warnings-denied Clippy, all-target/all-feature checking, default-feature
  tests, and warnings-denied documentation on Ubuntu, both macOS architectures, and Windows;
- the Rust 1.88.0 minimum-version check;
- all four fuzz targets on pinned nightly `2026-07-30`: the inner-packet and outer-frame codecs,
  remote payload parsers, and MQTT command parser;
- the lean Python 3.9 installed-wheel gate: import and Rust/Python version checks, licence
  inventory, `pip check`, and both examples;
- the full Python 3.14 gate: strict wheel metadata/Twine checks, formatting and lint, types and
  generated-API drift, Pytest, stubtest, and both examples;
- the real Mosquitto integration tests and Markdown link check; and
- `cargo deny` and `cargo audit` for both the main and fuzz lockfiles.

Do not substitute a single local host run for this matrix. A local archive made with
`scripts/package-release.sh` or `scripts/package-release.ps1` contains only the explicitly requested
target. Likewise, a local `maturin build` produces only a wheel for that build target. Those are
useful local smoke artifacts but are not the five native archives or five platform wheels assembled
by release CI, and they must not be uploaded as substitutes.

## 3. Create and push the annotated tag

Create an annotated tag object pointing at the already-gated commit, verify both the object type and
target, then push only that tag:

```bash
git tag --annotate --message "Meshquill $MESHQUILL_TAG" "$MESHQUILL_TAG" "$MESHQUILL_SHA"
test "$(git cat-file -t "$MESHQUILL_TAG")" = tag
test "$(git rev-parse "${MESHQUILL_TAG}^{commit}")" = "$MESHQUILL_SHA"
git push origin "refs/tags/$MESHQUILL_TAG"
```

The push triggers `release.yml`. Its validation job rejects a lightweight tag, a non-semantic tag,
a Rust/Python version mismatch, or a missing changelog heading. Find the resulting run and wait for
all jobs to pass:

```bash
gh run list --workflow release.yml --commit "$MESHQUILL_SHA" --event push --limit 5
export MESHQUILL_RUN_ID="$(gh run list --workflow release.yml --commit "$MESHQUILL_SHA" \
  --event push --limit 1 --json databaseId --jq '.[0].databaseId')"
test -n "$MESHQUILL_RUN_ID"
gh run watch "$MESHQUILL_RUN_ID" --exit-status
```

The release run reruns both quality workflows, then builds and smoke-tests these targets:

- native archives: Linux GNU x86-64 and ARM64, macOS Intel and Apple Silicon, and Windows x86-64
  MSVC;
- `abi3-py39` wheels for the same five platform/architecture combinations, built by Maturin 1.11.5;
  Linux wheels are additionally inspected with Auditwheel 6.7.0 and rejected when their actual
  symbol requirements are newer than `manylinux_2_28`, independent of the filename tag; and
- a draft GitHub release containing four `.tar.gz` files, one `.zip`, five sibling `.sha256` files,
  five wheels, and `SHA256SUMS`.

The Linux native smoke check also enforces a highest referenced glibc version no newer than 2.35.
Each target installs or extracts its own artifact and exercises the documented demo flow. Any failed
target blocks draft assembly.

If a job failed only because of a transient runner failure, inspect its log and use
`gh run rerun "$MESHQUILL_RUN_ID" --failed`. If source or packaging is defective, stop: do not move
or replace the tag and do not publish any registry package. Prepare a new release-candidate version.

## 4. Inspect the draft and CI artifacts

The workflow ends with `gh release create --draft`; it does not publish the release. Confirm that the
release is still a draft, then download exactly the artifacts produced by CI into a new directory:

```bash
gh release view "$MESHQUILL_TAG" \
  --json tagName,name,isDraft,isPrerelease,url,body,assets
export MESHQUILL_ARTIFACT_DIR="$(mktemp -d)"
gh release download "$MESHQUILL_TAG" --dir "$MESHQUILL_ARTIFACT_DIR"

native_archives=(
  meshquill-v0.1.0-rc.3-aarch64-apple-darwin.tar.gz
  meshquill-v0.1.0-rc.3-aarch64-unknown-linux-gnu.tar.gz
  meshquill-v0.1.0-rc.3-x86_64-apple-darwin.tar.gz
  meshquill-v0.1.0-rc.3-x86_64-pc-windows-msvc.zip
  meshquill-v0.1.0-rc.3-x86_64-unknown-linux-gnu.tar.gz
)
wheel_assets=(
  meshquill_sdk-0.1.0rc3-cp39-abi3-macosx_10_12_x86_64.whl
  meshquill_sdk-0.1.0rc3-cp39-abi3-macosx_11_0_arm64.whl
  meshquill_sdk-0.1.0rc3-cp39-abi3-manylinux_2_28_aarch64.whl
  meshquill_sdk-0.1.0rc3-cp39-abi3-manylinux_2_28_x86_64.whl
  meshquill_sdk-0.1.0rc3-cp39-abi3-win_amd64.whl
)
expected_assets=(SHA256SUMS "${native_archives[@]}" "${wheel_assets[@]}")
for archive in "${native_archives[@]}"; do
  expected_assets+=("${archive}.sha256")
done

test "${#expected_assets[@]}" -eq 16
test "$(find "$MESHQUILL_ARTIFACT_DIR" -maxdepth 1 -type f | wc -l)" -eq 16
for asset in "${expected_assets[@]}"; do
  test -s "$MESHQUILL_ARTIFACT_DIR/$asset"
done

checksum_payloads=("${native_archives[@]}" "${wheel_assets[@]}")
test "${#checksum_payloads[@]}" -eq 10
test "$(wc -l < "$MESHQUILL_ARTIFACT_DIR/SHA256SUMS")" -eq 10
for payload in "${checksum_payloads[@]}"; do
  test "$(awk -v expected="$payload" \
    '$2 == expected { matches++ } END { print matches + 0 }' \
    "$MESHQUILL_ARTIFACT_DIR/SHA256SUMS")" -eq 1
done

(
  cd "$MESHQUILL_ARTIFACT_DIR"
  sha256sum --check SHA256SUMS
  for archive in "${native_archives[@]}"; do
    checksum="${archive}.sha256"
    test "$(wc -l < "$checksum")" -eq 1
    test "$(awk 'NF == 2 { print $2 }' "$checksum")" = "$archive"
    sha256sum --check "$checksum"
  done
)
```

Inspect the Actions log, archive listings, wheel filenames/platform tags, generated notes, licences,
completions, man pages, and documented limitations. `SHA256SUMS` covers the five native archives and
five wheels; each native archive also has its own checksum file. These are integrity checksums, not
cryptographic signatures. The archives, wheels, macOS binary, and Windows binary are unsigned in
this RC.

Do not edit or replace CI-built assets after this check. A different byte sequence requires a new
version and workflow run.

## 5. Publish the Rust crates

The workspace contains seven crates intended for crates.io and one Rust package that is not:
`meshquill-python` has `publish = false` because it is delivered as the Python wheel. Internal Rust
dependencies use the exact requirement `=0.1.0-rc.3`, so publish in this order and wait for each
crate to appear in the crates.io index before publishing a dependent crate.

For every crate, the dry run and real publication must use the tagged, clean checkout. Do not use
`--allow-dirty` or `--no-verify`.

```bash
cargo publish --locked --registry crates-io --package meshquill-core --dry-run
cargo publish --locked --registry crates-io --package meshquill-core
cargo info --registry crates-io "meshquill-core@$MESHQUILL_VERSION"

cargo publish --locked --registry crates-io --package meshquill-hooks --dry-run
cargo publish --locked --registry crates-io --package meshquill-hooks
cargo info --registry crates-io "meshquill-hooks@$MESHQUILL_VERSION"

cargo publish --locked --registry crates-io --package meshquill-mqtt --dry-run
cargo publish --locked --registry crates-io --package meshquill-mqtt
cargo info --registry crates-io "meshquill-mqtt@$MESHQUILL_VERSION"

cargo publish --locked --registry crates-io --package meshquill-transport --dry-run
cargo publish --locked --registry crates-io --package meshquill-transport
cargo info --registry crates-io "meshquill-transport@$MESHQUILL_VERSION"

cargo publish --locked --registry crates-io --package meshquill-test-support --dry-run
cargo publish --locked --registry crates-io --package meshquill-test-support
cargo info --registry crates-io "meshquill-test-support@$MESHQUILL_VERSION"

cargo publish --locked --registry crates-io --package meshquill-store --dry-run
cargo publish --locked --registry crates-io --package meshquill-store
cargo info --registry crates-io "meshquill-store@$MESHQUILL_VERSION"

cargo publish --locked --registry crates-io --package meshquill --dry-run
cargo publish --locked --registry crates-io --package meshquill
cargo info --registry crates-io "meshquill@$MESHQUILL_VERSION"
```

`cargo info` is the propagation gate. If it cannot retrieve the exact version, wait and retry it;
do not continue to a crate that depends on that package. The dependency reasons for the ordering are:

- `meshquill-core` and `meshquill-hooks` have no internal dependencies;
- `meshquill-mqtt`, `meshquill-transport`, and `meshquill-test-support` depend on
  `meshquill-core`;
- `meshquill-store` depends on `meshquill-hooks` and `meshquill-mqtt`; and
- the `meshquill` CLI depends on all six component crates.

Once crates.io accepts a version, its contents are immutable. Do not continue after an unexpected
warning, ownership problem, partial publication, or packaged-content discrepancy; inspect what was
accepted and follow the recovery section.

After the final index gate, install the registry package—not the local path—under a temporary root
and run the deterministic smoke flow:

```bash
export MESHQUILL_CARGO_SMOKE="$(mktemp -d)"
cargo install --locked --version 0.1.0-rc.3 \
  --root "$MESHQUILL_CARGO_SMOKE/install" meshquill
export MESHQUILL_CARGO_SMOKE_CONFIG="$MESHQUILL_CARGO_SMOKE/config.toml"
"$MESHQUILL_CARGO_SMOKE/install/bin/meshquill" \
  --config "$MESHQUILL_CARGO_SMOKE_CONFIG" --non-interactive \
  init --name demo --demo --set-default
"$MESHQUILL_CARGO_SMOKE/install/bin/meshquill" \
  --config "$MESHQUILL_CARGO_SMOKE_CONFIG" device info
"$MESHQUILL_CARGO_SMOKE/install/bin/meshquill" \
  --config "$MESHQUILL_CARGO_SMOKE_CONFIG" send Alice 'registry smoke' --wait
```

## 6. Publish the CI wheels to PyPI

The tag workflow builds wheels but never uploads them. It builds no source distribution, so this RC's
PyPI release is the five downloaded CI wheels only. Install the same Maturin version used by the
release workflow, keep `MATURIN_PYPI_TOKEN` out of the command line, and upload the already-verified
wheel set:

```bash
python -m pip install "maturin==1.11.5"
test -n "${MATURIN_PYPI_TOKEN:-}"
test "$(find "$MESHQUILL_ARTIFACT_DIR" -maxdepth 1 -type f \
  -name 'meshquill_sdk-0.1.0rc3-*.whl' | wc -l)" -eq 5
maturin upload --non-interactive \
  "$MESHQUILL_ARTIFACT_DIR"/meshquill_sdk-0.1.0rc3-*.whl
unset MATURIN_PYPI_TOKEN
```

Do not use `maturin publish` here: it would build for the maintainer's current target instead of
uploading the five CI-produced wheels. There is no implemented OIDC/trusted-publishing path in this
repository; a configured PyPI trusted publisher alone does not change the workflow or authorize an
upload.

After PyPI's index sees the release, install it into a fresh environment and verify both version
spellings:

```bash
export MESHQUILL_PYPI_SMOKE="$(mktemp -d)"
python -m venv "$MESHQUILL_PYPI_SMOKE/venv"
"$MESHQUILL_PYPI_SMOKE/venv/bin/python" -m pip install \
  --pre "meshquill-sdk==0.1.0rc3"
"$MESHQUILL_PYPI_SMOKE/venv/bin/python" - <<'PY'
import importlib.metadata
import meshcore_sdk

assert importlib.metadata.version("meshquill-sdk") == "0.1.0rc3"
assert meshcore_sdk.__version__ == "0.1.0-rc.3"
PY
```

Index propagation can take time; a temporary install miss is a reason to wait and retry, not to
rebuild or upload differently named files.

## 7. Publish the GitHub release

Keep the GitHub release as a draft until the full tag run and artifact inspection pass. When
crates.io/PyPI credentials are available, complete their sequences and registry install checks
first. When either credential is unavailable, that is not a reason to withhold the tested GitHub
artifacts: state the unavailable registry explicitly in the notes and publish the GitHub prerelease.
Changing a private draft into a public release is an external disclosure boundary and requires an
explicit maintainer approval after the final asset/notes review; an earlier request to prepare or tag
the candidate is not that approval.
Review and, if necessary, replace the generated notes with an accurate summary of
[the changelog](../CHANGELOG.md), the lack of physical-hardware testing, the unsigned-artifact
boundary, and actual registry availability; use
`gh release edit "$MESHQUILL_TAG" --notes-file PATH` while it is a draft. Then publish it explicitly
as a prerelease, not as the latest stable release:

```bash
gh release edit "$MESHQUILL_TAG" \
  --verify-tag --prerelease --latest=false --draft=false
gh release view "$MESHQUILL_TAG" \
  --json tagName,isDraft,isPrerelease,publishedAt,url,assets
```

The final view must show `isDraft: false`, `isPrerelease: true`, the expected tag, and the inspected
asset inventory. Record the release workflow URL and registry URLs in the release evidence. Verify a
fresh download of the published GitHub archive against its sibling checksum.

## 8. Failure, rollback, and yanking

- Before the tag is pushed, fix the release commit and rerun every affected gate.
- After the tag is pushed, leave the GitHub release in draft until the tag workflow and artifact
  inspection pass. Rerun only demonstrably transient jobs against the same commit. For a code,
  version, metadata, or packaging defect, create a new RC; do not move or overwrite
  `v0.1.0-rc.3`. Missing registry credentials follow section 7 and do not change the tag or assets.
- Published crates and PyPI files cannot be replaced in place. Never upload locally rebuilt artifacts
  under the same version.
- For a crates.io defect, yank the affected public package and every published dependent. For a
  whole-release defect, yank in reverse dependency order:

  ```bash
  cargo yank --registry crates-io --version 0.1.0-rc.3 meshquill
  cargo yank --registry crates-io --version 0.1.0-rc.3 meshquill-store
  cargo yank --registry crates-io --version 0.1.0-rc.3 meshquill-test-support
  cargo yank --registry crates-io --version 0.1.0-rc.3 meshquill-transport
  cargo yank --registry crates-io --version 0.1.0-rc.3 meshquill-mqtt
  cargo yank --registry crates-io --version 0.1.0-rc.3 meshquill-hooks
  cargo yank --registry crates-io --version 0.1.0-rc.3 meshquill-core
  ```

  Yanking blocks new dependency resolution but does not erase downloads or existing lockfile use, so
  publish an advisory and a corrected RC.
- For a PyPI defect, use the PyPI project release page to yank `0.1.0rc3`; do not delete and recreate
  files. A yank does not remove already downloaded wheels.
- If the GitHub release is still a draft, keep it unpublished. If it is already public, preserve the
  tag and assets, add a prominent advisory to the notes, and publish a corrected RC. Do not silently
  replace assets.
- If any credential may have appeared in a terminal transcript, process argument, file, artifact, or
  log, stop publication, revoke it at the issuing service, review the exposure, and issue a new
  credential before continuing.

## Version support

Compatible schema additions may ship in a minor version. Wire behavior corrections, removals, or
machine-schema breaks require a new major schema/version and migration documentation. Firmware
compatibility is capability-negotiated where possible and otherwise listed by exact tested version.
