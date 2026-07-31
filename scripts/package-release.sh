#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: package-release.sh <rust-target> <version> <output-directory>" >&2
  exit 2
fi

target=$1
version=$2
output_dir=$3

if [[ ! $target =~ ^[A-Za-z0-9_.-]+$ ]] || [[ ! $version =~ ^v?[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9.-]+)?$ ]]; then
  echo "target or version contains unsupported characters" >&2
  exit 2
fi

package_name="meshquill-${version}-${target}"
binary="target/${target}/dist/meshquill"

cargo build --locked --profile dist --target "$target" --package meshquill

mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd -P)
stage_parent=$(mktemp -d "${TMPDIR:-/tmp}/meshquill-release.XXXXXX")
trap 'rm -rf -- "$stage_parent"' EXIT
stage_root="${stage_parent}/${package_name}"

mkdir -p \
  "$stage_root/bin" \
  "$stage_root/share/man/man1" \
  "$stage_root/share/completions"
install -m 0755 "$binary" "$stage_root/bin/meshquill"
install -m 0644 \
  README.md CHANGELOG.md CONTRIBUTING.md SECURITY.md \
  LICENSE-APACHE LICENSE-MIT \
  "$stage_root/"
cp -R docs examples schemas "$stage_root/"

"$binary" completions bash >"$stage_root/share/completions/meshquill.bash"
"$binary" completions zsh >"$stage_root/share/completions/_meshquill"
"$binary" completions fish >"$stage_root/share/completions/meshquill.fish"
"$binary" completions powershell >"$stage_root/share/completions/_meshquill.ps1"
"$binary" manpages "$stage_root/share/man/man1"
test -s "$stage_root/share/completions/meshquill.bash"
test -s "$stage_root/share/completions/_meshquill"
test -s "$stage_root/share/completions/meshquill.fish"
test -s "$stage_root/share/completions/_meshquill.ps1"
test -s "$stage_root/share/man/man1/meshquill.1"

archive="${output_dir}/${package_name}.tar.gz"
tar -C "$stage_parent" -czf "$archive" "$package_name"

if command -v sha256sum >/dev/null 2>&1; then
  hash=$(sha256sum "$archive" | awk '{print $1}')
else
  hash=$(shasum -a 256 "$archive" | awk '{print $1}')
fi
printf '%s  %s\n' "$hash" "$(basename "$archive")" >"${archive}.sha256"
