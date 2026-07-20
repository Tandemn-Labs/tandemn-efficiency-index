#!/bin/sh

set -eu

tei_os=$(uname -s)
case "$tei_os" in
  Darwin) tei_os=darwin ;;
  Linux) tei_os=linux ;;
  *) echo "Unsupported operating system: $tei_os" >&2; exit 1 ;;
esac

tei_arch=$(uname -m)
case "$tei_arch" in
  x86_64 | amd64) tei_arch=amd64 ;;
  arm64 | aarch64) tei_arch=arm64 ;;
  *) echo "Unsupported CPU architecture: $tei_arch" >&2; exit 1 ;;
esac

tei_asset="tei-${tei_os}-${tei_arch}.tar.gz"
tei_base_url="https://github.com/Tandemn-Labs/tandemn-efficiency-index/releases/latest/download"
tei_install_dir=${TEI_INSTALL_DIR:-"$HOME/.local/bin"}
tei_tmp_dir=$(mktemp -d)
trap 'rm -rf "$tei_tmp_dir"' 0 1 2 3 15

curl -fsSL "$tei_base_url/$tei_asset" -o "$tei_tmp_dir/$tei_asset"
curl -fsSL "$tei_base_url/checksums.txt" -o "$tei_tmp_dir/checksums.txt"

tei_expected=$(awk -v asset="$tei_asset" '$2 == asset { print $1 }' "$tei_tmp_dir/checksums.txt")
if [ -z "$tei_expected" ]; then
  echo "No checksum found for $tei_asset" >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  tei_actual=$(sha256sum "$tei_tmp_dir/$tei_asset" | awk '{ print $1 }')
else
  tei_actual=$(shasum -a 256 "$tei_tmp_dir/$tei_asset" | awk '{ print $1 }')
fi

if [ "$tei_expected" != "$tei_actual" ]; then
  echo "Checksum verification failed for $tei_asset" >&2
  exit 1
fi

tar -xzf "$tei_tmp_dir/$tei_asset" -C "$tei_tmp_dir"
mkdir -p "$tei_install_dir"
install -m 0755 "$tei_tmp_dir/tei" "$tei_install_dir/tei"

echo "Installed tei to $tei_install_dir/tei"
