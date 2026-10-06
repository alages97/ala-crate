#!/usr/bin/env bash
set -euo pipefail

readonly LIGHTER_GO_COMMIT="8854554703385766c73410b43e93075447f36a4c"
readonly DESTINATION="${1:-vendor/lighter-signer}"

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ "$DESTINATION" = /* ]]; then
  destination_path="$DESTINATION"
else
  destination_path="$project_root/$DESTINATION"
fi

mkdir -p "$destination_path"
checkout_dir="$(mktemp -d)"
trap 'rm -rf "$checkout_dir"' EXIT

git clone --quiet https://github.com/elliottech/lighter-go.git "$checkout_dir/lighter-go"
git -C "$checkout_dir/lighter-go" checkout --quiet "$LIGHTER_GO_COMMIT"

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)
    filename="lighter-signer-darwin-arm64.dylib"
    ;;
  Darwin-x86_64)
    filename="lighter-signer-darwin-amd64.dylib"
    ;;
  Linux-x86_64)
    filename="lighter-signer-linux-amd64.so"
    ;;
  Linux-aarch64|Linux-arm64)
    filename="lighter-signer-linux-arm64.so"
    ;;
  *)
    echo "unsupported local signer target: $(uname -s)-$(uname -m)" >&2
    exit 1
    ;;
esac

(
  cd "$checkout_dir/lighter-go"
  go mod vendor
  CGO_ENABLED=1 go build -buildmode=c-shared -trimpath \
    -o "$destination_path/$filename" ./sharedlib/main.go
)

if command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "$destination_path/$filename"
else
  sha256sum "$destination_path/$filename"
fi

echo "Built pinned official lighter-go signer at $destination_path/$filename"
