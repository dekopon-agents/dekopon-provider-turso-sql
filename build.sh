#!/usr/bin/env bash
# Build turso-sql-provider.wasm reproducibly.
#
# This is a self-contained port of dekopon's examples/providers/build-component.sh. The provider
# used to live in that tree and was built by that shared driver; the harness came with it, because
# provenance attestation proves who built an artifact while reproducibility proves what it was
# built from, and an 11 MB opaque SQL engine wants both.
#
# The metadata domain is deliberately still `dekopon-provider-repro-v1` — the same salt the
# in-tree build used. It already includes the package name, so it cannot collide with another
# provider, and keeping it means a component built here is byte-identical to the one the dekopon
# tree used to ship. That equality is the migration's proof; do not change this salt casually.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
manifest="$root/Cargo.toml"
target_root=${CARGO_TARGET_DIR:-"$root/target"}
mkdir -p "$target_root"
target_root=$(cd "$target_root" && pwd -P)
core="$target_root/wasm32-unknown-unknown/release/dekopon_turso_sql_provider.wasm"
component=${1:-"$root/turso-sql-provider.wasm"}

rust_toolchain="1.97.0"
required_rustc="rustc 1.97.0 (2d8144b78 2026-07-07)"
metadata_domain="dekopon-provider-repro-v1"
required_wasm_tools_version="1.236.1"

# Space-separated flags appended to the reproducibility set. turso_core reaches for entropy through
# three getrandom majors, none of which can be configured from Cargo.toml.
extra_rustflags=$(cat "$root/rustflags")

command -v rustup >/dev/null 2>&1 || {
  echo "error: rustup with Rust $rust_toolchain is required" >&2
  exit 1
}
if ! actual_rustc=$(rustup run "$rust_toolchain" rustc --version 2>/dev/null); then
  echo "error: Rust $rust_toolchain is required (rustup toolchain install $rust_toolchain --profile minimal)" >&2
  exit 1
fi
if [[ "$actual_rustc" != "$required_rustc" ]]; then
  echo "error: expected $required_rustc, found $actual_rustc" >&2
  exit 1
fi

command -v wasm-tools >/dev/null 2>&1 || {
  echo "error: wasm-tools $required_wasm_tools_version is required (cargo install wasm-tools --version $required_wasm_tools_version --locked)" >&2
  exit 1
}
actual_wasm_tools=$(wasm-tools --version)
actual_wasm_tools_version=${actual_wasm_tools#wasm-tools }
actual_wasm_tools_version=${actual_wasm_tools_version%% *}
if [[ "$actual_wasm_tools_version" != "$required_wasm_tools_version" ]]; then
  echo "error: expected wasm-tools $required_wasm_tools_version, found $actual_wasm_tools" >&2
  exit 1
fi

cargo_home=${CARGO_HOME:-"$HOME/.cargo"}
cargo_home=$(cd "$cargo_home" && pwd -P)
sysroot=$(rustup run "$rust_toolchain" rustc --print sysroot)
sysroot=$(cd "$sysroot" && pwd -P)
rustc_path=$(rustup which --toolchain "$rust_toolchain" rustc)
rustc_proxy="$target_root/deterministic-rustc"
cat >"$rustc_proxy" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

actual_rustc=${DEKOPON_BUILD_RUSTC:?}
source_root=${DEKOPON_BUILD_SOURCE_ROOT:?}
metadata_domain=${DEKOPON_BUILD_METADATA_DOMAIN:?}
manifest_dir=${CARGO_MANIFEST_DIR-}
repository_crate=false
if [[ "$manifest_dir" == "$source_root" || "$manifest_dir" == "$source_root/"* ]]; then
  repository_crate=true
fi

target=host
expect_target=false
for argument in "$@"; do
  if [[ "$expect_target" == true ]]; then
    target=$argument
    expect_target=false
    continue
  fi
  case $argument in
    --target)
      expect_target=true
      ;;
    --target=*)
      target=${argument#--target=}
      ;;
  esac
done

normalize_metadata=$repository_crate
if [[ "$target" == wasm32-unknown-unknown ]]; then
  normalize_metadata=true
fi

args=()
crate_name=
while (($#)); do
  case $1 in
    --crate-name)
      crate_name=$2
      args+=("$1" "$2")
      shift 2
      ;;
    --target)
      target=$2
      args+=("$1" "$2")
      shift 2
      ;;
    --target=*)
      target=${1#--target=}
      args+=("$1")
      shift
      ;;
    -C)
      if (($# >= 2)) && [[ $2 == metadata=* ]] && [[ "$normalize_metadata" == true ]]; then
        shift 2
      else
        args+=("$1")
        shift
      fi
      ;;
    -Cmetadata=*)
      if [[ "$normalize_metadata" == true ]]; then
        shift
      else
        args+=("$1")
        shift
      fi
      ;;
    *)
      args+=("$1")
      shift
      ;;
  esac
done

if [[ "$normalize_metadata" == true && -n "$crate_name" && -n "${CARGO_PKG_NAME-}" && -n "${CARGO_PKG_VERSION-}" ]]; then
  args+=(
    -C
    "metadata=$metadata_domain-${CARGO_PKG_NAME}-${CARGO_PKG_VERSION}-$crate_name-$target"
  )
fi
exec "$actual_rustc" "${args[@]}"
EOF
chmod 0700 "$rustc_proxy"

rustflags=(
  "--remap-path-prefix=$root=/dekopon/source"
  "--remap-path-prefix=$cargo_home=/dekopon/cargo"
  "--remap-path-prefix=$sysroot=/dekopon/rust/$rust_toolchain"
  '--cfg=dekopon_provider_repro_v1'
  '--check-cfg=cfg(dekopon_provider_repro_v1)'
  '-Ccodegen-units=1'
)
read -r -a extra_rustflags_array <<<"$extra_rustflags"
rustflags+=("${extra_rustflags_array[@]}")
encoded_rustflags=$(printf '%s\x1f' "${rustflags[@]}")
encoded_rustflags=${encoded_rustflags%$'\x1f'}

rustup target add --toolchain "$rust_toolchain" wasm32-unknown-unknown
CARGO_ENCODED_RUSTFLAGS="$encoded_rustflags" \
  DEKOPON_BUILD_RUSTC="$rustc_path" \
  DEKOPON_BUILD_SOURCE_ROOT="$root" \
  DEKOPON_BUILD_METADATA_DOMAIN="$metadata_domain" \
  RUSTC="$rustc_proxy" \
  CARGO_TARGET_DIR="$target_root" \
  rustup run "$rust_toolchain" cargo build \
  --locked --manifest-path "$manifest" --target wasm32-unknown-unknown --release
wasm-tools component new "$core" -o "$component"

for local_path in "$root" "$cargo_home" "$sysroot"; do
  if LC_ALL=C grep -aF -- "$local_path" "$component" >/dev/null; then
    echo "error: generated component embeds local build path: $local_path" >&2
    exit 1
  fi
done

( cd "$(dirname "$component")" && shasum -a 256 "$(basename "$component")" > "$(basename "$component").sha256" )
printf 'generated %s with Rust %s and remapped build paths\n' "$component" "$rust_toolchain"
