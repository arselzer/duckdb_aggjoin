#!/usr/bin/env bash
#
# Build the aggjoin extension as a WASM loadable extension for DuckDB-WASM.
#
# Prerequisites:
#   - Emscripten 3.1.71 installed and activated (MUST be this version)
#   - DuckDB-WASM-compatible DuckDB source in ../duckdb-wasm/ (or set DUCKDB_DIR)
#
# Usage:
#   ./scripts/build_wasm.sh                    # Build + patch + deploy
#   ./scripts/build_wasm.sh --build-only       # Build only, no deploy
#   DUCKDB_DIR=path/to/duckdb ./scripts/build_wasm.sh
#
# Output:
#   build/wasm_eh/extension/aggjoin/aggjoin.duckdb_extension.wasm
#
# After building, the script:
#   1. Patches the aggjoin metadata footer (platform, version, ABI type)
#   2. Copies aggjoin plus standard wasm extensions to frontend/public/duckdb/extensions/v1.4.3/wasm_eh/
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
EXT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$EXT_DIR/.." && pwd)"
DUCKDB_DIR="${DUCKDB_DIR:-$EXT_DIR/duckdb-wasm}"
DUCKDB_VERSION="${DUCKDB_VERSION:-v1.4.3}"
BUILD_DIR="$DUCKDB_DIR/build/wasm_eh"
BUILD_ONLY=false

for arg in "$@"; do
  case $arg in
    --build-only) BUILD_ONLY=true ;;
  esac
done

# --- Preflight checks ---

if ! command -v emcc &>/dev/null; then
  echo "ERROR: emcc not found. Install Emscripten 3.1.71:"
  echo "  cd /tmp && git clone https://github.com/emscripten-core/emsdk.git"
  echo "  cd emsdk && ./emsdk install 3.1.71 && ./emsdk activate 3.1.71"
  echo "  source emsdk_env.sh"
  exit 1
fi

EMCC_VERSION=$(emcc --version 2>&1 | head -1 | grep -oP '\d+\.\d+\.\d+' || echo "unknown")
if [[ "$EMCC_VERSION" != "3.1.71" ]]; then
  echo "WARNING: Emscripten version is $EMCC_VERSION, expected 3.1.71"
  echo "  Newer versions use native i64, causing ABI mismatch with the JS worker."
  echo "  Install 3.1.71: cd /tmp/emsdk && ./emsdk install 3.1.71 && ./emsdk activate 3.1.71"
  read -p "Continue anyway? [y/N] " -n 1 -r
  echo
  [[ $REPLY =~ ^[Yy]$ ]] || exit 1
fi

if [[ ! -d "$DUCKDB_DIR" ]]; then
  echo "ERROR: DuckDB source not found at $DUCKDB_DIR"
  echo "  Clone it: git clone --depth 1 --branch $DUCKDB_VERSION https://github.com/duckdb/duckdb.git $DUCKDB_DIR"
  exit 1
fi

ACTUAL_DUCKDB_VERSION="$(git -C "$DUCKDB_DIR" describe --tags --exact-match 2>/dev/null || git -C "$DUCKDB_DIR" describe --tags --always 2>/dev/null || echo unknown)"
if [[ "$ACTUAL_DUCKDB_VERSION" != "$DUCKDB_VERSION" ]]; then
  echo "ERROR: DuckDB source version mismatch"
  echo "  Expected: $DUCKDB_VERSION"
  echo "  Found:    $ACTUAL_DUCKDB_VERSION"
  echo "  The browser extension must be compiled against the exact DuckDB-WASM ABI version."
  echo "  Use a separate checkout, e.g.:"
  echo "    git clone --depth 1 --branch $DUCKDB_VERSION https://github.com/duckdb/duckdb.git $EXT_DIR/duckdb-wasm"
  exit 1
fi

# --- Link extension into DuckDB source tree ---

mkdir -p "$DUCKDB_DIR/extension_external"
ln -sfn "$EXT_DIR" "$DUCKDB_DIR/extension_external/aggjoin"

# --- Force recompilation (cmake may skip if it thinks source is unchanged) ---

touch "$EXT_DIR/src/aggjoin_optimizer.cpp"

# --- Configure ---

echo "=== Configuring WASM build ==="
cd "$DUCKDB_DIR"

emcmake cmake -G "Unix Makefiles" \
  -DWASM_LOADABLE_EXTENSIONS=1 \
  -DBUILD_EXTENSIONS_ONLY=1 \
  -DBUILD_EXTENSIONS="aggjoin" \
  -Bbuild/wasm_eh \
  -DCMAKE_CXX_FLAGS="-fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1 -DDUCKDB_CUSTOM_PLATFORM=wasm_eh" \
  -DDUCKDB_EXPLICIT_PLATFORM="wasm_eh" \
  -DOVERRIDE_GIT_DESCRIBE="${DUCKDB_VERSION}-0-g$(git rev-parse --short HEAD)"

# --- Build ---

echo "=== Building WASM extension ==="
emmake make -j"$(nproc)" -Cbuild/wasm_eh

# --- Find output ---

OUTPUT="$BUILD_DIR/extension/aggjoin/aggjoin.duckdb_extension.wasm"
if [[ ! -f "$OUTPUT" ]]; then
  echo "ERROR: Expected output not found at $OUTPUT"
  echo "  Check build/wasm_eh/ for the actual output location."
  exit 1
fi

echo "=== Built: $OUTPUT ($(stat -c%s "$OUTPUT") bytes) ==="

# --- Patch metadata ---

echo "=== Patching extension metadata ==="
python3 "$SCRIPT_DIR/patch_metadata.py" "$OUTPUT" \
  --platform wasm_eh \
  --version "$DUCKDB_VERSION" \
  --abi CPP

# --- Deploy ---

if [[ "$BUILD_ONLY" == false ]]; then
  DEPLOY_DIR="$REPO_ROOT/frontend/public/duckdb/extensions/$DUCKDB_VERSION/wasm_eh"
  mkdir -p "$DEPLOY_DIR"
  cp "$OUTPUT" "$DEPLOY_DIR/"
  REPO_EXT_DIR="$BUILD_DIR/repository/$DUCKDB_VERSION/wasm_eh"
  if [[ -d "$REPO_EXT_DIR" ]]; then
    cp "$REPO_EXT_DIR/parquet.duckdb_extension.wasm" "$DEPLOY_DIR/"
    cp "$REPO_EXT_DIR/core_functions.duckdb_extension.wasm" "$DEPLOY_DIR/"
  fi
  echo "=== Deployed to $DEPLOY_DIR/aggjoin.duckdb_extension.wasm ==="
  echo ""
  echo "Next steps:"
  echo "  1. Test locally: cd frontend && npm run dev"
  echo "  2. Open SQL Console, run: SELECT * FROM duckdb_extensions() WHERE extension_name = 'aggjoin'"
  echo "  3. Commit: git add -f $DEPLOY_DIR/aggjoin.duckdb_extension.wasm"
fi
