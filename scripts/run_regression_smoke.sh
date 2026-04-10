#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-$ROOT_DIR/build/Release}"
DUCKDB_BIN="$BUILD_DIR/duckdb"

if [[ ! -x "$BUILD_DIR/test/unittest" ]]; then
  echo "unittest binary not found at: $BUILD_DIR/test/unittest" >&2
  exit 2
fi

if [[ ! -x "$DUCKDB_BIN" ]]; then
  echo "DuckDB binary not found at: $DUCKDB_BIN" >&2
  exit 2
fi

echo "==> sqllogictests"
"$ROOT_DIR/scripts/run_sqllogictests.sh" "$BUILD_DIR"

declare -a BENCHMARKS=(
  "shape_comparisons/core_direct_100k_probe_side.sql:20"
  "shape_comparisons/core_sparse_build_side.sql:20"
  "benchmarks/bench_varchar_dense.sql:45"
  "benchmarks/bench_probe_build_mixed.sql:45"
)

status=0
for entry in "${BENCHMARKS[@]}"; do
  sql_file="${entry%%:*}"
  timeout_seconds="${entry##*:}"
  log_file="/tmp/$(basename "${sql_file%.sql}").smoke.log"
  echo "==> $sql_file (${timeout_seconds}s timeout)"
  if ! "$ROOT_DIR/benchmarks/run_with_timeout.sh" \
      "$ROOT_DIR/$sql_file" \
      "$timeout_seconds" \
      "$log_file" \
      "$DUCKDB_BIN"; then
    status=1
  fi
done

exit "$status"
