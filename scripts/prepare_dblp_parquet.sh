#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-$ROOT_DIR/build/Release}"
DUCKDB_BIN="${DUCKDB_BIN:-$BUILD_DIR/duckdb}"
OUT_PARQUET="${2:-$ROOT_DIR/benchmarks/data/dblp.parquet}"
SRC_SPEC="${3:-https://raw.githubusercontent.com/arselzer/spark-eval/refs/heads/main/benchmark/snap/noheader/com-dblp.ungraph.txt}"

if [[ ! -x "$DUCKDB_BIN" ]]; then
  echo "DuckDB binary not found at: $DUCKDB_BIN" >&2
  exit 2
fi

if [[ -f "$OUT_PARQUET" ]]; then
  echo "dblp parquet already exists at: $OUT_PARQUET"
  exit 0
fi

RAW_FILE="$(mktemp /tmp/aggjoin_dblp_raw.XXXXXX.txt)"
SQL_FILE="$(mktemp /tmp/aggjoin_dblp_prepare.XXXXXX.sql)"
cleanup() {
  rm -f "$RAW_FILE" "$SQL_FILE"
}
trap cleanup EXIT

if [[ -f "$SRC_SPEC" ]]; then
  echo "Copying dblp edge list from local file: $SRC_SPEC"
  cp "$SRC_SPEC" "$RAW_FILE"
else
  echo "Downloading dblp edge list to: $RAW_FILE"
  curl -L --fail --retry 3 --retry-delay 2 "$SRC_SPEC" -o "$RAW_FILE"
fi

cat >"$SQL_FILE" <<SQL
COPY (
    SELECT fromNode, toNode
    FROM read_csv(
        '$RAW_FILE',
        auto_detect = false,
        delim = '\t',
        header = false,
        columns = {'fromNode': 'BIGINT', 'toNode': 'BIGINT'}
    )
) TO '$OUT_PARQUET' (FORMAT PARQUET, COMPRESSION ZSTD);
SQL

echo "Writing parquet to: $OUT_PARQUET"
"$DUCKDB_BIN" <"$SQL_FILE" >/dev/null
echo "Done."
