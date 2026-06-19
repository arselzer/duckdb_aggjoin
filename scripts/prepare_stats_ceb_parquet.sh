#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-$ROOT_DIR/build/Release}"
DUCKDB_BIN="${DUCKDB_BIN:-$BUILD_DIR/duckdb}"
OUT_DIR="${2:-$ROOT_DIR/benchmarks/data/stats_ceb}"
BASE_URL="${3:-https://raw.githubusercontent.com/Nathaniel-Han/End-to-End-CardEst-Benchmark/master/datasets/stats_simplified}"

if [[ ! -x "$DUCKDB_BIN" ]]; then
  echo "DuckDB binary not found at: $DUCKDB_BIN" >&2
  exit 2
fi

cat >&2 <<'EOF'
Preparing STATS-CEB simplified CSVs as parquet.

Note: the upstream benchmark repository does not appear to publish a LICENSE
file. Verify redistribution terms before committing the generated parquet files.
EOF

mkdir -p "$OUT_DIR"

TMP_DIR="$(mktemp -d /tmp/aggjoin_stats_ceb.XXXXXX)"
SQL_FILE="$(mktemp /tmp/aggjoin_stats_ceb_prepare.XXXXXX.sql)"
cleanup() {
  rm -rf "$TMP_DIR"
  rm -f "$SQL_FILE"
}
trap cleanup EXIT

tables=(
  badges
  comments
  postHistory
  postLinks
  posts
  tags
  users
  votes
)

: >"$SQL_FILE"
for table in "${tables[@]}"; do
  raw="$TMP_DIR/$table.csv"
  out="$OUT_DIR/$table.parquet"
  if [[ -f "$out" ]]; then
    echo "parquet already exists: $out"
    continue
  fi
  echo "Downloading $table.csv"
  curl -L --fail --retry 3 --retry-delay 2 "$BASE_URL/$table.csv" -o "$raw"
  cat >>"$SQL_FILE" <<SQL
COPY (
  SELECT *
  FROM read_csv_auto('$raw', header = true, sample_size = -1)
) TO '$out' (FORMAT PARQUET, COMPRESSION ZSTD);
SQL
done

if [[ ! -s "$SQL_FILE" ]]; then
  echo "Nothing to do."
  exit 0
fi

echo "Writing parquet files to: $OUT_DIR"
"$DUCKDB_BIN" <"$SQL_FILE" >/dev/null
echo "Done."
