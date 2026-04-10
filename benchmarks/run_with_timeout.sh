#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 4 ]]; then
  echo "Usage: $0 <sql-file> [timeout-seconds] [log-file] [duckdb-binary]" >&2
  exit 2
fi

sql_file="$1"
timeout_seconds="${2:-120}"
log_file="${3:-/tmp/$(basename "${sql_file%.sql}").timeout.log}"
duckdb_bin="${4:-build/Release/duckdb}"

if [[ ! -f "$sql_file" ]]; then
  echo "SQL file not found: $sql_file" >&2
  exit 2
fi

if [[ ! -x "$duckdb_bin" ]]; then
  echo "DuckDB binary not found or not executable: $duckdb_bin" >&2
  exit 2
fi

echo "Running $sql_file with timeout ${timeout_seconds}s"
echo "Log: $log_file"

set +e
/usr/bin/timeout "${timeout_seconds}s" "$duckdb_bin" < "$sql_file" > "$log_file" 2>&1
status=$?
set -e

case "$status" in
  0)
    echo "Completed successfully"
    ;;
  124)
    echo "Timed out after ${timeout_seconds}s"
    ;;
  137)
    echo "Killed with exit 137 (likely OOM/resource kill)"
    ;;
  *)
    echo "Exited with status $status"
    ;;
esac

exit "$status"
