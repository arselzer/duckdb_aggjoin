#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-$ROOT_DIR/build/Release}"

if [[ ! -x "$BUILD_DIR/test/unittest" ]]; then
  echo "unittest binary not found at: $BUILD_DIR/test/unittest" >&2
  exit 1
fi

mapfile -t TEST_FILES < <(find "$ROOT_DIR/test/sql" -maxdepth 1 -name 'aggjoin_*.test' | sort)

if [[ ${#TEST_FILES[@]} -eq 0 ]]; then
  echo "no aggjoin sqllogictest files found under $ROOT_DIR/test/sql" >&2
  exit 1
fi

status=0
for test_file in "${TEST_FILES[@]}"; do
  echo "==> $test_file"
  if ! "$BUILD_DIR/test/unittest" "$test_file"; then
    status=1
  fi
done

exit "$status"
