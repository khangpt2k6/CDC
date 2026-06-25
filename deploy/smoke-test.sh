#!/usr/bin/env bash
# Smoke test for the local CDC stack (ROADMAP Issue 0.1 acceptance criteria).
#
# Run after `docker compose up -d` (give Connect ~40s to finish booting):
#   bash deploy/smoke-test.sh
#
# Asserts all four services are healthy and that the externally reachable
# endpoints answer: Connect REST on 8083, ClickHouse SELECT 1 on both HTTP (8123)
# and native (9000), and Postgres reporting wal_level=logical. Exits non-zero on
# any failure so CI can gate on it.
set -euo pipefail

# Resolve the repo root from this script's location so it runs from anywhere,
# then cd there: the checks invoke `docker compose` relative to the compose file.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

# shellcheck source=deploy/lib/checks.sh
source "$SCRIPT_DIR/lib/checks.sh"

echo "== CDC stack smoke test =="

# Don't let a single failing check abort the run; collect all results so the
# output shows the full picture, then exit on the aggregate.
fail=0

require_healthy postgres   || fail=1
require_healthy kafka      || fail=1
require_healthy connect    || fail=1
require_healthy clickhouse || fail=1

check_connect_rest                                                          || fail=1
check_http "ClickHouse HTTP SELECT 1 (port 8123)" \
  "http://localhost:8123/?query=SELECT%201" "1"                             || fail=1
check_clickhouse_native                                                     || fail=1
check_pg_wal_level                                                          || fail=1

echo "=========================="
if [ "$fail" -ne 0 ]; then
  echo "smoke test FAILED" >&2
  exit 1
fi
echo "smoke test PASSED"
