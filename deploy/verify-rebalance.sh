#!/usr/bin/env bash
# Rebalance-survival test for ROADMAP Issue 5.1 (khangpt2k6/Slipstream_CDC#102):
# prove that with 3 worker instances in one consumer group over 6-partition
# topics, killing a worker mid-load triggers a rebalance to the survivors and the
# pipeline still drains with no loss and no surviving duplicates.
#
# Usage:
#   bash deploy/verify-rebalance.sh            # against the current stack
#   bash deploy/verify-rebalance.sh --fresh    # cold start: down -v, then bring up
#
# --fresh runs `docker compose down -v`, which DROPS ALL VOLUMES. Opt-in only.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

# shellcheck source=deploy/lib/verify.sh
source "$SCRIPT_DIR/lib/verify.sh"
# shellcheck source=deploy/lib/loadgen.sh
source "$SCRIPT_DIR/lib/loadgen.sh"

COMPOSE=${COMPOSE:-docker compose}
TABLES=(customers orders)
GROUP=${CDC_KAFKA_GROUP:-cdc-clickhouse-sink}
SENTINEL_EMAIL_PREFIX="rebalance-load"
VICTIM=${VICTIM:-worker2}

FRESH=0
if [ "${1:-}" = "--fresh" ] || [ "${RECREATE:-0}" = "1" ]; then
  FRESH=1
fi

GEN_PID=""
cleanup() {
  if [ -n "$GEN_PID" ] && kill -0 "$GEN_PID" 2>/dev/null; then
    kill "$GEN_PID" 2>/dev/null || true
    wait "$GEN_PID" 2>/dev/null || true
  fi
  # Make sure the victim is back up so the stack is left in a consistent state.
  $COMPOSE start "$VICTIM" >/dev/null 2>&1 || true
}
trap cleanup EXIT

fail=0
echo "== CDC rebalance-survival verification (Issue 5.1) =="

if [ "$FRESH" -eq 1 ]; then
  echo "-- fresh start: dropping volumes --"
  $COMPOSE down -v
fi

echo "-- bringing up the stack (incl. worker1/2/3) --"
$COMPOSE up -d --build

echo "-- waiting for infra to be healthy --"
for svc in postgres kafka connect clickhouse; do
  waited=0
  until [ "$($COMPOSE ps --format '{{.Health}}' "$svc" 2>/dev/null)" = "healthy" ]; do
    sleep 2; waited=$((waited + 2))
    if [ "$waited" -ge 120 ]; then _fail "service '$svc' not healthy in 120s"; exit 1; fi
  done
  _pass "service '$svc' is healthy"
done

echo "-- registering the Debezium connector (creates 6-partition topics) --"
bash "$SCRIPT_DIR/debezium/register-connector.sh" >/dev/null
_pass "connector registered"

echo "-- AC: topics have 6 partitions --"
for t in "${TABLES[@]}"; do
  got=$(kafka_partition_count "cdc.public.$t")
  assert_eq "partition count cdc.public.$t" "6" "$got" || fail=1
done

echo "-- waiting for all 3 workers to join the group --"
waited=0
until [ "$(kafka_group_members "$GROUP")" = "3" ]; do
  sleep 2; waited=$((waited + 2))
  if [ "$waited" -ge 90 ]; then _fail "group did not reach 3 members in 90s"; break; fi
done
assert_eq "group members before kill" "3" "$(kafka_group_members "$GROUP")" || fail=1
assert_eq "assigned partitions before kill" "12" "$(kafka_assigned_partitions_total "$GROUP")" || fail=1

echo "-- baseline snapshot parity --"
for t in "${TABLES[@]}"; do
  got=$(wait_for_ch_count "$t" "$(pg_count "$t")" 60) \
    && _pass "baseline snapshot $t ($got rows)" \
    || { _fail "baseline snapshot $t did not settle (got '$got')"; fail=1; }
done

echo "-- starting background load across many keys --"
generate_load "$SENTINEL_EMAIL_PREFIX" 0.2 &
GEN_PID=$!
_pass "load generator started (pid $GEN_PID)"
sleep 5

echo "-- killing $VICTIM mid-load (SIGKILL) --"
$COMPOSE kill "$VICTIM" >/dev/null 2>&1 || true

echo "-- AC: group rebalances to 2 members still covering all 12 partitions --"
waited=0
until [ "$(kafka_group_members "$GROUP")" = "2" ]; do
  sleep 2; waited=$((waited + 2))
  if [ "$waited" -ge 60 ]; then break; fi
done
assert_eq "group members after kill" "2" "$(kafka_group_members "$GROUP")" || fail=1
assert_eq "assigned partitions after kill" "12" "$(kafka_assigned_partitions_total "$GROUP")" || fail=1

echo "-- keep writing while down, then restart $VICTIM --"
sleep 5
$COMPOSE start "$VICTIM" >/dev/null 2>&1 || true
waited=0
until [ "$(kafka_group_members "$GROUP")" = "3" ]; do
  sleep 2; waited=$((waited + 2))
  if [ "$waited" -ge 90 ]; then break; fi
done
assert_eq "group members after restart" "3" "$(kafka_group_members "$GROUP")" || fail=1

echo "-- stop load and drain --"
kill "$GEN_PID" 2>/dev/null || true
wait "$GEN_PID" 2>/dev/null || true
GEN_PID=""

waited=0
lag=$(kafka_total_lag "$GROUP")
while [ "$lag" != "0" ] && [ "$waited" -lt 90 ]; do
  sleep 2; waited=$((waited + 2)); lag=$(kafka_total_lag "$GROUP")
done
assert_eq "consumer lag drained to zero" "0" "$lag" || fail=1

echo "-- AC: final parity (count + checksum, zero surviving duplicates) --"
for t in "${TABLES[@]}"; do
  got=$(wait_for_parity "$t" 60) \
    && _pass "final count parity $t ($got rows)" \
    || { _fail "final count parity $t: ClickHouse '$got' != Postgres '$(pg_count "$t")'"; fail=1; }
  pg_ck=$(pg_checksum "$t")
  ch_ck=$(ch_checksum "$t")
  assert_eq "content checksum parity $t" "$pg_ck" "$ch_ck" || fail=1
  raw=$(ch_query "SELECT count(*) FROM cdc.$t")
  distinct=$(ch_query "SELECT uniqExact(id) FROM cdc.$t")
  assert_eq "no duplicate rows $t (raw==distinct)" "$raw" "$distinct" || fail=1
done

cleanup_load "$SENTINEL_EMAIL_PREFIX"

echo "=================================================="
if [ "$fail" -ne 0 ]; then
  echo "rebalance-survival verification FAILED" >&2
  exit 1
fi
echo "rebalance-survival verification PASSED"
