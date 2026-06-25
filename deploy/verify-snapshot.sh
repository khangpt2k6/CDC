#!/usr/bin/env bash
# End-to-end verification for ROADMAP Issue 2.1: confirm Debezium's initial
# snapshot (op "r") lands in ClickHouse and that streaming changes after it are
# neither lost nor duplicated.
#
# Usage:
#   bash deploy/verify-snapshot.sh            # verify against the current stack
#   bash deploy/verify-snapshot.sh --fresh    # cold start: down -v, then bring up
#
# The script drives the whole lifecycle: it brings the stack up, registers the
# Debezium connector, builds and runs the Go worker in the background, then:
#   AC#1  every pre-existing Postgres row appears in ClickHouse after snapshot.
#   AC#2  a live insert/update/delete on dedicated sentinel rows streams through
#         with no loss and no duplicates.
# It tears the worker down on exit and exits non-zero on any failure, so it is
# CI-gateable later (Issue 2.2 builds on this).
#
# --fresh runs `docker compose down -v`, which DROPS ALL VOLUMES (Postgres data,
# Kafka log, replication slot). It is the only destructive action and is opt-in.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

# shellcheck source=deploy/lib/verify.sh
source "$SCRIPT_DIR/lib/verify.sh"

COMPOSE=${COMPOSE:-docker compose}

# Tracked tables, mirroring clickhouse.Specs and the connector include.list.
TABLES=(customers orders)

# Sentinel identifiers for the streaming check. The email is unique so we can
# look the generated customer id back up; order id is GENERATED ALWAYS, so we
# never insert it explicitly and discover it after insert.
SENTINEL_EMAIL="verify-sentinel@example.com"

FRESH=0
if [ "${1:-}" = "--fresh" ] || [ "${RECREATE:-0}" = "1" ]; then
  FRESH=1
fi

WORKER_PID=""
WORKER_LOG="$REPO_ROOT/verify-worker.log"

# cleanup kills the background worker on any exit so reruns start clean. It does
# NOT tear the stack down — leaving it up makes a failed run debuggable.
cleanup() {
  if [ -n "$WORKER_PID" ] && kill -0 "$WORKER_PID" 2>/dev/null; then
    echo "stopping worker (pid $WORKER_PID) ..."
    kill "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

fail=0

echo "== CDC snapshot verification (ROADMAP Issue 2.1) =="

# ------------------------------------------------------------------------------
# 0. Lifecycle bring-up
# ------------------------------------------------------------------------------

if [ "$FRESH" -eq 1 ]; then
  echo "-- fresh start: dropping volumes --"
  $COMPOSE down -v
fi

echo "-- bringing up the stack --"
$COMPOSE up -d

echo "-- waiting for services to be healthy --"
for svc in postgres kafka connect clickhouse; do
  waited=0
  until [ "$($COMPOSE ps --format '{{.Health}}' "$svc" 2>/dev/null)" = "healthy" ]; do
    sleep 2
    waited=$((waited + 2))
    if [ "$waited" -ge 120 ]; then
      _fail "service '$svc' did not become healthy within 120s"
      exit 1
    fi
  done
  _pass "service '$svc' is healthy"
done

echo "-- registering the Debezium connector --"
bash "$SCRIPT_DIR/debezium/register-connector.sh" >/dev/null
_pass "connector registered"

echo "-- building and starting the worker --"
# Build first so the PID we trap is the worker itself, not a `go run` parent that
# would orphan its child binary on kill. Reuses the bin/worker convention.
if ! go build -o "$REPO_ROOT/bin/worker" ./cmd/worker; then
  _fail "go build ./cmd/worker"
  exit 1
fi
"$REPO_ROOT/bin/worker" >"$WORKER_LOG" 2>&1 &
WORKER_PID=$!
_pass "worker started (pid $WORKER_PID, log: $WORKER_LOG)"

# ------------------------------------------------------------------------------
# 1. Snapshot check (AC#1): every seeded row appears in ClickHouse
# ------------------------------------------------------------------------------

echo "-- AC#1: snapshot lands all pre-existing rows --"
declare -A baseline
for t in "${TABLES[@]}"; do
  want=$(pg_count "$t")
  baseline["$t"]=$want
  got=$(wait_for_ch_count "$t" "$want" 60) \
    && _pass "snapshot $t count" \
    || { _fail "snapshot $t: ClickHouse FINAL count '$got' != Postgres '$want' after 60s"; fail=1; }
done

# ------------------------------------------------------------------------------
# 2. Streaming check (AC#2): live insert/update/delete on sentinel rows
# ------------------------------------------------------------------------------

echo "-- AC#2: streaming picks up live changes with no loss or duplicates --"

# INSERT a sentinel customer, then discover its generated id.
pg_exec "INSERT INTO public.customers (email, full_name, country)
         VALUES ('$SENTINEL_EMAIL', 'Verify Sentinel', 'US')
         ON CONFLICT (email) DO NOTHING;" >/dev/null
cust_id=$(pg_query "SELECT id FROM public.customers WHERE email = '$SENTINEL_EMAIL'")

# INSERT a sentinel order for that customer, then discover its generated id.
pg_exec "INSERT INTO public.orders (customer_id, status, total_amount, currency)
         VALUES ($cust_id, 'pending', 10.00, 'USD');" >/dev/null
order_id=$(pg_query "SELECT id FROM public.orders
                     WHERE customer_id = $cust_id ORDER BY id DESC LIMIT 1")

# After the inserts, the live counts are baseline + 1 per table.
got=$(wait_for_ch_count customers "$((baseline[customers] + 1))" 30) \
  && _pass "streamed customer insert" \
  || { _fail "streamed customer insert: count '$got'"; fail=1; }
got=$(wait_for_ch_count orders "$((baseline[orders] + 1))" 30) \
  && _pass "streamed order insert" \
  || { _fail "streamed order insert: count '$got'"; fail=1; }

# UPDATE the sentinel order; the FINAL view must reflect the new values (and not
# duplicate the row — count must stay at baseline+1).
pg_exec "UPDATE public.orders
         SET status = 'paid', total_amount = 99.99, updated_at = now()
         WHERE id = $order_id;" >/dev/null
waited=0
status=""
while [ "$waited" -lt 30 ]; do
  status=$(ch_query "SELECT status FROM cdc.orders FINAL WHERE id = $order_id")
  [ "$status" = "paid" ] && break
  sleep 1; waited=$((waited + 1))
done
assert_eq "streamed order update (status)" "paid" "$status" || fail=1
amount=$(ch_query "SELECT total_amount FROM cdc.orders FINAL WHERE id = $order_id")
assert_eq "streamed order update (amount)" "99.99" "$amount" || fail=1
# No-duplicate proof: exactly one row for this key in the FINAL view.
rows=$(ch_query "SELECT count(*) FROM cdc.orders FINAL WHERE id = $order_id")
assert_eq "no duplicate rows for updated order" "1" "$rows" || fail=1

# DELETE the sentinel order; the row must drop out of the live (_is_deleted = 0)
# view and be marked deleted. Only the order was deleted (the sentinel customer
# still exists), so the live order count returns to baseline.
pg_exec "DELETE FROM public.orders WHERE id = $order_id;" >/dev/null
got=$(wait_for_ch_count orders "${baseline[orders]}" 30) \
  && _pass "streamed order delete (dropped from live view)" \
  || { _fail "streamed order delete: live order count '$got' != baseline '${baseline[orders]}'"; fail=1; }
deleted=$(ch_query "SELECT _is_deleted FROM cdc.orders FINAL WHERE id = $order_id")
assert_eq "deleted order marked _is_deleted=1" "1" "$deleted" || fail=1

# Clean up the sentinel customer so a rerun against the same volume starts clean.
pg_exec "DELETE FROM public.customers WHERE id = $cust_id;" >/dev/null

# ------------------------------------------------------------------------------
# Result
# ------------------------------------------------------------------------------

echo "=================================================="
if [ "$fail" -ne 0 ]; then
  echo "snapshot verification FAILED (worker log: $WORKER_LOG)" >&2
  exit 1
fi
echo "snapshot verification PASSED"
