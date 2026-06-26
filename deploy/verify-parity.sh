#!/usr/bin/env bash
# Source-to-sink parity test for ROADMAP Issue 2.3: an automated check that the
# ClickHouse current-state view faithfully reflects Postgres after a deterministic
# mix of inserts, updates, and deletes -- so correctness is provable, not assumed.
#
# Usage:
#   bash deploy/verify-parity.sh            # against the current stack
#   bash deploy/verify-parity.sh --fresh    # cold start: down -v, then bring up
#
# Unlike verify-restart.sh (Issue 2.2), which throws random chaos at the worker,
# this applies a FIXED, repeatable workload with hard-coded values so the end
# state is exactly known and the run is reproducible. It then:
#   1. waits for the pipeline to drain (consumer lag -> 0),
#   2. settles ClickHouse merges (OPTIMIZE TABLE ... FINAL), and
#   3. asserts, per table:
#        AC#1  row counts match (pg_count == ch_count_final), and
#        AC#2  an all-column content checksum matches (pg_checksum == ch_checksum)
#      -- the latter catching dropped updates/deletes that an equal count hides.
# It stops the worker on exit and exits non-zero on any failure (CI-gateable).
#
# --fresh runs `docker compose down -v`, which DROPS ALL VOLUMES. It is the only
# destructive action and is opt-in.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

# shellcheck source=deploy/lib/verify.sh
source "$SCRIPT_DIR/lib/verify.sh"

COMPOSE=${COMPOSE:-docker compose}

# Tracked tables, mirroring clickhouse.Specs and the connector include.list.
TABLES=(customers orders)

# The consumer group the worker joins (config.go default, CDC_KAFKA_GROUP).
GROUP=${CDC_KAFKA_GROUP:-cdc-clickhouse-sink}

# Marker so every row this test creates is identifiable and removable, leaving
# the seed story intact for reruns against the same volume.
SENTINEL_EMAIL_PREFIX="parity"

# Deterministic workload shape (small, per plan): N rows inserted, the first
# UPDATE_N updated, the first DELETE_N deleted. Fixed so the end state -- and thus
# both checksums -- is identical on every run.
ROWS=10
UPDATE_N=5
DELETE_N=2

FRESH=0
if [ "${1:-}" = "--fresh" ] || [ "${RECREATE:-0}" = "1" ]; then
  FRESH=1
fi

WORKER_PID=""
WORKER_LOG="$REPO_ROOT/verify-parity-worker.log"

# cleanup stops the background worker on any exit so reruns start clean. It does
# NOT tear the stack down -- leaving it up makes a failed run debuggable.
cleanup() {
  if [ -n "$WORKER_PID" ] && kill -0 "$WORKER_PID" 2>/dev/null; then
    echo "stopping worker (pid $WORKER_PID) ..."
    kill "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

fail=0

echo "== CDC source-to-sink parity verification (ROADMAP Issue 2.3) =="

# ------------------------------------------------------------------------------
# 0. Lifecycle bring-up (mirrors verify-snapshot.sh / verify-restart.sh)
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
# Build first so the trapped PID is the worker itself, not a `go run` parent that
# would orphan its child on kill. Reuses the bin/worker convention.
if ! go build -o "$REPO_ROOT/bin/worker" ./cmd/worker; then
  _fail "go build ./cmd/worker"
  exit 1
fi
: >"$WORKER_LOG"
"$REPO_ROOT/bin/worker" >"$WORKER_LOG" 2>&1 &
WORKER_PID=$!
_pass "worker started (pid $WORKER_PID, log: $WORKER_LOG)"

echo "-- waiting for the baseline snapshot to settle --"
for t in "${TABLES[@]}"; do
  got=$(wait_for_ch_count "$t" "$(pg_count "$t")" 60) \
    && _pass "baseline snapshot $t landed ($got rows)" \
    || { _fail "baseline snapshot $t did not settle (got '$got')"; fail=1; }
done

# ------------------------------------------------------------------------------
# 1. Deterministic workload: a fixed mix of inserts, updates, and deletes.
# ------------------------------------------------------------------------------
#
# Every value is hard-coded as a function of the row index, so re-running against
# the same volume (after the cleanup at the end) reproduces the exact same end
# state and therefore the exact same checksums. All rows are tagged with
# SENTINEL_EMAIL_PREFIX so the cleanup step is exact and the seed rows are left
# untouched.

# insert_workload creates ROWS sentinel customers, each with one 'pending' order
# of a fixed amount. id is GENERATED ALWAYS, so we key orders off the customer's
# email (unique) rather than a guessed id.
insert_workload() {
  local i email
  for i in $(seq 1 "$ROWS"); do
    email="${SENTINEL_EMAIL_PREFIX}-${i}@example.com"
    pg_exec "INSERT INTO public.customers (email, full_name, country)
             VALUES ('$email', 'Parity $i', 'US')
             ON CONFLICT (email) DO NOTHING;" >/dev/null
    pg_exec "INSERT INTO public.orders (customer_id, status, total_amount, currency)
             SELECT id, 'pending', 100.00 + $i, 'USD'
             FROM public.customers WHERE email = '$email';" >/dev/null
  done
}

# update_workload updates the first UPDATE_N sentinel orders to fixed 'paid'
# values, exercising the update path the content checksum (not the count) proves.
update_workload() {
  local i email
  for i in $(seq 1 "$UPDATE_N"); do
    email="${SENTINEL_EMAIL_PREFIX}-${i}@example.com"
    pg_exec "UPDATE public.orders
             SET status = 'paid', total_amount = 200.00 + $i, updated_at = now()
             WHERE customer_id = (SELECT id FROM public.customers WHERE email = '$email');" \
             >/dev/null
  done
}

# delete_workload deletes the first DELETE_N sentinel orders, exercising the
# tombstone path (AC: "after updates AND deletes"). The customers stay, so the
# end state is a known asymmetry: ROWS customers, ROWS-DELETE_N sentinel orders.
delete_workload() {
  local i email
  for i in $(seq 1 "$DELETE_N"); do
    email="${SENTINEL_EMAIL_PREFIX}-${i}@example.com"
    pg_exec "DELETE FROM public.orders
             WHERE customer_id = (SELECT id FROM public.customers WHERE email = '$email');" \
             >/dev/null
  done
}

echo "-- applying deterministic workload (insert $ROWS, update $UPDATE_N, delete $DELETE_N) --"
insert_workload
update_workload
delete_workload
_pass "workload applied"

# ------------------------------------------------------------------------------
# 2. Wait for the pipeline to drain (consumer lag -> 0).
# ------------------------------------------------------------------------------

echo "-- draining: waiting for consumer lag to reach zero --"
waited=0
lag=$(kafka_total_lag "$GROUP")
while [ "$lag" != "0" ] && [ "$waited" -lt 60 ]; do
  sleep 2; waited=$((waited + 2)); lag=$(kafka_total_lag "$GROUP")
done
assert_eq "consumer lag drained to zero" "0" "$lag" || fail=1

# ------------------------------------------------------------------------------
# 3. Settle ClickHouse merges before comparison (Issue 2.3 tech stack).
# ------------------------------------------------------------------------------

echo "-- settling ClickHouse merges (OPTIMIZE TABLE ... FINAL) --"
for t in "${TABLES[@]}"; do
  ch_optimize_final "$t"
done

# ------------------------------------------------------------------------------
# 4. Assert parity: AC#1 row counts, AC#2 content checksum, per table.
# ------------------------------------------------------------------------------

echo "-- AC#1 + AC#2: ClickHouse FINAL view matches Postgres (count + content) --"
for t in "${TABLES[@]}"; do
  got=$(wait_for_parity "$t" 60) \
    && _pass "row count parity $t ($got rows)" \
    || { _fail "row count parity $t: ClickHouse '$got' != Postgres '$(pg_count "$t")'"; fail=1; }
  pg_ck=$(pg_checksum "$t")
  ch_ck=$(ch_checksum "$t")
  assert_eq "content checksum parity $t" "$pg_ck" "$ch_ck" || fail=1
done

# ------------------------------------------------------------------------------
# 5. Cleanup sentinel rows so a rerun against the same volume starts clean.
# ------------------------------------------------------------------------------

pg_exec "DELETE FROM public.orders
         WHERE customer_id IN (SELECT id FROM public.customers
                               WHERE email LIKE '${SENTINEL_EMAIL_PREFIX}-%');" >/dev/null 2>&1
pg_exec "DELETE FROM public.customers
         WHERE email LIKE '${SENTINEL_EMAIL_PREFIX}-%';" >/dev/null 2>&1

# ------------------------------------------------------------------------------
# Result
# ------------------------------------------------------------------------------

echo "=================================================="
if [ "$fail" -ne 0 ]; then
  echo "source-to-sink parity verification FAILED (worker log: $WORKER_LOG)" >&2
  exit 1
fi
echo "source-to-sink parity verification PASSED"
