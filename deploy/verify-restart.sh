#!/usr/bin/env bash
# End-to-end restart-survival test for ROADMAP Issue 2.2: prove the worker
# resumes cleanly after a crash under load, with no loss and no surviving
# duplicates.
#
# Usage:
#   bash deploy/verify-restart.sh            # against the current stack
#   bash deploy/verify-restart.sh --fresh    # cold start: down -v, then bring up
#
# The script drives the whole lifecycle: bring the stack up, register the
# Debezium connector, build the worker, start a background load generator hammering
# Postgres, then repeatedly `kill -9` and restart the worker mid-stream. After it
# drains it asserts:
#   AC#1  the worker resumes from the committed Kafka offset after each restart
#         (read directly from the consumer group) and lag drains to 0.
#   AC#2  the ClickHouse FINAL view matches Postgres exactly -- equal counts AND
#         an all-column content checksum -- so nothing was lost or duplicated.
# It stops the worker and generator on exit and exits non-zero on any failure.
#
# --fresh runs `docker compose down -v`, which DROPS ALL VOLUMES. It is the only
# destructive action and is opt-in.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

# shellcheck source=deploy/lib/verify.sh
source "$SCRIPT_DIR/lib/verify.sh"
# shellcheck source=deploy/lib/loadgen.sh
source "$SCRIPT_DIR/lib/loadgen.sh"

COMPOSE=${COMPOSE:-docker compose}

# Tracked tables, mirroring clickhouse.Specs and the connector include.list.
TABLES=(customers orders)

# The consumer group the worker joins (config.go default, CDC_KAFKA_GROUP).
GROUP=${CDC_KAFKA_GROUP:-cdc-clickhouse-sink}

# How many kill/restart cycles to run. More cycles -> higher chance a kill lands
# in the flush/commit gap, which is the window the no-loss contract protects.
CYCLES=${CYCLES:-3}

# Marker so every row this test creates is identifiable and removable, leaving
# the seed story intact for reruns against the same volume.
SENTINEL_EMAIL_PREFIX="restart-load"

FRESH=0
if [ "${1:-}" = "--fresh" ] || [ "${RECREATE:-0}" = "1" ]; then
  FRESH=1
fi

WORKER_PID=""
GEN_PID=""
WORKER_LOG="$REPO_ROOT/verify-restart-worker.log"

# cleanup stops both background processes on any exit so reruns start clean. It
# does NOT tear the stack down -- leaving it up makes a failed run debuggable.
cleanup() {
  if [ -n "$GEN_PID" ] && kill -0 "$GEN_PID" 2>/dev/null; then
    kill "$GEN_PID" 2>/dev/null || true
    wait "$GEN_PID" 2>/dev/null || true
  fi
  if [ -n "$WORKER_PID" ] && kill -0 "$WORKER_PID" 2>/dev/null; then
    kill "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

fail=0

echo "== CDC restart-survival verification (ROADMAP Issue 2.2) =="

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

echo "-- building the worker --"
if ! go build -o "$REPO_ROOT/bin/worker" ./cmd/worker; then
  _fail "go build ./cmd/worker"
  exit 1
fi
_pass "worker built (bin/worker)"

# start_worker launches bin/worker in the background and records its PID. Build
# is done once above, so the PID we trap/kill is the real worker, not a `go run`
# parent that would orphan its child on kill.
start_worker() {
  "$REPO_ROOT/bin/worker" >>"$WORKER_LOG" 2>&1 &
  WORKER_PID=$!
}

echo "-- starting the worker; waiting for the snapshot to settle --"
: >"$WORKER_LOG"
start_worker
for t in "${TABLES[@]}"; do
  got=$(wait_for_ch_count "$t" "$(pg_count "$t")" 60) \
    && _pass "baseline snapshot $t landed ($got rows)" \
    || { _fail "baseline snapshot $t did not settle (got '$got')"; fail=1; }
done

# ------------------------------------------------------------------------------
# 1. Background load generator: steady inserts/updates/deletes under sentinel ids
# ------------------------------------------------------------------------------
#
# generate_load (deploy/lib/loadgen.sh) runs c/u/d continuously under the sentinel
# prefix until killed by the EXIT trap, so a worker kill lands mid-stream. Errors
# are swallowed inside it, so a transient failure during a kill never aborts the
# generator; correctness is judged by the final parity check, not the generator.

echo "-- starting background load generator --"
generate_load "$SENTINEL_EMAIL_PREFIX" 0.2 &
GEN_PID=$!
_pass "load generator started (pid $GEN_PID)"

# ------------------------------------------------------------------------------
# 2. Kill / restart cycles -- the actual resilience test
# ------------------------------------------------------------------------------

echo "-- running $CYCLES kill/restart cycles under load --"
for c in $(seq 1 "$CYCLES"); do
  # Let the worker consume under load for a bit so there's committed progress
  # AND an uncommitted in-flight window when we kill it.
  sleep 4

  before=$(kafka_total_lag "$GROUP")
  committed_before=$(kafka_group_offsets "$GROUP" | awk '{ s += $3 } END { print s+0 }')

  # Hard crash: SIGKILL gives no chance to flush/commit, so this exercises the
  # replay path (graceful SIGTERM would flush and prove nothing about recovery).
  echo "  cycle $c: kill -9 worker (pid $WORKER_PID); committed=$committed_before lag=$before"
  kill -9 "$WORKER_PID" 2>/dev/null || true
  wait "$WORKER_PID" 2>/dev/null || true

  # Keep writing while down so a backlog builds and offsets must advance on resume.
  sleep 3

  echo "  cycle $c: restart worker"
  start_worker

  # Resume proof: the restarted worker must rejoin and its committed offset must
  # not go backwards (it resumes from >= where it left off, never re-commits older).
  sleep 5
  committed_after=$(kafka_group_offsets "$GROUP" | awk '{ s += $3 } END { print s+0 }')
  if [ "$committed_after" -ge "$committed_before" ]; then
    _pass "cycle $c: resumed from committed offset (>= $committed_before, now $committed_after)"
  else
    _fail "cycle $c: committed offset went backwards ($committed_before -> $committed_after)"
    fail=1
  fi
done

# Corroborating evidence: the worker logged a consumer-group (re)join at least
# once. franz-go logs group management at info; tolerate its absence (log level)
# rather than fail, since the offset check above is the authoritative proof.
if grep -qiE "joining group|assigned|balance|group" "$WORKER_LOG"; then
  _pass "worker log shows consumer-group activity on (re)join"
else
  echo "NOTE  no group-join line in worker log (log level may suppress it); relying on offset check" >&2
fi

# ------------------------------------------------------------------------------
# 3. Drain and assert final parity (AC#1 lag->0, AC#2 counts + checksum)
# ------------------------------------------------------------------------------

echo "-- stopping load generator and draining --"
kill "$GEN_PID" 2>/dev/null || true
wait "$GEN_PID" 2>/dev/null || true
GEN_PID=""

# Give the worker time to consume the final backlog, then confirm lag is zero.
waited=0
lag=$(kafka_total_lag "$GROUP")
while [ "$lag" != "0" ] && [ "$waited" -lt 60 ]; do
  sleep 2; waited=$((waited + 2)); lag=$(kafka_total_lag "$GROUP")
done
assert_eq "consumer lag drained to zero" "0" "$lag" || fail=1

echo "-- AC#2: ClickHouse FINAL view matches Postgres (count + content) --"
for t in "${TABLES[@]}"; do
  got=$(wait_for_parity "$t" 60) \
    && _pass "final count parity $t ($got rows)" \
    || { _fail "final count parity $t: ClickHouse '$got' != Postgres '$(pg_count "$t")'"; fail=1; }
  pg_ck=$(pg_checksum "$t")
  ch_ck=$(ch_checksum "$t")
  assert_eq "content checksum parity $t" "$pg_ck" "$ch_ck" || fail=1
done

# ------------------------------------------------------------------------------
# 4. Cleanup sentinel rows so a rerun against the same volume starts clean.
# ------------------------------------------------------------------------------

cleanup_load "$SENTINEL_EMAIL_PREFIX"

# ------------------------------------------------------------------------------
# Result
# ------------------------------------------------------------------------------

echo "=================================================="
if [ "$fail" -ne 0 ]; then
  echo "restart-survival verification FAILED (worker log: $WORKER_LOG)" >&2
  exit 1
fi
echo "restart-survival verification PASSED"
