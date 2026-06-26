#!/usr/bin/env bash
# One-command demo for the CDC pipeline (ROADMAP Issue 3.4).
#
# Usage:
#   bash deploy/demo.sh            # against the current stack
#   bash deploy/demo.sh --fresh    # cold start: down -v, then bring up
#
# It brings the whole stack up (Postgres, Kafka, Debezium, ClickHouse, Prometheus,
# Grafana), registers the Debezium connector, builds and runs the Go worker on the
# host, and starts a steady load generator writing to Postgres. Then it prints the
# dashboard URLs and stays running so you can watch:
#
#   - Grafana "CDC Analytics" panels move as Postgres rows change.
#   - Grafana "CDC Pipeline Health" show throughput/lag/latency from the worker.
#   - Prometheus scraping the worker target.
#
# Press Ctrl-C to stop: it tears down the worker + generator (and removes the
# sentinel rows the generator created), leaving the stack up for inspection.
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

# Pace between load rounds; raise for a calmer demo, lower to hammer harder.
PACE=${PACE:-0.5}
# Sentinel prefix so the demo's rows are removed cleanly on exit.
DEMO_PREFIX="demo-load"

FRESH=0
if [ "${1:-}" = "--fresh" ] || [ "${RECREATE:-0}" = "1" ]; then
  FRESH=1
fi

WORKER_PID=""
GEN_PID=""
WORKER_LOG="$REPO_ROOT/demo-worker.log"

cleanup() {
  echo ""
  echo "-- stopping demo --"
  if [ -n "$GEN_PID" ] && kill -0 "$GEN_PID" 2>/dev/null; then
    kill "$GEN_PID" 2>/dev/null || true
    wait "$GEN_PID" 2>/dev/null || true
  fi
  cleanup_load "$DEMO_PREFIX"
  if [ -n "$WORKER_PID" ] && kill -0 "$WORKER_PID" 2>/dev/null; then
    kill "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
  fi
  echo "demo stopped (stack left running; 'docker compose down -v' to remove it)"
}
trap cleanup EXIT INT TERM

echo "== CDC pipeline demo =="

if [ "$FRESH" -eq 1 ]; then
  echo "-- fresh start: dropping volumes --"
  $COMPOSE down -v
fi

echo "-- bringing up the stack --"
$COMPOSE up -d

echo "-- waiting for services to be healthy --"
for svc in postgres kafka connect clickhouse prometheus grafana; do
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
if ! go build -o "$REPO_ROOT/bin/worker" ./cmd/worker; then
  _fail "go build ./cmd/worker"
  exit 1
fi
: >"$WORKER_LOG"
"$REPO_ROOT/bin/worker" >>"$WORKER_LOG" 2>&1 &
WORKER_PID=$!
_pass "worker started (pid $WORKER_PID, log: $WORKER_LOG)"

echo "-- starting the load generator --"
generate_load "$DEMO_PREFIX" "$PACE" &
GEN_PID=$!
_pass "load generator started (pid $GEN_PID, pace ${PACE}s)"

cat <<EOF

==================================================
  CDC demo is running. Open:

    Grafana     http://localhost:3000   (CDC folder: Analytics + Pipeline Health)
    Prometheus  http://localhost:9090

  The load generator is writing inserts/updates/deletes to Postgres; watch the
  analytics panels move and consumer lag stay near zero on the health dashboard.

  Press Ctrl-C to stop (removes the demo rows; leaves the stack up).
==================================================

EOF

# Stay alive until interrupted; the EXIT trap handles teardown. If either
# background process dies, surface it and exit so the demo doesn't look alive
# while silently broken.
while true; do
  if ! kill -0 "$WORKER_PID" 2>/dev/null; then
    _fail "worker exited unexpectedly (see $WORKER_LOG)"
    exit 1
  fi
  if ! kill -0 "$GEN_PID" 2>/dev/null; then
    _fail "load generator exited unexpectedly"
    exit 1
  fi
  sleep 2
done
