#!/usr/bin/env bash
# End-to-end verification for ROADMAP Issue 3.1 / 3.2: the worker exposes the
# pipeline-health metrics on /metrics, and the provisioned Prometheus + Grafana
# come up healthy on a fresh stack.
#
# Usage:
#   bash deploy/verify-metrics.sh            # verify against the current stack
#   bash deploy/verify-metrics.sh --fresh    # cold start: down -v, then bring up
#
# It brings the stack up (now including prometheus + grafana), registers the
# Debezium connector, builds and runs the worker on the host, then asserts:
#   AC (3.1)  /metrics exposes throughput, lag, batch-latency, and error series.
#   AC (3.2)  Prometheus is healthy and scrapes the worker target UP; Grafana is
#             healthy with both provisioned datasources present.
# It stops the worker on exit and exits non-zero on any failure.
#
# HTTP checks run INSIDE the clickhouse container (so the host needs no curl):
#   - prometheus / grafana are reached by compose service name.
#   - the worker /metrics is on the HOST, reached at host.docker.internal:9100.
#
# --fresh runs `docker compose down -v`, which DROPS ALL VOLUMES. Opt-in only.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

# shellcheck source=deploy/lib/verify.sh
source "$SCRIPT_DIR/lib/verify.sh"

COMPOSE=${COMPOSE:-docker compose}

# Where the worker serves /metrics (CDC_METRICS_ADDR default), as seen from
# inside a container.
METRICS_URL="http://host.docker.internal:9100/metrics"

FRESH=0
if [ "${1:-}" = "--fresh" ] || [ "${RECREATE:-0}" = "1" ]; then
  FRESH=1
fi

WORKER_PID=""
WORKER_LOG="$REPO_ROOT/verify-worker.log"

cleanup() {
  if [ -n "$WORKER_PID" ] && kill -0 "$WORKER_PID" 2>/dev/null; then
    echo "stopping worker (pid $WORKER_PID) ..."
    kill "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

fail=0

echo "== CDC metrics & monitoring verification (ROADMAP Issue 3.1/3.2) =="

# ------------------------------------------------------------------------------
# 0. Lifecycle bring-up (stack now includes prometheus + grafana)
# ------------------------------------------------------------------------------

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
"$REPO_ROOT/bin/worker" >"$WORKER_LOG" 2>&1 &
WORKER_PID=$!
_pass "worker started (pid $WORKER_PID, log: $WORKER_LOG)"

# Give the worker a moment to bind /metrics and take its first lag sample.
sleep 3

# ------------------------------------------------------------------------------
# 1. Worker /metrics exposes the health series (Issue 3.1)
# ------------------------------------------------------------------------------

echo "-- AC (3.1): /metrics exposes throughput, lag, latency, errors --"
for metric in \
  cdc_events_consumed_total \
  cdc_rows_written_total \
  cdc_batches_flushed_total \
  cdc_flush_duration_seconds \
  cdc_buffered_rows \
  cdc_consumer_lag \
  cdc_dlq_total \
  cdc_sink_retries_total; do
  check_http "metric $metric present" "$METRICS_URL" "$metric" || fail=1
done

# ------------------------------------------------------------------------------
# 2. Prometheus is healthy and scrapes the worker target UP (Issue 3.2)
# ------------------------------------------------------------------------------

echo "-- AC (3.2): Prometheus healthy and scraping the worker --"
check_http "Prometheus /-/healthy" "http://prometheus:9090/-/healthy" "Prometheus" || fail=1

# Poll the targets API until the cdc-worker job reports health=up (the first
# scrape may not have happened yet).
waited=0
up=""
while [ "$waited" -lt 30 ]; do
  up=$($COMPOSE exec -T clickhouse wget -qO- \
    'http://prometheus:9090/api/v1/targets?state=active' 2>/dev/null \
    | grep -o '"job":"cdc-worker"[^}]*"health":"up"' | head -n1)
  [ -n "$up" ] && break
  sleep 2; waited=$((waited + 2))
done
if [ -n "$up" ]; then
  _pass "Prometheus target cdc-worker is UP"
else
  _fail "Prometheus target cdc-worker not UP within 30s (is the worker on :9100?)"
  fail=1
fi

# ------------------------------------------------------------------------------
# 3. Grafana is healthy with both provisioned datasources (Issue 3.2)
# ------------------------------------------------------------------------------

echo "-- AC (3.2): Grafana healthy with provisioned datasources --"
check_http "Grafana /api/health" "http://grafana:3000/api/health" '"database"' || fail=1
# Anonymous admin is enabled, so the datasources API answers unauthenticated.
ds=$($COMPOSE exec -T clickhouse wget -qO- "http://grafana:3000/api/datasources" 2>/dev/null)
printf '%s' "$ds" | grep -q '"type":"prometheus"' \
  && _pass "Grafana Prometheus datasource provisioned" \
  || { _fail "Grafana Prometheus datasource missing: '${ds:-<empty>}'"; fail=1; }
printf '%s' "$ds" | grep -q 'clickhouse' \
  && _pass "Grafana ClickHouse datasource provisioned" \
  || { _fail "Grafana ClickHouse datasource missing"; fail=1; }

# ------------------------------------------------------------------------------
# Result
# ------------------------------------------------------------------------------

echo "=================================================="
if [ "$fail" -ne 0 ]; then
  echo "metrics verification FAILED (worker log: $WORKER_LOG)" >&2
  exit 1
fi
echo "metrics verification PASSED"
