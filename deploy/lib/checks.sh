#!/usr/bin/env bash
# Reusable smoke-test checks for the local CDC stack.
#
# Source this file; it defines functions only and has no side effects on load.
# Every function logs a PASS/FAIL line and returns non-zero on failure so callers
# can aggregate results and exit accordingly.
#
#   source "$(dirname "$0")/lib/checks.sh"
#   require_healthy clickhouse || fail=1
#
# All checks assume `docker compose` is run from the repo root (where the
# compose file lives), matching how the runner invokes them.

# COMPOSE is the compose command; override to e.g. "docker-compose" if needed.
COMPOSE=${COMPOSE:-docker compose}

# _pass / _fail emit a uniform, greppable result line.
_pass() { printf 'PASS  %s\n' "$1"; }
_fail() { printf 'FAIL  %s\n' "$1" >&2; }

# require_healthy <service> asserts the compose service reports "healthy".
# Uses the --format flag so we read state without parsing the table layout.
require_healthy() {
  local svc=$1 health
  health=$($COMPOSE ps --format '{{.Health}}' "$svc" 2>/dev/null)
  if [ "$health" = "healthy" ]; then
    _pass "service '$svc' is healthy"
    return 0
  fi
  _fail "service '$svc' health is '${health:-unknown}', want 'healthy'"
  return 1
}

# check_http <label> <url> <expected-substring> asserts an HTTP GET (run inside
# the clickhouse container so the host needs no curl/wget) contains the string.
# The request targets the host-published port, proving external reachability.
check_http() {
  local label=$1 url=$2 want=$3 body
  body=$($COMPOSE exec -T clickhouse wget -qO- "$url" 2>/dev/null)
  if printf '%s' "$body" | grep -q "$want"; then
    _pass "$label ($url)"
    return 0
  fi
  _fail "$label ($url): response '${body:-<empty>}' missing '$want'"
  return 1
}

# check_clickhouse_native runs SELECT 1 through clickhouse-client, which speaks
# the native protocol on port 9000 — proving that port, not just HTTP 8123.
check_clickhouse_native() {
  local out
  out=$($COMPOSE exec -T clickhouse clickhouse-client --query "SELECT 1" 2>/dev/null)
  if [ "$out" = "1" ]; then
    _pass "ClickHouse native SELECT 1 (port 9000)"
    return 0
  fi
  _fail "ClickHouse native SELECT 1: got '${out:-<empty>}', want '1'"
  return 1
}

# check_connect_rest asserts the Debezium REST API answers on 8083. A fresh
# stack has no connectors, so the endpoint returns an empty JSON array.
check_connect_rest() {
  local out
  out=$($COMPOSE exec -T connect curl -sf http://localhost:8083/connectors 2>/dev/null)
  if [ "$out" = "[]" ]; then
    _pass "Kafka Connect REST API (port 8083)"
    return 0
  fi
  _fail "Kafka Connect REST /connectors: got '${out:-<empty>}', want '[]'"
  return 1
}

# check_pg_wal_level asserts Postgres is configured for logical replication.
check_pg_wal_level() {
  local out
  out=$($COMPOSE exec -T postgres psql -U cdc -d cdc -tAc "SHOW wal_level" 2>/dev/null)
  if [ "$out" = "logical" ]; then
    _pass "Postgres wal_level=logical"
    return 0
  fi
  _fail "Postgres wal_level: got '${out:-<empty>}', want 'logical'"
  return 1
}
