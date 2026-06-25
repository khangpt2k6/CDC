#!/usr/bin/env bash
# Reusable data-verification checks for the CDC pipeline (ROADMAP Issue 2.1).
#
# Source this file; it defines functions only and has no side effects on load.
# It builds on lib/checks.sh for the _pass/_fail helpers and the $COMPOSE
# convention, so source that first (or let this file source it).
#
#   source "$(dirname "$0")/lib/verify.sh"
#   pg_count customers
#
# Unlike checks.sh (which asserts infrastructure health), these functions read
# row data out of Postgres and ClickHouse so callers can assert that the
# snapshot and subsequent streaming actually landed. All queries run inside the
# service containers, so the host needs no psql/clickhouse-client installed.

# Resolve this file's directory so we can source its sibling regardless of the
# caller's working directory, then pull in _pass/_fail/$COMPOSE if not already
# loaded.
_VERIFY_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if ! declare -f _pass >/dev/null 2>&1; then
  # shellcheck source=deploy/lib/checks.sh
  source "$_VERIFY_DIR/checks.sh"
fi

COMPOSE=${COMPOSE:-docker compose}

# ------------------------------------------------------------------------------
# Thin query wrappers
# ------------------------------------------------------------------------------

# pg_exec <sql> runs SQL against the source Postgres as user/db cdc. Output is
# whatever psql prints; use pg_query for a single scalar.
pg_exec() {
  $COMPOSE exec -T postgres psql -U cdc -d cdc -v ON_ERROR_STOP=1 -c "$1"
}

# pg_query <sql> runs SQL and returns a single bare value (-tA: tuples only,
# unaligned), trimmed. Use for scalars like counts.
pg_query() {
  $COMPOSE exec -T postgres psql -U cdc -d cdc -tAc "$1" 2>/dev/null | tr -d '[:space:]'
}

# ch_query <sql> runs SQL through clickhouse-client (native protocol) and
# returns the bare result, trimmed.
ch_query() {
  $COMPOSE exec -T clickhouse clickhouse-client --query "$1" 2>/dev/null | tr -d '[:space:]'
}

# ------------------------------------------------------------------------------
# Row counts
# ------------------------------------------------------------------------------

# pg_count <table> returns the live row count in the Postgres source table.
pg_count() {
  pg_query "SELECT count(*) FROM public.$1"
}

# ch_count_final <table> returns the count of live (non-deleted) rows in the
# ClickHouse target, collapsing ReplacingMergeTree versions with FINAL. FINAL is
# what makes this meaningful for the no-duplicates check: a replayed event or an
# insert->update->delete on one key collapses to a single row (or zero, once
# deleted), so the count reflects logical state, not raw inserts.
ch_count_final() {
  ch_query "SELECT count(*) FROM cdc.$1 FINAL WHERE _is_deleted = 0"
}

# ------------------------------------------------------------------------------
# Assertions and waits
# ------------------------------------------------------------------------------

# assert_eq <label> <expected> <actual> emits a PASS/FAIL line and returns
# non-zero on mismatch, matching checks.sh's aggregate-fail style.
assert_eq() {
  local label=$1 want=$2 got=$3
  if [ "$got" = "$want" ]; then
    _pass "$label (= $want)"
    return 0
  fi
  _fail "$label: got '${got:-<empty>}', want '$want'"
  return 1
}

# wait_for_ch_count <table> <expected> [timeout_s] polls ch_count_final until it
# equals expected or the timeout elapses. Landing is asynchronous (Debezium ->
# Kafka -> worker batch/flush), so callers must wait rather than read once.
# Returns 0 on match, 1 on timeout. Echoes the final observed count to stdout so
# the caller can feed it to assert_eq for a single, uniform PASS/FAIL line.
wait_for_ch_count() {
  local table=$1 want=$2 timeout=${3:-30}
  local waited=0 got=""
  while [ "$waited" -lt "$timeout" ]; do
    got=$(ch_count_final "$table")
    if [ "$got" = "$want" ]; then
      echo "$got"
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  echo "${got:-0}"
  return 1
}
