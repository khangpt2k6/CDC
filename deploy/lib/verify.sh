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

# ------------------------------------------------------------------------------
# Content parity (ROADMAP Issue 2.2: "final view matches source")
# ------------------------------------------------------------------------------
#
# A row count alone is too weak to prove parity: a dropped UPDATE leaves the same
# number of rows, just with stale values. So we fingerprint the *content* of the
# current state on each side and compare.
#
# The fingerprint is md5(concat of every row's columns, ordered by id). For the
# two hashes to match, both engines must render each value to a byte-identical
# string, so every column is normalized to a canonical text form:
#   - timestamps -> 'YYYY-MM-DD HH24:MI:SS.US' in UTC, no timezone suffix, fixed
#     6-digit microseconds. Postgres timestamptz prints '+00' and ClickHouse
#     DateTime64 does not, so both are reformatted rather than cast.
#   - decimals/ints/text -> cast to text (identical across engines at this scale).
# Columns mirror clickhouse.Specs (internal/sink/clickhouse/map.go) per table.

# pg_checksum <table> fingerprints the current Postgres state for a tracked table.
pg_checksum() {
  local expr
  case "$1" in
    customers)
      expr="id::text, email, full_name, country,
            to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'),
            to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')" ;;
    orders)
      expr="id::text, customer_id::text, status, total_amount::text, currency,
            to_char(placed_at  AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'),
            to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')" ;;
    *) echo "pg_checksum: unknown table '$1'" >&2; return 2 ;;
  esac
  # concat_ws with a separator the data can't contain (chr(31), the unit
  # separator; matches char(31) on the ClickHouse side), aggregated in id order,
  # then hashed. coalesce('') keeps a NULL from collapsing the whole row string.
  pg_query "SELECT md5(coalesce(string_agg(r, '|' ORDER BY id), ''))
            FROM (SELECT id, concat_ws(chr(31), $expr) AS r FROM public.$1) s"
}

# ch_checksum <table> fingerprints the current ClickHouse state (FINAL, live rows
# only) with the SAME normalization, so it compares equal to pg_checksum.
ch_checksum() {
  local expr
  # toString on a DateTime64(6) yields 'YYYY-MM-DD HH:MM:SS.ffffff' (6-digit,
  # UTC, no offset) -- byte-identical to Postgres to_char(... 'US'); verified
  # against both engines incl. whole-second and trailing-zero cases.
  case "$1" in
    customers)
      expr="toString(id), email, full_name, country,
            toString(created_at), toString(updated_at)" ;;
    orders)
      # toDecimalString(..., 2) pins the scale so 42.50 doesn't render as 42.5;
      # Postgres numeric(12,2)::text already keeps both decimals.
      expr="toString(id), toString(customer_id), status, toDecimalString(total_amount, 2), currency,
            toString(placed_at), toString(updated_at)" ;;
    *) echo "ch_checksum: unknown table '$1'" >&2; return 2 ;;
  esac
  # Reproduce Postgres's string_agg(... ORDER BY id): collect (id, row_text)
  # tuples, sort NUMERICALLY by id (arraySort's default lexical order would put
  # '10' before '2'), then concat the row_text parts. char(31) matches concat_ws
  # on the Postgres side; '|' is the inter-row separator.
  ch_query "SELECT lower(hex(MD5(arrayStringConcat(
              arrayMap(t -> t.2,
                arraySort(t -> t.1,
                  groupArray((id, arrayStringConcat([$expr], char(31)))))),
              '|'))))
            FROM cdc.$1 FINAL WHERE _is_deleted = 0"
}

# wait_for_parity <table> [timeout_s] waits until the ClickHouse live count equals
# the Postgres count (pipeline drained), then returns. It does NOT compare
# checksums itself — the caller does that once counts agree, so a count mismatch
# and a content mismatch produce distinct, readable failures. Returns 0 once
# counts converge, 1 on timeout. Echoes the final ClickHouse count.
wait_for_parity() {
  local table=$1 timeout=${2:-60}
  wait_for_ch_count "$table" "$(pg_count "$table")" "$timeout"
}

# ------------------------------------------------------------------------------
# Kafka consumer-group offsets (ROADMAP Issue 2.2: "resume from committed offset")
# ------------------------------------------------------------------------------

# kafka_group_offsets <group> prints "<topic> <partition> <current-offset> <lag>"
# for every partition the group has committed, by describing it through the Kafka
# CLI inside the broker container. Used to prove the worker resumes at (>=) the
# offset it had committed before a crash, and that lag drains to 0 afterward.
#
# kafka-consumer-groups.sh --describe columns are:
#   GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
# so $2=topic, $3=partition, $4=current-offset, $6=lag. We require a numeric
# current-offset to skip the header and any blank/owner-only lines.
#
# The command runs through `sh -c '...'` inside the container so the absolute
# /opt/kafka/... path is NOT mangled by MSYS/Git-Bash path conversion on Windows
# (which would rewrite it to C:/Program Files/Git/opt/kafka and fail). This form
# is portable across Windows Git Bash, Linux, and macOS.
kafka_group_offsets() {
  $COMPOSE exec -T kafka sh -c \
    "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group '$1'" \
    2>/dev/null \
    | awk '$4 ~ /^[0-9]+$/ { print $2, $3, $4, $6 }'
}

# kafka_total_lag <group> sums LAG across all partitions (0 means fully caught
# up). Echoes the total; non-numeric/absent lag rows count as 0.
kafka_total_lag() {
  kafka_group_offsets "$1" | awk '{ s += ($4 ~ /^[0-9]+$/ ? $4 : 0) } END { print s+0 }'
}

# ------------------------------------------------------------------------------
# Merge settling (ROADMAP Issue 2.3: settle before comparison)
# ------------------------------------------------------------------------------

# ch_optimize_final <table> forces ClickHouse to merge all ReplacingMergeTree
# parts so the table is fully collapsed before a content comparison. A FINAL
# query already collapses versions at read time, so this is not required for
# correctness; it settles the on-disk state so the checksum is taken over merged
# data, matching the "OPTIMIZE TABLE ... FINAL" step in Issue 2.3. Best-effort:
# OPTIMIZE can be a no-op or briefly contend with a background merge, so failure
# is swallowed (the FINAL-qualified checksum query is still authoritative).
ch_optimize_final() {
  ch_query "OPTIMIZE TABLE cdc.$1 FINAL" >/dev/null 2>&1
}
