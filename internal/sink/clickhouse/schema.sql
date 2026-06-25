-- ClickHouse target schema for the CDC sink.
--
-- Applied idempotently by the worker on startup (CREATE TABLE IF NOT EXISTS),
-- because the local ClickHouse service is not mounted with an init directory.
--
-- Each table mirrors its Postgres source plus two pipeline columns:
--   _version    the source LSN; ReplacingMergeTree keeps the row with the
--               highest _version per ORDER BY key, so a replayed or out-of-date
--               event never overwrites a newer one.
--   _is_deleted 1 for a delete tombstone, 0 otherwise.
--
-- Read the current state with FINAL to collapse versions, filtering tombstones:
--   SELECT * FROM cdc.orders FINAL WHERE _is_deleted = 0;

CREATE TABLE IF NOT EXISTS cdc.customers
(
    id          Int64,
    email       String,
    full_name   String,
    country     String,
    created_at  DateTime64(6, 'UTC'),
    updated_at  DateTime64(6, 'UTC'),
    _version    UInt64,
    _is_deleted UInt8
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY id;

CREATE TABLE IF NOT EXISTS cdc.orders
(
    id           Int64,
    customer_id  Int64,
    status       String,
    total_amount Decimal(12, 2),
    currency     String,
    placed_at    DateTime64(6, 'UTC'),
    updated_at   DateTime64(6, 'UTC'),
    _version     UInt64,
    _is_deleted  UInt8
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY id;
