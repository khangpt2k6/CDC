# Design

This document explains how the CDC pipeline works and why it is built the way
it is. For the short overview and quickstart, see [README.md](README.md). For
the phase-by-phase build plan, see [ROADMAP.md](ROADMAP.md).

## Goal

Stream row changes from PostgreSQL into ClickHouse in near real time, so
analytics run on a columnar store that trails the operational database by
seconds without ever querying the primary.

## Architecture

```
┌──────────┐  logical repl  ┌───────────┐   topic    ┌──────────────┐  batch insert  ┌────────────┐   query   ┌─────────┐
│ Postgres │ ─────────────▶ │  Debezium │ ─▶ Kafka ─▶│  Go consumer │ ─────────────▶ │ ClickHouse │ ────────▶ │ Grafana │
│  (OLTP)  │                │(KafkaConn)│            │ (this repo)  │                │ (analytics)│           │dashboard│
└──────────┘                └───────────┘            └──────────────┘                └────────────┘           └─────────┘
```

An interactive version of this diagram is in
[docs/architecture.html](docs/architecture.html) (open it in a browser).

## Components

| Component | Role |
| --------- | ---- |
| **PostgreSQL** | Source of truth (OLTP). Runs with logical replication and a publication listing the captured tables. |
| **Debezium** | A Kafka Connect connector using the `pgoutput` plugin. Snapshots existing rows, then streams every insert/update/delete as a JSON change event. |
| **Apache Kafka** | Durable transport for change events (KRaft mode, no ZooKeeper). |
| **Go consumer** | This repo. Reads change events, maps them to rows, batches them, and inserts into ClickHouse. |
| **ClickHouse** | Columnar analytics store. Holds each source table as a `ReplacingMergeTree`. |
| **Grafana** | Dashboard over ClickHouse. Updates within seconds of a change in Postgres. |
| **Prometheus** | Scrapes the worker `/metrics` endpoint. |

## How it works

1. **Postgres** runs with logical replication enabled (`wal_level=logical`) and a
   publication (`cdc_pub`) listing the captured tables. The demo schema uses
   `REPLICA IDENTITY FULL` so updates and deletes carry the full old row.
2. **Debezium** snapshots the existing rows, then streams every insert, update,
   and delete as a JSON change event onto a per-table Kafka topic.
3. The **Go consumer** reads those events, maps them to rows, batches them, and
   inserts them into ClickHouse. It commits Kafka offsets only after a
   successful ClickHouse flush, so nothing is lost on a crash.
4. **ClickHouse** stores each table with a `ReplacingMergeTree` engine plus a
   `_version` and `_is_deleted` column, so updates collapse to the latest
   version and deletes become tombstones. Current state is read with `FINAL` and
   `_is_deleted = 0`.
5. **Grafana** queries ClickHouse to render an analytics dashboard that updates
   within seconds of a change in Postgres.

## Repository layout

```
cmd/
  worker/        consumer entrypoint (config parsing + startup)
internal/
  consumer/      Kafka consumer + Debezium event parsing
  model/         the internal ChangeEvent struct
  sink/          ClickHouse sink (batching, mapping, flush)
  config/        worker configuration
deploy/
  postgres/      init SQL (demo schema, publication, REPLICA IDENTITY)
  debezium/      Debezium connector config + register script
  clickhouse/    target table DDL
  grafana/       provisioned datasources + dashboards
docs/
  architecture.html   interactive architecture visualization
```

## What this is (and is not)

This is not a from-scratch CDC engine. Debezium handles the hard, well-solved
part (reading the Postgres write-ahead log) so the effort goes where it adds
value: a Go service that lands change events into a columnar store correctly,
plus the operational glue (one-command local stack, metrics, a live dashboard).
That is the tradeoff a real backend engineer makes, and the design decisions
below explain the why.

## Design decisions

### Why Debezium, not a from-scratch WAL reader

Reading the Postgres WAL by hand is a solved problem and a large maintenance
surface. Using Debezium keeps the effort on the part that is actually this
project's value: correct, observable delivery into ClickHouse.

### Why ClickHouse

The point of CDC here is real-time analytics on operational data without
hitting the primary. ClickHouse is a columnar store built for exactly that, and
relational Postgres rows map to it cleanly.

### Why `ReplacingMergeTree`

ClickHouse is append-oriented. Encoding updates and deletes as versioned rows
(`_version`) plus tombstones (`_is_deleted`) is how you get correct
upsert/delete semantics in a columnar store. Reads use `FINAL` to collapse to
the latest version and filter `_is_deleted = 0`.

### Why commit offsets after the flush

Committing only after ClickHouse acks a batch means a crash replays events
rather than dropping them. The `ReplacingMergeTree` version collapses the
duplicate, so delivery is exactly-once in effect even though Kafka delivery is
at-least-once.

### Why a Go consumer instead of the off-the-shelf ClickHouse sink connector

A connector would remove the Go entirely. Writing the sink in Go is a
deliberate choice for control over batching and mapping, and to own the CDC
semantics end to end. In production the off-the-shelf connector would be worth
reconsidering.

## Delivery and correctness

- **At-least-once from Kafka, exactly-once in effect.** Offsets commit only
  after the ClickHouse flush acks. A crash between flush and commit replays the
  batch; `ReplacingMergeTree` dedupes on `_version`.
- **Ordering.** Per-key ordering is preserved through Kafka partitions and the
  monotonic `_version`, so the latest write wins after merge.
- **Snapshot then stream.** Debezium snapshots existing rows before switching to
  streaming, so ClickHouse converges to the full source state, not just changes
  going forward.

## Out of scope (future work)

See [ROADMAP.md](ROADMAP.md) for the full list. Highlights: more sources,
schema evolution, multi-tenancy, and a control-plane API (gRPC + REST) with a
Kubernetes operator to manage pipelines.
