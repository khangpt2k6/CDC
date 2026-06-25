# CDC Pipeline

A real-time Change Data Capture pipeline that streams row changes from
PostgreSQL into ClickHouse for analytics. Debezium captures the changes, Kafka
carries them, and a Go worker lands them in ClickHouse. Grafana on ClickHouse is
the dashboard, so there is no separate frontend.

```
┌──────────┐  logical repl  ┌───────────┐   topic    ┌──────────────┐  batch insert  ┌────────────┐   query   ┌─────────┐
│ Postgres │ ─────────────▶ │  Debezium │ ─▶ Kafka ─▶│  Go consumer │ ─────────────▶ │ ClickHouse │ ────────▶ │ Grafana │
│  (OLTP)  │                │(KafkaConn)│            │ (this repo)  │                │ (analytics)│           │dashboard│
└──────────┘                └───────────┘            └──────────────┘                └────────────┘           └─────────┘
```

**Stack:** PostgreSQL (source), Debezium on Kafka Connect (capture), Apache
Kafka (transport, KRaft mode), Go (sink worker), ClickHouse (analytics store),
Docker Compose (local stack), Prometheus + Grafana (metrics and dashboard),
GitHub Actions CI.

> **Status:** early. The architecture and plan are set; see
> [ROADMAP.md](ROADMAP.md) for the phase-by-phase build. The Go worker and the
> Debezium/ClickHouse half of the local stack are being built per Phase 0 and
> Phase 1.

## What this is (and is not)

This is not a from-scratch CDC engine. Debezium handles the hard, well-solved
part (reading the Postgres write-ahead log) so the effort goes where it adds
value: a Go service that lands change events into a columnar store correctly,
plus the operational glue (one-command local stack, metrics, a live dashboard).
That is the tradeoff a real backend engineer makes, and the design decisions
below explain the why.

## How it works

1. **Postgres** runs with logical replication enabled and a publication listing
   the captured tables.
2. **Debezium** (a Kafka Connect connector using the `pgoutput` plugin) snapshots
   the existing rows, then streams every insert, update, and delete as a JSON
   change event onto a Kafka topic.
3. The **Go consumer** reads those events, maps them to rows, batches them, and
   inserts them into ClickHouse. It commits Kafka offsets only after a successful
   ClickHouse flush, so nothing is lost on a crash.
4. **ClickHouse** stores each table with a `ReplacingMergeTree` engine plus a
   `_version` and `_is_deleted` column, so updates collapse to the latest version
   and deletes become tombstones. The current state is read with `FINAL` and
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
```

(The earlier `proto/`, `internal/gen/`, and `buf*` scaffolding from the
from-scratch design is legacy and is being removed; the consumer reads Debezium
JSON directly rather than a custom Protobuf envelope.)

## Prerequisites

- Go 1.26+
- Docker + Docker Compose (local stack)
- [golangci-lint](https://golangci-lint.run) v2 (lint)

## Common tasks

```sh
make build      # go build ./... + worker binary
make test       # go test ./...
make lint       # go vet + golangci-lint
make run        # run the worker against the local stack
```

## Quickstart

```sh
docker compose up -d                       # Postgres, Kafka, Debezium, ClickHouse, Grafana
./deploy/debezium/register-connector.sh    # register the Postgres source connector
make run                                    # start the Go consumer
# open Grafana, watch the analytics dashboard update as Postgres changes
```

## Local stack

`docker compose up -d` brings up the pipeline locally. All credentials are
local-only dev defaults.

| Service       | Host address            | Notes                                  |
| ------------- | ----------------------- | -------------------------------------- |
| Postgres      | `localhost:5432`        | user/pass/db = `cdc` / `cdc` / `cdc`   |
| Kafka         | `localhost:29092`       | PLAINTEXT bootstrap for host clients   |
| Kafka Connect | `http://localhost:8083` | Debezium connector REST API            |
| ClickHouse    | `localhost:8123` / `9000` | HTTP / native; analytics store       |
| Grafana       | `http://localhost:3000` | dashboards over ClickHouse + metrics   |
| Prometheus    | `http://localhost:9090` | scrapes the worker `/metrics`          |

Postgres runs with `wal_level=logical`, `max_replication_slots=10`,
`max_wal_senders=10`. The init script seeds a small demo schema with
`REPLICA IDENTITY FULL` and the `cdc_pub` publication.

```sh
docker compose up -d       # start
docker compose ps          # check health
docker compose down -v     # stop and remove volumes
```

## Continuous integration

Every pull request and every push to `main` runs the GitHub Actions pipeline
([.github/workflows/ci.yml](.github/workflows/ci.yml)): `go build ./...`,
`go vet ./...`, `go test ./...`, and `golangci-lint run`.

**CI must be green to merge.** Branch protection on `main` requires this
workflow so regressions cannot land.

## Design decisions

- **Why Debezium, not a from-scratch reader.** Reading the Postgres WAL by hand
  is a solved problem and a large maintenance surface. Using Debezium keeps the
  effort on the part that is actually this project's value: correct, observable
  delivery into ClickHouse.
- **Why ClickHouse.** The point of CDC here is real-time analytics on
  operational data without hitting the primary. ClickHouse is a columnar store
  built for exactly that, and relational Postgres rows map to it cleanly.
- **Why `ReplacingMergeTree`.** ClickHouse is append-oriented. Encoding updates
  and deletes as versioned rows (`_version`) plus tombstones (`_is_deleted`) is
  how you get correct upsert/delete semantics in a columnar store.
- **Why commit offsets after the flush.** Committing only after ClickHouse acks
  a batch means a crash replays events rather than dropping them; the
  `ReplacingMergeTree` version collapses the duplicate, so delivery is
  exactly-once in effect.
- **Why a Go consumer instead of the off-the-shelf ClickHouse sink connector.**
  A connector would remove the Go entirely. Writing the sink in Go is a
  deliberate choice for control over batching and mapping, and to own the CDC
  semantics end to end. In production the off-the-shelf connector would be worth
  reconsidering.

See [ROADMAP.md](ROADMAP.md) for the full plan and the out-of-scope future work
(more sources, multi-tenancy, a control-plane API).
