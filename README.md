# CDC Pipeline

Real-time Change Data Capture: streams row changes from PostgreSQL into
ClickHouse for analytics. Debezium captures the changes, Kafka carries them, and
a Go worker lands them in ClickHouse. Grafana on ClickHouse is the dashboard.

```
Postgres ──▶ Debezium ──▶ Kafka ──▶ Go consumer ──▶ ClickHouse ──▶ Grafana
 (OLTP)      (capture)   (transport)  (this repo)    (analytics)   (dashboard)
```

> **Status:** early. See [ROADMAP.md](ROADMAP.md) for the phase-by-phase build.

**Stack:** PostgreSQL, Debezium on Kafka Connect, Apache Kafka (KRaft), Go,
ClickHouse, Docker Compose, Prometheus + Grafana, GitHub Actions CI.

## Quickstart

```sh
docker compose up -d                       # Postgres, Kafka, Debezium, ClickHouse, Grafana
./deploy/debezium/register-connector.sh    # register the Postgres source connector
make run                                    # start the Go consumer
# open Grafana, watch the dashboard update as Postgres changes
```

## Common tasks

```sh
make build      # go build ./... + worker binary
make test       # go test ./...
make lint       # go vet + golangci-lint
make run        # run the worker against the local stack
```

## Local stack

`docker compose up -d` brings up the full pipeline locally (credentials are
local-only dev defaults).

| Service       | Host address              | Notes                                |
| ------------- | ------------------------- | ------------------------------------ |
| Postgres      | `localhost:5432`          | user/pass/db = `cdc` / `cdc` / `cdc` |
| Kafka         | `localhost:29092`         | PLAINTEXT bootstrap for host clients |
| Kafka Connect | `http://localhost:8083`   | Debezium connector REST API          |
| ClickHouse    | `localhost:8123` / `9000` | HTTP / native; analytics store       |
| Grafana       | `http://localhost:3000`   | dashboards over ClickHouse + metrics |
| Prometheus    | `http://localhost:9090`   | scrapes the worker `/metrics`        |

```sh
docker compose up -d       # start
docker compose ps          # check health
docker compose down -v     # stop and remove volumes
```

## Prerequisites

- Go 1.26+
- Docker + Docker Compose
- [golangci-lint](https://golangci-lint.run) v2

## Documentation

- **[DESIGN.md](DESIGN.md)** - how it works, components, and design decisions.
- **[docs/architecture.html](docs/architecture.html)** - interactive architecture diagram (open in a browser).
- **[ROADMAP.md](ROADMAP.md)** - the full phase-by-phase build plan.

## CI

Every PR and push to `main` runs [CI](.github/workflows/ci.yml): `go build`,
`go vet`, `go test`, and `golangci-lint run`. CI must be green to merge.
