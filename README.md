<div align="center">

# ⚡ CDC Pipeline

**Real time Change Data Capture from PostgreSQL to ClickHouse, with a live Grafana dashboard.**

Row changes flow out of the operational database and into a fast columnar store
in seconds. The primary is never touched by a query.

[![CI](https://github.com/khangpt2k6/Slipstream_CDC/actions/workflows/ci.yml/badge.svg)](https://github.com/khangpt2k6/Slipstream_CDC/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-KRaft-231F20?logo=apachekafka&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-analytics-FFCC01?logo=clickhouse&logoColor=black)
![Docker](https://img.shields.io/badge/Docker_Compose-one_command-2496ED?logo=docker&logoColor=white)

<br/>

![CDC pipeline in motion](docs/Visualize.gif)

</div>

---

## 🔁 The flow

```mermaid
flowchart LR
    PG[(PostgreSQL<br/>OLTP)]
    DBZ[Debezium<br/>capture]
    K[Kafka<br/>transport]
    GO[Go worker<br/>this repo]
    CH[(ClickHouse<br/>analytics)]
    GF[Grafana<br/>dashboard]

    PG -->|logical replication| DBZ
    DBZ -->|change events| K
    K -->|consume| GO
    GO -->|batch insert| CH
    CH -->|query| GF
```

> [!NOTE]
> Debezium reads the Postgres write ahead log and Kafka carries the events. The
> Go worker in this repo lands them in ClickHouse correctly, then Grafana shows
> the result within seconds.

---

## 🚀 Quickstart

```sh
docker compose up -d                       # bring up the full stack
./deploy/debezium/register-connector.sh    # register the Postgres connector
make run                                    # start the Go worker
# open Grafana and watch it update as Postgres changes
```

> [!TIP]
> Credentials are local only dev defaults. Tear everything down with
> `docker compose down -v`.

### One-command demo

```sh
bash deploy/demo.sh            # or --fresh for a cold start
```

`deploy/demo.sh` does the whole sequence for you: brings the stack up, registers
the connector, builds and runs the worker, and starts a load generator writing a
steady stream of inserts/updates/deletes to Postgres. It prints the Grafana
(`:3000`) and Prometheus (`:9090`) URLs and stays running so you can watch the
**CDC Analytics** panels move and consumer lag stay near zero on **CDC Pipeline
Health**. Press Ctrl-C to stop: it removes the generated rows and leaves the
stack up.

### Services

| Service | Address | Notes |
| ------- | ------- | ----- |
| Postgres | `localhost:5432` | user / pass / db = `cdc` |
| Kafka | `localhost:29092` | PLAINTEXT bootstrap for host clients |
| Kafka Connect | `http://localhost:8083` | Debezium connector REST API |
| ClickHouse | `localhost:8123` / `9000` | HTTP / native |
| Grafana | `http://localhost:3000` | provisioned dashboards (anonymous admin, local-dev) |
| Prometheus | `http://localhost:9090` | scrapes the worker `/metrics` |

Datasources (Prometheus + ClickHouse) and dashboards are **provisioned** from
`deploy/grafana/` and `deploy/prometheus/`, nothing to click. Two dashboards
load under the **CDC** folder:

- **CDC Pipeline Health** (Prometheus): throughput, consumer lag, flush latency,
  errors. Prometheus scrapes the worker at `host.docker.internal:9100`, so the
  worker must be running on the host (`make run`) for it to populate.
- **CDC Analytics (ClickHouse)**: the payoff is live customers/orders, revenue,
  orders by status, top customers, customers by country, all over the
  current-state view (`FINAL WHERE _is_deleted = 0`). Panels reflect Postgres
  writes within seconds.

---

## 🧱 Stack

| Layer | Tech |
| ----- | ---- |
| Source | **PostgreSQL** with logical replication |
| Capture | **Debezium** on Kafka Connect (`pgoutput`) |
| Transport | **Apache Kafka** in KRaft mode |
| Worker | **Go** (this repo) |
| Analytics | **ClickHouse** (`ReplacingMergeTree`) |
| Dashboard | **Grafana**, metrics by **Prometheus** |
| Local stack | **Docker Compose**, CI by **GitHub Actions** |

**Prerequisites:** Go 1.26+, Docker + Docker Compose,
[golangci-lint](https://golangci-lint.run) v2.

## 🛠️ Common tasks

```sh
make build   # build worker binary
make test    # run tests
make lint    # go vet + golangci-lint
make run     # run the worker against the local stack
```

---

## ⚙️ Configuration

The worker is fully env driven: nothing is hard coded, and it runs with no
variables set (every one has a built-in default). Set a `CDC_*` variable to
override. Full reference with the compose stack variables is in
[.env.example](.env.example).

| Variable | Default | What it controls |
| -------- | ------- | ---------------- |
| `CDC_KAFKA_BROKERS` | `localhost:29092` | Kafka brokers, comma separated `host:port` |
| `CDC_KAFKA_GROUP` | `cdc-clickhouse-sink` | consumer group id |
| `CDC_KAFKA_TOPICS` | `cdc.public.customers,cdc.public.orders` | topics to consume, comma separated |
| `CDC_CLICKHOUSE_DSN` | `clickhouse://default:@localhost:9000/cdc` | ClickHouse native DSN |
| `CDC_CLICKHOUSE_DIAL_TIMEOUT` | `5s` | bound the initial connect |
| `CDC_CLICKHOUSE_READ_TIMEOUT` | `30s` | bound a stalled read or write |
| `CDC_BATCH_SIZE` | `1000` | rows buffered before a flush |
| `CDC_FLUSH_INTERVAL` | `1s` | max time between flushes |
| `CDC_RETRY_BASE` | `1s` | first flush retry backoff (doubles each attempt) |
| `CDC_RETRY_MAX` | `30s` | cap on the flush retry backoff |
| `CDC_LAG_INTERVAL` | `5s` | how often to sample consumer lag |
| `CDC_METRICS_ADDR` | `:9100` | `host:port` serving Prometheus `/metrics` |
| `CDC_DLQ_TOPIC_SUFFIX` | `.dlq` | suffix forming a topic's dead-letter topic |
| `CDC_LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error` |

---

## 🔒 Security

Every push and pull request runs a [security workflow](.github/workflows/security.yml):
`govulncheck` (Go vulnerabilities), `gosec` (static analysis), CodeQL, Trivy
(dependencies and config), and `gitleaks` (secret scanning). Dependency and
action updates land via Dependabot. See [SECURITY.md](SECURITY.md) to report an
issue.

---

## 📚 Docs

| Doc | What is inside |
| --- | -------------- |
| **[DESIGN.md](DESIGN.md)** | How it works, the tradeoffs, and how correctness is proven. |
| **[ROADMAP.md](ROADMAP.md)** | The phase by phase build plan. |

---

<div align="center">

Built as a focused take on a real backend problem: correct, observable delivery
into a columnar store.

</div>
