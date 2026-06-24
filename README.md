# CDC Platform

A multi-tenant SaaS Change Data Capture platform, built from scratch.

**Stack:** Go (pipeline workers), PostgreSQL, Apache Kafka, gRPC + grpc-gateway,
Docker, Kubernetes, S3, HashiCorp Vault, OpenTelemetry + Prometheus + Grafana + Loki, CI/CD.
API-only - no frontend.

See [ROADMAP.md](ROADMAP.md) for the full phase-by-phase plan.

## Repository layout

```
cmd/
  worker/        worker entrypoint (config parsing + startup)
internal/
  source/        capture sources (Postgres, MySQL, Mongo)
  sink/          delivery sinks (Postgres, HTTP, S3/Iceberg)
  offset/        durable offset / position persistence
  config/        worker and tenant configuration
  pipeline/      wiring of source -> sink -> offset (contract test)
  gen/           generated protobuf code (buf generate)
proto/           Protobuf contract (cdc.v1)
deploy/          local infra config (Postgres init SQL, etc.)
```

## Prerequisites

- Go 1.26+
- [buf](https://buf.build) (proto lint / breaking / codegen)
- `protoc-gen-go`, `protoc-gen-go-grpc` (codegen plugins)
- [golangci-lint](https://golangci-lint.run) v2
- Docker + Docker Compose (local stack)

Install the Go-based tools:

```sh
go install github.com/bufbuild/buf/cmd/buf@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
```

## Common tasks

```sh
make build      # go build ./... + worker binary
make test       # go test ./...
make lint       # go vet + golangci-lint
make generate   # buf generate (regenerate proto bindings)
```

On a fresh clone, `make build` and `make lint` should both succeed.

## Local stack

`docker compose up -d` brings up Kafka (KRaft, no ZooKeeper), Postgres
configured for logical replication, and MinIO with the snapshot bucket
auto-created.

| Service    | Host address              | Credentials / notes                       |
| ---------- | ------------------------- | ----------------------------------------- |
| Kafka      | `localhost:29092`         | PLAINTEXT, bootstrap for host clients      |
| Postgres   | `localhost:5432`          | user/pass/db = `cdc` / `cdc` / `cdc`       |
| MinIO API  | `http://localhost:9000`   | access/secret = `minioadmin` / `minioadmin` |
| MinIO UI   | `http://localhost:9001`   | same credentials                          |

Postgres runs with `wal_level=logical`, `max_replication_slots=10`,
`max_wal_senders=10`. The init script seeds `public.demo_events` (REPLICA
IDENTITY FULL), a `cdc_heartbeat` table, and publication `cdc_pub`. MinIO gets
the `cdc-snapshots` bucket. See `.env.example` for ready-made connection
strings. All credentials are local-only dev defaults.

```sh
docker compose up -d       # start
docker compose ps          # check health
docker compose down -v     # stop and remove volumes
```

## Continuous integration

Every pull request and every push to `main` runs the GitHub Actions pipeline
([.github/workflows/ci.yml](.github/workflows/ci.yml)):

- `go build ./...`, `go vet ./...`, `go test ./...`
- `golangci-lint run`
- `buf lint`, `buf breaking` (against `main`), and a proto codegen drift check
  (`buf generate` must produce no diff)

**CI must be green to merge.** Configure branch protection on `main` to require
this workflow so regressions cannot land.
