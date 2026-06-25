# CDC Pipeline - Roadmap

A real-time Change Data Capture pipeline that streams row changes from
PostgreSQL into ClickHouse for analytics, using Debezium for capture and a
Go worker for delivery.

**Stack:** PostgreSQL (source), Debezium on Kafka Connect (capture), Apache
Kafka (transport, KRaft mode), Go (sink worker - the part you build),
ClickHouse (analytics store), Docker Compose (local stack), Prometheus +
Grafana (metrics and the demo dashboard), GitHub Actions CI. No frontend SPA:
Grafana on ClickHouse is the dashboard.

This is a solo, intern-sized project. Each **issue** is a focused unit of work
(a few hours up to one day). Issues are numbered `Phase.Issue`. Every issue
lists its goal, the specific tech, ordered steps, and testable acceptance
criteria. The whole thing is meant to be finishable in roughly 1 to 2 weeks of
part-time work and, crucially, to actually run end to end.

## What this project is (and is not)

This is not a from-scratch CDC engine. Debezium does the hard, well-solved part
(reading the Postgres write-ahead log) so the effort goes where it adds value:
a Go service that correctly lands change events into a columnar store, plus the
operational glue (one-command local stack, metrics, a live dashboard, a clean
demo). That is exactly the tradeoff a real backend engineer makes, and being
able to explain it is the point.

## The five things that carry this project

1. Debezium reliably capturing Postgres changes into Kafka.
2. The Go consumer that transforms those events and lands them in ClickHouse.
3. Correct CDC semantics in a columnar store: upsert and delete via
   `ReplacingMergeTree` (ClickHouse is append-oriented, so this is the real
   problem to solve).
4. Exactly-once-effective delivery: commit Kafka offsets only after a
   ClickHouse flush is acked, and dedupe on replay.
5. Observability: consumer lag plus a live analytics dashboard that visibly
   updates as Postgres changes.

## Build order (dependency spine)

```
Local stack (P0) -> Go consumer: Kafka to ClickHouse (P1)
   -> correctness and resilience (P2) -> observability and demo (P3)
   -> polish and docs (P4)
```

Phase 0 stands the whole pipeline up with off-the-shelf parts (no Go yet).
Phase 1 is where you write the centerpiece. Phases 2 to 4 make it trustworthy,
visible, and shippable.

## A note on the existing scaffold

Earlier work added a Protobuf envelope, `buf` tooling, and a gRPC control-plane
skeleton aimed at a from-scratch engine. This roadmap does not use them. The Go
consumer deserializes Debezium's own JSON change events into a plain Go struct,
so the `proto/`, `internal/gen/`, and `buf*` pieces can be deleted (or left
parked under "Future work" as an optional control API). Phase 0 includes an
issue to trim the repo and CI to match the new stack.

---

# Phase 0 - Local stack

Bring the entire pipeline up locally with one command, using only off-the-shelf
components. No application code yet. The goal of this phase is: change a row in
Postgres, see the event appear in a Kafka topic.

### Issue 0.1 - Docker Compose: Postgres, Kafka, Kafka Connect, ClickHouse

**Goal:** A single `docker compose up -d` brings up every infrastructure piece
healthy, so the rest of the project has a reproducible home. Getting Postgres
replication config and Kafka Connect's Debezium image right here is what unlocks
everything downstream.

**Tech stack:**
- Docker Compose - local orchestration.
- Postgres 16 with `wal_level=logical` - enables logical replication for Debezium.
- Kafka in KRaft mode - broker without ZooKeeper.
- Kafka Connect with the Debezium Postgres connector image (e.g. `debezium/connect`).
- ClickHouse (`clickhouse/clickhouse-server`) - the analytics sink.

**What to do:**
1. Define services: `postgres`, `kafka` (single-node KRaft, fixed advertised
   listeners for host access), `connect` (Debezium Kafka Connect), `clickhouse`.
2. Configure Postgres with `wal_level=logical`, `max_replication_slots=10`,
   `max_wal_senders=10` via command flags or a mounted conf.
3. Add `healthcheck` blocks to each service and use `depends_on: condition:
   service_healthy` so startup is ordered (Connect must wait for Kafka, etc.).
4. Expose host ports: Postgres 5432, Kafka 29092, Connect 8083, ClickHouse 8123
   (HTTP) and 9000 (native).
5. Keep all credentials to local-only dev defaults and document them.

**Acceptance criteria:**
- [ ] `docker compose up -d` brings Postgres, Kafka, Connect, and ClickHouse all to healthy.
- [ ] Postgres reports `wal_level=logical`.
- [ ] Kafka Connect REST API answers on `localhost:8083`.
- [ ] ClickHouse answers a `SELECT 1` on the HTTP and native ports.

**Depends on:** none.

### Issue 0.2 - Seed a demo OLTP schema in Postgres

**Goal:** Give Debezium something realistic to capture. A small, believable
schema (think an e-commerce slice) makes the demo tell a story and exercises
inserts, updates, and deletes.

**Tech stack:**
- Postgres init SQL (mounted into the container's entrypoint dir).
- `REPLICA IDENTITY FULL` - so UPDATE/DELETE carry full before-images.
- A Postgres publication - the set of tables Debezium streams.

**What to do:**
1. Create tables such as `customers` and `orders` with primary keys and a few
   typed columns (timestamps, numerics, text).
2. Set `REPLICA IDENTITY FULL` on each captured table so deletes and updates
   include old values.
3. Create a publication, for example `CREATE PUBLICATION cdc_pub FOR TABLE
   customers, orders;`.
4. Seed a handful of starter rows so the initial Debezium snapshot has content.

**Acceptance criteria:**
- [ ] Tables exist with primary keys and `REPLICA IDENTITY FULL`.
- [ ] Publication `cdc_pub` covers the captured tables.
- [ ] Starter rows are present after a fresh `compose up`.

**Depends on:** Issue 0.1.

### Issue 0.3 - Register the Debezium Postgres connector and verify events

**Goal:** Stand up capture with zero Go code and prove the tap works. This is
the moment the pipeline becomes real: a row change in Postgres shows up as a
Kafka message.

**Tech stack:**
- Debezium Postgres connector via the Kafka Connect REST API (POST to `/connectors`).
- The `pgoutput` logical decoding plugin (built into Postgres, nothing to install).
- JSON value converter with `value.converter.schemas.enable=false` for compact, easy-to-parse events.

**What to do:**
1. Write a connector config JSON: `connector.class` =
   `io.debezium.connector.postgresql.PostgresConnector`, the Postgres host and
   credentials, `plugin.name=pgoutput`, `publication.name=cdc_pub`,
   `slot.name=cdc_slot`, `topic.prefix=cdc`, and the table include list.
2. Disable schema envelopes in the JSON converter to keep payloads small.
3. POST the config to `http://localhost:8083/connectors`. Check status at
   `/connectors/<name>/status` until `RUNNING`.
4. Make an INSERT, an UPDATE, and a DELETE in Postgres and read the resulting
   topic with `kafka-console-consumer` (or `kcat`) to confirm `op` = c/u/d and
   `before`/`after` payloads look right.
5. Save the connector JSON and a `register-connector` helper script in the repo.

**Acceptance criteria:**
- [ ] Connector status is `RUNNING`.
- [ ] INSERT/UPDATE/DELETE in Postgres produce messages on the table topic.
- [ ] DELETE and UPDATE events carry a populated `before` image.
- [ ] The connector config and registration script are committed.

**Depends on:** Issue 0.2.

### Issue 0.4 - Go module skeleton and config

**Goal:** A buildable Go home for the consumer with config plumbing, so Phase 1
starts on solid ground rather than wiring boilerplate mid-feature.

**Tech stack:**
- Go modules and the standard `cmd/` + `internal/` layout.
- Config via environment variables (Kafka brokers, topic, ClickHouse DSN, batch knobs).
- `golangci-lint` and a `Makefile` with `build`/`test`/`lint`/`run`.

**What to do:**
1. Keep the module path; ensure `cmd/worker/main.go` builds and parses config
   from the environment with sensible defaults pointing at the compose stack.
2. Define a `Config` struct: Kafka brokers, consumer group, topic(s),
   ClickHouse DSN, batch size, flush interval.
3. Reduce `internal/` to what the new design needs (a consumer package, a
   clickhouse sink package, a model package). Remove from-scratch source code
   that no longer applies.
4. Keep `make build`, `make test`, `make lint` green from a fresh clone.

**Acceptance criteria:**
- [ ] `go build ./...` and `go vet ./...` pass.
- [ ] `golangci-lint run` is clean.
- [ ] The worker reads all config from the environment with documented defaults.

**Depends on:** none.

### Issue 0.5 - Trim CI to the new stack

**Goal:** Keep CI green and relevant. Drop the proto/buf gates that no longer
apply and keep the Go build/test/lint gates that do.

**Tech stack:**
- GitHub Actions on a pinned Go version.
- `go build`, `go vet`, `go test`, `golangci-lint`.

**What to do:**
1. Remove the `buf lint`/`buf breaking`/codegen-drift steps if the proto module
   is removed.
2. Keep `go build ./...`, `go vet ./...`, `go test ./...`, and `golangci-lint run`.
3. Confirm the pipeline is green on the trimmed skeleton and keep branch
   protection requiring it.

**Acceptance criteria:**
- [ ] CI runs build, vet, test, and lint, and is green.
- [ ] No CI steps reference removed tooling.
- [ ] Branch protection still blocks merge on red.

**Depends on:** Issue 0.4.

---

# Phase 1 - Go consumer: Kafka to ClickHouse

The centerpiece. Build the Go service that consumes Debezium change events from
Kafka and lands them in ClickHouse correctly. This is the code you will talk
about in interviews.

### Issue 1.1 - Kafka consumer scaffold

**Goal:** A consumer group that reads the Debezium topic and logs messages, so
the read side works before any transform or write logic is added.

**Tech stack:**
- A Go Kafka client. Recommended: `franz-go` (pure Go, no cgo, full featured).
  Simpler alternative: `segmentio/kafka-go`.
- Kafka consumer groups - so offsets are tracked server-side and restarts resume.

**What to do:**
1. Connect to the broker at `localhost:29092`, join a consumer group, subscribe
   to the table topic(s).
2. Poll in a loop, log key/value and partition/offset for each message.
3. Handle graceful shutdown on SIGINT/SIGTERM (stop polling, close the client).
4. Disable auto-commit; offsets will be committed manually in Issue 1.6.

**Acceptance criteria:**
- [ ] The worker joins a consumer group and prints incoming Debezium messages.
- [ ] Ctrl-C shuts the consumer down cleanly.
- [ ] Auto-commit is off (manual commit comes later).

**Depends on:** Issue 0.3, Issue 0.4.

### Issue 1.2 - Parse Debezium change events into a Go model

**Goal:** Turn the raw Debezium JSON into a typed Go struct the rest of the
pipeline can use, isolating the wire format in one place.

**Tech stack:**
- `encoding/json` - decode the Debezium event.
- A `ChangeEvent` Go struct - the internal model.

**What to do:**
1. Define `ChangeEvent`: operation (`c`/`u`/`d`/`r` for read/snapshot), table,
   primary key, `before` and `after` as maps or typed fields, source LSN, and
   commit timestamp from the `source` block.
2. Decode the Debezium payload into it; map `op` to an enum.
3. Handle the snapshot read op (`r`) the same as an insert for landing purposes.
4. Unit-test parsing against saved sample events for each op type (table-driven test).

**Acceptance criteria:**
- [ ] Insert, update, delete, and snapshot-read events all parse into `ChangeEvent`.
- [ ] LSN and commit timestamp are extracted from the `source` block.
- [ ] A table-driven unit test covers all four op types from fixture JSON.

**Depends on:** Issue 1.1.

### Issue 1.3 - Design the ClickHouse target table

**Goal:** A ClickHouse schema that can represent an evolving OLTP row in an
append-oriented store. This is the design decision that makes correct CDC
possible downstream.

**Tech stack:**
- ClickHouse `ReplacingMergeTree` - keeps only the latest version per key on merge.
- A `version` column (from LSN or commit timestamp) and an `is_deleted` flag.

**What to do:**
1. For each captured table, define a ClickHouse table mirroring its columns plus
   `_version UInt64` and `_is_deleted UInt8`.
2. Use `ReplacingMergeTree(_version)` with `ORDER BY` the primary key so the
   newest version wins and deletes can be represented as a tombstone row.
3. Write the DDL as an init script applied on stack startup.
4. Document the query pattern: read with `FINAL` (or `argMax`) and filter
   `_is_deleted = 0` to get the current state.

**Acceptance criteria:**
- [ ] ClickHouse tables exist with the source columns plus `_version` and `_is_deleted`.
- [ ] Engine is `ReplacingMergeTree(_version)` ordered by the primary key.
- [ ] The "current state" query pattern is documented.

**Depends on:** Issue 0.1.

### Issue 1.4 - Map change events to ClickHouse rows

**Goal:** Translate each `ChangeEvent` into the right ClickHouse row, correctly
encoding inserts, updates, and deletes in the `ReplacingMergeTree` model.

**Tech stack:**
- The official `clickhouse-go` v2 client (native protocol).
- Mapping logic from `ChangeEvent` to a row.

**What to do:**
1. For insert/update/snapshot: build a row from `after`, set `_version` from the
   LSN (monotonic), `_is_deleted = 0`.
2. For delete: build a row from `before` (the key is what matters), set
   `_is_deleted = 1`, `_version` from the LSN.
3. Centralize type conversion (Postgres types to ClickHouse types) in one mapper
   with tests for the tricky cases (timestamps, numerics, nulls).

**Acceptance criteria:**
- [ ] Inserts and updates land as live rows with an increasing `_version`.
- [ ] Deletes land as tombstones (`_is_deleted = 1`) keyed correctly.
- [ ] Type conversions are unit-tested for timestamps, numerics, and nulls.

**Depends on:** Issue 1.2, Issue 1.3.

### Issue 1.5 - Batch inserts with size and time flush

**Goal:** Insert into ClickHouse in batches, not row by row. ClickHouse strongly
prefers large batched inserts; this is a real performance concern and a good
talking point.

**Tech stack:**
- `clickhouse-go` batch API (prepare a batch, append rows, send).
- A buffer flushed on either a row-count threshold or a time interval.

**What to do:**
1. Accumulate mapped rows in an in-memory buffer.
2. Flush when the buffer reaches N rows or T milliseconds have passed since the
   last flush, whichever comes first.
3. Send the batch via the ClickHouse batch API; on success, record the highest
   Kafka offset covered by the batch (handed to Issue 1.6).
4. Make N and T configurable; log batch size and flush latency.

**Acceptance criteria:**
- [ ] Rows are inserted in batches sized by count or time, not one at a time.
- [ ] Batch size and flush interval are configurable.
- [ ] Each successful flush reports the highest offset it covered.

**Depends on:** Issue 1.4.

### Issue 1.6 - Commit offsets after flush (exactly-once-effective)

**Goal:** Guarantee no data loss and no duplicates surviving in the final view,
by committing Kafka offsets only after ClickHouse has durably accepted the batch
and relying on `ReplacingMergeTree` to absorb at-least-once replays.

**Tech stack:**
- Manual Kafka offset commits.
- `ReplacingMergeTree` dedupe by `_version` for idempotency on replay.

**What to do:**
1. Order strictly: flush batch to ClickHouse, get success, then commit the Kafka
   offset for that batch. Never commit before the flush.
2. On crash between flush and commit, the events replay on restart; because rows
   carry the same `_version`, the merge collapses duplicates, so the visible
   state is unchanged.
3. Add a test that kills the worker between flush and commit and confirms the
   final `FINAL` view has no duplicates and no missing rows.

**Acceptance criteria:**
- [ ] Offsets are committed only after a successful ClickHouse flush.
- [ ] A replay after a mid-flight crash leaves no duplicate rows in the `FINAL` view.
- [ ] The ordering contract is documented in code comments.

**Depends on:** Issue 1.5.

---

# Phase 2 - Correctness and resilience

Make the pipeline trustworthy: snapshots, restarts, parity, bad data, and
backpressure.

### Issue 2.1 - Verify the initial snapshot lands

**Goal:** Confirm that Debezium's initial consistent snapshot (op `r`) is
consumed and landed correctly, so existing rows are present before streaming
changes begin.

**Tech stack:**
- Debezium `snapshot.mode=initial` (the default).
- The existing parse and land path (snapshot reads handled as inserts).

**What to do:**
1. Start from a fresh stack with seeded rows. Let Debezium snapshot.
2. Confirm every seeded Postgres row appears in ClickHouse with `_is_deleted = 0`.
3. Then make live changes and confirm streaming picks up where the snapshot left off.

**Acceptance criteria:**
- [ ] All pre-existing Postgres rows appear in ClickHouse after the snapshot.
- [ ] Streaming changes after the snapshot are not lost or duplicated.

**Depends on:** Issue 1.6.

### Issue 2.2 - Restart survival test

**Goal:** Prove the worker resumes cleanly after a crash or restart with no loss
and no surviving duplicates, the core reliability promise.

**Tech stack:**
- Consumer-group offset resume.
- A scripted kill/restart sequence under load.

**What to do:**
1. Drive continuous writes to Postgres.
2. Kill the worker mid-stream and restart it.
3. Confirm it resumes from the last committed offset and the final ClickHouse
   view matches Postgres.

**Acceptance criteria:**
- [ ] The worker resumes from the committed offset after restart.
- [ ] The final view matches the source after a kill under load.

**Depends on:** Issue 2.1.

### Issue 2.3 - Source-to-sink parity test

**Goal:** An automated check that ClickHouse faithfully reflects Postgres, so
correctness is provable rather than assumed.

**Tech stack:**
- A test harness that writes a known workload to Postgres and queries both ends.
- `OPTIMIZE TABLE ... FINAL` (or a `FINAL` query) to settle merges before comparison.

**What to do:**
1. Apply a deterministic mix of inserts, updates, and deletes to Postgres.
2. Wait for the pipeline to drain (poll consumer lag to zero).
3. Compare row counts and a checksum of current rows between Postgres and the
   ClickHouse current-state view.

**Acceptance criteria:**
- [ ] Row counts match between Postgres and the ClickHouse current-state view.
- [ ] A content checksum matches after updates and deletes.

**Depends on:** Issue 2.2.

### Issue 2.4 - Dead-letter handling for bad events

**Goal:** A single malformed or unmappable event must not crash or wedge the
consumer. Route poison messages aside and keep going.

**Tech stack:**
- A dead-letter Kafka topic (or a structured error log + metric to start).
- Per-message error handling around parse and map.

**What to do:**
1. Wrap parse and map in error handling; on failure, send the raw message plus
   the error to a `*.dlq` topic (or log it with full context) and continue.
2. Increment a `dlq_total` metric so failures are visible.
3. Test with a deliberately malformed message and confirm the consumer survives.

**Acceptance criteria:**
- [ ] A malformed event is routed to the DLQ (or logged) and the consumer keeps running.
- [ ] A `dlq_total` metric increments on each bad event.

**Depends on:** Issue 1.6.

### Issue 2.5 - Bounded buffer and backpressure

**Goal:** A slow or paused ClickHouse must not let the in-memory buffer grow
without bound. Apply backpressure by pausing consumption.

**Tech stack:**
- A bounded buffer/channel between consume and flush.
- Kafka consumer pause/resume (or simply blocking the poll loop when full).

**What to do:**
1. Cap the in-flight buffer size.
2. When full, stop fetching from Kafka until a flush drains it.
3. Test by stalling ClickHouse (pause the container) and confirming memory stays
   bounded and the pipeline recovers when ClickHouse returns.

**Acceptance criteria:**
- [ ] Memory stays bounded when ClickHouse is stalled.
- [ ] The pipeline resumes automatically once ClickHouse recovers.

**Depends on:** Issue 1.5.

---

# Phase 3 - Observability and demo

Make the pipeline visible. This phase doubles as the demo and the "frontend":
Grafana on ClickHouse, no SPA and no WASM.

### Issue 3.1 - Prometheus metrics from the worker

**Goal:** Expose the numbers that prove the pipeline is healthy: throughput,
lag, batch behavior, and errors.

**Tech stack:**
- The Prometheus Go client (`prometheus/client_golang`).
- An HTTP `/metrics` endpoint on the worker.

**What to do:**
1. Expose counters and gauges: events consumed, rows inserted, batches flushed,
   flush latency (histogram), consumer lag, and `dlq_total`.
2. Serve `/metrics` on a configurable port.

**Acceptance criteria:**
- [ ] `/metrics` exposes throughput, lag, batch latency, and error metrics.
- [ ] Consumer lag is observable and goes to zero when caught up.

**Depends on:** Issue 1.6.

### Issue 3.2 - Add Prometheus and Grafana to the stack

**Goal:** Scrape the worker and have a place to build dashboards, with zero
manual setup on a fresh clone.

**Tech stack:**
- Prometheus and Grafana services in Docker Compose.
- Provisioned datasources and dashboards (config files, not click-ops).

**What to do:**
1. Add Prometheus scraping the worker's `/metrics`.
2. Add Grafana with a provisioned Prometheus datasource.
3. Build a pipeline-health dashboard: throughput, lag, flush latency, DLQ rate.

**Acceptance criteria:**
- [ ] Prometheus scrapes the worker and Grafana loads on a fresh `compose up`.
- [ ] A health dashboard shows throughput, lag, and errors.

**Depends on:** Issue 3.1.

### Issue 3.3 - Grafana ClickHouse dashboard (the analytics view)

**Goal:** The payoff. An analytics dashboard reading ClickHouse that visibly
updates as Postgres changes, which is the whole reason the pipeline exists.

**Tech stack:**
- The Grafana ClickHouse datasource plugin (provisioned).
- Queries over the ClickHouse current-state view.

**What to do:**
1. Provision a ClickHouse datasource in Grafana.
2. Build an analytics dashboard over the demo schema (for example: orders over
   time, revenue, top customers), querying current state with `FINAL` and
   `_is_deleted = 0`.
3. Confirm the panels refresh and reflect live writes to Postgres.

**Acceptance criteria:**
- [ ] Grafana queries ClickHouse and renders the analytics dashboard.
- [ ] Panels reflect live Postgres changes within seconds.

**Depends on:** Issue 3.2, Issue 1.6.

### Issue 3.4 - End-to-end demo and load generator

**Goal:** A one-command demo that makes the pipeline self-evidently work, which
is what sells the project to a reviewer.

**Tech stack:**
- A small Go or SQL load generator writing to Postgres.
- The full running stack.

**What to do:**
1. Write a generator that issues a steady stream of inserts/updates/deletes to
   the demo schema.
2. Document the demo: `compose up`, register the connector, start the worker,
   run the generator, open Grafana, watch numbers move.
3. Capture a screenshot or short clip for the README.

**Acceptance criteria:**
- [ ] Running the generator visibly drives the Grafana analytics dashboard.
- [ ] The demo steps are documented end to end.

**Depends on:** Issue 3.3.

---

# Phase 4 - Polish and docs

Ship it. Make it easy for a stranger to run and understand in five minutes.

### Issue 4.1 - README with architecture, quickstart, and design decisions

**Goal:** The front door. A reviewer should understand what this is, run it, and
see why the choices were made, without reading the code.

**Tech stack:**
- Markdown, an architecture diagram, and a quickstart.

**What to do:**
1. Write an architecture diagram (Postgres to Debezium to Kafka to Go to
   ClickHouse to Grafana) and a one-paragraph "what and why".
2. Add a copy-paste quickstart: bring the stack up, register the connector,
   start the worker, run the demo.
3. Add a "Design decisions" section: why Debezium over a from-scratch reader,
   why ClickHouse, why `ReplacingMergeTree`, why a Go consumer instead of the
   off-the-shelf ClickHouse sink connector, and the offset-commit ordering.

**Acceptance criteria:**
- [ ] The README explains the architecture and the why in plain language.
- [ ] A stranger can run the demo from the quickstart alone.
- [ ] Design tradeoffs are stated honestly, including the off-the-shelf alternatives.

**Depends on:** Issue 3.4.

### Issue 4.2 - Configuration and operability pass

**Goal:** No hard-coded hosts or magic numbers. Every knob is documented and set
from the environment.

**Tech stack:**
- Environment-based config and a documented `.env.example`.

**What to do:**
1. Ensure brokers, topic, ClickHouse DSN, batch size, flush interval, and ports
   all come from config with sane defaults.
2. Document every variable in `.env.example` and the README.

**Acceptance criteria:**
- [ ] All connection and tuning values are configurable, no hard-coded hosts.
- [ ] Every variable is documented with its default.

**Depends on:** Issue 4.1.

### Issue 4.3 - End-to-end integration test in CI (stretch)

**Goal:** Prove the whole pipeline in CI, not just unit tests, so correctness is
continuously verified.

**Tech stack:**
- `testcontainers-go` or a compose-based CI job.
- A scripted Postgres-write to ClickHouse-read assertion.

**What to do:**
1. Spin up the stack in CI (compose or testcontainers).
2. Write rows to Postgres, wait for drain, assert the ClickHouse current-state
   view matches.
3. Keep it as a separate, possibly slower CI job so unit tests stay fast.

**Acceptance criteria:**
- [ ] CI runs an end-to-end Postgres-to-ClickHouse assertion.
- [ ] The job is green and isolated from the fast unit-test job.

**Depends on:** Issue 2.3.

---

# Future work (explicitly out of scope)

Listed so reviewers see the ceiling without expecting it of an intern project:

- Additional sources: MySQL (binlog) and MongoDB (change streams), each added as
  another Debezium connector with the same Go landing path.
- Multi-tenancy: per-tenant topics, ACLs, and credential isolation (Vault).
- A control-plane API (gRPC + REST) and a Kubernetes operator to manage
  connectors as custom resources, including a finalizer that drops replication
  slots on teardown so none are orphaned.
- Schema evolution handling beyond additive columns.
- A from-scratch Postgres `pgoutput` reader, replacing Debezium, if the goal
  ever shifts to demonstrating low-level replication-protocol mastery.
