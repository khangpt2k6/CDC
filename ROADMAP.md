# CDC Platform — Roadmap

A multi-tenant SaaS Change Data Capture platform, built from scratch.

**Stack:** Go (pipeline workers), PostgreSQL, Apache Kafka, gRPC + grpc-gateway,
Docker, Kubernetes, S3, HashiCorp Vault, OpenTelemetry + Prometheus + Grafana + Loki, CI/CD.
API-only — no frontend.

Each **phase** is a milestone; each **issue** is a focused, independently-shippable unit
of work (~1–3 days). Issues are numbered `Phase.Issue`. Every issue lists its goal, the
specific tech, ordered implementation steps, testable acceptance criteria, and dependencies.

## The five load-bearing pieces (don't compromise)

1. Replication-protocol readers (Postgres `pgoutput`, MySQL binlog, Mongo change streams)
2. The durable, replayable log (Kafka)
3. Durable offset commits — strict `produce → ack → commit` ordering
4. The schema registry (schema evolution)
5. Per-tenant lag observability

## Build order (dependency spine)

```
Envelope (P0) → PG capture (P1) → durable offset → snapshot handoff (P2)
   → more sources (P3) → tenancy (P4) → sinks (P5) → ops (P6) → control API (P7)
```

Within phases, several tracks parallelize:
- **P3:** MySQL/Mongo sources run beside the schema-registry chain; converge at the conformance suite.
- **P4:** config store is the root; topic → ACL → quota is a chain, Vault and K8s tracks run beside it.
- **P7:** an API track and an operator track run in parallel, converging at "wire API to CRDs".

---

# Phase 0 — Foundation & event contract

Lock the change-event envelope and project skeleton before any capture logic.

### Issue 0.1 — Define the change-event envelope in Protobuf

**Goal:** Lock the canonical CDC envelope that every source and sink speaks, so the contract is fixed before any producer or consumer is written. Getting this right first prevents a costly retrofit later — especially `tenant_id` and schema-drift handling, which are painful to add once data is flowing.

**Tech stack:**
- Protobuf proto3 — wire format and IDL for the envelope.
- `buf` — build/lint of the proto module.
- Well-known types (`google.protobuf.Timestamp`, optional `Struct`) — for typed timestamps and flexible row payloads.

**What to do:**
1. Create `proto/cdc/v1/envelope.proto` with package `cdc.v1` and `go_package` option pointing at the generated Go path.
2. Define `enum Op { OP_UNSPECIFIED=0; INSERT=1; UPDATE=2; DELETE=3; SNAPSHOT=4; HEARTBEAT=5; }` — reserve 0 as unspecified per proto3 convention.
3. Define a `Source` message: `connector`, `db`, `schema`, `table`, `tenant_id`, `txid` (optional). Define a `Row` representation that survives schema drift — a repeated list of typed `Field{ name, type, value }` (oneof over scalar types + null flag) rather than raw JSON, so column types are preserved.
4. Define `Envelope`: `Op op`, `Row before`, `Row after`, `Source source`, `string position` (LSN as canonical string, e.g. `0/16B3748`), `uint64 schema_id`, `google.protobuf.Timestamp ts`, `string tenant_id`.
5. Document field semantics in comments: `before` is null for INSERT, `after` is null for DELETE, both null for HEARTBEAT; `position` is the commit LSN for streamed events and the snapshot consistent point for snapshot events.
6. Reserve a block of field numbers (e.g. 50-99) for future use so additions never collide.

**Acceptance criteria:**
- [ ] `Envelope` has `op` (enum with INSERT/UPDATE/DELETE/SNAPSHOT/HEARTBEAT), `before`, `after`, `source`, `position`, `schema_id`, `ts`, `tenant_id`.
- [ ] `before`/`after` model row state as typed named fields (not opaque JSON) and represent SQL NULL distinctly from absent.
- [ ] `buf build` compiles the module clean.
- [ ] Field comments specify null-ness rules per op and the meaning of `position`.

**Depends on:** none.

### Issue 0.2 — Set up buf proto tooling and codegen

**Goal:** Wire `buf` for linting, breaking-change detection, and Go codegen so the contract cannot silently regress or break consumers. This is the guardrail that keeps the envelope stable as the platform grows.

**Tech stack:**
- `buf` — lint, breaking-change detection, codegen orchestration.
- `protoc-gen-go` — Go message structs.
- `protoc-gen-go-grpc` — Go gRPC stubs (for later control-plane services).
- `buf.yaml` / `buf.gen.yaml` — module and generation config.

**What to do:**
1. Add `buf.yaml` at repo root (or `proto/`) declaring the module, with `lint` using the `DEFAULT` ruleset and `breaking` using `FILE` change category.
2. Add `buf.gen.yaml` configuring `protoc-gen-go` and `protoc-gen-go-grpc` plugins with `paths=source_relative`, output into a versioned Go package (e.g. `internal/gen/cdc/v1`).
3. Pin plugin versions (managed mode or `tools.go` with `go.mod` entries) so codegen is reproducible across machines and CI.
4. Configure `buf breaking` to compare against the `main` branch git ref.
5. Decide and document the policy: generated code is git-tracked AND regenerated in CI with a diff check (fail if dirty) — this catches stale stubs.
6. Add a `make generate` (or task) target wrapping `buf generate`.

**Acceptance criteria:**
- [ ] `buf lint` and `buf breaking --against '.git#branch=main'` run clean.
- [ ] `buf generate` emits Go stubs into a versioned package path.
- [ ] Generated code is git-tracked and a CI diff-check fails on drift.
- [ ] Plugin versions are pinned for reproducible output.

**Depends on:** Issue 0.1 — Define the change-event envelope in Protobuf.

### Issue 0.3 — Scaffold repo skeleton (cmd/, internal/, proto/)

**Goal:** Stand up the Go module layout, directory structure, and base tooling so every later issue has a buildable home. A clean skeleton prevents ad-hoc package sprawl and import-cycle pain later.

**Tech stack:**
- Go modules — dependency and module management.
- Standard Go project layout (`cmd/`, `internal/`, `proto/`) — entrypoints vs. private packages vs. contract.
- `golangci-lint` — aggregated linting.

**What to do:**
1. `go mod init` with the canonical module path; set the Go version to a current stable release.
2. Create `cmd/worker/main.go` with a minimal entrypoint that parses config (env/flags) and logs startup — buildable placeholder, no logic yet.
3. Create `internal/` subpackages as empty-but-real packages: `internal/source`, `internal/sink`, `internal/offset`, `internal/config`, plus `internal/gen` for generated code.
4. Add `.golangci.yml` enabling a sane set (`govet`, `staticcheck`, `errcheck`, `ineffassign`, `revive`); commit it.
5. Add a `Makefile`/task runner with `build`, `test`, `lint`, `generate` targets.
6. Add `.gitignore` (build artifacts, local env files) and a top-level README stub describing layout.

**Acceptance criteria:**
- [ ] `cmd/`, `internal/`, `proto/` exist with a buildable placeholder `main`.
- [ ] `go build ./...` and `go vet ./...` pass.
- [ ] `golangci-lint run` is clean with the committed config.
- [ ] `make build` / `make lint` work from a fresh clone.

**Depends on:** none.

### Issue 0.4 — Define Source, Sink, and OffsetStore Go interfaces

**Goal:** Define the core pluggable abstractions so capture, delivery, and offset persistence are swappable and independently testable. Pinning these signatures early lets Phase 1 and Phase 2 build against stable contracts instead of refactoring call sites repeatedly.

**Tech stack:**
- Go interfaces — the abstraction boundary.
- `context.Context` — cancellation and lifecycle on every blocking call.
- Channels — streaming envelopes from `Source` to the pipeline.
- Generated `cdc.v1.Envelope` — the unit of data crossing every interface.

**What to do:**
1. In `internal/source`, define `Source`: `Start(ctx, fromPosition string) (<-chan *cdcv1.Envelope, <-chan error, error)` plus `Stop(ctx) error`. Document that `fromPosition` empty means "from the source's natural beginning / configured start".
2. In `internal/sink`, define `Sink`: `Write(ctx, batch []*cdcv1.Envelope) (ackedPosition string, err error)` — returning the highest durably-acked position so the caller can drive offset commit ordering.
3. In `internal/offset`, define `OffsetStore`: `Load(ctx, key string) (position string, err error)` and `Commit(ctx, key, position string) error`, where `key` identifies the (tenant, source) stream.
4. Document the ordering contract in interface comments: the pipeline must call `Sink.Write` → receive acked position → `OffsetStore.Commit`, never the reverse.
5. Define error sentinels (e.g. `ErrNoOffset` from `Load` on first run) so callers branch cleanly.
6. Implement in-memory fakes (`fakeSource`, `fakeSink`, `fakeOffsetStore`) in a `_test` package and write a unit test wiring them through a trivial pipeline.

**Acceptance criteria:**
- [ ] `Source` exposes envelope stream + error channel + start-from-position + stop.
- [ ] `Sink.Write` returns the durably-acked position; `OffsetStore` has `Load`/`Commit(position)`.
- [ ] Interface comments state the produce→ack→commit ordering contract and `ErrNoOffset` semantics.
- [ ] In-memory fakes implement all three and pass a unit test exercising load→stream→write→commit.

**Depends on:** Issue 0.1 — Define the change-event envelope in Protobuf.

### Issue 0.5 — Add docker-compose for local Kafka, Postgres, MinIO

**Goal:** Provide a reproducible local stack so any engineer can run the platform end to end without cloud dependencies. Correct Postgres replication config here is a prerequisite for all of Phase 1 — getting `wal_level=logical` wrong is a common time-sink.

**Tech stack:**
- docker-compose — local orchestration.
- Kafka in KRaft mode — broker without ZooKeeper.
- Postgres with `wal_level=logical` — enables logical replication slots.
- MinIO — S3-compatible object store for snapshot spill.

**What to do:**
1. Write `docker-compose.yml` with services: `kafka` (KRaft single-node, fixed advertised listeners for host access), `postgres`, `minio`, and a `minio-setup` init container.
2. Configure Postgres via command flags or a mounted `postgresql.conf`: `wal_level=logical`, `max_replication_slots=10`, `max_wal_senders=10`, `wal_sender_timeout` set sane.
3. Seed Postgres with an init SQL script that creates a sample table and a publication (so Phase 1 has something to capture).
4. Add `healthcheck` blocks to each service; make dependent steps wait on health.
5. Have `minio-setup` create the snapshot bucket on startup using `mc`.
6. Document host connection strings/ports and credentials in the README; keep all secrets to local-only dev defaults.

**Acceptance criteria:**
- [ ] `docker compose up` brings up Kafka, Postgres, MinIO all reporting healthy.
- [ ] Postgres runs with `wal_level=logical` and `max_replication_slots`/`max_wal_senders` ≥ required.
- [ ] MinIO snapshot bucket is auto-created; connection details documented in README.
- [ ] Kafka is reachable from the host with the documented bootstrap address.

**Depends on:** none.

### Issue 0.6 — Green CI on the skeleton

**Goal:** Establish the PR pipeline that builds, lints, tests, and verifies the proto contract on every change, so regressions are caught before merge from day one. Green CI on the skeleton is the baseline every subsequent issue must keep passing.

**Tech stack:**
- CI/CD (GitHub Actions) — pipeline runner.
- `buf` — `lint` and `breaking` checks in CI.
- `golangci-lint` — lint gate.
- `go test` — unit-test gate.

**What to do:**
1. Add a workflow triggered on PRs and pushes to `main` with a single job (matrix optional) on a pinned Go version.
2. Steps in order: checkout (full history for `buf breaking`), set up Go with module cache, `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./...`.
3. Add proto steps: `buf lint`, `buf breaking` against `main`, and `buf generate` followed by a `git diff --exit-code` codegen-drift check.
4. Cache the Go build/module cache and the `buf` binary to keep runs fast.
5. Make every step required; configure branch protection to block merge on red.
6. Confirm the pipeline is green on the skeleton commit and document the contract ("CI must be green to merge") in the README.

**Acceptance criteria:**
- [ ] PR pipeline runs `go build`, `go test`, `golangci-lint`, `buf lint`, `buf breaking`.
- [ ] Pipeline is green on the skeleton commit.
- [ ] Proto codegen drift (`git diff` after `buf generate`) fails the build.
- [ ] Branch protection blocks merge when any step is red.

**Depends on:** Issue 0.3 — Scaffold repo skeleton; Issue 0.2 — Set up buf proto tooling and codegen; Issue 0.4 — Define Source/Sink/OffsetStore interfaces.

---

# Phase 1 — Single-source capture (PostgreSQL)

Tail one Postgres logical slot, emit envelopes to Kafka, survive restarts.

### Issue 1.1 — Establish logical replication slot and pgoutput connection

**Goal:** Connect to Postgres over the streaming replication protocol and create/attach a `pgoutput` logical slot bound to a publication. This is the tap the whole capture pipeline drinks from; reusing (not duplicating) slots on reconnect prevents WAL retention bugs and "slot already exists" failures.

**Tech stack:**
- `pgx` + `pglogrepl` — replication-protocol connection and command helpers.
- `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput` — creates the logical slot.
- `CREATE PUBLICATION` — defines which tables flow through `pgoutput`.
- `IDENTIFY_SYSTEM` / `START_REPLICATION` — handshake and stream start.

**What to do:**
1. Open a dedicated replication connection (`replication=database` in the conn string) using `pgconn`/`pglogrepl`.
2. Ensure the publication exists: create `FOR ALL TABLES` or a configurable allowlist (`FOR TABLE ...`); make scope a config option.
3. Check for the slot via `pg_replication_slots`; if absent, `CREATE_REPLICATION_SLOT` with `pgoutput` and capture the returned `consistent_point` LSN and snapshot name (needed by Phase 2). If present, reuse it — do not recreate.
4. Resolve the start LSN: use the offset from `OffsetStore.Load`; if none, fall back to the slot's confirmed/consistent point.
5. Call `START_REPLICATION` with plugin args `proto_version '1'` and `publication_names '<pub>'`.
6. Handle reconnect: on connection drop, re-`IDENTIFY_SYSTEM`, reattach to the existing slot, and resume `START_REPLICATION` from the last committed LSN. Add bounded retry with backoff.

**Acceptance criteria:**
- [ ] Worker creates the slot if absent and runs `START_REPLICATION` from the resolved start LSN.
- [ ] Publication scope (all tables vs. allowlist) is configurable.
- [ ] On reconnect, the existing slot is reused — never duplicated, no "already exists" error.
- [ ] Slot creation captures and persists/returns `consistent_point` for Phase 2 use.

**Depends on:** Issue 0.5 — Add docker-compose; Issue 0.4 — Define Source/Sink/OffsetStore interfaces.

### Issue 1.2 — Decode pgoutput messages into change events

**Goal:** Parse the binary `pgoutput` stream into internal row-change structs with correctly typed, named columns. Correct REPLICA IDENTITY and relation-cache handling here is what makes UPDATE/DELETE before-images trustworthy downstream.

**Tech stack:**
- `pglogrepl.Parse*` message parsers — decode Relation/Begin/Commit/Insert/Update/Delete/Type messages.
- Relation cache (map keyed by relation OID) — column names, types, and REPLICA IDENTITY.
- `XLogData` / `PrimaryKeepaliveMessage` handling — separate WAL data from keepalives.

**What to do:**
1. Read `CopyData` frames; branch on first byte: `XLogData` (`'w'`) carries logical messages, `PrimaryKeepalive` (`'k'`) requests status replies — handle both.
2. Parse `RelationMessage` and store in the relation cache: relation OID → {namespace, name, columns[{name, type OID, flags}], replica identity}. Refresh on every new Relation message (schema can change mid-stream).
3. On `InsertMessage`, build the `after` image from the new tuple using the cached relation columns; `before` is nil.
4. On `UpdateMessage`, build `after` from the new tuple; build `before` from the old tuple — present only when REPLICA IDENTITY is FULL or the key changed. Document that with default identity (primary key) `before` carries only key columns.
5. On `DeleteMessage`, build `before` from the old tuple (key columns under default identity, full row under FULL); `after` is nil.
6. Track `BeginMessage`/`CommitMessage`: stamp each change with the transaction's commit LSN and commit timestamp, and emit changes in commit order so per-txn LSN ordering is preserved.
7. Decode tuple column values per type, distinguishing SQL NULL, unchanged-TOAST ('u'), and present-text values.

**Acceptance criteria:**
- [ ] Relation messages are cached and used to name/type columns; mid-stream relation changes refresh the cache.
- [ ] Insert/Update/Delete decode to before/after images with REPLICA IDENTITY handled (full row when FULL, key-only otherwise) and unchanged-TOAST values handled.
- [ ] Begin/Commit boundaries are tracked; every change carries its commit LSN and commit timestamp.
- [ ] NULL vs. unchanged-TOAST vs. value are represented distinctly.

**Depends on:** Issue 1.1 — Establish logical replication slot and pgoutput connection.

### Issue 1.3 — Map decoded changes to envelopes and produce to Kafka

**Goal:** Transform decoded row changes into the Phase 0 envelope and publish to Kafka with topic/key chosen for per-row ordering and multi-tenancy. Keying by primary key guarantees same-row events stay ordered on one partition; idempotent acked writes are the foundation for the exactly-once-effective guarantee.

**Tech stack:**
- `franz-go` — Kafka client with idempotent producer support.
- `cdc.v1.Envelope` — output contract.
- Partition key = primary key bytes — per-row ordering within a partition.
- Idempotent producer (`enable.idempotence`/acks=all equivalent) — no broker-side duplicates on retry.

**What to do:**
1. Build a mapper: decoded change → `Envelope` setting `op`, `source` (db/schema/table/tenant), `position` = commit LSN string, `ts` from commit timestamp, `before`/`after` from the typed images, `schema_id` from the relation/version.
2. Derive the topic from a configurable pattern (e.g. `cdc.<tenant>.<schema>.<table>`); derive the record key from the primary-key column values so all events for one row hash to one partition.
3. Configure the `franz-go` client with idempotent production and `acks=all`; set bounded in-flight and retries.
4. Produce per transaction as a batch; collect the per-record ack results. Treat any produce error as fatal to offset advance — do NOT commit the LSN, surface the error up the pipeline.
5. Serialize envelopes with the generated proto marshaler; attach `schema_id`/content-type as a record header for consumers.
6. Return the highest acked commit LSN to the caller (satisfying the `Sink.Write` contract) only after all records in the batch are acked.

**Acceptance criteria:**
- [ ] Each change becomes an envelope with correct `op`, `source`, `position` (commit LSN), `ts`, `tenant_id`, `schema_id`.
- [ ] Records are keyed by primary key so same-row events land on one partition (ordering preserved).
- [ ] Producer is idempotent with `acks=all`; a produce error halts offset advance and propagates.
- [ ] `Sink.Write` returns the highest acked LSN only after the whole batch is acked.

**Depends on:** Issue 1.2 — Decode pgoutput messages into change events; Issue 0.1 — Define the change-event envelope.

### Issue 1.4 — Durable LSN offset commit in Postgres (exactly-once-effective)

**Goal:** Persist the confirmed LSN only after Kafka acknowledges, so restarts replay at most an in-flight batch and never silently drop changes. This ordering is the heart of the exactly-once-effective guarantee; reversing it would risk data loss on crash.

**Tech stack:**
- Postgres offset table — durable `(stream_key, confirmed_lsn)` store implementing `OffsetStore`.
- `pglogrepl.SendStandbyStatusUpdate` — tells Postgres how far we've flushed (advances slot).
- Strict ordering: produce → Kafka ack → persist LSN in Postgres → standby status update.

**What to do:**
1. Create an offset table `cdc_offsets(stream_key text primary key, confirmed_lsn pg_lsn, updated_at timestamptz)`; implement `OffsetStore.Load`/`Commit` against it with an UPSERT.
2. In the pipeline loop, enforce the ordering exactly: (a) produce batch to Kafka, (b) wait for all acks, (c) `Commit` the batch's max commit LSN to the offset table, (d) only then `SendStandbyStatusUpdate` with that LSN as flushed.
3. Use a transaction or single UPSERT for the offset write so a partial commit is impossible.
4. On startup, `Load` the confirmed LSN and pass it to `START_REPLICATION` (ties into 1.1).
5. Handle the crash window explicitly: a crash after ack but before `Commit` will replay that batch on restart — document this as at-least-once with downstream dedup by `(key, position)`; ensure no LSN advance happens for un-acked data.
6. Add a test harness that injects a crash between ack and commit and asserts replay-not-loss.

**Acceptance criteria:**
- [ ] LSN is committed to Postgres strictly after Kafka ack, and standby status update is sent strictly after the offset commit.
- [ ] On restart, replication resumes from the last committed LSN.
- [ ] A simulated crash between ack and commit replays the batch with zero data loss (test-verified).
- [ ] Offset write is atomic (single UPSERT/txn); no partial-commit state possible.

**Depends on:** Issue 1.3 — Map decoded changes to envelopes and produce to Kafka; Issue 0.4 — Define Source/Sink/OffsetStore interfaces.

### Issue 1.5 — Send replication heartbeats to advance confirmed_flush_lsn

**Goal:** Periodically send standby status updates — and force WAL on idle databases via a heartbeat table — so `confirmed_flush_lsn` advances and Postgres can recycle WAL. Without this, a slot on a low-traffic table pins WAL indefinitely and fills the disk: a classic production outage.

**Tech stack:**
- `pglogrepl.SendStandbyStatusUpdate` — periodic flush-position report.
- `PrimaryKeepaliveMessage` (reply-requested flag) — server-driven prompts to reply.
- Heartbeat table — a row updated on a timer to generate WAL the slot will see, advancing the slot even when watched tables are idle.

**What to do:**
1. Run a ticker that calls `SendStandbyStatusUpdate` on a fixed interval and immediately after each offset commit (from 1.4).
2. Honor `PrimaryKeepaliveMessage` with `ReplyRequested=true` by sending a status update right away to avoid `wal_sender_timeout` disconnects.
3. Create a dedicated heartbeat table included in the publication; on a timer, write/update a single row (`UPDATE ... SET ts=now()`), generating WAL that flows through the slot so its `confirmed_flush_lsn` can advance during idle periods.
4. When a heartbeat change is decoded, recognize it and emit a `HEARTBEAT`-op envelope (or drop it) but always treat its LSN as commit-able progress.
5. Make heartbeat interval and standby-update interval configurable; ensure heartbeat writes do not race with snapshot consistency (Phase 2) — gate appropriately.
6. Add an idle-DB soak test asserting `confirmed_flush_lsn` keeps advancing and retained WAL stays bounded.

**Acceptance criteria:**
- [ ] Standby status update is sent on a fixed interval, after each commit, and in response to reply-requested keepalives.
- [ ] `confirmed_flush_lsn` on the slot advances even when watched tables are idle.
- [ ] Retained WAL / slot lag stays bounded under an idle-DB soak test.
- [ ] Heartbeat changes are recognized and do not corrupt the envelope stream.

**Depends on:** Issue 1.4 — Durable LSN offset commit in Postgres.

### Issue 1.6 — Restart survival and crash-recovery test

**Goal:** Prove the worker resumes cleanly across process restarts and broker/DB outages with no gap and no loss. This is the integration-level confidence that the produce→ack→commit ordering and slot reuse actually hold under real failure.

**Tech stack:**
- docker-compose integration environment — real Kafka/Postgres.
- Process kill / container pause — fault injection mid-stream.
- Kafka consumer assertions — verify delivered records vs. source.

**What to do:**
1. Build an integration test that seeds writes into Postgres, runs the worker, then `SIGKILL`s it mid-stream and restarts it.
2. After restart, consume the target topics and assert every row committed-before-crash is present and the stream continues from the last committed LSN.
3. Add a Kafka-outage scenario: pause/stop the broker, generate writes, confirm no LSN advances during the outage; restore the broker and assert the backlog flushes and resumes correctly.
4. Add a Postgres-blip scenario: drop the replication connection, confirm reconnect reattaches the existing slot (ties to 1.1) and resumes from the committed LSN.
5. Assert the only duplicates are within the single replayed in-flight batch, deduplicable by `(key, position)`; assert zero missing rows against a source row-count/checksum.
6. Run the suite in CI against docker-compose so regressions are caught.

**Acceptance criteria:**
- [ ] Kill mid-stream → restart → all committed-before-crash rows present in Kafka; stream continues from last LSN.
- [ ] Kafka-unavailable-then-recovers → no LSN advance during outage; backlog resumes correctly.
- [ ] Postgres connection drop → existing slot reattached, resume from committed LSN.
- [ ] Duplicates bounded to the replayed in-flight batch; zero missing rows vs. source checksum.

**Depends on:** Issue 1.4 — Durable LSN offset commit; Issue 1.5 — Send replication heartbeats.

### Issue 1.7 — Throughput baseline benchmark

**Goal:** Measure sustained capture throughput and end-to-end lag to set a regression baseline and expose the metrics operators will rely on. Without a recorded baseline, future performance regressions are invisible until they hit production.

**Tech stack:**
- Load generator — concurrent writers driving Postgres at a defined rate.
- Prometheus client — export rows/sec, LSN lag, produce latency.
- docker-compose stack — realistic local end-to-end path.

**What to do:**
1. Write a load generator that inserts/updates at a configurable, sustained rate against the sample table(s).
2. Instrument the worker with Prometheus metrics: captured rows/sec, decode-to-produce latency histogram, Kafka produce latency, and replication lag (current WAL LSN minus confirmed LSN).
3. Compute replication lag using `pg_current_wal_lsn()` minus the slot's `confirmed_flush_lsn` (or worker's last committed LSN) and export it as a gauge.
4. Run a fixed-duration benchmark at a defined write load; record rows/sec and steady-state lag.
5. Wire Prometheus (and optionally a scrape config in compose) so the metrics are observable during the run.
6. Commit the baseline numbers and the run procedure into the repo for future comparison.

**Acceptance criteria:**
- [ ] Benchmark reports rows/sec and steady-state replication lag under a defined write load.
- [ ] Metrics (capture rate, LSN lag, Kafka produce latency) are exported to Prometheus.
- [ ] Replication lag is computed from `pg_current_wal_lsn()` vs. confirmed LSN.
- [ ] Baseline numbers and run procedure are recorded in the repo.

**Depends on:** Issue 1.3 — Map decoded changes to envelopes and produce to Kafka; Issue 1.4 — Durable LSN offset commit.

---

# Phase 2 — Snapshot + streaming handoff

Consistent initial load, seamless cutover to streaming with no gap/overlap.

### Issue 2.1 — Consistent snapshot via pg_export_snapshot + repeatable-read txn

**Goal:** Take the initial table snapshot inside a `REPEATABLE READ` transaction bound to an exported snapshot, giving a single stable point-in-time read. This consistency is the precondition for a gapless handoff to streaming; without it, rows written during the snapshot are double-counted or lost.

**Tech stack:**
- `BEGIN ISOLATION LEVEL REPEATABLE READ` — stable MVCC view for the whole snapshot.
- `pg_export_snapshot()` / `SET TRANSACTION SNAPSHOT` — share one snapshot across reader transactions/connections.
- `cdc.v1.Envelope` with `op = SNAPSHOT` — output form for snapshot rows.

**What to do:**
1. Open a snapshot-control transaction with `BEGIN ISOLATION LEVEL REPEATABLE READ`; call `pg_export_snapshot()` and capture the returned snapshot ID. Keep this transaction open for the snapshot's lifetime (it pins the snapshot).
2. For each reader connection/worker, `BEGIN ISOLATION LEVEL REPEATABLE READ; SET TRANSACTION SNAPSHOT '<id>';` so all readers observe the identical MVCC view.
3. Read target tables (initially `SELECT *`, chunking introduced in 2.3) and map each row to a `SNAPSHOT`-op envelope, setting `source`, `tenant_id`, and `position` = the snapshot's consistent LSN (provided by 2.2).
4. Ensure NULL/typed-value fidelity matches the streaming decoder's representation so snapshot and stream rows are comparable.
5. Close reader transactions when done; close the control transaction last.
6. Handle edge cases: snapshot export requires the exporting txn to stay alive; a dropped control connection invalidates the snapshot — detect and fail fast rather than silently reading an inconsistent view.

**Acceptance criteria:**
- [ ] Snapshot reads run under `REPEATABLE READ` transactions bound to one exported snapshot ID.
- [ ] Snapshot rows are emitted as `SNAPSHOT`-op envelopes with `source`/`tenant_id`/`position` set.
- [ ] Concurrent writes during the snapshot do not appear in the snapshot read.
- [ ] Loss of the snapshot-pinning transaction is detected and fails fast.

**Depends on:** Issue 1.2 — Decode pgoutput messages; Issue 1.3 — Map decoded changes to envelopes and produce to Kafka.

### Issue 2.2 — Record snapshot LSN and stream from exactly that LSN

**Goal:** Capture the LSN consistent with the snapshot and begin streaming from precisely that point, so there is no gap and no overlap between snapshot and live stream. This is the exact-handoff correctness boundary of the whole snapshot feature.

**Tech stack:**
- `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput` with snapshot export — returns `consistent_point` LSN and a snapshot name in one atomic step.
- `SET TRANSACTION SNAPSHOT` — bind the snapshot reader to the slot's exported snapshot.
- `START_REPLICATION ... <consistent_point>` — stream from exactly the snapshot LSN.

**What to do:**
1. Create the replication slot in a mode that exports its snapshot; capture both the returned `consistent_point` LSN and the snapshot name from the command result. These are guaranteed mutually consistent.
2. Use that exported snapshot name for the snapshot readers (instead of, or unifying with, the `pg_export_snapshot()` path in 2.1) so the snapshot view and the slot's LSN refer to the same instant.
3. Persist the `consistent_point` LSN as the stream start position in the offset store.
4. After the snapshot completes, call `START_REPLICATION` from the recorded `consistent_point` LSN — not from "now" and not from the slot's later confirmed point.
5. Guarantee no overlap/gap: any commit at LSN ≤ consistent_point is already in the snapshot; any commit at LSN > consistent_point arrives via the stream. Document this boundary explicitly in code.
6. Write a test: insert a row during the snapshot window and assert it appears exactly once across snapshot+stream combined (deduped by key/position where it legitimately overlaps).

**Acceptance criteria:**
- [ ] The slot's `consistent_point` LSN is recorded and used as the streaming start LSN.
- [ ] The snapshot readers bind to the slot's exported snapshot (same instant as `consistent_point`).
- [ ] No committed change is dropped or double-emitted across the boundary.
- [ ] A row inserted during the snapshot window appears exactly once across snapshot+stream (test-verified).

**Depends on:** Issue 2.1 — Consistent snapshot; Issue 1.1 — Establish logical replication slot; Issue 1.4 — Durable LSN offset commit.

### Issue 2.3 — DBLog-style watermarking for chunked snapshots

**Goal:** Interleave chunked snapshot reads with the live stream using low/high watermarks so huge tables can be snapshotted without blocking streaming or holding a long transaction. The watermark reconciliation guarantees that a live update never loses to a stale snapshot row for the same key.

**Tech stack:**
- DBLog watermark algorithm — low/high watermark sentinels around each chunk.
- A watermark table — a row whose UPDATE produces a uniquely identifiable WAL event the stream can observe.
- Per-chunk PK-range selects — bounded snapshot reads.
- In-memory reconciliation window — events seen between low and high watermarks.

**What to do:**
1. Snapshot in PK-ordered chunks of bounded size; process one chunk at a time while the live stream runs concurrently.
2. For each chunk, execute the DBLog sequence: (a) write the **low watermark** (UPDATE the watermark table with a unique chunk token), (b) `SELECT` the chunk by PK range, (c) write the **high watermark** with the same token.
3. From the live stream, capture all change events observed between seeing the low-watermark WAL event and the high-watermark WAL event for that token; record their PKs in a per-chunk set.
4. Reconcile: when emitting the chunk's snapshot rows, **drop any snapshot row whose PK appears in the between-watermarks event set** — the streamed event is authoritative (it's newer). Emit the remaining snapshot rows, then let the stream events flow normally.
5. Handle deletes during the window (PK present as a delete in-window → snapshot row suppressed) and chunk-boundary correctness (PK ranges are half-open, no gaps/overlaps between chunks).
6. Verify the final per-key output equals the latest streamed state for any key touched during its chunk window, against a ground-truth comparison.

**Acceptance criteria:**
- [ ] Snapshot is read in bounded PK-range chunks while streaming runs concurrently.
- [ ] Each chunk uses low-watermark → select → high-watermark sequence via the watermark table.
- [ ] Events between low/high watermarks override snapshot rows for the same PK (no stale snapshot value wins; in-window deletes suppress the snapshot row).
- [ ] Final output for an overlapping key equals its latest streamed state, verified against ground truth.

**Depends on:** Issue 2.2 — Record snapshot LSN and stream from exactly that LSN.

### Issue 2.4 — Spill large snapshots to S3

**Goal:** Buffer large snapshot output to S3/MinIO instead of holding it in memory or overrunning Kafka retention, so tables larger than RAM can be snapshotted safely. Prevents OOM kills and lets the pipeline produce to Kafka at a controlled pace.

**Tech stack:**
- S3 (MinIO locally) — object store for spilled chunks.
- AWS SDK for Go — multipart/chunked object writes and reads.
- PK-ordered chunk objects — preserve ordering across spill and replay.

**What to do:**
1. Add a spill threshold: chunks (or accumulated snapshot bytes) above the threshold are serialized and written to S3 objects rather than buffered in memory.
2. Name objects with a sortable scheme encoding `(tenant, table, chunk-sequence/PK-range)` so read-back order is deterministic.
3. Serialize spilled rows in the same envelope-compatible form used downstream; use multipart upload for large chunks.
4. On the produce side, read spilled objects back in PK/sequence order and produce to Kafka, applying the same watermark reconciliation results from 2.3 (drop superseded PKs).
5. Bound memory: stream rows from S3 rather than loading whole objects; delete or lifecycle-expire spilled objects after successful production.
6. Soak-test against a table larger than the worker's available RAM and confirm flat memory usage.

**Acceptance criteria:**
- [ ] Chunks above the threshold are written to S3 and referenced, not held in RAM.
- [ ] Spilled chunks are read back and produced to Kafka in PK/sequence-correct order.
- [ ] Watermark reconciliation (superseded PKs dropped) is applied on read-back.
- [ ] Memory stays bounded snapshotting a table larger than available RAM (soak test); spilled objects are cleaned up.

**Depends on:** Issue 2.3 — DBLog-style watermarking; Issue 1.3 — Map decoded changes to envelopes and produce to Kafka.

### Issue 2.5 — Byte-identical snapshot verification

**Goal:** Prove that snapshot + stream output reconstructs the source table exactly, including the overlap path where live writes occur during the snapshot. This is the acceptance gate that the whole snapshot/handoff design is correct.

**Tech stack:**
- Row-hash comparison — ordered-PK + per-column hash of source vs. reconstructed sink state.
- A materializer — consumes Kafka envelopes (snapshot + stream) into a comparable end state.
- Diff reporter — emits the exact PKs that differ.

**What to do:**
1. Build a materializer that consumes the target topics and applies envelopes (snapshot rows then ordered stream changes, last-write-wins by position) into a sink table or in-memory map keyed by PK.
2. Compute a deterministic hash of the source: `ORDER BY pk`, hash each row's columns canonically, fold into a table-level digest (or compare per-PK hashes).
3. Compute the same hash over the reconstructed sink state and compare.
4. Run two scenarios: (a) quiescent table (no writes during snapshot), (b) table receiving live writes throughout the snapshot — exercising the watermark overlap path from 2.3.
5. On mismatch, output the specific differing PKs and the source-vs-sink values for each, so failures are debuggable rather than just "hashes differ".
6. Wire this as a CI integration check so regressions in the handoff logic are caught.

**Acceptance criteria:**
- [ ] Source table row-hash equals the reconstructed sink-state hash after handoff.
- [ ] Test covers a table receiving live writes during the snapshot (overlap path exercised).
- [ ] Mismatch produces a precise diff (which PKs differ, with values) for debugging.
- [ ] Verification runs as a CI integration check.

**Depends on:** Issue 2.3 — DBLog-style watermarking; Issue 2.4 — Spill large snapshots to S3.

### Issue 2.6 — Snapshot resumability after interruption

**Goal:** Let a partially completed snapshot resume from the last finished chunk instead of restarting from zero, so a crash hours into a large-table snapshot doesn't waste all the work. Critical for operating snapshots of multi-hundred-GB tables.

**Tech stack:**
- Postgres state store — persists per-table chunk progress alongside offsets.
- PK-range chunk boundaries — the resumable unit.
- Re-derived snapshot/LSN handling — reconcile resume with the original consistent point.

**What to do:**
1. Extend the state store with `snapshot_progress(stream_key, table, last_completed_pk, consistent_lsn, status)`; persist the last completed PK range as each chunk finishes (and only after that chunk is durably produced/spilled).
2. On startup, if a snapshot is `in_progress`, load `last_completed_pk` and resume chunking from the next PK range rather than chunk zero.
3. Handle the snapshot-view problem on resume: the original exported snapshot/transaction is gone after a crash. Define the policy — either keep the original `consistent_lsn` and rely on watermark reconciliation (2.3) to fix rows changed since, or document that resume re-reads remaining chunks under a new repeatable-read view while the recorded `consistent_lsn` plus watermarking still guarantees correctness.
4. Mark the snapshot `complete` only after the final chunk; then transition to pure streaming from `consistent_lsn` (2.2).
5. Ensure idempotency: re-producing a resumed chunk yields the same keyed records so downstream dedup is clean.
6. Test: kill mid-snapshot, restart, assert it resumes at the next chunk and the final result still passes byte-identical verification (2.5).

**Acceptance criteria:**
- [ ] Last completed chunk boundary is persisted as chunks finish (after durable produce/spill).
- [ ] Killing mid-snapshot and restarting resumes from the next chunk, not chunk zero.
- [ ] Resume policy for the lost snapshot view is documented and preserves correctness via the recorded LSN + watermarking.
- [ ] Resumed snapshot still passes byte-identical verification (2.5).

**Depends on:** Issue 2.3 — DBLog-style watermarking; Issue 1.4 — Durable LSN offset commit; Issue 1.6 — Restart survival.

---

# Phase 3 — Multi-source + schema evolution

Add MySQL and Mongo behind the same envelope; schema changes don't break the pipeline.

### Issue 3.1 — Add Schema Registry client + schema_id to the envelope

**Goal:** Stand up a Schema Registry and wire a Go client into every pipeline worker so that each emitted change-event envelope carries the `schema_id` of its payload schema. This is the foundation for all schema-evolution work: without a stable schema identity in the envelope, consumers cannot decode events safely across versions, and incompatible changes ship silently. Prevents the failure mode of "consumer decodes a row with a column it doesn't know about and corrupts/drops data."

**Tech stack:**
- Apicurio Registry (or Confluent Schema Registry) — central store of payload schemas, returns stable IDs.
- Go registry client (REST or gRPC) — registers/looks up schemas from workers.
- Shared Protobuf envelope — already defined; gains a `schema_id` field referencing the payload schema.
- In-process LRU cache — maps subject+version → schema_id to avoid a registry round-trip per event.

**What to do:**
1. Deploy the registry in Docker Compose and K8s (Deployment + Service), backed by its own Postgres/Kafka storage; expose readiness on a known port.
2. Add a `schema_id` (uint64 or fixed string) field to the shared envelope Protobuf and regenerate Go bindings; keep it required at publish time.
3. Implement a Go client wrapper with `RegisterSchema(subject, schema)` and `GetID(subject, version)`; choose a stable subject naming scheme (`<tenant>.<db>.<table>-value`) — document it now since Phase 4 reuses it.
4. Add an in-process cache keyed by subject+fingerprint; on cache hit return immediately, on miss call the registry and populate. Use a content fingerprint (e.g. canonicalized schema hash), not just version number, so identical schemas dedupe.
5. In the publish path, resolve `schema_id` before serialization; if resolution fails (unregistered schema), reject the event before it reaches Kafka rather than publishing `schema_id=0`.
6. Add the registry to worker health/readiness probes, but ensure already-cached schemas keep flowing during a transient registry outage (degrade, don't stall).

**Acceptance criteria:**
- [ ] Registry runs in Docker and K8s and is reachable from a worker; readiness is checked in the health probe.
- [ ] Every published envelope includes a non-zero `schema_id` resolved from the registry.
- [ ] Schema lookups are cached by subject+fingerprint; a registry outage does not block already-cached schemas.
- [ ] Unit test asserts an envelope with an unregistered schema is rejected before publish (never `schema_id=0`).

**Depends on:** none.

### Issue 3.2 — Enforce compatibility mode + reject incompatible changes at registration

**Goal:** Configure the registry's compatibility policy and route all schema registration through it so that incompatible source-schema changes fail loudly at registration time instead of corrupting the stream. This is the gate that makes "incompatible changes rejected by the registry, not silently shipped" true. Prevents the failure mode where a DROP COLUMN or type-narrowing change is published and downstream consumers break or misread fields.

**Tech stack:**
- Registry compatibility modes — `BACKWARD` recommended (new schema can read data written by the previous schema, which matches consumers replaying recent history); `FULL` if both directions needed; never `NONE` in production.
- Registry compatibility API — `PUT /config/{subject}` to set mode; registration call returns 409/422 on violation.
- Go client error mapping — translate registry rejection into a typed, surfaced worker error.

**What to do:**
1. Set default compatibility to `BACKWARD` per subject at subject-creation time; make it overridable per subject via tenant config, and assert the active mode at worker startup.
2. Document the rationale inline: `BACKWARD` allows ADD COLUMN (nullable/defaulted) but rejects DROP of a required field and incompatible type changes; explain why not `FULL` (too strict for additive-only flows) or `NONE` (no protection).
3. Route every schema change through `RegisterSchema`, which first checks compatibility via the registry's compatibility-check endpoint, then registers.
4. Map a compatibility rejection to a distinct error type; the worker must surface it (log at error, increment a metric, optionally pause that source) — never swallow and continue with the old schema_id silently.
5. Add an integration test harness that registers a baseline schema, then attempts (a) an additive change → accepted as new version, (b) a DROP COLUMN / type-narrowing change → rejected.
6. Ensure the rejection path does not advance the source checkpoint, so the offending change is re-evaluated rather than skipped.

**Acceptance criteria:**
- [ ] Default compatibility is `BACKWARD` (configurable per subject) and asserted at startup.
- [ ] Registering an incompatible schema (DROP COLUMN / type narrowing) returns an error the worker surfaces, not swallows.
- [ ] An additive change (ADD COLUMN, nullable/defaulted) registers as a new version successfully.
- [ ] Integration test covers both the accepted and rejected paths.

**Depends on:** Issue 3.1 — Add Schema Registry client + schema_id to the envelope.

### Issue 3.3 — Additive schema evolution (ADD COLUMN) with no pipeline restart

**Goal:** Detect an additive source-schema change at runtime, register the new version, and begin emitting the new shape on the same running worker — no restart, no backfill stall. This delivers the "additive evolution flows with no pipeline restart" promise and keeps capture continuous during routine DDL. Prevents the failure mode where every ALTER TABLE forces an operator-driven restart and a capture gap.

**Tech stack:**
- In-band schema-change detection — MySQL DDL events on the binlog / Mongo field-shape changes feed a schema-diff step.
- Hot-swap of active schema_id — the worker's per-table schema state updated atomically without tearing down the source connection.
- Protobuf payload — forward/backward decodable so old and new events coexist on the topic.

**What to do:**
1. Maintain a per-table "active schema" struct in worker memory keyed by `tenant.db.table`, holding the current schema and its `schema_id`.
2. On a detected schema change, build the candidate schema, call `RegisterSchema` (which runs the compatibility check from 3.2); only swap the in-memory active schema_id after a successful registration.
3. Perform the swap atomically (e.g. `atomic.Pointer` or mutex-guarded) so in-flight events either use fully-old or fully-new schema_id, never a torn state.
4. Continue capturing through the change — do not close the binlog syncer / change stream; the DDL is a record in the stream, not a reason to reconnect.
5. Handle the edge case where the new schema fails compatibility (3.2): keep emitting the old schema_id, surface the error, and do not advance past the DDL until resolved.
6. Verify no event loss/dup across the transition using source sequence/offset continuity (GTID for MySQL, resume token for Mongo).

**Acceptance criteria:**
- [ ] ADD COLUMN on a live source produces events with the new `schema_id` within one capture cycle, no process restart.
- [ ] Events captured before the change keep their old `schema_id`; consumers can decode both.
- [ ] No events are dropped or duplicated across the schema transition (verified by sequence/offset check).
- [ ] A failed compatibility check during evolution keeps the old schema active and surfaces the error rather than swapping.

**Depends on:** Issue 3.2 — Enforce compatibility mode + reject incompatible changes at registration.

### Issue 3.4 — Implement MySQL source (row-based binlog + GTID)

**Goal:** Add a MySQL capture source behind the common `Source` interface that reads row-based binlog events with GTID-based positioning and produces the identical shared envelope. This brings a second relational source online without diverging the envelope contract. Prevents the failure mode where file+pos positioning breaks after a primary failover and capture silently resumes at the wrong place.

**Tech stack:**
- `go-mysql` (siddontang) binlog syncer — streams binlog events over the replication protocol.
- MySQL server config — `binlog_format=ROW`, `binlog_row_image=FULL` for complete before/after images.
- GTID set positioning — `BinlogSyncerConfig` started from a GTID set (not file+pos) to survive failover.

**What to do:**
1. Implement the `Source` interface with a `go-mysql` `BinlogSyncer`; register as a replica with a unique `server_id`.
2. Start the stream from a GTID set (`StartSyncGTID`), never file+pos, so a primary failover doesn't desync positioning.
3. Validate server config on connect: query `binlog_format` and `binlog_row_image`; if not `ROW`/`FULL`, fail fast with an explicit error naming the offending setting.
4. Map `WRITE_ROWS`/`UPDATE_ROWS`/`DELETE_ROWS` events to the shared envelope: INSERT → after image, DELETE → before image, UPDATE → both before and after.
5. Resolve column names/types from the `TABLE_MAP` event (binlog rows are positional, not named) and feed them into the schema-detection path (3.3).
6. Handle DDL/`QUERY_EVENT`: route to schema-change detection rather than crashing the syncer; skip events for non-configured tables.
7. Emit each row event with the correct `schema_id` for its table's active schema.

**Acceptance criteria:**
- [ ] INSERT/UPDATE/DELETE on a MySQL table produce envelopes matching the shared Protobuf shape (before/after images for UPDATE).
- [ ] Capture starts and resumes from a GTID set; a server without `ROW`/`FULL` fails fast with a clear error.
- [ ] Column names/types are correctly resolved from `TABLE_MAP` (no positional-only output).
- [ ] DDL events are routed to schema detection (or safely skipped), not crashing the syncer.

**Depends on:** Issue 3.1 — Add Schema Registry client + schema_id to the envelope.

### Issue 3.5 — Persist MySQL GTID position durably + resume semantics

**Goal:** Persist the executed-GTID set in ordering with Kafka acks so worker restarts and failovers resume exactly at the source boundary with no loss and no replay of committed events. This makes MySQL capture survivable in production. Prevents the failure mode where the worker persists position before the event is durably in Kafka (data loss on crash) or where purged binlogs are silently skipped (a hole in the stream).

**Tech stack:**
- Postgres per-source checkpoint row — stores the GTID set durably.
- Kafka producer acks — `acks=all`; position persisted only after publish confirmation.
- `gtid_purged` handling — detect when required GTIDs no longer exist on the server.

**What to do:**
1. Add a per-source checkpoint row in Postgres storing the GTID set string and update it transactionally.
2. Order the commit strictly: publish event to Kafka → await `acks=all` ack → only then persist the advanced GTID set. Never persist ahead of the ack.
3. On startup, load the persisted GTID set and pass it to `StartSyncGTID`; if empty, define and document the initial behavior (snapshot vs. start-from-now).
4. Detect `gtid_purged`/`ERROR 1236` (binlogs rotated away covering needed GTIDs): stop and emit an actionable error (e.g. "required GTIDs purged, snapshot required") rather than skipping ahead.
5. Batch position updates sensibly (per transaction boundary, not per row) to bound Postgres write load while preserving the ack-before-persist invariant.
6. Add a forced-restart test: kill mid-stream, restart, assert resume from last persisted GTID with no gap and no duplicate beyond at-least-once expectations.

**Acceptance criteria:**
- [ ] GTID set is persisted only after the corresponding events are acked by Kafka (`acks=all`).
- [ ] Killing the worker mid-stream and restarting resumes from the last persisted GTID with no data loss.
- [ ] If required GTIDs are purged from the server, the worker errors with an actionable message instead of silently skipping.
- [ ] Position updates occur on transaction boundaries, not per-row, without violating ack-before-persist.

**Depends on:** Issue 3.4 — Implement MySQL source (row-based binlog + GTID).

### Issue 3.6 — Implement MongoDB source (change streams + resume tokens)

**Goal:** Add a MongoDB capture source behind the `Source` interface using change streams with persisted resume tokens for gap-free resumption, producing the identical shared envelope. This brings a document source under the same contract as the relational sources. Prevents the failure mode where, after a restart, the worker resumes "from now" and silently loses every change that happened while it was down.

**Tech stack:**
- `mongo-go-driver` change streams — watch a collection/db/deployment for change events.
- `resumeAfter` / `startAfter` + persisted resume token — exact resumption point.
- `fullDocument: updateLookup` — fetch the post-image for updates.
- Replica set requirement — change streams require a replica set or sharded cluster.

**What to do:**
1. Implement the `Source` interface opening a change stream with `fullDocument: updateLookup` so UPDATE events carry an after-image.
2. Map `insert`/`update`/`replace`/`delete` change events to the shared envelope; populate before/after where available (delete has only the document key unless pre-images are enabled).
3. Persist the resume token (in Postgres, per source) only after the corresponding event is acked by Kafka — same ack-before-persist invariant as MySQL.
4. On startup resume via `startAfter` (preferred over `resumeAfter` to handle invalidate events) using the persisted token; if none, document the start-from-now-vs-snapshot choice.
5. Detect an invalid/expired resume token (`ChangeStreamHistoryLost` / oplog rolled past the token) and surface an actionable error rather than silently restarting from now.
6. Validate the deployment is a replica set/sharded cluster on connect; a standalone fails fast with a clear message.

**Acceptance criteria:**
- [ ] insert/update/replace/delete events map to the shared envelope with before/after where available.
- [ ] Resume token is persisted after Kafka ack and used on restart via `startAfter`; no missed or duplicated events.
- [ ] An invalidated/expired resume token is detected and surfaced, not silently restarted from now.
- [ ] A single-node standalone deployment fails fast with a clear message.

**Depends on:** Issue 3.1 — Add Schema Registry client + schema_id to the envelope.

### Issue 3.7 — Source conformance test suite (envelope + interface parity)

**Goal:** Build a shared test suite that runs against every `Source` implementation (flat/existing + MySQL + MongoDB) asserting identical envelope semantics and resume behavior. This locks the "identical envelope shape across all sources" contract so future sources can't quietly diverge. Prevents the failure mode where each source drifts in field naming, op-type mapping, or before/after rules and consumers must special-case per source.

**Tech stack:**
- Table-driven Go tests — one suite parameterized over source implementations.
- testcontainers-go — Dockerized MySQL and MongoDB (replica set) fixtures per test run.
- Golden envelope fixtures — canonical expected envelopes for each op type.

**What to do:**
1. Define a conformance interface/contract test that takes a `Source` factory plus a DB-seeding hook and runs the same assertions against each source.
2. Assert envelope parity: required fields present, op-type mapping (insert/update/delete/replace), before/after image rules, and a valid non-zero `schema_id`.
3. Spin up MySQL and a MongoDB replica set via testcontainers; seed identical logical changes and compare emitted envelopes against golden fixtures (normalizing source-specific position metadata).
4. Exercise each source's resume-from-checkpoint path under a forced restart (kill stream, restart from persisted position, assert continuity).
5. Make the suite fail when a source omits a required field or mismaps an op type, so a future source addition that violates parity is caught in CI.
6. Register the suite in CI so it runs on every change.

**Acceptance criteria:**
- [ ] Each source passes the same parity suite: required fields, op types, before/after image rules, non-zero `schema_id`.
- [ ] Every source's resume-from-checkpoint path is exercised under a forced restart.
- [ ] Adding a future source that violates envelope parity fails the suite.
- [ ] Suite runs in CI against testcontainers-backed MySQL and a MongoDB replica set.

**Depends on:** Issue 3.4 — Implement MySQL source; Issue 3.6 — Implement MongoDB source.

---

# Phase 4 — Multi-tenancy & isolation

Many tenants, isolated, with noisy-neighbor protection.

### Issue 4.1 — Per-tenant config store in Postgres

**Goal:** Model tenants and their per-source configuration in Postgres as the authoritative config source, with a Go accessor layer workers read at startup and on change. This is the root of multi-tenancy: every other Phase 4 issue resolves tenant-scoped behavior from here. Prevents the failure mode of tenant identity/config living in env vars or flags, which makes onboarding a redeploy and leaks tenant boundaries into the deployment topology.

**Tech stack:**
- Postgres schema (`tenants`, `tenant_sources`, …) — authoritative tenant + source config.
- SQL migrations (golang-migrate or similar) — versioned schema changes.
- gRPC config API — CRUD for tenants/sources.
- Config versioning column — lets a worker detect staleness.

**What to do:**
1. Design tables: `tenants`, `tenant_sources` (source type, connection metadata sans secrets, target db/table set), with a monotonic `config_version` per tenant.
2. Write forward/backward migrations and wire them into startup/CI.
3. Build a gRPC API for tenant/source CRUD that writes through to Postgres and bumps `config_version` on change.
4. Implement a Go accessor that resolves a worker's complete config from a `tenant_id` alone — no tenant data in env/flags.
5. Store only non-secret connection metadata here; source DB credentials are explicitly out of scope (they go to Vault in 4.4).
6. Expose `config_version` so a running worker can detect it is stale and reload, setting up the hot-config path used by later issues.

**Acceptance criteria:**
- [ ] Tenant and source config CRUD via gRPC API, persisted in Postgres with migrations.
- [ ] A worker resolves its full config from `tenant_id` alone (no tenant data in env/flags).
- [ ] Config changes bump `config_version`; a stale worker can detect it is out of date.
- [ ] No secrets are stored in any config table.

**Depends on:** none.

### Issue 4.2 — Per-tenant Kafka topic namespacing (`cdc.<tenant>.<db>.<table>`)

**Goal:** Route every tenant's events to topics namespaced as `cdc.<tenant>.<db>.<table>` with consistent naming, sanitization, and a defined topic-creation policy. This gives each tenant an isolated topic space, which is the substrate ACLs and quotas attach to. Prevents the failure mode of cross-tenant topic collisions or one shared topic mixing tenant data.

**Tech stack:**
- Topic naming convention `cdc.<tenant>.<db>.<table>` — derived from tenant + source metadata.
- Identifier sanitization — Kafka topic name rules (max 249 chars, `[a-zA-Z0-9._-]`).
- Kafka AdminClient — provisioned topic creation with per-tenant partition/retention defaults.

**What to do:**
1. Implement a topic-name builder from tenant + db + table, applying sanitization (allowed charset, length cap, collision-safe encoding for illegal chars rather than lossy stripping).
2. Resolve tenant/db/table from the config store (4.1) at publish time; reject events whose derived topic name is invalid.
3. Decide and enforce the creation policy: prefer provisioned creation via AdminClient on tenant/source onboarding over broker auto-create (auto-create can't set per-tenant partitions/retention and bypasses ACL setup).
4. Apply per-tenant partition count and retention defaults from config at topic creation.
5. Guarantee no two tenants can map to the same topic name (encode tenant id distinctly; test with adversarial names).
6. Document the convention since 4.3 ACLs depend on the `cdc.<tenant>.` prefix.

**Acceptance criteria:**
- [ ] Events publish to the correctly namespaced topic derived from tenant + source metadata.
- [ ] Topic names are sanitized/validated (length, illegal chars) and cross-tenant collisions are impossible.
- [ ] Topic creation policy (provisioned via AdminClient vs. auto) is enforced and documented.
- [ ] Per-tenant partition/retention defaults are applied at creation.

**Depends on:** Issue 4.1 — Per-tenant config store in Postgres.

### Issue 4.3 — Per-tenant Kafka ACLs

**Goal:** Provision Kafka ACLs so each tenant's principal can only produce/consume its own `cdc.<tenant>.*` topics, enforcing data isolation at the broker rather than trusting application code. Prevents the failure mode where a bug or compromised credential lets one tenant read or write another tenant's change stream.

**Tech stack:**
- Kafka ACLs with prefixed resource pattern (`PREFIXED`, `cdc.<tenant>.`) — one rule covers all of a tenant's topics.
- Per-tenant SASL principals — distinct identity per tenant.
- Kafka AdminClient — idempotent ACL create/delete on onboarding/offboarding.

**What to do:**
1. Assign each tenant a distinct SASL principal at onboarding.
2. Create `PREFIXED` ACLs granting that principal `Write`/`Read`/`Describe` on resource pattern `cdc.<tenant>.` and the necessary consumer-group permissions — and nothing outside the prefix.
3. Make ACL provisioning idempotent and part of onboarding; mirror with clean removal on offboarding.
4. Ensure the prefix exactly matches the naming convention from 4.2 (a mismatch silently grants nothing or too much).
5. Add a negative integration test: a tenant principal attempting another tenant's topic is denied by the broker.
6. Add a positive test: the tenant principal can produce/consume its own prefixed topics.

**Acceptance criteria:**
- [ ] A tenant principal can produce/consume only its prefixed topics; cross-tenant access is denied by the broker.
- [ ] ACLs are created/removed as part of onboarding/offboarding (idempotent).
- [ ] Negative test: a tenant credential attempting another tenant's topic is rejected by the broker.
- [ ] Positive test: the tenant principal succeeds on its own `cdc.<tenant>.*` topics.

**Depends on:** Issue 4.2 — Per-tenant Kafka topic namespacing.

### Issue 4.4 — Per-tenant source DB credentials in HashiCorp Vault

**Goal:** Store and retrieve each tenant's source-DB credentials from Vault at runtime so no secret ever lands in Postgres config, env, container images, or logs. This is the security spine of multi-tenancy. Prevents the failure mode where a config dump or env leak exposes every tenant's source database.

**Tech stack:**
- Vault KV v2 (static creds) or the database secrets engine (dynamic, leased creds) — per-tenant secret storage.
- Go Vault client (`hashicorp/vault/api`) — fetch at connect time.
- Kubernetes auth or AppRole — workload identity, no static root token.

**What to do:**
1. Define the Vault path layout per tenant (e.g. `secret/data/cdc/<tenant>/source`), and decide KV v2 vs. database secrets engine — prefer the database engine for rotatable dynamic creds where the source DB supports it.
2. Configure worker auth via Kubernetes service-account auth (or AppRole), never a static root token; document the policy granting read on only that tenant's path.
3. Fetch source-DB creds by tenant at connect time and hold them only in memory; ensure they never enter logs, metrics, or error strings (redact).
4. On a missing/denied secret, fail the source connection with a clear, non-leaking error.
5. Capture the lease/TTL metadata on fetch so 4.5 can renew/rotate (record lease ID and TTL even if static for now).
6. Add a test asserting no secret material appears in config tables, env, or log output.

**Acceptance criteria:**
- [ ] Worker fetches source-DB creds from Vault by tenant at connect time; none present in config/env/logs.
- [ ] Vault auth uses Kubernetes service-account/AppRole, not a static root token.
- [ ] A missing/denied secret fails the source connection with a clear, non-leaking error.
- [ ] Lease ID/TTL is captured on fetch for downstream rotation handling.

**Depends on:** Issue 4.1 — Per-tenant config store in Postgres.

### Issue 4.5 — Credential rotation with no redeploy (lease renewal / re-fetch)

**Goal:** Handle Vault lease expiry and rotated credentials so a tenant's source connection picks up new creds without a worker redeploy or restart, and without losing the capture position. This delivers "rotatable with no redeploy." Prevents the failure mode where a lease expires unnoticed and the source connection dies permanently, or rotation forces a redeploy and a capture gap.

**Tech stack:**
- Vault lease renewal / `LifetimeWatcher` — renews leases before expiry for dynamic secrets.
- Dynamic-secret TTLs — bounded credential lifetime from the database secrets engine.
- Reconnect-on-auth-failure — re-fetch creds and reconnect the source on auth error.

**What to do:**
1. Run a Vault `LifetimeWatcher` per active lease to renew before expiry; on renewal failure, fall back to re-fetching a fresh secret rather than letting the worker die.
2. On a source auth failure (creds rotated out from under the worker), trigger a re-fetch from Vault and reconnect the source.
3. Make reconnect resume from the persisted capture checkpoint (GTID set / Mongo resume token from Phase 3) so rotation does not drop position or replay.
4. Bound and jitter renewal/retry to avoid a thundering herd against Vault when many tenants' leases expire together.
5. Surface rotation events as metrics/logs (without secret values) so operators can see rotations succeeding.
6. Add a rotation drill test: rotate a tenant credential in Vault, assert successful reconnect with no redeploy and no position loss.

**Acceptance criteria:**
- [ ] Rotating a tenant's source credential in Vault leads to a successful reconnect with no redeploy.
- [ ] Leases are renewed before expiry; on renewal failure the worker re-fetches rather than dying.
- [ ] Rotation does not drop the capture position (resume from checkpoint after reconnect).
- [ ] Renewal/retry is jittered to avoid a thundering herd against Vault.

**Depends on:** Issue 4.4 — Per-tenant source DB credentials in Vault; Issue 3.5 — Persist MySQL GTID position; Issue 3.6 — Implement MongoDB source.

### Issue 4.6 — Per-tenant Kafka quotas (noisy-neighbor / lag isolation)

**Goal:** Apply per-tenant produce/consume and request-rate quotas at the broker so one tenant's burst cannot starve others' throughput. This is broker-level noisy-neighbor protection. Prevents the failure mode where a single high-volume tenant saturates broker bandwidth and inflates every other tenant's lag.

**Tech stack:**
- Kafka client/user quotas — `producer_byte_rate`, `consumer_byte_rate`, `request_percentage` keyed by the tenant's SASL principal (user quota).
- Kafka AdminClient (`alterClientQuotas` / `describeClientQuotas`) — set and verify quotas.
- Tenant config (4.1) — quota values stored per tenant.

**What to do:**
1. Define default quota values and store per-tenant overrides in the config store (4.1).
2. On onboarding, set `producer_byte_rate`, `consumer_byte_rate`, and `request_percentage` on the tenant's user/principal via `alterClientQuotas` (keyed by the same principal used for ACLs in 4.3).
3. Make quota application idempotent and re-runnable when config changes — quotas are dynamic broker config and take effect without broker restart.
4. Verify applied quotas via `describeClientQuotas`.
5. Run a load test: one tenant exceeds its produce quota and is throttled while other tenants' throughput stays within SLO.
6. Document that quotas throttle (delay) rather than reject, and how throttling interacts with consumer lag.

**Acceptance criteria:**
- [ ] Each tenant principal has produce/consume byte-rate quotas applied and verifiable via the admin API.
- [ ] A tenant exceeding its quota is throttled without affecting other tenants' throughput (load test).
- [ ] Quota values are part of tenant config and adjustable without restarting brokers.
- [ ] Quota application is idempotent on config change.

**Depends on:** Issue 4.3 — Per-tenant Kafka ACLs.

### Issue 4.7 — Per-tenant Kubernetes worker resource limits + lag isolation

**Goal:** Run tenant workers with enforced CPU/memory requests and limits plus per-tenant lag observability so a heavy tenant cannot degrade others and operators can see isolation holding. This is compute-side noisy-neighbor protection complementing the broker quotas. Prevents the failure mode where one tenant's worker consumes a node's resources and silently inflates other tenants' lag.

**Tech stack:**
- K8s resource requests/limits + ResourceQuota/LimitRange — per-tenant namespace or pod-per-tenant isolation.
- Prometheus per-tenant metrics — consumer lag and throughput labeled by `tenant`.
- Loki labels — per-tenant log scoping.

**What to do:**
1. Choose and document the isolation boundary (per-tenant namespace with ResourceQuota, or pod-per-tenant with requests/limits); set CPU/memory requests and limits on tenant worker pods.
2. Apply `ResourceQuota`/`LimitRange` so a tenant cannot exceed its allocation and an OOM/throttle is contained to that tenant's pod.
3. Export per-tenant consumer-lag and throughput metrics with a `tenant` label to Prometheus; add Loki log labels for `tenant`.
4. Add alerts/dashboards on per-tenant lag against SLO.
5. Run a load test: one tenant saturates its CPU/memory limits and is throttled/OOM-contained while other tenants' lag stays within SLO.
6. Ensure resource allocations are derived from tenant config (4.1) so onboarding sets them without manual edits.

**Acceptance criteria:**
- [ ] Tenant workers carry CPU/memory requests + limits; OOM/throttle is contained to that tenant's pod.
- [ ] Per-tenant consumer-lag and throughput metrics are exported with a `tenant` label.
- [ ] Load test: one tenant saturating its limits leaves other tenants' lag within SLO.
- [ ] Resource allocations are derived from tenant config, not manual per-pod edits.

**Depends on:** Issue 4.1 — Per-tenant config store in Postgres.

---

# Phase 5 — Sinks & delivery

Pluggable sinks; replay on demand.

### Issue 5.1 — Define the `Sink` interface and plugin registry

**Goal:** Establish the single Go contract every sink implements and a registry that constructs sinks from config. This decouples delivery logic from the pipeline and lets every later sink ticket build against a stable surface. Without it each sink reinvents lifecycle/batching and the DLQ/backpressure work in Phase 6 has nothing uniform to hook into.

**Tech stack:**
- Go interfaces — the `Sink` contract (`Write`/`Flush`/`Close`/`Name`).
- Factory/registry pattern keyed by sink-type string — config-driven construction.
- `context.Context` — cancellation and deadline propagation into every call.

**What to do:**
1. Define `ChangeEvent` carrying `Tenant`, `Table`, `PK []byte`, `Position` (LSN/offset, monotonic), `Op` (insert/update/delete), and `Before`/`After` payloads.
2. Define `Sink` with `Write(ctx, []ChangeEvent) error`, `Flush(ctx) error`, `Close(ctx) error`, `Name() string`. `Write` takes a batch (not a single event) so sinks control their own batching downstream.
3. Define a typed error contract: a `PermanentError` wrapper (non-retryable, route to DLQ) vs ordinary errors (retryable). Sinks classify their failures using these.
4. Build a registry: `Register(typeName string, factory func(cfg Config) (Sink, error))` and `New(typeName, cfg)`; return a clear error for unknown types.
5. Implement a reference stdout/no-op sink to exercise the full lifecycle.
6. Write a conformance test helper that any sink can run (calls Write/Flush/Close, checks context cancellation is honored).

**Acceptance criteria:**
- [ ] `Sink` interface defines batch `Write`, `Flush`, `Close`, and `Name()`.
- [ ] Registry resolves a sink by config string and returns an error for unknown types.
- [ ] A no-op/stdout reference sink passes the shared conformance test.
- [ ] `ChangeEvent` carries (tenant, table, pk, position/LSN, op, before/after payload).
- [ ] `PermanentError` is distinguishable from retryable errors by callers.

**Depends on:** none.

### Issue 5.2 — Implement idempotency key + dedupe helper

**Goal:** Provide one shared, deterministic idempotency key over `(tenant, table, pk, position)` plus last-write-wins-by-position semantics, so any sink can safely retry or replay without producing duplicate or stale rows. This is the foundation that makes Phase 5 replay and Phase 6 redelivery safe. Without it, retries silently corrupt sink data.

**Tech stack:**
- Stable byte encoding (length-prefixed fields) — order-independent, collision-resistant key material.
- `Position` as a monotonic comparable (int64/uint64 LSN or Kafka offset) — drives last-write-wins.
- Optional hashing (SHA-256 / xxhash) — fixed-width key for header/column use.

**What to do:**
1. Define `IdempotencyKey(e ChangeEvent)`: length-prefix each of tenant/table/pk and concatenate; do NOT naively join with a separator (pk bytes can contain it). Document encoding so it is stable across processes and releases.
2. Provide both the raw composite key (tenant,table,pk — the row identity) and the full key including position (the event identity). Sinks dedupe on event identity; they reconcile rows on row identity.
3. Define the conflict rule: an incoming event applies only if `event.Position > stored.Position`; equal or lower positions are no-ops. Expose a `ShouldApply(incoming, stored Position) bool` helper.
4. Document semantics as "last-write-wins by position," and handle the delete case (a delete at a higher position must win over an earlier insert/update).
5. Provide unit tests covering reordering, duplicate delivery, and pk bytes containing separator-like content.

**Acceptance criteria:**
- [ ] Key is deterministic and stable across process restarts and is injective (no separator-collision).
- [ ] Replaying the same event twice produces one logical row (verified per sink).
- [ ] An event with a lower position does not overwrite a higher position.
- [ ] A higher-position delete wins over an earlier upsert.

**Depends on:** Issue 5.1 — Define the `Sink` interface and plugin registry.

### Issue 5.3 — Build the Postgres sink

**Goal:** Deliver a sink that applies CDC events to a target Postgres table with position-guarded idempotent upserts and deletes, transactionally per batch. This is the simplest fully-relational sink and serves as the correctness reference for idempotency and replay. The failure mode it prevents: a retried batch double-applying or a stale event clobbering newer data.

**Tech stack:**
- `pgx` (v5) with batch/pipeline — efficient parameterized writes.
- `INSERT ... ON CONFLICT ... DO UPDATE ... WHERE` — position-guarded upsert.
- Postgres transactions — all-or-nothing per batch.

**What to do:**
1. Configure the sink with target schema/table, primary-key column(s), and a `position` column used as the conflict guard.
2. For inserts/updates emit `INSERT ... ON CONFLICT (pk) DO UPDATE SET ... , position = excluded.position WHERE excluded.position > <table>.position`. This makes stale updates a silent no-op.
3. For deletes emit `DELETE ... WHERE pk = $1 AND position < $2` (or a tombstone with position guard, depending on target design) so an out-of-order delete cannot resurrect/erase newer data.
4. Wrap each `Write(batch)` in a single transaction; on any error roll back so the batch can be retried wholesale (safe because every statement is position-guarded).
5. Group statements by op within the batch and use `pgx.Batch` to reduce round-trips; preserve per-pk ordering by position within the batch before sending.
6. Classify errors: constraint/serialization failures → retryable; malformed-schema/type errors → `PermanentError` for DLQ.

**Acceptance criteria:**
- [ ] Insert/update/delete events apply correctly to the target table.
- [ ] Replaying a batch is a no-op (idempotent on the key).
- [ ] The position-guarded upsert rejects stale updates.
- [ ] Each batch write is transactional (all-or-nothing).

**Depends on:** Issue 5.2 — Implement idempotency key + dedupe helper.

### Issue 5.4 — Build the HTTP/webhook sink

**Goal:** Deliver a sink that POSTs batched events to a tenant-configured endpoint with bounded retry/backoff and a stable idempotency header, so customer endpoints can dedupe safely. This is the integration path for external systems. The failure mode it prevents: hammering a flapping endpoint or silently dropping events on transient errors.

**Tech stack:**
- `net/http` with a tuned `http.Client` (timeouts, connection pool) — outbound delivery.
- Exponential backoff with full jitter (e.g. `cenkalti/backoff` or hand-rolled) — retry pacing.
- `Idempotency-Key` header derived from the shared key — receiver-side dedupe.

**What to do:**
1. Configure per-tenant endpoint URL, auth (header/token), batch size, and timeout.
2. Serialize each batch to JSON (documented envelope: events array + metadata) and POST it; set `Idempotency-Key` from the full event-identity key (5.2) — for a batch, use a deterministic batch key.
3. Classify responses: 2xx success; 5xx and timeouts → retryable with exponential backoff + jitter and a max attempt/elapsed cap; 429 → retryable, honor `Retry-After`; other 4xx → `PermanentError`.
4. On exhausting retries, return a `PermanentError` (or wrapped retryable-exhausted) so Phase 6 routes it to the DLQ rather than blocking.
5. Always set a request context with deadline; ensure response bodies are drained/closed to reuse connections.
6. Make backoff parameters configurable; never retry indefinitely (bound by attempts and total elapsed).

**Acceptance criteria:**
- [ ] Events are POSTed as configurable JSON batches.
- [ ] 5xx/timeout trigger bounded retries with backoff+jitter; non-429 4xx fail fast.
- [ ] 429 responses honor `Retry-After`.
- [ ] Each request carries a stable idempotency key; permanent failures surface for DLQ routing.

**Depends on:** Issue 5.2 — Implement idempotency key + dedupe helper.

### Issue 5.5 — Build the S3 / Iceberg / Parquet sink

**Goal:** Deliver a sink that buffers events into Parquet data files on S3 and commits them atomically as Iceberg snapshots, giving a queryable lakehouse table. This is the analytics delivery path and the hardest sink to get right because of commit semantics. The failure mode it prevents: readers seeing partial/uncommitted files, or concurrent writers corrupting table metadata.

**Tech stack:**
- Parquet writer (Go) — columnar data files.
- Apache Iceberg table format + catalog — atomic metadata pointer swap, snapshot isolation.
- Optimistic concurrency on the metadata version — conflict-safe concurrent commits.
- S3 object store (MinIO locally) — durable file storage.

**What to do:**
1. Buffer events per `(tenant, table)` and flush to Parquet files partitioned accordingly; size/time-trigger flushes to bound file count.
2. Write Parquet data files to S3 FIRST (they are invisible until referenced by a snapshot) — this is the staging step.
3. Commit via the Iceberg catalog: read current metadata version, build a new snapshot referencing the new data files, and atomically swap the metadata pointer (compare-and-set on the version). This is the only point at which data becomes visible.
4. On commit conflict (another writer advanced the version), retry: re-read latest metadata, rebuild the snapshot on top, re-attempt the CAS. Bound retries with backoff.
5. On any failure before/at commit, do NOT reference the staged files; treat them as orphan candidates to be GC'd — never leave them in a snapshot.
6. Apply CDC semantics: for an append-only ingest table, deletes/updates are encoded as additional rows (op + position) and resolved at read time, or use Iceberg row-level deletes if supported — document which.

**Acceptance criteria:**
- [ ] Events are buffered and written as Parquet files partitioned by tenant/table.
- [ ] Each batch is an atomic Iceberg snapshot commit (no partial/visible-but-uncommitted files).
- [ ] Commit conflicts retry against the latest metadata version.
- [ ] Orphan data files from a failed commit are not referenced by any snapshot.

**Depends on:** Issue 5.1 — Define the `Sink` interface and plugin registry.

### Issue 5.6 — CDC→Iceberg row-count parity test

**Goal:** Prove end-to-end that a full load plus a change stream lands correctly in Iceberg and the table's row counts reconcile with the source. This is the acceptance gate for the lakehouse path and guards against silent data loss in the Parquet/commit pipeline. Without it, count drift would only surface in production analytics.

**Tech stack:**
- Query engine over Iceberg (DuckDB or Trino) — read-side verification.
- MinIO + a local Iceberg catalog — CI-friendly object store/catalog.
- Reconciliation harness (Go test) — count comparison with tolerance.

**What to do:**
1. Seed a source table, run a full snapshot through the Iceberg sink, then apply a scripted change stream (inserts/updates/deletes with positions).
2. Resolve the latest logical state in Iceberg (collapse by row identity to the highest position; honor deletes) — via a query or a view.
3. Compare `COUNT(*)` and a checksum of key columns against the source; allow eventual-consistency tolerance during streaming, require exact match after drain.
4. Run the whole flow in CI against MinIO + local catalog; fail the build on mismatch.
5. Include a reorder/duplicate-injection variant to confirm dedupe holds at read time.

**Acceptance criteria:**
- [ ] Full snapshot + change stream lands in Iceberg and is queryable.
- [ ] Per-table `COUNT(*)` matches source (exact after dedupe/drain).
- [ ] Key-column checksum matches source after drain.
- [ ] Test runs in CI against MinIO + local catalog.

**Depends on:** Issue 5.5 — Build the S3 / Iceberg / Parquet sink.

### Issue 5.7 — Implement replay-from-offset

**Goal:** Let an operator re-emit historical events by resetting a consumer-group offset to a timestamp or LSN, relying on sink idempotency for safe redelivery. This enables backfills and recovery after a sink bug without rebuilding pipelines. The failure mode it prevents: ad-hoc manual offset surgery and duplicate rows from uncontrolled replays.

**Tech stack:**
- Kafka admin client `OffsetsForTimes` — map timestamp → offset per partition.
- Explicit offset commit on a stopped consumer group — the reset mechanism.
- Idempotency helper (5.2) — makes redelivery a no-op for already-applied events.

**What to do:**
1. Require the target consumer group to be stopped/paused before reset (Kafka rejects commits for an active group member); validate no live members.
2. For a timestamp replay, call `OffsetsForTimes` for every partition to get the earliest offset at/after the timestamp; for an LSN replay, map LSN→offset via the stored position index.
3. Validate the resolved offsets exist (not negative/out-of-range); fail clearly if the timestamp predates retention.
4. Commit the resolved offsets for the group, then resume consumption — events re-flow through sinks and are deduped by position.
5. Expose this as an internal operation that Phase 7's `ReplayFrom` RPC will call; record an audit log entry (who/when/target).

**Acceptance criteria:**
- [ ] Replay resets the consumer group to the offset for a given timestamp/LSN.
- [ ] Re-emitted events are deduped by sinks (no duplicate rows).
- [ ] Replay requires the group to be stopped/paused and validates target offsets exist.
- [ ] A timestamp before retention fails with a clear error.

**Depends on:** Issue 5.3 — Build the Postgres sink; Issue 5.2 — Implement idempotency key + dedupe helper.

---

# Phase 6 — Operability & reliability

Run it in production without heroics.

### Issue 6.1 — Add dead-letter queue for undeliverable events

**Goal:** Route events that exhaust retries or fail permanently to a dedicated Kafka DLQ topic with failure metadata, so a single poison event cannot block or be silently dropped. This preserves liveness of the main pipeline and gives operators a place to inspect/redrive failures. The failure mode it prevents: head-of-line blocking and data loss.

**Tech stack:**
- Dedicated Kafka DLQ topic(s) (per tenant/sink) — quarantine channel.
- Kafka record headers — carry original topic/partition/offset, error, attempt count.
- Producer with the same delivery guarantees as the main path.

**What to do:**
1. Define DLQ topic naming/partitioning (per tenant or per tenant+sink) and provisioning.
2. On a `PermanentError` or retry exhaustion from a sink, produce the original event to the DLQ with headers: source topic/partition/offset, idempotency key, error string, attempt count, timestamp.
3. After a successful DLQ produce, commit/advance the main consumer past the offending offset so the pipeline proceeds (no head-of-line block).
4. Ensure ordering safety: only advance after the DLQ write is acknowledged (don't lose the event).
5. Emit a `dlq_depth` / `dlq_produced_total` metric (labeled by tenant/sink) for Phase 6 alerting.

**Acceptance criteria:**
- [ ] Events failing terminal/exhausted-retry are produced to the DLQ topic.
- [ ] DLQ records include original topic/partition/offset and error cause.
- [ ] The main pipeline advances past a DLQ'd event (no head-of-line block).
- [ ] DLQ depth/produced count is exposed as a metric.

**Depends on:** Issue 5.1 — Define the `Sink` interface and plugin registry.

### Issue 6.2 — Implement backpressure with bounded channels

**Goal:** Propagate slowness from a stalled sink back to the source via bounded buffers so memory stays flat under a sink outage. This keeps workers OOM-safe and lets them recover instead of crashing. The failure mode it prevents: unbounded in-memory queue growth during a sink outage leading to OOM kills.

**Tech stack:**
- Bounded Go channels (fixed capacity) between source → buffer → sink — the backpressure mechanism.
- Kafka consumer pause/resume — stop fetching when downstream is full.
- `context` cancellation — clean shutdown under pressure.

**What to do:**
1. Insert a bounded channel between the source consumer and the sink worker; capacity is configurable and fixed.
2. When the channel is full, block the producing goroutine; detect sustained fullness and call Kafka consumer `Pause` on the assigned partitions so no more records are fetched (don't just buffer in memory).
3. When the channel drains below a low-water mark, `Resume` consumption.
4. Ensure offsets are only committed after the sink confirms delivery, so a pause/crash never loses uncommitted records.
5. Add a sustained-outage soak test asserting heap stays bounded (pprof/`runtime.MemStats` sampling) — no unbounded growth.
6. Honor context cancellation so a paused worker shuts down cleanly.

**Acceptance criteria:**
- [ ] The buffer is a bounded channel; a full buffer pauses source consumption.
- [ ] A sustained sink outage holds memory flat (no unbounded growth / OOM).
- [ ] The source resumes automatically when the sink recovers.
- [ ] Offsets are committed only after confirmed delivery.

**Depends on:** Issue 6.1 — Add dead-letter queue for undeliverable events.

### Issue 6.3 — Instrument the pipeline with OpenTelemetry

**Goal:** Add OTel tracing and metrics spanning source → Kafka → sink, with trace context propagated across the Kafka boundary so one change event is one end-to-end trace. This is the backbone for debugging latency and correlating logs/metrics. Without it, cross-boundary debugging is guesswork.

**Tech stack:**
- OpenTelemetry Go SDK — spans, metrics, context.
- OTel messaging semantic conventions — standardized span/attribute names.
- Propagator over Kafka headers (`TextMapPropagator`) — context across the broker.

**What to do:**
1. Initialize an OTel tracer/meter provider with an OTLP exporter; wire a `TraceContext` propagator.
2. On the source/producer side, start a span per event/batch and inject trace context into Kafka headers using the propagator.
3. On the consumer/sink side, extract context from headers and start a linked child span so producer and consumer belong to one trace.
4. Follow messaging semantic conventions: set `messaging.system=kafka`, `messaging.destination.name`, `messaging.operation` (publish/receive/process), plus batch size attributes.
5. Attach `tenant.id` and `connector.id` as span attributes (and as metric attributes, watching cardinality).
6. Record core metrics from instrumentation: process duration, batch size, errors.

**Acceptance criteria:**
- [ ] Spans follow OTel messaging semantic conventions (system, destination, operation).
- [ ] Trace context propagates from source through Kafka to sink as one trace.
- [ ] Tenant/connector IDs are attached as span attributes.
- [ ] Process duration and error metrics are emitted.

**Depends on:** none.

### Issue 6.4 — Export Prometheus metrics and Grafana dashboards

**Goal:** Expose per-tenant operational metrics (lag, throughput, slot/binlog retention) and ship provisioned Grafana dashboards for the golden signals. This gives operators a real-time view of platform health per tenant. The failure mode it prevents: flying blind on replication lag or retention until a customer complains.

**Tech stack:**
- `prometheus/client_golang` (or OTel metrics → Prometheus exporter) — metric exposition.
- Grafana provisioning (dashboards-as-code JSON) — versioned dashboards.
- Postgres replication views / Kafka offsets — source data for lag and retention.

**What to do:**
1. Define metrics: `replication_lag_seconds`, `events_throughput_total`, `slot_retained_bytes` / `binlog_retention_bytes`, each labeled by `tenant` (and `connector`).
2. Compute lag from source position vs latest committed sink position; compute slot/binlog retention from Postgres `pg_replication_slots` (restart_lsn distance) or binlog equivalents.
3. Bound cardinality: only stable, low-cardinality labels (tenant, connector, sink type) — never per-event IDs.
4. Expose a `/metrics` endpoint; reuse instrumentation from 6.3 where possible.
5. Author Grafana dashboards as provisioned JSON (panels for lag, throughput, retention, DLQ depth) and check them into the repo.

**Acceptance criteria:**
- [ ] Per-tenant replication lag, throughput, and slot/binlog retention are exported.
- [ ] Metrics carry a `tenant` label and are cardinality-bounded.
- [ ] A provisioned Grafana dashboard renders the golden signals.
- [ ] `/metrics` endpoint is scrapeable in the local stack.

**Depends on:** Issue 6.3 — Instrument the pipeline with OpenTelemetry.

### Issue 6.5 — Ship structured logs to Loki correlated with trace IDs

**Goal:** Emit structured JSON logs carrying `trace_id`/`span_id` so logs and traces cross-link in Grafana, and ship them to Loki. This collapses the gap between "I see a slow trace" and "show me that worker's logs." The failure mode it prevents: unstructured logs that can't be correlated to a specific traced event.

**Tech stack:**
- `log/slog` — structured JSON logging.
- OTel trace context extraction — pull `trace_id`/`span_id` from the active span.
- Loki + Promtail (or OTLP log export) — log aggregation.

**What to do:**
1. Configure `slog` with a JSON handler and standard fields: `tenant`, `connector`, `level`, `msg`, timestamp.
2. Add a logging helper/middleware that, given a context, extracts the current span's `trace_id`/`span_id` and adds them to the log record.
3. Ensure all pipeline log sites take a context so correlation works on hot paths (source, sink, DLQ).
4. Ship logs to Loki via Promtail (scrape JSON) or OTLP; define labels carefully (tenant/connector as labels, not high-cardinality fields).
5. Verify in Grafana that a trace links to its logs and a Loki query by `trace_id` returns the lines.

**Acceptance criteria:**
- [ ] Logs are structured JSON with tenant and connector fields.
- [ ] Each log line carries `trace_id`/`span_id` when within a span.
- [ ] A Loki query by trace ID returns the correlated log lines.
- [ ] Trace→logs linking works in Grafana.

**Depends on:** Issue 6.3 — Instrument the pipeline with OpenTelemetry.

### Issue 6.6 — Define golden-signal alert rules

**Goal:** Codify Prometheus alerting rules for the platform's key reliability signals with sane thresholds and runbook links, so operators are paged before customers notice. This turns the dashboards into actionable alerts. The failure mode it prevents: a filling replication slot or rising DLQ going unnoticed until data loss or disk exhaustion.

**Tech stack:**
- Prometheus alerting rules (PromQL) — alert definitions.
- Alertmanager — routing/severity/silencing.
- `promtool test rules` — unit tests for alert logic.

**What to do:**
1. Author alerts: high `replication_lag_seconds`, slot/binlog retention nearing the configured limit, elevated sink error rate, and growing `dlq_depth`.
2. Use `for:` durations to avoid flapping; set thresholds per signal (e.g. retention as a percentage of max).
3. Label each alert with `severity`, `tenant`, and a `runbook` annotation URL.
4. Write `promtool test rules` cases with synthetic series proving each alert fires and recovers.
5. Wire Alertmanager routing by severity (and document tenant-scoped routing if applicable).

**Acceptance criteria:**
- [ ] Alerts fire on high replication lag, slot/binlog retention near limit, sink error rate, and DLQ depth.
- [ ] Each alert has severity, a tenant label, and a runbook annotation.
- [ ] Alert rules pass `promtool test rules`.
- [ ] Alerts clear when the condition resolves.

**Depends on:** Issue 6.4 — Export Prometheus metrics and Grafana dashboards; Issue 6.1 — Add dead-letter queue for undeliverable events.

### Issue 6.7 — Add fault-injection test suite

**Goal:** Automated tests that inject sink outages, Kafka unavailability, and slow consumers to validate that backpressure, DLQ, and alerting actually work under failure. This converts reliability claims into regression-protected guarantees. The failure mode it prevents: backpressure/DLQ code that quietly rots and fails the first real incident.

**Tech stack:**
- Toxiproxy (or container pause/network partition) — fault injection.
- Docker Compose test stack (Kafka, Postgres, MinIO, app) — realistic environment.
- Go test harness asserting on metrics/DLQ — verification.

**What to do:**
1. Stand up a Compose-based stack and a harness that can inject faults via Toxiproxy (latency, blackhole) against sink/Kafka endpoints.
2. Sink-outage scenario: blackhole the sink, assert consumer pauses (backpressure metric) and heap stays bounded; restore and assert drain.
3. Terminal-failure scenario: force `PermanentError`, assert events land in the DLQ with correct headers.
4. Slow-consumer scenario: inject latency, assert lag rises and the lag alert condition is met.
5. Recovery scenario: remove faults, assert backlog drains and offsets advance with no duplicates (idempotency holds).
6. Run as a tagged/integration CI job.

**Acceptance criteria:**
- [ ] Sink-outage scenario asserts backpressure engages and no OOM.
- [ ] Terminal-failure scenario asserts events land in the DLQ.
- [ ] Recovery scenario asserts the backlog drains, offsets advance, and no duplicates appear.
- [ ] Suite runs as an integration CI job.

**Depends on:** Issue 6.2 — Implement backpressure with bounded channels; Issue 6.1 — Add dead-letter queue for undeliverable events.

---

# Phase 7 — Control plane API (gRPC + REST gateway)

Manage connectors/tenants programmatically. API-only, no UI.

### Issue 7.1 — Define the control-plane gRPC service and protobufs

**Goal:** Author the protobuf service definition for the control plane (CreateConnector, PauseConnector, ResumeConnector, GetLag, ReplayFrom, ListTenants) and generate Go stubs with `buf`. This is the API contract every other Phase 7 ticket builds on and the source of truth for both gRPC and REST. Getting the messages right up front prevents churn across the gateway, server, and clients.

**Tech stack:**
- Protocol Buffers (proto3) — the service/message contract.
- `buf` — lint, breaking-change detection, codegen orchestration.
- `protoc-gen-go` / `protoc-gen-go-grpc` — Go server/client stubs.

**What to do:**
1. Define the service with all six RPCs and their request/response messages; include `tenant_id` where relevant and a connector spec message (source, sink type/config, table set).
2. Model `ReplayFrom` to accept either a timestamp or an LSN (oneof) and `GetLag` to return per-connector lag.
3. Configure `buf.yaml` + `buf.gen.yaml` for Go codegen; pin plugin versions.
4. Enable `buf lint` and `buf breaking` (against the main branch) in CI.
5. Document field semantics inline (comments become OpenAPI descriptions in 7.2).

**Acceptance criteria:**
- [ ] `.proto` defines all six RPCs with request/response messages.
- [ ] `buf generate` produces Go server/client stubs in CI.
- [ ] `buf lint` passes and `buf breaking` runs against main.
- [ ] `ReplayFrom` supports timestamp-or-LSN; `GetLag` returns per-connector lag.

**Depends on:** none.

### Issue 7.2 — Add grpc-gateway REST/JSON + OpenAPI generation

**Goal:** Expose the gRPC service as REST/JSON through grpc-gateway and auto-generate the OpenAPI spec, so the platform is usable by plain HTTP clients without a separate hand-written API. This satisfies the API-only product requirement. The failure mode it prevents: drift between gRPC and a manually maintained REST layer.

**Tech stack:**
- grpc-gateway — reverse proxy translating REST/JSON ↔ gRPC.
- `google.api.http` annotations — verb/path mapping in proto.
- `protoc-gen-openapiv2` — generated OpenAPI spec.

**What to do:**
1. Add `google.api.http` annotations to each RPC mapping to REST verbs/paths (e.g. `POST /v1/connectors`, `POST /v1/connectors/{id}:pause`, `GET /v1/connectors/{id}/lag`, `GET /v1/tenants`).
2. Extend `buf.gen.yaml` to generate the gateway and OpenAPI artifacts.
3. Stand up the gateway mux in front of the gRPC server (same process or sidecar); map gRPC status codes to HTTP codes correctly.
4. Serve the generated OpenAPI spec at a documented endpoint.
5. Write an integration test that exercises at least one RPC over REST/JSON end to end.

**Acceptance criteria:**
- [ ] Each RPC is reachable over REST/JSON with correct verbs/paths.
- [ ] OpenAPI spec is generated and served.
- [ ] gRPC status codes map to correct HTTP status codes.
- [ ] A REST round-trip integration test passes for one RPC.

**Depends on:** Issue 7.1 — Define the control-plane gRPC service and protobufs.

### Issue 7.3 — Implement the gRPC service with tenant auth

**Goal:** Wire the gRPC handlers to control-plane storage with enforced multi-tenant authn/authz, so tenants can only see and act on their own connectors. This is the security boundary of the whole platform. The failure mode it prevents: cross-tenant data access or connector manipulation in a shared SaaS.

**Tech stack:**
- gRPC interceptors (unary/stream) — authentication and tenant scoping.
- Postgres (`pgx`) — connector/tenant state store.
- Token/JWT or mTLS identity — caller identity → tenant mapping.

**What to do:**
1. Add an auth interceptor that authenticates the caller and resolves a `tenant_id`; reject unauthenticated/unauthorized calls with the correct gRPC codes.
2. Inject the resolved tenant into context; every handler scopes all queries by it (defense-in-depth: never trust a `tenant_id` field from the request body over the authenticated identity).
3. Implement `CreateConnector` (validate spec, persist record), `ListTenants`, `GetLag` (read from the metrics/position store), and `PauseConnector`/`ResumeConnector` state transitions.
4. Wire `ReplayFrom` to the Phase 5 replay operation.
5. Validate inputs (sink config, table set) and return precise error codes; add handler unit tests with a fake store.

**Acceptance criteria:**
- [ ] Each RPC enforces tenant scoping (no cross-tenant access).
- [ ] `CreateConnector` persists a connector record and validates input.
- [ ] `GetLag` and `ListTenants` return correct data from the store.
- [ ] Unauthenticated/unauthorized calls are rejected with correct codes.

**Depends on:** Issue 7.1 — Define the control-plane gRPC service and protobufs.

### Issue 7.4 — Define the Connector CRD and operator scaffold

**Goal:** Create the Connector CustomResourceDefinition and scaffold a controller-runtime operator, establishing the declarative resource the API will write and the operator will reconcile. This is the foundation of the operator pattern that keeps the API out of the business of spawning pods. Getting the CRD schema right early avoids painful migrations later.

**Tech stack:**
- Kubebuilder / controller-runtime — operator scaffold and reconciler framework.
- CRD OpenAPI validation schema — reject malformed specs at admission.
- `controller-gen` — CRD manifests and deepcopy generation.

**What to do:**
1. Define `Connector` types: `Spec` (tenant, source config, sink type/config, table set, desired state running/paused, replay target) and `Status` (phase, observed worker state, conditions).
2. Generate deepcopy and CRD YAML via `controller-gen`; add OpenAPI validation (required fields, enums for state/sink type).
3. Scaffold the controller-runtime manager and register a reconciler for `Connector` (no real logic yet — just the loop).
4. Ensure the CRD installs cleanly into a kind/k8s test cluster.
5. Lay groundwork for finalizers by declaring the finalizer name constant (used in 7.7).

**Acceptance criteria:**
- [ ] Connector CRD `Spec`/`Status` types are defined and installable.
- [ ] The operator scaffold builds and registers a reconciler for the CRD.
- [ ] CRD OpenAPI validation rejects malformed specs.
- [ ] CRD installs cleanly into a kind cluster in CI.

**Depends on:** none.

### Issue 7.5 — Wire API to write Connector CRDs (no direct pod spawning)

**Goal:** Make the gRPC API create/update/delete Connector CRDs only — never spawning worker pods directly — so all workload lifecycle flows through the operator. This enforces the architectural rule that the control plane is declarative. The failure mode it prevents: the API and operator both managing pods, causing races and orphans.

**Tech stack:**
- controller-runtime client (or client-go) from the API process — CRUD on the CRD.
- Kubernetes RBAC — API service account limited to Connector resources.

**What to do:**
1. Give the API a Kubernetes client and an RBAC role scoped to `Connector` CRs only (explicitly NOT Pods/Deployments) — this enforces the rule at the cluster level.
2. `CreateConnector` writes a `Connector` CR (name/namespace derived from tenant+connector id); `PauseConnector`/`ResumeConnector` patch `spec.desiredState`.
3. `ReplayFrom` patches the CR's replay target (operator/worker acts on it).
4. `DeleteConnector` deletes the CR object (teardown handled by the finalizer in 7.7).
5. Add a test/lint check asserting the API never references Pod/Deployment create APIs.

**Acceptance criteria:**
- [ ] `CreateConnector` writes a Connector CR; Pause/Resume patch its spec.
- [ ] The API never creates Pods/Deployments directly (enforced by RBAC and verified by test).
- [ ] `DeleteConnector` deletes the CR object.
- [ ] Replay is expressed as a CR spec change.

**Depends on:** Issue 7.3 — Implement the gRPC service with tenant auth; Issue 7.4 — Define the Connector CRD and operator scaffold.

### Issue 7.6 — Reconcile Connector CRD into worker pods

**Goal:** Implement the operator's reconcile loop so a Connector CR produces and maintains a worker Deployment reflecting its spec (running/paused, replay), with status conditions mirroring reality. This is where declarative intent becomes a running pipeline. The failure mode it prevents: drift between desired connector state and actual workers.

**Tech stack:**
- controller-runtime reconcile loop — converge actual → desired.
- Owner references — tie the worker Deployment to the CR for GC.
- Status conditions — observable reconcile outcome.

**What to do:**
1. In `Reconcile`, fetch the CR; create/update a worker Deployment carrying the connector config (env/configmap), with an owner reference to the CR so Kubernetes GC removes it if the CR is deleted.
2. Map `spec.desiredState`: running → replicas=1 (or N); paused → replicas=0 (or a paused flag) without deleting state.
3. Make reconcile idempotent: compute desired state and only patch on diff; return early (no-op) when nothing changed.
4. Reflect actual state into `status.conditions` (e.g. `Ready`, `Progressing`) and the observed generation.
5. Handle transient errors with requeue/backoff; ensure repeated reconciles don't thrash the Deployment.

**Acceptance criteria:**
- [ ] Creating a Connector produces a worker Deployment owned by the CR.
- [ ] Pause/Resume scales/reconfigures the worker accordingly.
- [ ] Status conditions reflect actual worker state.
- [ ] Reconcile is idempotent (no-op on unchanged spec).

**Depends on:** Issue 7.4 — Define the Connector CRD and operator scaffold.

### Issue 7.7 — Add CRD finalizer for clean teardown / no orphan slots

**Goal:** Add a finalizer so deleting a Connector first drops the Postgres replication slot and removes the worker before the CR is allowed to disappear, guaranteeing no orphaned slots. Orphan slots are the classic CDC failure: they pin WAL and eventually fill the source disk. This issue prevents that operational landmine.

**Tech stack:**
- controller-runtime finalizers — block deletion until cleanup completes.
- Postgres admin (`pg_drop_replication_slot`) — remove the slot.
- Owner references / explicit delete — worker teardown.

**What to do:**
1. Add the finalizer to the Connector on create/first-reconcile (using the constant from 7.4).
2. On deletion (CR has a `deletionTimestamp`), run ordered cleanup: stop/delete the worker, then drop the replication slot via `pg_drop_replication_slot` (do slot drop only after the worker is gone so nothing re-creates it).
3. Make each cleanup step idempotent and safe to retry (slot already dropped / worker already gone is success).
4. Remove the finalizer only after all steps succeed; the CR then deletes naturally. If a step fails, requeue and retry — the CR stays until cleanup completes.
5. Add an integration test that creates and deletes a connector and asserts `pg_replication_slots` has no leftover slot.

**Acceptance criteria:**
- [ ] Deleting a Connector triggers finalizer-driven cleanup before removal.
- [ ] The Postgres replication slot is dropped (no orphan slots remain).
- [ ] The worker Deployment is removed; the CR is gone only after cleanup succeeds.
- [ ] Cleanup is retried safely if a step fails mid-teardown.

**Depends on:** Issue 7.6 — Reconcile Connector CRD into worker pods; Issue 7.5 — Wire API to write Connector CRDs.
