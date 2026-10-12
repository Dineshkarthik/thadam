# Spike: Sparkplug B prototype and OPC UA PubSub evaluation

| | |
|---|---|
| Status | Complete (evaluation for product.md Phase 3 "Sparkplug B / OPC UA PubSub evaluation"; no ADR proposed yet) |
| Date | 2026-10-12 |
| Code | `spikes/sparkplug-eval/` (standalone Cargo project, own `[workspace]`, excluded by root `exclude = ["spikes"]`) |
| Subject | Sparkplug B (Sparkplug spec 3.0.0, `spBv1.0` namespace) decode + OPC UA PubSub (OPC UA Part 14) desk research |
| Recommendation in one line | Build `decode: sparkplug_b` in the transform chain (structured in §5); do **not** build OPC UA PubSub now (§6) |

Everything below either ran on this machine (quoted) or is marked as desk research. No data left the machine: the broker was a local container, no cloud resources were touched, and crate/doc lookups were limited to crates.io, GitHub raw docs, and the RustSec database via `cargo deny`.

## 1. Environment

| Item | Value |
|---|---|
| Host | Apple Silicon, macOS (arm64) |
| Toolchain | rustc/cargo 1.99.0 (`b940084d7e`, the repo-pinned toolchain), rustfmt 1.10.0, cargo-deny 0.20.2 |
| Container runtime | Docker 29.8.2 |
| Broker | `eclipse-mosquitto:2` = Mosquitto 2.1.2, `sha256:38c0da4f2ef84284d47b3b3eeea1cb3bdeabe81ee10caf0cd5c5ff61ee3ea408`, anonymous listener, `127.0.0.1:18883` |
| Spec reference | Sparkplug specification 3.0.0, `eclipse-sparkplug/sparkplug` master (fetched 2026-10-12); normative statements cited by their `[tck-id-*]` markers |

## 2. What was built (`spikes/sparkplug-eval/`)

```text
proto/sparkplug_b.proto   vendored from Eclipse Tahu (eclipse-tahu/tahu, sparkplug_b/sparkplug_b.proto), EPL-2.0 header intact — see §4.5 license note
build.rs                  protox compile (pure Rust, no protoc) + prost-build — the thadam-grpc pattern
src/topic.rs              spBv1.0 topic namespace parser (unit-tested)
src/decode.rs             stateful consumer: BIRTH tables, bdSeq sessions, seq-gap detection, DEATH staleness (unit-tested)
src/bin/publish.rs        13-message scenario publisher (rumqttc blocking client)
src/bin/subscribe.rs      subscriber: decodes the scenario, checks 13 invariants, measures per-message decode cost
deny.toml                 mirror of the root policy (ADR-0020)
```

Dependencies (all version-consistent with the root workspace): `prost` 0.14.4, `bytes` 1.12.1, `rumqttc` **=0.25.1** default-features=false (exactly what `thadam-mqtt` builds), build-time `protox` 0.10.0 + `prost-build` 0.14.4. **The decoder library itself needs only `prost`+`bytes`; `rumqttc` is used by the scenario binaries because the spike owns its transport.**

## 3. Sparkplug B findings

### 3.1 Wire format and prost

The payload is one protobuf message (`Payload { timestamp, metrics[], seq, uuid, body }`) in proto2 syntax with `oneof value` per metric and a top-level `DataType` enum. prost decodes it as-is; `protox` compiles the vendored `.proto` with no `protoc` binary. One limitation to record: the proto declares `extensions N to max` ranges (third-party extension points). prost has no proto2-extension API, so extension fields decode as unknown and are skipped. Sparkplug B core metrics never use them; if a deployment proves to need them, we would have to revisit (open question O-6).

### 3.2 Protocol facts that drive the design (verified against the spec, TCK ids in brackets)

- Every edge message (NBIRTH, NDATA, NCMD, DBIRTH, DDATA, DDEATH, DCMD) **MUST be QoS 0, retain false** (`topics-nbirth-mqtt`, `topics-ndata-mqtt`, `topics-ncmd-mqtt`, `topics-dbirth-mqtt`, `topics-ddata-mqtt`, `topics-ddeath-mqtt`, `topics-dcmd-mqtt`). NBIRTH **MUST** carry seq 0 (`topics-nbirth-seq-num`); subsequent messages carry `previous+1` with 255→0 wrap.
- The edge node's death certificate is its **MQTT Will: Will QoS MUST be 1, Will retain false**, Will payload = protobuf with a metric named `bdSeq` (datatype INT64), and the Will's `bdSeq` equals the `bdSeq` of the NBIRTH published right after that CONNECT; `bdSeq` increments per CONNECT with 255→0 wrap (`message-flow-edge-node-birth-publish-will-message-*`, `payload-bdSeq`, `nbirth-payload-bdSeq`).
- On an NDEATH whose `bdSeq` matches the current session, all metrics of the node (and its devices) go **STALE**; an NDEATH with a stale/duplicate `bdSeq` MUST be ignored (§5.2 edge-node session termination).
- Aliases: unique across the edge node's entire metric set (`payloads-alias-uniqueness`); BIRTH metrics MUST carry name **and** alias (`payloads-alias-birth-requirement`); DATA/CMD metrics MUST carry **only** the alias (`payloads-alias-data-cmd-requirement`).
- The host application publishes its own birth/death on `spBv1.0/<group>/STATE/<host_id>` as **retained JSON, QoS 1** (`host-topic-phid-birth-qos`, `-retain`, `-death-*`). Only a host application publishes STATE; a passive recorder such as Thadam MUST NOT publish one unless a host mode is explicitly configured (§5.5).
- seq crosses BIRTH/DATA/DEATH of the node uniformly: DBIRTH/DDATA consume the same 0..=255 stream as NDATA (TCK `*-seq` statements read "one greater than the previous MQTT message from the Edge Node").

### 3.3 The scenario and the run

`publish.rs` plays one edge-node lifecycle against the broker: NBIRTH (bdSeq 0; node metrics temp/alias 1, pressure/2, rpm/3) → DBIRTH (dev1: motor.speed/10, motor.amps/11) → alias-only NDATA and DDATA → NDATA with seq 4 skipped → DDATA carrying unknown alias 99 → NDEATH (bdSeq 0) → NDATA after death → NBIRTH rebirth (bdSeq 1, alias 3 rebound rpm→vibration) → DBIRTH with aliases 10/11 swapped → alias-only DDATA and NDATA → one retained host STATE (JSON).

`subscribe.rs` output (release build, machine of §1; messages are 15–110 B):

```text
msg 01 NBIRTH     spBv1.0/plantA/NBIRTH/edge1       102B  80.00µs Birth
msg 03 NDATA      spBv1.0/plantA/NDATA/edge1         45B  15.33µs  temp, pressure
msg 04 DDATA      spBv1.0/plantA/DDATA/edge1/dev1    45B  14.83µs  motor.speed, motor.amps
msg 05 NDATA      spBv1.0/plantA/NDATA/edge1         25B   9.29µs  rpm   [GAP expected seq 4, got 5]
msg 06 DDATA      spBv1.0/plantA/DDATA/edge1/dev1    45B  27.33µs  motor.speed, alias:99  alias 99 not in the BIRTH table (2 aliases)
msg 07 NDEATH     spBv1.0/plantA/NDEATH/edge1        29B  15.38µs Death
msg 08 NDATA      spBv1.0/plantA/NDATA/edge1         27B  15.46µs  temp  data arrived for a scope past its DEATH (stale)
msg 09 NBIRTH     spBv1.0/plantA/NBIRTH/edge1       110B  21.88µs Rebirth  rebirth: bdSeq 1 replaces all alias tables
msg 11 DDATA      spBv1.0/plantA/DDATA/edge1/dev1    27B  17.58µs  motor.amps
msg 12 NDATA      spBv1.0/plantA/NDATA/edge1         27B  14.92µs  vibration
msg 13 STATE      spBv1.0/plantA/STATE/thadam-host-01 15B   3.17µs   host STATE passthrough (host-application JSON; observed, not decoded)
--- summary ---
messages: 13/13  protobuf: 12  retained-state: 1
decode+state update: n=12 mean 24.27µs p50 15.46µs p95 40.29µs max 80.00µs
protobuf decode only: n=12 mean 1.32µs p50 0.46µs p95 1.50µs max 9.71µs
checks: 13/13 PASSED
RESULT: PASS
```

### 3.4 Measurement reading

n=12: indicative, not a benchmark. The shape of the result matters more than the digits:

- **Bare protobuf decode is cheap** (p50 0.46 µs/message at 25–110 B). prost is not the bottleneck and never will be at Sparkplug message sizes.
- **Stateful resolution costs ~15 µs p50** in this prototype — almost all of it `Box<str>` name clones per resolved metric plus HashMap churn. A production decoder in `thadam-transform` would intern names (`Sym`) and borrow from the BIRTH table; the per-metric cost should land near protobuf-only territory. Even unoptimized, ~15 µs/message is roughly 60k messages/s single-threaded — far above realistic edge traffic for one connection.
- BIRTH/DEATH bookkeeping (80/40/27 µs on messages 1/2/6 — first-touch HashMap growth) is on rare messages and irrelevant to the data rate.

### 3.5 Dependency weight and licenses

- `cargo deny check` with the spike's `deny.toml` (mirror of `deny.toml` policy): **advisories ok, bans ok, licenses ok, sources ok** (output in §7). One non-error warning: the `target-lexicon` exception inherited from the root file is unmatched in this tree.
- Tree sizes measured here: 38 unique crates in the runtime tree, 65 with build deps. But the *marginal* weight for the workspace is what matters: `prost`, `bytes`, `prost-build`, `protox` are **already in the root `Cargo.lock`** (via `tonic*`/`protox` for `thadam-grpc`). A chain decoder adds **zero new third-party crates** — just the generated `Payload` type (a few hundred lines) and the state machine.
- License caveat: the vendored `sparkplug_b.proto` is **EPL-2.0** (Eclipse Tahu). It is a schema file, not a crate, so `cargo deny` does not see it — but ADR-0020 currently allows EPL-2.0 "only after legal review" (stated for `paho`). This is an owner-decision blocker for merging, not for the spike; alternatives are in open question O-1.

## 4. Design analysis for Thadam

### 4.1 Where decode lives: chain `decode: sparkplug_b`, not the source

**Recommendation: a stateful decoder in `thadam-transform`, `DecodeSpec::SparkplugB`, shared per chain** (built once per chain, exactly like the `ros2` decoder is at `crates/thadam-transform/src/chain.rs:149`).

Why not source-side:

1. **Capture-first WAL stays byte-exact.** Raw frames in the WAL decode against anything later; a buggy decoder never forces a re-capture. Source-side decode would either drop raw capture or double-store.
2. The source's contract is transport: sessions, acks, TLS, windows. Sparkplug state is keyed by *(group, edge node, device)* derived from payload semantics, not connection semantics; two datasets on one MQTT connection, or one dataset spanning nothing, don't change it.
3. `decode: ros2` sets the precedent for stateful, store-backed chain decode (`mcap.schema_hash` + schema store; §4.2).
4. `match.topic` / `{topic.N}` routing and the rejects path (`DECODE_*` codes) apply unchanged per dataset.

The one source-side thing worth having: the MQTT source already tags `mqtt.qos`/`mqtt.retain`/`mqtt.dup`, so a Sparkplug dataset can abuse-check (`retain=true` on NDATA is a spec violation) without new plumbing. Not required for v1.

### 4.2 The hard part: alias state across restarts and replay

The decoder is stateful, and the needed state is **not** in the replayed bytes when replay starts mid-session: an NBIRTH from day 1 can be required to decode day-30 DDATA, while WAL retention may have trimmed day 1. The `decode: ros2` chain solves the same shape of problem with the schema store (`thadam-core/src/schema_store.rs`): schema-carrying messages write content to the data directory **before** dependent messages commit; decoders resolve by hash at replay or export time.

**Proposal (owner to confirm; details in step plan B):** a Sparkplug birth store at `<data_dir>/sparkplug/births/`:

- Key: `(group_id, edge_node_id, device_id?)` (node entries have no device part); file name = SHA-256 of the key, same family of layout as the schema store.
- Value: the raw BIRTH payload bytes (self-describing, no re-serialization ever needed), plus a small header: `bdSeq`, `seq` at BIRTH time, staleness flag, wall-clock of last update.
- Write discipline: tmp file + fsync + rename + dir fsync, copied from `SchemaStore::put`. BIRTHs are rare, so write-through is cheap.
- **Replay picks it up as a seed**: the decoder loads all entries at construction. During `Chain::warm` (crates/thadam-transform/src/chain.rs:357), replayed BIRTHs update both memory and the store; replayed DATA past the cursor resolve against seeded or replayed state. If the replayed range contains the BIRTH, the seed is simply overwritten — identical outcome; if it does not, the seed is what makes day-30 DDATA decodable.
- The store is a **cache, not truth**: the WAL holds everything the store does. Snapshot can lag the newest committed BIRTH by one crash window; the stored `bdSeq`+`seq` lets the decoder notice (DATA seq inconsistent with the seed is flagged; the next BIRTH resyncs). NDEATH also writes the entry (staleness flag set), so post-restart state stays correct across a death.
- Redistribution note: alias uniqueness is defined per **edge node** (`payloads-alias-uniqueness`), but device tables are still keyed separately from node tables to honor DDEATH scoping.

Rejected alternative: asking the edge for a rebirth (publishing NCMD "rebirth request"). That is an *active* host behavior; a passive recorder must not do it. Possible future opt-in mode, not part of the base design.

### 4.3 Gaps, rebirth, death → quality flags and `Gap` semantics

Existing surfaces: `QualityFlags` (`thadam-core/src/quality.rs`) with `SEQUENCE_GAP` (1<<6), `STALE` (1<<5), `DECODE_ERROR` (1<<7), and the source-side `mqtt.gap` attribute (session_lost/queue_full/oversize) plus `mqtt.gap_dropped`.

Mapping (one event per DATA message, §4.6; flags are event-level):

| Sparkplug observation | Thadam surface |
|---|---|
| `seq` discontinuity (expected N, got M) | `SEQUENCE_GAP` flag, reason `sparkplug.seq expected N got M`; status stays Good (values are trustworthy; ordering isn't) |
| Metric alias not in BIRTH table (or name-only where aliases required) | keep the event, mark `DECODE_ERROR` (payload only partly decoded), reason `sparkplug alias N`; metric name falls back to `alias:N` |
| DATA for a node/device past DEATH | `STALE` flag + status Uncertain, reason `sparkplug.past-ndeath`; matches the spec's "set to a STALE data quality" |
| NDEATH with non-matching bdSeq | no data event; a metric/log observation only (spec says ignore) |
| Rebirth (bdSeq changes) | not an error; the BIRTH itself is emitted as a normal event (§4.6), state tables replaced; no gap is flagged on the seq reset (reset is expected) |
| Transport session loss | untouched: the source already emits `mqtt.gap=session_lost`; complementary to `SEQUENCE_GAP` (source sees MQTT, decoder sees Sparkplug) |

Sparkplug itself gives no "Gap event" concept — the `Gap` semantics question resolves to: **a decoder-raised `SEQUENCE_GAP` quality flag on the first event after the hole**, which is exactly Thadam's existing vocabulary.

### 4.4 QoS / ack interplay with `crates/thadam-mqtt`

- Sparkplug telemetry is **QoS 0 by spec** (§3.2). Thadam's ack-after-commit machinery simply has nothing to ack for it; loss is impossible to prevent at MQTT level and is surfaced by `SEQUENCE_GAP` flags (and, for bulk drops, the source's QoS-0 byte-budget drop counter). This is consistent with the spec's own answer (rebirthes and stale data), so nothing new is owed.
- **NDEATH is QoS 1** (Will QoS 1), delivered at min(will, subscription) = QoS 1 with the default `qos: 1` topic config: it flows through the normal ack-after-commit path. STATE (if observed) is QoS 1 retained; the passive consumer just records it.
- Persistent sessions (`clean_start: false`) still help: during a Thadam disconnect the broker queues QoS 1 messages (NDEATHs from edges going down); QoS 0 telemetry is gone by protocol design, detectable via seq.
- Ordering: the decoder must key state by payload `seq`/`bdSeq`, **not** by receive order. MQTT orders within a topic; a real edge interleaves NDATA and DBIRTH/DDATA topics on one connection, and brokers preserve per-connection receipt order in practice — but `seq` is the normative ordering and it is free to use.

### 4.5 Host-application STATE messages

Thadam is a passive consumer. It may subscribe to `spBv1.0/#` and therefore sees host STATE topics; it must (a) parse STATE as JSON or pass it through, never as protobuf; (b) **never publish** STATE, NBIRTH-proxy, or command topics — that would make Thadam a competing Primary Host Application. Any future "Thadam as Sparkplug host" mode is a separate, opt-in feature with its own identity (`spBv1.0/<group>/STATE/<host_id>` retained birth+will, rebirth-request handling, `bdSeq` tracking); explicitly out of scope here.

### 4.6 Output shape: one event per DATA message

**Recommendation: one event per MQTT message**, payload as a record of `metric_name → value` (Thadam `Value::Record` — the same materialization the `decode: ros2` direct-read path uses, `ds.process_record`), event timestamp from the payload-level `timestamp` (ms epoch), attributes carrying `sparkplug.group`, `sparkplug.node`, `sparkplug.device`, `sparkplug.kind`, `sparkplug.seq`, `sparkplug.bd_seq`.

- One-event-per-metric was rejected: it multiplies event rate by metrics-per-message (commonly tens to hundreds), explodes identity churn, severs the sibling-metric atomicity that steps like `joint_state` rely on, and quality flags are event-level anyway.
- BIRTH/DBIRTH are emitted as events too (they carry the full metric set with values — a free state snapshot for sinks and replays; flag `sparkplug.kind` distinguishes them).
- NDEATH/DDEATH emit no data event by default (staleness is carried by subsequent DATA events; death fact is in metrics/logs). Make it a config toggle only if a user asks.
- Per-metric rows for wide-column sinks stay available through a later `explode` chain step over the record — a general Thadam feature, not Sparkplug-specific.

### 4.7 Config sketch — **PROPOSAL, not implemented**

```yaml
datasets:
  - id: plant-metrics
    match: { source: plc-mqtt, topic: "spBv1.0/plantA/#" }
    decode: sparkplug_b        # new DecodeSpec variant
    sparkplug:                 # all optional, shown with defaults
      emit_birth_events: true  # BIRTH/DBIRTH as data events
      emit_death_events: false # no marker events on NDEATH/DDEATH
# runtime: the chain gets <data_dir>/sparkplug/births/ the same way it gets
# <data_dir>/schemas today (ChainSpec.schema_dir precedent)
```

## 5. Recommendation and step plan

**Build Sparkplug B decode; defer OPC UA PubSub.**

- A: (owner decision O-1) settle the `.proto` license; then move `proto` + `build.rs` shape into the workspace — either a tiny `thadam-sparkplug` crate (proto + decoder, mirroring `thadam-ros2-msg`'s role) or a module in `thadam-transform`. I recommend the crate: the same decoder will be wanted by a future MCP/export path without the full chain.
- B: decoder crate — port `topic.rs`/`decode.rs` from this spike, intern metric names (`Sym`), emit `Value::Record` instead of printing; add the birth store (new small keyed-bytes store in `thadam-core` beside `SchemaStore`, or generalize it — open question O-2).
- C: chain wiring — `DecodeSpec::SparkplugB`, one decoder per chain, runtime passes `<data_dir>/sparkplug/births/`; quality-flag mapping per §4.3.
- D: MQTT docs — point Sparkplug users at `match.topic: "spBv1.0/#"` datasets; no source changes needed.
- E: tests — the spike's unit tests ported; a docker e2e mirroring `crates/thadam-mqtt/tests/docker_e2e.rs` (mosquitto:2, this scenario as fixture bytes); a replay test that starts decode from a WAL range **after** the NBIRTH and verifies alias resolution from the birth store; proptest for seq-wrap math; cargo-fuzz target for the payload decoder (ADR-0020 fuzzing rule for untrusted parsers).
- F: docs/readiness — Sparkplug B moves to Available only when C+E land; website statuses stay Planned until then.

P0 sinks (local Parquet, S3) are unaffected — events are events.

## 6. OPC UA PubSub (Part 14) — desk research

**Background.** Part 14 publishes *DataSetMessages* from *DataSetWriters* grouped under *WriterGroups*. Transports: **UADP binary over UDP** (multicast; also raw L2 Ethernet), and **MQTT** (or AMQP) as a broker transport. Encodings: **UADP (binary)** and, for MQTT/WebSocket-style links, **JSON** NetworkMessages. Metadata (`DataSetMetaData`) flows on its own topics so readers can learn schemas without configuration, and there is a key-distribution (SKS) story for secured groups. Thadam's existing OPC UA source (`thadam-opcua`, client/subscriptions over a SecureChannel, async-opcua 0.19) shares nothing with it: PubSub is connectionless one-to-many, no sessions.

**Rust crate check (done live against crates.io + the vendored 0.19 sources):**

- `async-opcua` 0.19.0 (the workspace's pinned line): **no PubSub**. Zero PubSub code in `async-opcua`, `-client`, `-core`, `-server`, `-crypto`; `async-opcua-types` only carries the generated address-space *config* structs (`PubSubConnectionDataType` etc.) any OPC UA type set has. Nothing to build on.
- `opcua` 0.12.0 (the original Slock.it crate that async-opcua forked): no PubSub; not actively developed.
- `zerodds-opcua-pubsub` 1.0.0-rc.6 (Apache-2.0, github zero-objects/zero-dds): a genuine Part-14 find — "OPC-UA Pub/Sub Part 14 (UADP) native wire stack: Part-6 binary codec, NetworkMessage/DataSetMessage types", features `std`/`alloc`, an optional `json` mapping, and optional `security` (aes-gcm). It is a **wire codec**, pre-release (rc.6) from a DDS-gateway project: no transport, no discovery client, no stability guarantee. Desk research only; not compiled here.
- `open62541` 0.12.0 / `open62541-sys` (MPL-2.0): the C library does implement PubSub (UADP/UDP; MQTT via its own plugin), but the Rust path is C bindings with bindgen — against the ADR-0020 pure-Rust/ring posture (whose MPL exception is scoped to the async-opcua crates).

**Could the MQTT+JSON mapping ride on thadam-mqtt + a decoder?** Structurally yes: JSON NetworkMessages are JSON documents (`MessageType`/`DataSetWriterId`/`Messages[]`) and would decode as a `decode: opcua_pubsub_json` chain step with no new transport. But without metadata messages or per-writer config you cannot map numeric field ids to names reliably, and the payoff is unproven — no user has asked, and the same plants usually also run Sparkplug, which this report covers properly.

**Recommendation: do not build OPC UA PubSub now.** Gate it on (a) a real user need, or (b) `zerodds-opcua-pubsub` reaching a stable release — at which point a UADP/UDP-multicast source (new transport) is a day-scale decision spike of its own. Note in readiness as Planned (evaluation done; awaiting trigger).

## 7. Verification evidence

All commands were run in `spikes/sparkplug-eval/` with `CARGO_TARGET_DIR=$PWD/target/sparkplug` (repo root). Last lines verbatim:

```text
$ rustfmt --check --edition 2024 build.rs src/lib.rs src/topic.rs src/decode.rs src/bin/publish.rs src/bin/subscribe.rs
RUSTFMT_OK

$ cargo clippy --all-targets -- -D warnings
cargo clippy: No issues found

$ cargo test
cargo test: 3 passed (4 suites, 0.00s)

$ cargo deny check
advisories ok, bans ok, licenses ok, sources ok

$ ./target/sparkplug/release/subscribe   # with ./target/sparkplug/release/publish against eclipse-mosquitto:2 on 127.0.0.1:18883
... (full output in §3.3)
checks: 13/13 PASSED
RESULT: PASS

$ docker ps -a
(no containers)
```

## 8. Open questions for the owner

- **O-1 (blocking for merge): EPL-2.0 on `sparkplug_b.proto`.** Options: (a) legal review of the vendored Tahu file (same posture as ADR-0020's `paho` deferral); (b) author a Thadam-licensed `.proto` from the field numbers the spec publishes — field numbers and message layout are protocol constants, not copyrightable expression, and the spec chapters are the normative source; (c) depend on `sparkplug-rs` 0.5.1 (EPL-2.0, same review needed) or `lb-sparkplugb-rs` 0.1.1 (Apache-2.0/MIT, single author, 0.1.x). The spike used (a)-style vendoring.
- **O-2:** birth store: new keyed-bytes store next to `SchemaStore`, or generalize `StoredSchema` to carry non-UTF-8 bodies? (SchemaStore's `text: String` cannot hold protobuf bytes.)
- **O-3:** should NDEATH/DDEATH emit marker events (default: no)? and `emit_birth_events` default (proposal: true)?
- **O-4:** one decoder per chain (shared across that chain's datasets) — OK, or must two datasets never share Sparkplug state?
- **O-5:** OPC UA PubSub: any concrete user demand, or leave gated on zerodds stabilization (§6)?
- **O-6:** proto2 `extensions` (third-party Sparkplug extensions) are silently dropped by prost — acceptable forever, or document as a limitation?

## 9. Skipped / not checked

- Throughput/load testing beyond the 13-message correctness run (measurements are µs-scale indicators, not benchmarks).
- QoS 2, MQTT v5, TLS, multi-broker matrix, broker restarts mid-run.
- DDEATH in the live scenario (same code path as NDEATH/device scope; unit tests cover node scope only).
- Sparkplug DataSet/Template/PropertySet/property payloads: decoded on the wire path but not exercised in the scenario.
- `zerodds-opcua-pubsub` not compiled or run (desk research: crates.io metadata + feature list only). OPC UA PubSub MQTT-JSON decoding not prototyped (would be JSON-schema work if §6's trigger fires). OPC UA Part 14 SKS/security not evaluated.
- Fuzz targets (step-plan item E), conforming to the Sparkplug TCK (out of budget; TCK ids were used as the spec-citation format instead).
- Historical data (`is_historical`) and file-transfer metrics: parsed, not designed for.
- No website/readiness changes (this file is the record), no CHANGELOG entry (advised to be added with step A).
