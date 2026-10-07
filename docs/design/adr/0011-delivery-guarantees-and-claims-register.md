# ADR-0011: Delivery guarantees and claims register

- **Status:** Accepted
- **Date:** 2026-10-07
- **Deciders:** Project owner

**Acceptance.** Accepted by project owner on 2026-10-07 (`docs/design/owner-decisions.md`, section "ADR acceptances"). The unticked items in "Evidence and validation required" are implementation-time verification, not blockers. Claims in the register that depend on unticked evidence (for example anything about power loss, or the S1-derived rows before the hardware re-run) must not be published as measured claims until that evidence is attached.

## Context

Thadam's value proposition rests on reliability: capturing data durably at the edge and delivering it despite outages. product.md section 11 sets at-least-once delivery as the default and treats exactly-once as a sink-specific capability. Section 27 says not to claim exactly-once, zero-loss, or deterministic latency universally, because they depend on protocol, source, storage, network, and sink behavior. Section 26 requires every performance statement to be backed by reproducible benchmarks.

The architecture review checked the original plan against those rules and found several statements that cannot be guaranteed as written, and one real defect:

- **Throughput under acknowledge-after-commit.** One MQTT connection is bounded by roughly the broker's inflight window divided by commit latency. Mosquitto's default `max_inflight_messages` is 20 and EMQX's is 32 **[V]**. With a 100 ms commit that is about 200 and 320 messages per second (20 / 0.1 and 32 / 0.1). The original plan and its benchmark hypotheses missed this.
- **Backpressure moves loss.** Persistent-session queues are bounded (Mosquitto's `max_queued_messages` defaults to 1000 **[V]**), so under `block` loss can happen invisibly in the broker. Under `drop_oldest` it happens visibly in Thadam.
- **Duplicate-object defect.** In the original design a file could be finalized, uploaded, and deleted locally (`delete_after_upload`) before the WAL cursor was durably committed. A crash then regenerated the same range with different boundaries and uploaded a second object under a new key.
- **In-memory deduplication loses its state exactly when duplicates occur.** Broker redelivery happens after a crash or reconnect, which is when in-memory state is gone. Also, product.md UC-05's "remove duplicate values" is deadband filtering, not deduplication.
- **Several claims were too strong:** bounded memory (Thadam can bound only its own buffers), byte-identical replay (rotation uses wall-clock `max_age`, footer metadata includes the version, the enrichment file may change), effectively-once ClickHouse (needs specific table settings), and survival of power loss (no evidence without hardware).

This ADR records the per-hop guarantees, the decisions that determine them (D4, D7, D17, D18, D21), and a claims register that gates what Thadam may publish. It also absorbs, for now, the content of reserved ADR-0009 (the cursor-before-ship ordering; the rest of the object-store design is ARCHITECTURE.md section 8 and ADR-0013) and reserved ADR-0019 (deduplication and deadband semantics; the rest of the transform set is ARCHITECTURE.md section 7).

## Decision

### Rule

No README, benchmark post, release note, or demo text may state a guarantee that is not in the claims register below, and each statement must carry the register's conditions. A register row may be published only after its evidence is attached. A claim whose evidence changes is revised before the next release.

### Guarantees per hop

- **MQTT to WAL.** At-least-once for QoS 1 and 2 with a persistent session and manual acknowledgement after WAL commit, in receive order. Messages the broker accepts while Thadam is down survive only within the broker's queue limits and session expiry. QoS 0 is at-most-once. `ack_mode: on_receive` is at-most-once across a Thadam crash.
- **WAL durability.** With `fsync != never`, on storage that honors flush **[A]**, committed data survives a process crash and power loss. Power loss is claimed only after hardware evidence **[V]**. `fsync: never` survives a process crash only.
- **Ordering.** WAL order equals receive order per connection. There is no event-time order and no global order. After a reconnect, MQTT redelivery replays older unacknowledged messages after a gap.
- **WAL to Parquet.** No loss within the disk budget. After the commit-then-ship rule, each WAL range lands in exactly one finalized file. Duplicates from source redelivery remain unless deduplication uses a content-derived event ID.
- **Parquet to object store.** At-least-once attempts. Deterministic keys, immutable files, and create-if-absent make each object effectively written once. Conditional-put support varies by store and is probed at startup.
- **WAL to ClickHouse (P1).** At-least-once. Effectively-once only for replicated tables or when `non_replicated_deduplication_window` is set (it defaults to 0); asynchronous inserts also need `async_insert_deduplicate` **[V]**. Thadam probes table settings at startup and reports the effective delivery class rather than assuming it.
- **Replay.** Row-set equivalence, not byte identity (ADR-0003).
- **Disk full.** Loss is explicit and counted under both `on_full` policies. `on_full` is the single place inside Thadam where data can be lost.

### D4: `on_full` has no default

The operator must choose `block` or `drop_oldest`. `thadam init` writes `block` with an explanatory comment. Configuration validation fails if `on_full` is missing. Decided by the owner on 2026-10-07 (B3, D-B3-1).

- `block` withholds acknowledgements when the WAL budget is full. It moves loss to the broker, where it can happen invisibly once broker queue limits are exceeded.
- `drop_oldest` evicts the oldest WAL segments, surfaces `Gap::Evicted` records to readers, and increments `thadam_wal_evicted_bytes_total`. Loss is visible in Thadam.

### D7: `ack_mode` defaults to `on_commit`

The throughput ceiling of one connection is roughly the broker inflight window divided by commit latency. Documented examples: 20 inflight (Mosquitto) and 32 (EMQX) **[V]** with a 100 ms commit give about 200 to 320 messages per second. The owner did not select a producer-batching assumption (B3, 2026-10-07), so batching is not assumed: the ceiling is documented as a limit and is not hidden, and `on_commit` stays the default. Mitigations an operator may choose are producer-side batching (several samples per message), raising broker limits (operator controlled), MQTT 5 Receive Maximum where the broker allows it, and the opt-in `on_receive`. Shared subscriptions are a documented future option with an ordering caveat (ADR-0006, D23). The runbook covers broker settings: `max_queued_messages`, `max_inflight_messages`, and session expiry.

### D17: deduplication is best-effort in Thadam and authoritative downstream

- A bounded time-and-size window warm-starts from the WAL tail, by re-reading `[cursor - window, cursor)` through `Step::warm` without emitting. This is deterministic and needs no separate persistent store.
- Deduplication requires a content-derived event ID (`event_id.key`, ADR-0001). Enabling `dedup` without it fails validation. It runs at message level before `explode`, which keeps its memory small.
- A duplicate arriving after the window, or after `max_keys` eviction, is not detected in Thadam. Authoritative deduplication happens downstream: `ReplacingMergeTree` in ClickHouse, or `QUALIFY` or `DISTINCT ON` over Parquet. Thadam's role is stable identity plus best-effort flagging or dropping (`outcome="deduped"`).
- `on_change` (deadband) is a separate transform. It is value-based, lossy by design, and per stream. product.md section 5 (UC-05) conflates the two operations. The amendment is cataloged in `docs/design/product-amendments.md` **[OPEN]**.

### D18: commit the cursor durably, then mark a segment ship-eligible

1. The `SegmentSink` finalizes a file: close, `fdatasync`, rename from `staging/` to `ready/`, directory fsync, then a `Finalized` catalog record.
2. The runtime commits the WAL cursor to the new watermark, durably.
3. Only then is the sink told (`cursor_committed`) and an `Eligible` record appended. The shipper never uploads an ineligible file.
4. **Local deletion requires both an `Uploaded` record and a cursor at or beyond the file's end.**

Why: in the original design the upload could happen, and the local file be deleted, before the cursor was durable. After a crash the footer scan could not find the deleted file, the range was regenerated with different boundaries, and a second object was uploaded under a new key. With the ordering above, recovery finds every finalized file in `ready/`, advances the cursor to the highest contiguous finalized range, and only then marks files eligible. Details: ARCHITECTURE.md sections 8 and 11.

### D21: memory claim wording

Thadam-owned buffers are bounded by configuration. Total RSS, including `rumqttc`, `hyper`, zstd, and allocator fragmentation, is measured and published per workload under cgroup limits. `thadam validate` prints an estimate of Thadam-owned memory and warns above `runtime.memory_target`.

### Flash endurance

Example: 50 sensors at 1000 Hz with 200-byte samples is 50 x 1000 x 200 = 10,000,000 bytes per second, about 10 MB/s, or about 860 GB per day of WAL writes (86,400 seconds), before framing overhead, filesystem write amplification, and the Parquet writes that follow. Consumer SD cards wear out quickly at that rate. The owner decided on 2026-10-07 (B3, D-B3-5) that industrial SSD or eMMC that honors flush is the supported baseline and that SD cards are NOT a supported target for durability claims. No durability or endurance claim may be published for SD cards. The math is published with the register. Producer-side batching changes message counts but not payload bytes.

### Claims register

Each row gives the only allowed wording. "Evidence" is what must be attached before the claim is published.

| Claim | Allowed wording | Conditions | Evidence required before publishing |
|---|---|---|---|
| No loss while Thadam is down | "QoS 1 and 2 messages accepted by the broker while Thadam is down are delivered after restart, within broker queue limits and session expiry." | Persistent session, stable client ID, manual ack after commit. Not for QoS 0. | Kill -9 test while publishing continues (PR12); broker limit settings documented |
| Survives process crash | "Acknowledged data survives a process crash." | `fsync != never`, ack after commit | Kill -9 loops with zero committed-ticket loss over 1,000 iterations (PR6); S1 results |
| Survives power loss | "Acknowledged data survives power loss on storage that honors flush." | `fsync != never`; flush-honoring SSD or eMMC **[A]**; Linux | Hardware power-cut rig, 500 cycles; LazyFS results are supporting only |
| Bounded memory | "Thadam-owned buffers are bounded by configuration; RSS is measured per workload." | Stated workload and cgroup limit | B2 (CPU and memory) under cgroup limits, including a stalled sink |
| Bounded disk | "Disk use stays within the configured budget plus one segment; any loss is counted." | `on_full` chosen explicitly | Disk-budget tests (PR7); demo scenario F7 |
| Exactly one file per WAL range | "Each WAL range lands in exactly one finalized file." | Cursor committed before ship eligibility | Crash-between-steps matrix: no duplicate keys, no missing ranges |
| Effectively-once ClickHouse | "Effectively-once insertion when the startup probe confirms deduplication settings." | Replicated table, or `non_replicated_deduplication_window` set; `async_insert_deduplicate` if async | ClickHouse dedup probe against tables with and without the window **[V]** (P1) |
| Effectively-once S3 | "Each object is effectively written once: deterministic keys and immutable files, with create-if-absent where the store supports it." | Probe result reported; overwrite fallback safe only for immutable files | Conditional-write probe on MinIO and a second store **[V]**; crash mid-multipart test |
| Deduplication | "Best-effort removal of redelivery duplicates; authoritative deduplication happens downstream." | Content-derived `event_id.key`; duplicates inside the window; warm start | Crash-redelivery test with and without warm start; dedup never drops events with differing keys |
| Deterministic replay | "Replay produces an equal row set for a fixed WAL range, configuration hash, and enrichment hash." | Not byte identity; file boundaries may differ | Row-set-equivalence tests (PR17); warm-then-process equals continuous run |
| Backpressure | "Effective for flow-controlled sources; QoS 0 and best-effort sources drop visibly." | `block` moves loss to the broker | Stall-for-3x-keepalive test; QoS 0 flood accounting (sent = received + dropped) |
| Ordered delivery | "WAL order equals receive order per connection." | No event-time order, no global order; reconnect redelivery can appear after a gap | Ack-ordering tests; ordering across reconnect |
| Throughput | "Measured msg/s for a stated broker configuration and workload." | `ack_mode: on_commit`: bounded by broker inflight window / commit latency | S2 ack-window curve; benchmark results with environment metadata on x86_64 and ARM64 |
| Exactly-once or zero-loss, universally | Not claimable. | product.md section 27 | None possible; do not publish |
| Device time never silently overwritten | "Source time is preserved; corrections set a quality flag." | ADR-0001 builder invariants | Invariant tests (PR3) |
| Storage endurance | "At 10 MB/s of payload, WAL writes are about 860 GB per day before Parquet writes." | Arithmetic, not a measurement; supported media are industrial SSD or eMMC that honors flush (owner decision B3) | Measured write amplification from S1 for a measured claim |
| Any durability or endurance claim on SD cards | Not claimable. "SD cards are not a supported target for durability claims." | Owner decision B3, 2026-10-07 | None possible; do not publish |
| Throughput ceiling is a documented limit | "With `ack_mode: on_commit`, one connection's throughput is bounded by roughly the broker inflight window divided by commit latency. Producer batching is not assumed." | Owner decision B3, 2026-10-07: the ceiling must appear in user documentation, not be hidden | S2 ack-window curve; documentation review |

### What the demo does NOT prove

The demo README (`examples/edge-capture`) must carry this list:

- **Power loss.** Containers cannot simulate it. Link the nightly LazyFS suite and the planned hardware power-cut rig, and state the exposure window of the configured fsync policy **[OPEN: wording derived from S1]**.
- **MQTT throughput under default broker limits.** The demo raises Mosquitto to `max_inflight_messages 100` and `max_queued_messages 200000` (defaults 20 and 1000 **[V]**). Scenario F7 shows the broker-side drop; the ack-window chart shows the ceiling.
- **Multi-node operation.** One node and one data directory.

### What this ADR supersedes

From ARCHITECTURE.md section 19: the Parquet commit point being the rename with the cursor recovered from footers, replaced by cursor-then-ship (D18). The architecture review named ADR-009 as D18's owner; that ownership is replaced, and D18 is recorded here and in ADR-0013. Also superseded: `on_full: block | drop_oldest` with `block` as the default (D4); the event ID choice with in-memory deduplication (D17; the review named ADR-019 as owner, replaced by this ADR for now); byte-identical Parquet on replay; and the 50,000 events per second hypothesis, withdrawn as a target (hypotheses are only things to falsify).

## Alternatives considered

- **Ship-then-commit (original).** Simple, but it caused the duplicate-object defect. Not chosen.
- **Persistent deduplication store.** Survives restarts without WAL reads but adds a durable store to fsync and recover. Not chosen: warm start from the WAL is deterministic and needs no new state.
- **In-memory-only deduplication (original).** Loses state at the moment of redelivery. Not chosen.
- **`block` as the default `on_full`.** Hides broker-side loss behind a safe-sounding name. Not chosen.
- **`drop_oldest` as the default.** Silently discards the oldest captured data, which contradicts the product thesis. Not chosen. A required explicit choice makes the trade-off visible.
- **A hard memory bound claim.** Thadam cannot account for dependencies and allocator fragmentation. Not chosen (D21).
- **A byte-identical replay claim.** False for the reasons in Context. Not chosen.
- **`ack_mode: on_receive` as default.** Lifts the ceiling but gives at-most-once across a crash. Opt-in only.
- **Treating deadband as deduplication.** Different operation with different semantics. Kept separate.
- **Universal exactly-once.** Against product.md section 27. Not claimed.

## Consequences

### Easier

- Publication discipline: one table decides what may be said, with its conditions.
- Recovery reasoning: with cursor-then-ship, each WAL range maps to one file and one object.
- Duplicate handling has a clear owner: Thadam flags best-effort, downstream is authoritative.

### Harder

- Every guarantee needs evidence before it ships, including a hardware rig for power loss.
- Operators must choose `on_full` and must configure broker limits. The ack-window ceiling constrains one-sample-per-message deployments.
- Users who need strict deduplication must run a downstream step (`ReplacingMergeTree`, `QUALIFY`) and must configure `event_id.key`.
- Memory sizing needs measured RSS per workload rather than a single promised figure.

### Fixed

- The register rule, the cursor-then-ship ordering, the explicit `on_full` choice, and the "best-effort here, authoritative downstream" deduplication split. Delivery wording in public text follows the register.

## Evidence and validation required

- [ ] **Crash-between-steps matrix** over finalize, cursor commit, eligible, upload, and delete (PR10, PR14, PR16 failpoints `segments::after_rename_before_catalog`, `segments::before_cursor_commit`, `ship::after_put_before_mark`, `ship::mid_multipart`): no duplicate keys and no missing ranges.
- [x] **Ack-window throughput curve (spike S2).** Report at `docs/design/spikes/S2-mqtt.md` (2026-10-07). Inflight windows 20, 100, and 1,000 against simulated commit latencies of 0, 10, 50, and 100 ms on Mosquitto 2.1.2 and EMQX 5.8.6; the model `window / (commit latency + RTT)` reproduces the measurements within 1 to 6%. Keepalive under stall passed (0 reconnects, 0 loss), and the broker defaults are confirmed (Mosquitto window 20 and queue 1000; EMQX window 32 and queue 1000). [ ] Repeat on target ARM hardware with real commit latencies and repeated runs for variance. The WAL group-commit default is now provisionally 10 ms (D3 was 50 ms), per S1 and ADR-0004.
- [x] **S1 fsync results.** Report at `docs/design/spikes/S1-wal.md` (2026-10-07): commit latency by fsync mode, fsync-error behavior (the custom log latches, SQLite does not), and process-level write amplification are reported, on macOS and a Linux VM. [ ] Re-run on NVMe and eMMC (SD only as an informational data point, since SD is not a supported target) with device-level write amplification, before the S1-derived rows in the register are quoted as measured claims.
- [ ] **Hardware power-cut rig**, 500 cycles, before any power-loss claim.
- [ ] **ClickHouse deduplication probe** against tables with and without the window **[V]** (P1).
- [ ] **S3 conditional-write probe** against MinIO and a second store **[V]**; clock-skew test (plus or minus 20 minutes).
- [ ] **RSS under cgroup limits** (benchmark B2), including an out-of-memory kill and recovery.
- [ ] **Deduplication crash-redelivery test**: duplicates dropped with warm start, counted without it; a property test that deduplication never drops events with differing keys.
- [x] **Owner decision B3** on `on_full`, producer batching, and storage media: recorded 2026-10-07 in `docs/design/owner-decisions.md`.
- [ ] Demo acceptance F1 to F8 pass the verifier (zero missing acknowledged samples, zero duplicate rows, zero duplicate or partial objects); every loss in F7 is attributed to broker drop or Thadam eviction.

## Revisit when

- A new sink type with a different guarantee class lands.
- A claim's evidence changes, or a row in the register is falsified.
- The owner changes the `on_full` policy.
- Target-producer data or an ARM re-run of the S2 curve shows the ack-window ceiling makes `on_commit` unworkable for target producers (the laptop run showed the ceiling is predictable: about 1.7 thousand messages per second per connection at a 10 ms commit with Mosquitto's default window of 20).
- A persistent dedup store becomes necessary because windows must exceed WAL retention.

## References

- ARCHITECTURE.md section 3 (D4, D7, D17, D18, D21), section 7 (determinism, dedup), section 8 (finalize sequence, shipper), section 10 (backpressure), section 11 (recovery), section 12 (delivery guarantees), section 19, section 22.
- ADR-0001 (event ID rules), ADR-0002 (acknowledgement and delivery classes), ADR-0003 (chains, replay), ADR-0004 (durable buffer, fsync, `on_full`), ADR-0006 (MQTT client), ADR-0013 (Parquet layout and keys), reserved ADR-0009 and ADR-0019.
- product.md sections 5 (UC-05), 11, 26, 27.
- Plans: `thadam_architecture_review` sections 4 to 7 and 11 and decision matrix D4, D7, D17, D18, D21; `thadam_edge-capture_demo` sections 2, 8, 9, 12, 13; `thadam_rust_implementation_plan` sections 4.5 to 4.7; `thadam_ecosystem_maturity_review` section 10.
- Spike reports: `docs/design/spikes/S1-wal.md`, `docs/design/spikes/S2-mqtt.md` (both 2026-10-07).
