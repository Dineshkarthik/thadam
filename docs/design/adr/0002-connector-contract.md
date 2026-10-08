# ADR-0002: Connector contract

- **Status:** Accepted
- **Date:** 2026-10-07
- **Updated:** 2026-10-07 (owner decisions B6 recorded; spike S2 evidence folded in; accepted); 2026-10-08 ("Implementation notes" added for the grouped segment-sink append of the PR10 follow-up `f2c2282`; status unchanged)
- **Deciders:** Project owner

**Acceptance.** Accepted by project owner on 2026-10-07 (`docs/design/owner-decisions.md`, section "ADR acceptances"). The owner signed D5 and D6 on 2026-10-07 (B6), and spike S2 has confirmed the mechanics the contract depends on (liveness by withholding acknowledgements, ordered acknowledgement, payload-stripped handles) and settled the cancel-safety question. The unticked items in "Evidence and validation required" are implementation-time verification, not blockers: they happen in PR3, PR4, PR10, and PR12 and in later milestones. Three of them could in principle reopen a clause, and each has a named revisit trigger: the Zenoh pull-contract prototype (M2), the Parquet and ClickHouse prototypes against both sink shapes (PR14 and M3), and the contract-overhead benchmarks (PR4). Open item O8 (the `SinkBatch` type) is an interface detail that PR3 and PR4 define and does not change a decision here.

## Context

Connectors are the boundary between Thadam's runtime and the outside world: sources bring messages in, sinks send data out. The contract defines who owns batching, retries, cancellation, acknowledgement, and protocol liveness. It lives in `thadam-core`, and every connector crate (`thadam-mqtt`, `thadam-segments`, `thadam-object-store`, later `thadam-clickhouse` and the ROS 2 crates) implements it. A wrong shape here is expensive, but the contract is a pre-1.0 internal API (medium reversibility), so it is documented as 0.x and unstable until after the first release.

The original MVP plan chose a push-style `Source::run(ctx)` with an `Emitter` whose `emit().await` was the backpressure point, one `Sink` trait (`open`, `write`, `flush`, `close`) with watermark acknowledgements, broad capability descriptors with a `contract_version`, and an untyped `ConfigValue`. The architecture review found these problems:

- **Backpressure breaks MQTT keepalive.** If the runtime applies backpressure by blocking `emit().await`, the MQTT client stops polling, stops sending PINGREQ (the keepalive ping), and the broker drops the connection after about 1.5 times the keepalive interval. The result is reconnect churn and redelivery. Correct MQTT backpressure keeps polling and withholds PUBACKs. This applies to any protocol with heartbeats, so it must be a contract clause, not an MQTT detail.
- **Acknowledgement ordering was under-specified.** MQTT requires PUBACKs in receive order. A lost acknowledgement token stalls the session window forever.
- **One sink trait hides two different sink kinds.** Batch-transactional sinks (ClickHouse, a single object PUT) want the runtime to form deterministic batches and retry the same batch. Accumulating segment sinks (Parquet, NDJSON) own open state and report a watermark. In one trait, "retry the same batch ID" is meaningless for Parquet and watermarks are unnecessary for ClickHouse.
- **Over-design.** Capability negotiation, `contract_version`, `accepts`, `ordering`, and `requires_schema` have no consumer before a plugin mechanism exists.
- **Type leakage.** A `ConfigValue` tied to a YAML crate leaks a deprecated or fragmented YAML ecosystem (`serde_yaml` is deprecated) into every connector.
- **Cancellation safety.** Dropping a future mid-protocol-operation (for example in `tokio::select!`) can corrupt client state. Spike S2 settled the question for `rumqttc`: `EventLoop::poll` is not cancel-safe. On a healthy socket, 1.2 to 1.3 million randomly dropped polls per broker lost nothing, but when the socket write side was backed up a dropped poll silently lost acknowledgements (with poll-once-then-drop, 300 `PubAck` events were emitted and none reached the wire; with a 50 ms timeout, 9 to 12 of 300 were lost), with no error and a healthy-looking connection. The same hazard exists in other client libraries that await on the write path.

Under decision D1 (ADR-0003) every raw message is committed to the WAL before filtering, which simplifies acknowledgement: sources are acknowledged after WAL commit regardless of what chains later do with the data.

## Decision

All signatures below are sketches. The owner signed the decisions on 2026-10-07 (B6, D-B6-1 and D-B6-2; D8 to D10 are in ADR-0001), so the shapes of the contract are settled. The exact Rust signatures are still defined in PR3 and PR4.

### Source: pull contract (D5)

Decided by the owner on 2026-10-07: pull-based sources with a cancel-safe `next_batch` and the liveness clause.

```rust
#[async_trait] pub trait Source: Send + 'static {
    async fn open(&mut self, ctx: &SourceContext) -> Result<(), ThadamError>;
    async fn next_batch(&mut self, limits: BatchLimits) -> Result<Option<SourceBatch>, ThadamError>; // MUST be cancel-safe
    async fn close(&mut self) -> Result<(), ThadamError>;
}
pub struct SourceBatch { pub batch: RawBatch, pub ack: AckHandle }
```

The runtime's `SourceDriver` calls `next_batch(BatchLimits { max_messages, max_bytes, linger })`. The connector drains whatever is queued, waiting at most `linger` (default 5 ms), so batches grow under load and stay small under light traffic. Connectors are free to run an internal task. Each source also implements a factory (`SourceFactory`, with a sibling `BatchSinkFactory`, `SegmentSinkFactory`, and `ServiceFactory`) exposing `kind`, a JSON Schema, `validate`, and `build`.

Every source implementation must satisfy these clauses, each tested by the conformance suite in `thadam-testkit`:

- **Liveness.** The connector keeps protocol liveness (MQTT PINGREQ, DDS heartbeats, session maintenance) regardless of how slowly the runtime pulls. The suite stalls the consumer for three times the keepalive interval and requires that the connection stays up and no QoS 1 message is lost.
- **Cancel-safety.** Dropping the `next_batch` future never loses a message; messages stay in the connector's queue. The suite drops the future at random points. All other cancellation is cooperative through a `Shutdown` token. The runtime never drops any other connector future mid-operation, and uses `select!` only on cancel-safe operations.
- **Ordered acknowledgement.** The runtime completes `AckHandle`s strictly in receive order after the corresponding WAL commit. The `SourceDriver` keeps a bounded FIFO of (commit ticket, ack handle) pairs (`max_uncommitted_batches`, default 64). An empty batch is still acknowledged, and only after earlier batches commit. Under D1, filtering happens after acknowledgement, so this case only arises for batches reduced before submission **[A]**; the clause stays because it costs nothing.
- **The protocol task never awaits the runtime.** The connector's protocol task (for MQTT, the task that owns the client event loop) may only wait on the protocol library itself. It never awaits a bounded queue that the runtime drains, never takes a lock the runtime holds, and is never raced against a timer or token inside the library call. Spike S2 showed that blocking it (a bounded queue smaller than the broker window) caused a broker disconnect and then a deadlock. Memory is bounded by byte accounting and by the protocol's own flow control, and a guard that disconnects (rather than blocks) covers a runaway. Shutdown is a token check between library calls.
- **Epoch-scoped acknowledgement handles.** An `AckHandle` is valid only for the connection or session in which its message arrived. Completing a handle from an older epoch is a no-op that is counted, never forwarded to the protocol. Spike S2 (E1) showed that acknowledging with a pre-session-loss handle acknowledges the wrong, new message, which is silent loss under at-least-once. After a session loss the connector also restores its subscriptions itself.
- **Handles hold no payload.** An `AckHandle` carries only the protocol's acknowledgement identity (for MQTT, packet identifier and QoS). Spike S2 confirmed that acknowledging with a payload-stripped publish works for QoS 1 and 2: 1000 unacknowledged 64 KiB messages cost 1.9 MiB of resident memory stripped against 79.7 MiB retained.
- **Lost-token detection.** The metric `thadam_ack_oldest_age_seconds` and a watchdog expose handles that were never completed.
- **Panic isolation.** A connector panic is caught at the task boundary and marks the component `Failed`. This requires `panic = "unwind"` in release builds.
- **Ack-mode capability.** `AckMode` is `None`, `OnCommit`, or `OnReceive`. It is the only source capability in the MVP. The MQTT default is decided in ADR-0006 and ADR-0011.

### Sinks: two shapes (D6)

Decided by the owner on 2026-10-07: sinks are split into `BatchSink` and `SegmentSink`, and the shipper is a service, not a sink.

```rust
#[async_trait] pub trait BatchSink: Send + 'static {
    async fn open(&mut self, ctx: &SinkContext) -> Result<SinkCapabilities, ThadamError>; // probes; reports effective delivery class
    async fn write(&mut self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError>;    // same BatchId on every retry
    async fn close(&mut self) -> Result<(), ThadamError>;
}
#[async_trait] pub trait SegmentSink: Send + 'static {
    async fn recover(&mut self, ctx: &SinkContext) -> Result<Option<WalPos>, ThadamError>;
    async fn append(&mut self, batch: EventBatch) -> Result<Vec<Reject>, ThadamError>;  // takes ownership; may buffer
    async fn poll_commit(&mut self) -> Result<Option<WalPos>, ThadamError>;             // finalize due files; new watermark
    async fn finalize_all(&mut self) -> Result<Option<WalPos>, ThadamError>;
    fn cursor_committed(&mut self, pos: WalPos);                                       // enables ship eligibility
}
```

- **`BatchSink`** (ClickHouse, a single object PUT): the runtime's `BatchDriver` forms batches on WAL frame boundaries, identified by a deterministic `BatchId` (epoch, first frame, last frame), persists the in-flight range in the cursor before sending, retries the same `BatchId`, and advances the cursor on success. The only sink capability is the delivery class, `AtLeastOnce` or `IdempotentBatch`, which `open` reports from probing the target rather than assuming.
- **`SegmentSink`** (Parquet, NDJSON for debugging): the sink owns open state and reports a committed watermark. The runtime advances the cursor only to a committed watermark, at frame granularity, then calls `cursor_committed`, which lets the sink make finalized files eligible for shipping (ADR-0011, D18).
- **The shipper is a service, not a sink.** It consumes finalized segments through the segment catalog, not the WAL (ARCHITECTURE.md section 8).
- **Not yet specified [A]:** the `SinkBatch` type (`BatchId` plus an `EventBatch`) is defined in PR3 and PR4 (open item O8).

### Errors and configuration

- Every error is a `ThadamError { domain, code, message, retry, severity, context }` with retry class `Transient`, `Permanent`, or `Fatal`. Transient errors retry with the same `BatchId` and capped, jittered backoff. Permanent per-event errors become rejects routed to quarantine. Fatal errors mark the component `Failed`. A dropped HTTP request has an unknown outcome and is retried with the same batch ID.
- Factories receive configuration as `serde_json::Value` (D22), with typed structs inside each connector. No YAML types cross the boundary. The choice of YAML crate is ADR-0012.
- There is no `contract_version` and no broad capability negotiation. The plugin question is deferred to reserved ADR-0017.

### Conformance suites in `thadam-testkit`

Source suite: liveness, cancel-safety (random drops), ack ordering under randomized commit completion, lost-token watchdog, cancellation during connect, receive, and ack, and panic isolation. `BatchSink` suite: a repeated batch ID is deduplicated or tolerated, and error classification is checked against recorded server responses. `SegmentSink` suite: a monotonic watermark and recovery after a crash at every failpoint. Memory connectors (`MemorySource`, `MemoryBatchSink`, `MemorySegmentSink`) must pass their own suites first.

### What this ADR supersedes

From ARCHITECTURE.md section 19: the push-style `Source::run(ctx)` with an `Emitter`; the single `Sink` trait with `open/write/flush/close`; capability negotiation, `contract_version`, and `SourceFactory::descriptor` with `accepts`, `ordering`, and `requires_schema`; and `ConfigValue` tied to a YAML crate. product.md section 19 sketched push-style traits and says not to lock the design before benchmarks, so the contract is accepted but stays 0.x and unstable until after the first release.

## Implementation notes (PR10 and `f2c2282`, 2026-10-08)

The trait sketches in the Decision section are what the owner signed on 2026-10-07; the first runtime implementation (PR10, `e2dd744`, and its follow-up `f2c2282`) fixed the real signatures. One change relative to the sketch is worth recording here:

- **`SegmentSink::append` receives events grouped by dataset (`f2c2282`).** The sketch above passed an `EventBatch`, one dataset's events. As built, `append` takes a `SegmentBatch` (`thadam-core`): the segment driver passes each chain read as a single `append` covering one WAL range, with the events per dataset collected in `datasets: Vec<DatasetEvents>` in declaration order (empty datasets included). Sink rejects index the events in the flattened order (datasets in declaration order, WAL order within each), the same order `SegmentBatch::events` iterates, and the runtime never calls `poll_commit`, `finalize_all`, or `recover` in the middle of an `append`; successive batches have strictly increasing, non-overlapping origins, so a sink may derive its watermark from appended origins. Rows routed to quarantine are appended under `QUARANTINE_DATASET` (`"_quarantine"`) so the sink keeps them separate. `BatchSink` is unchanged: `write` still takes a `SinkBatch` with no dataset id, so the batch driver concatenates the datasets' events; grouping it the same way is open work that `thadam-clickhouse` (one table per dataset) needs before PR20.

This changes no decision of this ADR (the two sink shapes, ordered acknowledgement, and the cursor rules are untouched); the contract is 0.x and unstable, so the signature change needs no deprecation period.

## Alternatives considered

- **Push emitter (`Source::run` with `emit().await`).** Suits callback libraries, but awaiting the emitter encourages blocking the protocol loop, and the runtime loses control over batching, fairness across sources, metrics, and cancellation. `rumqttc`, `r2r`, and Zenoh all expose pull or stream APIs. Not chosen. Pull costs slightly more connector boilerplate.
- **One `Sink` trait with `flush` and watermark acknowledgements.** Fewer traits, but it hides two different sink kinds and makes both retry semantics and watermarks partly meaningless. Not chosen. Two traits mean more API surface, each far less ambiguous.
- **Broad capability negotiation with `contract_version`.** Not chosen: no consumer exists before plugins, and every extra capability is a test obligation. Revisit with ADR-0017.
- **`ConfigValue` tied to a YAML crate, or an erased deserializer.** The first leaks the YAML ecosystem; the second adds machinery for no gain. Not chosen in favor of `serde_json::Value` (YAML is converted to a JSON value before the boundary).
- **Cancelling by dropping futures.** Simple, but dropping a future mid-protocol operation can corrupt client state. Spike S2 confirmed this for `rumqttc`: `EventLoop::poll` is not cancel-safe on its write path, and a dropped `poll` while the socket was backed up lost 9 to 12 of 300 acknowledgements under a 50 ms timeout and all 300 under poll-once-then-drop, with no error and a healthy-looking connection. Not chosen. Cancellation is cooperative through a token, with the single exception of `next_batch`, which is made cancel-safe by contract.
- **Making the shipper a third sink kind.** It consumes finalized segments and the catalog, not events or the WAL. Treating it as a sink would blur the cursor and eligibility rules. Not chosen.
- **Acknowledging as soon as a message is received.** Removes the throughput ceiling but gives at-most-once across a crash. Kept only as the opt-in `AckMode::OnReceive` (ADR-0011).

## Consequences

### Easier

- The runtime owns batching, retries, cursor commits, fairness, and metrics, so connectors implement protocol I/O only.
- A liveness failure under backpressure is caught by a test instead of discovered in production.
- Each sink kind has unambiguous retry and commit semantics. A source that is pull-based composes with a runtime-owned ordered ack FIFO.

### Harder

- Each connector with a protocol loop needs an internal liveness task and a bounded queue, plus a cancel-safe `next_batch`. Every connector must pass the conformance suites.
- Ack handles must be stripped of payload memory so acknowledgements never pin payloads (confirmed in S2), and must carry an epoch so stale handles are dropped.
- The protocol task must never await the runtime, so connectors bound memory through byte accounting and protocol flow control rather than a blocking bounded queue. This is more careful design than the original sketch assumed.
- Two sink traits and their drivers (`SegmentDriver`, `BatchDriver`) are more surface to maintain than one.

### Fixed

- Pull sources, the split sink traits, the liveness and cancel-safety clauses, and ordered acknowledgement are the shape every connector is written against from PR4 on. Unstable 0.x status applies until after the first release.

## Evidence and validation required

- [x] Owner sign-off on decisions D5 and D6 (owner decision B6), signed 2026-10-07, before PR3 and PR4. PR10 additionally requires D5 and D6 signed, which is now satisfied.
- [x] Prototype ergonomics of the MQTT source (spike S2) against the pull contract. S2 ran the shape the contract implies (a protocol task that owns the client and feeds a queue, plus an acknowledgement task completing in receive order) against Mosquitto 2.1.2 and EMQX 5.8.6 and found it workable, with the corrections folded into the clauses above. It was a harness, not an implementation of the `Source` trait, which is PR12.
- [ ] Prototype ergonomics of a Zenoh subscriber against the pull contract (M2; not required for M1).
- [x] Liveness, evidence from S2: with acknowledgements withheld for 3 times the keepalive (15 s at a 5 s keepalive) while the protocol task kept polling, there were 0 reconnects, 3 PINGREQ and 3 PINGRESP, the broker stopped at exactly its inflight limit, and everything was delivered after release with 0 loss and 0 duplicates, on both brokers. [ ] The conformance-suite version of this test remains to be written in PR4 and run against the real connector in PR12.
- [x] Ack ordering, evidence from S2: in-order issue gave in-order PUBACKs on the wire (0 descents in 3000, both brokers); shuffled acknowledgements went out shuffled and both brokers tolerated it, so ordering is entirely the application's job. [ ] Ack ordering under randomized commit completion in the conformance suite (PR4).
- [x] Cancel-safety of `next_batch`, by construction: `next_batch` only receives from the connector's internal queue (S2 confirms the protocol task must be a separate task). [ ] Random drops of `next_batch`, and cancellation during connect, receive, and ack, in the conformance suite (PR4).
- [ ] Parquet and ClickHouse prototypes run against both sink shapes.
- [ ] Benchmark per-batch overhead of `async_trait` boxing and ack bookkeeping.
- [ ] Benchmark ack-tracker throughput at 100,000 acks per second.
- [ ] Lost-token watchdog test and panic-isolation test.
- [ ] Define `SinkBatch` (open item O8) in PR3 and PR4, and record how `BatchId` is formed and persisted.
- [x] Spike S2 refuted `EventLoop::poll` cancel-safety on the write path (see Alternatives). The liveness task is interrupted only by a token check between polls, and a test or lint in PR12 fails on any `select!`, `timeout`, or `now_or_never` around `poll`.
- [ ] A conformance test for the epoch rule: the S2 E1 scenario (stale acknowledgements after session loss) must lose 0 messages through the connector (PR4 suite, PR12 connector).

## Revisit when

- A plugin mechanism (reserved ADR-0017) needs a versioned contract.
- A third sink kind appears.
- Prototypes show the pull contract is awkward for a callback-style library.
- Benchmarks show contract overhead is material at target rates.

## References

- ARCHITECTURE.md section 3 (D5, D6, D22), section 5 (connector contracts), section 10 (cancellation contract), section 13 (error model), section 19, section 22 (O8).
- ADR-0001 (canonical event model), ADR-0003 (execution and process model, drivers), ADR-0004 (durable buffer, commit tickets), ADR-0006 (MQTT client), ADR-0011 (acknowledgement modes and delivery classes), ADR-0012 (config format), reserved ADR-0017 (plugin mechanism).
- product.md section 19 (connector contract) and section 28 (error handling).
- Plans: `thadam_architecture_review` section 2 and decision matrix D5, D6, D22; `thadam_rust_implementation_plan` sections 4.4 and 5.1, 5.5; `thadam_mvp_architecture_plan` section D.
- Spike S2 report: `docs/design/spikes/S2-mqtt.md` (sections 5.2 to 5.4, 5.7, and 8.2).
- Owner decisions: `docs/design/owner-decisions.md`, D-B6-1 (D5) and D-B6-2 (D6).
