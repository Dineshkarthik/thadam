# Delivery guarantees

Thadam's reliability claims are governed by [ADR-0011](../design/index.md), *delivery guarantees and claims register*. The rule: **no README, benchmark post, release note, or demo text may state a guarantee that is not in the claims register, and each row may be published only after its evidence is attached**. This page follows the register exactly; where evidence is pending, it says so.

## Guarantees per hop

| Hop | Guarantee | Conditions |
|---|---|---|
| MQTT → WAL | At-least-once, in receive order | QoS 1 or 2 (QoS 2 experimental), persistent session, stable `client_id`, manual ack after WAL commit. QoS 0 is at-most-once. `ack_mode: on_receive` is at-most-once across a Thadam crash. Messages the broker accepts while Thadam is down survive only within the broker's queue limits and session expiry. |
| WAL durability | Acknowledged data survives a process crash | `fsync` other than `never`. Kill-9 loop evidence exists from spike S1. Power-loss survival additionally requires flush-honoring industrial SSD or eMMC on Linux and is **not claimed yet**: the hardware power-cut rig (500 cycles) that ADR-0011 requires has not run. `fsync: never` survives a process crash only. |
| Ordering | WAL order equals receive order per connection | No event-time order, no global order. After a reconnect, MQTT redelivery replays older unacknowledged messages after a gap. |
| WAL → Parquet | No loss within the disk budget; each WAL range lands in exactly one finalized file | The commit-then-ship rule (D18): the cursor is committed durably *before* a file becomes upload-eligible, which closed a duplicate-object defect in the original design. |
| Parquet → object storage | Each object is effectively written once | Deterministic keys, immutable files, create-if-absent where the store supports it (probed at startup, currently MinIO; fallback overwrite is safe only because files are immutable). *(In progress)* |
| WAL → ClickHouse (P1) | At-least-once; effectively-once only with specific table settings | A replicated table or `non_replicated_deduplication_window`, plus `async_insert_deduplicate` for async inserts. Thadam probes at startup and reports the effective delivery class. *(Planned)* |
| Replay | Row-set equivalence | Same WAL range, config hash, and enrichment hash — not byte identity. |
| Deduplication | Best-effort removal of redelivery duplicates | Requires content-derived `event_id.key` and a warm start; window-bounded. Authoritative deduplication happens downstream. |
| Disk full | Loss is explicit and counted | Under both `on_full` policies. `block` moves loss to the broker, where it can be invisible; `drop_oldest` makes it visible in Thadam. |

## Throughput ceiling {#throughput-ceiling}

With `ack_mode: on_commit` — the default, because it is the only at-least-once mode — one connection's throughput is bounded by roughly

```text
max messages/second ≈ broker inflight window / (commit latency + RTT)
```

Spike S2 validated that model within 1–6% against Mosquitto 2.1.2 and EMQX 5.8.6 ([S2 report](../design/index.md)). With defaults (Mosquitto window 20, EMQX 32) and a 100 ms commit that is about 200–320 msg/s per connection; at the provisional 10 ms group commit, Mosquitto's default window yields about 1,700 msg/s. Producer-side batching is **not** assumed in these numbers. Mitigations an operator may choose: several samples per message, raised broker limits ([required broker settings](../sources/mqtt.md#required-broker-settings)), MQTT 5 Receive Maximum (can only lower the window), or `ack_mode: on_receive` (at-most-once across a crash).

## Memory and storage discipline

- **Memory (D21).** Thadam-owned buffers are bounded by configuration — the [memory estimate](../getting-started/validating-configs.md#the-memory-estimate) in `thadam validate`. Total RSS (including `rumqttc`, `hyper`, zstd, and allocator fragmentation) is measured per workload under cgroup limits and published with benchmarks, not promised as one number.
- **Flash endurance.** At 10 MB/s of payload (50 sensors × 1,000 Hz × 200 B), WAL writes are about 860 GB per day before framing overhead, filesystem write amplification, and the Parquet writes that follow. The supported durability baseline is industrial SSD or eMMC that honors flush; **SD cards are not a supported target** and no durability or endurance claim may be published for them (owner decision, 2026-10-07).

## What Thadam does not claim

- Universal exactly-once or zero-loss — these depend on protocol, source, storage, network, and sink behavior ([product.md](https://github.com/Dineshkarthik/thadam/blob/main/product.md) section 27).
- Deterministic end-to-end latency.
- Byte-identical replay.
- Any durability or endurance claim on SD cards, or on storage that does not honor flush.
- Bounded total RSS as a single figure (see above).

The binding wording for each claim lives in the claims-register table in [ADR-0011](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0011-delivery-guarantees-and-claims-register.md); when this page and the ADR disagree, the ADR wins.
