# Per-sink chains

Every sink gets its own **chain**: an independent WAL reader with its own cursor, its own decode and transform pipeline, and its own sink driver. Chains are the isolation unit of Thadam — a slow or failing chain cannot block capture or any other chain, because capture ([the WAL](wal-and-replay.md)) commits before any chain runs.

## What a chain is

A chain is synchronous, pure, and owned by one task. It:

1. reads committed frames from its WAL cursor,
2. routes records to datasets by `match` (source id and topic filter),
3. decodes payloads (JSON today) into the declared shapes,
4. runs the dataset pipeline and the `transforms:` list, and
5. hands one `EventBatch` per dataset to its sink driver — the segment driver (Parquet) or the batch driver (ClickHouse, planned).

Records no dataset matched are counted (`unmatched`) but are not rejects. Per-event errors become rejects with the raw bytes retained.

## Processing order within a dataset

The fixed order (ADR-0003; `thadam-transform` crate docs):

1. Decode at message level.
2. Event ID derivation and **message-level deduplication** — before `explode`, so dedup memory stays small even when one message becomes many events.
3. `explode`, when configured.
4. Field extraction and unit conversion (`from:`, `convert_to:`).
5. Time extraction, domain handling, and skew flagging (`time:` with `max_skew`).
6. The `transforms:` list, in listed order, filtered by each step's `dataset:` selector (and `sinks:` list when a step is restricted to specific sinks).

## Determinism

Chains never read the wall clock or a random source: dedup remembers by ingest time and arrival order, sampling counts events or ingest-time intervals, enrichment files are hashed into lineage. Two runs over the same WAL range with the same config hash and enrichment hash produce the same row set — the property [replay](wal-and-replay.md#replay) verifies.

## Current configuration, always

A chain always runs with the configuration from its last load. Restart Thadam with a corrected config and any unprocessed WAL data — everything after each sink's cursor — is processed with the *new* configuration. Reprocessing older data is replaying, which resets the cursor into the retained window. This rule is stated explicitly because it surprises: chains do not pick up "the config that was current when the data was captured".

## Failure handling

- **Transient errors** (broker hiccup, sink timeout) retry with capped, jittered backoff and mark the component `Degraded`; the chain keeps its cursor.
- **Poison records**: a frame whose processing panics is retried (`poison_retries`, default 2), then its records are quarantined; the cursor advances. Multi-frame batches that keep failing are bisected (`max_attempts_before_bisect`, default 5) to isolate the poison event, and the bisection is recorded in lineage.
- **Panic isolation**: a chain panic is caught at the task boundary, counted (`thadam_chain_panic_total`), and the chain is rebuilt; the process keeps running. Open [`window`](../configuration/transforms.md#window) state is rebuilt by re-reading from the oldest frame an open window needs.
- **Open windows hold the cursor back** (experimental `window` step): the sink cursor stops short of the first frame of any window whose aggregate the sink does not hold, so a restart re-reads those frames. See [`kind: window`](../configuration/transforms.md#window).
- **Fatal errors** (authentication, configuration) mark the component `Failed` and flip readiness to false; capture continues within budget and policy.

One chain is designated **primary** for reject and quarantine metrics, so several chains seeing the same raw message do not double-count rejects.

## Quarantine

Rejects go to a quarantine segment sink with a fixed schema: `_wal_epoch`, `_wal_pos`, `_ingest_time`, `source_id`, `topic`, `error_domain`, `error_code`, `reason`, and the raw bytes (`raw`, truncated at 64 KiB with a `raw_truncated` flag), under the dataset id `_quarantine`. Quarantine has its own disk budget ([`quarantine`](../configuration/wal.md#quarantine)) so a schema change or a poison flood cannot starve normal data. Format is `parquet`; `ndjson` is reserved and rejected by `thadam validate`.

Quarantine never holds back the sink it belongs to. A reject is made durable in a small per-quarantine **staging file** (append-only, length-prefixed, CRC32C-protected, fsynced per batch) before the sink's WAL cursor may pass it; the quarantine Parquet segment is then built from staging and rotates on its own schedule (`quarantine.rotation`). So a single reject does not pause the sink's cursor, its uploads, or shipping while the quarantine segment is still open. After a crash, rejects that were staged but not yet in a durable quarantine segment are recovered from staging into the next quarantine segment, exactly once: rows are matched against the quarantine's durable WAL position, so a restart, a rewind, or a re-read of the same WAL frames never duplicates a reject. Staging bytes count toward the quarantine disk budget and are released when the covering segment is durable. Rejects the sink itself returns (a value its columns cannot hold) are staged before the sink makes the rows of that batch durable, so a crash in between cannot lose them.

## Limits

Chains enforce `ChainLimits` — shape registry size (`max_shapes`), dedup key count (`max_dedup_keys`), JSON depth, predicate cost — because payload-derived keys are untrusted input. Overflow becomes a *counted reject*, never unbounded memory growth (metric: `thadam_rejects_total{domain="schema"}` for the shape registry).
