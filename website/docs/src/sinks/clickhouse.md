# ClickHouse sink

**Status: Available.** The `clickhouse` kind is a batch sink that inserts one dataset's events into an **existing** ClickHouse table. It is the first analytical-database sink — for low-latency queries over live telemetry, dashboards, and time-window aggregations — and deliberately *not* a prerequisite for using Thadam or the project's defining storage path: it has its own cursor and its own chain, so a ClickHouse outage never blocks Parquet or S3 ([ADR-0008](https://github.com/Dineshkarthik/thadam/blob/main/docs/design/adr/0008-clickhouse-sink.md), still Draft in the design record).

The binary includes it by default (the `clickhouse` Cargo feature of `thadam`; build with `--no-default-features` to leave the client out).

## How it works

1. The runtime forms deterministic batches on WAL frame boundaries and hands each one to the sink with a stable batch id, persisting the in-flight range before it sends. After a crash the same range is rebuilt as the same batch with the same id.
2. `open` probes the server and the table, then reports the effective [delivery class](../concepts/delivery-guarantees.md):
   - `SELECT version()` (logged),
   - `DESCRIBE TABLE`, validated against the dataset schema (below),
   - the table engine and the deduplication windows, from `system.tables` and `system.merge_tree_settings`.
3. `write` encodes the batch with `thadam-arrow`, turns the Arrow batch into one ClickHouse **Native** block, and inserts it with the official `clickhouse` crate's `insert_native()`. When the probe found deduplication support the insert carries `insert_deduplication_token`.
4. The sink never runs `CREATE TABLE` or `ALTER TABLE`. Creating the table and keeping it in step with the dataset is your job; a mismatch fails at `open`, not per event.

### Insert path

The Native block path is the only vendor-maintained way to insert **dynamically typed** columns: the crate's `Client::insert::<T>()` needs compile-time row types, and Thadam schemas are declared in YAML. `clickhouse-ext-arrow` 0.1.0 would reuse Arrow batches directly but pins `arrow ^58` while this workspace is on `arrow 60` (Arrow majors make `RecordBatch` incompatible), and raw `FORMAT ArrowStream` over HTTP would duplicate the vendor's transport without a measured win (ADR-0008's parity rule prefers the vendor crate until the HTTP-vs-native client benchmark says otherwise). The sink builds each block with the crate's public `BlockBuilder` and `Encode` trait, picking the wire type from the *table's* own column type, so the server's implicit conversion is the identity except for `LowCardinality` (exact), `DateTime64(0)` into `DateTime`, and `String` into `FixedString(n)`; the last two are range- and length-checked in the sink (below).

Known cost: the vendor's `InsertNative` hard-codes block compression off in 0.15.2 (a `FIXME` in the crate), so **`compression: lz4` currently compresses probe responses only**; insert bodies travel uncompressed. The key is accepted so configurations keep working when the crate adds it.

## Configuration

```yaml
sinks:
  - id: ch-imu
    kind: clickhouse
    config:
      url: https://clickhouse.internal:8443   # the HTTP(S) interface
      database: telemetry                      # default: default
      table: imu                               # must exist; one table per sink
      user: thadam                              # default: default
      password: "${file:/run/secrets/clickhouse_password}"
      compression: lz4                         # lz4 (default) | none; see "Insert path"
      request_timeout: 30s                     # probes and each insert (default 30s, max 1h)
      dedup: auto                              # auto (default) | token | off
      # columns:                               # optional explicit mapping, dataset -> table;
      #   temp: temperature                    # when present it must list every declared field
      #   accel: accel
      datasets: [imu]                          # exactly one dataset per sink entry
```

- `password` is a plain string **already resolved** by `${env:}`/`${file:}` interpolation ([secrets](../configuration/index.md#secrets)); the sink wraps it in a redacting type and never logs it. It warns at `open` when a password travels over plain `http://`.
- `url` accepts `https://` (rustls with the `ring` provider only, ADR-0020) and `http://` (local setups). HTTPS trusts the operating system's root certificates, so a private CA works once it is installed in the OS trust store (the host needs a CA package such as `ca-certificates`; with an empty store the sink fails to build with a configuration error). Client certificates cannot be configured in this release.
- `dedup`: `auto` sends the token when the probe finds deduplication support and reports what it found; `token` insists on it and refuses to start when the table cannot honor it; `off` never sends a token and always reports `at_least_once`.
- A `kind: clickhouse` sink consumes exactly one dataset; add one sink entry per dataset. The restriction lifts when the batch contract groups events per dataset (ADR-0002 open work).

## The table

Create the table yourself. A minimal one for a dataset with an `imu`-style record, deduplicating on a plain `MergeTree`:

```sql
CREATE TABLE telemetry.imu
(
    _event_id        UUID,
    _ingest_time     DateTime64(6, 'UTC'),
    _event_time      Nullable(DateTime64(6, 'UTC')),
    _quality_status  LowCardinality(String),
    _wal_pos         UInt64,
    robot            LowCardinality(Nullable(String)),
    accel            Array(Float32),
    temp             Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (_ingest_time, _wal_pos)
SETTINGS non_replicated_deduplication_window = 1000;   -- 0 (the default) means no dedup
```

### Column projection

- **Without `columns`:** every declared dataset field must exist in the table under its own name. Envelope columns (`_event_id`, `_ingest_time`, `_source_time`, `_event_time`, `_time_domain`, `_quality_status`, `_quality_flags`, `_quality_reason`, `_clock_synced`, `_wal_epoch`, `_wal_pos`, `sample_idx`) and identity-level columns (`robot`, ...) are inserted **only where the table has a column of that name**.
- **With `columns`:** the mapping is the complete projection. Every declared dataset field must appear in it; envelope and level columns are inserted only when mapped.
- Table columns the dataset does not feed (for example `inserted_at DateTime DEFAULT now()`) are left to their server-side defaults. `MATERIALIZED` and `ALIAS` columns cannot be targets.

`open` reports **every** problem it finds in one `Config` error (permanent, so the pipeline fails fast with the list), including a missing table or database and the server's own explanation.

### Names are quoted, dots included

Every identifier in the SQL the sink generates is backticked (the `DESCRIBE TABLE` probe quotes database and table; ClickHouse backticks), and everywhere names are *matched* — the `DESCRIBE` rows and the server's Native-block column matching on insert — they compare verbatim. That means a flattened ROS 2 column such as `pose.position.x` is one literal column throughout. Dots inside a backticked name are carried literally, never read as ClickHouse's nested-column syntax. Declare such columns backticked in your own DDL:

```sql
CREATE TABLE telemetry.poses
(
    `header.frame_id`    LowCardinality(String),
    `pose.position.x`    Nullable(Float64),
    `pose.orientation.w` Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY _ingest_time;
```

The same holds for the `columns` mapping: keys are dataset column names and values are table column names, both compared exactly, so a mapping entry can point a dotted dataset field at any table column (or vice versa).

### Type mapping

| Dataset / envelope Arrow type | Accepted table type |
|---|---|
| `bool` | `Bool` |
| `i8`…`i64` | `Int8`…`Int64` of the same or wider width (never narrowing) |
| `u8`…`u64` | `UInt8`…`UInt64` of the same or wider width; or a strictly wider `Int` (`u64` into `Int64` passes, and a value above `i64::MAX` is then a data error) |
| `f32` | `Float32`, `Float64` |
| `f64` | `Float64` |
| `str`, dictionary-encoded strings | `String`, `FixedString(n)` (a value longer than `n` bytes is a data error) |
| `bytes` | `String` (raw bytes), `FixedString(n)` |
| 16-byte `_event_id` | `UUID` (or `String`/`FixedString(16)` as raw bytes) |
| timestamps (`µs` or `ns`, UTC) | `DateTime64(P)` (any `P`, value floored or scaled), `DateTime` (`0`…`2106`; out of range is a data error) |
| list of any of the above | `Array(T)` or `Array(Nullable(T))` |

`Nullable`, `LowCardinality(..)`, and `SimpleAggregateFunction(.., T)` wrappers are handled. A nullable dataset field needs a `Nullable` table column (declare the field `required` otherwise); an `Array` column cannot be NULL, so a null list is stored as `[]`. Not supported in this release: `Tuple`, `Map`, `Enum`, `Decimal`, `Date`/`Date32`, 128/256-bit integers, nested arrays (`Array(Array(..))`), and record (struct) fields. `open` names the offending column.

ClickHouse turns a value that fails an implicit conversion into `NULL` in a nullable column instead of raising an error. The sink therefore checks every conversion that could lose data (narrowing, `FixedString` length, `DateTime`/`DateTime64` range) itself, so an over-long or out-of-range value fails the batch as a data error and is quarantined instead of silently becoming `NULL`.

## Delivery guarantees

- **At-least-once by default.** The reported class comes from the probe, not the configuration: `IdempotentBatch` for a `Replicated*MergeTree` table (unless `replicated_deduplication_window` is set to 0, on the table or server-wide) or a `MergeTree` table whose `non_replicated_deduplication_window` is above 0 (table setting, falling back to the server default, which is 0). Anything else, and every probe that cannot be read, reports `AtLeastOnce`. The class shows in `/status` and `thadam_sink_delivery_class{sink, class}`.
- **The token is the batch id.** With deduplication on, every insert carries `insert_deduplication_token = {epoch}-{sink}-{first}-{last}` (and `insert_deduplicate = 1`). It is a function of the batch id, so a retry after a timeout, a dropped connection, or a crash and replay repeats it and the server drops the duplicate. The sink id keeps two sinks writing one table from colliding on equal WAL ranges.
- **A bisected batch gets new tokens.** The runtime splits a batch that keeps failing; the halves cover different ranges and so carry different tokens, the same ones every time the same split happens.
- **Window limit.** Deduplication only covers the table's window (a number of recent blocks). A retry that arrives after more than that many other inserts can be applied twice; size `non_replicated_deduplication_window` (or the replicated one) above the number of batches you can have in flight across the longest outage you tolerate.
- **Without a window** (the default), a retry after an unknown outcome **duplicates**: the first attempt may have landed. Thadam logs a warning at `open` and reports `at_least_once`. Row-level deduplication downstream (for example `ReplacingMergeTree(_event_id)`) is the fallback; the event id is a stable key (ADR-0008, ADR-0011).
- **`dedup: off`** sends no token. A table that keeps a window still drops a byte-identical block by content hash, a ClickHouse behavior there is no insert setting to disable on non-replicated tables; Thadam does not rely on it and reports `at_least_once`. Prefer `auto`: with a token, distinct batches never collide even when their rows are identical.
- Inserts run with `async_insert = 0`: an asynchronous insert would acknowledge before the data is durable and, without `async_insert_deduplicate`, void the token.

## Errors and poison rows

| Condition | Class | Notes |
|---|---|---|
| Parse, type, range, constraint errors (codes 6, 26, 27, 33, 38, 41, 53, 70, 72, 117, 131, 321, 349, 469), and client-side data errors | **Permanent** | the runtime bisects the batch after `max_attempts_before_bisect` and quarantines the poison events |
| Memory limit, too many parts, too many simultaneous queries, timeouts, overload, Keeper trouble (codes 159, 173, 202, 203, 209, 210, 225, 236, 241, 242, 252, 279, 285, 319, 369, 473, 574, 745, 999) | **Transient** | retried with backoff |
| Authentication or permission failures (codes 192, 193, 194, 516, 497, 164) | **Fatal** | the sink is marked `Failed`; fix credentials or grants |
| Request timeout, dropped connection, connection refused | **Transient**, unknown outcome | retried with the **same** token, so a deduplicating table keeps one copy |
| Table or column dropped under a running sink (codes 10, 16, 60, 81) | **Transient** | the events stay in the WAL until the table is back; quarantining them would lose the ability to replay |
| Any code not listed | **Transient** | the error carries `unclassified_code`; bisection still bounds the retries |

The code tables were checked against `ErrorCodes.cpp` of ClickHouse `v26.8.22.13-lts`. Data errors keep the exception **code** in the error context but never the server's text, which can quote the offending row.

A constraint violation is the typical poison row: a table `CHECK temp > -273.15` refuses a batch holding `temp = -300` with code 469, the sink answers Permanent, the runtime splits the batch, and every clean half still lands.

## Integration tests

Docker-gated tests run the sink against a real server (public Docker Hub only, pinned to the LTS tag `clickhouse/clickhouse-server:26.8.22.13`; re-pin or try the current stable with `THADAM_TEST_CLICKHOUSE_IMAGE`):

```text
export CARGO_TARGET_DIR=$PWD/target/ch
cargo nextest run -p thadam-clickhouse --run-ignored only -E 'binary(docker_clickhouse)'
```

They cover insert and read-back of every column family, a retried batch with the same token (one copy with the window, documented duplicates without), the server stopped and restarted, a frozen server (timeout, then a retry that leaves one copy), poison rows and bisection, schema mismatch with a proof that nothing is created or altered, bad credentials, and the shared batch-sink conformance suite. Each test starts its own container with a label (`thadam-clickhouse-test`) and removes it, with its volumes, when it ends.
