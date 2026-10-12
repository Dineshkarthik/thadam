# ROS 2

Thadam's ROS 2 support is staged (ADR-0007). Stage 1 (`thadam-ros2-msg`, the transport-independent `.msg` parser and CDR decoder) and the Stage 2 file source (`thadam-mcap`, replaying MCAP and rosbag2-MCAP recordings) exist today.

| Stage | Status |
|---|---|
| Stage 1: `thadam-ros2-msg` (.msg parser + CDR decoder) | **Done** |
| Stage 2: MCAP / rosbag2 file source (`thadam-mcap`) | **Done** |
| Stage 2: `thadam export --mcap` ([`thadam export`](../cli/export.md)) | **Done** |
| Stage 2, rest: [`examples/ros2-mcap-to-parquet`](https://github.com/Dineshkarthik/thadam/tree/main/examples/ros2-mcap-to-parquet) (MCAP to decoded, validated Parquet and back) | **Done** |
| Stage 3: live-transport spike (Zenoh-native, sidecar, `r2r`) | **Done** (spike S3: rmw_zenoh primary, bridge mapped) |
| Stage 4: live ROS 2 source ([`ros2_live`](ros2-live.md)) | **In progress** (rmw_zenoh transport, experimental) |

## How it works

```text
.msg definition text ──parse──▶ Registry ──compile──▶ CompiledMessage
 (or a ros2msg schema document)      │                   │ Shape + decode plan
 CDR payload  ───────────────────────┴──── decode ──────▶ Record
```

1. **Parse.** `Registry::register(package, name, definition)` parses one `.msg` definition: the fifteen primitive types, bounded strings (`string<=N`, `wstring<=N`), fixed arrays `T[N]`, bounded sequences `T[<=N]`, unbounded sequences `T[]`, and nested references (`pkg/Type`, `pkg/msg/Type`, or bare `Type` in the same package). Constants are parsed, range-checked, and kept on the definition but never encoded; default values (`float64 w 1`) are parsed and ignored for decoding; `#` comments are stripped. `Registry::register_ros2msg(main_type, text)` takes the concatenated multi-definition format carried by MCAP/rosbag2 `ros2msg` schemas (the main definition, then `================================================================================` + `MSG: pkg/Type` sections).
2. **Compile.** `Registry::compile(type_name)` resolves nested references and builds one Thadam `Shape` plus a decode plan per message type (cached; re-registering a type invalidates its entry). Nested scalar messages flatten to **dotted field names**: `std_msgs/Header header` becomes `header.stamp.sec`, `header.stamp.nanosec`, `header.frame_id`, … A `builtin_interfaces/Time` field `t` additionally produces a `t` field of type `Ts` holding `sec * 10^9 + nanosec` (the value always fits; no normalization). Definition cycles and nesting beyond 32 levels are rejected at compile time.
3. **Decode.** `CompiledMessage::decode(&payload)` decodes one XCDR1 CDR payload (encapsulation schemes `0x0000` big-endian and `0x0001` little-endian; XCDR2 is rejected as `decode.unsupported_encoding`) into a `Record` of that shape. Alignment is relative to the start of the payload **after** the 4-byte encapsulation header. Every read is bounds-checked; a corrupted declared length fails before any allocation (`decode.invalid_payload`), and sequences of zero-size element types are capped at 16 384 elements (`decode.too_large`). Malformed or truncated payloads always error — a proptest drives arbitrary bytes and encapsulation headers through every compiled shape; decoding never panics and never puts payload text in error messages.

## Type mapping

`byte` is the ROS alias of `int8`, `char` of `uint8`.

| ROS 2 type | Thadam `FieldType` | Thadam `Value` |
|---|---|---|
| `bool` | `Bool` | `Bool` (any non-zero byte is `true`) |
| `byte` | `I8` | `I8` |
| `char` | `U8` | `U8` |
| `int8` … `int64` | `I8` … `I64` | matching |
| `uint8` … `uint64` | `U8` … `U64` | matching |
| `float32` / `float64` | `F32` / `F64` | matching |
| `string`, `wstring`, `string<=N`, `wstring<=N` | `Str` | `Str` (`wstring` re-encoded UTF-16→UTF-8; bounds enforced) |
| `builtin_interfaces/Time t` | `t.sec`: `I32`, `t.nanosec`: `U32`, `t`: `Ts` | sec/nanosec plus the timestamp |
| `byte[]`, `char[]`, `uint8[]` | `Bytes` | `Bytes` (raw bytes) |
| `float32` array | `List(F32)` | `F32s` typed array |
| `float64` array | `List(F64)` | `F64s` typed array |
| `int8` / `int16` / `uint16` / `int32` array | `List(I32)` | `I32s` (lossless widen to i32) |
| `int64` / `uint32` array | `List(I64)` | `I64s` (lossless widen to i64) |
| `uint64` array | `List(U64)` | `List` (no wider signed typed array exists) |
| `bool` array | `List(Bool)` | `List` |
| `string` / `wstring` array | `List(Str)` | `List` |
| `pkg/Type` array (nested message) | `List(Record(shape))` | `List` of `Record` |

Array rules: all three bound forms (`T[]`, `T[<=N]`, `T[N]`) decode to the same container; fixed arrays carry no length prefix on the wire. **Widening rule:** narrower integers decode into the nearest typed array that holds them losslessly — int8/int16/uint16 land in `I32s`, uint32 in `I64s`; `uint64` has no lossless typed array and stays a `List` of `U64`. Nested-message arrays decode to a `List` of `Record` values whose shape is the recursively flattened element type (a `Time` scalar inside still yields the sec/nanosec/`Ts` triple).

## Wire details and limits

- A CDR `string` is a `uint32` length **including** the trailing NUL, then the bytes; a missing NUL or invalid UTF-8 is an error (`decode.invalid_payload`, `decode.invalid_utf8`). A `wstring` is a `uint32` UTF-16 code-unit count plus units; one trailing NUL unit is tolerated. Declared `<=N` bounds are enforced.
- `float64[9]` and other fixed arrays read exactly `N` elements with no length.
- Trailing bytes are ignored, so a padded transport frame still decodes.
- Compile-time limits: 4 096 fields per shape (Thadam core), 32 levels of message nesting.

## API sketch

```rust,ignore
let mut registry = Registry::new();
registry.register_ros2msg("sensor_msgs/JointState", &mcap_schema_text)?;
let msg = registry.compile("sensor_msgs/JointState")?;      // cached
let shape = msg.shape();                                     // header.stamp, position, …
let record = msg.decode(&payload)?;                          // Record or ThadamError
```

The parsed definition, including constants, is available as `registry.spec(type_name)`. See the crate rustdoc for the full surface. Performance on the reference machine (Apple Silicon): `sensor_msgs/JointState` (6 joints) decodes at roughly 2.4 M messages/s (~420 ns each), `sensor_msgs/Imu` at roughly 3.5 M messages/s (~290 ns each) — see `cargo bench -p thadam-ros2-msg`.

## MCAP file source

The `mcap` source replays one or more [MCAP](https://mcap.dev) files — including rosbag2's MCAP storage format — as one finite stream, merged in log-time order. Built as the `thadam-mcap` crate on the pure-Rust `mcap` crate (with the zstd and lz4 chunk decompressors), satisfying ADR-0007 stage 2. It is a *file* source: there is no broker, no session, and nothing to acknowledge.

```yaml
sources:
  - id: rosbag
    kind: mcap
    config:
      paths: [/bags/run-12, /bags/run-13.mcap]
      topics: [/joint_states, /tf/]
      start: 2026-10-01T12:00:00Z
      end: 1_759_363_200_000_000_000
      pace: realtime
      decode: ros2
```

### Configuration

Unknown keys are rejected. `paths` is the only required key.

| Key | Type | Default | Description |
|---|---|---|---|
| `paths` | list | — | **Required, non-empty.** MCAP files, or directories scanned one level deep for `*.mcap`. Duplicates are removed, the list is sorted, and all files are merged into one stream. |
| `topics` | list | all | Topic allow-list: exact names, or prefixes when the entry ends with `/` (`/tf/` matches `/tf/static` but `/tf` is not a prefix). |
| `start` / `end` | log time | unbounded | Inclusive bounds on the MCAP log time: an RFC 3339 string or integer nanoseconds since the Unix epoch. `start` must not be after `end`. |
| `pace` | `max`, `realtime`, or a positive number | `max` | Replay pace. `max` reads as fast as possible; `realtime` preserves the original inter-message gaps; a number is a time factor (`2.0` halves gaps, `0.5` doubles them). A single gap never sleeps longer than a day. |
| `decode` | `raw` or `ros2` | `raw` | See below. Payloads stay raw in both modes. |

`paths` may name files being copied into place: a missing file is a transient `connector.connect_failed` error, so the run retries; a directory with no `*.mcap` in it fails validation as `config.invalid`. A truncated or corrupt file (bad magic, a cut record, a failed chunk CRC, unsupported compression) is a permanent, classified `connector.protocol` error, never a panic — and a file that ends before its `DataEnd` record is treated as truncated, so partial replays never pass silently as complete ones.

### `decode: raw` versus `decode: ros2`

The source never turns payload bytes into records — that is the transform chain's job, matching ADR-0007's separation of transport and schema. The difference:

- `raw` passes every channel through untouched.
- `ros2` additionally pre-flights each channel whose schema encoding is `ros2msg`: the schema is registered and compiled when the channel is first seen, so a schema that does not compile fails `open` (a `schema.*` error naming the file) instead of surprising the pipeline mid-run. Messages on such channels additionally carry `mcap.ros2.fields` (see below).

### The schema store and `decode: ros2` on a dataset

In both modes the source writes every `ros2msg` channel's schema text to the pipeline's **schema store** the first time the channel is seen — durable (`tmp` file, fsync, rename, directory fsync), content-addressed, and idempotent — *before* the first message referencing it can be committed to the WAL, so a replay never lands a message whose schema was lost. The store lives at [`<data_dir>/schemas/`](../operations/data-directory.md#the-schema-store-schemas) (`thadam_core::SchemaStore`, one `<sha-256 hash>.schema` file per schema holding name, encoding, and text); a store write failure is a classified error, never silently skipped. Every message on such a channel carries the schema's hash as `mcap.schema_hash`.

A dataset then opts into ROS 2 decoding:

```yaml
datasets:
  - id: joints
    match: { source: rosbag, topic: /joint_states }
    decode: ros2
    time: { field: /header.stamp, unit: ns, domain: utc }
    fields:
      - { id: 1000, name: frame, type: str, from: /header.frame_id }
      - { id: 1001, name: position, type: list<f64>, from: /position }
```

The chain resolves the schema text by the message's `mcap.schema_hash` — once per hash, since the store is content-addressed, so a compiled-cache hit costs no store read — decodes the CDR payload with `thadam-ros2-msg`, and reads the decoded **record** directly: a dataset payload pointer that is a single JSON Pointer token (`/header.stamp`, `/pose.position.x`, the documented spelling) resolves to one field lookup on the record, with the declared-field coercion running on the record's own values, so a `Ts` stays a timestamp and `Bytes` stay bytes. Failures are quarantined rejects with classified codes, never silent drops, and error text never contains payload bytes: a missing `mcap.schema_hash` is `decode.schema_ref_missing`, a hash the store does not hold is `decode.unknown_schema`, and schema or CDR problems are the `schema.*`/`decode.*` codes the decoder already classifies.

Only the pointers a record cannot serve fall back to spelling the record as one flat JSON object (one key per dotted name), materialized per message that needs it: nested or indexed multi-token pointers such as `/name/0`, and `explode` over a typed array (the JSON spelling treats it as an array of scalars). One behavior change falls out of the direct read: non-finite floats used to become `null` in the JSON spelling — a `pose` step read them as `pose_missing_field`, and a NaN typed array was rejected at extraction. They now reach `pose`/`joint_state` as non-finite values and are flagged `pose_not_finite`/`joint_state_not_finite` as designed; `required` float fields no longer reject the message for them.

Dotted names may be declared as dataset fields: each is one literal flat column (`pose.position.x`), and an omitted `from` defaults to `/<name>` as a single JSON Pointer token — the spelling the direct record read serves — see [dataset fields](../configuration/datasets.md#fields). The [robotics conventions](../concepts/robotics-conventions.md) page has a complete `PoseStamped` dataset example.

### Ordering, timing, and replay determinism

Files are merged by MCAP log time; ties break by sorted file path, then write order. Within one file the stream follows file (write) order — recorders write log-time-ordered streams. **With `pace: max`, each message's ingest time is set to its MCAP log time**, so replaying the same inputs lands byte-identical in the WAL: replays are deterministic. With a paced replay the ingest time is the wall clock at emission, like a live capture. Pacing follows emitted messages: a filtered-out message does not contribute to gaps, and out-of-order log times never sleep negative. `BatchLimits.linger` bounds how long one paced pull waits to fill its batch, exactly as for the network sources.

### Messages, attributes, and acknowledgements

Each MCAP message becomes one raw message with these attributes:

| Attribute | Value |
|---|---|
| `topic` | The channel's topic (the normalized attribute read by `match.topic` filters and `{topic.N}` identity templates). |
| `mcap.topic` | The channel's topic, namespaced. |
| `mcap.schema_name` | The schema name (for example `sensor_msgs/msg/JointState`), or `""` for schemaless channels. |
| `mcap.schema_encoding` | For example `ros2msg` or `jsonschema`, or `""`. |
| `mcap.message_encoding` | For example `cdr` or `json`. |
| `mcap.channel_id` | The channel id, decimal. |
| `mcap.sequence` | The recorder's sequence number, decimal. |
| `mcap.log_time` | The MCAP log time, nanoseconds, decimal. |
| `mcap.publish_time` | The MCAP publish time, nanoseconds, decimal. |
| `mcap.ros2.fields` | With `decode: ros2` only, and only on channels with a `ros2msg` schema: the comma-separated flattened field names of the compiled shape (for example `header.stamp.sec,header.stamp.nanosec,header.stamp,header.frame_id,name,position,velocity,effort`). |
| `mcap.schema_hash` | On every message of a channel with a `ros2msg` schema: the hex SHA-256 key of the channel's schema in the pipeline's [schema store](../operations/data-directory.md#the-schema-store-schemas). The chain's dataset `decode: ros2` resolves schema text with it. |

### Timestamps

ROS 2 has two clocks you can choose from in `time`:

- Publisher time, from the message: `header.stamp` (a `builtin_interfaces/Time`, decoded to unix nanoseconds). Use `field: /header.stamp, unit: ns`. The dotted name is one JSON Pointer token, because the decoded field is the flattened key `header.stamp`.
- Recorder time, from MCAP: `attr: mcap.log_time, unit: ns`. Publish time is also available as `attr: mcap.publish_time, unit: ns`.

`time.field` reads the decoded record too: with dataset `decode: ros2`, `/header.stamp` addresses the flattened key the decoder writes (a nanosecond `Ts`, read as-is with `unit: ns`), exactly as the field paths above do.

The source is finite: it ends with `None` once the inputs are drained. Acks are no-ops (`ack_mode: none`) and stay ordered trivially, because files are re-readable: replay is idempotent from the input, so after a crash or restart the source simply reads the files again from the start and nothing is skipped or acknowledged early.

### Performance note

Files stream from disk one chunk at a time (peak memory is one chunk record plus its decompressed form; the reader is safe Rust throughout — no mmap). Chunk reads and decompression run synchronously inside batch pulls; per-chunk cost is small next to a WAL commit.
