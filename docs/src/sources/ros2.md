# ROS 2

Thadam's ROS 2 support is staged (ADR-0007). **Stage 1, the transport-independent decoder crate `thadam-ros2-msg`, is what exists today**: it parses ROS 2 `.msg` definitions and decodes CDR payloads into Thadam records, so every later source decodes the same way. There is no ROS 2 source connector yet.

| Stage | Status |
|---|---|
| Stage 1: `thadam-ros2-msg` (.msg parser + CDR decoder) | **Done** |
| Stage 2: MCAP / rosbag2 file source, `thadam export --mcap`, `examples/ros2-mcap-to-parquet` | Planned |
| Stage 3: live-transport spike (Zenoh-native, sidecar, `r2r`) | Planned |
| Stage 4: live ROS 2 source | Planned |

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
