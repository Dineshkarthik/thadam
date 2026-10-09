//! Transport-independent ROS 2 message decoding into the Thadam canonical
//! model (ADR-0007 stage 1).
//!
//! This crate does three things:
//!
//! 1. Parses ROS 2 `.msg` interface definitions ([`msg`]), including the
//!    concatenated multi-definition format used by MCAP/rosbag2
//!    `ros2msg` schemas ([`msg::SchemaDocument`]).
//! 2. Resolves nested type references and builds one Thadam [`Shape`] per
//!    message type ([`Registry`]).
//! 3. Decodes CDR-serialized payloads (XCDR1, little- and big-endian, with
//!    the 4-byte encapsulation header) into [`Record`]s of that shape
//!    ([`CompiledMessage::decode`]).
//!
//! The decoder is transport-independent: MCAP files, rosbag2 files, and any
//! future live transport all produce the same `Record`s from the same schema
//! text.
//!
//! # CDR coverage and limits
//!
//! - Encapsulation schemes `0x0000` (big-endian) and `0x0001` (little-endian)
//!   are supported; anything else (including XCDR2) is
//!   `decode.unsupported_encoding`.
//! - Alignment is computed relative to the start of the payload **after** the
//!   4-byte encapsulation header, with primitive sizes as alignments (1, 2, 4,
//!   or 8 bytes).
//! - `string` is read as a length (including the trailing NUL) plus bytes;
//!   `wstring` is read as a UTF-16 code-unit count plus code units, with a
//!   single trailing NUL unit tolerated. `wstring` is re-encoded to UTF-8.
//! - A `bool` written as any non-zero byte decodes to `true`.
//! - Nested message nesting deeper than 32 levels and definition cycles are
//!   rejected at compile time (`schema.invalid_shape`).
//! - Every read is bounds-checked. A declared sequence/string length that
//!   cannot fit in the remaining payload fails with
//!   `decode.invalid_payload`; sequences of zero-minimum-size element types
//!   are additionally capped at 16 384 elements (`decode.too_large`), since a
//!   corrupt length cannot otherwise be cross-checked against payload size.
//! - Error messages never contain payload text.
//!
//! # Type mapping
//!
//! ROS 2 scalar types map onto the extended Thadam value set as follows.
//! `byte` is the ROS alias of `int8` and `char` of `uint8`.
//!
//! | ROS 2 type | `FieldType` | `Value` |
//! |---|---|---|
//! | `bool` | `Bool` | `Bool` |
//! | `byte` | `I8` | `I8` |
//! | `char` | `U8` | `U8` |
//! | `int8` / `int16` / `int32` / `int64` | `I8` / `I16` / `I32` / `I64` | same |
//! | `uint8` / `uint16` / `uint32` / `uint64` | `U8` / `U16` / `U32` / `U64` | same |
//! | `float32` / `float64` | `F32` / `F64` | same |
//! | `string`, `wstring`, `string<=N`, `wstring<=N` | `Str` | `Str` |
//! | `builtin_interfaces/Time t` | `t.sec`: `I32`, `t.nanosec`: `U32`, **plus** `t`: `Ts` | `Ts` holds `sec * 10^9 + nanosec` (always fits; no normalization) |
//!
//! Nested messages flatten into dotted field names, recursively:
//! `std_msgs/Header header` becomes `header.stamp.sec`,
//! `header.stamp.nanosec`, `header.stamp` (a `Ts`), and `header.frame_id`.
//! The `builtin_interfaces/Time` triple applies wherever `Time` appears as a
//! scalar field, including inside record-list element shapes.
//!
//! Array types:
//!
//! | ROS 2 array | `FieldType` | `Value` |
//! |---|---|---|
//! | `byte[]`, `char[]`, `uint8[]` (any bound form) | `Bytes` | `Bytes` (raw bytes) |
//! | `float32*` | `List(F32)` | `F32s` |
//! | `float64*` | `List(F64)` | `F64s` |
//! | `int8*`, `int16*`, `uint16*`, `int32*` | `List(I32)` | `I32s` (lossless widening to i32) |
//! | `int64*`, `uint32*` | `List(I64)` | `I64s` (lossless widening to i64) |
//! | `uint64*` | `List(U64)` | `List` (no wider signed typed array exists) |
//! | `bool*` | `List(Bool)` | `List` |
//! | `string*`, `wstring*` | `List(Str)` | `List` |
//! | `pkg/Type*` (nested message) | `List(Record(shape))` | `List` of `Record`, item shape built recursively |
//!
//! where `*` is any array bound form: unbounded `T[]`, bounded `T[<=N]`, or
//! fixed-size `T[N]` (fixed arrays carry no length prefix on the wire).
//! Fixed arrays that arrive with a wrong element count, bounded sequences
//! and strings that exceed their bound, and fixed arrays decoded past the
//! payload all fail with `decode.invalid_payload`.
//!
//! Nested message arrays become a `List` of `Record` values whose shape is
//! the recursively flattened element shape (scalar-only fields remain dotted
//! inside it). A fixed-size nested array is decoded as the same `List`.
//!
//! # Definition-only constructs
//!
//! - **Constants**: parsed, range-checked, and kept on [`msg::MessageSpec`]
//!   (reachable through the [`Registry`]); they are never encoded into the
//!   shape or the record. ROS distinguishes constants from defaults by the
//!   `=`: with or without spaces around it the line is a constant.
//! - **Default values**: parsed and stored on [`msg::Field`], ignored for
//!   decoding — every CDR payload carries every field explicitly.
//! - Comments (`#` to end of line) are stripped, per the `.msg` grammar.
//!
//! # Example
//!
//! ```
//! use thadam_ros2_msg::Registry;
//!
//! let mut registry = Registry::new();
//! registry
//!     .register("builtin_interfaces", "Time", "int32 sec\nuint32 nanosec\n")
//!     .unwrap();
//! registry
//!     .register(
//!         "std_msgs",
//!         "Header",
//!         "builtin_interfaces/Time stamp\nstring frame_id\n",
//!     )
//!     .unwrap();
//! let msg = registry.compile("std_msgs/Header").unwrap();
//! assert!(msg.shape().index_of("stamp").is_some()); // the Timestamp field
//!
//! // 4-byte LE encapsulation header, sec, nanosec, frame_id "cam" (NUL-terminated).
//! let mut cdr = vec![0x00, 0x01, 0x00, 0x00, 1, 0, 0, 0, 250, 0, 0, 0, 4, 0, 0, 0];
//! cdr.extend_from_slice(b"cam\0");
//! let record = msg.decode(&cdr).unwrap();
//! assert_eq!(record.get("frame_id").and_then(|v| v.as_str()), Some("cam"));
//! ```

pub mod msg;
mod registry;

mod cdr;

pub use registry::{CompiledMessage, MAX_MESSAGE_DEPTH, Registry};
