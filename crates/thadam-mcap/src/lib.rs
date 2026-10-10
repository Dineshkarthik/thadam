//! MCAP / rosbag2-MCAP file source connector (ADR-0007 stage 2).
//!
//! The `mcap` source replays one or more [MCAP](https://mcap.dev) files —
//! including rosbag2's MCAP storage format — as one finite stream, merged in
//! log-time order. It is a *file* source: replay is idempotent because the
//! input re-reads identically from the start.
//!
//! ```yaml
//! sources:
//!   - id: rosbag
//!     kind: mcap
//!     config:
//!       paths: [/bags/run-12]          # files and/or directories of *.mcap
//!       topics: [/joint_states, /tf/]  # optional: exact, or prefix ending in /
//!       start: 2026-10-01T12:00:00Z    # optional inclusive log-time bound
//!       end: 1_759_363_200_000_000_000 #   (RFC 3339 or integer nanoseconds)
//!       pace: realtime                 # max | realtime | <factor>, e.g. 2.0
//!       decode: ros2                   # raw | ros2
//! ```
//!
//! # Semantics in one paragraph
//!
//! Payloads stay raw: `decode: ros2` only pre-flights `ros2msg` schemas
//! through the `thadam-ros2-msg` compiler (fail fast on a schema that does
//! not compile) and attaches the compiled shape's field names; turning CDR
//! bytes into records is the transform chain's job, using [`decode_ros2`].
//! Every message carries `mcap.*` attributes (topic, schema name and
//! encoding, message encoding, channel id, sequence, log and publish time)
//! plus the normalized `topic` attribute that `match.topic` filters and
//! `{topic.N}` identity templates read. With `pace: max` the ingest time is
//! the message's log time, so replays are byte-deterministic; paced replays
//! stamp the wall clock, like a live capture. The source is finite
//! (`next_batch` returns `None` at the end) and ack-free
//! ([`AckMode::None`](thadam_core::AckMode::None)) — files are re-readable,
//! so there is nothing to acknowledge and a crash mid-replay loses nothing
//! on re-run.
//!
//! # Export
//!
//! [`McapExporter`] is the writer side: it turns captured messages into one
//! indexed MCAP file. Each `(source, topic)` becomes a channel and keeps its
//! original encoding, with schema text supplied by a [`SchemaResolver`]
//! because the WAL does not carry it. WAL gaps and file provenance are
//! written as `thadam.gap` and `thadam.provenance` metadata records. Unresolved
//! schemas are listed in a `thadam.unresolved_schemas` record, so nothing is
//! lost silently.

mod config;
mod decode;
mod error;
mod export;
mod factory;
mod file_reader;
mod source;

pub use config::{DecodeMode, McapConfig, Pace, TimeBound, TopicFilter, config_schema};
pub use decode::decode_ros2;
pub use error::McapError;
pub use export::{
    DefaultEncoding, ExportOptions, ExportReport, GapKind, McapExporter, Provenance,
    SchemaResolver, UnresolvedSchema,
};
pub use factory::McapSourceFactory;
pub use source::{McapSource, attr};
