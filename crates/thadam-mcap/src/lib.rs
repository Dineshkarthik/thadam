//! MCAP / rosbag2-MCAP file source connector (ADR-0007 stage 2).
//!
//! The `mcap` source replays one or more [MCAP](https://mcap.dev) files —
//! including rosbag2's MCAP storage format — as one finite stream, merged in
//! log-time order. It is a *file* source, finite, and resumable: re-reading a
//! file would put its messages at new WAL positions (duplicate rows), so the
//! source remembers per file how much of it was durably committed.
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
//! bytes into records is the transform chain's job (dataset `decode: ros2`),
//! which resolves schema text from the pipeline's durable
//! [`SchemaStore`](thadam_core::SchemaStore) by the `mcap.schema_hash`
//! attribute every `ros2msg`-channel message carries. The source writes each
//! `ros2msg` schema to the store the first time its channel is seen — in
//! both decode modes — before the first message referencing it can be
//! committed to the WAL; a store write failure is a classified error, never
//! silently skipped.
//! Every message carries `mcap.*` attributes (topic, schema name and
//! encoding, message encoding, channel id, sequence, log and publish time)
//! plus the normalized `topic` attribute that `match.topic` filters and
//! `{topic.N}` identity templates read. With `pace: max` the ingest time is
//! the message's log time, so replays are byte-deterministic; paced replays
//! stamp the wall clock, like a live capture. The source is finite
//! (`next_batch` returns `None` at the end).
//!
//! # Resume
//!
//! Built with a data directory (the runtime always does), the source
//! acknowledges after the WAL commit
//! ([`AckMode::OnCommit`](thadam_core::AckMode::OnCommit)) and writes
//! `resume.json` into its component directory on every ack: per file, its
//! identity (path, size, modification time) and how many of its messages the
//! committed batches cover. A restart skips completed files and continues the
//! others after their last committed message; uncommitted batches are read
//! again, so no uncommitted message is skipped, and a crash inside the window between a WAL
//! commit and the state write may repeat that window's batches
//! (at-least-once). A changed file, or changed `topics`/`start`/`end`, is
//! replayed from the start with a warning; delete `resume.json` to force
//! that. Built without a data directory the source has no resume and acks
//! are no-ops ([`AckMode::None`](thadam_core::AckMode::None)).
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
mod resume;
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
