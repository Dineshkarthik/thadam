//! Canonical in-memory model and contracts shared by every Thadam crate.
//!
//! # Responsibility
//!
//! - The event model: event, value, shape, times, quality, identity.
//! - Errors and their classification.
//! - WAL position types.
//! - Connector traits.
//! - Clock, shutdown, and disk budget primitives.
//!
//! # Deliberately not exposed
//!
//! - Event field mutability beyond invariant-preserving setters.
//! - Interning internals.
//! - Any WAL encoding, Arrow types, protocol types, YAML types, or the Tokio runtime.
//! - `contract_version` and broad capability sets.
//!
//! # Record representation (D8) and the opaque `Record`
//!
//! A [`Record`] is a shared [`Shape`] plus positional [`Value`]s. Neither is a
//! public field, and callers never compare shape pointers: identity is a
//! [`ShapeKey`] and equality is structural. Moving to a per-chain `ShapeId`
//! registry (D8, representation C) would change only the name-based accessors
//! [`Record::get`] and [`Record::iter`].
//!
//! # The 24-byte `Value` layout (ADR-0001 follow-up)
//!
//! Outcome: **adopted.** `size_of::<Value>()` is 24 bytes, down from 40.
//!
//! The 40 bytes came from `Utf8Bytes(Bytes)`: `Bytes` is 32 bytes, so it set the
//! width of every slot. The layout here holds a string as a thin-pointer
//! `Arc<str>`, boxes the two rare wide payloads (`Bytes`, nested `Record`), and
//! keeps `List(Vec<Value>)` (a `Vec`'s capacity niche absorbs the discriminant).
//! A typical W1 record of 7 fields shrinks by 7 x 16 = 112 bytes, about 30% of
//! the D8 report's 368 bytes per event.
//!
//! What it costs, measured in a scratch benchmark (arm64 laptop, 14-byte
//! strings, release build, validation excluded from both sides since decode
//! validates either way): slicing a `Bytes` took about 5 ns per string; copying
//! into an `Arc<str>` took about 25 ns, so about 20 ns more per string at
//! decode. The D8 spike already copied strings, so its baseline is unchanged.
//!
//! Three further points decided it:
//!
//! - A zero-copy `Bytes` slice pins the whole WAL read chunk (up to 1 MiB) for
//!   as long as the event lives, which ARCHITECTURE.md section 10 warns against
//!   for long-lived state.
//! - Handing out `&str` from a `Bytes` slice without re-validating needs
//!   `unsafe` (forbidden here) or a dependency that contains it; `Arc<str>` is
//!   validated once and free to read afterwards.
//! - [`Utf8Bytes`] is opaque, so a zero-copy representation can return behind
//!   the same API if the real decoder (PR8) shows the copy is a bottleneck.
//!   The costs above come from a scratch benchmark, not the real decoder, so
//!   PR8 should re-measure on the reference ARM and x86 hardware.
//!
//! `Value` size and the `Event` size budget are asserted by tests.

mod batch;
pub mod birth_store;
mod budget;
mod clock;
pub mod codes;
mod connector;
mod dataset;
mod error;
mod event;
mod identity;
mod metrics;
mod position;
mod quality;
mod record;
pub mod schema_store;
mod shape;
mod shutdown;
mod text;
mod time;
mod value;

pub use batch::{
    DatasetEvents, EventBatch, QUARANTINE_DATASET, RawBatch, RawMessage, SegmentBatch,
};
pub use birth_store::{BirthKey, BirthRecord, BirthStore};
pub use budget::{BudgetCategory, BudgetExceeded, CategoryUsage, DiskBudget};
pub use clock::{Clock, SyncFlag, SystemClock};
pub use connector::{
    AckHandle, AckMode, AckOutcome, AckStatus, AckTarget, AckToken, BatchLimits, BatchSink,
    BatchSinkFactory, BuildContext, ConnectionEpoch, DeliveryClass, EpochGate, SegmentSink,
    SegmentSinkFactory, Service, ServiceContext, ServiceFactory, SharedObjects, SinkBatch,
    SinkCapabilities, SinkContext, Source, SourceBatch, SourceCapabilities, SourceContext,
    SourceFactory, WriteOutcome,
};
pub use dataset::{DatasetDecl, LevelDecl};
pub use error::{Classify, ErrorContext, ErrorDomain, Reject, Retry, ThadamError, Severity};
pub use event::{Event, EventBuildError, EventBuilder, EventId, ObjectRef, Payload, RawPayload};
pub use identity::{
    AssetLevel, AssetPath, AssetPathError, Attributes, MAX_ASSET_LEVELS, SinkId, SourceId,
};
pub use metrics::{ConnectorMetric, MetricsRecorder, NoopMetricsRecorder};
pub use position::{BatchId, InvertedWalRange, ParseBatchIdError, WalEpoch, WalPos, WalRange};
pub use quality::{Quality, QualityFlags, QualityStatus};
pub use record::{Record, RecordError};
pub use schema_store::{SCHEMA_HASH_ATTR, SchemaStore, StoredSchema, write_atomic};
pub use shape::{
    FieldDef, FieldIdx, FieldType, MAX_SHAPE_FIELDS, Shape, ShapeError, ShapeFingerprint, ShapeKey,
    ShapeLimitExceeded, ShapeRef, ShapeRegistry,
};
pub use shutdown::Shutdown;
pub use text::{Sym, Utf8Bytes};
pub use time::{TimeDomain, TimeError, Times, Timestamp};
pub use value::{Value, ValueType};
