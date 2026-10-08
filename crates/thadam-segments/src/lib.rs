//! Parquet segment sink, segment catalog, and quarantine writer (PR14;
//! ADR-0013 layout, ADR-0011 commit-then-ship).
//!
//! # Responsibility
//!
//! - The [`SegmentSink`] implementation for Parquet
//!   ([`ParquetSegmentSink`], [`ParquetSegmentSinkFactory`], `kind:
//!   parquet`), with per-dataset open segments, rotation, and
//!   least-recently-written eviction at `max_open_files`.
//! - The staging-to-ready file lifecycle and the D18 finalize sequence
//!   (close + SHA-256, fdatasync, rename, directory fsync, `Finalized`
//!   catalog record, watermark, cursor commit, `Eligible`).
//! - The [`SegmentCatalog`]: append-only CRC-checked `catalog.log`, the
//!   shipper's pending-upload queue, and startup recovery (staging cleanup,
//!   log replay, footer rebuild, drop-beyond-watermark).
//! - [`QuarantineSinkFactory`]: the rejects sink with the fixed quarantine
//!   schema, on its own disk budget.
//! - Local retention (`release_local`): the budget eviction order *uploaded
//!   and expired*, *uploaded*, *backpressure*.
//!
//! # On-disk layout
//!
//! ```text
//! <sink dir>/
//!   catalog.log                          appended, CRC-checked frames
//!   staging/<dataset>/<seq>.inprogress   open segments
//!   ready/<dataset>/date=YYYY-MM-DD/hour=HH/{node}_{epoch:016x}_{first:020}_{last:020}.parquet
//!   ready/<dataset>/unsynced/...         rows received with clock_synced=false
//! ```
//!
//! `first` and `last` are the inclusive WAL positions the file covers (the
//! `[A]` of ADR-0013 "Paths, names, and keys" is fixed here as inclusive).
//! Footer key-values: `thadam.version`, `thadam.dataset`, `thadam.schema_fp`,
//! `thadam.wal_range`, `thadam.config_hash`, `thadam.enrichment_hash`,
//! `thadam.clock_synced`.
//!
//! # Crash safety in one paragraph
//!
//! A segment file claims every frame appended since its dataset's previous
//! finalize (empty datasets become *coverage markers*, no file), so the
//! watermark is the highest WAL position every configured dataset covers
//! contiguously. Recovery recomputes that over the replayed and
//! footer-rebuilt catalog and deletes every non-eligible record beyond it:
//! those rows are never durable-then-replayed, and the WAL regenerates them
//! into fresh files. This is what makes "each WAL range lands in exactly
//! one finalized file" hold across crash-replay.
//!
//! # Deliberately not exposed
//!
//! - Writers, file handles, staging paths, the catalog log format
//!   (`catalog` hides them; the footer read-back lives in its recovery).
//! - NDJSON (the debug format is not part of this build), Iceberg, Delta,
//!   narrow (long) layout, and user partition templates.

pub mod catalog;
mod config;
mod error;
mod failpoints;
mod partition;
mod quarantine;
mod rows;
mod sink;
mod writer;

pub use catalog::{
    CatalogVersion, LocalRetention, ReleaseReport, SegmentCatalog, SegmentId, SegmentRecord,
};
pub use config::{
    Compression, ParquetSinkConfig, QuarantineConfig, RotationConfig, parse_quarantine_config,
    parse_sink_config, quarantine_config_schema, sink_config_schema,
};
pub use error::SegmentError;
pub use quarantine::QuarantineSinkFactory;
pub use sink::{ParquetSegmentSink, ParquetSegmentSinkFactory};

#[cfg(any(test, feature = "failpoints"))]
pub mod crash {
    //! Failpoint arming for crash tests (the `failpoints` feature), matching
    //! `thadam_wal::failpoints` in spirit. See [`Failpoint`] for the names.
    pub use crate::failpoints::{Failpoint, Failpoints};
}

#[cfg(test)]
mod tests;
