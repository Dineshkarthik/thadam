//! The typed metrics hook for connectors and services.
//!
//! Connector crates report progress as [`ConnectorMetric`]s through the
//! [`MetricsRecorder`] on their `SinkContext`/`ServiceContext`, and the
//! runtime forwards them to its own metrics sink. This crate knows nothing
//! of the runtime or any metrics library: the event vocabulary is fixed
//! here, with label values restricted to small compile-time sets (outcome,
//! state) so downstream cardinality stays bounded (ARCHITECTURE.md section
//! 14). Recording must be cheap and must not block — it happens on the hot
//! path.

use std::time::Duration;

/// One reportable thing a connector (sink or service) did.
///
/// Borrowed so reporting allocates nothing. Every `&'static str` names a
/// fixed set documented on the variant; runtime id strings (`sink`) are
/// borrowed from the caller.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum ConnectorMetric<'a> {
    /// One object-store upload settled, worth one count of
    /// `thadam_ship_uploads_total{outcome}`:
    ///
    /// - `ok`: the store accepted this upload.
    /// - `conflict_equal`: a create-if-absent conflict settled by verifying
    ///   identical content (the crash-between-put-and-record path).
    /// - `fatal`: an unresolvable object-key collision; the pipeline fails.
    ///
    /// `bytes` is the segment's file size (`0` for `fatal`, which stores
    /// nothing new). `latency` covers the whole upload including
    /// verification, and is `None` for `fatal`.
    ShipUploaded {
        /// The shipper service's sink id.
        sink: &'a str,
        /// `ok`, `conflict_equal`, or `fatal`.
        outcome: &'static str,
        /// Segment bytes settled (0 for `fatal`).
        bytes: u64,
        /// Upload processing time, absent for `fatal`.
        latency: Option<Duration>,
    },
    /// An upload attempt failed transiently and a retry is pending; worth
    /// one `thadam_ship_uploads_total{outcome="retry"}` count and one
    /// `thadam_ship_retry_age_seconds` observation.
    ShipRetried {
        /// The shipper service's sink id.
        sink: &'a str,
        /// Time since the upload's first failed attempt.
        retry_age: Duration,
    },
    /// The absolute segment-catalog population at a state change, worth one
    /// sample of `thadam_segment_files{state}` and `thadam_segment_bytes{state}`.
    /// Reporters send totals (not deltas) after every catalog transition, so
    /// last-writer-wins keeps the gauges exact. `state` is one of:
    ///
    /// - `staging`: files still open for writing (only the segment sink can
    ///   report these; values include in-progress byte estimates).
    /// - `finalized`: durable locally, not yet ship-eligible.
    /// - `eligible`: the WAL cursor passed the segment's end.
    /// - `uploaded`: shipped, still on local disk.
    /// - `deleted`: local file removed after upload (tombstone count only;
    ///   `bytes` is the file's recorded size at finalization).
    SegmentFiles {
        /// The segment sink the catalog belongs to.
        sink: &'a str,
        /// `staging`, `finalized`, `eligible`, `uploaded`, or `deleted`.
        state: &'static str,
        /// Records in `state` that have (or had) a file.
        files: u64,
        /// Recorded file bytes for those records.
        bytes: u64,
    },
    /// A segment file completed the D18 finalize sequence (close, checksum,
    /// rename, catalog append), worth one `thadam_segment_finalize_seconds`
    /// observation. Coverage markers are not files and report nothing.
    SegmentFinalized {
        /// The segment sink id.
        sink: &'a str,
        /// The finalized file's size.
        bytes: u64,
        /// Duration of the finalize sequence.
        latency: Duration,
    },
}

/// Receives [`ConnectorMetric`]s. Implementations must be cheap and must
/// not block: they are called on the hot path.
pub trait MetricsRecorder: Send + Sync + 'static {
    /// Records one metric.
    fn record(&self, metric: ConnectorMetric<'_>);
}

/// Discards everything (the default for contexts built without a recorder).
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopMetricsRecorder;

impl MetricsRecorder for NoopMetricsRecorder {
    fn record(&self, _metric: ConnectorMetric<'_>) {}
}
