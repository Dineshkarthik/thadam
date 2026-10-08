//! Ship progress reporting.
//!
//! There is intentionally no typed channel from a service to the runtime's
//! `MetricsSink` (`thadam-core` knows nothing of runtime types, and the
//! runtime knows connectors only through `thadam-core` traits), so the shipper
//! reports through this crate-level hook and structured `tracing` fields
//! named for the planned `thadam_ship_*` metric names (metrics.md "Planned
//! metric names"). A Prometheus mapping lands when the runtime gains a
//! service metric channel.

use std::time::Duration;

/// Receives shipper progress. Implementations must be cheap and non-blocking.
pub trait ShipMetrics: Send + Sync + 'static {
    /// One segment reached the store and was verified.
    fn uploaded(&self, bytes: u64, latency: Duration, outcome: &str);
    /// An upload attempt failed and will be retried.
    fn retry(&self, segment: &SegmentId, attempt: u32, retry_age: Duration);
    /// A create-if-absent conflict was settled by verification.
    fn conflict_verified(&self, same_content: bool);
    /// A retention pass freed local files.
    fn released(&self, files: u64, bytes: u64);
    /// The eligible backlog at the last scan.
    fn backlog(&self, files: usize);
}

use thadam_segments::SegmentId;

/// Discards everything (the default until the runtime can feed a sink).
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopShipMetrics;

impl ShipMetrics for NoopShipMetrics {
    fn uploaded(&self, _bytes: u64, _latency: Duration, _outcome: &str) {}
    fn retry(&self, _segment: &SegmentId, _attempt: u32, _retry_age: Duration) {}
    fn conflict_verified(&self, _same_content: bool) {}
    fn released(&self, _files: u64, _bytes: u64) {}
    fn backlog(&self, _files: usize) {}
}
