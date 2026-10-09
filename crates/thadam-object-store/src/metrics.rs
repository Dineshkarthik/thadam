//! Ship progress reporting.
//!
//! The shipper reports through the crate-level [`ShipMetrics`] hook, kept
//! small for tests. In production the runtime threads a
//! [`thadam_core::MetricsRecorder`] through the service context and
//! [`RecorderShipMetrics`] adapts it to this hook, landing the
//! `thadam_ship_*` families (metrics.md); segment-catalog gauges
//! (`thadam_segment_*`) are emitted through the same recorder, labeled with
//! the shipper's `from` sink.

use std::sync::Arc;
use std::time::Duration;

use thadam_core::{ConnectorMetric, MetricsRecorder};
use thadam_segments::SegmentId;

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

/// Discards everything (the default when no receiver is plumbed).
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopShipMetrics;

impl ShipMetrics for NoopShipMetrics {
    fn uploaded(&self, _bytes: u64, _latency: Duration, _outcome: &str) {}
    fn retry(&self, _segment: &SegmentId, _attempt: u32, _retry_age: Duration) {}
    fn conflict_verified(&self, _same_content: bool) {}
    fn released(&self, _files: u64, _bytes: u64) {}
    fn backlog(&self, _files: usize) {}
}

/// Adapts the runtime's metrics hook to [`ShipMetrics`]: what the shipper
/// reports lands in the `thadam_ship_*` families, labeled `sink` with the
/// shipper service's own id.
pub struct RecorderShipMetrics {
    sink: String,
    recorder: Arc<dyn MetricsRecorder>,
}

impl RecorderShipMetrics {
    /// An adapter reporting through `recorder` with `sink` as the `sink`
    /// label.
    pub fn new(sink: impl Into<String>, recorder: Arc<dyn MetricsRecorder>) -> Self {
        RecorderShipMetrics {
            sink: sink.into(),
            recorder,
        }
    }
}

impl ShipMetrics for RecorderShipMetrics {
    fn uploaded(&self, bytes: u64, latency: Duration, outcome: &str) {
        // The shipper's internal outcomes: a fresh put (single or
        // multipart) is `ok`; a verified conflict is `conflict_equal`
        // (counted once here, so `conflict_verified(true)` adds nothing).
        let outcome = match outcome {
            "verified" => "conflict_equal",
            _ => "ok",
        };
        self.recorder.record(ConnectorMetric::ShipUploaded {
            sink: &self.sink,
            outcome,
            bytes,
            latency: Some(latency),
        });
    }

    fn retry(&self, _segment: &SegmentId, _attempt: u32, retry_age: Duration) {
        self.recorder.record(ConnectorMetric::ShipRetried {
            sink: &self.sink,
            retry_age,
        });
    }

    fn conflict_verified(&self, same_content: bool) {
        // A mismatch is the Fatal OBJECT_KEY_COLLISION: the upload never
        // happened, so bytes stay zero and there is no latency.
        if !same_content {
            self.recorder.record(ConnectorMetric::ShipUploaded {
                sink: &self.sink,
                outcome: "fatal",
                bytes: 0,
                latency: None,
            });
        }
    }

    // Local retention and the backlog scan have no reserved metric names;
    // they stay on structured tracing fields.
    fn released(&self, _files: u64, _bytes: u64) {}
    fn backlog(&self, _files: usize) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Seen {
        Uploaded { outcome: String, bytes: u64 },
        Retried { retry_age: Duration },
    }

    struct Recording(Mutex<Vec<Seen>>);

    impl MetricsRecorder for Recording {
        fn record(&self, metric: ConnectorMetric<'_>) {
            let seen = match metric {
                ConnectorMetric::ShipUploaded { outcome, bytes, .. } => Seen::Uploaded {
                    outcome: outcome.to_owned(),
                    bytes,
                },
                ConnectorMetric::ShipRetried { retry_age, .. } => Seen::Retried { retry_age },
                _ => return,
            };
            self.0.lock().unwrap_or_else(|e| e.into_inner()).push(seen);
        }
    }

    fn segment_id() -> SegmentId {
        SegmentId(
            "edge01_0000000000000001_00000000000000000001_00000000000000000002.parquet".into(),
        )
    }

    #[test]
    fn adapter_maps_outcomes_and_retries() {
        let recording = Arc::new(Recording(Mutex::new(Vec::new())));
        let metrics = RecorderShipMetrics::new("s3", recording.clone());
        let id = segment_id();

        metrics.uploaded(120, Duration::from_millis(8), "put");
        metrics.uploaded(90, Duration::from_millis(4), "multipart");
        metrics.uploaded(70, Duration::from_millis(3), "verified");
        metrics.retry(&id, 2, Duration::from_secs(5));
        metrics.conflict_verified(true);
        metrics.conflict_verified(false);

        let seen = recording.0.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(
            seen.as_slice(),
            &[
                Seen::Uploaded {
                    outcome: "ok".to_owned(),
                    bytes: 120
                },
                Seen::Uploaded {
                    outcome: "ok".to_owned(),
                    bytes: 90
                },
                Seen::Uploaded {
                    outcome: "conflict_equal".to_owned(),
                    bytes: 70
                },
                Seen::Retried {
                    retry_age: Duration::from_secs(5)
                },
                Seen::Uploaded {
                    outcome: "fatal".to_owned(),
                    bytes: 0
                },
            ],
            "put/multipart count ok, a verified conflict counts conflict_equal, a mismatch counts fatal"
        );
    }
}
