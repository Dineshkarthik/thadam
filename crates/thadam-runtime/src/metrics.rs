//! The hook metrics exporters attach to.
//!
//! The runtime reports what happened as [`MetricEvent`]s and never depends on
//! a metrics library; PR11 maps these onto the consolidated names in
//! ARCHITECTURE.md section 14. Gauges (WAL disk bytes, budgets, lag, component
//! states) are read from [`PipelineHandle::status`](crate::PipelineHandle::status)
//! instead. Labels are only source, sink, dataset, and bounded enums.

use std::time::Duration;

use thadam_core::WalPos;

/// One thing the runtime did. Borrowed so reporting allocates nothing.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum MetricEvent<'a> {
    /// A source batch was submitted to the WAL.
    SourceBatch {
        /// The source id.
        source: &'a str,
        /// Messages in the batch.
        messages: usize,
        /// Payload bytes in the batch.
        bytes: usize,
    },
    /// A source batch was committed and acknowledged, in order.
    SourceAcked {
        /// The source id.
        source: &'a str,
        /// From submission to acknowledgement.
        latency: Duration,
    },
    /// Messages a source received but the runtime could not store
    /// (`thadam_source_dropped_total{reason}`).
    SourceDropped {
        /// The source id.
        source: &'a str,
        /// Why, such as `too_large`.
        reason: &'static str,
        /// How many.
        count: usize,
    },
    /// A source was reopened after a transient error.
    SourceRestart {
        /// The source id.
        source: &'a str,
    },
    /// A chain produced events for one dataset.
    ChainEvents {
        /// The sink whose chain it is.
        sink: &'a str,
        /// The dataset.
        dataset: &'a str,
        /// What became of the events: `written`, `filtered`, `deduped`,
        /// `rejected`, or `exploded`. A compile-time string, so the label
        /// stays bounded.
        outcome: &'static str,
        /// Events with that outcome.
        events: usize,
    },
    /// Records or events were rejected.
    Rejected {
        /// The sink whose chain or connector rejected them.
        sink: &'a str,
        /// The error domain.
        domain: &'a str,
        /// How many.
        count: usize,
    },
    /// A reader received a gap (`corrupt`, `evicted`, or `missing`).
    WalGap {
        /// The sink whose reader saw it.
        sink: &'a str,
        /// The gap kind.
        kind: &'static str,
        /// Positions covered.
        frames: u64,
    },
    /// A chain panicked and was rebuilt.
    ChainPanic {
        /// The sink whose chain it is.
        sink: &'a str,
    },
    /// A sink's cursor was durably committed.
    CursorCommitted {
        /// The sink id.
        sink: &'a str,
        /// The committed position.
        pos: WalPos,
    },
    /// A batch-sink write succeeded.
    BatchWritten {
        /// The sink id.
        sink: &'a str,
        /// Events in the batch.
        events: usize,
        /// Time spent in `write`, retries excluded.
        latency: Duration,
    },
    /// A sink operation is being retried.
    Retry {
        /// The component id.
        component: &'a str,
    },
    /// A batch was bisected to isolate poison events.
    Bisect {
        /// The sink id.
        sink: &'a str,
    },
    /// Records or events were written to quarantine.
    Quarantined {
        /// The sink whose quarantine received them.
        sink: &'a str,
        /// How many.
        count: usize,
    },
    /// Rejects were dropped because no quarantine sink is registered, or
    /// because a non-primary chain saw them (ARCHITECTURE.md O3).
    RejectsDropped {
        /// The sink id.
        sink: &'a str,
        /// How many.
        count: usize,
    },
    /// A component changed state.
    ComponentState {
        /// The component id.
        component: &'a str,
        /// The new state's name.
        state: &'static str,
    },
    /// One retention pass ran.
    WalMaintained {
        /// Bytes deleted by retention.
        deleted_bytes: u64,
        /// Bytes evicted under `drop_oldest`.
        evicted_bytes: u64,
    },
    /// The clock-sync state was read.
    ClockSynced {
        /// Whether the clock is synchronized.
        synced: bool,
    },
}

/// Receives [`MetricEvent`]s. Implementations must be cheap and must not
/// block: they are called on the hot path.
pub trait MetricsSink: Send + Sync + 'static {
    /// Records one event.
    fn record(&self, event: MetricEvent<'_>);
}

/// Discards everything.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopMetrics;

impl MetricsSink for NoopMetrics {
    fn record(&self, _event: MetricEvent<'_>) {}
}
