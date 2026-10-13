//! Prometheus metrics sink for exporting OpenMetrics format.
//!
//! This module provides a [`PrometheusMetrics`] sink that implements the
//! [`MetricsSink`](crate::MetricsSink) trait and exports metrics in the
//! OpenMetrics text format suitable for Prometheus scraping.

use std::sync::atomic::{AtomicU64, Ordering};

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use thadam_core::ConnectorMetric;

use crate::metrics::{MetricEvent, MetricsSink};

/// Label set for source metrics: the source only. Sink- and dataset-scoped
/// detail belongs to the chain metrics instead of empty labels on intake ones.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SourceLabels {
    /// The source identifier.
    pub source: String,
}

/// Label set for metrics with source and reason.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SourceReasonLabels {
    /// The source identifier.
    pub source: String,
    /// The reason for the event.
    pub reason: String,
}

/// Label set for metrics with sink only.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkLabels {
    /// The sink identifier.
    pub sink: String,
}

/// Label set for metrics with sink and domain.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkDomainLabels {
    /// The sink identifier.
    pub sink: String,
    /// The error domain.
    pub domain: String,
}

/// Label set for metrics with sink and kind.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkKindLabels {
    /// The sink identifier.
    pub sink: String,
    /// The kind of event.
    pub kind: String,
}

/// Label set for metrics with component only.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ComponentLabels {
    /// The component identifier.
    pub component: String,
}

/// Label set for metrics with component and state.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ComponentStateLabels {
    /// The component identifier.
    pub component: String,
    /// The state name.
    pub state: String,
}

/// Label set for sink/dataset/outcome metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkDatasetOutcomeLabels {
    /// The sink identifier.
    pub sink: String,
    /// The dataset identifier.
    pub dataset: String,
    /// The outcome of the event.
    pub outcome: String,
}

/// Label set for the shipper's per-outcome upload counts.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkOutcomeLabels {
    /// The shipper service's sink identifier.
    pub sink: String,
    /// The upload outcome (`ok`, `conflict_equal`, `retry`, `fatal`).
    pub outcome: String,
}

/// Label set for segment-catalog gauges.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkStateLabels {
    /// The segment sink identifier.
    pub sink: String,
    /// The segment state (`staging`, `finalized`, `eligible`, `uploaded`,
    /// `deleted`).
    pub state: String,
}

/// Label set for `thadam_build_info`, the one metric whose labels identify the
/// process: one series per process, so it is the only place a config hash or
/// node id appears as a label.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BuildInfoLabels {
    /// The thadam version.
    pub version: String,
    /// The config hash, hexadecimal.
    pub config_hash: String,
    /// The node identity (`pipeline.node_id`).
    pub node_id: String,
    /// The pipeline name.
    pub pipeline: String,
}

/// Label set for `thadam_quality_flags_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DatasetFlagLabels {
    /// The dataset identifier.
    pub dataset: String,
    /// The quality flag name.
    pub flag: String,
}

/// Label set for `thadam_sink_delivery_class`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkClassLabels {
    /// The sink identifier.
    pub sink: String,
    /// The effective delivery class (`at_least_once`, `idempotent_batch`,
    /// `at_most_once`).
    pub class: String,
}

/// Buckets for WAL group-commit and `fsync` durations: half a millisecond up
/// to a second.
fn wal_io_seconds() -> Histogram {
    Histogram::new([
        0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
    ])
}

/// Buckets for whole-file operations (uploads, finalizes): tens of
/// milliseconds up to a minute.
fn file_op_seconds() -> Histogram {
    Histogram::new([0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0])
}

/// Buckets for a retry's age at the attempt: seconds up to an hour (shipper
/// retries are unbounded).
fn retry_age_seconds() -> Histogram {
    Histogram::new([
        1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0,
    ])
}

/// A Prometheus metrics sink that implements the MetricsSink trait.
///
/// This sink collects metrics from the runtime and exposes them in OpenMetrics
/// text format via the [`encode`](PrometheusMetrics::encode) method.
#[derive(Debug)]
pub struct PrometheusMetrics {
    registry: Registry,
    // Counters
    source_messages_total: Family<SourceLabels, Counter>,
    source_bytes_total: Family<SourceLabels, Counter>,
    source_dropped_total: Family<SourceReasonLabels, Counter>,
    source_restart_total: Family<SourceLabels, Counter>,
    source_backpressure_seconds_total: Family<SourceLabels, Counter<f64, AtomicU64>>,
    source_stale_acks_total: Family<SourceLabels, Counter>,
    chain_events_total: Family<SinkDatasetOutcomeLabels, Counter>,
    rejects_total: Family<SinkDomainLabels, Counter>,
    quality_flags_total: Family<DatasetFlagLabels, Counter>,
    wal_gap_total: Family<SinkKindLabels, Counter>,
    chain_panic_total: Family<SinkLabels, Counter>,
    chain_unmatched_total: Family<SinkLabels, Counter>,
    cursor_committed_total: Family<SinkLabels, Counter>,
    batch_written_total: Family<SinkLabels, Counter>,
    batch_written_events_total: Family<SinkLabels, Counter>,
    retry_total: Family<ComponentLabels, Counter>,
    bisect_total: Family<SinkLabels, Counter>,
    quarantined_total: Family<SinkLabels, Counter>,
    rejects_dropped_total: Family<SinkLabels, Counter>,
    wal_maintained_deleted_bytes_total: Counter,
    wal_maintained_evicted_bytes_total: Counter,
    ship_bytes_total: Family<SinkLabels, Counter>,
    ship_uploads_total: Family<SinkOutcomeLabels, Counter>,
    log_dropped_total: Counter,
    synced_log_drops: AtomicU64,
    // Gauges
    component_state: Family<ComponentStateLabels, Gauge>,
    clock_synced: Gauge,
    build_info: Family<BuildInfoLabels, Gauge>,
    segment_files: Family<SinkStateLabels, Gauge>,
    segment_bytes: Family<SinkStateLabels, Gauge>,
    wal_disk_bytes: Gauge,
    wal_budget_bytes: Gauge,
    recovery_seconds: Gauge<f64, AtomicU64>,
    recovery_truncated_bytes: Gauge,
    source_connected: Family<SourceLabels, Gauge>,
    ack_oldest_age_seconds: Family<SourceLabels, Gauge<f64, AtomicU64>>,
    source_unacked_max: Family<SourceLabels, Gauge>,
    sink_lag_bytes: Family<SinkLabels, Gauge>,
    sink_lag_seconds: Family<SinkLabels, Gauge<f64, AtomicU64>>,
    sink_delivery_class: Family<SinkClassLabels, Gauge>,
    // Histograms
    source_ack_latency_seconds: Family<SourceLabels, Histogram, fn() -> Histogram>,
    batch_write_latency_seconds: Family<SinkLabels, Histogram, fn() -> Histogram>,
    ship_upload_seconds: Family<SinkLabels, Histogram, fn() -> Histogram>,
    ship_retry_age_seconds: Family<SinkLabels, Histogram, fn() -> Histogram>,
    segment_finalize_seconds: Family<SinkLabels, Histogram, fn() -> Histogram>,
    wal_commit_seconds: Histogram,
    wal_fsync_seconds: Histogram,
}

impl PrometheusMetrics {
    /// Creates a new Prometheus metrics sink with all metrics registered.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::default();

        // Counters register without the `_total` suffix: the text encoder
        // appends it once, so a suffixed registration would encode twice.
        let source_messages_total = Family::default();
        registry.register(
            "thadam_source_messages",
            "Total number of messages received from sources",
            source_messages_total.clone(),
        );

        let source_bytes_total = Family::default();
        registry.register(
            "thadam_source_bytes",
            "Total bytes received from sources",
            source_bytes_total.clone(),
        );

        let source_dropped_total = Family::default();
        registry.register(
            "thadam_source_dropped",
            "Total messages dropped by source",
            source_dropped_total.clone(),
        );

        let source_restart_total = Family::default();
        registry.register(
            "thadam_source_restart",
            "Total number of source restarts",
            source_restart_total.clone(),
        );

        let source_backpressure_seconds_total = Family::default();
        registry.register(
            "thadam_source_backpressure_seconds",
            "Total seconds a source spent backpressured by the WAL or ack window",
            source_backpressure_seconds_total.clone(),
        );

        let source_stale_acks_total = Family::default();
        registry.register(
            "thadam_source_stale_acks",
            "Total acknowledgements dropped because they belonged to an older connection epoch",
            source_stale_acks_total.clone(),
        );

        let chain_events_total = Family::default();
        registry.register(
            "thadam_chain_events",
            "Total events produced by chains",
            chain_events_total.clone(),
        );

        let rejects_total = Family::default();
        registry.register(
            "thadam_rejects",
            "Total records or events rejected",
            rejects_total.clone(),
        );

        let quality_flags_total = Family::default();
        registry.register(
            "thadam_quality_flags",
            "Total quality flags raised on emitted events",
            quality_flags_total.clone(),
        );

        let wal_gap_total = Family::default();
        registry.register(
            "thadam_wal_gap",
            "Total WAL gaps seen by readers",
            wal_gap_total.clone(),
        );

        let chain_panic_total = Family::default();
        registry.register(
            "thadam_chain_panic",
            "Total chain panics",
            chain_panic_total.clone(),
        );

        let chain_unmatched_total = Family::default();
        registry.register(
            "thadam_chain_unmatched",
            "Total records no dataset matched (not rejects)",
            chain_unmatched_total.clone(),
        );

        let cursor_committed_total = Family::default();
        registry.register(
            "thadam_cursor_committed",
            "Total cursor commits",
            cursor_committed_total.clone(),
        );

        let batch_written_total = Family::default();
        registry.register(
            "thadam_batch_written",
            "Total batch writes",
            batch_written_total.clone(),
        );

        let batch_written_events_total = Family::default();
        registry.register(
            "thadam_batch_written_events",
            "Total events written in batches",
            batch_written_events_total.clone(),
        );

        let retry_total = Family::default();
        registry.register("thadam_retry", "Total retry attempts", retry_total.clone());

        let bisect_total = Family::default();
        registry.register(
            "thadam_batch_bisect",
            "Total batch bisects",
            bisect_total.clone(),
        );

        let quarantined_total = Family::default();
        registry.register(
            "thadam_quarantined",
            "Total records quarantined",
            quarantined_total.clone(),
        );

        let rejects_dropped_total = Family::default();
        registry.register(
            "thadam_rejects_dropped",
            "Total rejects dropped",
            rejects_dropped_total.clone(),
        );

        let wal_maintained_deleted_bytes_total = Counter::default();
        registry.register(
            "thadam_wal_maintained_deleted_bytes",
            "Total bytes deleted by WAL maintenance",
            wal_maintained_deleted_bytes_total.clone(),
        );

        let wal_maintained_evicted_bytes_total = Counter::default();
        registry.register(
            "thadam_wal_evicted_bytes",
            "Total bytes evicted by WAL maintenance",
            wal_maintained_evicted_bytes_total.clone(),
        );

        let ship_bytes_total = Family::default();
        registry.register(
            "thadam_ship_bytes",
            "Total segment bytes settled in the object store",
            ship_bytes_total.clone(),
        );

        let ship_uploads_total = Family::default();
        registry.register(
            "thadam_ship_uploads",
            "Total shipper uploads by outcome",
            ship_uploads_total.clone(),
        );

        let log_dropped_total = Counter::default();
        registry.register(
            "thadam_log_dropped",
            "Total log lines dropped by the lossy logging writer",
            log_dropped_total.clone(),
        );

        // Gauges
        let component_state = Family::default();
        registry.register(
            "thadam_component_state",
            "Current state of components",
            component_state.clone(),
        );

        let clock_synced = Gauge::default();
        registry.register(
            "thadam_clock_synced",
            "Whether the clock is synchronized (1 or 0)",
            clock_synced.clone(),
        );

        let build_info = Family::default();
        registry.register(
            "thadam_build_info",
            "Build and configuration identity of this process; always 1",
            build_info.clone(),
        );

        let segment_files = Family::default();
        registry.register(
            "thadam_segment_files",
            "Segment records per state in the catalog",
            segment_files.clone(),
        );

        let segment_bytes = Family::default();
        registry.register(
            "thadam_segment_bytes",
            "Recorded segment bytes per state in the catalog",
            segment_bytes.clone(),
        );

        let wal_disk_bytes = Gauge::default();
        registry.register(
            "thadam_wal_disk_bytes",
            "Current bytes held by retained WAL segments",
            wal_disk_bytes.clone(),
        );

        let wal_budget_bytes = Gauge::default();
        registry.register(
            "thadam_wal_budget_bytes",
            "Configured WAL disk budget in bytes",
            wal_budget_bytes.clone(),
        );

        let recovery_seconds = Gauge::default();
        registry.register(
            "thadam_recovery_seconds",
            "Duration of startup WAL recovery in seconds",
            recovery_seconds.clone(),
        );

        let recovery_truncated_bytes = Gauge::default();
        registry.register(
            "thadam_recovery_truncated_bytes",
            "Bytes truncated from the last WAL segment during startup recovery",
            recovery_truncated_bytes.clone(),
        );

        let source_connected = Family::default();
        registry.register(
            "thadam_source_connected",
            "Whether the source is currently connected (1 or 0)",
            source_connected.clone(),
        );

        let ack_oldest_age_seconds = Family::default();
        registry.register(
            "thadam_ack_oldest_age_seconds",
            "Age in seconds of the oldest unacknowledged batch for a source",
            ack_oldest_age_seconds.clone(),
        );

        let source_unacked_max = Family::default();
        registry.register(
            "thadam_source_unacked_max",
            "Observed maximum unacknowledged batch count for a source",
            source_unacked_max.clone(),
        );

        let sink_lag_bytes = Family::default();
        registry.register(
            "thadam_sink_lag_bytes",
            "Unconsumed WAL segment bytes after a sink's committed cursor",
            sink_lag_bytes.clone(),
        );

        let sink_lag_seconds = Family::default();
        registry.register(
            "thadam_sink_lag_seconds",
            "Age in seconds of the oldest unconsumed WAL segment for a sink",
            sink_lag_seconds.clone(),
        );

        let sink_delivery_class = Family::default();
        registry.register(
            "thadam_sink_delivery_class",
            "Effective delivery class of a sink; 1 for the active class",
            sink_delivery_class.clone(),
        );

        // Histograms
        let source_ack_latency_seconds = Family::new_with_constructor(
            (|| {
                Histogram::new([
                    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                ])
            }) as fn() -> Histogram,
        );
        registry.register(
            "thadam_source_ack_latency_seconds",
            "Latency from submission to acknowledgement",
            source_ack_latency_seconds.clone(),
        );

        let batch_write_latency_seconds = Family::new_with_constructor(
            (|| {
                Histogram::new([
                    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                ])
            }) as fn() -> Histogram,
        );
        registry.register(
            "thadam_batch_write_seconds",
            "Time spent in batch sink writes",
            batch_write_latency_seconds.clone(),
        );

        let ship_upload_seconds =
            Family::new_with_constructor(file_op_seconds as fn() -> Histogram);
        registry.register(
            "thadam_ship_upload_seconds",
            "Time to upload one segment, verification included",
            ship_upload_seconds.clone(),
        );

        let ship_retry_age_seconds =
            Family::new_with_constructor(retry_age_seconds as fn() -> Histogram);
        registry.register(
            "thadam_ship_retry_age_seconds",
            "Age of the first failure when an upload is retried",
            ship_retry_age_seconds.clone(),
        );

        let segment_finalize_seconds =
            Family::new_with_constructor(file_op_seconds as fn() -> Histogram);
        registry.register(
            "thadam_segment_finalize_seconds",
            "Time to finalize one segment file",
            segment_finalize_seconds.clone(),
        );

        let wal_commit_seconds = wal_io_seconds();
        registry.register(
            "thadam_wal_commit_seconds",
            "Duration of each WAL group-commit write",
            wal_commit_seconds.clone(),
        );

        let wal_fsync_seconds = wal_io_seconds();
        registry.register(
            "thadam_wal_fsync_seconds",
            "Duration of each WAL segment fsync",
            wal_fsync_seconds.clone(),
        );

        Self {
            registry,
            source_messages_total,
            source_bytes_total,
            source_dropped_total,
            source_restart_total,
            source_backpressure_seconds_total,
            source_stale_acks_total,
            chain_events_total,
            rejects_total,
            quality_flags_total,
            wal_gap_total,
            chain_panic_total,
            chain_unmatched_total,
            cursor_committed_total,
            batch_written_total,
            batch_written_events_total,
            retry_total,
            bisect_total,
            quarantined_total,
            rejects_dropped_total,
            wal_maintained_deleted_bytes_total,
            wal_maintained_evicted_bytes_total,
            ship_bytes_total,
            ship_uploads_total,
            log_dropped_total,
            synced_log_drops: AtomicU64::new(crate::logging::dropped_logs()),
            component_state,
            clock_synced,
            build_info,
            segment_files,
            segment_bytes,
            wal_disk_bytes,
            wal_budget_bytes,
            recovery_seconds,
            recovery_truncated_bytes,
            source_connected,
            ack_oldest_age_seconds,
            source_unacked_max,
            sink_lag_bytes,
            sink_lag_seconds,
            sink_delivery_class,
            source_ack_latency_seconds,
            batch_write_latency_seconds,
            ship_upload_seconds,
            ship_retry_age_seconds,
            segment_finalize_seconds,
            wal_commit_seconds,
            wal_fsync_seconds,
        }
    }

    /// Encodes all metrics into OpenMetrics text format.
    #[must_use]
    pub fn encode(&self) -> String {
        let total_dropped = crate::logging::dropped_logs();
        let prev = self.synced_log_drops.swap(total_dropped, Ordering::Relaxed);
        if total_dropped > prev {
            self.log_dropped_total.inc_by(total_dropped - prev);
        }
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry)
            .unwrap_or_else(|e| panic!("encoding to string should not fail: {e}"));
        buffer
    }

    /// Returns a sorted list of the exported metric names: the registered
    /// names plus the `_total` the encoder adds to counters.
    #[must_use]
    pub fn metric_names(&self) -> Vec<&'static str> {
        let mut names = vec![
            "thadam_source_messages_total",
            "thadam_source_bytes_total",
            "thadam_source_dropped_total",
            "thadam_source_restart_total",
            "thadam_source_backpressure_seconds_total",
            "thadam_source_stale_acks_total",
            "thadam_chain_events_total",
            "thadam_rejects_total",
            "thadam_quality_flags_total",
            "thadam_wal_gap_total",
            "thadam_chain_panic_total",
            "thadam_chain_unmatched_total",
            "thadam_cursor_committed_total",
            "thadam_batch_written_total",
            "thadam_batch_written_events_total",
            "thadam_retry_total",
            "thadam_batch_bisect_total",
            "thadam_quarantined_total",
            "thadam_rejects_dropped_total",
            "thadam_wal_maintained_deleted_bytes_total",
            "thadam_wal_evicted_bytes_total",
            "thadam_ship_bytes_total",
            "thadam_ship_uploads_total",
            "thadam_log_dropped_total",
            "thadam_component_state",
            "thadam_clock_synced",
            "thadam_build_info",
            "thadam_segment_files",
            "thadam_segment_bytes",
            "thadam_wal_disk_bytes",
            "thadam_wal_budget_bytes",
            "thadam_recovery_seconds",
            "thadam_recovery_truncated_bytes",
            "thadam_source_connected",
            "thadam_ack_oldest_age_seconds",
            "thadam_source_unacked_max",
            "thadam_sink_lag_bytes",
            "thadam_sink_lag_seconds",
            "thadam_sink_delivery_class",
            "thadam_source_ack_latency_seconds",
            "thadam_batch_write_seconds",
            "thadam_ship_upload_seconds",
            "thadam_ship_retry_age_seconds",
            "thadam_segment_finalize_seconds",
            "thadam_wal_commit_seconds",
            "thadam_wal_fsync_seconds",
        ];
        names.sort_unstable();
        names
    }
}

impl Default for PrometheusMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsSink for PrometheusMetrics {
    fn record(&self, event: MetricEvent<'_>) {
        match event {
            MetricEvent::SourceBatch {
                source,
                messages,
                bytes,
            } => {
                let labels = SourceLabels {
                    source: source.to_string(),
                };
                self.source_messages_total
                    .get_or_create(&labels)
                    .inc_by(messages as u64);
                self.source_bytes_total
                    .get_or_create(&labels)
                    .inc_by(bytes as u64);
            }
            MetricEvent::SourceAcked { source, latency } => {
                let labels = SourceLabels {
                    source: source.to_string(),
                };
                self.source_ack_latency_seconds
                    .get_or_create(&labels)
                    .observe(latency.as_secs_f64());
            }
            MetricEvent::SourceDropped {
                source,
                reason,
                count,
            } => {
                let labels = SourceReasonLabels {
                    source: source.to_string(),
                    reason: reason.to_string(),
                };
                self.source_dropped_total
                    .get_or_create(&labels)
                    .inc_by(count as u64);
            }
            MetricEvent::SourceRestart { source } => {
                let labels = SourceLabels {
                    source: source.to_string(),
                };
                self.source_restart_total.get_or_create(&labels).inc();
            }
            MetricEvent::ChainEvents {
                sink,
                dataset,
                outcome,
                events,
            } => {
                let labels = SinkDatasetOutcomeLabels {
                    sink: sink.to_string(),
                    dataset: dataset.to_string(),
                    outcome: outcome.to_string(),
                };
                self.chain_events_total
                    .get_or_create(&labels)
                    .inc_by(events as u64);
            }
            MetricEvent::Rejected {
                sink,
                domain,
                count,
            } => {
                let labels = SinkDomainLabels {
                    sink: sink.to_string(),
                    domain: domain.to_string(),
                };
                self.rejects_total
                    .get_or_create(&labels)
                    .inc_by(count as u64);
            }
            MetricEvent::WalGap { sink, kind, frames } => {
                let labels = SinkKindLabels {
                    sink: sink.to_string(),
                    kind: kind.to_string(),
                };
                self.wal_gap_total.get_or_create(&labels).inc_by(frames);
            }
            MetricEvent::ChainPanic { sink } => {
                let labels = SinkLabels {
                    sink: sink.to_string(),
                };
                self.chain_panic_total.get_or_create(&labels).inc();
            }
            MetricEvent::ChainUnmatched { sink, messages } => {
                let labels = SinkLabels {
                    sink: sink.to_string(),
                };
                self.chain_unmatched_total
                    .get_or_create(&labels)
                    .inc_by(messages as u64);
            }
            MetricEvent::CursorCommitted { sink, .. } => {
                let labels = SinkLabels {
                    sink: sink.to_string(),
                };
                self.cursor_committed_total.get_or_create(&labels).inc();
            }
            MetricEvent::BatchWritten {
                sink,
                events,
                latency,
            } => {
                let labels = SinkLabels {
                    sink: sink.to_string(),
                };
                self.batch_written_total.get_or_create(&labels).inc();
                self.batch_written_events_total
                    .get_or_create(&labels)
                    .inc_by(events as u64);
                self.batch_write_latency_seconds
                    .get_or_create(&labels)
                    .observe(latency.as_secs_f64());
            }
            MetricEvent::Retry { component } => {
                let labels = ComponentLabels {
                    component: component.to_string(),
                };
                self.retry_total.get_or_create(&labels).inc();
            }
            MetricEvent::Bisect { sink } => {
                let labels = SinkLabels {
                    sink: sink.to_string(),
                };
                self.bisect_total.get_or_create(&labels).inc();
            }
            MetricEvent::Quarantined { sink, count } => {
                let labels = SinkLabels {
                    sink: sink.to_string(),
                };
                self.quarantined_total
                    .get_or_create(&labels)
                    .inc_by(count as u64);
            }
            MetricEvent::RejectsDropped { sink, count } => {
                let labels = SinkLabels {
                    sink: sink.to_string(),
                };
                self.rejects_dropped_total
                    .get_or_create(&labels)
                    .inc_by(count as u64);
            }
            MetricEvent::ComponentState { component, state } => {
                let labels = ComponentStateLabels {
                    component: component.to_string(),
                    state: state.to_string(),
                };
                self.component_state.get_or_create(&labels).set(1);
            }
            MetricEvent::WalMaintained {
                deleted_bytes,
                evicted_bytes,
            } => {
                self.wal_maintained_deleted_bytes_total
                    .inc_by(deleted_bytes);
                self.wal_maintained_evicted_bytes_total
                    .inc_by(evicted_bytes);
            }
            MetricEvent::ClockSynced { synced } => {
                self.clock_synced.set(if synced { 1 } else { 0 });
            }
            MetricEvent::BuildInfo {
                version,
                config_hash,
                node_id,
                pipeline,
            } => {
                self.build_info
                    .get_or_create(&BuildInfoLabels {
                        version: version.to_string(),
                        config_hash: config_hash.to_string(),
                        node_id: node_id.to_string(),
                        pipeline: pipeline.to_string(),
                    })
                    .set(1);
            }
            MetricEvent::WalCommit { duration } => {
                self.wal_commit_seconds.observe(duration.as_secs_f64());
            }
            MetricEvent::WalFsync { duration } => {
                self.wal_fsync_seconds.observe(duration.as_secs_f64());
            }
            MetricEvent::WalDiskBytes { bytes } => {
                self.wal_disk_bytes
                    .set(i64::try_from(bytes).unwrap_or(i64::MAX));
            }
            MetricEvent::WalBudgetBytes { bytes } => {
                self.wal_budget_bytes
                    .set(i64::try_from(bytes).unwrap_or(i64::MAX));
            }
            MetricEvent::WalRecovery {
                elapsed,
                truncated_bytes,
            } => {
                self.recovery_seconds.set(elapsed.as_secs_f64());
                self.recovery_truncated_bytes
                    .set(i64::try_from(truncated_bytes).unwrap_or(i64::MAX));
            }
            MetricEvent::SourceConnected { source, connected } => {
                self.source_connected
                    .get_or_create(&SourceLabels {
                        source: source.to_string(),
                    })
                    .set(if connected { 1 } else { 0 });
            }
            MetricEvent::SourceBackpressure { source, duration } => {
                self.source_backpressure_seconds_total
                    .get_or_create(&SourceLabels {
                        source: source.to_string(),
                    })
                    .inc_by(duration.as_secs_f64());
            }
            MetricEvent::AckOldestAge { source, age } => {
                self.ack_oldest_age_seconds
                    .get_or_create(&SourceLabels {
                        source: source.to_string(),
                    })
                    .set(age.as_secs_f64());
            }
            MetricEvent::SourceUnackedMax { source, unacked } => {
                let g = self.source_unacked_max.get_or_create(&SourceLabels {
                    source: source.to_string(),
                });
                let v = i64::try_from(unacked).unwrap_or(i64::MAX);
                if v > g.get() {
                    g.set(v);
                }
            }
            MetricEvent::QualityFlags {
                dataset,
                flag,
                count,
            } => {
                self.quality_flags_total
                    .get_or_create(&DatasetFlagLabels {
                        dataset: dataset.to_string(),
                        flag: flag.to_string(),
                    })
                    .inc_by(count as u64);
            }
            MetricEvent::SinkLag {
                sink,
                bytes,
                seconds,
            } => {
                let labels = SinkLabels {
                    sink: sink.to_string(),
                };
                self.sink_lag_bytes
                    .get_or_create(&labels)
                    .set(i64::try_from(bytes).unwrap_or(i64::MAX));
                self.sink_lag_seconds.get_or_create(&labels).set(seconds);
            }
            MetricEvent::SinkDeliveryClass { sink, class } => {
                self.sink_delivery_class
                    .get_or_create(&SinkClassLabels {
                        sink: sink.to_string(),
                        class: class.to_string(),
                    })
                    .set(1);
            }
            MetricEvent::LogDropped { count } => {
                self.log_dropped_total.inc_by(count);
            }
            MetricEvent::Connector(metric) => self.record_connector(metric),
        }
    }
}

impl PrometheusMetrics {
    fn record_connector(&self, metric: ConnectorMetric<'_>) {
        match metric {
            ConnectorMetric::ShipUploaded {
                sink,
                outcome,
                bytes,
                latency,
            } => {
                let sink_only = SinkLabels {
                    sink: sink.to_string(),
                };
                self.ship_uploads_total
                    .get_or_create(&SinkOutcomeLabels {
                        sink: sink.to_string(),
                        outcome: outcome.to_string(),
                    })
                    .inc();
                self.ship_bytes_total
                    .get_or_create(&sink_only)
                    .inc_by(bytes);
                if let Some(latency) = latency {
                    self.ship_upload_seconds
                        .get_or_create(&sink_only)
                        .observe(latency.as_secs_f64());
                }
            }
            ConnectorMetric::ShipRetried { sink, retry_age } => {
                let sink_only = SinkLabels {
                    sink: sink.to_string(),
                };
                self.ship_uploads_total
                    .get_or_create(&SinkOutcomeLabels {
                        sink: sink.to_string(),
                        outcome: "retry".to_string(),
                    })
                    .inc();
                self.ship_retry_age_seconds
                    .get_or_create(&sink_only)
                    .observe(retry_age.as_secs_f64());
            }
            ConnectorMetric::SegmentFiles {
                sink,
                state,
                files,
                bytes,
            } => {
                let labels = SinkStateLabels {
                    sink: sink.to_string(),
                    state: state.to_string(),
                };
                self.segment_files
                    .get_or_create(&labels)
                    .set(i64::try_from(files).unwrap_or(i64::MAX));
                self.segment_bytes
                    .get_or_create(&labels)
                    .set(i64::try_from(bytes).unwrap_or(i64::MAX));
            }
            ConnectorMetric::SegmentFinalized { sink, latency, .. } => {
                self.segment_finalize_seconds
                    .get_or_create(&SinkLabels {
                        sink: sink.to_string(),
                    })
                    .observe(latency.as_secs_f64());
            }
            ConnectorMetric::SourceStaleAcks { source, count } => {
                self.source_stale_acks_total
                    .get_or_create(&SourceLabels {
                        source: source.to_string(),
                    })
                    .inc_by(count);
            }
            // The core enum is open; new connector metrics land here until
            // they get a Prometheus mapping.
            _ => {}
        }
    }
}
