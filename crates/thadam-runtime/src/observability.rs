//! Prometheus metrics sink for exporting OpenMetrics format.
//!
//! This module provides a [`PrometheusMetrics`] sink that implements the
//! [`MetricsSink`](crate::MetricsSink) trait and exports metrics in the
//! OpenMetrics text format suitable for Prometheus scraping.

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

use crate::metrics::{MetricEvent, MetricsSink};

/// Label set for metrics that need source, sink, and dataset labels.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MetricLabels {
    /// The source identifier.
    pub source: String,
    /// The sink identifier.
    pub sink: String,
    /// The dataset identifier.
    pub dataset: String,
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

/// A Prometheus metrics sink that implements the MetricsSink trait.
///
/// This sink collects metrics from the runtime and exposes them in OpenMetrics
/// text format via the [`encode`](PrometheusMetrics::encode) method.
#[derive(Debug)]
pub struct PrometheusMetrics {
    registry: Registry,
    // Counters
    source_messages_total: Family<MetricLabels, Counter>,
    source_bytes_total: Family<MetricLabels, Counter>,
    source_dropped_total: Family<SourceReasonLabels, Counter>,
    source_restart_total: Family<MetricLabels, Counter>,
    chain_events_total: Family<SinkDatasetOutcomeLabels, Counter>,
    rejects_total: Family<SinkDomainLabels, Counter>,
    wal_gap_total: Family<SinkKindLabels, Counter>,
    chain_panic_total: Family<SinkLabels, Counter>,
    cursor_committed_total: Family<SinkLabels, Counter>,
    batch_written_total: Family<SinkLabels, Counter>,
    batch_written_events_total: Family<SinkLabels, Counter>,
    retry_total: Family<ComponentLabels, Counter>,
    bisect_total: Family<SinkLabels, Counter>,
    quarantined_total: Family<SinkLabels, Counter>,
    rejects_dropped_total: Family<SinkLabels, Counter>,
    wal_maintained_deleted_bytes_total: Counter,
    wal_maintained_evicted_bytes_total: Counter,
    // Gauges
    component_state: Family<ComponentStateLabels, Gauge>,
    clock_synced: Gauge,
    // Histograms
    source_ack_latency_seconds: Family<MetricLabels, Histogram, fn() -> Histogram>,
    batch_write_latency_seconds: Family<SinkLabels, Histogram, fn() -> Histogram>,
}

impl PrometheusMetrics {
    /// Creates a new Prometheus metrics sink with all metrics registered.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::default();

        // Counters
        let source_messages_total = Family::default();
        registry.register(
            "thadam_source_messages_total",
            "Total number of messages received from sources",
            source_messages_total.clone(),
        );

        let source_bytes_total = Family::default();
        registry.register(
            "thadam_source_bytes_total",
            "Total bytes received from sources",
            source_bytes_total.clone(),
        );

        let source_dropped_total = Family::default();
        registry.register(
            "thadam_source_dropped_total",
            "Total messages dropped by source",
            source_dropped_total.clone(),
        );

        let source_restart_total = Family::default();
        registry.register(
            "thadam_source_restart_total",
            "Total number of source restarts",
            source_restart_total.clone(),
        );

        let chain_events_total = Family::default();
        registry.register(
            "thadam_chain_events_total",
            "Total events produced by chains",
            chain_events_total.clone(),
        );

        let rejects_total = Family::default();
        registry.register(
            "thadam_rejects_total",
            "Total records or events rejected",
            rejects_total.clone(),
        );

        let wal_gap_total = Family::default();
        registry.register(
            "thadam_wal_gap_total",
            "Total WAL gaps seen by readers",
            wal_gap_total.clone(),
        );

        let chain_panic_total = Family::default();
        registry.register(
            "thadam_chain_panic_total",
            "Total chain panics",
            chain_panic_total.clone(),
        );

        let cursor_committed_total = Family::default();
        registry.register(
            "thadam_cursor_committed_total",
            "Total cursor commits",
            cursor_committed_total.clone(),
        );

        let batch_written_total = Family::default();
        registry.register(
            "thadam_batch_written_total",
            "Total batch writes",
            batch_written_total.clone(),
        );

        let batch_written_events_total = Family::default();
        registry.register(
            "thadam_batch_written_events_total",
            "Total events written in batches",
            batch_written_events_total.clone(),
        );

        let retry_total = Family::default();
        registry.register(
            "thadam_retry_total",
            "Total retry attempts",
            retry_total.clone(),
        );

        let bisect_total = Family::default();
        registry.register(
            "thadam_batch_bisect_total",
            "Total batch bisects",
            bisect_total.clone(),
        );

        let quarantined_total = Family::default();
        registry.register(
            "thadam_quarantined_total",
            "Total records quarantined",
            quarantined_total.clone(),
        );

        let rejects_dropped_total = Family::default();
        registry.register(
            "thadam_rejects_dropped_total",
            "Total rejects dropped",
            rejects_dropped_total.clone(),
        );

        let wal_maintained_deleted_bytes_total = Counter::default();
        registry.register(
            "thadam_wal_maintained_deleted_bytes_total",
            "Total bytes deleted by WAL maintenance",
            wal_maintained_deleted_bytes_total.clone(),
        );

        let wal_maintained_evicted_bytes_total = Counter::default();
        registry.register(
            "thadam_wal_evicted_bytes_total",
            "Total bytes evicted by WAL maintenance",
            wal_maintained_evicted_bytes_total.clone(),
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

        Self {
            registry,
            source_messages_total,
            source_bytes_total,
            source_dropped_total,
            source_restart_total,
            chain_events_total,
            rejects_total,
            wal_gap_total,
            chain_panic_total,
            cursor_committed_total,
            batch_written_total,
            batch_written_events_total,
            retry_total,
            bisect_total,
            quarantined_total,
            rejects_dropped_total,
            wal_maintained_deleted_bytes_total,
            wal_maintained_evicted_bytes_total,
            component_state,
            clock_synced,
            source_ack_latency_seconds,
            batch_write_latency_seconds,
        }
    }

    /// Encodes all metrics into OpenMetrics text format.
    #[must_use]
    pub fn encode(&self) -> String {
        let mut buffer = String::new();
        encode(&mut buffer, &self.registry)
            .unwrap_or_else(|e| panic!("encoding to string should not fail: {e}"));
        buffer
    }

    /// Returns a sorted list of all registered metric names.
    #[must_use]
    pub fn metric_names(&self) -> Vec<&'static str> {
        let mut names = vec![
            "thadam_source_messages_total",
            "thadam_source_bytes_total",
            "thadam_source_dropped_total",
            "thadam_source_restart_total",
            "thadam_chain_events_total",
            "thadam_rejects_total",
            "thadam_wal_gap_total",
            "thadam_chain_panic_total",
            "thadam_cursor_committed_total",
            "thadam_batch_written_total",
            "thadam_batch_written_events_total",
            "thadam_retry_total",
            "thadam_batch_bisect_total",
            "thadam_quarantined_total",
            "thadam_rejects_dropped_total",
            "thadam_wal_maintained_deleted_bytes_total",
            "thadam_wal_evicted_bytes_total",
            "thadam_component_state",
            "thadam_clock_synced",
            "thadam_source_ack_latency_seconds",
            "thadam_batch_write_seconds",
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
                let labels = MetricLabels {
                    source: source.to_string(),
                    sink: String::new(),
                    dataset: String::new(),
                };
                self.source_messages_total
                    .get_or_create(&labels)
                    .inc_by(messages as u64);
                self.source_bytes_total
                    .get_or_create(&labels)
                    .inc_by(bytes as u64);
            }
            MetricEvent::SourceAcked { source, latency } => {
                let labels = MetricLabels {
                    source: source.to_string(),
                    sink: String::new(),
                    dataset: String::new(),
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
                let labels = MetricLabels {
                    source: source.to_string(),
                    sink: String::new(),
                    dataset: String::new(),
                };
                self.source_restart_total.get_or_create(&labels).inc();
            }
            MetricEvent::ChainEvents {
                sink,
                dataset,
                events,
            } => {
                let labels = SinkDatasetOutcomeLabels {
                    sink: sink.to_string(),
                    dataset: dataset.to_string(),
                    outcome: "written".to_string(),
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
        }
    }
}
