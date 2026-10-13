//! Tests for the Prometheus metrics sink.

use thadam_core::ConnectorMetric;
use thadam_runtime::{MetricEvent, MetricsSink, PrometheusMetrics};

/// The exact list of exported metric names: every counter carries its `_total`
/// suffix exactly once (the text encoder appends it; registrations omit it).
/// This pins the names the way an insta snapshot would, so they cannot drift.
#[test]
fn test_metric_names_exact_list() {
    let metrics = PrometheusMetrics::new();
    let names = metrics.metric_names();
    let expected: Vec<&str> = vec![
        "thadam_batch_bisect_total",
        "thadam_batch_write_seconds",
        "thadam_batch_written_events_total",
        "thadam_batch_written_total",
        "thadam_build_info",
        "thadam_chain_events_total",
        "thadam_chain_panic_total",
        "thadam_clock_synced",
        "thadam_component_state",
        "thadam_cursor_committed_total",
        "thadam_quarantined_total",
        "thadam_rejects_dropped_total",
        "thadam_rejects_total",
        "thadam_retry_total",
        "thadam_segment_bytes",
        "thadam_segment_files",
        "thadam_segment_finalize_seconds",
        "thadam_ship_bytes_total",
        "thadam_ship_retry_age_seconds",
        "thadam_ship_upload_seconds",
        "thadam_ship_uploads_total",
        "thadam_source_ack_latency_seconds",
        "thadam_source_bytes_total",
        "thadam_source_dropped_total",
        "thadam_source_messages_total",
        "thadam_source_restart_total",
        "thadam_wal_evicted_bytes_total",
        "thadam_wal_gap_total",
        "thadam_wal_maintained_deleted_bytes_total",
    ];
    assert_eq!(names, expected, "metric_names() pins the exported names");
    for name in &names {
        assert!(
            name.starts_with("thadam_"),
            "metric name '{name}' should start with 'thadam_'",
        );
        assert!(
            !name.contains("_total_total"),
            "metric name '{name}' must carry `_total` exactly once",
        );
    }
}

/// Sending two events of a counter variant makes encode() contain that metric
/// with exactly one `_total` and the given labels, with value 2.
#[test]
fn test_counter_increment() {
    let metrics = PrometheusMetrics::new();

    let event = MetricEvent::ChainEvents {
        sink: "test_sink",
        dataset: "test_dataset",
        outcome: "filtered",
        events: 1,
    };

    metrics.record(event);
    metrics.record(event);

    let encoded = metrics.encode();

    assert!(
        encoded.contains(
            "thadam_chain_events_total{sink=\"test_sink\",dataset=\"test_dataset\",outcome=\"filtered\"} 2"
        ),
        "encoded metrics should contain thadam_chain_events_total with value 2, got:\n{}",
        encoded
    );
    assert!(
        !encoded.contains("chain_events_total_total"),
        "counter names encode with exactly one _total, got:\n{}",
        encoded
    );
}

/// Source metrics carry only the `source` label; the sink/dataset labels are
/// chain-metric detail and must not appear on intake series.
#[test]
fn test_source_metrics_have_only_the_source_label() {
    let metrics = PrometheusMetrics::new();

    metrics.record(MetricEvent::SourceBatch {
        source: "mqtt-in",
        messages: 3,
        bytes: 512,
    });

    let encoded = metrics.encode();

    for family in ["thadam_source_messages_total", "thadam_source_bytes_total"] {
        let series = encoded
            .lines()
            .find(|l| l.starts_with(family) && !l.starts_with("# "))
            .unwrap_or_else(|| panic!("no series for {family} in:\n{encoded}"));
        assert!(
            series.contains("source=\"mqtt-in\""),
            "{family} should be labelled with the source: {series}"
        );
        assert!(
            !series.contains("sink=") && !series.contains("dataset="),
            "{family} must not carry sink/dataset labels: {series}"
        );
    }
}

/// Each chain outcome lands in its own series.
#[test]
fn test_chain_outcomes_are_separate_series() {
    let metrics = PrometheusMetrics::new();

    for (outcome, events) in [
        ("written", 4usize),
        ("filtered", 2),
        ("deduped", 1),
        ("rejected", 1),
        ("exploded", 6),
    ] {
        metrics.record(MetricEvent::ChainEvents {
            sink: "out",
            dataset: "d",
            outcome,
            events,
        });
    }

    let encoded = metrics.encode();

    for (outcome, events) in [
        ("written", 4),
        ("filtered", 2),
        ("deduped", 1),
        ("rejected", 1),
        ("exploded", 6),
    ] {
        let want = format!(
            "thadam_chain_events_total{{sink=\"out\",dataset=\"d\",outcome=\"{outcome}\"}} {events}"
        );
        assert!(
            encoded.contains(&want),
            "missing outcome series `{want}` in:\n{encoded}"
        );
    }
}

/// Shipper outcomes, bytes, and durations land in their dedicated
/// families, with `fatal` counting no bytes and observing no latency and
/// each retry counting once against the `retry` outcome.
#[test]
fn test_ship_metrics_encode() {
    let metrics = PrometheusMetrics::new();
    let ok = |bytes| {
        MetricEvent::Connector(ConnectorMetric::ShipUploaded {
            sink: "s3",
            outcome: "ok",
            bytes,
            latency: Some(std::time::Duration::from_millis(30)),
        })
    };
    metrics.record(ok(120));
    metrics.record(ok(70));
    metrics.record(MetricEvent::Connector(ConnectorMetric::ShipUploaded {
        sink: "s3",
        outcome: "conflict_equal",
        bytes: 90,
        latency: Some(std::time::Duration::from_millis(6)),
    }));
    metrics.record(MetricEvent::Connector(ConnectorMetric::ShipUploaded {
        sink: "s3",
        outcome: "fatal",
        bytes: 0,
        latency: None,
    }));
    metrics.record(MetricEvent::Connector(ConnectorMetric::ShipRetried {
        sink: "s3",
        retry_age: std::time::Duration::from_secs(42),
    }));

    let encoded = metrics.encode();

    for want in [
        "thadam_ship_uploads_total{sink=\"s3\",outcome=\"ok\"} 2",
        "thadam_ship_uploads_total{sink=\"s3\",outcome=\"conflict_equal\"} 1",
        "thadam_ship_uploads_total{sink=\"s3\",outcome=\"fatal\"} 1",
        "thadam_ship_uploads_total{sink=\"s3\",outcome=\"retry\"} 1",
        "thadam_ship_bytes_total{sink=\"s3\"} 280",
        "thadam_ship_upload_seconds_count{sink=\"s3\"} 3",
        "thadam_ship_retry_age_seconds_count{sink=\"s3\"} 1",
    ] {
        assert!(encoded.contains(want), "missing `{want}` in:\n{encoded}");
    }
}

/// Segment gauges take absolute values per (sink, state) series, and a
/// finalized file observes the finalize histogram.
#[test]
fn test_segment_metrics_encode() {
    let metrics = PrometheusMetrics::new();

    for (state, files, bytes) in [("finalized", 3u64, 900u64), ("staging", 1, 55)] {
        metrics.record(MetricEvent::Connector(ConnectorMetric::SegmentFiles {
            sink: "local",
            state,
            files,
            bytes,
        }));
    }
    // A later absolute snapshot replaces the earlier one.
    metrics.record(MetricEvent::Connector(ConnectorMetric::SegmentFiles {
        sink: "local",
        state: "staging",
        files: 0,
        bytes: 0,
    }));
    metrics.record(MetricEvent::Connector(ConnectorMetric::SegmentFinalized {
        sink: "local",
        bytes: 42,
        latency: std::time::Duration::from_millis(12),
    }));

    let encoded = metrics.encode();

    for want in [
        "thadam_segment_files{sink=\"local\",state=\"finalized\"} 3",
        "thadam_segment_bytes{sink=\"local\",state=\"finalized\"} 900",
        "thadam_segment_files{sink=\"local\",state=\"staging\"} 0",
        "thadam_segment_bytes{sink=\"local\",state=\"staging\"} 0",
        "thadam_segment_finalize_seconds_count{sink=\"local\"} 1",
        "thadam_segment_finalize_seconds_sum{sink=\"local\"} 0.012",
    ] {
        assert!(encoded.contains(want), "missing `{want}` in:\n{encoded}");
    }
}
