//! Tests for the Prometheus metrics sink.

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
        "thadam_chain_events_total",
        "thadam_chain_panic_total",
        "thadam_clock_synced",
        "thadam_component_state",
        "thadam_cursor_committed_total",
        "thadam_quarantined_total",
        "thadam_rejects_dropped_total",
        "thadam_rejects_total",
        "thadam_retry_total",
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
