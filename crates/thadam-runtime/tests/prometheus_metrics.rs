//! Tests for the Prometheus metrics sink.

use thadam_runtime::{MetricEvent, MetricsSink, PrometheusMetrics};

/// Test that metric_names() returns a non-empty, sorted list where every
/// name starts with `thadam_`.
#[test]
fn test_metric_names_sorted_and_prefixed() {
    let metrics = PrometheusMetrics::new();
    let names = metrics.metric_names();

    // Must be non-empty
    assert!(
        !names.is_empty(),
        "metric_names() should return a non-empty list"
    );

    // Must be sorted
    let mut sorted_names = names.clone();
    sorted_names.sort_unstable();
    assert_eq!(
        names, sorted_names,
        "metric_names() should return a sorted list"
    );

    // Every name must start with "thadam_"
    for name in &names {
        assert!(
            name.starts_with("thadam_"),
            "metric name '{name}' should start with 'thadam_'",
        );
    }
}

/// Test that sending two events of a counter-type variant makes encode()
/// contain that metric with value 2.
#[test]
fn test_counter_increment() {
    let metrics = PrometheusMetrics::new();

    // Use ChainEvents as a counter-type variant
    let event = MetricEvent::ChainEvents {
        sink: "test_sink",
        dataset: "test_dataset",
        events: 1,
    };

    // Record two events
    metrics.record(event);
    metrics.record(event);

    // Encode and check for the metric with value 2
    let encoded = metrics.encode();

    // The metric should contain the counter with value 2
    // Note: prometheus-client adds _total suffix for counters
    assert!(
        encoded.contains("thadam_chain_events_total_total{sink=\"test_sink\",dataset=\"test_dataset\",outcome=\"written\"} 2")
        || encoded.contains("thadam_chain_events_total_total{dataset=\"test_dataset\",outcome=\"written\",sink=\"test_sink\"} 2"),
        "encoded metrics should contain thadam_chain_events_total with value 2, got:\n{}",
        encoded
    );
}
