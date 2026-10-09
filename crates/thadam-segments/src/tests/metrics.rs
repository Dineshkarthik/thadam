//! Segment metrics through the sink-context hook: every catalog transition
//! republishes the absolute `SegmentFiles` gauges and each finalized file
//! observes `SegmentFinalized` (ARCHITECTURE.md section 14 names).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};

use thadam_core::{ConnectorMetric, MetricsRecorder, SegmentBatch, SegmentSink, WalPos};
use thadam_testkit::test_segment_batch_across;

use crate::config::RotationConfig;
use crate::tests::fixture::ParquetFixture;

const ONE: [&str; 1] = ["only"];

fn batch(datasets: &[&str], first: u64, last: u64) -> SegmentBatch {
    test_segment_batch_across(1, first, last, datasets)
}

/// Connector metrics debug-formatted in order (what the runtime would
/// see).
#[derive(Debug, Default)]
struct Recording(Mutex<Vec<String>>);

impl Recording {
    fn new() -> Arc<Self> {
        Arc::new(Recording::default())
    }

    fn events(&self) -> Vec<String> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl MetricsRecorder for Recording {
    fn record(&self, metric: ConnectorMetric<'_>) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(format!("{metric:?}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sink_reports_segment_metrics_through_the_context_hook() {
    let fixture = ParquetFixture::new(
        &ONE,
        RotationConfig {
            max_bytes: None,
            max_rows: Some(1),
            max_age: None,
        },
        4,
    );
    let recording = Recording::new();
    let ctx = fixture.ctx().with_metrics(recording.clone());
    let mut sink = fixture.open_sink();

    sink.recover(&ctx).await.unwrap();
    let after_recover = recording.events();
    assert!(
        after_recover
            .iter()
            .any(|e| e.starts_with(r#"SegmentFiles { sink: "test", state: "finalized", files: 0"#)),
        "recovery publishes an initial snapshot: {after_recover:?}"
    );

    // max_rows 1 finalizes on append: a file lands finalized.
    sink.append(batch(&ONE, 1, 1)).await.unwrap();
    sink.poll_commit().await.unwrap();
    let after_append = recording.events();
    assert!(
        after_append.iter().any(
            |e| e.starts_with(r#"SegmentFinalized { sink: "test""#) && !e.contains("bytes: 0,")
        ),
        "the finalized file reports its bytes and latency: {after_append:?}"
    );
    let finalized_pos = after_append
        .iter()
        .position(|e| e.starts_with(r#"SegmentFiles { sink: "test", state: "finalized", files: 1"#))
        .unwrap_or_else(|| panic!("a finalized=1 snapshot: {after_append:?}"));

    // The cursor passing the segment's end makes it eligible.
    sink.cursor_committed(WalPos::new(1));
    sink.poll_commit().await.unwrap();
    let events = recording.events();
    let eligible_pos = events
        .iter()
        .position(|e| e.starts_with(r#"SegmentFiles { sink: "test", state: "eligible", files: 1"#))
        .unwrap_or_else(|| panic!("an eligible=1 snapshot: {events:?}"));
    assert!(
        finalized_pos < eligible_pos,
        "finalized precedes eligible: {events:?}"
    );
    let zeroed = events
        .iter()
        .rposition(|e| e.starts_with(r#"SegmentFiles { sink: "test", state: "finalized""#))
        .unwrap_or_else(|| panic!("finalized series present: {events:?}"));
    assert!(
        events[zeroed].contains("files: 0, bytes: 0"),
        "the eligible transition re-zeroes finalized (absolute totals): {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| e.starts_with(r#"SegmentFiles { sink: "test", state: "staging", files: 0"#)),
        "no open segments remain after the rotation finalize: {events:?}"
    );

    sink.finalize_all().await.unwrap();
}
