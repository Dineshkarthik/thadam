//! The memory sinks must pass their own suites, and the suites must catch sinks
//! that break the rules.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use thadam_core::{
    BatchId, BatchSink, DatasetEvents, DeliveryClass, ErrorDomain, Reject, Retry, SegmentBatch,
    SegmentSink, SinkBatch, SinkCapabilities, SinkContext, ThadamError, WalPos, WriteOutcome,
    codes,
};
use thadam_testkit::conformance::batch_sink::{
    self, BatchSinkCase, BatchSinkFixture, BatchSinkProbe, InjectedFault, MemoryBatchSinkFixture,
};
use thadam_testkit::conformance::segment_sink::{self, MemorySegmentFixture, SegmentSinkFixture};
use thadam_testkit::{Failpoint, MemoryBatchSinkConfig, MemorySegmentSink};

#[tokio::test]
async fn idempotent_memory_batch_sink_passes() {
    batch_sink::run_all(&MemoryBatchSinkFixture::default()).await;
}

#[tokio::test]
async fn at_least_once_memory_batch_sink_passes() {
    let fixture = MemoryBatchSinkFixture {
        config: MemoryBatchSinkConfig {
            delivery: DeliveryClass::AtLeastOnce,
            ..MemoryBatchSinkConfig::default()
        },
    };
    batch_sink::run_all(&fixture).await;
}

#[tokio::test(start_paused = true)]
async fn a_slow_memory_batch_sink_passes() {
    let fixture = MemoryBatchSinkFixture {
        config: MemoryBatchSinkConfig {
            latency: Duration::from_millis(40),
            ..MemoryBatchSinkConfig::default()
        },
    };
    batch_sink::run_all(&fixture).await;
}

/// Claims to be idempotent but applies every write.
struct DuplicatingSink {
    applied: Arc<Mutex<(usize, Vec<BatchId>)>>,
}

#[async_trait]
impl BatchSink for DuplicatingSink {
    async fn open(&mut self, _: &SinkContext) -> Result<SinkCapabilities, ThadamError> {
        Ok(SinkCapabilities::new(DeliveryClass::IdempotentBatch))
    }
    async fn write(&mut self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError> {
        let mut st = self.applied.lock().unwrap();
        st.0 += batch.events().len();
        st.1.push(batch.id());
        Ok(WriteOutcome::default())
    }
    async fn close(&mut self) -> Result<(), ThadamError> {
        Ok(())
    }
}

struct DuplicatingProbe(Arc<Mutex<(usize, Vec<BatchId>)>>);

impl BatchSinkProbe for DuplicatingProbe {
    fn applied_events(&self) -> usize {
        self.0.lock().unwrap().0
    }
    fn attempt_ids(&self) -> Vec<BatchId> {
        self.0.lock().unwrap().1.clone()
    }
    fn inject(&self, _: InjectedFault) -> bool {
        false
    }
}

struct DuplicatingFixture;

impl BatchSinkFixture for DuplicatingFixture {
    fn create(&self) -> BatchSinkCase {
        let applied = Arc::new(Mutex::new((0, Vec::new())));
        BatchSinkCase {
            sink: Box::new(DuplicatingSink {
                applied: Arc::clone(&applied),
            }),
            probe: Box::new(DuplicatingProbe(applied)),
        }
    }
}

#[tokio::test]
#[should_panic(expected = "an idempotent sink applied a batch twice")]
async fn a_sink_that_lies_about_idempotence_is_caught() {
    batch_sink::repeated_batch_id_is_tolerated(&DuplicatingFixture).await;
}

#[tokio::test]
async fn faults_that_cannot_be_injected_skip_their_checks() {
    batch_sink::transient_failure_retries_the_same_batch_id(&DuplicatingFixture).await;
    batch_sink::unknown_outcome_retry_is_safe(&DuplicatingFixture).await;
    batch_sink::error_classes_are_reported(&DuplicatingFixture).await;
    batch_sink::distinct_batches_accumulate(&DuplicatingFixture).await;
}

/// Reports every failure as permanent, whatever it was.
struct FlatteningSink(Box<dyn BatchSink>);

#[async_trait]
impl BatchSink for FlatteningSink {
    async fn open(&mut self, ctx: &SinkContext) -> Result<SinkCapabilities, ThadamError> {
        self.0.open(ctx).await
    }
    async fn write(&mut self, batch: &SinkBatch) -> Result<WriteOutcome, ThadamError> {
        self.0.write(batch).await.map_err(|mut e| {
            e.retry = Retry::Permanent;
            e
        })
    }
    async fn close(&mut self) -> Result<(), ThadamError> {
        self.0.close().await
    }
}

struct FlatteningFixture(MemoryBatchSinkFixture);

impl BatchSinkFixture for FlatteningFixture {
    fn create(&self) -> BatchSinkCase {
        let inner = self.0.create();
        BatchSinkCase {
            sink: Box::new(FlatteningSink(inner.sink)),
            probe: inner.probe,
        }
    }
}

#[tokio::test]
#[should_panic(expected = "transient failure misclassified")]
async fn a_sink_that_misclassifies_transient_errors_is_caught() {
    let fixture = FlatteningFixture(MemoryBatchSinkFixture::default());
    batch_sink::transient_failure_retries_the_same_batch_id(&fixture).await;
}

#[tokio::test]
async fn memory_segment_sink_passes() {
    for frames_per_segment in [1, 2, 3, 5] {
        let fixture = MemorySegmentFixture::new(frames_per_segment);
        segment_sink::run_all(&fixture, 11 + frames_per_segment as u64).await;
    }
}

/// Forgets what is durable when it restarts, as a sink that rebuilds nothing
/// would.
struct AmnesiacSink(MemorySegmentSink);

#[async_trait]
impl SegmentSink for AmnesiacSink {
    async fn recover(&mut self, ctx: &SinkContext) -> Result<Option<WalPos>, ThadamError> {
        self.0.recover(ctx).await?;
        Ok(None)
    }
    async fn append(&mut self, batch: SegmentBatch) -> Result<Vec<Reject>, ThadamError> {
        self.0.append(batch).await
    }
    async fn poll_commit(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.0.poll_commit().await
    }
    async fn finalize_all(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.0.finalize_all().await
    }
    fn cursor_committed(&mut self, pos: WalPos) {
        self.0.cursor_committed(pos);
    }
}

struct AmnesiacFixture(MemorySegmentFixture);

impl SegmentSinkFixture for AmnesiacFixture {
    fn open_sink(&self) -> Box<dyn SegmentSink> {
        Box::new(AmnesiacSink(MemorySegmentSink::new(
            self.0.store().clone(),
            2,
        )))
    }
    fn failpoints(&self) -> Vec<&'static str> {
        self.0.failpoints()
    }
    fn arm(&self, failpoint: &str) {
        self.0.arm(failpoint);
    }
    fn disarm(&self) {
        self.0.disarm();
    }
    fn finalized_ranges(&self) -> Vec<thadam_core::WalRange> {
        self.0.finalized_ranges()
    }
    fn reset(&self) {
        self.0.reset();
    }
}

#[tokio::test]
#[should_panic(expected = "behind the")]
async fn a_segment_sink_that_forgets_its_watermark_on_restart_is_caught() {
    segment_sink::recovery_after_crash_at_every_failpoint(&AmnesiacFixture(
        MemorySegmentFixture::new(2),
    ))
    .await;
}

/// Reports a watermark beyond what it appended.
struct OverreachingSink(MemorySegmentSink);

#[async_trait]
impl SegmentSink for OverreachingSink {
    async fn recover(&mut self, ctx: &SinkContext) -> Result<Option<WalPos>, ThadamError> {
        self.0.recover(ctx).await
    }
    async fn append(&mut self, batch: SegmentBatch) -> Result<Vec<Reject>, ThadamError> {
        self.0.append(batch).await
    }
    async fn poll_commit(&mut self) -> Result<Option<WalPos>, ThadamError> {
        Ok(self
            .0
            .poll_commit()
            .await?
            .map(|w| WalPos::new(w.get() + 5)))
    }
    async fn finalize_all(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.0.finalize_all().await
    }
    fn cursor_committed(&mut self, pos: WalPos) {
        self.0.cursor_committed(pos);
    }
}

struct OverreachingFixture(MemorySegmentFixture);

impl SegmentSinkFixture for OverreachingFixture {
    fn open_sink(&self) -> Box<dyn SegmentSink> {
        Box::new(OverreachingSink(MemorySegmentSink::new(
            self.0.store().clone(),
            2,
        )))
    }
    fn failpoints(&self) -> Vec<&'static str> {
        self.0.failpoints()
    }
    fn arm(&self, failpoint: &str) {
        self.0.arm(failpoint);
    }
    fn disarm(&self) {
        self.0.disarm();
    }
    fn finalized_ranges(&self) -> Vec<thadam_core::WalRange> {
        self.0.finalized_ranges()
    }
    fn reset(&self) {
        self.0.reset();
    }
}

#[tokio::test]
#[should_panic(expected = "is ahead of the")]
async fn a_segment_sink_that_reports_a_watermark_ahead_of_its_data_is_caught() {
    segment_sink::watermark_is_monotonic_and_complete(
        &OverreachingFixture(MemorySegmentFixture::new(2)),
        5,
    )
    .await;
}

/// Folds every dataset into one, as a sink that ignores the grouping would.
struct MergingSink(MemorySegmentSink);

#[async_trait]
impl SegmentSink for MergingSink {
    async fn recover(&mut self, ctx: &SinkContext) -> Result<Option<WalPos>, ThadamError> {
        self.0.recover(ctx).await
    }
    async fn append(&mut self, batch: SegmentBatch) -> Result<Vec<Reject>, ThadamError> {
        let events = batch.datasets.into_iter().flat_map(|d| d.events).collect();
        let merged = SegmentBatch::new(
            batch.origin,
            batch.clock_synced,
            vec![DatasetEvents::new("merged", events)],
        );
        self.0.append(merged).await
    }
    async fn poll_commit(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.0.poll_commit().await
    }
    async fn finalize_all(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.0.finalize_all().await
    }
    fn cursor_committed(&mut self, pos: WalPos) {
        self.0.cursor_committed(pos);
    }
}

struct MergingFixture(MemorySegmentFixture);

impl SegmentSinkFixture for MergingFixture {
    fn open_sink(&self) -> Box<dyn SegmentSink> {
        Box::new(MergingSink(MemorySegmentSink::new(
            self.0.store().clone(),
            2,
        )))
    }
    fn failpoints(&self) -> Vec<&'static str> {
        self.0.failpoints()
    }
    fn arm(&self, failpoint: &str) {
        self.0.arm(failpoint);
    }
    fn disarm(&self) {
        self.0.disarm();
    }
    fn finalized_ranges(&self) -> Vec<thadam_core::WalRange> {
        self.0.finalized_ranges()
    }
    fn finalized_dataset_events(&self) -> Option<std::collections::BTreeMap<String, usize>> {
        self.0.finalized_dataset_events()
    }
    fn reset(&self) {
        self.0.reset();
    }
}

#[tokio::test]
#[should_panic(expected = "datasets were merged")]
async fn a_segment_sink_that_merges_datasets_is_caught() {
    segment_sink::datasets_are_kept_apart(&MergingFixture(MemorySegmentFixture::new(2))).await;
}

#[test]
fn failpoints_are_named_like_the_catalog_ones() {
    for fp in Failpoint::ALL {
        assert!(fp.name().starts_with("memory::"));
    }
    let _ = (ErrorDomain::Sink, codes::SINK_IO);
}
