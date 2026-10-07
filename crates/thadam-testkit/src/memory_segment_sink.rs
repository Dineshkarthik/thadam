//! An in-memory [`SegmentSink`] with a durable store and crash failpoints.
//!
//! [`MemorySegmentStore`] stands in for the filesystem: it outlives sinks, so
//! dropping a sink and opening a new one over the same store simulates a
//! process restart. Finalizing a segment goes through the same steps as the
//! Parquet sink (rename the file, record it in the catalog, report the
//! watermark), with a [`Failpoint`] between each.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use thadam_core::{
    ErrorDomain, EventBatch, Reject, ThadamError, SegmentSink, SinkContext, WalPos, WalRange, codes,
};

use crate::util::lock;

/// A point where an armed sink "crashes".
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Failpoint {
    /// Before anything about the segment is durable.
    BeforeFinalize,
    /// The segment file is renamed into place but not yet in the catalog.
    AfterRenameBeforeCatalog,
    /// The catalog is updated but the watermark was not reported to the caller.
    AfterCatalogBeforeReturn,
}

impl Failpoint {
    /// Every failpoint, in execution order.
    pub const ALL: [Failpoint; 3] = [
        Failpoint::BeforeFinalize,
        Failpoint::AfterRenameBeforeCatalog,
        Failpoint::AfterCatalogBeforeReturn,
    ];

    /// The failpoint's stable name.
    pub const fn name(self) -> &'static str {
        match self {
            Failpoint::BeforeFinalize => "memory::before_finalize",
            Failpoint::AfterRenameBeforeCatalog => "memory::after_rename_before_catalog",
            Failpoint::AfterCatalogBeforeReturn => "memory::after_catalog_before_return",
        }
    }

    /// Looks a failpoint up by [`Failpoint::name`].
    pub fn from_name(name: &str) -> Option<Failpoint> {
        Self::ALL.into_iter().find(|f| f.name() == name)
    }
}

/// A finalized segment file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinalizedSegment {
    /// The WAL frames the file covers.
    pub range: WalRange,
    /// Events in the file.
    pub events: usize,
}

#[derive(Default)]
struct StoreInner {
    files: Vec<FinalizedSegment>,
    catalog: Vec<FinalizedSegment>,
    eligible: Vec<WalRange>,
    armed: Option<Failpoint>,
}

/// The durable state shared by successive sink instances.
#[derive(Clone, Default)]
pub struct MemorySegmentStore(Arc<Mutex<StoreInner>>);

impl MemorySegmentStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Arms a failpoint: the next time a sink reaches it, the sink crashes.
    pub fn arm(&self, failpoint: Failpoint) {
        lock(&self.0).armed = Some(failpoint);
    }

    /// Disarms any failpoint.
    pub fn disarm(&self) {
        lock(&self.0).armed = None;
    }

    /// Whether an armed failpoint is still waiting to be hit.
    pub fn is_armed(&self) -> bool {
        lock(&self.0).armed.is_some()
    }

    /// Segment files on "disk", whether or not the catalog knows them.
    pub fn files(&self) -> Vec<FinalizedSegment> {
        lock(&self.0).files.clone()
    }

    /// The catalog's view of finalized segments.
    pub fn catalog(&self) -> Vec<FinalizedSegment> {
        lock(&self.0).catalog.clone()
    }

    /// WAL ranges of the segment files on disk.
    pub fn finalized_ranges(&self) -> Vec<WalRange> {
        lock(&self.0).files.iter().map(|f| f.range).collect()
    }

    /// Ranges made eligible for shipping by `cursor_committed`.
    pub fn eligible_ranges(&self) -> Vec<WalRange> {
        lock(&self.0).eligible.clone()
    }

    /// Wipes everything.
    pub fn reset(&self) {
        *lock(&self.0) = StoreInner::default();
    }
}

/// The sink half.
pub struct MemorySegmentSink {
    store: MemorySegmentStore,
    frames_per_segment: usize,
    buffered: Vec<EventBatch>,
    last_appended: Option<WalPos>,
    crashed: bool,
}

impl MemorySegmentSink {
    /// A sink over `store` that finalizes a segment once it buffers
    /// `frames_per_segment` frames.
    pub fn new(store: MemorySegmentStore, frames_per_segment: usize) -> Self {
        MemorySegmentSink {
            store,
            frames_per_segment: frames_per_segment.max(1),
            buffered: Vec::new(),
            last_appended: None,
            crashed: false,
        }
    }

    fn check_alive(&self) -> Result<(), ThadamError> {
        if self.crashed {
            return Err(ThadamError::fatal(
                ErrorDomain::Sink,
                codes::SINK_IO,
                "the sink crashed at a failpoint",
            ));
        }
        Ok(())
    }

    fn hit(&mut self, failpoint: Failpoint) -> Result<(), ThadamError> {
        let mut inner = lock(&self.store.0);
        if inner.armed == Some(failpoint) {
            inner.armed = None;
            self.crashed = true;
            return Err(ThadamError::fatal(
                ErrorDomain::Sink,
                codes::SINK_IO,
                format!("crashed at failpoint {}", failpoint.name()),
            ));
        }
        Ok(())
    }

    fn buffered_frames(&self) -> u64 {
        self.buffered.iter().map(|b| b.origin.len()).sum()
    }

    fn durable_through(&self) -> Option<WalPos> {
        lock(&self.store.0).files.last().map(|f| f.range.last())
    }

    fn finalize(&mut self) -> Result<Option<WalPos>, ThadamError> {
        let (Some(first), Some(last)) = (self.buffered.first(), self.buffered.last()) else {
            return Ok(None);
        };
        let range = WalRange::new(
            first.origin.epoch(),
            first.origin.first(),
            last.origin.last(),
        )
        .map_err(|e| {
            ThadamError::permanent(ErrorDomain::Sink, codes::SINK_WRITE_FAILED, e.to_string())
        })?;
        let segment = FinalizedSegment {
            range,
            events: self.buffered.iter().map(EventBatch::len).sum(),
        };

        self.hit(Failpoint::BeforeFinalize)?;
        lock(&self.store.0).files.push(segment.clone());
        self.hit(Failpoint::AfterRenameBeforeCatalog)?;
        lock(&self.store.0).catalog.push(segment);
        self.buffered.clear();
        self.hit(Failpoint::AfterCatalogBeforeReturn)?;
        Ok(Some(range.last()))
    }
}

#[async_trait]
impl SegmentSink for MemorySegmentSink {
    async fn recover(&mut self, _ctx: &SinkContext) -> Result<Option<WalPos>, ThadamError> {
        self.check_alive()?;
        {
            let mut inner = lock(&self.store.0);
            // Files are the truth; the catalog is rebuilt from them, like the
            // Parquet sink rebuilds it from footers.
            inner.catalog = inner.files.clone();
        }
        self.buffered.clear();
        let durable = self.durable_through();
        self.last_appended = durable;
        Ok(durable)
    }

    async fn append(&mut self, batch: EventBatch) -> Result<Vec<Reject>, ThadamError> {
        self.check_alive()?;
        if self
            .last_appended
            .is_some_and(|last| batch.origin.first() <= last)
        {
            return Err(ThadamError::permanent(
                ErrorDomain::Sink,
                codes::SINK_NON_MONOTONIC_APPEND,
                format!(
                    "batch {} does not come after position {}",
                    batch.origin,
                    self.last_appended.map_or(0, WalPos::get)
                ),
            ));
        }
        self.last_appended = Some(batch.origin.last());
        self.buffered.push(batch);
        Ok(Vec::new())
    }

    async fn poll_commit(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.check_alive()?;
        if self.buffered_frames() < self.frames_per_segment as u64 {
            return Ok(None);
        }
        self.finalize()
    }

    async fn finalize_all(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.check_alive()?;
        self.finalize()?;
        Ok(self.durable_through())
    }

    fn cursor_committed(&mut self, pos: WalPos) {
        let mut inner = lock(&self.store.0);
        let StoreInner {
            files, eligible, ..
        } = &mut *inner;
        for f in files.iter().filter(|f| f.range.last() <= pos) {
            if !eligible.contains(&f.range) {
                eligible.push(f.range);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::test_batch;
    use thadam_core::{DiskBudget, Shutdown, SinkId};
    use std::path::PathBuf;

    use crate::FakeClock;

    fn ctx() -> SinkContext {
        SinkContext::new(
            SinkId::new("mem"),
            Shutdown::new(),
            FakeClock::default().shared(),
            PathBuf::new(),
            Arc::new(DiskBudget::new()),
        )
    }

    #[tokio::test]
    async fn finalizes_when_enough_frames_are_buffered() {
        let store = MemorySegmentStore::new();
        let mut sink = MemorySegmentSink::new(store.clone(), 3);
        assert_eq!(sink.recover(&ctx()).await.unwrap(), None);
        sink.append(test_batch(1, 1, 2)).await.unwrap();
        assert_eq!(sink.poll_commit().await.unwrap(), None);
        sink.append(test_batch(1, 3, 3)).await.unwrap();
        assert_eq!(sink.poll_commit().await.unwrap(), Some(WalPos::new(3)));
        assert_eq!(store.files().len(), 1);
        assert_eq!(store.files()[0].events, 3);
        assert_eq!(store.catalog(), store.files());
    }

    #[tokio::test]
    async fn out_of_order_append_is_refused() {
        let mut sink = MemorySegmentSink::new(MemorySegmentStore::new(), 3);
        sink.append(test_batch(1, 5, 6)).await.unwrap();
        for (a, b) in [(3, 4), (6, 7), (5, 5)] {
            let err = sink.append(test_batch(1, a, b)).await.unwrap_err();
            assert_eq!(err.code, codes::SINK_NON_MONOTONIC_APPEND);
        }
        sink.append(test_batch(1, 7, 7)).await.unwrap();
    }

    #[tokio::test]
    async fn failpoints_crash_the_sink_and_recovery_rebuilds_the_catalog() {
        let store = MemorySegmentStore::new();
        let mut sink = MemorySegmentSink::new(store.clone(), 1);
        sink.recover(&ctx()).await.unwrap();
        store.arm(Failpoint::AfterRenameBeforeCatalog);
        sink.append(test_batch(1, 1, 1)).await.unwrap();
        assert!(sink.poll_commit().await.is_err());
        assert!(
            sink.append(test_batch(1, 2, 2)).await.is_err(),
            "a crashed sink stays dead"
        );
        assert_eq!(store.files().len(), 1);
        assert!(store.catalog().is_empty());

        let mut restarted = MemorySegmentSink::new(store.clone(), 1);
        assert_eq!(
            restarted.recover(&ctx()).await.unwrap(),
            Some(WalPos::new(1))
        );
        assert_eq!(store.catalog().len(), 1);
    }

    #[tokio::test]
    async fn before_finalize_loses_only_buffered_data() {
        let store = MemorySegmentStore::new();
        let mut sink = MemorySegmentSink::new(store.clone(), 1);
        sink.recover(&ctx()).await.unwrap();
        sink.append(test_batch(1, 1, 1)).await.unwrap();
        assert_eq!(sink.poll_commit().await.unwrap(), Some(WalPos::new(1)));
        store.arm(Failpoint::BeforeFinalize);
        sink.append(test_batch(1, 2, 2)).await.unwrap();
        assert!(sink.poll_commit().await.is_err());
        let mut restarted = MemorySegmentSink::new(store.clone(), 1);
        assert_eq!(
            restarted.recover(&ctx()).await.unwrap(),
            Some(WalPos::new(1))
        );
    }

    #[tokio::test]
    async fn cursor_committed_makes_finalized_files_eligible() {
        let store = MemorySegmentStore::new();
        let mut sink = MemorySegmentSink::new(store.clone(), 1);
        sink.recover(&ctx()).await.unwrap();
        for p in 1..=3 {
            sink.append(test_batch(1, p, p)).await.unwrap();
            sink.poll_commit().await.unwrap();
        }
        sink.cursor_committed(WalPos::new(2));
        assert_eq!(store.eligible_ranges().len(), 2);
        sink.cursor_committed(WalPos::new(2));
        assert_eq!(store.eligible_ranges().len(), 2, "idempotent");
    }

    #[test]
    fn failpoint_names_round_trip() {
        for fp in Failpoint::ALL {
            assert_eq!(Failpoint::from_name(fp.name()), Some(fp));
        }
        assert_eq!(Failpoint::from_name("nope"), None);
    }
}
