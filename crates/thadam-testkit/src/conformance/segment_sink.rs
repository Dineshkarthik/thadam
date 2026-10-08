//! Conformance suite for [`SegmentSink`] implementations (ADR-0002, ADR-0013).
//!
//! A segment sink owns open state and reports a committed watermark. The
//! runtime advances its cursor only to that watermark and, after a restart,
//! resumes from the position `recover` reports. The checks here cover the two
//! things that must hold for that to be safe: the watermark only moves forward
//! and never ahead of what is durable, and after a crash at any failpoint no
//! WAL frame is lost or lands in two files. Every check appends
//! [`SegmentBatch`](thadam_core::SegmentBatch)es that spread events over three
//! datasets, and one check confirms the datasets stay apart.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use thadam_core::{DiskBudget, SegmentSink, Shutdown, SinkContext, SinkId, WalPos, WalRange};

use crate::FakeClock;
use crate::fixtures::test_segment_batch_across;
use crate::memory_segment_sink::{Failpoint, MemorySegmentSink, MemorySegmentStore};
use crate::util::SplitMix64;

/// Builds sinks over durable state that outlives them.
pub trait SegmentSinkFixture: Send + Sync {
    /// A new sink over the fixture's durable state. Calling this again after
    /// dropping the previous sink simulates a process restart.
    fn open_sink(&self) -> Box<dyn SegmentSink>;

    /// The names of the failpoints this sink supports.
    fn failpoints(&self) -> Vec<&'static str>;

    /// Arms a failpoint: the next time a sink reaches it, the operation fails
    /// and the sink is dead until a new one is opened.
    fn arm(&self, failpoint: &str);

    /// Disarms any failpoint that did not fire.
    fn disarm(&self);

    /// WAL ranges of the finalized files currently on "disk".
    fn finalized_ranges(&self) -> Vec<WalRange>;

    /// Events per dataset across the finalized files on "disk", or `None` if
    /// the fixture cannot tell (the per-dataset check is then skipped).
    fn finalized_dataset_events(&self) -> Option<BTreeMap<String, usize>> {
        None
    }

    /// Wipes the durable state.
    fn reset(&self);
}

/// The datasets every check spreads its events over.
const DATASETS: [&str; 3] = ["alpha", "beta", "gamma"];

fn batch(first: u64, last: u64) -> thadam_core::SegmentBatch {
    test_segment_batch_across(1, first, last, &DATASETS)
}

/// Runs the suite against [`MemorySegmentSink`].
#[derive(Clone, Default)]
pub struct MemorySegmentFixture {
    store: MemorySegmentStore,
    frames_per_segment: usize,
}

impl MemorySegmentFixture {
    /// A fixture whose sinks finalize a segment per `frames_per_segment` frames.
    pub fn new(frames_per_segment: usize) -> Self {
        MemorySegmentFixture {
            store: MemorySegmentStore::new(),
            frames_per_segment,
        }
    }

    /// The store behind the fixture.
    pub fn store(&self) -> &MemorySegmentStore {
        &self.store
    }
}

impl SegmentSinkFixture for MemorySegmentFixture {
    fn open_sink(&self) -> Box<dyn SegmentSink> {
        Box::new(MemorySegmentSink::new(
            self.store.clone(),
            self.frames_per_segment,
        ))
    }

    fn failpoints(&self) -> Vec<&'static str> {
        Failpoint::ALL.iter().map(|f| f.name()).collect()
    }

    fn arm(&self, failpoint: &str) {
        match Failpoint::from_name(failpoint) {
            Some(f) => self.store.arm(f),
            None => panic!("unknown failpoint {failpoint}"),
        }
    }

    fn disarm(&self) {
        self.store.disarm();
    }

    fn finalized_ranges(&self) -> Vec<WalRange> {
        self.store.finalized_ranges()
    }

    fn finalized_dataset_events(&self) -> Option<BTreeMap<String, usize>> {
        let mut total: BTreeMap<String, usize> = BTreeMap::new();
        for file in self.store.files() {
            for (dataset, n) in file.datasets {
                *total.entry(dataset).or_default() += n;
            }
        }
        Some(total)
    }

    fn reset(&self) {
        self.store.reset();
    }
}

fn context() -> SinkContext {
    SinkContext::new(
        SinkId::new("conformance"),
        Shutdown::new(),
        FakeClock::default().shared(),
        PathBuf::new(),
        Arc::new(DiskBudget::new()),
    )
}

async fn recover(sink: &mut dyn SegmentSink) -> Option<WalPos> {
    sink.recover(&context())
        .await
        .unwrap_or_else(|e| panic!("recover failed: {e}"))
}

fn assert_exactly_covers(ranges: &[WalRange], first: u64, last: u64, when: &str) {
    let mut sorted = ranges.to_vec();
    sorted.sort_by_key(|r| r.first());
    let mut next = first;
    for r in &sorted {
        assert_eq!(
            r.first().get(),
            next,
            "{when}: finalized files overlap or leave a gap before frame {next}: {sorted:?}"
        );
        next = r.last().get() + 1;
    }
    assert_eq!(
        next,
        last + 1,
        "{when}: frames {next}..={last} are in no finalized file: {sorted:?}"
    );
}

/// The watermark never decreases, never passes what was appended, and ends at
/// the last frame after `finalize_all`; every frame lands in exactly one file.
pub async fn watermark_is_monotonic_and_complete(fixture: &dyn SegmentSinkFixture, seed: u64) {
    const FRAMES: u64 = 40;
    fixture.reset();
    let mut sink = fixture.open_sink();
    assert_eq!(
        recover(sink.as_mut()).await,
        None,
        "a fresh sink must recover to nothing"
    );

    let mut rng = SplitMix64::new(seed);
    let mut appended = 0u64;
    let mut watermark: Option<WalPos> = None;
    while appended < FRAMES {
        let n = rng.between(1, 3).min(FRAMES - appended);
        let batch = batch(appended + 1, appended + n);
        let rejected = sink
            .append(batch)
            .await
            .unwrap_or_else(|e| panic!("append failed: {e}"));
        assert!(rejected.is_empty(), "the suite appends only valid events");
        appended += n;

        let reported = sink
            .poll_commit()
            .await
            .unwrap_or_else(|e| panic!("poll_commit failed: {e}"));
        if let Some(w) = reported {
            assert!(
                watermark.is_none_or(|prev| w > prev),
                "watermark went from {watermark:?} to {w}"
            );
            assert!(
                w.get() <= appended,
                "watermark {w} is ahead of the {appended} frames appended"
            );
            watermark = Some(w);
            sink.cursor_committed(w);
        }
    }
    let last = sink
        .finalize_all()
        .await
        .unwrap_or_else(|e| panic!("finalize_all failed: {e}"));
    assert_eq!(
        last,
        Some(WalPos::new(FRAMES)),
        "finalize_all must commit everything appended"
    );
    assert!(watermark.is_none_or(|w| Some(w) <= last));
    assert_exactly_covers(&fixture.finalized_ranges(), 1, FRAMES, "after finalize_all");
}

/// After a crash at any failpoint (and at several points in the stream), the
/// recovered position is at least every watermark reported before the crash,
/// and replaying the frames after it leaves every frame in exactly one file.
pub async fn recovery_after_crash_at_every_failpoint(fixture: &dyn SegmentSinkFixture) {
    const FRAMES: u64 = 12;
    for failpoint in fixture.failpoints() {
        for arm_before_frame in [1u64, 4, 7, 11] {
            let scenario = format!("failpoint {failpoint}, armed before frame {arm_before_frame}");
            fixture.reset();
            let mut sink = fixture.open_sink();
            recover(sink.as_mut()).await;

            let mut reported: Option<WalPos> = None;
            let mut crashed = false;
            for frame in 1..=FRAMES {
                if frame == arm_before_frame {
                    fixture.arm(failpoint);
                }
                if sink.append(batch(frame, frame)).await.is_err() {
                    crashed = true;
                    break;
                }
                match sink.poll_commit().await {
                    Ok(Some(w)) => reported = Some(w),
                    Ok(None) => {}
                    Err(_) => {
                        crashed = true;
                        break;
                    }
                }
            }
            if !crashed && sink.finalize_all().await.is_err() {
                crashed = true;
            }
            fixture.disarm();
            drop(sink);

            let mut restarted = fixture.open_sink();
            let durable = recover(restarted.as_mut()).await;
            assert!(
                durable >= reported,
                "{scenario}: recover reported {durable:?}, behind the {reported:?} already reported (crashed: {crashed})"
            );
            assert!(
                durable.is_none_or(|p| p.get() <= FRAMES),
                "{scenario}: recover reported {durable:?}, beyond the frames appended"
            );

            let resume = durable.map_or(1, |p| p.get() + 1);
            for frame in resume..=FRAMES {
                restarted
                    .append(batch(frame, frame))
                    .await
                    .unwrap_or_else(|e| panic!("{scenario}: replay of frame {frame} failed: {e}"));
                restarted.poll_commit().await.unwrap_or_else(|e| {
                    panic!("{scenario}: poll_commit during replay failed: {e}")
                });
            }
            let last = restarted
                .finalize_all()
                .await
                .unwrap_or_else(|e| panic!("{scenario}: finalize_all after replay failed: {e}"));
            assert_eq!(last, Some(WalPos::new(FRAMES)), "{scenario}");
            assert_exactly_covers(&fixture.finalized_ranges(), 1, FRAMES, &scenario);
        }
    }
}

/// Events of different datasets stay in their own dataset: after everything is
/// finalized, each dataset holds exactly the events it was given, including a
/// dataset that receives none in most batches.
pub async fn datasets_are_kept_apart(fixture: &dyn SegmentSinkFixture) {
    const FRAMES: u64 = 30;
    fixture.reset();
    let mut sink = fixture.open_sink();
    recover(sink.as_mut()).await;

    let mut expected: BTreeMap<String, usize> = BTreeMap::new();
    for frame in 1..=FRAMES {
        let b = batch(frame, frame);
        for d in &b.datasets {
            *expected.entry(d.dataset.to_string()).or_default() += d.events.len();
        }
        let rejected = sink
            .append(b)
            .await
            .unwrap_or_else(|e| panic!("append failed: {e}"));
        assert!(rejected.is_empty(), "the suite appends only valid events");
        sink.poll_commit()
            .await
            .unwrap_or_else(|e| panic!("poll_commit failed: {e}"));
    }
    sink.finalize_all()
        .await
        .unwrap_or_else(|e| panic!("finalize_all failed: {e}"));

    let Some(actual) = fixture.finalized_dataset_events() else {
        return;
    };
    assert_eq!(
        actual, expected,
        "events per dataset after finalize_all differ from what was appended (datasets were merged, lost, or duplicated)"
    );
}

/// Runs every check against `fixture`.
pub async fn run_all(fixture: &dyn SegmentSinkFixture, seed: u64) {
    watermark_is_monotonic_and_complete(fixture, seed).await;
    recovery_after_crash_at_every_failpoint(fixture).await;
    datasets_are_kept_apart(fixture).await;
}
