//! A sink-level reject (one only known once `append` returns) must be staged
//! for quarantine before the rows of its batch can be durable in the sink.
//! Otherwise a crash in between leaves a sink watermark the restart treats as
//! complete, the re-read skips the sink append, and the reject is never
//! regenerated.
//!
//! The sink here rotates (makes its rows durable) inside `append` unless the
//! runtime asked it to defer, which is the worst case for the ordering.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use async_trait::async_trait;
use common::*;
use thadam_core::{
    BuildContext, ErrorDomain, Reject, SegmentBatch, SegmentSink, SegmentSinkFactory, SinkContext,
    ThadamError, WalPos, WalRange, codes,
};
use thadam_runtime::{ExitStatus, Pipeline, failpoints};
use thadam_testkit::TempDataDir;

/// Rejects the event whose `n` is 13; keeps every other row.
struct RejectingSink {
    store: RecStore,
    defer: bool,
    deferred: bool,
    buffered: Vec<(WalRange, Vec<Row>)>,
    last: Option<WalPos>,
}

impl RejectingSink {
    fn finalize(&mut self) -> Option<WalPos> {
        let (first, last) = (self.buffered.first()?.0, self.buffered.last()?.0);
        let range = WalRange::new(first.epoch(), first.first(), last.last()).unwrap();
        let rows = self.buffered.drain(..).flat_map(|(_, r)| r).collect();
        lock(&self.store.0).files.push((range, rows));
        self.deferred = false;
        Some(range.last())
    }
}

#[async_trait]
impl SegmentSink for RejectingSink {
    async fn recover(&mut self, _ctx: &SinkContext) -> Result<Option<WalPos>, ThadamError> {
        self.buffered.clear();
        self.deferred = false;
        self.last = lock(&self.store.0).files.last().map(|(r, _)| r.last());
        Ok(self.last)
    }

    async fn append(&mut self, batch: SegmentBatch) -> Result<Vec<Reject>, ThadamError> {
        if self.deferred {
            self.finalize();
        }
        let mut rejects = Vec::new();
        let mut rows = Vec::new();
        for (i, (_, e)) in batch.events().enumerate() {
            if event_n(e) == Some(13) {
                rejects.push(Reject::new(
                    u32::try_from(i).unwrap(),
                    ThadamError::permanent(
                        ErrorDomain::Schema,
                        codes::SCHEMA_NARROWING,
                        "value is out of range",
                    ),
                ));
            } else {
                rows.push(Row::of(e));
            }
        }
        self.last = Some(batch.origin.last());
        self.buffered.push((batch.origin, rows));
        if self.defer {
            self.deferred = true;
        } else {
            self.finalize();
        }
        Ok(rejects)
    }

    async fn poll_commit(&mut self) -> Result<Option<WalPos>, ThadamError> {
        Ok(self.finalize())
    }

    async fn finalize_all(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.finalize();
        Ok(lock(&self.store.0).files.last().map(|(r, _)| r.last()))
    }

    fn cursor_committed(&mut self, _pos: WalPos) {}

    fn defer_durability(&mut self, defer: bool) {
        self.defer = defer;
    }
}

struct RejectingFactory(RecStores);

impl SegmentSinkFactory for RejectingFactory {
    fn kind(&self) -> &'static str {
        "rejseg"
    }
    fn config_schema(&self) -> serde_json::Value {
        serde_json::json!({"type": "object"})
    }
    fn validate(&self, _cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        Ok(())
    }
    fn build(
        &self,
        _cfg: serde_json::Value,
        ctx: &BuildContext,
    ) -> Result<Box<dyn SegmentSink>, ThadamError> {
        Ok(Box::new(RejectingSink {
            store: self.0.store(&ctx.component_id),
            defer: false,
            deferred: false,
            buffered: Vec::new(),
            last: None,
        }))
    }
}

/// Crash after `append` returned the sink reject and before it was staged
/// (`segment::after_append_before_stage`). The sink rotated inside `append`
/// unless it was told to defer, so before the fix the batch was already
/// durable and the restart skipped it. Now the restart regenerates the
/// reject, and every other row lands in the sink once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_between_a_sink_reject_and_its_staging_loses_nothing() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let y = yaml(dir.path(), "  - { id: out, kind: rejseg }", "");
    let registry = || {
        let mut r = kit.registry();
        r.segment_sink(Box::new(RejectingFactory(kit.rec.clone())));
        r
    };

    kit.sources
        .scripted("src", [json_n(1), json_n(13), json_n(2)]);
    let mut opts = fast_opts();
    opts.stop_when_sources_finish = true;
    let fp = opts.failpoints.clone();
    fp.arm(failpoints::SEGMENT_AFTER_APPEND_BEFORE_STAGE, 1);
    let p = Pipeline::build(spec(&y), &registry(), opts).unwrap();
    let first = p.run(thadam_core::Shutdown::new()).await.unwrap();
    assert_eq!(
        first.exit,
        ExitStatus::RuntimeFailure,
        "{:#?}",
        first.status
    );
    assert!(!fp.is_armed(failpoints::SEGMENT_AFTER_APPEND_BEFORE_STAGE));
    assert!(
        kit.quarantine_rows("out").is_empty(),
        "the crash came before staging"
    );

    kit.sources.scripted("src", std::iter::empty());
    let mut opts = fast_opts();
    opts.stop_when_sources_finish = true;
    let p = Pipeline::build(spec(&y), &registry(), opts).unwrap();
    let second = p.run(thadam_core::Shutdown::new()).await.unwrap();
    assert_eq!(second.exit, ExitStatus::Ok, "{:#?}", second.status);
    assert_eq!(
        kit.quarantine_rows("out").len(),
        1,
        "the sink-level reject, exactly once"
    );
    assert_eq!(
        kit.rec.store("out").ns(),
        vec![1, 2],
        "no row lost or doubled"
    );
    assert!(cursor(&second.status, "out") >= 1);
}
