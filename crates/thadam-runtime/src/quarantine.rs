//! Rejects as events of the fixed quarantine schema, and the per-sink
//! quarantine writer.
//!
//! Schema (ARCHITECTURE.md section 8, plus the sink and dataset the
//! implementation plan lists): `_wal_epoch`, `_wal_pos`, `_record_index`,
//! `_ingest_time`, `source_id`, `topic`, `sink_id`, `dataset`,
//! `error_domain`, `error_code`, `reason`, `raw` (truncated at 64 KiB), and
//! `raw_truncated`. A reject raised by a sink after decoding has no raw bytes:
//! `raw` is null and `_record_index` and `source_id` are unknown.
//!
//! # Decoupled from the owner's cursor
//!
//! A reject becomes durable in the quarantine instance's small staging file
//! (see [`crate::staging`]) before the owning sink's cursor may pass it; the
//! quarantine Parquet segment is built from the same rows on its own rotation
//! cadence. [`Quarantine::push`] therefore returns only after the rows are
//! fsynced into staging, and the owner never has to wait for a quarantine
//! segment. Staging is the source of truth for rows that are not yet in a
//! durable segment: [`Quarantine::recover`] (also used to heal the quarantine
//! sink after a failed append) replays every staged row above the sink's
//! durable watermark into the sink, so a reject lands in exactly one durable
//! quarantine segment.

use std::io;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use thadam_core::{
    BudgetCategory, BudgetExceeded, ErrorDomain, Event, EventBatch, EventId, FieldDef, FieldType,
    QUARANTINE_DATASET, Record, SegmentBatch, SegmentSink, Shape, ShapeRef, SinkContext, Sym,
    ThadamError, TimeDomain, Times, Timestamp, Utf8Bytes, Value, WalEpoch, WalPos, WalRange, codes,
};
use thadam_transform::RejectedRaw;

use crate::failpoints::{self, Failpoints};
use crate::metrics::MetricEvent;
use crate::segment::simulated_crash;
use crate::staging::{StagedBatch, StagedRow, Staging};
use crate::status::{Component, add};

/// The staging file inside the quarantine instance's data directory.
const STAGING_FILE: &str = "rejects.staging";

/// Live staged bytes beyond which the open quarantine segment is closed early,
/// which keeps the staging file small however slowly segments rotate.
const STAGING_SOFT_LIMIT: u64 = 8 << 20;

/// Raw payloads are cut to this many bytes in quarantine.
pub const QUARANTINE_RAW_MAX: usize = 64 << 10;

/// The quarantine record shape.
pub fn quarantine_shape() -> ShapeRef {
    static SHAPE: OnceLock<ShapeRef> = OnceLock::new();
    SHAPE
        .get_or_init(|| {
            let f = |name: &str, ty: FieldType| FieldDef::new(name, ty);
            let fields = vec![
                f("_wal_epoch", FieldType::U64).required(),
                f("_wal_pos", FieldType::U64).required(),
                f("_record_index", FieldType::U32),
                f("_ingest_time", FieldType::Ts),
                f("source_id", FieldType::Str),
                f("topic", FieldType::Str),
                f("sink_id", FieldType::Str).required(),
                f("dataset", FieldType::Str),
                f("error_domain", FieldType::Str).required(),
                f("error_code", FieldType::Str).required(),
                f("reason", FieldType::Str).required(),
                f("raw", FieldType::Bytes),
                f("raw_truncated", FieldType::Bool).required(),
            ];
            match Shape::new(fields) {
                Ok(s) => ShapeRef::new(s),
                Err(e) => unreachable!("the quarantine shape is static and valid: {e}"),
            }
        })
        .clone()
}

/// One rejected record or event.
pub(crate) enum Rejected {
    /// The chain could not turn a raw record into an event.
    Raw(Box<RejectedRaw>),
    /// A sink refused a decoded event.
    Event {
        epoch: WalEpoch,
        pos: WalPos,
        ingest: Timestamp,
        dataset: Option<Sym>,
        error: ThadamError,
    },
}

impl Rejected {
    fn pos(&self) -> WalPos {
        match self {
            Rejected::Raw(r) => r.pos,
            Rejected::Event { pos, .. } => *pos,
        }
    }
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

fn text(s: &str) -> Value {
    Value::Str(Utf8Bytes::from(s))
}

fn to_row(r: Rejected, sink_id: &str, seq: u64) -> StagedRow {
    let (epoch, pos, idx, ingest, source, topic, dataset, error, raw) = match r {
        Rejected::Raw(r) => {
            let r = *r;
            let topic = r.attrs.get("topic").map(str::to_owned);
            (
                r.epoch,
                r.pos,
                Some(r.record_index),
                r.ingest_time,
                Some(r.source.as_str().to_owned()),
                topic,
                r.dataset,
                r.error,
                Some(r.payload),
            )
        }
        Rejected::Event {
            epoch,
            pos,
            ingest,
            dataset,
            error,
        } => (epoch, pos, None, ingest, None, None, dataset, error, None),
    };
    let (raw, truncated) = match raw {
        Some(p) if p.len() > QUARANTINE_RAW_MAX => (Some(p.slice(..QUARANTINE_RAW_MAX)), true),
        other => (other, false),
    };
    let hi = mix(epoch.get() ^ mix(pos.get()));
    let lo = mix(u64::from(idx.unwrap_or(u32::MAX)) ^ (seq << 32) ^ 0x5155_4152);
    StagedRow {
        event_id: (u128::from(hi) << 64) | u128::from(lo),
        epoch: epoch.get(),
        pos: pos.get(),
        record_index: idx,
        ingest_nanos: ingest.as_nanos(),
        source,
        topic,
        sink_id: sink_id.to_owned(),
        dataset: dataset.map(|d| d.to_string()),
        error_domain: error.domain.as_str().to_owned(),
        error_code: error.code.to_owned(),
        reason: error.message,
        raw,
        raw_truncated: truncated,
    }
}

fn row_to_event(row: StagedRow) -> Option<Event> {
    let opt = |s: Option<String>| s.map_or(Value::Null, |s| text(&s));
    let ingest = Timestamp::from_nanos(row.ingest_nanos);
    let values = vec![
        Value::U64(row.epoch),
        Value::U64(row.pos),
        row.record_index.map_or(Value::Null, Value::U32),
        Value::Ts(ingest),
        opt(row.source),
        opt(row.topic),
        text(&row.sink_id),
        opt(row.dataset),
        text(&row.error_domain),
        text(&row.error_code),
        text(&row.reason),
        row.raw.map_or(Value::Null, Value::bytes),
        Value::Bool(row.raw_truncated),
    ];
    let record = Record::new(quarantine_shape(), values).ok()?;
    Event::builder(
        EventId::from_u128(row.event_id),
        WalPos::new(row.pos),
        Times::new(ingest, TimeDomain::Unknown),
    )
    .payload(record)
    .build()
    .ok()
}

#[cfg(test)]
fn to_event(r: Rejected, sink_id: &str, seq: u64) -> Option<Event> {
    row_to_event(to_row(r, sink_id, seq))
}

/// The slice of `origin` a quarantine that already holds everything through
/// `floor` still has to cover, or `None` if there is none. It starts right
/// after `floor` (not at `origin.first()`) so the rows' ranges chain without
/// gaps: the segment catalog's watermark only advances over contiguous
/// coverage, and no reject lies in the gap or it would have been pushed.
fn span(origin: WalRange, floor: u64) -> Option<WalRange> {
    if origin.last().get() <= floor {
        return None;
    }
    let first = if floor == 0 {
        origin.first()
    } else {
        WalPos::new(floor + 1)
    };
    WalRange::new(origin.epoch(), first, origin.last()).ok()
}

fn staging_error(op: &'static str, path: &Path, e: &io::Error) -> ThadamError {
    let message = format!("quarantine staging {op} {}: {e}", path.display());
    if e.raw_os_error() == Some(rustix::io::Errno::NOSPC.raw_os_error()) {
        ThadamError::transient(ErrorDomain::Buffer, codes::BUFFER_IO, message)
    } else if e.kind() == io::ErrorKind::InvalidData {
        ThadamError::fatal(ErrorDomain::Buffer, codes::BUFFER_CORRUPT, message)
    } else {
        ThadamError::fatal(ErrorDomain::Sink, codes::SINK_IO, message)
    }
}

fn join_error(op: &'static str) -> ThadamError {
    ThadamError::fatal(
        ErrorDomain::Sink,
        codes::SINK_IO,
        format!("quarantine staging {op} task failed"),
    )
}

fn budget_error(e: &BudgetExceeded) -> ThadamError {
    ThadamError::transient(
        ErrorDomain::Buffer,
        codes::BUFFER_BUDGET_EXHAUSTED,
        format!(
            "quarantine staging needs {} bytes but the quarantine disk budget has {} of {:?} in use",
            e.requested, e.used, e.limit
        ),
    )
}

/// A sink's quarantine instance. Rejects are durable in staging once
/// [`Quarantine::push`] returns, so the sink's cursor may pass them.
pub(crate) struct Quarantine {
    pub sink: Box<dyn SegmentSink>,
    pub ctx: SinkContext,
    owner: String,
    component: Arc<Component>,
    failpoints: Failpoints,
    staging: Option<Staging>,
    /// Staging bytes currently charged to the quarantine disk budget.
    charged: u64,
    /// Rejects at or below this position are already staged or in a durable
    /// segment: a re-read (after a restart or rewind) must not stage them again.
    floor_pos: u64,
    /// The highest position appended to the quarantine sink.
    applied: u64,
    /// The quarantine sink's durable watermark.
    watermark: u64,
    /// The quarantine sink failed an operation and its open segment can no
    /// longer be trusted; rebuild it from staging before the next use.
    dirty: bool,
}

impl Quarantine {
    pub fn new(
        sink: Box<dyn SegmentSink>,
        ctx: SinkContext,
        owner: String,
        component: Arc<Component>,
        failpoints: Failpoints,
    ) -> Self {
        Quarantine {
            sink,
            ctx,
            owner,
            component,
            failpoints,
            staging: None,
            charged: 0,
            floor_pos: 0,
            applied: 0,
            watermark: 0,
            dirty: false,
        }
    }

    fn staging_path(&self) -> std::path::PathBuf {
        self.ctx.data_dir.join(STAGING_FILE)
    }

    /// Recovers the quarantine sink, reopens staging (cutting a torn tail),
    /// and replays every staged row the sink's durable segments do not cover
    /// into the sink. Returns the highest position quarantine holds (staged or
    /// durable): the owner may treat everything through it as handled.
    pub async fn recover(&mut self) -> Result<u64, ThadamError> {
        self.dirty = true;
        let durable = self.sink.recover(&self.ctx).await?.map_or(0, WalPos::get);
        self.watermark = self.watermark.max(durable);
        self.applied = durable;
        self.floor_pos = self.floor_pos.max(durable);

        self.staging = None;
        self.reconcile(0);
        let path = self.staging_path();
        let open_path = path.clone();
        let opened = tokio::task::spawn_blocking(move || Staging::open(&open_path))
            .await
            .map_err(|_| join_error("open"))?
            .map_err(|e| staging_error("open", &path, &e))?;
        if opened.torn_bytes > 0 {
            tracing::warn!(sink = %self.owner, bytes = opened.torn_bytes, "cut a torn tail off the quarantine staging file");
        }
        self.reconcile(opened.staging.bytes());
        self.staging = Some(opened.staging);
        self.release_staged().await;

        let mut replayed = 0usize;
        for batch in opened.batches {
            if batch.range.last().get() <= self.applied {
                continue;
            }
            let last = batch.range.last().get();
            let (kept, refused) = self.append_to_sink(batch).await?;
            self.floor_pos = self.floor_pos.max(last);
            replayed += kept + refused;
        }
        if replayed > 0 {
            tracing::info!(sink = %self.owner, rows = replayed, "replayed staged rejects into the quarantine segment");
        }
        self.dirty = false;
        Ok(self.floor_pos)
    }

    /// Stages `rejected` (all inside `origin`) durably, then appends them to
    /// the quarantine sink. Rejects at or below what quarantine already holds
    /// are skipped, so re-reading after a restart or rewind never duplicates
    /// them. Once this returns `Ok`, the rows survive a crash.
    pub async fn push(
        &mut self,
        origin: WalRange,
        clock_synced: bool,
        rejected: Vec<Rejected>,
    ) -> Result<(), ThadamError> {
        self.heal().await?;
        let floor = self.floor_pos;
        let rows: Vec<StagedRow> = rejected
            .into_iter()
            .filter(|r| r.pos().get() > floor)
            .enumerate()
            .map(|(i, r)| to_row(r, &self.owner, i as u64))
            .collect();
        let Some(range) = span(origin, floor) else {
            return Ok(());
        };
        if rows.is_empty() {
            return Ok(());
        }
        let batch = StagedBatch {
            range,
            clock_synced,
            rows,
        };
        self.stage(&batch).await?;
        self.floor_pos = range.last().get();
        if self.failpoints.hit(failpoints::QUARANTINE_AFTER_STAGE) {
            return Err(simulated_crash(failpoints::QUARANTINE_AFTER_STAGE));
        }
        let (kept, refused) = self.append_to_sink(batch).await?;
        add(&self.component.counters.quarantined, kept as u64);
        self.component.metrics().record(MetricEvent::Quarantined {
            sink: &self.owner,
            count: kept,
        });
        if refused > 0 {
            self.component
                .metrics()
                .record(MetricEvent::RejectsDropped {
                    sink: &self.owner,
                    count: refused,
                });
        }
        Ok(())
    }

    /// Appends a staged batch's rows to the quarantine sink, skipping what the
    /// sink already took. Returns `(kept, refused)`.
    async fn append_to_sink(&mut self, batch: StagedBatch) -> Result<(usize, usize), ThadamError> {
        let applied = self.applied;
        let Some(range) = span(batch.range, applied) else {
            return Ok((0, 0));
        };
        let events: Vec<Event> = batch
            .rows
            .into_iter()
            .filter(|r| r.pos > applied)
            .filter_map(row_to_event)
            .collect();
        let n = events.len();
        let appended = self
            .sink
            .append(SegmentBatch::single(
                QUARANTINE_DATASET,
                EventBatch::new(events, range, batch.clock_synced),
            ))
            .await;
        let refused = match appended {
            Ok(r) => r.len(),
            Err(e) => {
                self.dirty = true;
                return Err(e);
            }
        };
        if refused > 0 {
            tracing::error!(sink = %self.owner, count = refused, "quarantine refused rejects");
        }
        self.applied = range.last().get();
        Ok((n - refused.min(n), refused))
    }

    /// Appends the batch to the staging file and fsyncs it, charging the
    /// quarantine budget. If the live staged bytes are over the soft limit or
    /// the budget has no room, the open quarantine segment is closed first:
    /// once durable it frees the staged rows it covers.
    async fn stage(&mut self, batch: &StagedBatch) -> Result<(), ThadamError> {
        let path = self.staging_path();
        let frame = batch
            .encode_frame()
            .map_err(|e| staging_error("encode", &path, &e))?;
        let need = frame.len() as u64;
        let live = self.staging.as_ref().map_or(0, Staging::live_bytes);
        if live > 0 && live.saturating_add(need) > STAGING_SOFT_LIMIT {
            self.finalize().await?;
        }
        let budget = Arc::clone(&self.ctx.budget);
        if budget
            .try_reserve(BudgetCategory::Quarantine, need)
            .is_err()
        {
            self.finalize().await?;
            budget
                .try_reserve(BudgetCategory::Quarantine, need)
                .map_err(|e| budget_error(&e))?;
        }
        self.charged += need;
        let last = batch.range.last().get();
        self.with_staging("append", move |s| s.append_frame(&frame, last))
            .await
    }

    /// Runs blocking `op` on the staging file, then makes the budget charge
    /// match the file size whatever the outcome.
    async fn with_staging<T: Send + 'static>(
        &mut self,
        what: &'static str,
        op: impl FnOnce(&mut Staging) -> io::Result<T> + Send + 'static,
    ) -> Result<T, ThadamError> {
        let path = self.staging_path();
        let Some(mut staging) = self.staging.take() else {
            return Err(ThadamError::fatal(
                ErrorDomain::Sink,
                codes::SINK_IO,
                "quarantine staging is not open",
            ));
        };
        let (staging, result) = tokio::task::spawn_blocking(move || {
            let result = op(&mut staging);
            (staging, result)
        })
        .await
        .map_err(|_| join_error(what))?;
        self.reconcile(staging.bytes());
        self.staging = Some(staging);
        result.map_err(|e| staging_error(what, &path, &e))
    }

    fn reconcile(&mut self, bytes: u64) {
        let budget = &self.ctx.budget;
        if bytes > self.charged {
            budget.charge(BudgetCategory::Quarantine, bytes - self.charged);
        } else if bytes < self.charged {
            budget.release(BudgetCategory::Quarantine, self.charged - bytes);
        }
        self.charged = bytes;
    }

    /// Lets staging forget the rows a durable quarantine segment now covers.
    /// A failure only means the file stays larger for a while: the rows are
    /// filtered by the durable watermark on the next recovery.
    async fn release_staged(&mut self) {
        let durable = self.watermark;
        if !self.staging.as_ref().is_some_and(|s| s.releasable(durable)) {
            return;
        }
        if let Err(e) = self
            .with_staging("release", move |s| s.release_through(durable))
            .await
        {
            tracing::warn!(sink = %self.owner, error = %e, "could not shrink the quarantine staging file");
        }
    }

    async fn heal(&mut self) -> Result<(), ThadamError> {
        if self.dirty {
            self.recover().await?;
        }
        Ok(())
    }

    async fn settle(&mut self, wm: Option<WalPos>) {
        if let Some(w) = wm {
            self.watermark = self.watermark.max(w.get());
        }
        self.release_staged().await;
    }

    pub async fn poll(&mut self) -> Result<(), ThadamError> {
        self.heal().await?;
        match self.sink.poll_commit().await {
            Ok(wm) => {
                self.settle(wm).await;
                Ok(())
            }
            Err(e) => {
                self.dirty = true;
                Err(e)
            }
        }
    }

    pub async fn finalize(&mut self) -> Result<(), ThadamError> {
        self.heal().await?;
        match self.sink.finalize_all().await {
            Ok(wm) => {
                self.settle(wm).await;
                Ok(())
            }
            Err(e) => {
                self.dirty = true;
                Err(e)
            }
        }
    }

    pub fn cursor_committed(&mut self, pos: u64) {
        let p = pos.min(self.watermark);
        if p > 0 {
            self.sink.cursor_committed(WalPos::new(p));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::NoopMetrics;
    use crate::status::ComponentKind;
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::sync::Mutex;
    use thadam_core::{Attributes, DiskBudget, Reject, Shutdown, SinkId, SourceId, SystemClock};

    #[derive(Default)]
    struct FakeState {
        durable: Vec<(WalRange, Vec<u64>)>,
        open: Vec<(WalRange, Vec<u64>)>,
        fail_appends: u32,
    }

    impl FakeState {
        fn durable_positions(&self) -> Vec<u64> {
            self.durable.iter().flat_map(|(_, p)| p.clone()).collect()
        }
        fn open_positions(&self) -> Vec<u64> {
            self.open.iter().flat_map(|(_, p)| p.clone()).collect()
        }
    }

    /// A quarantine sink that keeps `open` rows in memory (lost on `recover`,
    /// like a crash) and moves them to `durable` on `finalize_all`.
    #[derive(Clone, Default)]
    struct Fake(Arc<Mutex<FakeState>>);

    impl Fake {
        fn state(&self) -> std::sync::MutexGuard<'_, FakeState> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    }

    #[async_trait]
    impl SegmentSink for Fake {
        async fn recover(&mut self, _ctx: &SinkContext) -> Result<Option<WalPos>, ThadamError> {
            let mut st = self.state();
            st.open.clear();
            Ok(st.durable.last().map(|(r, _)| r.last()))
        }

        async fn append(
            &mut self,
            batch: thadam_core::SegmentBatch,
        ) -> Result<Vec<Reject>, ThadamError> {
            let mut st = self.state();
            if st.fail_appends > 0 {
                st.fail_appends -= 1;
                return Err(ThadamError::transient(
                    ErrorDomain::Sink,
                    codes::SINK_IO,
                    "injected append failure",
                ));
            }
            let last = st.open.last().or(st.durable.last()).map(|(r, _)| r.last());
            assert!(
                last.is_none_or(|l| batch.origin.first() > l),
                "non-monotonic append {} after {last:?}",
                batch.origin
            );
            let pos = batch.events().map(|(_, e)| e.wal_pos().get()).collect();
            st.open.push((batch.origin, pos));
            Ok(Vec::new())
        }

        async fn poll_commit(&mut self) -> Result<Option<WalPos>, ThadamError> {
            Ok(None)
        }

        async fn finalize_all(&mut self) -> Result<Option<WalPos>, ThadamError> {
            let mut st = self.state();
            let open = std::mem::take(&mut st.open);
            st.durable.extend(open);
            Ok(st.durable.last().map(|(r, _)| r.last()))
        }

        fn cursor_committed(&mut self, _pos: WalPos) {}
    }

    fn quarantine(dir: &std::path::Path, sink: &Fake, budget: &Arc<DiskBudget>) -> Quarantine {
        let clock: Arc<dyn thadam_core::Clock> = Arc::new(SystemClock::new());
        let ctx = SinkContext::new(
            SinkId::new("quarantine.pq"),
            Shutdown::new(),
            Arc::clone(&clock),
            dir.to_owned(),
            Arc::clone(budget),
        );
        let component = Component::new(
            "sink:pq".into(),
            ComponentKind::SegmentSink,
            Arc::new(NoopMetrics),
            clock,
        );
        Quarantine::new(
            Box::new(sink.clone()),
            ctx,
            "pq".into(),
            component,
            Failpoints::default(),
        )
    }

    fn range(first: u64, last: u64) -> WalRange {
        WalRange::new(WalEpoch::new(1), WalPos::new(first), WalPos::new(last)).unwrap()
    }

    fn reject(pos: u64) -> Rejected {
        Rejected::Event {
            epoch: WalEpoch::new(1),
            pos: WalPos::new(pos),
            ingest: Timestamp::from_nanos(1),
            dataset: Some(Sym::from("d")),
            error: ThadamError::permanent(ErrorDomain::Sink, codes::SINK_WRITE_FAILED, "bad row"),
        }
    }

    fn staged_positions(dir: &std::path::Path) -> Vec<u64> {
        Staging::open(&dir.join(STAGING_FILE))
            .unwrap()
            .batches
            .iter()
            .flat_map(|b| b.rows.iter().map(|r| r.pos))
            .collect()
    }

    fn budget(limit: u64) -> Arc<DiskBudget> {
        Arc::new(DiskBudget::new().with_limit(BudgetCategory::Quarantine, limit))
    }

    #[tokio::test]
    async fn rows_are_in_staging_before_the_segment_and_survive_losing_it() {
        let dir = tempfile::tempdir().unwrap();
        let (fake, budget) = (Fake::default(), budget(1 << 20));
        let mut q = quarantine(dir.path(), &fake, &budget);
        assert_eq!(q.recover().await.unwrap(), 0);
        q.push(range(3, 6), true, vec![reject(5)]).await.unwrap();
        assert_eq!(staged_positions(dir.path()), vec![5]);
        assert_eq!(fake.state().open_positions(), vec![5]);

        // "Crash": the open segment is lost; a new instance recovers from staging.
        let mut q = quarantine(dir.path(), &fake, &budget);
        assert_eq!(q.recover().await.unwrap(), 6);
        assert_eq!(fake.state().open_positions(), vec![5]);
        assert!(fake.state().durable_positions().is_empty());

        // Re-reading the same frames after the restart stages nothing new.
        q.push(range(3, 6), true, vec![reject(5)]).await.unwrap();
        assert_eq!(staged_positions(dir.path()), vec![5]);
        assert_eq!(fake.state().open_positions(), vec![5]);
    }

    #[tokio::test]
    async fn recovery_skips_staged_rows_a_durable_segment_covers() {
        let dir = tempfile::tempdir().unwrap();
        let (fake, budget) = (Fake::default(), budget(1 << 20));
        let mut q = quarantine(dir.path(), &fake, &budget);
        q.recover().await.unwrap();
        q.push(range(1, 4), true, vec![reject(2)]).await.unwrap();
        q.push(range(5, 9), true, vec![reject(7)]).await.unwrap();
        // The segment holding the first batch became durable, but the crash
        // came before staging shrank (simulated: durable state set directly).
        {
            let mut st = fake.state();
            let first = st.open.remove(0);
            st.open.clear();
            st.durable.push(first);
        }
        let mut q = quarantine(dir.path(), &fake, &budget);
        assert_eq!(q.recover().await.unwrap(), 9);
        let st = fake.state();
        assert_eq!(st.durable_positions(), vec![2]);
        assert_eq!(st.open_positions(), vec![7], "each reject exactly once");
    }

    #[tokio::test]
    async fn finalizing_empties_staging_and_the_budget_charge() {
        let dir = tempfile::tempdir().unwrap();
        let (fake, budget) = (Fake::default(), budget(1 << 20));
        let mut q = quarantine(dir.path(), &fake, &budget);
        q.recover().await.unwrap();
        let base = budget.used(BudgetCategory::Quarantine);
        q.push(range(1, 4), true, vec![reject(2), reject(3)])
            .await
            .unwrap();
        let file = std::fs::metadata(dir.path().join(STAGING_FILE))
            .unwrap()
            .len();
        assert!(file > 8);
        assert_eq!(budget.used(BudgetCategory::Quarantine), file);
        assert!(budget.used(BudgetCategory::Quarantine) > base);

        q.finalize().await.unwrap();
        assert_eq!(
            std::fs::metadata(dir.path().join(STAGING_FILE))
                .unwrap()
                .len(),
            8
        );
        assert_eq!(budget.used(BudgetCategory::Quarantine), 8);
        assert_eq!(budget.accounting_errors(), 0);
        assert_eq!(fake.state().durable_positions(), vec![2, 3]);
    }

    #[tokio::test]
    async fn a_failed_append_is_healed_from_staging_without_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let (fake, budget) = (Fake::default(), budget(1 << 20));
        let mut q = quarantine(dir.path(), &fake, &budget);
        q.recover().await.unwrap();
        fake.state().fail_appends = 1;
        let e = q
            .push(range(1, 4), true, vec![reject(2)])
            .await
            .unwrap_err();
        assert!(e.is_transient());
        // The rows were fsynced before the sink failed.
        assert_eq!(staged_positions(dir.path()), vec![2]);
        assert!(fake.state().open_positions().is_empty());

        // The driver re-reads and pushes again: staging is not duplicated and
        // the sink gets the row once, from the replay.
        q.push(range(1, 4), true, vec![reject(2)]).await.unwrap();
        assert_eq!(staged_positions(dir.path()), vec![2]);
        assert_eq!(fake.state().open_positions(), vec![2]);
        q.push(range(5, 6), true, vec![reject(6)]).await.unwrap();
        assert_eq!(fake.state().open_positions(), vec![2, 6]);
    }

    #[tokio::test]
    async fn ranges_chain_without_gaps_across_pushes() {
        let dir = tempfile::tempdir().unwrap();
        let (fake, budget) = (Fake::default(), budget(1 << 20));
        let mut q = quarantine(dir.path(), &fake, &budget);
        q.recover().await.unwrap();
        q.push(range(3, 4), true, vec![reject(3)]).await.unwrap();
        q.finalize().await.unwrap();
        q.push(range(20, 22), true, vec![reject(21)]).await.unwrap();
        let st = fake.state();
        let ranges: Vec<_> = st
            .durable
            .iter()
            .chain(st.open.iter())
            .map(|(r, _)| (r.first().get(), r.last().get()))
            .collect();
        assert_eq!(ranges, vec![(3, 4), (5, 22)]);
    }

    #[tokio::test]
    async fn a_full_budget_closes_the_open_segment_to_make_room() {
        let dir = tempfile::tempdir().unwrap();
        let fake = Fake::default();
        let probe = self::budget(1 << 20);
        let mut q = quarantine(dir.path(), &fake, &probe);
        q.recover().await.unwrap();
        q.push(range(1, 2), true, vec![reject(1)]).await.unwrap();
        let one = probe.used(BudgetCategory::Quarantine);

        let dir = tempfile::tempdir().unwrap();
        let fake = Fake::default();
        let tight = self::budget(one + 8);
        let mut q = quarantine(dir.path(), &fake, &tight);
        q.recover().await.unwrap();
        q.push(range(1, 2), true, vec![reject(1)]).await.unwrap();
        q.push(range(3, 4), true, vec![reject(3)]).await.unwrap();
        let st = fake.state();
        assert_eq!(st.durable_positions(), vec![1], "closed to free staging");
        assert_eq!(st.open_positions(), vec![3]);
        assert!(!tight.is_over_limit(BudgetCategory::Quarantine));
    }

    #[tokio::test]
    async fn no_budget_for_staging_is_a_transient_error_without_row_text() {
        let dir = tempfile::tempdir().unwrap();
        let (fake, budget) = (Fake::default(), budget(8));
        let mut q = quarantine(dir.path(), &fake, &budget);
        q.recover().await.unwrap();
        let secret = Rejected::Event {
            epoch: WalEpoch::new(1),
            pos: WalPos::new(2),
            ingest: Timestamp::from_nanos(1),
            dataset: None,
            error: ThadamError::permanent(
                ErrorDomain::Sink,
                codes::SINK_WRITE_FAILED,
                "SECRET-PAYLOAD-TEXT",
            ),
        };
        let e = q.push(range(1, 4), true, vec![secret]).await.unwrap_err();
        assert!(e.is_transient());
        assert_eq!(e.code, codes::BUFFER_BUDGET_EXHAUSTED);
        assert!(!e.message.contains("SECRET"), "{}", e.message);
        assert!(staged_positions(dir.path()).is_empty());
        assert_eq!(budget.used(BudgetCategory::Quarantine), 8);
    }

    #[tokio::test]
    async fn a_foreign_staging_file_fails_recovery_without_its_content() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(STAGING_FILE),
            b"SECRET-PAYLOAD-TEXT, not staging",
        )
        .unwrap();
        let (fake, budget) = (Fake::default(), budget(1 << 20));
        let mut q = quarantine(dir.path(), &fake, &budget);
        let e = q.recover().await.unwrap_err();
        assert_eq!(e.retry, thadam_core::Retry::Fatal);
        assert_eq!(e.code, codes::BUFFER_CORRUPT);
        assert!(!e.message.contains("SECRET"), "{}", e.message);
    }

    #[test]
    fn raw_rejects_become_quarantine_rows_with_truncation() {
        let mut attrs = Attributes::new();
        attrs.insert("topic", "a/b");
        let raw = RejectedRaw {
            error: ThadamError::permanent(
                ErrorDomain::Decode,
                codes::DECODE_INVALID_PAYLOAD,
                "bad",
            ),
            dataset: Some(Sym::from("d")),
            source: SourceId::new("src"),
            epoch: WalEpoch::new(9),
            pos: WalPos::new(4),
            record_index: 2,
            sample_idx: None,
            ingest_time: Timestamp::from_nanos(5),
            attrs,
            payload: Bytes::from(vec![7u8; QUARANTINE_RAW_MAX + 10]),
        };
        let e = to_event(Rejected::Raw(Box::new(raw)), "sink", 0).unwrap();
        let rec = e.payload().as_record().unwrap();
        assert_eq!(rec.get("_wal_pos"), Some(&Value::U64(4)));
        assert_eq!(rec.get("topic"), Some(&text("a/b")));
        assert_eq!(
            rec.get("error_code"),
            Some(&text(codes::DECODE_INVALID_PAYLOAD))
        );
        assert_eq!(rec.get("raw_truncated"), Some(&Value::Bool(true)));
        match rec.get("raw") {
            Some(Value::Bytes(b)) => assert_eq!(b.len(), QUARANTINE_RAW_MAX),
            other => panic!("raw is {other:?}"),
        }
        assert_eq!(e.wal_pos(), WalPos::new(4));
    }
}
