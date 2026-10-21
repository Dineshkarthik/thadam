//! The segment-sink driver.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use thadam_core::{
    DatasetEvents, ErrorDomain, Event, Retry, SegmentBatch, SegmentSink, Shutdown, SinkContext,
    Sym, ThadamError, Timestamp, WalPos, WalRange, codes,
};
use thadam_wal::{CursorState, Wal};
use tokio::sync::{oneshot, watch};

use crate::backoff::Backoff;
use crate::chain::wal_error;
use crate::failpoints::{self, Failpoints};
use crate::lane::{ChainBatch, LaneReceiver, Rewind};
use crate::metrics::MetricEvent;
use crate::options::BackoffPolicy;
use crate::quarantine::{Quarantine, Rejected};
use crate::status::{Component, PipelineHandle, add};

pub(crate) struct DriverOpts {
    pub poll_interval: Duration,
    pub commit_interval: Duration,
    pub retry: BackoffPolicy,
    pub failpoints: Failpoints,
}

pub(crate) fn simulated_crash(point: &str) -> ThadamError {
    ThadamError::fatal(
        ErrorDomain::Sink,
        codes::SINK_IO,
        format!("simulated crash at failpoint {point}"),
    )
}

/// What the driver needs to know about each event after the batch moved into
/// the sink, to quarantine sink rejects.
pub(crate) struct EventMeta {
    pub pos: WalPos,
    pub ingest: Timestamp,
    pub dataset: Sym,
}

/// Moves a chain batch's events out, grouped by dataset in declaration order.
/// `meta` lists the events in the order [`SegmentBatch::events`] yields them,
/// which is how a sink's [`thadam_core::Reject::index`] addresses them.
pub(crate) fn split(batch: &mut ChainBatch) -> (Vec<DatasetEvents>, Vec<EventMeta>) {
    let n = batch.datasets.iter().map(|d| d.batch.len()).sum();
    let mut meta = Vec::with_capacity(n);
    let datasets = batch
        .datasets
        .iter_mut()
        .map(|d| {
            let events = std::mem::take(&mut d.batch.events);
            meta.extend(events.iter().map(|e| EventMeta {
                pos: e.wal_pos(),
                ingest: e.times().ingest(),
                dataset: d.dataset.clone(),
            }));
            DatasetEvents::new(d.dataset.clone(), events)
        })
        .collect();
    (datasets, meta)
}

/// Concatenates a chain batch's per-dataset events in declaration order, for
/// batch sinks (whose contract carries no dataset).
pub(crate) fn merge(batch: &mut ChainBatch) -> (Vec<Event>, Vec<EventMeta>) {
    let (datasets, meta) = split(batch);
    (datasets.into_iter().flat_map(|d| d.events).collect(), meta)
}

/// Drops every event at or before `skip`, keeping `meta` aligned.
fn trim_through(datasets: &mut [DatasetEvents], meta: &mut Vec<EventMeta>, skip: u64) {
    for d in datasets {
        d.events.retain(|e| e.wal_pos().get() > skip);
    }
    meta.retain(|m| m.pos.get() > skip);
}

/// The chain's hold after each batch the driver received since the last
/// rewind, and not yet passed by the sink's watermark: `(last frame, hold)`.
#[derive(Default)]
struct HoldBack {
    batches: VecDeque<(u64, Option<u64>)>,
}

impl HoldBack {
    fn push(&mut self, last: u64, hold: Option<u64>) {
        self.batches.push_back((last, hold));
    }

    fn clear(&mut self) {
        self.batches.clear();
    }

    /// The highest cursor the open windows allow once the sink holds
    /// everything through `watermark`.
    ///
    /// A restart re-reads from the cursor to rebuild every window whose
    /// aggregate the sink does not hold yet: the open ones, and those closed
    /// after the watermark. Let `p` be the last batch the watermark covers.
    /// Those windows were open after `p`, so the earliest frame they need is
    /// the chain's hold after `p`, or they opened later still, so after `p`'s
    /// last frame. The cursor stops one short of the first and never passes
    /// the second. With no such batch on record the cursor stays at
    /// `committed`.
    fn cap(&mut self, watermark: u64, committed: u64) -> u64 {
        while self
            .batches
            .get(1)
            .is_some_and(|(last, _)| *last <= watermark)
        {
            self.batches.pop_front();
        }
        match self.batches.front() {
            Some((last, hold)) if *last <= watermark => hold.map_or(*last, |h| h.saturating_sub(1)),
            _ => committed,
        }
    }
}

/// Sleeps for the next backoff delay; `false` if `stop` triggered first.
pub(crate) async fn pause(backoff: &mut Backoff, stop: &Shutdown) -> bool {
    tokio::select! {
        () = tokio::time::sleep(backoff.next_delay()) => true,
        () = stop.triggered() => false,
    }
}

/// Awaits `$op` (re-evaluated per attempt) until it succeeds, retrying
/// transient errors with backoff and marking `$component` degraded meanwhile.
/// Yields `Ok(None)` if `$stop` triggered. A macro rather than a function
/// taking an async closure, which would not be `Send`.
macro_rules! retry_transient {
    ($component:expr, $policy:expr, $stop:expr, $what:expr, $op:expr) => {{
        let mut backoff = $crate::backoff::Backoff::new($policy);
        loop {
            match $op.await {
                Ok(v) => break Ok(Some(v)),
                Err(e) if e.retry == thadam_core::Retry::Transient => {
                    tracing::warn!(component = %$component.id, error = %e, "{} failed; retrying", $what);
                    $component.degraded(&e);
                    if !$crate::segment::pause(&mut backoff, $stop).await {
                        break Ok(None);
                    }
                }
                Err(e) => break Err(e),
            }
        }
    }};
}
pub(crate) use retry_transient;

pub(crate) struct SegmentDriver {
    pub sink_id: String,
    pub cursor: String,
    pub component: Arc<Component>,
    pub sink: Box<dyn SegmentSink>,
    pub ctx: SinkContext,
    pub quarantine: Option<Quarantine>,
    pub wal: Wal,
    pub handle: PipelineHandle,
    pub rx: LaneReceiver,
    pub rewind: watch::Sender<Rewind>,
    pub start: Option<oneshot::Sender<u64>>,
    pub opts: DriverOpts,
    /// Ends startup retries once shutdown begins.
    pub stopping: Shutdown,
    /// Ends other retries (forced shutdown only).
    pub force: Shutdown,
    /// The sink is configured `start_from: latest`, so a cursor ahead of an
    /// empty sink is expected until its first data arrives.
    pub start_latest: bool,
    /// The sink's chain has a `window` step: its cursor trails the open
    /// windows (see [`HoldBack::cap`]) and a restart re-reads from
    /// the cursor instead of committing up to what the sink recovered.
    pub holds_back: bool,
    committed: u64,
    watermark: u64,
    seen_through: u64,
    skip_through: u64,
    generation: u64,
    last_commit: Instant,
    holds: HoldBack,
}

impl SegmentDriver {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sink_id: String,
        cursor: String,
        component: Arc<Component>,
        mut sink: Box<dyn SegmentSink>,
        ctx: SinkContext,
        quarantine: Option<Quarantine>,
        wal: Wal,
        handle: PipelineHandle,
        rx: LaneReceiver,
        rewind: watch::Sender<Rewind>,
        start: oneshot::Sender<u64>,
        opts: DriverOpts,
        stopping: Shutdown,
        force: Shutdown,
    ) -> Self {
        // Rejects the sink returns from `append` are staged before the rows
        // that produced them may become durable (see `on_batch`).
        sink.defer_durability(true);
        SegmentDriver {
            sink_id,
            cursor,
            component,
            sink,
            ctx,
            quarantine,
            wal,
            handle,
            rx,
            rewind,
            start: Some(start),
            opts,
            stopping,
            force,
            start_latest: false,
            holds_back: false,
            committed: 0,
            watermark: 0,
            seen_through: 0,
            skip_through: 0,
            generation: 0,
            last_commit: Instant::now(),
            holds: HoldBack::default(),
        }
    }

    async fn recover_sink(&mut self) -> Result<Option<u64>, ThadamError> {
        let (sink, ctx) = (&mut self.sink, &self.ctx);
        let stop = if self.start.is_some() {
            &self.stopping
        } else {
            &self.force
        };
        let r: Result<Option<_>, ThadamError> = retry_transient!(
            self.component,
            self.opts.retry,
            stop,
            "recover",
            sink.recover(ctx)
        );
        let r = r?;
        Ok(r.map(|m| m.map_or(0, WalPos::get)))
    }

    /// Startup recovery (ARCHITECTURE.md section 11, step 4).
    async fn recover(&mut self) -> Result<bool, ThadamError> {
        let Some(m) = self.recover_sink().await? else {
            return Ok(false);
        };
        self.component
            .set_delivery(thadam_core::DeliveryClass::IdempotentBatch);
        self.component
            .metrics()
            .record(MetricEvent::SinkDeliveryClass {
                sink: &self.sink_id,
                class: thadam_core::DeliveryClass::IdempotentBatch.as_str(),
            });
        let mut q = 0;
        if let Some(quar) = &mut self.quarantine {
            let r: Result<Option<u64>, ThadamError> = retry_transient!(
                self.component,
                self.opts.retry,
                &self.stopping,
                "quarantine recover",
                quar.recover()
            );
            let r = r?;
            match r {
                Some(v) => q = v,
                None => return Ok(false),
            }
        }
        let c0 = self
            .wal
            .cursor(&self.cursor)
            .map_or(0, |c| c.committed.get());
        let durable = self.wal.durable_end().map_or(0, WalPos::get);
        let m = if m > durable {
            tracing::warn!(sink = %self.sink_id, recovered = m, durable, "sink is ahead of the durable wal end");
            durable
        } else {
            m
        };
        self.committed = c0;
        self.watermark = m.max(c0);
        self.seen_through = c0;
        if m > c0 {
            // With open windows the chain must re-read from the cursor to
            // rebuild them, so the output the sink already holds is skipped
            // rather than the cursor moved past it.
            if !self.holds_back && (self.quarantine.is_none() || q >= m) {
                self.commit_to(m).await?;
                self.seen_through = m;
            } else {
                self.skip_through = m;
            }
        } else if m < c0 {
            if m == 0 && self.start_latest {
                tracing::info!(sink = %self.sink_id, cursor = c0, "sink has no data yet; it starts after its cursor (start_from: latest)");
            } else {
                tracing::warn!(sink = %self.sink_id, recovered = m, cursor = c0, "sink recovered behind its cursor");
            }
        }
        self.handle.set_cursor(
            &self.sink_id,
            &self.cursor,
            CursorState::at(WalPos::new(self.committed)),
        );
        let (lag_bytes, lag_secs) = self.wal.cursor_lag(&self.cursor);
        self.component.metrics().record(MetricEvent::SinkLag {
            sink: &self.sink_id,
            bytes: lag_bytes,
            seconds: lag_secs,
        });
        if self.committed > 0 {
            self.sink.cursor_committed(WalPos::new(self.committed));
            if let Some(q) = &mut self.quarantine {
                q.cursor_committed(self.committed);
            }
        }
        tracing::info!(
            sink = %self.sink_id, cursor = self.committed, recovered = m,
            skip_through = self.skip_through, "segment sink recovered"
        );
        if let Some(start) = self.start.take() {
            let _ = start.send(self.committed);
        }
        Ok(true)
    }

    async fn commit_to(&mut self, target: u64) -> Result<(), ThadamError> {
        if self
            .opts
            .failpoints
            .hit(failpoints::SEGMENT_BEFORE_CURSOR_COMMIT)
        {
            return Err(simulated_crash(failpoints::SEGMENT_BEFORE_CURSOR_COMMIT));
        }
        let state = CursorState::at(WalPos::new(target));
        self.wal
            .commit_cursor(&self.cursor, state)
            .await
            .map_err(|e| wal_error(&e))?;
        self.committed = target;
        self.last_commit = Instant::now();
        self.handle.set_cursor(&self.sink_id, &self.cursor, state);
        self.component
            .metrics()
            .record(MetricEvent::CursorCommitted {
                sink: &self.sink_id,
                pos: WalPos::new(target),
            });
        let (lag_bytes, lag_secs) = self.wal.cursor_lag(&self.cursor);
        self.component.metrics().record(MetricEvent::SinkLag {
            sink: &self.sink_id,
            bytes: lag_bytes,
            seconds: lag_secs,
        });
        if self
            .opts
            .failpoints
            .hit(failpoints::SEGMENT_BEFORE_CURSOR_COMMITTED)
        {
            return Err(simulated_crash(failpoints::SEGMENT_BEFORE_CURSOR_COMMITTED));
        }
        self.sink.cursor_committed(WalPos::new(target));
        if let Some(q) = &mut self.quarantine {
            q.cursor_committed(target);
        }
        Ok(())
    }

    async fn maybe_commit(&mut self, force: bool) -> Result<(), ThadamError> {
        let mut target = self.watermark.min(self.seen_through);
        if self.holds_back {
            target = target.min(self.holds.cap(self.watermark, self.committed));
        }
        if target <= self.committed {
            return Ok(());
        }
        if !force && self.last_commit.elapsed() < self.opts.commit_interval {
            return Ok(());
        }
        match self.commit_to(target).await {
            Err(e) if e.retry == Retry::Transient => {
                self.component.degraded(&e);
                Ok(())
            }
            other => other,
        }
    }

    fn note_watermark(&mut self, wm: Option<WalPos>) {
        if let Some(w) = wm {
            self.watermark = self.watermark.max(w.get());
        }
    }

    async fn on_tick(&mut self) -> Result<(), ThadamError> {
        match self.sink.poll_commit().await {
            Ok(wm) => self.note_watermark(wm),
            Err(e) if e.retry == Retry::Transient => self.component.degraded(&e),
            Err(e) => return Err(e),
        }
        if let Some(q) = &mut self.quarantine {
            match q.poll().await {
                Ok(()) => {}
                Err(e) if e.retry == Retry::Transient => self.component.degraded(&e),
                Err(e) => return Err(e),
            }
        }
        self.maybe_commit(false).await
    }

    /// The sink lost an append: reset it to its durable state and have the
    /// chain re-read everything after the cursor.
    async fn rewind_after(&mut self, e: &ThadamError) -> Result<bool, ThadamError> {
        tracing::warn!(sink = %self.sink_id, error = %e, "append failed; recovering the sink");
        self.component.degraded(e);
        let mut backoff = Backoff::new(self.opts.retry);
        if !pause(&mut backoff, &self.force).await {
            return Ok(false);
        }
        let Some(m) = self.recover_sink().await? else {
            return Ok(false);
        };
        self.skip_through = self.skip_through.max(m);
        self.watermark = self.watermark.max(m);
        self.seen_through = self.committed;
        self.holds.clear();
        self.generation += 1;
        let _ = self.rewind.send(Rewind {
            generation: self.generation,
            from: self.committed + 1,
        });
        Ok(true)
    }

    async fn on_batch(&mut self, mut batch: ChainBatch) -> Result<bool, ThadamError> {
        let origin = batch.origin;
        let synced = batch.clock_synced;
        let (mut datasets, mut meta) = split(&mut batch);
        let rejects = std::mem::take(&mut batch.rejects);
        // Ordering: sink rejects are only known once `append` returns, and
        // they are fsynced into quarantine staging below. The sink was asked
        // (`defer_durability`) not to make this batch's rows durable inside
        // `append`, so no restart can see this batch as complete in the sink
        // while its rejects are unstaged. The rows become durable at the next
        // `append` or tick, after this function returned (rejects staged).
        // Chain rejects need no such care: a re-read regenerates them.
        add(&self.component.counters.batches, 1);

        let mut sink_rejects = Vec::new();
        let skip = self.skip_through;
        let main = if origin.first().get() <= skip {
            if origin.last().get() <= skip {
                None
            } else {
                trim_through(&mut datasets, &mut meta, skip);
                WalRange::new(origin.epoch(), WalPos::new(skip + 1), origin.last()).ok()
            }
        } else {
            Some(origin)
        };
        if let Some(range) = main {
            let n = meta.len() as u64;
            match self
                .sink
                .append(SegmentBatch::new(range, synced, datasets))
                .await
            {
                Ok(r) => {
                    add(&self.component.counters.events, n);
                    sink_rejects = r;
                }
                Err(e) if e.retry == Retry::Transient => return self.rewind_after(&e).await,
                Err(e) => return Err(e),
            }
        }
        self.seen_through = self.seen_through.max(origin.last().get());
        if self.holds_back {
            self.holds
                .push(origin.last().get(), batch.hold.map(WalPos::get));
        }

        let mut rejected: Vec<Rejected> = rejects
            .into_iter()
            .map(|r| Rejected::Raw(Box::new(r)))
            .collect();
        for r in sink_rejects {
            if let Some(m) = meta.get(r.index as usize) {
                self.component.metrics().record(MetricEvent::Rejected {
                    sink: &self.sink_id,
                    domain: r.error.domain.as_str(),
                    count: 1,
                });
                rejected.push(Rejected::Event {
                    epoch: origin.epoch(),
                    pos: m.pos,
                    ingest: m.ingest,
                    dataset: Some(m.dataset.clone()),
                    error: r.error,
                });
            }
        }
        if !rejected.is_empty() {
            add(&self.component.counters.rejects, rejected.len() as u64);
            if self
                .opts
                .failpoints
                .hit(failpoints::SEGMENT_AFTER_APPEND_BEFORE_STAGE)
            {
                return Err(simulated_crash(
                    failpoints::SEGMENT_AFTER_APPEND_BEFORE_STAGE,
                ));
            }
            match &mut self.quarantine {
                Some(q) => match q.push(origin, synced, rejected).await {
                    Ok(()) => {}
                    // The rejects were consumed; re-reading from the cursor
                    // restores them (quarantine skips what it already holds).
                    Err(e) if e.retry == Retry::Transient => return self.rewind_after(&e).await,
                    Err(e) => return Err(e),
                },
                None => self
                    .component
                    .metrics()
                    .record(MetricEvent::RejectsDropped {
                        sink: &self.sink_id,
                        count: rejected.len(),
                    }),
            }
        }
        Ok(true)
    }

    async fn finish(&mut self) -> Result<(), ThadamError> {
        self.component.draining();
        let sink = &mut self.sink;
        let wm: Result<Option<_>, ThadamError> = retry_transient!(
            self.component,
            self.opts.retry,
            &self.force,
            "finalize",
            sink.finalize_all()
        );
        let wm = wm?;
        if let Some(wm) = wm {
            self.note_watermark(wm);
        }
        if let Some(q) = &mut self.quarantine {
            let r: Result<Option<()>, ThadamError> = retry_transient!(
                self.component,
                self.opts.retry,
                &self.force,
                "quarantine finalize",
                q.finalize()
            );
            r?;
        }
        self.maybe_commit(true).await?;
        tracing::info!(sink = %self.sink_id, cursor = self.committed, "segment sink finalized");
        Ok(())
    }

    pub async fn run(mut self) -> Result<(), ThadamError> {
        if !self.recover().await? {
            self.component.stopped();
            return Ok(());
        }
        self.component.running();
        let mut tick = tokio::time::interval(self.opts.poll_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    let Some(env) = msg else { break };
                    if env.item.generation != self.generation {
                        continue;
                    }
                    let mut env = env;
                    let ok = self.on_batch(env.take()).await?;
                    drop(env);
                    if !ok {
                        break;
                    }
                    if matches!(self.component.state(), crate::ComponentState::Degraded { .. }) {
                        self.component.running();
                    }
                }
                _ = tick.tick() => self.on_tick().await?,
            }
        }
        self.finish().await?;
        self.component.stopped();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holds(batches: &[(u64, Option<u64>)]) -> HoldBack {
        let mut h = HoldBack::default();
        for (last, hold) in batches {
            h.push(*last, *hold);
        }
        h
    }

    #[test]
    fn the_cursor_stops_one_short_of_the_hold_at_the_watermark() {
        let mut h = holds(&[(10, Some(4)), (20, Some(12)), (30, Some(25))]);
        assert_eq!(h.cap(10, 0), 3);
        assert_eq!(h.cap(20, 0), 11);
        assert_eq!(h.cap(30, 0), 24);
    }

    #[test]
    fn a_later_batch_does_not_raise_the_cap_before_the_sink_catches_up() {
        // Batch 3's hold (25) is not known to be safe until the sink holds
        // batch 3: a window closed in batch 3 may not be durable yet.
        let mut h = holds(&[(10, Some(4)), (20, Some(12)), (30, Some(25))]);
        assert_eq!(h.cap(25, 0), 11);
    }

    #[test]
    fn a_watermark_inside_a_batch_never_lets_the_cursor_pass_the_last_whole_batch() {
        // The sink recovered frame 25, which is mid-way through the batch
        // ending at 30. Batch 2 left no window open, but one may have opened
        // at frames 21..=25, so the cursor stays at 20.
        let mut h = holds(&[(10, Some(4)), (20, None), (30, Some(22))]);
        assert_eq!(h.cap(25, 0), 20);
    }

    #[test]
    fn with_no_batch_covered_the_cursor_stays_put() {
        let mut h = holds(&[(30, Some(22))]);
        assert_eq!(h.cap(25, 7), 7);
        assert_eq!(HoldBack::default().cap(25, 7), 7);
    }

    #[test]
    fn covered_batches_are_forgotten_and_a_rewind_clears_them() {
        let mut h = holds(&[(10, Some(4)), (20, Some(12)), (30, Some(25))]);
        assert_eq!(h.cap(25, 0), 11);
        assert_eq!(h.batches.len(), 2, "batch 1 is behind the watermark");
        h.clear();
        assert_eq!(h.cap(25, 3), 3);
    }
}
