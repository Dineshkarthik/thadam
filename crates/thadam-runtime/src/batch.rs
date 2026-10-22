//! The batch-sink driver: frame-aligned batches, a persisted in-flight range,
//! retries with the same batch id, and bisection of poison batches.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use thadam_core::{
    BatchSink, Event, EventBatch, Retry, Shutdown, SinkBatch, SinkContext, ThadamError, WalPos,
    WalRange,
};
use thadam_transform::RejectedRaw;
use thadam_wal::{CursorState, Wal};
use tokio::sync::{oneshot, watch};

use crate::backoff::Backoff;
use crate::chain::wal_error;
use crate::failpoints::{self, Failpoints};
use crate::lane::{ChainBatch, LaneReceiver, Rewind};
use crate::metrics::MetricEvent;
use crate::options::BackoffPolicy;
use crate::quarantine::{Quarantine, Rejected};
use crate::segment::{EventMeta, merge, pause, retry_transient, simulated_crash};
use crate::status::{Component, ComponentState, PipelineHandle, add};

pub(crate) struct BatchOpts {
    pub max_events: usize,
    pub max_bytes: usize,
    pub max_age: Duration,
    pub max_attempts_before_bisect: u32,
    pub retry: BackoffPolicy,
    pub failpoints: Failpoints,
}

/// Events over a WAL range, with what quarantine needs about each.
struct Part {
    range: WalRange,
    events: Vec<Event>,
    meta: Vec<EventMeta>,
    rejects: Vec<RejectedRaw>,
    bytes: usize,
    synced: bool,
}

impl Part {
    /// Splits into positions `..=at` and `at+1..`; `at` must lie in the range
    /// with something after it.
    fn split_at(self, at: u64) -> (Part, Part) {
        let epoch = self.range.epoch();
        let left_range =
            WalRange::new(epoch, self.range.first(), WalPos::new(at)).unwrap_or(self.range);
        let right_range =
            WalRange::new(epoch, WalPos::new(at + 1), self.range.last()).unwrap_or(self.range);
        let (mut le, mut lm, mut re, mut rm) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for (e, m) in self.events.into_iter().zip(self.meta) {
            if e.wal_pos().get() <= at {
                le.push(e);
                lm.push(m);
            } else {
                re.push(e);
                rm.push(m);
            }
        }
        let (lr, rr): (Vec<_>, Vec<_>) = self.rejects.into_iter().partition(|r| r.pos.get() <= at);
        let share = |n: u64| {
            usize::try_from(n * self.bytes as u64 / self.range.len().max(1)).unwrap_or(self.bytes)
        };
        (
            Part {
                range: left_range,
                events: le,
                meta: lm,
                rejects: lr,
                bytes: share(left_range.len()),
                synced: self.synced,
            },
            Part {
                range: right_range,
                events: re,
                meta: rm,
                rejects: rr,
                bytes: share(right_range.len()),
                synced: self.synced,
            },
        )
    }
}

struct Work {
    batch: SinkBatch,
    meta: Vec<EventMeta>,
    attempts: u32,
}

impl Work {
    fn positions(&self) -> BTreeSet<u64> {
        self.batch
            .events()
            .iter()
            .map(|e| e.wal_pos().get())
            .collect()
    }

    /// Halves on a frame boundary; `None` if every event is in one frame.
    fn bisect(self) -> Option<(Work, Work)> {
        let positions: Vec<u64> = self.positions().into_iter().collect();
        if positions.len() < 2 {
            return None;
        }
        let mid = positions[(positions.len() - 1) / 2];
        let batch = self.batch.into_batch();
        let part = Part {
            range: batch.origin,
            events: batch.events,
            meta: self.meta,
            rejects: Vec::new(),
            bytes: 0,
            synced: batch.clock_synced,
        };
        let (l, r) = part.split_at(mid);
        let work = |p: Part| Work {
            batch: SinkBatch::new(EventBatch::new(p.events, p.range, p.synced)),
            meta: p.meta,
            attempts: 0,
        };
        Some((work(l), work(r)))
    }
}

#[derive(Default)]
struct Acc {
    part: Option<Part>,
    opened: Option<tokio::time::Instant>,
}

pub(crate) struct BatchDriver {
    pub sink_id: String,
    pub cursor: String,
    pub component: Arc<Component>,
    pub sink: Box<dyn BatchSink>,
    pub ctx: SinkContext,
    pub quarantine: Option<Quarantine>,
    pub wal: Wal,
    pub handle: PipelineHandle,
    pub rx: LaneReceiver,
    /// Held so the chain's rewind receiver stays open; batch drivers keep
    /// their batches and never rewind.
    pub _rewind: watch::Sender<Rewind>,
    pub start: Option<oneshot::Sender<u64>>,
    pub opts: BatchOpts,
    /// Ends startup retries once shutdown begins.
    pub stopping: Shutdown,
    /// Ends delivery retries (forced shutdown only: a graceful shutdown waits
    /// for the in-flight batch).
    pub force: Shutdown,
    pub committed: u64,
    pub forced: Option<WalRange>,
    acc: Acc,
}

impl BatchDriver {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sink_id: String,
        cursor: String,
        component: Arc<Component>,
        sink: Box<dyn BatchSink>,
        ctx: SinkContext,
        quarantine: Option<Quarantine>,
        wal: Wal,
        handle: PipelineHandle,
        rx: LaneReceiver,
        rewind: watch::Sender<Rewind>,
        start: oneshot::Sender<u64>,
        opts: BatchOpts,
        stopping: Shutdown,
        force: Shutdown,
    ) -> Self {
        BatchDriver {
            sink_id,
            cursor,
            component,
            sink,
            ctx,
            quarantine,
            wal,
            handle,
            rx,
            _rewind: rewind,
            start: Some(start),
            opts,
            stopping,
            force,
            committed: 0,
            forced: None,
            acc: Acc::default(),
        }
    }

    async fn recover(&mut self) -> Result<bool, ThadamError> {
        let (sink, ctx) = (&mut self.sink, &self.ctx);
        let caps: Result<Option<_>, ThadamError> = retry_transient!(
            self.component,
            self.opts.retry,
            &self.stopping,
            "open",
            sink.open(ctx)
        );
        let caps = caps?;
        let Some(caps) = caps else { return Ok(false) };
        self.component.set_delivery(caps.delivery);
        self.component
            .metrics()
            .record(MetricEvent::SinkDeliveryClass {
                sink: &self.sink_id,
                class: caps.delivery.as_str(),
            });
        if let Some(q) = &mut self.quarantine {
            let r: Result<Option<u64>, ThadamError> = retry_transient!(
                self.component,
                self.opts.retry,
                &self.stopping,
                "quarantine recover",
                q.recover()
            );
            let r = r?;
            if r.is_none() {
                return Ok(false);
            }
        }
        let state = self.wal.cursor(&self.cursor);
        self.committed = state.map_or(0, |c| c.committed.get());
        self.forced = state
            .and_then(|c| c.inflight)
            .filter(|r| r.first().get() > self.committed);
        self.handle.set_cursor(
            &self.sink_id,
            &self.cursor,
            state.unwrap_or(CursorState::at(WalPos::new(self.committed))),
        );
        let (lag_bytes, lag_secs) = self.wal.cursor_lag(&self.cursor);
        self.component.metrics().record(MetricEvent::SinkLag {
            sink: &self.sink_id,
            bytes: lag_bytes,
            seconds: lag_secs,
        });
        if let Some(r) = self.forced {
            tracing::info!(sink = %self.sink_id, batch = %r, "re-sending the in-flight batch first");
        }
        if let Some(start) = self.start.take() {
            let _ = start.send(self.committed);
        }
        Ok(true)
    }

    async fn store_cursor(&mut self, state: CursorState) -> Result<bool, ThadamError> {
        let (wal, cursor) = (&self.wal, &self.cursor);
        let r: Result<Option<()>, ThadamError> = retry_transient!(
            self.component,
            self.opts.retry,
            &self.force,
            "cursor commit",
            async {
                wal.commit_cursor(cursor, state)
                    .await
                    .map_err(|e| wal_error(&e))
            }
        );
        let r = r?;
        if r.is_some() {
            self.handle.set_cursor(&self.sink_id, &self.cursor, state);
        }
        Ok(r.is_some())
    }

    /// Writes `batch` until every event is applied or quarantined. `Ok(None)`
    /// on a forced stop.
    async fn deliver(
        &mut self,
        batch: SinkBatch,
        meta: Vec<EventMeta>,
    ) -> Result<Option<Vec<Rejected>>, ThadamError> {
        let epoch = batch.batch().origin.epoch();
        let mut stack = vec![Work {
            batch,
            meta,
            attempts: 0,
        }];
        let mut rejected = Vec::new();
        let mut backoff = Backoff::new(self.opts.retry);
        while let Some(mut w) = stack.pop() {
            loop {
                let started = Instant::now();
                let err = match self.sink.write(&w.batch).await {
                    Ok(outcome) => {
                        let n = w.batch.events().len();
                        add(
                            &self.component.counters.events,
                            (n - outcome.rejected.len().min(n)) as u64,
                        );
                        add(&self.component.counters.batches, 1);
                        self.component.metrics().record(MetricEvent::BatchWritten {
                            sink: &self.sink_id,
                            events: n,
                            latency: started.elapsed(),
                        });
                        for r in outcome.rejected {
                            if let Some(m) = w.meta.get(r.index as usize) {
                                rejected.push(Rejected::Event {
                                    epoch,
                                    pos: m.pos,
                                    ingest: m.ingest,
                                    dataset: Some(m.dataset.clone()),
                                    error: r.error,
                                });
                            }
                        }
                        backoff.reset();
                        if matches!(self.component.state(), ComponentState::Degraded { .. }) {
                            self.component.running();
                        }
                        break;
                    }
                    Err(e) => e,
                };
                if err.retry == Retry::Fatal {
                    return Err(err);
                }
                let id = w.batch.id();
                if err.retry == Retry::Transient {
                    w.attempts += 1;
                    self.component.degraded(&err);
                    tracing::warn!(sink = %self.sink_id, batch = %id, attempts = w.attempts, error = %err, "batch write failed");
                    if w.attempts < self.opts.max_attempts_before_bisect || w.positions().len() < 2
                    {
                        if !pause(&mut backoff, &self.force).await {
                            return Ok(None);
                        }
                        continue;
                    }
                }
                if w.positions().len() < 2 {
                    tracing::error!(sink = %self.sink_id, batch = %id, error = %err, "permanent failure; quarantining the frame");
                    for m in w.meta {
                        rejected.push(Rejected::Event {
                            epoch,
                            pos: m.pos,
                            ingest: m.ingest,
                            dataset: Some(m.dataset),
                            error: err.clone(),
                        });
                    }
                    break;
                }
                if let Some((l, r)) = w.bisect() {
                    add(&self.component.counters.bisects, 1);
                    self.component.metrics().record(MetricEvent::Bisect {
                        sink: &self.sink_id,
                    });
                    tracing::warn!(sink = %self.sink_id, batch = %id, left = %l.batch.id(), right = %r.batch.id(), "bisecting batch");
                    stack.push(r);
                    stack.push(l);
                }
                break;
            }
        }
        Ok(Some(rejected))
    }

    /// Writes the accumulated batch and advances the cursor. `Ok(false)` on a
    /// forced stop.
    async fn flush(&mut self) -> Result<bool, ThadamError> {
        let acc = std::mem::take(&mut self.acc);
        let Some(part) = acc.part else {
            return Ok(true);
        };
        let range = part.range;
        let mut rejected: Vec<Rejected> = part
            .rejects
            .into_iter()
            .map(|r| Rejected::Raw(Box::new(r)))
            .collect();
        if !part.events.is_empty() {
            let inflight = CursorState {
                committed: WalPos::new(self.committed),
                inflight: Some(range),
            };
            if !self.store_cursor(inflight).await? {
                return Ok(false);
            }
            if self.opts.failpoints.hit(failpoints::BATCH_AFTER_INFLIGHT) {
                return Err(simulated_crash(failpoints::BATCH_AFTER_INFLIGHT));
            }
            let batch = SinkBatch::new(EventBatch::new(part.events, range, part.synced));
            match self.deliver(batch, part.meta).await? {
                Some(r) => rejected.extend(r),
                None => return Ok(false),
            }
            if self.opts.failpoints.hit(failpoints::BATCH_AFTER_WRITE) {
                return Err(simulated_crash(failpoints::BATCH_AFTER_WRITE));
            }
        }
        if !rejected.is_empty() {
            add(&self.component.counters.rejects, rejected.len() as u64);
            match &mut self.quarantine {
                Some(q) => {
                    let mut once = Some(rejected);
                    let r: Result<Option<()>, ThadamError> = retry_transient!(
                        self.component,
                        self.opts.retry,
                        &self.force,
                        "quarantine",
                        async {
                            if let Some(batch) = once.take() {
                                q.push(range, part.synced, batch).await?;
                            }
                            q.finalize().await
                        }
                    );
                    let r = r?;
                    if r.is_none() {
                        return Ok(false);
                    }
                }
                None => self
                    .component
                    .metrics()
                    .record(MetricEvent::RejectsDropped {
                        sink: &self.sink_id,
                        count: rejected.len(),
                    }),
            }
        }
        if !self.store_cursor(CursorState::at(range.last())).await? {
            return Ok(false);
        }
        self.committed = range.last().get();
        self.component
            .metrics()
            .record(MetricEvent::CursorCommitted {
                sink: &self.sink_id,
                pos: range.last(),
            });
        let (lag_bytes, lag_secs) = self.wal.cursor_lag(&self.cursor);
        self.component.metrics().record(MetricEvent::SinkLag {
            sink: &self.sink_id,
            bytes: lag_bytes,
            seconds: lag_secs,
        });
        if self.forced.is_some_and(|f| f.last() <= range.last()) {
            self.forced = None;
        }
        Ok(true)
    }

    fn full(&self) -> bool {
        let Some(p) = &self.acc.part else {
            return false;
        };
        p.events.len() >= self.opts.max_events
            || p.bytes >= self.opts.max_bytes
            || self.forced.is_some_and(|f| p.range.last() >= f.last())
    }

    fn extend(&mut self, part: Part) -> Option<Part> {
        match &mut self.acc.part {
            None => {
                self.acc.opened = Some(tokio::time::Instant::now());
                self.acc.part = Some(part);
                None
            }
            Some(acc) => match acc.range.extended_by(&part.range) {
                Some(r) if acc.synced == part.synced => {
                    acc.range = r;
                    acc.events.extend(part.events);
                    acc.meta.extend(part.meta);
                    acc.rejects.extend(part.rejects);
                    acc.bytes += part.bytes;
                    None
                }
                _ => Some(part),
            },
        }
    }

    async fn add(&mut self, mut batch: ChainBatch, bytes: u64) -> Result<bool, ThadamError> {
        let (events, meta) = merge(&mut batch);
        let mut part = Part {
            range: batch.origin,
            events,
            meta,
            rejects: std::mem::take(&mut batch.rejects),
            bytes: usize::try_from(bytes).unwrap_or(usize::MAX),
            synced: batch.clock_synced,
        };
        loop {
            if let Some(f) = self.forced
                && part.range.first() <= f.last()
                && part.range.last() > f.last()
            {
                let (left, right) = part.split_at(f.last().get());
                if let Some(rest) = self.extend(left) {
                    if !self.flush().await? {
                        return Ok(false);
                    }
                    let _ = self.extend(rest);
                }
                if !self.flush().await? {
                    return Ok(false);
                }
                part = right;
                continue;
            }
            if let Some(rest) = self.extend(part) {
                if !self.flush().await? {
                    return Ok(false);
                }
                let _ = self.extend(rest);
            }
            break;
        }
        if self.full() {
            return self.flush().await;
        }
        Ok(true)
    }

    pub async fn run(mut self) -> Result<(), ThadamError> {
        if !self.recover().await? {
            self.component.stopped();
            return Ok(());
        }
        self.component.running();
        let mut stopped = false;
        loop {
            let deadline = self.acc.opened.map(|t| t + self.opts.max_age);
            tokio::select! {
                msg = self.rx.recv() => {
                    let Some(mut env) = msg else { break };
                    if env.item.eof {
                        break;
                    }
                    let bytes = env.bytes();
                    if !self.add(env.take(), bytes).await? {
                        stopped = true;
                        break;
                    }
                }
                () = async move {
                    match deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending().await,
                    }
                } => {
                    if !self.flush().await? {
                        stopped = true;
                        break;
                    }
                }
            }
        }
        if !stopped {
            self.component.draining();
            self.flush().await?;
        }
        if let Err(e) = self.sink.close().await {
            tracing::warn!(sink = %self.sink_id, error = %e, "batch sink close failed");
        }
        self.component.stopped();
        Ok(())
    }
}
