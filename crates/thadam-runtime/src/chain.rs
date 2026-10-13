//! The chain task: WAL reader, chain, and the hand-off to the sink driver.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use thadam_core::{
    Classify, ErrorDomain, RawBatch, ThadamError, Shutdown, WalEpoch, WalPos, WalRange, codes,
};
use thadam_transform::{
    Chain, ChainLimits, ChainOutput, ChainSpec, ChainStats, DatasetStats, FrameInput,
    RawRecordView, RejectedRaw,
};
use thadam_wal::{Frame, Gap, ReadItem, StartFrom, Wal, WalError, WalReader};
use tokio::sync::{oneshot, watch};

use crate::failpoints::Failpoints;
use crate::lane::{ChainBatch, LaneSender, Rewind};
use crate::metrics::MetricEvent;
use crate::status::{Component, add};

pub(crate) fn wal_error(e: &WalError) -> ThadamError {
    e.to_thadam_error()
}

fn join_error(what: &str) -> ThadamError {
    ThadamError::fatal(
        ErrorDomain::Buffer,
        codes::BUFFER_IO,
        format!("{what} task panicked"),
    )
}

/// Reads one item if it is durable now, on the blocking pool.
pub(crate) async fn read_now(
    reader: WalReader,
    max_bytes: usize,
) -> Result<(WalReader, Option<ReadItem>), ThadamError> {
    let (reader, item) = tokio::task::spawn_blocking(move || {
        let mut r = reader;
        let item = r.try_next(max_bytes);
        (r, item)
    })
    .await
    .map_err(|_| join_error("wal read"))?;
    Ok((reader, item.map_err(|e| wal_error(&e))?))
}

/// Splits frames into runs with the same clock state, since an event batch
/// carries one `clock_synced` flag.
pub(crate) fn clock_groups(frames: &[Frame]) -> impl Iterator<Item = &[Frame]> {
    frames.chunk_by(|a, b| a.clock_synced() == b.clock_synced())
}

/// Bytes a group pins in memory, for the lane's byte budget.
pub(crate) fn group_bytes(frames: &[Frame]) -> usize {
    frames
        .iter()
        .flat_map(|f| f.records())
        .map(|r| r.payload().len() + 64)
        .sum()
}

pub(crate) fn group_range(epoch: WalEpoch, frames: &[Frame]) -> Option<WalRange> {
    let first = frames.first()?.seq();
    let last = frames.last()?.seq();
    WalRange::new(epoch, first, last).ok()
}

/// Adapts WAL frames to the chain's borrowed input and runs `f` on it.
pub(crate) fn with_input<R>(
    epoch: WalEpoch,
    frames: &[Frame],
    f: impl FnOnce(&FrameInput<'_>) -> R,
) -> R {
    let batches: Vec<RawBatch> = frames.iter().map(Frame::to_raw_batch).collect();
    let mut views = Vec::with_capacity(batches.iter().map(RawBatch::len).sum());
    for (frame, batch) in frames.iter().zip(&batches) {
        for (i, m) in batch.messages.iter().enumerate() {
            let idx = u32::try_from(i).unwrap_or(u32::MAX);
            views.push(RawRecordView::new(
                &batch.source,
                m,
                epoch,
                frame.seq(),
                idx,
            ));
        }
    }
    let synced = frames.first().is_some_and(Frame::clock_synced);
    f(&FrameInput::new(&views, synced))
}

type Processed = (ChainOutput, Vec<RejectedRaw>);

/// Runs the chain over one clock group, catching a panic.
pub(crate) fn try_process(
    chain: &mut Chain,
    epoch: WalEpoch,
    frames: &[Frame],
    failpoints: &Failpoints,
) -> Option<Processed> {
    let origin = group_range(epoch, frames)?;
    let synced = frames.first().is_some_and(Frame::clock_synced);
    let mut out = chain.new_output(origin, synced);
    let mut rejects = Vec::new();
    let ok = catch_unwind(AssertUnwindSafe(|| {
        if failpoints.chain_panic(origin.first(), origin.last()) {
            panic!("injected chain panic in frames {origin}");
        }
        with_input(epoch, frames, |input| {
            chain.process(input, &mut out, &mut rejects)
        });
    }))
    .is_ok();
    ok.then_some((out, rejects))
}

/// Every record of `frame` as a reject with `error`.
pub(crate) fn reject_frame(epoch: WalEpoch, frame: &Frame, error: &ThadamError) -> Vec<RejectedRaw> {
    let batch = frame.to_raw_batch();
    batch
        .messages
        .into_iter()
        .enumerate()
        .map(|(i, m)| RejectedRaw {
            error: error.clone(),
            dataset: None,
            source: batch.source.clone(),
            epoch,
            pos: frame.seq(),
            record_index: u32::try_from(i).unwrap_or(u32::MAX),
            sample_idx: None,
            ingest_time: m.ingest_time,
            attrs: m.attrs,
            payload: m.payload,
        })
        .collect()
}

/// Rebuilds step state from frames `(upto - frames, upto]` without emitting.
/// Returns the number of frames read.
///
/// With `hold` (a chain that was running and lost its state), the read starts
/// no later than `hold` and the `window` steps are fed too, so the open
/// windows come back exactly as they were. Without it, windows are skipped:
/// a restart re-reads them from the held-back cursor instead.
pub(crate) async fn warm(
    chain: &mut Chain,
    wal: &Wal,
    reader_name: &str,
    upto: u64,
    frames: u64,
    hold: Option<u64>,
    read_bytes: usize,
) -> Result<u64, ThadamError> {
    if upto == 0 || (frames == 0 && hold.is_none()) {
        return Ok(0);
    }
    let mut start = if frames == 0 {
        upto
    } else {
        upto.saturating_sub(frames - 1).max(1)
    };
    if let Some(h) = hold {
        start = start.min(h.max(1));
    }
    let epoch = wal.epoch();
    let mut reader = wal
        .reader(reader_name, StartFrom::At(WalPos::new(start)))
        .map_err(|e| wal_error(&e))?;
    let mut warmed = 0;
    loop {
        let (r, item) = read_now(reader, read_bytes).await?;
        reader = r;
        match item {
            None => break,
            Some(ReadItem::Gap(g)) => {
                if g.range().last().get() >= upto {
                    break;
                }
            }
            Some(ReadItem::Frames(chunk)) => {
                let past = chunk.range().last().get() >= upto;
                let frames = chunk.into_frames();
                let keep = frames.partition_point(|f| f.seq().get() <= upto);
                for group in clock_groups(&frames[..keep]) {
                    with_input(epoch, group, |input| {
                        if hold.is_some() {
                            chain.warm_open_windows(input);
                        } else {
                            chain.warm(input);
                        }
                    });
                    warmed += group.len() as u64;
                }
                if past {
                    break;
                }
            }
        }
    }
    Ok(warmed)
}

pub(crate) fn gap_kind(g: &Gap) -> &'static str {
    match g {
        Gap::Corrupt { .. } => "corrupt",
        Gap::Evicted { .. } => "evicted",
        Gap::Missing { .. } => "missing",
        _ => "unknown",
    }
}

/// Settings a chain task needs besides its chain.
pub(crate) struct ChainConfig {
    pub sink_id: String,
    pub cursor: String,
    pub spec: ChainSpec,
    pub limits: ChainLimits,
    /// Where the chain starts when the sink has no committed cursor.
    pub start_from: StartFrom,
    pub primary: bool,
    pub has_state: bool,
    pub warm_frames: u64,
    pub read_bytes: usize,
    pub poison_retries: u32,
    pub failpoints: Failpoints,
}

pub(crate) struct ChainTask {
    pub cfg: ChainConfig,
    pub component: Arc<Component>,
    pub chain: Chain,
    pub wal: Wal,
    pub tx: LaneSender,
    pub rewind: watch::Receiver<Rewind>,
    pub drain: Shutdown,
    /// Where the driver's recovery left the cursor; the chain starts after it.
    pub start: oneshot::Receiver<u64>,
    /// Signalled once warmed and reading, so sources start after every chain.
    pub ready: Option<oneshot::Sender<()>>,
    /// The chain counters after the previous group; chain events report the
    /// deltas between snapshots, and a rebuild starts them over.
    pub last_stats: Option<ChainStats>,
}

impl ChainTask {
    fn rebuild(&mut self) -> Result<(), ThadamError> {
        self.chain = Chain::compile(&self.cfg.spec, self.cfg.limits.clone()).map_err(|errs| {
            errs.into_iter().next().unwrap_or_else(|| {
                ThadamError::fatal(
                    ErrorDomain::Config,
                    codes::CONFIG_INVALID,
                    "chain rebuild failed",
                )
            })
        })?;
        self.last_stats = None;
        add(&self.component.counters.restarts, 1);
        Ok(())
    }

    /// Warms the rebuilt chain up to frame `upto`. `hold` is the oldest frame
    /// an open window needed before the rebuild (see [`warm`]); `None` at
    /// start-up and after a rewind, when the chain begins from the cursor.
    async fn warm_to(&mut self, upto: u64, hold: Option<WalPos>) -> Result<(), ThadamError> {
        if !self.cfg.has_state && hold.is_none() {
            return Ok(());
        }
        let name = format!("warm.{}", self.cfg.sink_id);
        let n = warm(
            &mut self.chain,
            &self.wal,
            &name,
            upto,
            self.cfg.warm_frames,
            hold.map(WalPos::get),
            self.cfg.read_bytes,
        )
        .await?;
        tracing::info!(sink = %self.cfg.sink_id, frames = n, upto, "chain warm start");
        Ok(())
    }

    /// Processes one clock group. A panic rebuilds the chain (its step state
    /// may be inconsistent), warms it from the frames before, and retries
    /// frame by frame; a frame that still panics after `poison_retries`
    /// rebuilds is rejected whole. `hold` is the chain's hold before the
    /// group, from which a rebuild replays the open windows.
    async fn process_group(
        &mut self,
        epoch: WalEpoch,
        frames: &[Frame],
        hold: Option<WalPos>,
    ) -> Result<Processed, ThadamError> {
        if let Some(done) = try_process(&mut self.chain, epoch, frames, &self.cfg.failpoints) {
            return Ok(done);
        }
        self.on_panic(frames)?;
        let before = frames
            .first()
            .map_or(0, |f| f.seq().get().saturating_sub(1));
        self.warm_to(before, hold).await?;
        let Some(origin) = group_range(epoch, frames) else {
            return Err(ThadamError::fatal(
                ErrorDomain::Transform,
                codes::TRANSFORM_FAILED,
                "empty frame group",
            ));
        };
        let synced = frames.first().is_some_and(Frame::clock_synced);
        let mut out = self.chain.new_output(origin, synced);
        let mut rejects = Vec::new();
        for frame in frames {
            let one = std::slice::from_ref(frame);
            let hold = self.chain.hold();
            let mut attempts = 0;
            loop {
                if let Some((o, r)) = try_process(&mut self.chain, epoch, one, &self.cfg.failpoints)
                {
                    for (dst, src) in out.batches.iter_mut().zip(o.batches) {
                        dst.batch.events.extend(src.batch.events);
                    }
                    rejects.extend(r);
                    break;
                }
                self.on_panic(one)?;
                self.warm_to(frame.seq().get().saturating_sub(1), hold)
                    .await?;
                attempts += 1;
                if attempts > self.cfg.poison_retries {
                    let err = ThadamError::permanent(
                        ErrorDomain::Transform,
                        codes::TRANSFORM_PANIC,
                        format!("processing frame {} panicked {attempts} times", frame.seq()),
                    );
                    tracing::error!(sink = %self.cfg.sink_id, pos = %frame.seq(), "poison frame quarantined");
                    rejects.extend(reject_frame(epoch, frame, &err));
                    break;
                }
            }
        }
        Ok((out, rejects))
    }

    fn on_panic(&mut self, frames: &[Frame]) -> Result<(), ThadamError> {
        add(&self.component.counters.panics, 1);
        self.component.metrics().record(MetricEvent::ChainPanic {
            sink: &self.cfg.sink_id,
        });
        tracing::error!(
            sink = %self.cfg.sink_id,
            first = frames.first().map_or(0, |f| f.seq().get()),
            last = frames.last().map_or(0, |f| f.seq().get()),
            "chain panicked; rebuilding it"
        );
        self.rebuild()
    }

    async fn send(&mut self, batch: ChainBatch, bytes: usize) -> bool {
        tokio::select! {
            r = self.tx.send(batch, bytes) => r.is_ok(),
            () = self.drain.triggered() => false,
        }
    }

    fn count_output(&mut self, out: &ChainOutput, rejects: &[RejectedRaw]) {
        let c = &self.component.counters;
        for d in &out.batches {
            add(&c.events, d.batch.len() as u64);
        }
        // The chain stats are cumulative per dataset; the outcome labels
        // report the deltas since the previous group. `Chain::warm` restores
        // the counters it saves, and `rebuild` resets `last_stats`, so neither
        // a warm start nor a panic rebuild distorts the deltas.
        let stats = self.chain.stats();
        for (dataset, ds) in &stats.datasets {
            let prev: DatasetStats = self
                .last_stats
                .as_ref()
                .and_then(|s| s.datasets.iter().find(|(d, _)| d == dataset))
                .map(|(_, ds)| ds.clone())
                .unwrap_or_default();
            let deltas = [
                ("written", ds.written.saturating_sub(prev.written)),
                ("filtered", ds.filtered.saturating_sub(prev.filtered)),
                ("deduped", ds.deduped.saturating_sub(prev.deduped)),
                ("rejected", ds.rejected.saturating_sub(prev.rejected)),
                ("exploded", ds.exploded.saturating_sub(prev.exploded)),
            ];
            for (outcome, delta) in deltas {
                if delta > 0 {
                    self.component.metrics().record(MetricEvent::ChainEvents {
                        sink: &self.cfg.sink_id,
                        dataset,
                        outcome,
                        events: delta as usize,
                    });
                }
            }
        }
        let prev_unmatched = self.last_stats.as_ref().map_or(0, |s| s.unmatched);
        let unmatched = stats.unmatched.saturating_sub(prev_unmatched);
        if unmatched > 0 {
            // shortcut: a panic that rebuilds the chain mid-group drops the unmatched
            // count of the frames before it in that group, as the outcome counts do;
            // upgrade by counting per frame in `process_group`.
            add(&c.unmatched, unmatched);
            self.component
                .metrics()
                .record(MetricEvent::ChainUnmatched {
                    sink: &self.cfg.sink_id,
                    messages: unmatched as usize,
                });
        }
        self.last_stats = Some(stats);
        if !rejects.is_empty() {
            add(&c.rejects, rejects.len() as u64);
            for r in rejects {
                self.component.metrics().record(MetricEvent::Rejected {
                    sink: &self.cfg.sink_id,
                    domain: r.error.domain.as_str(),
                    count: 1,
                });
            }
        }
    }

    pub async fn run(mut self) -> Result<(), ThadamError> {
        let start = tokio::select! {
            s = &mut self.start => match s {
                Ok(s) => s,
                Err(_) => {
                    self.component.stopped();
                    return Ok(());
                }
            },
            () = self.drain.triggered() => {
                self.component.stopped();
                return Ok(());
            }
        };
        self.warm_to(start, None).await?;
        let from = if start == 0 {
            self.cfg.start_from
        } else {
            StartFrom::At(WalPos::new(start + 1))
        };
        let mut reader = self
            .wal
            .reader(&self.cfg.cursor, from)
            .map_err(|e| wal_error(&e))?;
        if start > 0 {
            reader.seek(WalPos::new(start + 1));
        }
        let epoch = self.wal.epoch();
        let mut generation = self.rewind.borrow().generation;
        if let Some(ready) = self.ready.take() {
            let _ = ready.send(());
        }
        self.component.running();

        loop {
            let item = tokio::select! {
                biased;
                () = self.drain.triggered() => break,
                changed = self.rewind.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let rw = *self.rewind.borrow_and_update();
                    generation = rw.generation;
                    self.rebuild()?;
                    self.warm_to(rw.from.saturating_sub(1), None).await?;
                    reader.seek(WalPos::new(rw.from));
                    tracing::warn!(sink = %self.cfg.sink_id, from = rw.from, "chain rewound by its driver");
                    continue;
                }
                item = reader.next(self.cfg.read_bytes) => item.map_err(|e| wal_error(&e))?,
            };
            let Some(item) = item else { break };
            match item {
                ReadItem::Gap(g) => {
                    let range = g.range();
                    add(&self.component.counters.gaps, 1);
                    self.component.metrics().record(MetricEvent::WalGap {
                        sink: &self.cfg.sink_id,
                        kind: gap_kind(&g),
                        frames: range.len(),
                    });
                    tracing::warn!(sink = %self.cfg.sink_id, gap = ?g, "wal gap");
                    let out = self.chain.new_output(range, true);
                    let batch = ChainBatch {
                        origin: range,
                        clock_synced: true,
                        datasets: out.batches,
                        rejects: Vec::new(),
                        hold: self.chain.hold(),
                        generation,
                    };
                    if !self.send(batch, 1).await {
                        break;
                    }
                }
                ReadItem::Frames(chunk) => {
                    let frames = chunk.into_frames();
                    let c = &self.component.counters;
                    add(&c.batches, 1);
                    let mut stop = false;
                    for group in clock_groups(&frames) {
                        let records: usize = group.iter().map(Frame::len).sum();
                        add(&self.component.counters.messages, records as u64);
                        let bytes = group_bytes(group);
                        add(&self.component.counters.bytes, bytes as u64);
                        let hold = self.chain.hold();
                        let (out, mut rejects) = self.process_group(epoch, group, hold).await?;
                        self.count_output(&out, &rejects);
                        if !self.cfg.primary && !rejects.is_empty() {
                            self.component
                                .metrics()
                                .record(MetricEvent::RejectsDropped {
                                    sink: &self.cfg.sink_id,
                                    count: rejects.len(),
                                });
                            rejects.clear();
                        }
                        let Some(origin) = group_range(epoch, group) else {
                            continue;
                        };
                        let batch = ChainBatch {
                            origin,
                            clock_synced: group.first().is_some_and(Frame::clock_synced),
                            datasets: out.batches,
                            rejects,
                            hold: self.chain.hold(),
                            generation,
                        };
                        if !self.send(batch, bytes).await {
                            stop = true;
                            break;
                        }
                    }
                    if stop {
                        break;
                    }
                }
            }
        }
        self.component.stopped();
        Ok(())
    }
}
