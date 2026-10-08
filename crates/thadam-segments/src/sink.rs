//! The Parquet segment sink: per-dataset open segments, rotation, least-
//! recently-written eviction at `max_open_files`, the D18 finalize sequence,
//! and the `kind: parquet` factory.
//!
//! # Concurrency shape
//!
//! The sink is a state machine owned by its driver (`&mut self` methods, no
//! locks inside). Encoding and finalization move the encoder and the open
//! file into `tokio::task::spawn_blocking` and back (ownership ping-pong):
//! nothing shared is locked across encoding, and every fsync happens on the
//! blocking pool. The catalog is shared (`SegmentCatalog`, Arc + mutex never
//! held across await); its fsynced appends serialize through its
//! single-writer queue.
//!
//! # Watermark and coverage
//!
//! A segment file claims the whole appended WAL range `[first, last]` of the
//! frames processed since the dataset's previous finalize, including frames
//! that produced no rows for it (the `SegmentBatch` contract lists every
//! dataset in every batch so they advance together). A dataset whose segment
//! closes with zero rows writes no file; a *coverage marker* record goes
//! into the catalog instead, so recovery can tell "empty" from "lost"
//! without footer-only Parquet files. The watermark is the highest WAL
//! position every configured dataset covers contiguously; rotation triggers
//! are checked per dataset, and a crash mid-drain is what the catalog's
//! drop-beyond-watermark recovery exists for (see `catalog`).
//!
//! # Error and crash semantics
//!
//! Any error while finalizing (including an armed failpoint) latches the
//! sink dead until `recover` is called, exactly like a process crash: open
//! segments are dropped and their rows regenerate from the WAL, because the
//! watermark only advances after the catalog append is fsynced.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use thadam_arrow::{BatchContext, DatasetSchema};
use thadam_core::{
    BudgetCategory, Clock, DiskBudget, ErrorDomain, Event, Reject, ThadamError, SegmentBatch,
    SegmentSink, SegmentSinkFactory, SinkContext, Timestamp, WalEpoch, WalPos, WalRange, codes,
};

use crate::catalog::{LocalRetention, SegmentCatalog, SegmentRecord};
use crate::config::{self, ParquetSinkConfig, RotationConfig};
use crate::error::SegmentError;
use crate::failpoints::{Failpoint, Failpoints};
use crate::partition::Partition;
use crate::quarantine::QuarantineEncoderSpec;
use crate::rows::{ArrowRowsEncoder, RowsEncoder};
use crate::writer::{Hooks, OpenFile, marker_record};

/// Rows staged per encode call; the Parquet writer accumulates row groups
/// across calls, so this stays small.
const ENCODE_CAPACITY_HINT: usize = 1024;

/// What encoder a dataset gets.
#[derive(Clone)]
pub(crate) enum EncoderKind {
    /// A declared-schema dataset encoded by `thadam-arrow`.
    Arrow(Arc<DatasetSchema>),
    /// The fixed quarantine schema.
    Quarantine(QuarantineEncoderSpec),
}

/// One dataset the sink writes, with its prebuilt writer properties.
pub(crate) struct DatasetPlan {
    pub(crate) name: String,
    pub(crate) props: parquet::file::properties::WriterProperties,
    pub(crate) kind: EncoderKind,
}

impl DatasetPlan {
    fn make_encoder(&self) -> Box<dyn RowsEncoder> {
        match &self.kind {
            EncoderKind::Arrow(schema) => Box::new(ArrowRowsEncoder::new(
                Arc::clone(schema),
                ENCODE_CAPACITY_HINT,
            )),
            EncoderKind::Quarantine(spec) => spec.make_encoder(),
        }
    }
}

/// Everything the sink needs that is fixed after construction.
pub(crate) struct EngineConfig {
    pub(crate) dir: PathBuf,
    pub(crate) node_id: String,
    pub(crate) plans: Vec<DatasetPlan>,
    pub(crate) rotation: RotationConfig,
    pub(crate) max_open_files: usize,
    pub(crate) row_group_bytes: usize,
    pub(crate) max_disk: Option<u64>,
    pub(crate) finalize_on_shutdown: bool,
    pub(crate) budget_category: BudgetCategory,
}

impl EngineConfig {
    fn dataset_names(&self) -> Vec<String> {
        self.plans.iter().map(|p| p.name.clone()).collect()
    }
}

/// A per-dataset open segment. The Parquet file exists lazily, from the
/// first rows; a segment that closes with zero rows becomes a coverage
/// marker instead of a file.
struct OpenSegment {
    encoder: Option<Box<dyn RowsEncoder>>,
    file: Option<OpenFile>,
    first: WalPos,
    last: WalPos,
    epoch: WalEpoch,
    partition: Option<Partition>,
    bounds: (Option<Timestamp>, Option<Timestamp>),
    opened_at: Instant,
    last_write: u64,
    synced: bool,
}

impl OpenSegment {
    fn new(first: WalPos, epoch: WalEpoch, opened_at: Instant, synced: bool, tick: u64) -> Self {
        OpenSegment {
            encoder: None,
            file: None,
            first,
            last: first,
            epoch,
            partition: None,
            bounds: (None, None),
            opened_at,
            last_write: tick,
            synced,
        }
    }

    /// Rows actually written to the file (the encoder is drained into
    /// batches per append).
    fn rows(&self) -> u64 {
        self.file.as_ref().map_or(0, OpenFile::rows)
    }

    /// Exact flushed bytes plus the in-progress row group's estimate.
    fn bytes_est(&self) -> u64 {
        self.file.as_ref().map_or(0, |f| {
            f.bytes_written()
                .saturating_add(u64::try_from(f.in_progress_size()).unwrap_or(0))
        })
    }
}

struct Live {
    catalog: SegmentCatalog,
    open: BTreeMap<String, OpenSegment>,
    watermark: Option<WalPos>,
    last_appended: Option<WalPos>,
    /// An eligibility position from `cursor_committed`, drained into
    /// `Eligible` records at the next async call (the trait method is
    /// synchronous; the catalog append is not).
    pending_eligible: WalPos,
    write_seq: u64,
    tick: u64,
    clock: Arc<dyn Clock>,
    budget: Arc<DiskBudget>,
}

enum State {
    New,
    Live(Live),
    /// Latched crash: only `recover` clears this, exactly like a restart.
    Dead,
}

/// The fixed-size parameter bundle for one dataset encode pass.
struct EncodeJob<'a> {
    name: &'a str,
    props: parquet::file::properties::WriterProperties,
    origin: WalRange,
    synced: bool,
}

/// The `kind: parquet` segment sink.
///
/// Construct with [`ParquetSegmentSink::new`]; the driver calls
/// [`SegmentSink::recover`] before appends. See the module docs for the
/// concurrency and watermark design.
pub struct ParquetSegmentSink {
    engine: EngineConfig,
    state: State,
    failpoints: Failpoints,
}

impl ParquetSegmentSink {
    /// A sink from validated configuration. Does no IO;
    /// [`SegmentSink::recover`] opens the directory.
    pub fn new(cfg: ParquetSinkConfig) -> Result<Self, SegmentError> {
        let mut plans = Vec::with_capacity(cfg.datasets.len());
        for (name, schema) in &cfg.datasets {
            plans.push(DatasetPlan {
                name: name.clone(),
                props: crate::writer::dataset_props(schema, &cfg)?,
                kind: EncoderKind::Arrow(Arc::clone(schema)),
            });
        }
        Ok(ParquetSegmentSink {
            engine: EngineConfig {
                dir: cfg.dir,
                node_id: cfg.node_id,
                plans,
                rotation: cfg.rotation,
                max_open_files: cfg.max_open_files,
                row_group_bytes: cfg.row_group_bytes,
                max_disk: cfg.max_disk,
                finalize_on_shutdown: cfg.finalize_on_shutdown,
                budget_category: BudgetCategory::Segments,
            },
            state: State::New,
            failpoints: Failpoints::default(),
        })
    }

    /// A sink over caller-built plans; the quarantine factory supplies its
    /// fixed schema and budget category this way.
    pub(crate) fn from_plans(engine: EngineConfig) -> Self {
        ParquetSegmentSink {
            engine,
            state: State::New,
            failpoints: Failpoints::default(),
        }
    }

    /// The catalog, once [`SegmentSink::recover`] has opened it.
    pub fn catalog(&self) -> Option<&SegmentCatalog> {
        match &self.state {
            State::Live(l) => Some(&l.catalog),
            _ => None,
        }
    }

    /// Arms crash failpoints (unit tests and the `failpoints` feature).
    #[cfg(any(test, feature = "failpoints"))]
    pub fn set_failpoints(&mut self, failpoints: Failpoints) {
        self.failpoints = failpoints;
    }

    fn live_mut(&mut self) -> Result<&mut Live, ThadamError> {
        match &mut self.state {
            State::Live(l) => Ok(l),
            State::New => Err(ThadamError::fatal(
                ErrorDomain::Sink,
                codes::SINK_IO,
                "sink is not recovered",
            )),
            State::Dead => Err(ThadamError::fatal(
                ErrorDomain::Sink,
                codes::SINK_IO,
                "the sink crashed at a failpoint",
            )),
        }
    }

    fn plan(&self, dataset: &str) -> Result<&DatasetPlan, ThadamError> {
        self.engine
            .plans
            .iter()
            .find(|p| p.name == dataset)
            .ok_or_else(|| {
                ThadamError::permanent(
                    ErrorDomain::Config,
                    codes::CONFIG_MISSING,
                    format!("no schema configured for dataset {dataset:?}"),
                )
            })
    }

    /// Latches the sink dead and returns the error: used for anything that
    /// leaves an open segment in an unknown state.
    fn latch(&mut self, r: Result<SegmentRecord, ThadamError>) -> Result<SegmentRecord, ThadamError> {
        match r {
            Err(e) => {
                self.state = State::Dead;
                Err(e)
            }
            ok => ok,
        }
    }

    /// Finalizes one open segment and catalogs it (a coverage marker when it
    /// never produced rows). Removes the segment from the open set first;
    /// on failure the rows regenerate from the WAL after `recover`, because
    /// the watermark never advanced.
    async fn finalize_one(&mut self, name: &str) -> Result<(), ThadamError> {
        enum Job {
            Marker(SegmentRecord, SegmentCatalog),
            File(Box<dyn FnOnce() -> Result<SegmentRecord, SegmentError> + Send>),
        }

        let root = self.engine.dir.clone();
        let node_id = self.engine.node_id.clone();
        let fps_before = self.failpoints.clone();
        let fps_after = self.failpoints.clone();
        let job = {
            let l = self.live_mut()?;
            let Some(mut seg) = l.open.remove(name) else {
                return Ok(());
            };
            let Some(range) = WalRange::new(seg.epoch, seg.first, seg.last).ok() else {
                return Err(ThadamError::permanent(
                    ErrorDomain::Sink,
                    codes::SINK_WRITE_FAILED,
                    "segment range is inverted",
                ));
            };
            match seg.file.take() {
                None => Job::Marker(marker_record(name, range, seg.synced), l.catalog.clone()),
                Some(file) => {
                    let catalog = l.catalog.clone();
                    let bounds = seg.bounds;
                    let job = move || {
                        let hooks = Hooks {
                            before: move || fps_before.hit(Failpoint::BeforeFinalize),
                            after_rename: move || {
                                fps_after.hit(Failpoint::AfterRenameBeforeCatalog)
                            },
                        };
                        file.finalize(&root, &catalog, &node_id, range, bounds, hooks)
                    };
                    Job::File(Box::new(job))
                }
            }
        };

        match job {
            Job::Marker(record, catalog) => {
                if let Err(e) = catalog.append_finalized(record).await {
                    self.state = State::Dead;
                    return Err(ThadamError::from(e));
                }
                Ok(())
            }
            Job::File(job) => {
                let result = tokio::task::spawn_blocking(job)
                    .await
                    .map_err(|e| ThadamError::from(SegmentError::Join(e.to_string())))
                    .and_then(|r| r.map_err(ThadamError::from));
                let record = self.latch(result)?;
                self.pressure_release(record.bytes).await
            }
        }
    }

    /// After a file is finalized: charged through the catalog, it may push
    /// the budget over its limit. Evicts per the retention order (uploaded +
    /// expired, then uploaded) and, if pressure remains, reports it as a
    /// transient budget error, which the driver turns into a rewind.
    /// Intentionally not latched: the file is durable, so a transient
    /// backpressure error is safe.
    async fn pressure_release(&mut self, bytes: u64) -> Result<(), ThadamError> {
        let category = self.engine.budget_category;
        if !self.live_mut()?.budget.is_over_limit(category) {
            return Ok(());
        }
        let catalog = self.live_mut()?.catalog.clone();
        catalog
            .release_local(&PRESSURE_POLICY)
            .await
            .map_err(ThadamError::from)?;
        let usage = self.live_mut()?.budget.usage(category);
        if self.live_mut()?.budget.is_over_limit(category) {
            return Err(SegmentError::Budget {
                needed: bytes,
                used: usage.used,
                limit: usage.limit,
            }
            .into());
        }
        Ok(())
    }

    /// Drains `pending_eligible` into `Eligible` records (D18 step 3).
    async fn drain_eligible(&mut self) -> Result<(), ThadamError> {
        let at = self.live_mut()?.pending_eligible;
        if at.get() == 0 {
            return Ok(());
        }
        let catalog = self.live_mut()?.catalog.clone();
        catalog.mark_eligible(at).await.map_err(ThadamError::from)?;
        Ok(())
    }

    /// Advances the committed watermark if the catalog's coverage moved past
    /// it. The `segments::before_cursor_commit` failpoint fires exactly
    /// here: catalog record fsynced, new watermark not yet returned.
    fn advance_watermark(&mut self) -> Result<Option<WalPos>, ThadamError> {
        let names = self.engine.dataset_names();
        let l = self.live_mut()?;
        let w = l.catalog.watermark(&names);
        if w > l.watermark {
            let fps = self.failpoints.clone();
            if fps.hit(Failpoint::BeforeCursorCommit).is_err() {
                self.state = State::Dead;
                return Err(ThadamError::fatal(
                    ErrorDomain::Sink,
                    codes::SINK_IO,
                    "crashed at failpoint segments::before_cursor_commit",
                ));
            }
            let l = self.live_mut()?;
            l.watermark = w;
            return Ok(w);
        }
        Ok(None)
    }

    /// Finalizes every age-due open segment (markers for empty ones, which
    /// is how empty datasets keep the watermark moving), then advances.
    async fn commit_due(&mut self) -> Result<Option<WalPos>, ThadamError> {
        let Some(max_age) = self.engine.rotation.max_age else {
            return self.advance_watermark();
        };
        let due: Vec<String> = {
            let l = self.live_mut()?;
            let now = l.clock.monotonic();
            l.open
                .iter()
                .filter(|(_, seg)| now.duration_since(seg.opened_at) >= max_age)
                .map(|(name, _)| name.clone())
                .collect()
        };
        for name in due {
            self.finalize_one(&name).await?;
        }
        self.advance_watermark()
    }

    /// Encodes one dataset's events of one batch on the blocking pool,
    /// writes the finished batch into the open file, and returns any
    /// rejects with global indices plus the rows' event-time bounds.
    async fn encode_dataset(
        &mut self,
        job: EncodeJob<'_>,
        events: Vec<Event>,
        base: usize,
        rejects: &mut Vec<Reject>,
    ) -> Result<(), ThadamError> {
        let EncodeJob {
            name,
            props,
            origin,
            synced,
        } = job;
        let maker = self.make_encoder(name);
        let ctx = BatchContext {
            epoch: origin.epoch(),
            clock_synced: synced,
        };
        let encoder = {
            let l = self.live_mut()?;
            let seg = l
                .open
                .get_mut(name)
                .unwrap_or_else(|| unreachable!("the caller keeps this segment open"));
            seg.encoder.take().unwrap_or(maker)
        };
        let (enc, outcome) =
            tokio::task::spawn_blocking(move || encode_events(encoder, ctx, events))
                .await
                .map_err(|e| ThadamError::from(SegmentError::Join(e.to_string())))?;
        let outcome = match outcome {
            Ok(o) => o,
            Err(e) => {
                self.state = State::Dead;
                return Err(ThadamError::from(e));
            }
        };
        let schema = enc.arrow_schema();
        let EncodeOutcome {
            rejects: local,
            bounds,
            batch,
            ..
        } = outcome;

        {
            let l = self.live_mut()?;
            l.tick += 1;
            let tick = l.tick;
            let seg = l
                .open
                .get_mut(name)
                .unwrap_or_else(|| unreachable!("the sink does not yield this segment"));
            seg.encoder = Some(enc);
            seg.last_write = tick;
            seg.last = origin.last();
            seg.epoch = origin.epoch();
            seg.synced = synced;
            seg.bounds = merge_bounds(seg.bounds, bounds);
            for mut r in local {
                r.index = u32::try_from(base.saturating_add(r.index as usize)).unwrap_or(u32::MAX);
                rejects.push(r);
            }
        }

        if let Some(batch) = batch {
            let created = {
                let l = self.live_mut()?;
                let seg = l.open.get_mut(name).unwrap_or_else(|| unreachable!());
                seg.file.is_none()
            };
            if created {
                let dir = self.engine.dir.clone();
                let l = self.live_mut()?;
                l.write_seq = l.write_seq.wrapping_add(1);
                let seq = l.write_seq;
                let seg = l.open.get_mut(name).unwrap_or_else(|| unreachable!());
                let partition = seg.partition.ok_or_else(|| {
                    ThadamError::permanent(
                        ErrorDomain::Sink,
                        codes::SINK_WRITE_FAILED,
                        "internal: rows without a partition",
                    )
                })?;
                let file = OpenFile::create(&dir, seq, name, schema, props, partition, synced)
                    .map_err(ThadamError::from)?;
                seg.file = Some(file);
            }
            let rb = self.engine.row_group_bytes;
            let write = {
                let l = self.live_mut()?;
                l.open
                    .get_mut(name)
                    .and_then(|seg| seg.file.as_mut())
                    .map(|f| f.write_batch(&batch, rb))
            };
            if let Some(Err(e)) = write {
                self.state = State::Dead;
                return Err(ThadamError::from(e));
            }
        }
        Ok(())
    }

    fn make_encoder(&self, name: &str) -> Box<dyn RowsEncoder> {
        match self.plan(name) {
            Ok(plan) => plan.make_encoder(),
            // Callers validate the dataset against the plans first.
            Err(_) => unreachable!("dataset validated before encode"),
        }
    }
}

/// What came back together with the encoder after a blocking encode.
struct EncodeOutcome {
    rejects: Vec<Reject>,
    bounds: (Option<Timestamp>, Option<Timestamp>),
    batch: Option<RecordBatch>,
}

fn encode_events(
    mut enc: Box<dyn RowsEncoder>,
    ctx: BatchContext,
    events: Vec<Event>,
) -> (Box<dyn RowsEncoder>, Result<EncodeOutcome, SegmentError>) {
    enc.set_context(ctx);
    let mut outcome = EncodeOutcome {
        rejects: Vec::new(),
        bounds: (None, None),
        batch: None,
    };
    for ev in &events {
        match enc.push(ev) {
            Ok(()) => {
                if let Some(t) = ev.times().event_time() {
                    outcome.bounds.0 = Some(outcome.bounds.0.map_or(t, |m| m.min(t)));
                    outcome.bounds.1 = Some(outcome.bounds.1.map_or(t, |m| m.max(t)));
                }
            }
            Err(r) => outcome.rejects.push(r),
        }
    }
    match enc.finish_batch() {
        Ok(batch) => {
            if batch.num_rows() > 0 {
                outcome.batch = Some(batch);
            }
            (enc, Ok(outcome))
        }
        Err(e) => (enc, Err(e)),
    }
}

fn merge_bounds(
    a: (Option<Timestamp>, Option<Timestamp>),
    b: (Option<Timestamp>, Option<Timestamp>),
) -> (Option<Timestamp>, Option<Timestamp>) {
    let min = match (a.0, b.0) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (x, y) => x.or(y),
    };
    let max = match (a.1, b.1) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (x, y) => x.or(y),
    };
    (min, max)
}

/// The retention policy applied inline when the budget is over limit after a
/// finalize: uploaded files only, oldest first. Expired-then-uploaded is the
/// full order; a sink without a shipper has no uploaded files, so pressure
/// surfaces as a transient error instead of data loss.
const PRESSURE_POLICY: LocalRetention = LocalRetention {
    delete_after_upload: false,
    keep_for: None,
    keep_until_disk_pressure: true,
};

#[async_trait]
impl SegmentSink for ParquetSegmentSink {
    async fn recover(&mut self, ctx: &SinkContext) -> Result<Option<WalPos>, ThadamError> {
        if let Some(limit) = self.engine.max_disk {
            ctx.budget
                .set_limit(self.engine.budget_category, Some(limit));
        }
        // A previous Live instance charged the budget; release it so the
        // catalog's fresh open charges exactly what is on disk now.
        if let State::Live(l) = &self.state {
            let charged = l.catalog.charged_bytes();
            if charged > 0 {
                ctx.budget.release(self.engine.budget_category, charged);
            }
        }
        let dir = self.engine.dir.clone();
        let node_id = self.engine.node_id.clone();
        let names = self.engine.dataset_names();
        let budget = Arc::clone(&ctx.budget);
        let category = self.engine.budget_category;
        let open = tokio::task::spawn_blocking(move || {
            SegmentCatalog::open(dir, node_id, Some(budget), category, names.as_slice())
        });
        let (catalog, watermark) = open
            .await
            .map_err(|e| ThadamError::from(SegmentError::Join(e.to_string())))?
            .map_err(ThadamError::from)?;
        // The shipper service finds the catalog here through its `from` id
        // (last-write-wins, so a re-recovery replaces the stale handle).
        ctx.shared
            .publish(ctx.sink_id.as_str(), Arc::new(catalog.clone()));
        self.state = State::Live(Live {
            catalog,
            open: BTreeMap::new(),
            watermark,
            last_appended: watermark,
            pending_eligible: WalPos::new(0),
            write_seq: 0,
            tick: 0,
            clock: Arc::clone(&ctx.clock),
            budget: Arc::clone(&ctx.budget),
        });
        Ok(watermark)
    }

    async fn append(&mut self, batch: SegmentBatch) -> Result<Vec<Reject>, ThadamError> {
        {
            let l = self.live_mut()?;
            if let Some(last) = l.last_appended
                && batch.origin.first() <= last
            {
                return Err(ThadamError::permanent(
                    ErrorDomain::Sink,
                    codes::SINK_NON_MONOTONIC_APPEND,
                    format!(
                        "batch {} does not come after position {}",
                        batch.origin,
                        last.get()
                    ),
                ));
            }
        }
        let origin = batch.origin;
        let synced = batch.clock_synced;
        let rotation = self.engine.rotation;
        let mut rejects: Vec<Reject> = Vec::new();
        let mut base = 0usize;

        for ds in batch.datasets {
            let name = ds.dataset.to_string();
            let props = self.plan(&name)?.props.clone();

            // The segment exists from the first batch listing the dataset,
            // events or not (they advance together).
            if !self.live_mut()?.open.contains_key(&name) {
                if self.live_mut()?.open.len() >= self.engine.max_open_files {
                    let victim = {
                        let l = self.live_mut()?;
                        l.open
                            .iter()
                            .filter(|(n, _)| n.as_str() != name)
                            .min_by_key(|(_, s)| s.last_write)
                            .map(|(n, _)| n.clone())
                    };
                    if let Some(victim) = victim {
                        self.finalize_one(&victim).await?;
                    }
                }
                let l = self.live_mut()?;
                let now = l.clock.monotonic();
                let tick = l.tick;
                l.open.insert(
                    name.clone(),
                    OpenSegment::new(origin.first(), origin.epoch(), now, synced, tick),
                );
            }

            if !ds.events.is_empty() {
                // Partition rotation before encoding: the current batch's
                // events belong to the new partition's file, so the closed
                // file claims only through the previous append.
                let first_ingest = ds.events[0].times().ingest();
                let key = Partition::of(synced, first_ingest);
                let changed = {
                    let l = self.live_mut()?;
                    let seg = l.open.get_mut(&name).unwrap_or_else(|| unreachable!());
                    let changed = seg.partition.is_some_and(|p| p != key);
                    if seg.partition.is_none() {
                        seg.partition = Some(key);
                    }
                    changed
                };
                if changed {
                    self.finalize_one(&name).await?;
                    let l = self.live_mut()?;
                    let now = l.clock.monotonic();
                    let tick = l.tick;
                    let seg = l.open.entry(name.clone()).or_insert_with(|| {
                        OpenSegment::new(origin.first(), origin.epoch(), now, synced, tick)
                    });
                    seg.last = origin.last();
                    seg.epoch = origin.epoch();
                    seg.synced = synced;
                    seg.partition = Some(key);
                }

                // Claim the whole batch range before encoding, so a byte or
                // row rotation below closes a range that includes these
                // rows.
                {
                    let l = self.live_mut()?;
                    let seg = l.open.get_mut(&name).unwrap_or_else(|| unreachable!());
                    seg.last = origin.last();
                    seg.epoch = origin.epoch();
                    seg.synced = synced;
                }

                let events = ds.events;
                let events_len = events.len();
                self.encode_dataset(
                    EncodeJob {
                        name: &name,
                        props,
                        origin,
                        synced,
                    },
                    events,
                    base,
                    &mut rejects,
                )
                .await?;
                base += events_len;

                // Rotation by bytes or rows, checked after this batch wrote.
                let due = {
                    let l = self.live_mut()?;
                    l.open.get(&name).is_some_and(|seg| {
                        let size = seg.bytes_est();
                        (rotation.max_bytes.is_some_and(|m| size >= m) && seg.file.is_some())
                            || rotation.max_rows.is_some_and(|m| seg.rows() >= m)
                    })
                };
                if due {
                    self.finalize_one(&name).await?;
                }
            } else {
                let l = self.live_mut()?;
                let seg = l.open.get_mut(&name).unwrap_or_else(|| unreachable!());
                seg.last = origin.last();
                seg.epoch = origin.epoch();
                seg.synced = synced;
            }
        }

        let l = self.live_mut()?;
        l.last_appended = Some(origin.last());
        Ok(rejects)
    }

    async fn poll_commit(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.drain_eligible().await?;
        self.commit_due().await
    }

    async fn finalize_all(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.drain_eligible().await?;
        if self.engine.finalize_on_shutdown {
            let names: Vec<String> = {
                let l = self.live_mut()?;
                l.open.keys().cloned().collect()
            };
            for name in names {
                self.finalize_one(&name).await?;
            }
        }
        let new = self.advance_watermark()?;
        if new.is_some() {
            return Ok(new);
        }
        let l = self.live_mut()?;
        Ok(l.watermark)
    }

    fn cursor_committed(&mut self, pos: WalPos) {
        if let State::Live(l) = &mut self.state
            && pos > l.pending_eligible
        {
            l.pending_eligible = pos;
        }
    }
}

/// The `kind: parquet` factory registered by `thadam-cli`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ParquetSegmentSinkFactory;

impl ParquetSegmentSinkFactory {
    /// The factory instance.
    pub fn new() -> Self {
        ParquetSegmentSinkFactory
    }
}

impl SegmentSinkFactory for ParquetSegmentSinkFactory {
    fn kind(&self) -> &'static str {
        "parquet"
    }

    fn config_schema(&self) -> serde_json::Value {
        config::sink_config_schema()
    }

    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        config::parse_sink_config(cfg, PathBuf::new(), "sink").map(|_| ())
    }

    fn build(
        &self,
        cfg: serde_json::Value,
        ctx: &thadam_core::BuildContext,
    ) -> Result<Box<dyn SegmentSink>, ThadamError> {
        let parsed = config::parse_sink_config(&cfg, ctx.data_dir.clone(), &ctx.component_id)
            .and_then(|options| options.with_datasets(&ctx.datasets))
            .map_err(config::first_error)?;
        let sink = ParquetSegmentSink::new(parsed)?;
        Ok(Box::new(sink))
    }
}
