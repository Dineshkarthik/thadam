//! The segment catalog: an append-only, CRC-checked `catalog.log` plus the
//! in-memory view the shipper reads.
//!
//! # Recovery semantics (ARCHITECTURE.md section 11, step 4)
//!
//! [`SegmentCatalog::open`] deletes everything under `staging/`, replays the
//! log, and rebuilds `Finalized` records for `ready/` files the log does not
//! mention (by reading their Parquet footers — this is why the footer carries
//! `thadam.wal_range`). It then computes, for the sink's configured datasets,
//! the highest WAL position every one of them covers contiguously (`W`), and
//! **drops every non-eligible record beyond `W`**, deleting its file:
//!
//! - Records at or below `W` are complete waves: every dataset's rows up to
//!   `W` are durable, so the WAL never replays them.
//! - A `Finalized` record beyond `W` is partial: some dataset's matching
//!   segment is missing, so the WAL replays from `W` on. Keeping the file
//!   would put the replayed rows in two files; deleting it is safe because
//!   its rows were never reported durable (the watermark only advances at
//!   complete-wave boundaries).
//! - `Eligible` and `Uploaded` records are never dropped: the WAL cursor
//!   already passed their end, so those ranges never replay.
//!
//! The catalog is shared through `Arc`; its mutex is held only across
//! in-memory updates, never across `.await`. Log appends serialize through a
//! single-writer mutex and fdatasync on the blocking pool.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thadam_core::{BudgetCategory, DiskBudget, Timestamp, WalPos, WalRange};
use tokio::sync::watch;

use self::log::LogWriter;
use record::{CatalogRecord, DeletedRecord, EligibleRecord, SegmentState, UploadedRecord, unhex};

use crate::error::SegmentError;
use crate::partition;
pub use record::{SegmentId, SegmentRecord};

pub(crate) mod log;
pub(crate) mod record;

/// Bumped on every applied batch of log records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CatalogVersion(pub u64);

/// An in-memory catalog entry.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub(crate) struct SegmentEntry {
    pub(crate) record: SegmentRecord,
    pub(crate) state: SegmentState,
    pub(crate) key: Option<String>,
    pub(crate) etag: Option<String>,
}

impl SegmentEntry {
    /// Whether the entry proves its WAL range is durably off the WAL replay
    /// path (finalized locally, or shipped and optionally deleted locally).
    fn counts_for_coverage(&self) -> bool {
        match self.state {
            SegmentState::Deleted { uploaded } => uploaded,
            _ => true,
        }
    }

    /// Bytes on local disk right now (uploaded-but-not-released and
    /// pre-upload files).
    fn on_disk(&self) -> bool {
        self.record.has_file()
            && matches!(
                self.state,
                SegmentState::Finalized
                    | SegmentState::Eligible { .. }
                    | SegmentState::Uploaded { .. }
            )
    }
}

/// Local-retention policy for [`SegmentCatalog::release_local`]
/// (ARCHITECTURE.md section 8, "Local retention modes").
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LocalRetention {
    /// Files may be deleted once uploaded; equivalent to `keep_for` zero.
    pub delete_after_upload: bool,
    /// Keep uploaded files at least this long after their upload completed.
    pub keep_for: Option<Duration>,
    /// Allow deleting uploaded-but-unexpired files when the budget is under
    /// pressure (eviction order: uploaded and expired, then uploaded).
    pub keep_until_disk_pressure: bool,
}

impl LocalRetention {
    /// Uploaded files are deleted as soon as the budget needs the space.
    pub fn delete_after_upload() -> Self {
        LocalRetention {
            delete_after_upload: true,
            keep_for: None,
            keep_until_disk_pressure: true,
        }
    }

    /// Keep uploaded files for `keep_for`; only expired ones leave under
    /// pressure.
    pub fn keep_for(keep_for: Duration) -> Self {
        LocalRetention {
            delete_after_upload: false,
            keep_for: Some(keep_for),
            keep_until_disk_pressure: false,
        }
    }
}

/// What [`SegmentCatalog::release_local`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReleaseReport {
    /// Files unlinked.
    pub deleted_files: u64,
    /// Bytes those files occupied.
    pub deleted_bytes: u64,
    /// The category is still over its limit after eviction
    /// (`uploaded + expired`, then `uploaded` were not enough): the caller
    /// applies backpressure.
    pub pressure: bool,
}

struct Inner {
    root: PathBuf,
    node_id: String,
    state: Mutex<CatalogState>,
    log: Mutex<LogWriter>,
    version: watch::Sender<CatalogVersion>,
    budget: Option<(Arc<DiskBudget>, BudgetCategory)>,
    charged: AtomicU64,
}

#[derive(Default)]
struct CatalogState {
    segments: BTreeMap<String, SegmentEntry>,
}

fn lock<'a, T>(m: &'a Mutex<T>) -> MutexGuard<'a, T> {
    // A holder that panicked mid-update leaves nothing inconsistent: every
    // update applies whole records after their fsynced log append.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// The shared segment catalog.
///
/// Clone freely: clones share the log, the in-memory state, and the
/// subscription feed.
#[derive(Clone)]
pub struct SegmentCatalog {
    inner: Arc<Inner>,
}

impl SegmentCatalog {
    /// Opens (or creates) the catalog at `root`, the sink's directory, and
    /// performs startup recovery for the configured `datasets`: delete
    /// `staging/`, replay the log (truncating a torn tail), rebuild records
    /// for `ready/` files the log misses, and drop non-eligible records
    /// beyond the highest position every dataset covers (`W`).
    ///
    /// Returns the catalog and that position, or `None` when the sink holds
    /// nothing durable. Blocks on filesystem IO; call from a blocking
    /// context.
    pub fn open(
        root: impl Into<PathBuf>,
        node_id: impl Into<String>,
        budget: Option<Arc<DiskBudget>>,
        category: BudgetCategory,
        datasets: &[String],
    ) -> Result<(Self, Option<WalPos>), SegmentError> {
        let root = root.into();
        log::create_dirs(&root)?;
        log::create_dirs(&root.join("staging"))?;
        log::create_dirs(&root.join("ready"))?;
        clean_staging(&root)?;

        let log_path = root.join("catalog.log");
        let (mut writer, replayed) = if log_path.exists() {
            let rep = log::replay(&log_path)?;
            let writer = LogWriter::resume(&log_path, rep.end, rep.truncated)?;
            (writer, rep.records)
        } else {
            (LogWriter::create(&log_path)?, Vec::new())
        };

        let mut state = CatalogState::default();
        for record in &replayed {
            apply_ref(&mut state, record);
        }

        // Adopt ready files the log does not know (crash between rename and
        // catalog append). Their footers make them whole again.
        let rebuilt = rebuild_from_footers(&root, &state)?;
        if !rebuilt.is_empty() {
            writer.append(&rebuilt)?;
            for record in &rebuilt {
                apply_ref(&mut state, record);
            }
        }

        let watermark = watermark_of(&state, datasets.iter().map(String::as_str));
        let drops = drop_beyond_watermark(&root, &state, watermark)?;
        if !drops.is_empty() {
            writer.append(&drops)?;
            for record in &drops {
                apply_ref(&mut state, record);
            }
        }
        let gaps = watermark.map_or_else(Vec::new, |w| bridge_gaps_below(&state, datasets, w));
        if !gaps.is_empty() {
            writer.append(&gaps)?;
            for record in &gaps {
                apply_ref(&mut state, record);
            }
        }

        let total = live_bytes(&state);
        if let Some(b) = &budget {
            b.charge(category, total);
        }
        let (tx, _) = watch::channel(CatalogVersion(0));
        let versioned = SegmentCatalog {
            inner: Arc::new(Inner {
                root,
                node_id: node_id.into(),
                state: Mutex::new(state),
                log: Mutex::new(writer),
                version: tx,
                budget: budget.map(|b| (b, category)),
                charged: AtomicU64::new(total),
            }),
        };
        Ok((versioned, watermark))
    }

    /// The directory the catalog manages.
    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    /// The node id files were written under.
    pub fn node_id(&self) -> &str {
        &self.inner.node_id
    }

    /// Segments ready for the shipper: `Finalized` and `Eligible` through
    /// cursor commit, oldest WAL range first. Coverage markers are not files
    /// and never appear here.
    pub fn eligible_for_upload(&self) -> Vec<SegmentRecord> {
        let state = lock(&self.inner.state);
        let mut out: Vec<SegmentRecord> = state
            .segments
            .values()
            .filter(|e| e.record.has_file() && matches!(e.state, SegmentState::Eligible { .. }))
            .map(|e| e.record.clone())
            .collect();
        out.sort_by_key(|r| (r.range.first(), r.dataset.clone()));
        out
    }

    /// Appends a `Finalized` record for `segment` (fsynced on the blocking
    /// pool) and applies it.
    pub async fn append_finalized(&self, segment: SegmentRecord) -> Result<(), SegmentError> {
        let inner = Arc::clone(&self.inner);
        let record = CatalogRecord::from(&segment);
        spawn_append(inner, vec![record]).await
    }

    /// Synchronous variant for callers already on a blocking thread (the
    /// finalize path runs its fsyncs there).
    pub(crate) fn append_finalized_sync(
        &self,
        segment: &SegmentRecord,
    ) -> Result<(), SegmentError> {
        let record = CatalogRecord::from(segment);
        append_apply(&self.inner, vec![record])
    }

    /// Marks every finalized segment whose range ends at or below `pos`
    /// eligible for shipping (D18 step 3). Idempotent. Returns how many
    /// records were appended.
    pub async fn mark_eligible(&self, pos: WalPos) -> Result<usize, SegmentError> {
        let records: Vec<CatalogRecord> = {
            let state = lock(&self.inner.state);
            state
                .segments
                .values()
                .filter(|e| {
                    e.record.has_file()
                        && matches!(e.state, SegmentState::Finalized)
                        && e.record.range.last() <= pos
                })
                .map(|e| {
                    CatalogRecord::Eligible(EligibleRecord {
                        id: e.record.id.0.clone(),
                        pos: pos.get(),
                    })
                })
                .collect()
        };
        let n = records.len();
        if n == 0 {
            return Ok(0);
        }
        let inner = Arc::clone(&self.inner);
        spawn_append(inner, records).await?;
        Ok(n)
    }

    /// Records a finished upload. The segment must be eligible: the shipper
    /// never uploads an ineligible file.
    pub async fn mark_uploaded(
        &self,
        id: &SegmentId,
        key: &str,
        etag: Option<&str>,
    ) -> Result<(), SegmentError> {
        {
            let state = lock(&self.inner.state);
            match state.segments.get(id.as_str()).map(|e| e.state) {
                Some(SegmentState::Uploaded { .. }) => return Ok(()),
                Some(SegmentState::Finalized) => {
                    return Err(SegmentError::catalog(format!(
                        "segment {id} is not eligible for upload"
                    )));
                }
                Some(SegmentState::Eligible { .. }) => {}
                Some(SegmentState::Deleted { .. }) => {
                    return Err(SegmentError::catalog(format!("segment {id} is deleted")));
                }
                None => return Err(SegmentError::catalog(format!("unknown segment {id}"))),
            }
        }
        let inner = Arc::clone(&self.inner);
        let record = CatalogRecord::Uploaded(UploadedRecord {
            id: id.0.clone(),
            key: key.to_owned(),
            etag: etag.map(str::to_owned),
            uploaded_unix: now_unix(),
        });
        spawn_append(inner, vec![record]).await
    }

    /// Deletes local files per `policy`, in the eviction order *uploaded and
    /// expired*, then *uploaded* (only under `keep_until_disk_pressure`).
    /// `pressure` in the report tells the caller to apply backpressure.
    /// Files that were never uploaded are never deleted here: their rows
    /// exist only locally.
    pub async fn release_local(
        &self,
        policy: &LocalRetention,
    ) -> Result<ReleaseReport, SegmentError> {
        let inner = Arc::clone(&self.inner);
        let policy = *policy;
        tokio::task::spawn_blocking(move || release_local_sync(&inner, &policy))
            .await
            .map_err(|e| SegmentError::Join(e.to_string()))?
    }

    /// Changes to the catalog: bumped once per appended batch.
    pub fn subscribe(&self) -> watch::Receiver<CatalogVersion> {
        self.inner.version.subscribe()
    }

    /// Bytes charged to the shared budget by files on local disk.
    pub fn charged_bytes(&self) -> u64 {
        self.inner.charged.load(Ordering::Relaxed)
    }

    /// The watermark over `datasets`: the highest WAL position every one of
    /// them covers contiguously through live records.
    pub(crate) fn watermark(&self, datasets: &[String]) -> Option<WalPos> {
        let state = lock(&self.inner.state);
        watermark_of(&state, datasets.iter().map(String::as_str))
    }

    #[cfg(test)]
    /// Test hook: appends caller-built records (explicit upload times, etc.)
    /// through the real log path.
    #[cfg(test)]
    pub(crate) fn append_records_now(
        &self,
        records: Vec<CatalogRecord>,
    ) -> Result<(), SegmentError> {
        append_apply(&self.inner, records)
    }
}

async fn spawn_append(inner: Arc<Inner>, records: Vec<CatalogRecord>) -> Result<(), SegmentError> {
    tokio::task::spawn_blocking(move || append_apply(&inner, records))
        .await
        .map_err(|e| SegmentError::Join(e.to_string()))?
}

/// Appends records to the fsynced log, then applies them to memory and bumps
/// the version. Log-then-memory keeps the on-disk order authoritative.
fn append_apply(inner: &Arc<Inner>, records: Vec<CatalogRecord>) -> Result<(), SegmentError> {
    {
        lock(&inner.log).append(&records)?;
    }
    let mut state = lock(&inner.state);
    for record in &records {
        apply_ref(&mut state, record);
    }
    drop(state);
    reconcile_charge(inner);
    bump(inner);
    Ok(())
}

fn bump(inner: &Arc<Inner>) {
    let current = inner.version.borrow().0;
    let _ = inner.version.send(CatalogVersion(current.wrapping_add(1)));
}

fn live_bytes(state: &CatalogState) -> u64 {
    state
        .segments
        .values()
        .filter(|e| e.on_disk())
        .map(|e| e.record.bytes)
        .sum()
}

fn reconcile_charge(inner: &Arc<Inner>) {
    let target = {
        let state = lock(&inner.state);
        live_bytes(&state)
    };
    let Some((budget, category)) = &inner.budget else {
        inner.charged.store(target, Ordering::Relaxed);
        return;
    };
    let mut cur = inner.charged.load(Ordering::Relaxed);
    loop {
        if cur == target {
            return;
        }
        match inner
            .charged
            .compare_exchange(cur, target, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => {
                if cur < target {
                    budget.charge(*category, target - cur);
                } else {
                    budget.release(*category, cur - target);
                }
                return;
            }
            Err(actual) => cur = actual,
        }
    }
}

fn apply_ref(state: &mut CatalogState, record: &CatalogRecord) {
    match record {
        CatalogRecord::Finalized(f) => {
            // Rebuilding the same file twice, or replaying over an applied
            // record, is idempotent.
            if state.segments.contains_key(&f.id) {
                return;
            }
            let checksum = unhex(&f.sha256).unwrap_or([0; 32]);
            let range = WalRange::new(
                thadam_core::WalEpoch::new(f.epoch),
                WalPos::new(f.first),
                WalPos::new(f.last),
            )
            .unwrap_or_else(|_| {
                // Records in the log were valid when written; degrade a
                // hand-edited one to a point range rather than failing the
                // whole recovery.
                WalRange::single(thadam_core::WalEpoch::new(f.epoch), WalPos::new(f.last))
            });
            state.segments.insert(
                f.id.clone(),
                SegmentEntry {
                    record: SegmentRecord {
                        id: SegmentId(f.id.clone()),
                        dataset: f.dataset.clone(),
                        path: f.path.clone(),
                        range,
                        rows: f.rows,
                        bytes: f.bytes,
                        min_event_time: f.min_event_nanos.map(Timestamp::from_nanos),
                        max_event_time: f.max_event_nanos.map(Timestamp::from_nanos),
                        checksum,
                        clock_synced: f.clock_synced,
                    },
                    state: SegmentState::Finalized,
                    key: None,
                    etag: None,
                },
            );
        }
        CatalogRecord::Eligible(e) => {
            if let Some(entry) = state.segments.get_mut(&e.id)
                && matches!(entry.state, SegmentState::Finalized)
            {
                entry.state = SegmentState::Eligible {
                    pos: WalPos::new(e.pos),
                };
            }
        }
        CatalogRecord::Uploaded(u) => {
            if let Some(entry) = state.segments.get_mut(&u.id)
                && matches!(
                    entry.state,
                    SegmentState::Finalized | SegmentState::Eligible { .. }
                )
            {
                entry.state = SegmentState::Uploaded {
                    uploaded_unix: u.uploaded_unix,
                };
                entry.key = Some(u.key.clone());
                entry.etag = u.etag.clone();
            }
        }
        CatalogRecord::Deleted(d) => {
            if d.uploaded {
                if let Some(entry) = state.segments.get_mut(&d.id) {
                    entry.state = SegmentState::Deleted { uploaded: true };
                }
            } else {
                // Dropped by recovery before ever being eligible: its rows
                // regenerate from the WAL, so it must not count for coverage.
                state.segments.remove(&d.id);
            }
        }
    }
}

/// Chains the live, coverage-counting records of one dataset: `(start, end,
/// epoch)` of the longest contiguous run beginning at the earliest record
/// start. Overlapping records extend nothing; an epoch change stops the
/// chain.
fn coverage_of(
    state: &CatalogState,
    dataset: &str,
) -> Option<(WalPos, WalPos, thadam_core::WalEpoch)> {
    let mut ranges: Vec<WalRange> = state
        .segments
        .values()
        .filter(|e| e.record.dataset == dataset && e.counts_for_coverage())
        .map(|e| e.record.range)
        .collect();
    ranges.sort_by_key(|r| (r.first(), r.last()));
    let first = ranges.first().copied()?;
    let epoch = first.epoch();
    let (start, mut end) = (first.first(), first.last());
    for r in ranges.iter().skip(1) {
        if r.epoch() != epoch {
            break;
        }
        if r.first().get() > end.get().saturating_add(1) {
            break;
        }
        if r.last() > end {
            end = r.last();
        }
    }
    Some((start, end, epoch))
}

fn watermark_of<'a>(
    state: &CatalogState,
    datasets: impl Iterator<Item = &'a str>,
) -> Option<WalPos> {
    let mut min_end: Option<WalPos> = None;
    for d in datasets {
        let (_, end, _) = coverage_of(state, d)?;
        min_end = Some(match min_end {
            Some(m) if m <= end => m,
            _ => end,
        });
    }
    min_end
}

/// After recovery dropped the non-eligible records beyond `watermark`, a
/// dataset's kept records may end below it (its rows up to `watermark` are
/// complete in the WAL's view, so replay skips them). A coverage marker
/// bridges each such gap — the same records the sink would have written for
/// empty segments had no crash happened — so the watermark keeps moving
/// forward after replay instead of stalling at a hole that has no rows.
fn bridge_gaps_below(
    state: &CatalogState,
    datasets: &[String],
    watermark: WalPos,
) -> Vec<CatalogRecord> {
    let mut out = Vec::new();
    for d in datasets {
        let Some((_, end, epoch)) = coverage_of(state, d) else {
            continue;
        };
        if end >= watermark {
            continue;
        }
        let Some(first) = end.checked_next() else {
            continue;
        };
        let Ok(range) = WalRange::new(epoch, first, watermark) else {
            continue;
        };
        let record = SegmentRecord {
            id: SegmentId::marker(d, range),
            dataset: d.clone(),
            path: None,
            range,
            rows: 0,
            bytes: 0,
            min_event_time: None,
            max_event_time: None,
            checksum: [0; 32],
            clock_synced: true,
        };
        out.push(CatalogRecord::from(&record));
    }
    out
}

fn clean_staging(root: &Path) -> Result<(), SegmentError> {
    let staging = root.join("staging");
    let mut touched = BTreeSet::new();
    let entries = match std::fs::read_dir(&staging) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(SegmentError::io("read_dir", &staging, e)),
    };
    for entry in entries {
        let entry = entry.map_err(|e| SegmentError::io("read_dir", &staging, e))?;
        let dir = entry.path();
        if dir.is_dir() {
            let files =
                std::fs::read_dir(&dir).map_err(|e| SegmentError::io("read_dir", &dir, e))?;
            for f in files {
                let f = f.map_err(|e| SegmentError::io("read_dir", &dir, e))?;
                std::fs::remove_file(f.path())
                    .map_err(|e| SegmentError::io("unlink staging", f.path(), e))?;
                touched.insert(dir.clone());
            }
            // Keep the (now empty) dataset subdirectories: removing them
            // would need an extra fsync dance for no benefit.
        } else {
            std::fs::remove_file(&dir).map_err(|e| SegmentError::io("unlink staging", &dir, e))?;
            touched.insert(staging.clone());
        }
    }
    for d in &touched {
        log::fsync_dir(d).map_err(|e| SegmentError::io("fsync staging", d, e))?;
    }
    if !touched.is_empty() {
        log::fsync_dir(&staging).map_err(|e| SegmentError::io("fsync staging", &staging, e))?;
    }
    Ok(())
}

/// Adopts `ready/` files the catalog state does not know, reading footer
/// metadata. Returns the records to append.
fn rebuild_from_footers(
    root: &Path,
    state: &CatalogState,
) -> Result<Vec<CatalogRecord>, SegmentError> {
    let ready = root.join("ready");
    let mut files = Vec::new();
    walk_ready(&ready, 0, &mut files)?;
    let mut records = Vec::new();
    for path in files {
        let Some(name) = path.file_name().and_then(|n| n.to_str()).map(str::to_owned) else {
            continue;
        };
        if partition::parse_segment_name(&name).is_none() {
            continue; // foreign file: leave alone
        }
        let Ok(rel) = path.strip_prefix(root).map(Path::to_path_buf) else {
            continue;
        };
        // The id is the dataset-scoped relative path, matching the writer
        // (file names alone collide across datasets).
        let id = rel.to_string_lossy().to_string();
        if state.segments.contains_key(&id) {
            continue;
        }
        let dataset = rel
            .components()
            .nth(1)
            .and_then(|c| c.as_os_str().to_str())
            .unwrap_or_default()
            .to_owned();
        let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        match footer_facts(&path) {
            Ok((range, clock_synced, rows, footer_dataset)) => {
                if let Some(d) = footer_dataset
                    && d != dataset
                {
                    tracing::warn!(file = %path.display(), dataset = %d, dir_dataset = %dataset,
                        "footer dataset differs from the directory; the directory wins");
                }
                // The footer's wal range is authoritative; the file name was
                // its fallback inside `footer_facts`.
                let record = SegmentRecord {
                    id: SegmentId(id),
                    dataset,
                    path: Some(rel),
                    range,
                    rows,
                    bytes,
                    // The checksum and event-time stats are not recovered
                    // from the footer at rebuild time.
                    min_event_time: None,
                    max_event_time: None,
                    checksum: [0; 32],
                    clock_synced,
                };
                records.push(CatalogRecord::from(&record));
            }
            Err(e) => {
                tracing::warn!(file = %path.display(), error = %e,
                    "unreadable segment footer; leaving the file out of the catalog");
            }
        }
    }
    Ok(records)
}

fn walk_ready(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) -> Result<(), SegmentError> {
    if depth > 4 {
        return Ok(());
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(SegmentError::io("read_dir", dir, e)),
    };
    for entry in entries {
        let entry = entry.map_err(|e| SegmentError::io("read_dir", dir, e))?;
        let path = entry.path();
        if path.is_dir() {
            walk_ready(&path, depth + 1, out)?;
        } else if path.extension().is_some_and(|e| e == "parquet") {
            out.push(path);
        }
    }
    Ok(())
}

/// `(wal range, clock-synced, rows, footer dataset)` from a segment's
/// footer, falling back to the file name for the range when the metadata is
/// absent.
fn footer_facts(path: &Path) -> Result<(WalRange, bool, u64, Option<String>), SegmentError> {
    let file = std::fs::File::open(path).map_err(|e| SegmentError::io("open footer", path, e))?;
    let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
        .parse_and_finish(&file)
        .map_err(|e| SegmentError::catalog(format!("{}: {e}", path.display())))?;
    let kvs = metadata.file_metadata().key_value_metadata();
    let get = |key: &str| {
        kvs.and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == key)
                .and_then(|kv| kv.value.clone())
        })
    };
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_default();
    let range = get("thadam.wal_range")
        .and_then(|v| partition::parse_wal_range(&v))
        .or_else(|| {
            partition::parse_segment_name(name)
                .and_then(|n| WalRange::new(n.epoch, n.first, n.last).ok())
        })
        .ok_or_else(|| SegmentError::catalog(format!("{}: no wal range", path.display())))?;
    let clock_synced = get("thadam.clock_synced").is_none_or(|v| v == "true");
    let rows: i64 = (0..metadata.num_row_groups())
        .map(|i| metadata.row_group(i).num_rows())
        .sum();
    let rows = u64::try_from(rows).unwrap_or(0);
    Ok((range, clock_synced, rows, get("thadam.dataset")))
}

/// The non-eligible records that extend past `watermark`: their files are
/// deleted and `Deleted { uploaded: false }` records returned. Applying
/// those records removes the entries from `state`.
fn drop_beyond_watermark(
    root: &Path,
    state: &CatalogState,
    watermark: Option<WalPos>,
) -> Result<Vec<CatalogRecord>, SegmentError> {
    let dropped: Vec<SegmentEntry> = state
        .segments
        .values()
        .filter(|e| {
            matches!(e.state, SegmentState::Finalized)
                && watermark.is_none_or(|w| e.record.range.last() > w)
        })
        .cloned()
        .collect();
    if dropped.is_empty() {
        return Ok(Vec::new());
    }
    let mut touched = BTreeSet::new();
    for entry in &dropped {
        if let Some(rel) = &entry.record.path {
            let abs = root.join(rel);
            match std::fs::remove_file(&abs) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(SegmentError::io("unlink dropped", &abs, e)),
            }
            if let Some(parent) = abs.parent() {
                touched.insert(parent.to_path_buf());
            }
        }
    }
    for d in &touched {
        log::fsync_dir(d).map_err(|e| SegmentError::io("fsync dropped dir", d, e))?;
    }
    let now = now_unix();
    Ok(dropped
        .into_iter()
        .map(|e| {
            CatalogRecord::Deleted(DeletedRecord {
                id: e.record.id.0,
                uploaded: false,
                deleted_unix: now,
            })
        })
        .collect())
}

fn release_local_sync(
    inner: &Arc<Inner>,
    policy: &LocalRetention,
) -> Result<ReleaseReport, SegmentError> {
    let over = |inner: &Arc<Inner>| -> bool {
        inner
            .budget
            .as_ref()
            .is_some_and(|(b, c)| b.is_over_limit(*c))
    };
    // Keep window in seconds: `delete_after_upload` folds into keep-for zero.
    let keep_secs = if policy.delete_after_upload {
        Some(Duration::ZERO.as_secs())
    } else {
        policy.keep_for.map(|d| d.as_secs())
    };

    let now = now_unix();
    let mut deleted_files = 0u64;
    let mut deleted_bytes = 0u64;

    // Phase 1: uploaded and expired, oldest uploads first.
    if let Some(keep) = keep_secs {
        loop {
            let candidate = {
                let state = lock(&inner.state);
                uploaded_candidates(&state)
                    .into_iter()
                    .filter(|(_, unix)| unix.saturating_add(keep) <= now)
                    .min_by_key(|(_, unix)| *unix)
            };
            let Some((id, _)) = candidate else { break };
            deleted_bytes = deleted_bytes.saturating_add(delete_one(inner, &id)?);
            deleted_files += 1;
        }
    }

    // Phase 2: uploaded, even unexpired, while the pressure lasts.
    if policy.keep_until_disk_pressure && over(inner) {
        loop {
            let candidate = {
                let state = lock(&inner.state);
                uploaded_candidates(&state)
                    .into_iter()
                    .min_by_key(|(_, unix)| *unix)
            };
            let Some((id, _)) = candidate else { break };
            deleted_bytes = deleted_bytes.saturating_add(delete_one(inner, &id)?);
            deleted_files += 1;
            if !over(inner) {
                break;
            }
        }
    }

    Ok(ReleaseReport {
        deleted_files,
        deleted_bytes,
        pressure: over(inner),
    })
}

fn uploaded_candidates(state: &CatalogState) -> Vec<(SegmentId, u64)> {
    state
        .segments
        .values()
        .filter_map(|e| match (&e.record.path, e.state) {
            (Some(_), SegmentState::Uploaded { uploaded_unix }) => {
                Some((e.record.id.clone(), uploaded_unix))
            }
            _ => None,
        })
        .collect()
}

/// Unlinks one uploaded segment's file and appends its `Deleted` record.
/// Returns the deleted file's size in bytes.
fn delete_one(inner: &Arc<Inner>, id: &SegmentId) -> Result<u64, SegmentError> {
    let (rel, bytes) = {
        let state = lock(&inner.state);
        let Some(entry) = state.segments.get(id.as_str()) else {
            return Ok(0);
        };
        match (&entry.record.path, entry.state) {
            (Some(rel), SegmentState::Uploaded { .. }) => (rel.clone(), entry.record.bytes),
            _ => return Ok(0),
        }
    };
    let abs = inner.root.join(&rel);
    match std::fs::remove_file(&abs) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(SegmentError::io("unlink uploaded", &abs, e)),
    }
    if let Some(parent) = abs.parent() {
        let _ = log::fsync_dir(parent);
    }
    append_apply(
        inner,
        vec![CatalogRecord::Deleted(DeletedRecord {
            id: id.0.clone(),
            uploaded: true,
            deleted_unix: now_unix(),
        })],
    )?;
    Ok(bytes)
}
