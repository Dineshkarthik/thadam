//! Opening (recovery) and the `Wal` handle.

use std::collections::{HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use thadam_core::{BudgetCategory, Clock, DiskBudget, WalEpoch, WalPos, WalRange};

use crate::cursor::{self, CursorState, validate_name};
use crate::error::WalError;
use crate::format::{self, SEG_HDR, ScanEnd, SegmentHeaderError, scan_frames};
use crate::fsutil::{remove_if_exists, sync_dir, sync_file, try_lock, write_atomic};
use crate::meta;
use crate::options::{StartFrom, WalOptions};
use crate::reader::WalReader;
use crate::retention::{self, RetentionReport};
use crate::shared::{Index, SegMeta, Shared, SharedInit, WriterStart, WriterState, lock};
use crate::writer::{self, WalWriter, WriterThread};

pub(crate) const LOCK_FILE: &str = "LOCK";
pub(crate) const CLEAN_MARKER: &str = "clean_shutdown";
const SEGMENTS_DIR: &str = "segments";
const CURSORS_DIR: &str = "cursors";

/// What [`open`] found and did.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RecoveryReport {
    /// The WAL's epoch.
    pub epoch: WalEpoch,
    /// The directory was empty and a new WAL was created.
    pub created: bool,
    /// The previous writer shut down cleanly.
    pub clean_shutdown: bool,
    /// Segments retained after recovery.
    pub segments: usize,
    /// The oldest retained frame position.
    pub first_seq: WalPos,
    /// The position the next frame will get.
    pub next_seq: WalPos,
    /// The last durable frame, if any.
    pub durable_end: Option<WalPos>,
    /// Bytes of the last segment that were scanned and verified.
    pub scanned_bytes: u64,
    /// Bytes removed from the end of the last segment.
    pub truncated_bytes: u64,
    /// The last segment ended in non-zero bytes that were not a valid frame.
    pub torn_tail: bool,
    /// The last segment ended in a zero-filled tail (clean end of a
    /// preallocated segment).
    pub zero_tail: bool,
    /// Leftover temporary files that were removed.
    pub removed_temp_files: usize,
    /// An empty last segment with an unreadable header was removed.
    pub removed_empty_last_segment: bool,
    /// Positions skipped because a cursor was at or beyond the recovered end
    /// (possible after a power loss under `interval` or `never`). They are
    /// never reused.
    pub skipped: Option<WalRange>,
    /// Cursors loaded.
    pub cursors: usize,
    /// Time spent in recovery.
    pub elapsed: Duration,
}

/// Counters and positions for metrics and `/status`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct WalStats {
    /// The WAL's epoch.
    pub epoch: WalEpoch,
    /// The oldest retained position.
    pub first_seq: WalPos,
    /// The position the next frame will get.
    pub next_seq: WalPos,
    /// The last durable frame (0: none).
    pub durable_end: WalPos,
    /// Writer thread state.
    pub writer: WriterState,
    /// Retained segment files.
    pub segments: usize,
    /// Bytes held by retained segment files.
    pub disk_bytes: u64,
    /// Frames written since open.
    pub frames_written: u64,
    /// Frame bytes written since open.
    pub bytes_written: u64,
    /// Group commits since open.
    pub groups: u64,
    /// Segment flushes since open.
    pub fsyncs: u64,
    /// Segments deleted by retention.
    pub deleted_segments: u64,
    /// Bytes deleted by retention.
    pub deleted_bytes: u64,
    /// Segments evicted under `drop_oldest` before every required consumer
    /// had read them.
    pub evicted_segments: u64,
    /// Bytes evicted (`thadam_wal_evicted_bytes_total`).
    pub evicted_bytes: u64,
    /// Times a required consumer stopped pinning retention after exceeding
    /// `max_lag`.
    pub lag_evictions: u64,
    /// `Gap::Corrupt` items handed to readers.
    pub corrupt_gaps: u64,
    /// Times the writer waited for budget under `on_full: block`.
    pub budget_waits: u64,
}

/// Opens (creating if needed) the WAL in `dir` and runs recovery. Blocking;
/// call it from a blocking thread.
///
/// Recovery never allocates space in an existing WAL: it validates every
/// segment header, scans and truncates the last segment, and leaves creating
/// the next segment to the first append. `budget` is charged for the segments
/// found; its [`BudgetCategory::Wal`] limit is the WAL's `max_disk`.
pub fn open(
    dir: &Path,
    opts: WalOptions,
    budget: Arc<DiskBudget>,
    clock: Arc<dyn Clock>,
) -> Result<(Wal, RecoveryReport), WalError> {
    let started = Instant::now();
    opts.validate(budget.limit(BudgetCategory::Wal))?;
    fs::create_dir_all(dir).map_err(|e| WalError::io("create directory", dir, e))?;
    let lock_path = dir.join(LOCK_FILE);
    let lock_file = try_lock(&lock_path)
        .map_err(|e| WalError::io("lock", &lock_path, e))?
        .ok_or(WalError::Locked { path: lock_path })?;

    let seg_dir = dir.join(SEGMENTS_DIR);
    let cur_dir = dir.join(CURSORS_DIR);
    let (meta, created) = match meta::load(dir)? {
        Some(m) => (m, false),
        None => {
            if has_segments(&seg_dir)? {
                return Err(WalError::Corrupt(format!(
                    "{} holds segments but no {}",
                    dir.display(),
                    meta::META_FILE
                )));
            }
            for d in [&seg_dir, &cur_dir] {
                fs::create_dir_all(d).map_err(|e| WalError::io("create directory", d, e))?;
            }
            sync_dir(dir).map_err(|e| WalError::io("fsync", dir, e))?;
            let m = meta::create(dir, opts.node_id.clone(), opts.machine_id.clone())?;
            (m, true)
        }
    };
    meta::check_binding(&meta, opts.node_id.as_deref(), opts.machine_id.as_deref())?;
    for d in [&seg_dir, &cur_dir] {
        if !d.is_dir() {
            return Err(WalError::Corrupt(format!("{} is missing", d.display())));
        }
    }
    let epoch = meta.epoch;
    let clean_path = dir.join(CLEAN_MARKER);
    let clean_shutdown =
        remove_if_exists(&clean_path).map_err(|e| WalError::io("remove", &clean_path, e))?;

    let opened_at = clock.monotonic();
    let mut rec = Recovered::default();
    let mut segs = list_segments(&seg_dir, &mut rec)?;
    let segs = validate_headers(&mut segs, epoch, opened_at, &mut rec)?;
    let mut index = Index {
        segs: VecDeque::from(segs),
    };

    let mut next_seq = rec.removed_base.unwrap_or(1).max(1);
    let mut durable = 0;
    let mut reuse = None;
    if let Some(last) = index.segs.back_mut() {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&last.path)
            .map_err(|e| WalError::io("open", &last.path, e))?;
        let len = file
            .metadata()
            .map_err(|e| WalError::io("stat", &last.path, e))?
            .len();
        let scan = scan_frames(
            &file,
            len,
            SEG_HDR,
            last.base,
            u64::MAX,
            opts.frame_max_bytes,
        )
        .map_err(|e| WalError::io("scan", &last.path, e))?;
        rec.scanned = scan.end;
        rec.torn = matches!(scan.stop, ScanEnd::Bad(_));
        rec.zero_tail = scan.stop == ScanEnd::Zero && scan.end < len;
        if scan.end < len {
            // Bytes past the last valid frame are never appended to in place:
            // a re-appended frame could line up with a stale one and make it
            // valid again (S1 mutant test).
            file.set_len(scan.end)
                .map_err(|e| WalError::io("truncate", &last.path, e))?;
            rec.truncated = len - scan.end;
            last.bytes = scan.end;
        }
        sync_file(&file).map_err(|e| WalError::io("fsync", &last.path, e))?;
        durable = scan.next_seq - 1;
        next_seq = next_seq.max(scan.next_seq);
        if scan.frames == 0 {
            reuse = Some((last.base, last.path.clone(), scan.end));
        }
    }

    let cursors = load_cursors(&cur_dir, epoch)?;
    let high = cursors
        .values()
        .map(CursorState::high_water)
        .max()
        .unwrap_or(0);
    let mut skipped = None;
    if high >= next_seq {
        skipped = WalRange::new(epoch, WalPos::new(next_seq), WalPos::new(high)).ok();
        next_seq = high + 1;
    }
    let reuse = reuse.filter(|(base, _, _)| *base == next_seq);
    let first_seq = index.segs.front().map_or(next_seq, |s| s.base);

    let report = RecoveryReport {
        epoch,
        created,
        clean_shutdown,
        segments: index.segs.len(),
        first_seq: WalPos::new(first_seq),
        next_seq: WalPos::new(next_seq),
        durable_end: (durable > 0).then_some(WalPos::new(durable)),
        scanned_bytes: rec.scanned,
        truncated_bytes: rec.truncated,
        torn_tail: rec.torn,
        zero_tail: rec.zero_tail,
        removed_temp_files: rec.removed_temp,
        removed_empty_last_segment: rec.removed_base.is_some(),
        skipped,
        cursors: cursors.len(),
        elapsed: started.elapsed(),
    };
    let shared = Shared::new(SharedInit {
        dir: dir.to_owned(),
        seg_dir,
        cur_dir,
        epoch,
        opts,
        budget,
        clock,
        lock: lock_file,
        index,
        cursors,
        durable,
        start: WriterStart { next_seq, reuse },
    });
    let wal = Wal {
        inner: Arc::new(WalInner {
            sh: Arc::new(shared),
            writer: Mutex::new(None),
        }),
    };
    Ok((wal, report))
}

#[derive(Default)]
struct Recovered {
    scanned: u64,
    truncated: u64,
    torn: bool,
    zero_tail: bool,
    removed_temp: usize,
    removed_base: Option<u64>,
}

fn has_segments(seg_dir: &Path) -> Result<bool, WalError> {
    match fs::read_dir(seg_dir) {
        Ok(rd) => Ok(rd
            .filter_map(Result::ok)
            .any(|e| e.file_name().to_string_lossy().ends_with(".seg"))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(WalError::io("list", seg_dir, e)),
    }
}

fn list_segments(
    seg_dir: &Path,
    rec: &mut Recovered,
) -> Result<Vec<(u64, std::path::PathBuf)>, WalError> {
    let mut out = Vec::new();
    let rd = fs::read_dir(seg_dir).map_err(|e| WalError::io("list", seg_dir, e))?;
    for entry in rd {
        let entry = entry.map_err(|e| WalError::io("list", seg_dir, e))?;
        let path = entry.path();
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.ends_with(".tmp") {
            // A segment that never got its final name; removing it frees space.
            remove_if_exists(&path).map_err(|e| WalError::io("remove", &path, e))?;
            rec.removed_temp += 1;
        } else if let Some(base) = format::parse_segment_file_name(name) {
            out.push((base, path));
        }
    }
    if rec.removed_temp > 0 {
        sync_dir(seg_dir).map_err(|e| WalError::io("fsync", seg_dir, e))?;
    }
    out.sort_unstable_by_key(|(b, _)| *b);
    Ok(out)
}

fn validate_headers(
    found: &mut Vec<(u64, std::path::PathBuf)>,
    epoch: WalEpoch,
    opened_at: Instant,
    rec: &mut Recovered,
) -> Result<Vec<SegMeta>, WalError> {
    let n = found.len();
    let mut segs = Vec::with_capacity(n);
    for (i, (base, path)) in found.drain(..).enumerate() {
        let file = File::open(&path).map_err(|e| WalError::io("open", &path, e))?;
        let len = file
            .metadata()
            .map_err(|e| WalError::io("stat", &path, e))?
            .len();
        let mut h = [0u8; SEG_HDR as usize];
        let got =
            format::read_full_at(&file, &mut h, 0).map_err(|e| WalError::io("read", &path, e))?;
        match format::decode_segment_header(&h[..got]) {
            Ok(hdr) => {
                if hdr.epoch != epoch.get() {
                    return Err(WalError::Incompatible(format!(
                        "segment {} belongs to epoch {:x}, the wal is epoch {epoch}",
                        path.display(),
                        hdr.epoch
                    )));
                }
                if hdr.base != base {
                    return Err(WalError::Corrupt(format!(
                        "segment {} has base {} in its header",
                        path.display(),
                        hdr.base
                    )));
                }
            }
            Err(SegmentHeaderError::UnsupportedVersion(v)) => {
                return Err(WalError::Incompatible(format!(
                    "segment {}: {}",
                    path.display(),
                    SegmentHeaderError::UnsupportedVersion(v).describe()
                )));
            }
            Err(e) if i + 1 == n && len <= SEG_HDR => {
                // An empty last segment holds no frames, so removing it loses
                // nothing; its base still bounds the next sequence number.
                let _ = e;
                remove_if_exists(&path).map_err(|e| WalError::io("remove", &path, e))?;
                if let Some(parent) = path.parent() {
                    sync_dir(parent).map_err(|e| WalError::io("fsync", parent, e))?;
                }
                rec.removed_base = Some(base);
                continue;
            }
            Err(e) => {
                return Err(WalError::Corrupt(format!(
                    "segment {}: {}",
                    path.display(),
                    e.describe()
                )));
            }
        }
        segs.push(SegMeta {
            base,
            path,
            bytes: len,
            created_at: opened_at,
            sealed_at: Some(opened_at),
        });
    }
    Ok(segs)
}

fn load_cursors(cur_dir: &Path, epoch: WalEpoch) -> Result<HashMap<String, CursorState>, WalError> {
    let mut out = HashMap::new();
    let rd = fs::read_dir(cur_dir).map_err(|e| WalError::io("list", cur_dir, e))?;
    for entry in rd {
        let entry = entry.map_err(|e| WalError::io("list", cur_dir, e))?;
        let path = entry.path();
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.ends_with(".cur.tmp") {
            remove_if_exists(&path).map_err(|e| WalError::io("remove", &path, e))?;
            continue;
        }
        let Some(cursor) = name.strip_suffix(".cur") else {
            continue;
        };
        if validate_name(cursor).is_err() {
            continue;
        }
        let bytes = fs::read(&path).map_err(|e| WalError::io("read", &path, e))?;
        out.insert(cursor.to_owned(), cursor::decode(&path, epoch, &bytes)?);
    }
    Ok(out)
}

/// A handle to an open WAL. Cheap to clone.
///
/// Dropping the last clone shuts the writer down gracefully and joins its
/// thread, which blocks for at most one group commit; prefer [`Wal::close`]
/// in async code. The directory lock is released once every `Wal`,
/// [`WalWriter`], [`WalReader`], and pending ticket is gone.
#[derive(Clone)]
pub struct Wal {
    inner: Arc<WalInner>,
}

struct WalInner {
    sh: Arc<Shared>,
    writer: Mutex<Option<WriterThread>>,
}

impl Drop for WalInner {
    fn drop(&mut self) {
        if let Some(h) = lock(&self.writer).take() {
            self.sh.closing.store(true, Ordering::SeqCst);
            self.sh.notify_space();
            let _ = h.stop();
        }
    }
}

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("dir", &self.inner.sh.dir)
            .field("epoch", &self.inner.sh.epoch)
            .finish_non_exhaustive()
    }
}

impl Wal {
    fn sh(&self) -> &Arc<Shared> {
        &self.inner.sh
    }

    /// The WAL's epoch.
    pub fn epoch(&self) -> WalEpoch {
        self.sh().epoch
    }

    /// Spawns the writer OS thread, or returns the running writer.
    pub fn start_writer(&self) -> Result<WalWriter, WalError> {
        let mut slot = lock(&self.inner.writer);
        if let Some(h) = slot.as_ref() {
            return Ok(h.handle().clone());
        }
        if let Some(e) = self.sh().poison.get() {
            return Err(e.clone());
        }
        if self.sh().closing.load(Ordering::SeqCst) {
            return Err(WalError::Closed);
        }
        let thread = writer::spawn(Arc::clone(self.sh()))?;
        let handle = thread.handle().clone();
        *slot = Some(thread);
        Ok(handle)
    }

    /// Stops the writer after a final group commit and flush, and waits for
    /// it. Submissions after this fail with [`WalError::Closed`]. Returns the
    /// writer's failure, if it had one.
    pub async fn close(&self) -> Result<(), WalError> {
        let Some(h) = lock(&self.inner.writer).take() else {
            return Ok(());
        };
        self.sh().closing.store(true, Ordering::SeqCst);
        self.sh().notify_space();
        tokio::task::spawn_blocking(move || h.stop())
            .await
            .map_err(|_| WalError::Corrupt("wal writer thread panicked".into()))?
    }

    /// Opens a reader. A committed cursor named `cursor` sets its position;
    /// otherwise `start` does.
    pub fn reader(&self, cursor: &str, start: StartFrom) -> Result<WalReader, WalError> {
        validate_name(cursor)?;
        let sh = self.sh();
        let committed = lock(&sh.cursors).get(cursor).map(|c| c.committed.get());
        let pos = match committed {
            Some(c) => c + 1,
            None => match start {
                StartFrom::Earliest => lock(&sh.index)
                    .segs
                    .front()
                    .map_or(sh.durable() + 1, |s| s.base),
                StartFrom::Latest => sh.durable() + 1,
                StartFrom::At(p) => p.get().max(1),
            },
        };
        Ok(WalReader::new(Arc::clone(sh), cursor.to_owned(), pos))
    }

    /// The last committed state of `cursor`.
    pub fn cursor(&self, cursor: &str) -> Option<CursorState> {
        lock(&self.sh().cursors).get(cursor).copied()
    }

    /// Durably records `state` for `cursor` (temporary file, flush, rename,
    /// directory flush) on the blocking pool. The committed position may not
    /// pass the durable end. Callers batch commits (ADR-0004:
    /// `cursor_commit_interval`, 200 ms) because each one costs two flushes.
    pub async fn commit_cursor(&self, cursor: &str, state: CursorState) -> Result<(), WalError> {
        self.check_cursor(cursor, &state)?;
        let sh = Arc::clone(self.sh());
        let name = cursor.to_owned();
        tokio::task::spawn_blocking(move || store_cursor(&sh, &name, state))
            .await
            .map_err(|_| WalError::Corrupt("cursor commit task panicked".into()))?
    }

    /// [`Wal::commit_cursor`] for callers already on a blocking thread.
    pub fn commit_cursor_blocking(&self, cursor: &str, state: CursorState) -> Result<(), WalError> {
        self.check_cursor(cursor, &state)?;
        store_cursor(self.sh(), cursor, state)
    }

    fn check_cursor(&self, cursor: &str, state: &CursorState) -> Result<(), WalError> {
        validate_name(cursor)?;
        let sh = self.sh();
        let durable = sh.durable();
        let previous = lock(&sh.cursors)
            .get(cursor)
            .map_or(0, |c| c.committed.get());
        let c = state.committed.get();
        if c > durable && c > previous {
            return Err(WalError::invalid(format!(
                "cursor {cursor}: committed position {c} is beyond the durable end {durable}"
            )));
        }
        if let Some(r) = state.inflight {
            if r.epoch() != sh.epoch {
                return Err(WalError::invalid(format!(
                    "cursor {cursor}: in-flight range is from epoch {}, the wal is epoch {}",
                    r.epoch(),
                    sh.epoch
                )));
            }
            if r.last().get() > durable {
                return Err(WalError::invalid(format!(
                    "cursor {cursor}: in-flight range {r} is beyond the durable end {durable}"
                )));
            }
        }
        Ok(())
    }

    /// One retention pass: deletes segments every required consumer has
    /// passed (subject to `retain_for`), applies `max_lag`, and frees space
    /// down to one segment of headroom under the disk budget, evicting under
    /// `on_full: drop_oldest`. Called periodically by the maintenance task.
    pub async fn maintain(&self) -> Result<RetentionReport, WalError> {
        let sh = Arc::clone(self.sh());
        tokio::task::spawn_blocking(move || retention::maintain(&sh))
            .await
            .map_err(|_| WalError::Corrupt("maintenance task panicked".into()))?
    }

    /// [`Wal::maintain`] for callers already on a blocking thread.
    pub fn maintain_blocking(&self) -> Result<RetentionReport, WalError> {
        retention::maintain(self.sh())
    }

    /// The last durable position, if any frame is durable.
    pub fn durable_end(&self) -> Option<WalPos> {
        let d = self.sh().durable();
        (d > 0).then_some(WalPos::new(d))
    }

    /// Counters and positions.
    pub fn stats(&self) -> WalStats {
        let sh = self.sh();
        let status = *sh.status.borrow();
        let next_seq = sh.next_seq.load(Ordering::Relaxed);
        let (first_seq, segments, disk_bytes) = {
            let idx = lock(&sh.index);
            (
                idx.segs.front().map_or(next_seq, |s| s.base),
                idx.segs.len(),
                idx.segs.iter().map(|s| s.bytes).sum(),
            )
        };
        let c = &sh.counters;
        let get = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
        WalStats {
            epoch: sh.epoch,
            first_seq: WalPos::new(first_seq),
            next_seq: WalPos::new(next_seq),
            durable_end: WalPos::new(status.durable),
            writer: status.state,
            segments,
            disk_bytes,
            frames_written: get(&c.frames),
            bytes_written: get(&c.bytes_written),
            groups: get(&c.groups),
            fsyncs: get(&c.fsyncs),
            deleted_segments: get(&c.deleted_segments),
            deleted_bytes: get(&c.deleted_bytes),
            evicted_segments: get(&c.evicted_segments),
            evicted_bytes: get(&c.evicted_bytes),
            lag_evictions: get(&c.lag_evictions),
            corrupt_gaps: get(&c.corrupt_gaps),
            budget_waits: get(&c.budget_waits),
        }
    }
}

fn store_cursor(sh: &Shared, name: &str, state: CursorState) -> Result<(), WalError> {
    let path = sh.cur_dir.join(format!("{name}.cur"));
    {
        let _io = lock(&sh.cursor_io);
        write_atomic(&path, &cursor::encode(sh.epoch, &state))
            .map_err(|e| WalError::io("commit cursor", &path, e))?;
        lock(&sh.cursors).insert(name.to_owned(), state);
    }
    sh.notify_space();
    Ok(())
}
