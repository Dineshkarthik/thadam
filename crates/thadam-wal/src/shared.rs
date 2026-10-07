//! State shared by the writer thread, readers, cursor commits, and retention.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use thadam_core::{BudgetCategory, BudgetExceeded, Clock, DiskBudget, WalEpoch};
use tokio::sync::watch;

use crate::cursor::CursorState;
use crate::error::WalError;
use crate::options::WalOptions;

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One segment file.
#[derive(Clone, Debug)]
pub(crate) struct SegMeta {
    pub base: u64,
    pub path: PathBuf,
    /// Bytes charged to the disk budget for this file.
    pub bytes: u64,
    /// Monotonic creation time (the open time for segments found at startup).
    pub created_at: Instant,
    /// Monotonic seal time; `None` while the writer appends to it.
    pub sealed_at: Option<Instant>,
}

/// Segments in base order. The newest segment is never deleted: it carries the
/// sequence high-water mark across restarts.
#[derive(Debug, Default)]
pub(crate) struct Index {
    pub segs: VecDeque<SegMeta>,
}

impl Index {
    /// Position of the segment that holds `seq`, if any segment starts at or
    /// before it.
    pub(crate) fn segment_for(&self, seq: u64) -> Option<usize> {
        self.segs.partition_point(|s| s.base <= seq).checked_sub(1)
    }
}

/// What the writer thread is doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriterState {
    /// [`Wal::start_writer`](crate::Wal::start_writer) has not been called.
    NotStarted,
    /// Accepting submissions.
    Running,
    /// Shut down cleanly; nothing more will become durable.
    Closed,
    /// Stopped after a write or `fsync` failure; the WAL must be reopened.
    Failed,
}

/// Published through a `watch` channel to readers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Status {
    /// The last durable frame (0: none). Readers and cursor commits never go
    /// past it.
    pub durable: u64,
    pub state: WriterState,
}

#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub frames: AtomicU64,
    pub bytes_written: AtomicU64,
    pub groups: AtomicU64,
    pub fsyncs: AtomicU64,
    pub deleted_segments: AtomicU64,
    pub deleted_bytes: AtomicU64,
    pub evicted_segments: AtomicU64,
    pub evicted_bytes: AtomicU64,
    pub lag_evictions: AtomicU64,
    pub corrupt_gaps: AtomicU64,
    pub budget_waits: AtomicU64,
}

impl Counters {
    pub(crate) fn add(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Ordering::Relaxed);
    }
}

/// Where the writer starts, as determined by recovery.
#[derive(Debug)]
pub(crate) struct WriterStart {
    pub next_seq: u64,
    /// An empty last segment at `next_seq` that can be appended to without
    /// allocating a new file.
    pub reuse: Option<(u64, PathBuf, u64)>,
}

pub(crate) struct Shared {
    pub dir: PathBuf,
    pub seg_dir: PathBuf,
    pub cur_dir: PathBuf,
    pub epoch: WalEpoch,
    pub opts: WalOptions,
    pub budget: Arc<DiskBudget>,
    pub clock: Arc<dyn Clock>,
    pub _lock: File,
    pub index: Mutex<Index>,
    pub cursors: Mutex<HashMap<String, CursorState>>,
    pub cursor_io: Mutex<()>,
    pub status: watch::Sender<Status>,
    pub next_seq: AtomicU64,
    space: Mutex<u64>,
    space_cv: Condvar,
    pub closing: AtomicBool,
    pub poison: OnceLock<WalError>,
    charged: AtomicU64,
    pub counters: Counters,
    pub lagging: Mutex<HashSet<String>>,
    pub start: Mutex<Option<WriterStart>>,
}

pub(crate) struct SharedInit {
    pub dir: PathBuf,
    pub seg_dir: PathBuf,
    pub cur_dir: PathBuf,
    pub epoch: WalEpoch,
    pub opts: WalOptions,
    pub budget: Arc<DiskBudget>,
    pub clock: Arc<dyn Clock>,
    pub lock: File,
    pub index: Index,
    pub cursors: HashMap<String, CursorState>,
    pub durable: u64,
    pub start: WriterStart,
}

impl Shared {
    pub(crate) fn new(init: SharedInit) -> Shared {
        let charged: u64 = init.index.segs.iter().map(|s| s.bytes).sum();
        init.budget.charge(BudgetCategory::Wal, charged);
        let (status, _) = watch::channel(Status {
            durable: init.durable,
            state: WriterState::NotStarted,
        });
        Shared {
            dir: init.dir,
            seg_dir: init.seg_dir,
            cur_dir: init.cur_dir,
            epoch: init.epoch,
            opts: init.opts,
            budget: init.budget,
            clock: init.clock,
            _lock: init.lock,
            index: Mutex::new(init.index),
            cursors: Mutex::new(init.cursors),
            cursor_io: Mutex::new(()),
            status,
            next_seq: AtomicU64::new(init.start.next_seq),
            space: Mutex::new(0),
            space_cv: Condvar::new(),
            closing: AtomicBool::new(false),
            poison: OnceLock::new(),
            charged: AtomicU64::new(charged),
            counters: Counters::default(),
            lagging: Mutex::new(HashSet::new()),
            start: Mutex::new(Some(init.start)),
        }
    }

    pub(crate) fn durable(&self) -> u64 {
        self.status.borrow().durable
    }

    pub(crate) fn try_reserve(&self, n: u64) -> Result<(), BudgetExceeded> {
        self.budget.try_reserve(BudgetCategory::Wal, n)?;
        self.charged.fetch_add(n, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn release(&self, n: u64) {
        if n == 0 {
            return;
        }
        self.budget.release(BudgetCategory::Wal, n);
        saturating_sub(&self.charged, n);
    }

    /// Wakes a writer waiting for budget under `on_full: block`.
    pub(crate) fn notify_space(&self) {
        *lock(&self.space) += 1;
        self.space_cv.notify_all();
    }

    pub(crate) fn wait_for_space(&self, timeout: Duration) {
        let g = lock(&self.space);
        let _ = self.space_cv.wait_timeout(g, timeout);
    }

    pub(crate) fn stopped_error(&self) -> WalError {
        self.poison.get().cloned().unwrap_or(WalError::Closed)
    }

    pub(crate) fn now(&self) -> Instant {
        self.clock.monotonic()
    }

    pub(crate) fn segment_path(&self, base: u64) -> PathBuf {
        self.seg_dir.join(crate::format::segment_file_name(base))
    }
}

fn saturating_sub(a: &AtomicU64, n: u64) {
    let mut cur = a.load(Ordering::Relaxed);
    while let Err(actual) = a.compare_exchange_weak(
        cur,
        cur.saturating_sub(n),
        Ordering::Relaxed,
        Ordering::Relaxed,
    ) {
        cur = actual;
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        let n = self.charged.load(Ordering::Relaxed);
        if n > 0 {
            self.budget.release(BudgetCategory::Wal, n);
        }
    }
}
