//! The writer thread and its submission handle.
//!
//! One OS thread owns the writable segment file. It drains submissions into a
//! group (up to `group_max_bytes`), performs one `write` per group, flushes per
//! the fsync policy, publishes the new durable end, and only then resolves the
//! group's tickets, in submission order.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use prost::Message;
use thadam_core::{RawBatch, WalPos, WalRange};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

use crate::error::WalError;
use crate::format::{FRAME_HDR, SEG_HDR, begin_frame, encode_segment_header, finish_frame};
use crate::fsutil::{preallocate, remove_if_exists, sync_dir, sync_file, write_atomic};
use crate::meta::unix_now;
use crate::options::{Fsync, OnFull};
use crate::proto::RawFrame;
use crate::retention;
use crate::shared::{Counters, SegMeta, Shared, WriterState, lock};
use crate::wal::CLEAN_MARKER;

type Reply = oneshot::Sender<Result<WalRange, WalError>>;

pub(crate) enum Msg {
    Batch(Submission),
    Close,
}

pub(crate) struct Submission {
    frame: RawFrame,
    len: usize,
    reply: Reply,
    _permits: (OwnedSemaphorePermit, OwnedSemaphorePermit),
}

/// Submits batches to the writer thread. Cheap to clone.
#[derive(Clone)]
pub struct WalWriter {
    tx: mpsc::Sender<Msg>,
    count: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    sh: Arc<Shared>,
}

impl std::fmt::Debug for WalWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalWriter").finish_non_exhaustive()
    }
}

impl WalWriter {
    /// Queues `batch` as one frame. Waits for a slot in the bounded submission
    /// queue (a count bound plus a byte bound; a batch larger than the byte
    /// bound takes all of it). The returned ticket resolves once the frame is
    /// durable per the fsync policy.
    ///
    /// Cancel-safe: dropping the future before it returns queues nothing.
    pub async fn submit(&self, batch: RawBatch) -> Result<CommitTicket, WalError> {
        self.check()?;
        let frame = RawFrame::from_batch(&batch);
        let len = frame.encoded_len();
        let max = self.sh.opts.frame_max_bytes;
        if len > max {
            return Err(WalError::FrameTooLarge { size: len, max });
        }
        let count = Arc::clone(&self.count)
            .acquire_owned()
            .await
            .map_err(|_| WalError::Closed)?;
        let want = len.min(self.sh.opts.max_pending_bytes);
        let bytes = Arc::clone(&self.bytes)
            .acquire_many_owned(u32::try_from(want).unwrap_or(u32::MAX))
            .await
            .map_err(|_| WalError::Closed)?;
        self.check()?;
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Msg::Batch(Submission {
                frame,
                len,
                reply,
                _permits: (count, bytes),
            }))
            .map_err(|_| self.sh.stopped_error())?;
        Ok(CommitTicket {
            rx,
            sh: Arc::clone(&self.sh),
        })
    }

    fn check(&self) -> Result<(), WalError> {
        if let Some(e) = self.sh.poison.get() {
            return Err(e.clone());
        }
        if self.sh.closing.load(Ordering::SeqCst) {
            return Err(WalError::Closed);
        }
        Ok(())
    }
}

/// Resolves to the frame's position once it is durable, or to the error that
/// prevented it. Never resolves `Ok` for a frame written after a failed
/// `fsync`.
#[must_use = "a commit ticket must be awaited before acknowledging the source"]
pub struct CommitTicket {
    rx: oneshot::Receiver<Result<WalRange, WalError>>,
    sh: Arc<Shared>,
}

impl std::fmt::Debug for CommitTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitTicket").finish_non_exhaustive()
    }
}

impl Future for CommitTicket {
    type Output = Result<WalRange, WalError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            Poll::Ready(Err(_)) => Poll::Ready(Err(self.sh.stopped_error())),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The running writer thread.
pub(crate) struct WriterThread {
    tx: mpsc::Sender<Msg>,
    thread: JoinHandle<Result<(), WalError>>,
    handle: WalWriter,
}

impl WriterThread {
    pub(crate) fn handle(&self) -> &WalWriter {
        &self.handle
    }

    /// Asks the writer to finish (final group commit, flush, seal) and waits.
    pub(crate) fn stop(self) -> Result<(), WalError> {
        let _ = self.tx.send(Msg::Close);
        drop(self.handle);
        self.thread
            .join()
            .unwrap_or_else(|_| Err(WalError::Corrupt("wal writer thread panicked".into())))
    }
}

pub(crate) fn spawn(sh: Arc<Shared>) -> Result<WriterThread, WalError> {
    let (tx, rx) = mpsc::channel();
    let start = lock(&sh.start).take();
    let (next_seq, reuse) = match start {
        Some(s) => (s.next_seq, s.reuse),
        None => (sh.next_seq.load(Ordering::SeqCst), None),
    };
    let handle = WalWriter {
        tx: tx.clone(),
        count: Arc::new(Semaphore::new(sh.opts.max_pending_batches)),
        bytes: Arc::new(Semaphore::new(sh.opts.max_pending_bytes)),
        sh: Arc::clone(&sh),
    };
    let writer = Writer {
        prealloc: sh.opts.preallocate_effective(),
        sh: Arc::clone(&sh),
        rx,
        active: None,
        reuse,
        next_seq,
        buf: Vec::with_capacity(sh.opts.group_max_bytes + sh.opts.frame_max_bytes + FRAME_HDR),
        staged: Vec::new(),
        written: VecDeque::new(),
        last_sync: None,
    };
    sh.status.send_modify(|s| s.state = WriterState::Running);
    let thread = std::thread::Builder::new()
        .name("thadam-wal-writer".into())
        .spawn(move || writer.run())
        .map_err(|e| {
            sh.status.send_modify(|s| s.state = WriterState::Failed);
            WalError::io("spawn writer thread", &sh.dir, e)
        })?;
    Ok(WriterThread { tx, thread, handle })
}

struct Pending {
    seq: u64,
    reply: Reply,
}

struct Active {
    file: File,
    path: PathBuf,
    base: u64,
    /// Bytes written (header plus frames).
    len: u64,
    /// Length covered by tickets that resolved `Ok`.
    acked_len: u64,
    unsynced: bool,
    /// Bytes reserved by preallocation (0 without it).
    prealloc_len: u64,
}

impl Active {
    fn charged(&self) -> u64 {
        self.len.max(self.prealloc_len)
    }
}

enum Exit {
    Closed,
    #[cfg(any(test, feature = "failpoints"))]
    Crashed,
}

struct Writer {
    sh: Arc<Shared>,
    rx: mpsc::Receiver<Msg>,
    active: Option<Active>,
    reuse: Option<(u64, PathBuf, u64)>,
    next_seq: u64,
    /// The group being built: encoded frames not yet written.
    buf: Vec<u8>,
    staged: Vec<Pending>,
    /// Written, waiting for the flush that makes them durable.
    written: VecDeque<Pending>,
    last_sync: Option<Instant>,
    prealloc: bool,
}

/// Errors that fail one submission without stopping the writer.
fn is_per_submission(e: &WalError) -> bool {
    matches!(e, WalError::BudgetExhausted(_) | WalError::Closed)
}

impl Writer {
    fn run(mut self) -> Result<(), WalError> {
        match self.main_loop() {
            Ok(Exit::Closed) => {
                self.sh
                    .status
                    .send_modify(|s| s.state = WriterState::Closed);
                Ok(())
            }
            #[cfg(any(test, feature = "failpoints"))]
            Ok(Exit::Crashed) => {
                self.sh
                    .status
                    .send_modify(|s| s.state = WriterState::Closed);
                Ok(())
            }
            Err(e) => {
                self.poison(e.clone());
                Err(e)
            }
        }
    }

    fn main_loop(&mut self) -> Result<Exit, WalError> {
        let group_max = self.sh.opts.group_max_bytes;
        loop {
            let first = match self.flush_deadline() {
                None => self.rx.recv().ok(),
                Some(wait) => match self.rx.recv_timeout(wait) {
                    Ok(m) => Some(m),
                    Err(RecvTimeoutError::Timeout) => {
                        self.sync_now()?;
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => None,
                },
            };
            let mut close = false;
            match first {
                None | Some(Msg::Close) => close = true,
                Some(Msg::Batch(s)) => {
                    let mut group = self.stage(s)?;
                    while group < group_max {
                        match self.rx.try_recv() {
                            Ok(Msg::Batch(s)) => group += self.stage(s)?,
                            Ok(Msg::Close) => {
                                close = true;
                                break;
                            }
                            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                        }
                    }
                }
            }
            if close {
                return self.finish();
            }
            self.write_staged()?;
            match self.sh.opts.fsync {
                Fsync::Always => self.sync_now()?,
                Fsync::Never => self.ack_written(),
                Fsync::Interval(d) => {
                    if self.since_sync().is_none_or(|e| e >= d) {
                        self.sync_now()?;
                    }
                }
            }
        }
    }

    fn since_sync(&self) -> Option<Duration> {
        self.last_sync.map(|t| t.elapsed())
    }

    /// How long until written-but-unflushed data must be flushed. The first
    /// group after an idle period is flushed at once.
    fn flush_deadline(&self) -> Option<Duration> {
        match self.sh.opts.fsync {
            Fsync::Interval(d) if !self.written.is_empty() => Some(
                self.since_sync()
                    .map_or(Duration::ZERO, |e| d.saturating_sub(e)),
            ),
            _ => None,
        }
    }

    /// Encodes one submission into the group. Returns the bytes it added.
    fn stage(&mut self, s: Submission) -> Result<usize, WalError> {
        let frame_len = FRAME_HDR + s.len;
        if let Err(e) = self.make_room(frame_len as u64) {
            if is_per_submission(&e) {
                let _ = s.reply.send(Err(e));
                return Ok(0);
            }
            let _ = s.reply.send(Err(e.clone()));
            return Err(e);
        }
        let start = begin_frame(&mut self.buf);
        s.frame.encode_raw(&mut self.buf);
        finish_frame(&mut self.buf, start, self.next_seq);
        self.staged.push(Pending {
            seq: self.next_seq,
            reply: s.reply,
        });
        self.next_seq += 1;
        self.sh.next_seq.store(self.next_seq, Ordering::Relaxed);
        Ok(frame_len)
    }

    /// Makes sure the next frame fits the active segment and the disk budget.
    fn make_room(&mut self, frame_len: u64) -> Result<(), WalError> {
        let seg_max = self.sh.opts.segment_bytes;
        match &self.active {
            None => self.open_segment()?,
            Some(a) => {
                let used = a.len + self.buf.len() as u64;
                if used > SEG_HDR && used + frame_len > seg_max {
                    self.roll()?;
                }
            }
        }
        if self.active.as_ref().is_some_and(|a| a.prealloc_len > 0) {
            // The whole preallocated segment was reserved when it was opened.
            return Ok(());
        }
        self.reserve(frame_len)
    }

    /// Reserves `n` bytes of WAL budget, reclaiming first (delete before
    /// allocate) and then applying `on_full`.
    fn reserve(&mut self, n: u64) -> Result<(), WalError> {
        let mut made_durable = false;
        let mut waited = false;
        loop {
            let exceeded = match self.sh.try_reserve(n) {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };
            if !made_durable {
                // Consumers can only free space by reading what is durable.
                self.write_staged()?;
                match self.sh.opts.fsync {
                    Fsync::Never => self.ack_written(),
                    _ => self.sync_now()?,
                }
                made_durable = true;
            }
            let freed = retention::reclaim_for(&self.sh, n)?;
            if freed > 0 {
                continue;
            }
            match self.sh.opts.on_full {
                OnFull::DropOldest => return Err(WalError::BudgetExhausted(exceeded)),
                OnFull::Block => {
                    if self.sh.closing.load(Ordering::SeqCst) {
                        return Err(WalError::Closed);
                    }
                    if !waited {
                        Counters::add(&self.sh.counters.budget_waits, 1);
                        waited = true;
                    }
                    self.sh.wait_for_space(Duration::from_millis(20));
                }
            }
        }
    }

    /// Opens the segment the next frame goes to. Created lazily, never by
    /// `open` or recovery.
    fn open_segment(&mut self) -> Result<(), WalError> {
        let base = self.next_seq;
        let seg_max = self.sh.opts.segment_bytes;
        if let Some((rbase, path, len)) = self.reuse.take()
            && rbase == base
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|e| WalError::io("open", &path, e))?;
            let mut prealloc_len = 0;
            if self.prealloc {
                self.reserve(seg_max.saturating_sub(len))?;
                preallocate(&file, seg_max).map_err(|e| WalError::io("preallocate", &path, e))?;
                prealloc_len = seg_max;
            }
            {
                let mut idx = lock(&self.sh.index);
                if let Some(seg) = idx.segs.back_mut().filter(|s| s.base == rbase) {
                    seg.sealed_at = None;
                    seg.bytes = len.max(prealloc_len);
                }
            }
            #[cfg(any(test, feature = "failpoints"))]
            self.sh.opts.failpoints.record_synced(path.clone(), len);
            self.active = Some(Active {
                file,
                path,
                base,
                len,
                acked_len: len,
                unsynced: false,
                prealloc_len,
            });
            return Ok(());
        }
        let alloc = if self.prealloc { seg_max } else { SEG_HDR };
        self.reserve(alloc)?;
        let path = self.sh.segment_path(base);
        let file = match self.create_segment_file(base, &path) {
            Ok(f) => f,
            Err(e) => {
                self.sh.release(alloc);
                return Err(e);
            }
        };
        let now = self.sh.now();
        lock(&self.sh.index).segs.push_back(SegMeta {
            base,
            path: path.clone(),
            bytes: alloc,
            created_at: now,
            sealed_at: None,
        });
        #[cfg(any(test, feature = "failpoints"))]
        self.sh.opts.failpoints.record_synced(path.clone(), SEG_HDR);
        self.active = Some(Active {
            file,
            path,
            base,
            len: SEG_HDR,
            acked_len: SEG_HDR,
            unsynced: false,
            prealloc_len: if self.prealloc { seg_max } else { 0 },
        });
        Ok(())
    }

    /// Temporary file, header, optional preallocation, flush, rename, then a
    /// directory flush: a half-written segment never has a valid name.
    fn create_segment_file(&self, base: u64, path: &PathBuf) -> Result<File, WalError> {
        let mut tmp_name = path.as_os_str().to_owned();
        tmp_name.push(".tmp");
        let tmp = PathBuf::from(tmp_name);
        let result = (|| -> io::Result<File> {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all_at(
                &encode_segment_header(self.sh.epoch.get(), base, unix_now()),
                0,
            )?;
            if self.prealloc {
                preallocate(&f, self.sh.opts.segment_bytes)?;
            }
            self.fsync_point()?;
            sync_file(&f)?;
            std::fs::rename(&tmp, path)?;
            sync_dir(&self.sh.seg_dir)?;
            Ok(f)
        })();
        result.map_err(|e| {
            let _ = remove_if_exists(&tmp);
            WalError::FsyncFailed {
                op: "create segment",
                source: Arc::new(e),
            }
        })
    }

    /// Flushes and seals the active segment, then opens the next one.
    fn roll(&mut self) -> Result<(), WalError> {
        self.write_staged()?;
        self.sync_now()?;
        self.seal_active()?;
        self.open_segment()
    }

    fn seal_active(&mut self) -> Result<(), WalError> {
        let Some(a) = self.active.take() else {
            return Ok(());
        };
        if a.prealloc_len > a.len {
            a.file
                .set_len(a.len)
                .map_err(|e| WalError::io("truncate", &a.path, e))?;
            self.fsync_point()
                .and_then(|()| sync_file(&a.file))
                .map_err(|e| WalError::FsyncFailed {
                    op: "fsync",
                    source: Arc::new(e),
                })?;
            self.sh.release(a.prealloc_len - a.len);
        }
        let now = self.sh.now();
        let mut idx = lock(&self.sh.index);
        if let Some(seg) = idx.segs.iter_mut().rev().find(|s| s.base == a.base) {
            seg.bytes = a.len;
            seg.sealed_at = Some(now);
        }
        Ok(())
    }

    fn write_staged(&mut self) -> Result<(), WalError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let Some(a) = self.active.as_mut() else {
            return Err(WalError::Corrupt(
                "staged frames without an active segment".into(),
            ));
        };
        let res = write_point(&self.sh).and_then(|()| a.file.write_all_at(&self.buf, a.len));
        if let Err(e) = res {
            return Err(WalError::io("write", &a.path, e));
        }
        let n = self.buf.len() as u64;
        a.len += n;
        a.unsynced = true;
        let charged = a.charged();
        let base = a.base;
        {
            let mut idx = lock(&self.sh.index);
            if let Some(seg) = idx.segs.back_mut().filter(|s| s.base == base) {
                seg.bytes = charged;
            }
        }
        let c = &self.sh.counters;
        Counters::add(&c.frames, self.staged.len() as u64);
        Counters::add(&c.bytes_written, n);
        Counters::add(&c.groups, 1);
        self.written.extend(self.staged.drain(..));
        self.buf.clear();
        #[cfg(any(test, feature = "failpoints"))]
        self.sh.opts.failpoints.after_write_point();
        Ok(())
    }

    fn fsync_point(&self) -> io::Result<()> {
        #[cfg(any(test, feature = "failpoints"))]
        self.sh.opts.failpoints.fsync_point()?;
        Ok(())
    }

    /// Flushes written data and acknowledges it. A failure is never retried
    /// (fsyncgate): the caller poisons the writer.
    fn sync_now(&mut self) -> Result<(), WalError> {
        if self.active.as_ref().is_some_and(|a| a.unsynced) {
            self.force_sync()?;
        }
        self.ack_written();
        Ok(())
    }

    fn force_sync(&mut self) -> Result<(), WalError> {
        let Some(a) = self.active.as_ref() else {
            return Ok(());
        };
        self.fsync_point()
            .and_then(|()| sync_file(&a.file))
            .map_err(|e| WalError::FsyncFailed {
                op: "fsync",
                source: Arc::new(e),
            })?;
        #[cfg(any(test, feature = "failpoints"))]
        self.sh.opts.failpoints.record_synced(a.path.clone(), a.len);
        if let Some(a) = self.active.as_mut() {
            a.unsynced = false;
        }
        Counters::add(&self.sh.counters.fsyncs, 1);
        self.last_sync = Some(Instant::now());
        Ok(())
    }

    /// Publishes the durable end, then resolves the written tickets in order.
    fn ack_written(&mut self) {
        let Some(last) = self.written.back().map(|p| p.seq) else {
            return;
        };
        if let Some(a) = self.active.as_mut() {
            a.acked_len = a.len;
        }
        self.sh.status.send_modify(|s| s.durable = last);
        let epoch = self.sh.epoch;
        for p in self.written.drain(..) {
            let _ = p
                .reply
                .send(Ok(WalRange::single(epoch, WalPos::new(p.seq))));
        }
    }

    fn finish(&mut self) -> Result<Exit, WalError> {
        #[cfg(any(test, feature = "failpoints"))]
        if self.sh.opts.failpoints.crash_on_close_set() {
            return Ok(Exit::Crashed);
        }
        self.write_staged()?;
        self.sync_now()?;
        self.seal_active()?;
        let marker = self.sh.dir.join(CLEAN_MARKER);
        let _ = write_atomic(&marker, self.next_seq.to_string().as_bytes());
        Ok(Exit::Closed)
    }

    /// Stops the writer after a failure. Nothing written since the last
    /// acknowledged group is trusted: it is truncated away (so a reopen cannot
    /// read it back from the page cache) and its tickets fail.
    fn poison(&mut self, e: WalError) {
        let _ = self.sh.poison.set(WalError::Poisoned(Arc::new(e.clone())));
        self.sh
            .status
            .send_modify(|s| s.state = WriterState::Failed);
        self.buf.clear();
        if let Some(a) = self.active.take() {
            let keep = a.acked_len;
            let mut bytes = a.charged();
            if bytes > keep && a.file.set_len(keep).is_ok() {
                self.sh.release(bytes - keep);
                bytes = keep;
            }
            let now = self.sh.now();
            let mut idx = lock(&self.sh.index);
            if let Some(seg) = idx.segs.iter_mut().rev().find(|s| s.base == a.base) {
                seg.bytes = bytes;
                seg.sealed_at = Some(now);
            }
        }
        for p in self.staged.drain(..).chain(self.written.drain(..)) {
            let _ = p.reply.send(Err(e.clone()));
        }
        let stopped = self.sh.stopped_error();
        while let Ok(m) = self.rx.try_recv() {
            if let Msg::Batch(s) = m {
                let _ = s.reply.send(Err(stopped.clone()));
            }
        }
    }
}

fn write_point(_sh: &Shared) -> io::Result<()> {
    #[cfg(any(test, feature = "failpoints"))]
    _sh.opts.failpoints.write_point()?;
    Ok(())
}
