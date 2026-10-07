//! Fault injection for tests (feature `failpoints`).
//!
//! A [`Failpoints`] handle is attached to one WAL through
//! `WalOptions::failpoints`, so concurrent tests never interfere.

use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// Shared switches the writer consults at its fault points.
#[derive(Clone, Debug, Default)]
pub struct Failpoints(Arc<Inner>);

#[derive(Debug, Default)]
struct Inner {
    fail_fsyncs: AtomicU32,
    fail_writes: AtomicU32,
    write_errno: AtomicI32,
    crash_on_close: AtomicBool,
    abort_after_write: AtomicBool,
    last_synced: Mutex<Option<(PathBuf, u64)>>,
}

impl Failpoints {
    /// The next `times` segment `fsync`s fail with `EIO` (`u32::MAX`: all).
    pub fn fail_fsyncs(&self, times: u32) {
        self.0.fail_fsyncs.store(times, Ordering::SeqCst);
    }

    /// The next `times` segment writes fail with the OS error `errno`.
    pub fn fail_writes(&self, errno: i32, times: u32) {
        self.0.write_errno.store(errno, Ordering::SeqCst);
        self.0.fail_writes.store(times, Ordering::SeqCst);
    }

    /// On close, the writer exits at once: no final write, `fsync`, seal, or
    /// clean-shutdown marker. Simulates a process crash in-process.
    pub fn crash_on_close(&self) {
        self.0.crash_on_close.store(true, Ordering::SeqCst);
    }

    /// `wal::after_write_before_fsync`: abort the process right after a group
    /// is written and before it is flushed.
    pub fn abort_after_write(&self) {
        self.0.abort_after_write.store(true, Ordering::SeqCst);
    }

    /// The active segment and the length it had at its last successful
    /// `fsync`. Truncating the file to it simulates losing unflushed data.
    pub fn last_synced(&self) -> Option<(PathBuf, u64)> {
        self.0
            .last_synced
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn fsync_point(&self) -> io::Result<()> {
        if take_one(&self.0.fail_fsyncs) {
            return Err(io::Error::from_raw_os_error(
                rustix::io::Errno::IO.raw_os_error(),
            ));
        }
        Ok(())
    }

    pub(crate) fn write_point(&self) -> io::Result<()> {
        if take_one(&self.0.fail_writes) {
            return Err(io::Error::from_raw_os_error(
                self.0.write_errno.load(Ordering::SeqCst),
            ));
        }
        Ok(())
    }

    pub(crate) fn after_write_point(&self) {
        if self.0.abort_after_write.load(Ordering::SeqCst) {
            std::process::abort();
        }
    }

    pub(crate) fn crash_on_close_set(&self) -> bool {
        self.0.crash_on_close.load(Ordering::SeqCst)
    }

    pub(crate) fn record_synced(&self, path: PathBuf, len: u64) {
        *self
            .0
            .last_synced
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((path, len));
    }
}

fn take_one(counter: &AtomicU32) -> bool {
    let mut n = counter.load(Ordering::SeqCst);
    loop {
        let next = match n {
            0 => return false,
            u32::MAX => return true,
            n => n - 1,
        };
        match counter.compare_exchange_weak(n, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(actual) => n = actual,
        }
    }
}
