//! Capture-first raw write-ahead log.
//!
//! # Responsibility
//!
//! - The segment and frame format.
//! - The writer thread with group commit.
//! - Async readers and durable cursors.
//! - Retention, disk budget, and `on_full` policy.
//! - Crash recovery.
//!
//! # Deliberately not exposed
//!
//! - Frame and segment layout, the writer thread handle, file descriptors.
//! - `mmap` (rejected: SIGBUS risk).
//! - Random access by time (a time index is post-MVP).
//! - Priority queues.
//! - Any event or model decoding: the WAL stores raw messages only.
//!
//! # Status of the on-disk format
//!
//! ADR-0004 is still **Draft**. The owner decided to build the WAL now from
//! the spike S1 design and to re-validate it on reference hardware (x86 NVMe,
//! ARM eMMC) later. Until ADR-0004 is accepted the format is versioned but not
//! frozen: segment headers, `wal.json`, and cursor files carry format versions,
//! and a binary refuses any version it does not know instead of guessing. The
//! `interval(10ms)` fsync default, Linux preallocation, the segment size, and
//! the `RawFrame` field numbers are all provisional pending that re-run.
//!
//! # Directory layout
//!
//! `dir` passed to [`open`] is the WAL root (the runtime uses
//! `data_dir/wal`):
//!
//! ```text
//! LOCK                      flock held while the WAL is open
//! wal.json                  kind, format versions, epoch, node and machine binding
//! clean_shutdown            present only after a clean writer shutdown
//! segments/{base:020}.seg   64-byte header, then frames
//! cursors/<name>.cur        one durable cursor per consumer
//! ```
//!
//! # Guarantees
//!
//! - A [`CommitTicket`] resolves `Ok` only after its frame is durable per the
//!   [`Fsync`] policy, in submission order. After a failed write or `fsync`,
//!   no ticket resolves `Ok`; the writer truncates whatever was not yet
//!   acknowledged (so the page cache is never trusted), stops, and the WAL
//!   must be reopened, which re-verifies it.
//! - Readers and cursor commits never pass the durable end. Recovery never
//!   reuses a position at or below a durable cursor.
//! - Open and recovery never allocate space in an existing WAL; the next
//!   segment is created on the first append, after reclaim has run.
//! - Retention deletes whole segments, oldest first, never the newest one.
//!   Loss is visible: readers receive [`Gap`] items and [`WalStats`] counts
//!   evicted bytes.
//!
//! # Example
//!
//! ```no_run
//! # async fn demo() -> Result<(), thadam_wal::WalError> {
//! use std::sync::Arc;
//! use thadam_core::{DiskBudget, RawBatch, SourceId, SystemClock};
//! use thadam_wal::{CursorState, OnFull, ReadItem, StartFrom, WalOptions};
//!
//! let budget = Arc::new(DiskBudget::new());
//! let (wal, _report) = thadam_wal::open(
//!     "/var/lib/thadam/p/wal".as_ref(),
//!     WalOptions::new(OnFull::Block),
//!     budget,
//!     Arc::new(SystemClock::new()),
//! )?;
//! let writer = wal.start_writer()?;
//! let ticket = writer.submit(RawBatch::new(SourceId::new("mqtt"), vec![], true)).await?;
//! let range = ticket.await?; // durable: acknowledge the source now
//!
//! let mut reader = wal.reader("local", StartFrom::Earliest)?;
//! if let Some(ReadItem::Frames(chunk)) = reader.next(1 << 20).await? {
//!     wal.commit_cursor("local", CursorState::at(chunk.range().last())).await?;
//! }
//! # let _ = range;
//! # Ok(()) }
//! ```

mod cursor;
mod error;
#[cfg(any(test, feature = "failpoints"))]
pub mod failpoints;
mod format;
mod fsutil;
mod meta;
mod options;
mod proto;
mod reader;
mod retention;
mod shared;
mod wal;
mod writer;

#[cfg(test)]
mod tests;

pub use cursor::CursorState;
pub use error::WalError;
pub use options::{ConsumerSpec, Fsync, MaxLag, OnFull, StartFrom, WalOptions};
pub use reader::{Frame, FrameChunk, Gap, ReadItem, RecordView, WalReader};
pub use retention::RetentionReport;
pub use shared::WriterState;
pub use wal::{RecoveryReport, Wal, WalStats, open};
pub use writer::{CommitTicket, WalWriter};
