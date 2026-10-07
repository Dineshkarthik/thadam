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
