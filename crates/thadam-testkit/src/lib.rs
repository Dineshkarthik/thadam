//! Shared test infrastructure. Dev-dependency only.
//!
//! # Responsibility
//!
//! - Memory connectors: source, batch sink, and segment sink.
//! - A fake clock and temporary data directories.
//! - Failpoint helpers.
//! - Conformance suites for sources and sinks.
//! - Container helpers for integration tests.
//! - The crash harness.
//!
//! # Deliberately not exposed
//!
//! - A production dependency path: no non-test crate may depend on this crate.
//!
//! # Status
//!
//! This version has the memory connectors, `FakeClock`, `TempDataDir`, and the
//! source, batch-sink, and segment-sink conformance suites. Container helpers,
//! the `failpoints` feature, and the process crash harness arrive with the
//! crates that need them.
//!
//! # Running a suite
//!
//! ```ignore
//! #[tokio::test(start_paused = true)]
//! async fn my_source_conforms() {
//!     let fixture = MyFixture::new();
//!     thadam_testkit::conformance::source::run_all(&fixture, 42).await;
//! }
//! ```
//!
//! The source suite sleeps for three keepalive intervals, so run it on paused
//! virtual time.

pub mod conformance;

mod clock;
mod data_dir;
mod fixtures;
mod memory_batch_sink;
mod memory_segment_sink;
mod memory_source;
mod util;

pub use clock::FakeClock;
pub use data_dir::TempDataDir;
pub use fixtures::{
    TEST_DATASET, id_payload, payload_id, test_batch, test_event, test_segment_batch,
    test_segment_batch_across,
};
pub use memory_batch_sink::{Fault, MemoryBatchSink, MemoryBatchSinkConfig, MemoryBatchSinkHandle};
pub use memory_segment_sink::{Failpoint, FinalizedSegment, MemorySegmentSink, MemorySegmentStore};
pub use memory_source::{
    MemorySource, MemorySourceConfig, MemoryUpstream, NextBatchMode, ProtocolTask, UpstreamStats,
};
pub use util::{SplitMix64, poll_once, poll_up_to};
