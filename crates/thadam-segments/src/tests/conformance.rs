//! Runs `thadam-testkit`'s segment-sink conformance suite against the Parquet
//! sink (ADR-0002 segment suite: monotonic watermark, recovery after a crash
//! at every failpoint, dataset isolation).

use thadam_testkit::conformance::segment_sink as suite;

use crate::tests::fixture::ParquetFixture;

const DATASETS: [&str; 3] = ["alpha", "beta", "gamma"];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watermark_is_monotonic_and_complete() {
    let fixture = ParquetFixture::suite(&DATASETS);
    suite::watermark_is_monotonic_and_complete(&fixture, 0x5eed).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_after_crash_at_every_failpoint() {
    let fixture = ParquetFixture::suite(&DATASETS);
    suite::recovery_after_crash_at_every_failpoint(&fixture).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn datasets_are_kept_apart() {
    let fixture = ParquetFixture::suite(&DATASETS);
    suite::datasets_are_kept_apart(&fixture).await;
}

/// The whole suite in one run, like testkit's own memory-sink check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_all() {
    let fixture = ParquetFixture::suite(&DATASETS);
    suite::run_all(&fixture, 7).await;
}

/// An extended matrix over our failpoints driving full row-level checks, in
/// `catalog_recovery`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_at_every_finalize_failpoint_keeps_one_file_per_wal_range() {
    crate::tests::catalog_recovery::failpoint_matrix_no_duplicate_rows(&ParquetFixture::suite(
        &DATASETS,
    ))
    .await;
}
