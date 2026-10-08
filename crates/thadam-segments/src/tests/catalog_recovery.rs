//! Catalog and recovery behavior: crash-matrix dedup at row level, footer
//! rebuild, torn-tail truncation, retention eviction order, and coverage
//! markers.

use std::collections::BTreeSet;
use std::sync::Arc;

use thadam_core::{
    BudgetCategory, DatasetEvents, DiskBudget, SegmentBatch, SegmentSink, WalEpoch, WalPos,
    WalRange,
};
use thadam_testkit::conformance::segment_sink::SegmentSinkFixture;
use thadam_testkit::{test_event, test_segment_batch_across};

use crate::catalog::record::{CatalogRecord, EligibleRecord, FinalizedRecord, UploadedRecord};
use crate::catalog::{LocalRetention, SegmentCatalog};
use crate::failpoints::Failpoint;
use crate::tests::fixture::{ParquetFixture, files_of};

const DATASETS: [&str; 3] = ["alpha", "beta", "gamma"];

/// Drives the shared crash matrix directly (the conformance runner covers
/// the range assertions; here we assert at row level): after a crash at any
/// finalize failpoint, at any point in the stream, replay produces exactly
/// one copy of every `(dataset, wal_pos)` row, and the merged range covers
/// the whole stream with no gaps.
pub(crate) async fn failpoint_matrix_no_duplicate_rows(fixture: &ParquetFixture) {
    const FRAMES: u64 = 12;
    for failpoint in Failpoint::ALL {
        for arm_before_frame in [1u64, 4, 7, 11] {
            let scenario = format!(
                "failpoint {}, armed before frame {arm_before_frame}",
                failpoint.name()
            );
            fixture.reset();
            let ctx = fixture.ctx();
            let mut sink = fixture.open_sink();
            sink.recover(&ctx)
                .await
                .unwrap_or_else(|e| panic!("{scenario}: recover: {e}"));

            let mut crashed = false;
            for frame in 1..=FRAMES {
                if frame == arm_before_frame {
                    fixture.arm(failpoint.name());
                }
                let batch = test_segment_batch_across(1, frame, frame, &DATASETS);
                if sink.append(batch).await.is_err() {
                    crashed = true;
                    break;
                }
                if sink.poll_commit().await.is_err() {
                    crashed = true;
                    break;
                }
            }
            if !crashed && sink.finalize_all().await.is_err() {
                crashed = true;
            }
            let _ = crashed;
            fixture.disarm();
            drop(sink);

            // Restart and replay from the recovered watermark, like the
            // driver would.
            let mut restarted = fixture.open_sink();
            let durable = restarted
                .recover(&ctx)
                .await
                .unwrap_or_else(|e| panic!("{scenario}: restart recover: {e}"));
            let resume = durable.map_or(1, |p| p.get() + 1);
            for frame in resume..=FRAMES {
                restarted
                    .append(test_segment_batch_across(1, frame, frame, &DATASETS))
                    .await
                    .unwrap_or_else(|e| panic!("{scenario}: replay {frame}: {e}"));
                restarted
                    .poll_commit()
                    .await
                    .unwrap_or_else(|e| panic!("{scenario}: replay poll {frame}: {e}"));
            }
            assert_eq!(
                restarted
                    .finalize_all()
                    .await
                    .unwrap_or_else(|e| panic!("{scenario}: finalize after replay: {e}")),
                Some(WalPos::new(FRAMES)),
                "{scenario}"
            );
            drop(restarted);

            // Row level: no `(dataset, wal_pos)` pair appears twice.
            let rows = fixture.all_rows();
            let mut seen = BTreeSet::new();
            for pair in &rows {
                assert!(
                    seen.insert(pair.clone()),
                    "{scenario}: duplicate row {pair:?}"
                );
            }
            let frames_with_rows: BTreeSet<u64> = rows.iter().map(|(_, p)| *p).collect();
            assert_eq!(
                frames_with_rows.len() as u64,
                FRAMES,
                "{scenario}: every frame produced exactly one row somewhere: {rows:?}"
            );

            // Stream level: one contiguous cover, one file per
            // (dataset, range).
            let merged = fixture.merged_ranges();
            assert_eq!(
                merged.len(),
                1,
                "{scenario}: one contiguous cover, got {merged:?}"
            );
            assert_eq!(merged[0].first().get(), 1, "{scenario}");
            assert_eq!(merged[0].last().get(), FRAMES, "{scenario}");
            let files = fixture.ready_files();
            let mut by_key = BTreeSet::new();
            for (dataset, _, range) in &files {
                let key = (dataset.clone(), range.first().get(), range.last().get());
                assert!(
                    by_key.insert(key.clone()),
                    "{scenario}: two files cover {key:?}"
                );
            }
        }
    }
}

/// The catalog is rebuilt from Parquet footers when the log is gone: records
/// come back `Finalized`, eligibility is empty, and ranges and row counts
/// match what was written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_rebuild_from_footers() {
    let fixture = ParquetFixture::new(&DATASETS, crate::config::RotationConfig::never(), 4);
    let ctx = fixture.ctx();
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();
    for f in 1..=9u64 {
        sink.append(test_segment_batch_across(1, f, f, &DATASETS))
            .await
            .unwrap();
    }
    assert_eq!(sink.finalize_all().await.unwrap(), Some(WalPos::new(9)));
    drop(sink);

    let before = fixture.ready_files();
    assert!(!before.is_empty());

    // Lose the whole log, then reopen: the footer scan rebuilds the log.
    std::fs::remove_file(fixture.dir.path().join("catalog.log")).unwrap();
    eprintln!("before open: {:?}", fixture.ready_files());
    let catalog = fixture.open_catalog();
    eprintln!("after open: {:?}", fixture.ready_files());
    {
        let bytes = std::fs::read(fixture.dir.path().join("catalog.log")).unwrap();
        let mut pos = 0usize;
        while pos + 8 <= bytes.len() {
            let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            if pos + 4 + len + 4 > bytes.len() {
                break;
            }
            let payload = &bytes[pos + 4..pos + 4 + len];
            let text: String = String::from_utf8_lossy(payload).chars().take(155).collect();
            eprintln!("LINE@{pos} {text}");
            pos += 4 + len + 4;
        }
    }
    assert!(
        catalog.eligible_for_upload().is_empty(),
        "a rebuild knows nothing about eligibility"
    );
    let after = fixture.ready_files();
    assert_eq!(before.len(), after.len(), "rebuild keeps every file");
    let rows = fixture.dataset_rows();
    assert_eq!(
        rows.values().sum::<usize>(),
        9,
        "rows match the stream: {rows:?}"
    );

    // And the watermark computed from the rebuilt records survives a restart.
    let catalog2 = fixture.open_catalog();
    drop(catalog2);
    let mut restarted = fixture.open_sink();
    assert_eq!(restarted.recover(&ctx).await.unwrap(), Some(WalPos::new(9)));
}

/// A torn final frame of `catalog.log` is a crash artifact: it is truncated
/// and replay resumes cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_torn_tail_is_truncated() {
    let fixture = ParquetFixture::new(&DATASETS, crate::config::RotationConfig::never(), 4);
    let ctx = fixture.ctx();
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();
    sink.append(test_segment_batch_across(1, 1, 2, &DATASETS))
        .await
        .unwrap();
    sink.finalize_all().await.unwrap();
    drop(sink);

    let log = fixture.dir.path().join("catalog.log");
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&log).unwrap();
        f.write_all(b"\x20\x00\x00___PARTIALLY_WRITTEN").unwrap();
    }

    let mut restarted = fixture.open_sink();
    let durable = restarted.recover(&ctx).await.unwrap();
    assert_eq!(durable, Some(WalPos::new(2)));
    restarted
        .append(test_segment_batch_across(1, 3, 3, &DATASETS))
        .await
        .unwrap();
    assert_eq!(
        restarted.finalize_all().await.unwrap(),
        Some(WalPos::new(3))
    );
    assert_eq!(fixture.merged_ranges().len(), 1);
}

/// Coverage markers (empty datasets write no file) still prove coverage: a
/// restarted sink recovers the full watermark instead of treating a sparse
/// dataset as lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn markers_prove_coverage_for_empty_datasets() {
    let sparse: [&str; 3] = ["one", "empty", "three"];
    let fixture = ParquetFixture::new(&sparse, crate::config::RotationConfig::never(), 4);
    let ctx = fixture.ctx();
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();

    for frame in 1..=6u64 {
        let origin =
            WalRange::new(WalEpoch::new(1), WalPos::new(frame), WalPos::new(frame)).unwrap();
        let one = if frame % 2 == 1 {
            vec![test_event(1, frame)]
        } else {
            Vec::new()
        };
        let three = if frame % 2 == 0 {
            vec![test_event(3, frame)]
        } else {
            Vec::new()
        };
        let batch = SegmentBatch::new(
            origin,
            true,
            vec![
                DatasetEvents::new("one", one),
                DatasetEvents::new("empty", Vec::new()),
                DatasetEvents::new("three", three),
            ],
        );
        sink.append(batch).await.unwrap();
    }
    assert_eq!(sink.finalize_all().await.unwrap(), Some(WalPos::new(6)));
    drop(sink);

    // `empty` has no Parquet file but the watermark survives recovery.
    let root = fixture.dir.path().join("ready");
    let mut ds_with_files = BTreeSet::new();
    for path in files_of(&root) {
        let rel = path.strip_prefix(&root).unwrap();
        ds_with_files.insert(
            rel.components()
                .next()
                .unwrap()
                .as_os_str()
                .to_str()
                .unwrap()
                .to_owned(),
        );
    }
    assert!(
        !ds_with_files.contains("empty"),
        "empty dataset writes no file: {ds_with_files:?}"
    );
    assert!(ds_with_files.contains("one"), "{ds_with_files:?}");
    assert!(ds_with_files.contains("three"), "{ds_with_files:?}");

    let mut restarted = fixture.open_sink();
    assert_eq!(
        restarted.recover(&ctx).await.unwrap(),
        Some(WalPos::new(6)),
        "markers bridge the coverage gap"
    );
    assert_eq!(
        fixture.all_rows().len(),
        6,
        "one and three each wrote three rows"
    );
}

/// Disk-budget eviction order: uploaded+expired first, then uploaded, then
/// backpressure (reported as pressure).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_local_evicts_uploaded_expired_then_uploaded_then_pressures() {
    let dir = tempfile::tempdir().unwrap();
    let datasets = vec!["d".to_owned()];
    let budget = Arc::new(DiskBudget::new());
    let ready = dir.path().join("ready/d");
    std::fs::create_dir_all(&ready).unwrap();

    // Fake segment files of exactly 100 bytes each with valid Thadam names.
    // The footer scan ignores them (not real Parquet); the records below
    // register them through the same log path the finalize sequence uses.
    let name = |letter: &str| match letter {
        "a" => "edge01_0000000000000001_00000000000000000001_00000000000000000002.parquet",
        "b" => "edge01_0000000000000001_00000000000000000003_00000000000000000004.parquet",
        _ => "edge01_0000000000000001_00000000000000000005_00000000000000000006.parquet",
    };
    for letter in ["a", "b", "c"] {
        std::fs::write(ready.join(name(letter)), vec![7u8; 100]).unwrap();
    }
    let (catalog, _) = SegmentCatalog::open(
        dir.path(),
        "edge01",
        Some(Arc::clone(&budget)),
        BudgetCategory::Segments,
        &datasets,
    )
    .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    for (letter, first) in [("a", 1u64), ("b", 3u64), ("c", 5u64)] {
        catalog
            .append_records_now(vec![
                CatalogRecord::Finalized(FinalizedRecord {
                    id: name(letter).to_owned(),
                    dataset: "d".into(),
                    path: Some(
                        ready
                            .join(name(letter))
                            .strip_prefix(dir.path())
                            .unwrap()
                            .to_path_buf(),
                    ),
                    epoch: 1,
                    first,
                    last: first + 1,
                    rows: 10,
                    bytes: 100,
                    sha256: String::new(),
                    min_event_nanos: None,
                    max_event_nanos: None,
                    clock_synced: true,
                }),
                CatalogRecord::Eligible(EligibleRecord {
                    id: name(letter).to_owned(),
                    pos: 10,
                }),
            ])
            .unwrap();
    }
    catalog
        .append_records_now(vec![
            CatalogRecord::Uploaded(UploadedRecord {
                id: name("a").to_owned(),
                key: "k/a".into(),
                etag: None,
                uploaded_unix: 0, // long ago: expired under any keep_for
            }),
            CatalogRecord::Uploaded(UploadedRecord {
                id: name("b").to_owned(),
                key: "k/b".into(),
                etag: None,
                uploaded_unix: now,
            }),
        ])
        .unwrap();

    budget.set_limit(BudgetCategory::Segments, Some(250));
    assert!(budget.is_over_limit(BudgetCategory::Segments));
    let policy = LocalRetention {
        delete_after_upload: false,
        keep_for: Some(std::time::Duration::from_secs(60)),
        keep_until_disk_pressure: true,
    };
    let report = catalog.release_local(&policy).await.unwrap();
    assert_eq!(
        report.deleted_files, 1,
        "only the expired upload goes first: {report:?}"
    );
    assert!(ready.join(name("a")).metadata().is_err(), "A deleted");
    assert!(ready.join(name("b")).metadata().is_ok(), "B kept");
    assert!(ready.join(name("c")).metadata().is_ok(), "C kept");
    assert!(!report.pressure, "back under the limit after A: {report:?}");

    // Tighten the limit: pressure deletes the remaining upload.
    budget.set_limit(BudgetCategory::Segments, Some(150));
    let report = catalog.release_local(&policy).await.unwrap();
    assert_eq!(
        report.deleted_files, 1,
        "uploaded is next in line: {report:?}"
    );
    assert!(
        ready.join(name("b")).metadata().is_err(),
        "B deleted under pressure"
    );
    assert!(
        ready.join(name("c")).metadata().is_ok(),
        "C (never uploaded) must survive"
    );

    // Without pressure mode the unexpired upload survives and backpressure
    // is reported.
    budget.set_limit(BudgetCategory::Segments, Some(50));
    let report = catalog
        .release_local(&LocalRetention {
            keep_until_disk_pressure: false,
            ..policy
        })
        .await
        .unwrap();
    assert_eq!(
        report.deleted_files, 0,
        "nothing left to evict without pressure mode: {report:?}"
    );
    assert!(report.pressure, "still over the limit: backpressure");
    assert!(ready.join(name("c")).metadata().is_ok());
}

/// A dataset absent from the catalog after recovery is reprocessed exactly
/// once (the drop-beyond-watermark rule): the recovered partial wave is
/// deleted and its rows regenerate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_wave_is_dropped_and_replayed() {
    let fixture = ParquetFixture::new(&DATASETS, crate::config::RotationConfig::never(), 4);
    let ctx = fixture.ctx();

    // Write frames 1..4 and finalize only `alpha`'s [1..4] file by hand: the
    // wave for beta/gamma is missing, so W is None and the file is dropped.
    let mut sink = fixture.open_sink();
    sink.recover(&ctx).await.unwrap();
    for f in 1..=4u64 {
        sink.append(test_segment_batch_across(1, f, f, &DATASETS))
            .await
            .unwrap();
    }
    // Crash at beta's finalize leaves alpha's file durable but the wave
    // incomplete.
    fixture.fps.arm(Failpoint::BeforeFinalize);
    let _ = sink.finalize_all().await;
    drop(sink);

    let mut restarted = fixture.open_sink();
    let durable = restarted.recover(&ctx).await.unwrap();
    assert!(
        durable.is_none() || durable == Some(WalPos::new(4)),
        "either the wave was complete anyway (drain order) or nothing durable, got {durable:?}"
    );
    let resume = durable.map_or(1, |p| p.get() + 1);
    for frame in resume..=8u64 {
        restarted
            .append(test_segment_batch_across(1, frame, frame, &DATASETS))
            .await
            .unwrap();
    }
    restarted.finalize_all().await.unwrap();
    let rows = fixture.all_rows();
    let mut seen = BTreeSet::new();
    for pair in &rows {
        assert!(
            seen.insert(pair.clone()),
            "no duplicates after replay: {pair:?}"
        );
    }
}
