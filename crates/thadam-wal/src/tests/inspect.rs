//! Read-only inspection: it reports the WAL as it is and never changes it.

use std::io::Write;

use thadam_core::{Timestamp, WalPos};

use super::util::*;
use crate::{CursorState, WalError, WalOptions};

fn opts() -> WalOptions {
    small_opts(crate::OnFull::Block)
}

#[tokio::test]
async fn inspect_reports_layout_cursors_and_durable_end() {
    let env = Env::new();
    let (wal, _) = env.open(opts());
    let w = wal.start_writer().unwrap();
    let seqs = append_all(&w, (1..=60).map(|k| batch(0, k, 1 + (k % 4) as usize, 600))).await;
    assert!(seqs.iter().all(Result::is_ok));
    wal.commit_cursor("sink.a", CursorState::at(WalPos::new(30)))
        .await
        .unwrap();
    wal.close().await.unwrap();

    let info = crate::inspect(&env.wal_dir()).unwrap();
    assert_eq!(info.epoch, wal.epoch());
    assert_eq!(info.first_seq, WalPos::new(1));
    assert_eq!(info.next_seq, WalPos::new(61));
    assert_eq!(info.durable_end, Some(WalPos::new(60)));
    assert!(info.segments.len() > 3, "expected several segments");
    assert_eq!(info.segments[0].base, WalPos::new(1));
    assert_eq!(
        info.retained_bytes,
        env.segment_bytes_on_disk(),
        "retained bytes are the segment file sizes"
    );
    assert!(info.clean_shutdown);
    assert!(!info.torn_tail && !info.zero_tail);
    assert_eq!(info.cursors.len(), 1);
    assert_eq!(info.cursors[0].name, "sink.a");
    assert_eq!(info.cursors[0].state, CursorState::at(WalPos::new(30)));
}

#[tokio::test]
async fn inspect_works_while_a_writer_holds_the_wal_and_changes_nothing() {
    let env = Env::new();
    let (wal, _) = env.open(opts());
    let w = wal.start_writer().unwrap();
    append_all(&w, (1..=20).map(|k| batch(0, k, 2, 300))).await;

    let files_before = env.segment_files();
    let lens_before: Vec<u64> = files_before
        .iter()
        .map(|p| std::fs::metadata(p).unwrap().len())
        .collect();
    let info = crate::inspect(&env.wal_dir()).unwrap();
    assert_eq!(info.durable_end, Some(WalPos::new(20)));
    assert!(env.wal_dir().join("LOCK").exists());

    assert_eq!(env.segment_files(), files_before);
    let lens_after: Vec<u64> = env
        .segment_files()
        .iter()
        .map(|p| std::fs::metadata(p).unwrap().len())
        .collect();
    assert_eq!(lens_before, lens_after);

    assert!(matches!(env.try_open(opts()), Err(WalError::Locked { .. })));
}

#[tokio::test]
async fn a_torn_tail_is_reported_and_left_in_place() {
    let env = Env::new();
    let (wal, _) = env.open(opts());
    let w = wal.start_writer().unwrap();
    append_all(&w, (1..=5).map(|k| batch(0, k, 1, 100))).await;
    wal.close().await.unwrap();

    let last = env.segment_files().pop().unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&last)
        .unwrap()
        .write_all(&[1, 2, 3])
        .unwrap();
    let len = std::fs::metadata(&last).unwrap().len();

    let info = crate::inspect(&env.wal_dir()).unwrap();
    assert!(info.torn_tail);
    assert_eq!(info.durable_end, Some(WalPos::new(5)));
    assert_eq!(info.next_seq, WalPos::new(6));
    assert_eq!(
        std::fs::metadata(&last).unwrap().len(),
        len,
        "inspect truncated"
    );
}

#[tokio::test]
async fn newest_frames_returns_the_tail_in_position_order() {
    let env = Env::new();
    let (wal, _) = env.open(opts());
    let w = wal.start_writer().unwrap();
    append_all(&w, (1..=60).map(|k| batch(0, k, 1, 200))).await;
    wal.close().await.unwrap();

    let info = crate::inspect(&env.wal_dir()).unwrap();
    let newest = info.newest_frames(5).unwrap();
    let seqs: Vec<u64> = newest.iter().map(|f| f.seq.get()).collect();
    assert_eq!(seqs, vec![56, 57, 58, 59, 60]);
    let last = &newest[4];
    assert_eq!(last.records.len(), 1);
    assert_eq!(last.records[0].ingest_time, Timestamp::from_nanos(60_000));
    assert_eq!(last.source, "src-0");
    assert!(last.clock_synced);

    assert_eq!(info.newest_frames(1000).unwrap().len(), 60);
    assert!(info.newest_frames(0).unwrap().is_empty());
}

#[tokio::test]
async fn time_bounds_map_to_positions() {
    let env = Env::new();
    let (wal, _) = env.open(opts());
    let w = wal.start_writer().unwrap();
    append_all(&w, (1..=60).map(|k| batch(0, k, 1, 100))).await;
    wal.close().await.unwrap();

    let info = crate::inspect(&env.wal_dir()).unwrap();
    assert_eq!(
        info.first_seq_ingested_since(Timestamp::from_nanos(31_000))
            .unwrap(),
        Some(WalPos::new(31))
    );
    assert_eq!(
        info.last_seq_ingested_until(Timestamp::from_nanos(30_500))
            .unwrap(),
        Some(WalPos::new(30))
    );
    assert_eq!(
        info.first_seq_ingested_since(Timestamp::from_nanos(1_000_000))
            .unwrap(),
        None
    );
    assert_eq!(
        info.last_seq_ingested_until(Timestamp::from_nanos(0))
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn a_directory_without_wal_metadata_is_not_inspected() {
    let env = Env::new();
    let dir = env.wal_dir();
    std::fs::create_dir_all(&dir).unwrap();
    assert!(matches!(
        crate::inspect(&dir),
        Err(WalError::InvalidArgument(_))
    ));
    assert!(!dir.join("LOCK").exists(), "inspect created files");
}
