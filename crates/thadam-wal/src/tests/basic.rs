//! Writer, reader, cursor, and recovery behavior through the public API.

use std::time::Duration;

use thadam_core::{WalEpoch, WalPos, WalRange};

use super::util::*;
use crate::{CursorState, Fsync, Gap, OnFull, ReadItem, StartFrom, WalError, WriterState};

#[tokio::test]
async fn open_creates_the_layout_and_never_allocates_a_segment() {
    let env = Env::new();
    let (wal, report) = env.open(small_opts(OnFull::Block));
    assert!(report.created);
    assert_eq!(report.next_seq, WalPos::new(1));
    assert_eq!(report.durable_end, None);
    for name in ["wal.json", "LOCK", "segments", "cursors"] {
        assert!(env.wal_dir().join(name).exists(), "{name}");
    }
    assert!(env.segment_files().is_empty());
    drop(wal);
    let (_wal, report) = env.open(small_opts(OnFull::Block));
    assert!(!report.created);
    assert!(env.segment_files().is_empty(), "reopen allocated a segment");
}

#[tokio::test]
async fn a_second_open_is_refused_by_the_lock() {
    let env = Env::new();
    let (_wal, _) = env.open(small_opts(OnFull::Block));
    assert!(matches!(
        env.try_open(small_opts(OnFull::Block)),
        Err(WalError::Locked { .. })
    ));
}

#[tokio::test]
async fn frames_round_trip_across_segment_rolls() {
    let env = Env::new();
    let (wal, _) = env.open(small_opts(OnFull::Block));
    let w = wal.start_writer().unwrap();
    let seqs = append_all(&w, (1..=60).map(|k| batch(0, k, 1 + (k % 4) as usize, 600))).await;
    let seqs: Vec<u64> = seqs.into_iter().map(Result::unwrap).collect();
    assert_eq!(seqs, (1..=60).collect::<Vec<_>>());
    assert!(env.segment_files().len() > 3, "expected several segments");
    assert_eq!(wal.durable_end(), Some(WalPos::new(60)));

    let mut r = wal.reader("c", StartFrom::Earliest).unwrap();
    let items = drain(&mut r, 2048);
    let expected: Vec<Item> = (1..=60).map(|k| Item::Frame(k, Some((0, k)))).collect();
    assert_eq!(items, expected);
    assert_eq!(r.position(), WalPos::new(61));
}

#[tokio::test]
async fn record_views_expose_attributes_and_zero_copy_payloads() {
    let env = Env::new();
    let (wal, _) = env.open(small_opts(OnFull::Block));
    let w = wal.start_writer().unwrap();
    let b = batch(0, 4, 3, 100);
    append_all(&w, [b.clone()]).await[0].as_ref().unwrap();
    let mut r = wal.reader("c", StartFrom::Earliest).unwrap();
    let Some(ReadItem::Frames(chunk)) = r.try_next(1 << 20).unwrap() else {
        panic!("expected frames")
    };
    let f = &chunk.frames()[0];
    assert_eq!(f.source().as_str(), "src-0");
    assert!(f.clock_synced());
    assert_eq!(f.len(), 3);
    let rec = f.records().next().unwrap();
    assert_eq!(rec.attr("topic"), Some("robots/r1/t"));
    assert_eq!(rec.ingest_time(), b.messages[0].ingest_time);
    assert_eq!(rec.payload(), &b.messages[0].payload);
    assert_eq!(f.to_raw_batch(), b);
    assert_eq!(chunk.range(), WalRange::single(wal.epoch(), WalPos::new(1)));
}

#[tokio::test]
async fn empty_batches_are_frames_and_oversized_batches_are_refused() {
    let env = Env::new();
    let (wal, _) = env.open(small_opts(OnFull::Block));
    let w = wal.start_writer().unwrap();
    let empty = thadam_core::RawBatch::new(thadam_core::SourceId::new("s"), vec![], false);
    assert_eq!(append_all(&w, [empty]).await[0].as_ref().unwrap(), &1);
    let err = w.submit(batch(0, 1, 2, 4096)).await.unwrap_err();
    assert!(matches!(err, WalError::FrameTooLarge { .. }), "{err}");
}

#[tokio::test]
async fn reopen_continues_the_sequence_in_a_new_segment() {
    let env = Env::new();
    {
        let (wal, _) = env.open(small_opts(OnFull::Block));
        let w = wal.start_writer().unwrap();
        append_all(&w, (1..=5).map(|k| batch(0, k, 1, 50))).await;
        wal.close().await.unwrap();
        assert_eq!(wal.stats().writer, WriterState::Closed);
    }
    let segments_before = env.segment_files().len();
    let (wal, report) = env.open(small_opts(OnFull::Block));
    assert!(report.clean_shutdown);
    assert_eq!(report.next_seq, WalPos::new(6));
    assert_eq!(report.durable_end, Some(WalPos::new(5)));
    assert_eq!(env.segment_files().len(), segments_before, "open allocated");
    let w = wal.start_writer().unwrap();
    let seqs = append_all(&w, (6..=8).map(|k| batch(1, k, 1, 50))).await;
    assert_eq!(
        seqs.into_iter().map(Result::unwrap).collect::<Vec<_>>(),
        vec![6, 7, 8]
    );
    assert_eq!(env.segment_files().len(), segments_before + 1);
    let mut r = wal.reader("c", StartFrom::Earliest).unwrap();
    let ids: Vec<Item> = drain(&mut r, 1 << 20);
    assert_eq!(ids.len(), 8);
    assert_eq!(ids[5], Item::Frame(6, Some((1, 6))));
    assert!(matches!(
        env.try_open(small_opts(OnFull::Block)),
        Err(WalError::Locked { .. })
    ));
    drop((r, w));
    assert!(
        matches!(
            env.try_open(small_opts(OnFull::Block)),
            Err(WalError::Locked { .. })
        ),
        "the Wal handle still holds the lock"
    );
    drop(wal);
    let (_wal, report) = env.open(small_opts(OnFull::Block));
    assert!(
        report.clean_shutdown,
        "dropping the last handle closes the writer gracefully"
    );
}

#[tokio::test]
async fn cursor_commits_are_bounded_by_the_durable_end_and_persist() {
    let env = Env::new();
    let (wal, _) = env.open(small_opts(OnFull::Block));
    let w = wal.start_writer().unwrap();
    append_all(&w, (1..=10).map(|k| batch(0, k, 1, 50))).await;
    let beyond = wal
        .commit_cursor("c", CursorState::at(WalPos::new(11)))
        .await;
    assert!(matches!(beyond, Err(WalError::InvalidArgument(_))));
    assert!(
        wal.commit_cursor("bad/name", CursorState::at(WalPos::new(1)))
            .await
            .is_err()
    );
    let foreign = WalRange::new(
        WalEpoch::new(wal.epoch().get() ^ 1),
        WalPos::new(1),
        WalPos::new(2),
    )
    .unwrap();
    let bad_epoch = CursorState {
        committed: WalPos::new(1),
        inflight: Some(foreign),
    };
    assert!(wal.commit_cursor("c", bad_epoch).await.is_err());

    let inflight = WalRange::new(wal.epoch(), WalPos::new(5), WalPos::new(7)).unwrap();
    let state = CursorState {
        committed: WalPos::new(4),
        inflight: Some(inflight),
    };
    wal.commit_cursor("c", state).await.unwrap();
    assert_eq!(wal.cursor("c"), Some(state));
    drop(w);
    drop(wal);

    let (wal, report) = env.open(small_opts(OnFull::Block));
    assert_eq!(report.cursors, 1);
    assert_eq!(wal.cursor("c"), Some(state));
    let mut r = wal.reader("c", StartFrom::Latest).unwrap();
    assert_eq!(
        r.position(),
        WalPos::new(5),
        "a committed cursor overrides start_from"
    );
    assert_eq!(drain(&mut r, 1 << 20).len(), 6);
    let r = wal.reader("new", StartFrom::Latest).unwrap();
    assert_eq!(r.position(), WalPos::new(11));
}

#[tokio::test]
async fn a_cursor_from_another_epoch_fails_the_open() {
    let env = Env::new();
    drop(env.open(small_opts(OnFull::Block)));
    let other = Env::new();
    {
        let (wal, _) = other.open(small_opts(OnFull::Block));
        let w = wal.start_writer().unwrap();
        append_all(&w, [batch(0, 1, 1, 10)]).await;
        wal.commit_cursor("c", CursorState::at(WalPos::new(1)))
            .await
            .unwrap();
    }
    std::fs::copy(
        other.wal_dir().join("cursors/c.cur"),
        env.wal_dir().join("cursors/c.cur"),
    )
    .unwrap();
    assert!(matches!(
        env.try_open(small_opts(OnFull::Block)),
        Err(WalError::Incompatible(_))
    ));
}

#[tokio::test]
async fn readers_never_pass_the_durable_end() {
    let env = Env::new();
    let mut o = small_opts(OnFull::Block);
    o.fsync = Fsync::Interval(Duration::from_secs(3600));
    let (wal, _) = env.open(o);
    let w = wal.start_writer().unwrap();
    // The first group after an idle period flushes at once; later ones wait.
    append_all(&w, [batch(0, 1, 1, 10)]).await;
    let ticket = w.submit(batch(0, 2, 1, 10)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(wal.durable_end(), Some(WalPos::new(1)));
    let mut r = wal.reader("c", StartFrom::Earliest).unwrap();
    assert_eq!(drain(&mut r, 1 << 20), vec![Item::Frame(1, Some((0, 1)))]);
    assert!(
        wal.commit_cursor("c", CursorState::at(WalPos::new(2)))
            .await
            .is_err(),
        "a cursor may not cover a written but unflushed frame"
    );
    wal.close().await.unwrap();
    assert_eq!(ticket.await.unwrap().first(), WalPos::new(2));
    assert_eq!(drain(&mut r, 1 << 20), vec![Item::Frame(2, Some((0, 2)))]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_next_waits_for_data_and_ends_after_close() {
    let env = Env::new();
    let (wal, _) = env.open(small_opts(OnFull::Block));
    let w = wal.start_writer().unwrap();
    let mut r = wal.reader("c", StartFrom::Earliest).unwrap();
    let consumer = tokio::spawn(async move {
        let mut seen = Vec::new();
        while let Some(item) = r.next(1 << 20).await.unwrap() {
            if let ReadItem::Frames(c) = item {
                seen.extend(c.frames().iter().map(|f| f.seq().get()));
            }
        }
        seen
    });
    for k in 1..=20 {
        append_all(&w, [batch(0, k, 1, 10)]).await;
    }
    wal.close().await.unwrap();
    assert_eq!(consumer.await.unwrap(), (1..=20).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_next_is_cancel_safe() {
    let env = Env::new();
    let (wal, _) = env.open(small_opts(OnFull::Block));
    let w = wal.start_writer().unwrap();
    let mut r = wal.reader("c", StartFrom::Earliest).unwrap();
    let producer = {
        let w = w.clone();
        tokio::spawn(async move {
            for k in 1..=200 {
                append_all(&w, [batch(0, k, 1, 64)]).await;
            }
        })
    };
    let mut seen = Vec::new();
    while seen.len() < 200 {
        // Most of these time out mid-read; nothing may be lost or repeated.
        if let Ok(item) = tokio::time::timeout(Duration::from_micros(50), r.next(256)).await
            && let Some(ReadItem::Frames(c)) = item.unwrap()
        {
            seen.extend(c.frames().iter().map(|f| f.seq().get()));
        }
    }
    producer.await.unwrap();
    assert_eq!(seen, (1..=200).collect::<Vec<_>>());
}

#[tokio::test]
async fn seek_repositions_inside_a_segment() {
    let env = Env::new();
    let (wal, _) = env.open(small_opts(OnFull::Block));
    let w = wal.start_writer().unwrap();
    append_all(&w, (1..=40).map(|k| batch(0, k, 2, 200))).await;
    let mut r = wal.reader("c", StartFrom::At(WalPos::new(17))).unwrap();
    let items = drain(&mut r, 1 << 20);
    assert_eq!(items.first(), Some(&Item::Frame(17, Some((0, 17)))));
    assert_eq!(items.len(), 24);
    r.seek(WalPos::new(3));
    assert_eq!(drain(&mut r, 1 << 20).len(), 38);
}

#[tokio::test]
async fn power_loss_never_reuses_a_position_at_or_below_a_durable_cursor() {
    let env = Env::new();
    let mut o = small_opts(OnFull::Block);
    o.fsync = Fsync::Never;
    let fp = o.failpoints.clone();
    {
        let (wal, _) = env.open(o.clone());
        let w = wal.start_writer().unwrap();
        append_all(&w, (1..=10).map(|k| batch(0, k, 1, 50))).await;
        wal.commit_cursor("c", CursorState::at(WalPos::new(9)))
            .await
            .unwrap();
        fp.crash_on_close();
    }
    // Power loss: everything after the last flush of the active segment is gone.
    let (path, len) = fp.last_synced().unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len)
        .unwrap();

    let mut o = small_opts(OnFull::Block);
    o.fsync = Fsync::Never;
    let (wal, report) = env.open(o);
    assert_eq!(report.durable_end, None);
    assert_eq!(
        report.skipped,
        Some(WalRange::new(wal.epoch(), WalPos::new(1), WalPos::new(9)).unwrap())
    );
    assert_eq!(report.next_seq, WalPos::new(10));
    let w = wal.start_writer().unwrap();
    assert_eq!(
        append_all(&w, [batch(1, 10, 1, 50)]).await[0]
            .as_ref()
            .unwrap(),
        &10
    );
    let mut r = wal.reader("v", StartFrom::Earliest).unwrap();
    let items = drain(&mut r, 1 << 20);
    assert_eq!(
        items,
        vec![
            Item::Gap(Gap::Missing {
                range: WalRange::new(wal.epoch(), WalPos::new(1), WalPos::new(9)).unwrap()
            }),
            Item::Frame(10, Some((1, 10))),
        ]
    );
}

/// Preallocation is Linux-only in production; unit tests emulate it elsewhere.
#[tokio::test]
async fn preallocated_segments_are_sealed_to_their_data_and_recover_after_a_crash() {
    let env = Env::new();
    let mut o = small_opts(OnFull::Block);
    o.preallocate = true;
    let fp = o.failpoints.clone();
    {
        let (wal, _) = env.open(o.clone());
        let w = wal.start_writer().unwrap();
        append_all(&w, (1..=40).map(|k| batch(0, k, 2, 400))).await;
        let used = env.budget.used(thadam_core::BudgetCategory::Wal);
        assert_eq!(used, wal.stats().disk_bytes);
        assert_eq!(
            used,
            env.segment_bytes_on_disk(),
            "budget must match allocated bytes"
        );
        fp.crash_on_close();
    }
    let files = env.segment_files();
    let sealed: Vec<u64> = files[..files.len() - 1]
        .iter()
        .map(|p| std::fs::metadata(p).unwrap().len())
        .collect();
    assert!(
        sealed.iter().all(|&len| len < o.segment_bytes),
        "{sealed:?}"
    );
    let mut o = small_opts(OnFull::Block);
    o.preallocate = true;
    let (wal, report) = env.open(o);
    assert_eq!(report.next_seq, WalPos::new(41));
    assert!(!report.torn_tail);
    assert!(report.zero_tail, "{report:?}");
    let mut r = wal.reader("c", StartFrom::Earliest).unwrap();
    assert_eq!(drain(&mut r, 1 << 20).len(), 40);
    assert_eq!(
        env.budget.used(thadam_core::BudgetCategory::Wal),
        env.segment_bytes_on_disk()
    );
}

#[tokio::test]
async fn binding_and_incompatible_metadata_are_refused() {
    let env = Env::new();
    let mut o = small_opts(OnFull::Block);
    o.machine_id = Some("m1".into());
    drop(env.open(o.clone()));
    o.machine_id = Some("m2".into());
    assert!(matches!(env.try_open(o), Err(WalError::Incompatible(_))));

    let meta = env.wal_dir().join("wal.json");
    let text = std::fs::read_to_string(&meta).unwrap();
    std::fs::write(
        &meta,
        text.replace("\"wal_format_version\": 1", "\"wal_format_version\": 9"),
    )
    .unwrap();
    let err = env.try_open(small_opts(OnFull::Block)).unwrap_err();
    assert!(matches!(err, WalError::Incompatible(_)), "{err}");
}

#[tokio::test]
async fn segments_without_metadata_are_refused() {
    let env = Env::new();
    {
        let (wal, _) = env.open(small_opts(OnFull::Block));
        let w = wal.start_writer().unwrap();
        append_all(&w, [batch(0, 1, 1, 10)]).await;
    }
    std::fs::remove_file(env.wal_dir().join("wal.json")).unwrap();
    assert!(matches!(
        env.try_open(small_opts(OnFull::Block)),
        Err(WalError::Corrupt(_))
    ));
}
