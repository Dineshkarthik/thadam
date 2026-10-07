//! Fault injection through `WalOptions::failpoints`.

use std::time::Duration;

use thadam_core::WalPos;

use super::util::*;
use crate::{CursorState, Fsync, OnFull, StartFrom, WalError, WriterState};

/// Runs the fsync-failure scenario for one fsync mode and returns the
/// positions that resolved `Ok` before the failure.
async fn fsync_failure(fsync: Fsync) {
    let env = Env::new();
    let mut o = small_opts(OnFull::Block);
    o.fsync = fsync;
    let fp = o.failpoints.clone();
    let (wal, _) = env.open(o);
    let w = wal.start_writer().unwrap();
    let ok: Vec<u64> = append_all(&w, (1..=3).map(|k| batch(0, k, 1, 100)))
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();
    assert_eq!(ok, vec![1, 2, 3]);

    fp.fail_fsyncs(1);
    // Several frames in flight when the flush fails: none may resolve Ok,
    // and neither may anything submitted afterwards.
    let mut tickets = Vec::new();
    for k in 4..=12 {
        match w.submit(batch(0, k, 1, 100)).await {
            Ok(t) => tickets.push(t),
            Err(e) => assert!(matches!(e, WalError::Poisoned(_)), "{e}"),
        }
    }
    for t in tickets {
        let r = t.await;
        assert!(
            matches!(r, Err(WalError::FsyncFailed { .. } | WalError::Poisoned(_))),
            "ticket resolved without the fsync failure after it: {r:?}"
        );
    }
    for k in 13..=15 {
        let r = match w.submit(batch(0, k, 1, 100)).await {
            Ok(t) => t.await.map(|_| ()),
            Err(e) => Err(e),
        };
        assert!(r.is_err(), "submission after poisoning succeeded");
    }
    assert_eq!(wal.stats().writer, WriterState::Failed);
    assert_eq!(wal.durable_end(), Some(WalPos::new(3)));
    assert!(
        wal.start_writer().is_ok(),
        "start_writer returns the stopped writer handle"
    );
    let closed = wal.close().await;
    assert!(
        matches!(closed, Err(WalError::FsyncFailed { .. })),
        "{closed:?}"
    );
    drop(w);
    drop(wal);

    // Reopen re-verifies from disk. Unacknowledged frames were truncated, so
    // the page cache cannot hand them back.
    let (wal, report) = env.open(small_opts(OnFull::Block));
    assert_eq!(report.next_seq, WalPos::new(4), "{report:?}");
    let mut r = wal.reader("c", StartFrom::Earliest).unwrap();
    let items = drain(&mut r, 1 << 20);
    assert_eq!(
        items,
        (1..=3)
            .map(|k| Item::Frame(k, Some((0, k))))
            .collect::<Vec<_>>()
    );
    let w = wal.start_writer().unwrap();
    assert_eq!(
        append_all(&w, [batch(1, 4, 1, 10)]).await[0]
            .as_ref()
            .unwrap(),
        &4
    );
}

#[tokio::test]
async fn no_ticket_resolves_ok_after_a_failed_fsync_always() {
    fsync_failure(Fsync::Always).await;
}

#[tokio::test]
async fn no_ticket_resolves_ok_after_a_failed_fsync_interval() {
    fsync_failure(Fsync::Interval(Duration::from_millis(20))).await;
}

#[tokio::test]
async fn sticky_fsync_failure_at_segment_creation_poisons() {
    let env = Env::new();
    let o = small_opts(OnFull::Block);
    let fp = o.failpoints.clone();
    let (wal, _) = env.open(o);
    let w = wal.start_writer().unwrap();
    fp.fail_fsyncs(u32::MAX);
    let r = append_all(&w, [batch(0, 1, 1, 10)]).await;
    assert!(matches!(r[0], Err(WalError::FsyncFailed { .. })), "{r:?}");
    assert!(
        env.segment_files().is_empty(),
        "a half-created segment kept its name"
    );
    assert!(w.submit(batch(0, 2, 1, 10)).await.is_err());
}

#[tokio::test]
async fn write_enospc_then_reopen_drain_and_reclaim_without_allocating() {
    let env = Env::new();
    let mut o = small_opts(OnFull::Block);
    o.consumers = vec![crate::ConsumerSpec::required("c")];
    let fp = o.failpoints.clone();
    let (wal, _) = env.open(o.clone());
    let w = wal.start_writer().unwrap();
    let acked: Vec<u64> = append_all(&w, (1..=40).map(|k| batch(0, k, 2, 300)))
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();
    assert_eq!(acked.len(), 40);
    fp.fail_writes(rustix::io::Errno::NOSPC.raw_os_error(), u32::MAX);
    let r = append_all(&w, [batch(0, 41, 1, 10)]).await;
    assert!(
        matches!(&r[0], Err(WalError::Io { op: "write", .. })),
        "{r:?}"
    );
    drop(w);
    drop(wal);

    // The disk is "full": reopening must not create or grow anything.
    let before: Vec<(std::path::PathBuf, u64)> = env
        .segment_files()
        .into_iter()
        .map(|p| {
            let len = std::fs::metadata(&p).unwrap().len();
            (p, len)
        })
        .collect();
    let mut o2 = o.clone();
    o2.failpoints = crate::failpoints::Failpoints::default();
    let (wal, report) = env.open(o2);
    let after: Vec<(std::path::PathBuf, u64)> = env
        .segment_files()
        .into_iter()
        .map(|p| {
            let len = std::fs::metadata(&p).unwrap().len();
            (p, len)
        })
        .collect();
    assert_eq!(before, after, "recovery allocated or changed segments");
    assert_eq!(report.next_seq, WalPos::new(41));

    // Consumers drain and reclaim on the full disk, then appends resume.
    let mut r = wal.reader("c", StartFrom::Earliest).unwrap();
    assert_eq!(drain(&mut r, 1 << 20).len(), 40);
    wal.commit_cursor("c", CursorState::at(WalPos::new(40)))
        .await
        .unwrap();
    let rep = wal.maintain().await.unwrap();
    assert!(rep.deleted_segments > 0);
    assert_eq!(
        env.segment_files().len(),
        1,
        "only the newest segment is kept"
    );
    let w = wal.start_writer().unwrap();
    assert_eq!(
        append_all(&w, [batch(1, 41, 1, 10)]).await[0]
            .as_ref()
            .unwrap(),
        &41
    );
}
