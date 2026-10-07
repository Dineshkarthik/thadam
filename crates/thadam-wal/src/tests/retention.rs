//! Retention, disk budget, `on_full`, `max_lag`, monotonic time, and readers
//! racing segment deletion.

use std::time::Duration;

use thadam_core::{BudgetCategory, WalPos, WalRange};

use super::util::*;
use crate::{ConsumerSpec, CursorState, Gap, MaxLag, OnFull, ReadItem, StartFrom};

const SEG: u64 = 16 << 10;
const LIMIT: u64 = 4 * SEG;

/// Frames of about 1.1 KiB, so a 16 KiB segment holds about 14.
fn big(run: u32, k: u64) -> thadam_core::RawBatch {
    batch(run, k, 2, 512)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn block_withholds_tickets_and_stays_within_budget_plus_one_segment() {
    let env = Env::with_limit(LIMIT);
    let mut o = small_opts(OnFull::Block);
    o.consumers = vec![ConsumerSpec::required("c")];
    let (wal, _) = env.open(o);
    let w = wal.start_writer().unwrap();
    let mut r = wal.reader("c", StartFrom::Earliest).unwrap();
    let mut next_k = 1;
    let mut max_disk = 0;
    for _round in 0..4 {
        // Fill until a ticket stops resolving: the writer is blocked.
        let blocked = loop {
            let t = w.submit(big(0, next_k)).await.unwrap();
            next_k += 1;
            match tokio::time::timeout(Duration::from_millis(300), t).await {
                Ok(res) => {
                    res.unwrap();
                }
                Err(_) => break true,
            }
            max_disk = max_disk.max(env.segment_bytes_on_disk());
            assert!(next_k < 10_000, "never blocked");
        };
        assert!(blocked);
        max_disk = max_disk.max(env.segment_bytes_on_disk());
        assert!(env.budget.used(BudgetCategory::Wal) <= LIMIT);

        // The consumer catches up; the writer resumes.
        let mut last = 0;
        while let Some(item) = r.try_next(1 << 20).unwrap() {
            match item {
                ReadItem::Frames(c) => last = c.range().last().get(),
                ReadItem::Gap(g) => panic!("block must not lose data: {g:?}"),
            }
        }
        wal.commit_cursor("c", CursorState::at(WalPos::new(last)))
            .await
            .unwrap();
    }
    assert!(
        max_disk <= LIMIT + SEG,
        "disk {max_disk} exceeded budget plus one segment"
    );
    let stats = wal.stats();
    assert!(stats.budget_waits >= 4);
    assert_eq!(stats.evicted_bytes, 0);
    assert!(stats.deleted_segments > 0);
}

#[tokio::test]
async fn drop_oldest_evicts_within_budget_and_the_reader_sees_the_gap() {
    let env = Env::with_limit(LIMIT);
    let mut o = small_opts(OnFull::DropOldest);
    o.consumers = vec![ConsumerSpec::required("c")];
    let (wal, _) = env.open(o);
    let w = wal.start_writer().unwrap();
    let mut max_disk = 0;
    for k in 1..=200 {
        let r = append_all(&w, [big(0, k)]).await;
        assert_eq!(
            r[0].as_ref().unwrap(),
            &k,
            "drop_oldest never withholds acknowledgements"
        );
        max_disk = max_disk.max(env.segment_bytes_on_disk());
    }
    assert!(
        max_disk <= LIMIT + SEG,
        "disk {max_disk} exceeded budget plus one segment"
    );
    let stats = wal.stats();
    assert!(
        stats.evicted_bytes > 0 && stats.evicted_segments > 0,
        "{stats:?}"
    );
    let first = stats.first_seq.get();
    assert!(first > 1);

    let mut r = wal.reader("c", StartFrom::Earliest).unwrap();
    r.seek(WalPos::new(1));
    let items = drain(&mut r, 1 << 20);
    assert_eq!(
        items[0],
        Item::Gap(Gap::Evicted {
            range: WalRange::new(wal.epoch(), WalPos::new(1), WalPos::new(first - 1)).unwrap(),
            cursor: "c".into(),
        })
    );
    let frames: Vec<u64> = items[1..]
        .iter()
        .map(|i| match i {
            Item::Frame(s, Some((0, k))) if s == k => *s,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(frames, (first..=200).collect::<Vec<_>>());
}

#[tokio::test]
async fn maintain_under_drop_oldest_keeps_headroom() {
    let env = Env::with_limit(LIMIT);
    let mut o = small_opts(OnFull::DropOldest);
    o.consumers = vec![ConsumerSpec::required("c")];
    let (wal, _) = env.open(o);
    let w = wal.start_writer().unwrap();
    append_all(&w, (1..=60).map(|k| big(0, k))).await;
    let rep = wal.maintain().await.unwrap();
    assert!(
        env.budget.used(BudgetCategory::Wal) <= LIMIT - SEG,
        "{rep:?}"
    );
    assert_eq!(rep.retained_bytes, env.budget.used(BudgetCategory::Wal));
}

#[tokio::test]
async fn retention_waits_for_every_required_consumer() {
    let env = Env::new();
    let mut o = small_opts(OnFull::Block);
    o.consumers = vec![
        ConsumerSpec::required("a"),
        ConsumerSpec::required("b"),
        ConsumerSpec::optional("o"),
    ];
    let (wal, _) = env.open(o);
    let w = wal.start_writer().unwrap();
    append_all(&w, (1..=100).map(|k| big(0, k))).await;
    let segments = env.segment_files().len();
    assert!(segments >= 6);

    let rep = wal.maintain().await.unwrap();
    assert_eq!(
        rep.deleted_segments, 0,
        "a required consumer without a cursor pins everything"
    );

    wal.commit_cursor("a", CursorState::at(WalPos::new(100)))
        .await
        .unwrap();
    wal.commit_cursor("o", CursorState::at(WalPos::new(100)))
        .await
        .unwrap();
    assert_eq!(
        wal.maintain().await.unwrap().deleted_segments,
        0,
        "b still pins"
    );

    wal.commit_cursor("b", CursorState::at(WalPos::new(50)))
        .await
        .unwrap();
    let rep = wal.maintain().await.unwrap();
    assert!(rep.deleted_segments > 0);
    assert!(rep.first_retained.get() <= 51, "deleted past b: {rep:?}");

    // An in-flight range pins its frames too.
    let inflight = WalRange::new(wal.epoch(), WalPos::new(60), WalPos::new(70)).unwrap();
    wal.commit_cursor(
        "b",
        CursorState {
            committed: WalPos::new(99),
            inflight: Some(inflight),
        },
    )
    .await
    .unwrap();
    let rep = wal.maintain().await.unwrap();
    assert!(
        rep.first_retained.get() <= 60,
        "deleted an in-flight range: {rep:?}"
    );

    wal.commit_cursor("b", CursorState::at(WalPos::new(100)))
        .await
        .unwrap();
    wal.maintain().await.unwrap();
    assert_eq!(
        env.segment_files().len(),
        1,
        "the newest segment is always kept"
    );
}

#[tokio::test]
async fn retain_for_uses_monotonic_time_not_wall_clock() {
    let env = Env::new();
    let mut o = small_opts(OnFull::Block);
    o.consumers = vec![ConsumerSpec::required("c")];
    o.retain_for = Duration::from_secs(60);
    let (wal, _) = env.open(o);
    let w = wal.start_writer().unwrap();
    append_all(&w, (1..=60).map(|k| big(0, k))).await;
    wal.commit_cursor("c", CursorState::at(WalPos::new(60)))
        .await
        .unwrap();
    let segments = env.segment_files().len();

    assert_eq!(wal.maintain().await.unwrap().deleted_segments, 0);
    // An NTP step forward by a day must not age anything.
    env.clock.step_wall(86_400 * 1_000_000_000);
    assert_eq!(wal.maintain().await.unwrap().deleted_segments, 0);
    // Nor may a backwards step by a year make anything look younger.
    env.clock.step_wall(-365 * 86_400 * 1_000_000_000);
    env.clock.advance(Duration::from_secs(59));
    assert_eq!(wal.maintain().await.unwrap().deleted_segments, 0);
    assert_eq!(env.segment_files().len(), segments);
    env.clock.advance(Duration::from_secs(2));
    let rep = wal.maintain().await.unwrap();
    assert_eq!(rep.deleted_segments, segments - 1);
}

#[tokio::test]
async fn budget_pressure_overrides_retain_for() {
    let env = Env::with_limit(LIMIT);
    let mut o = small_opts(OnFull::Block);
    o.consumers = vec![ConsumerSpec::required("c")];
    o.retain_for = Duration::from_secs(3600);
    let (wal, _) = env.open(o);
    let w = wal.start_writer().unwrap();
    let mut r = wal.reader("c", StartFrom::Earliest).unwrap();
    for k in 1..=150 {
        let t = w.submit(big(0, k)).await.unwrap();
        // Keep the consumer caught up so only retain_for holds segments.
        let pos = t.await.unwrap().last();
        while r.try_next(1 << 20).unwrap().is_some() {}
        wal.commit_cursor("c", CursorState::at(pos)).await.unwrap();
    }
    assert!(env.segment_bytes_on_disk() <= LIMIT + SEG);
    assert!(wal.stats().deleted_segments > 0);
    assert_eq!(wal.stats().evicted_bytes, 0);
}

#[tokio::test]
async fn a_consumer_past_max_lag_stops_pinning_and_sees_eviction() {
    let env = Env::new();
    let mut o = small_opts(OnFull::Block);
    o.consumers = vec![ConsumerSpec::required("slow").with_max_lag(MaxLag::Bytes(3 * SEG))];
    let (wal, _) = env.open(o);
    let w = wal.start_writer().unwrap();
    append_all(&w, (1..=20).map(|k| big(0, k))).await;
    let rep = wal.maintain().await.unwrap();
    assert!(rep.lagging_consumers.is_empty(), "{rep:?}");
    assert_eq!(rep.deleted_segments, 0);

    append_all(&w, (21..=100).map(|k| big(0, k))).await;
    let rep = wal.maintain().await.unwrap();
    assert_eq!(rep.lagging_consumers, vec!["slow".to_owned()]);
    assert!(rep.deleted_segments > 0);
    assert_eq!(wal.stats().lag_evictions, 1);
    let mut r = wal.reader("slow", StartFrom::Earliest).unwrap();
    r.seek(WalPos::new(1));
    let first = drain(&mut r, 1 << 20).remove(0);
    assert!(matches!(first, Item::Gap(Gap::Evicted { .. })), "{first:?}");
}

#[tokio::test]
async fn max_lag_by_age_uses_the_monotonic_clock() {
    let env = Env::new();
    let mut o = small_opts(OnFull::Block);
    o.consumers =
        vec![ConsumerSpec::required("slow").with_max_lag(MaxLag::Age(Duration::from_secs(30)))];
    let (wal, _) = env.open(o);
    let w = wal.start_writer().unwrap();
    append_all(&w, (1..=40).map(|k| big(0, k))).await;
    env.clock.step_wall(-86_400 * 1_000_000_000);
    assert!(wal.maintain().await.unwrap().lagging_consumers.is_empty());
    env.clock.advance(Duration::from_secs(31));
    assert_eq!(
        wal.maintain().await.unwrap().lagging_consumers,
        vec!["slow".to_owned()]
    );
}

#[tokio::test]
async fn a_reader_keeps_reading_a_segment_unlinked_under_it() {
    let env = Env::new();
    let o = small_opts(OnFull::Block); // no required consumers: nothing pins
    let (wal, _) = env.open(o);
    let w = wal.start_writer().unwrap();
    append_all(&w, (1..=60).map(|k| big(0, k))).await;
    assert!(env.segment_files().len() >= 4);

    // Positioned mid-way through the first segment, holding its descriptor.
    let mut r = wal.reader("r", StartFrom::Earliest).unwrap();
    let Some(ReadItem::Frames(c)) = r.try_next(3000).unwrap() else {
        panic!()
    };
    let read_so_far = c.range().last().get();
    assert!(read_so_far >= 1);
    // A second reader that has not opened anything yet.
    let mut late = wal.reader("late", StartFrom::At(WalPos::new(1))).unwrap();

    let rep = wal.maintain().await.unwrap();
    assert_eq!(env.segment_files().len(), 1, "{rep:?}");
    let first = rep.first_retained.get();

    let items = drain(&mut r, 1 << 20);
    let mut expect = read_so_far + 1;
    let mut saw_gap = false;
    let mut from_unlinked = 0;
    for item in &items {
        match item {
            Item::Frame(s, Some((0, k))) if s == k && *s == expect => {
                expect += 1;
                if !saw_gap {
                    from_unlinked += 1;
                }
            }
            Item::Gap(Gap::Evicted { range, .. }) if range.first().get() == expect => {
                assert_eq!(range.last().get() + 1, first);
                saw_gap = true;
                expect = first;
            }
            other => panic!("unexpected {other:?} at {expect}"),
        }
    }
    assert!(saw_gap);
    assert_eq!(expect, 61);
    assert!(
        from_unlinked > 0,
        "the rest of the unlinked segment was not read"
    );

    let items = drain(&mut late, 1 << 20);
    assert!(matches!(&items[0], Item::Gap(Gap::Evicted { range, .. }) if range.first().get() == 1));
    assert_eq!(items.len() as u64, 1 + (61 - first));
}
