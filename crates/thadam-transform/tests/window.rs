//! `window` through a whole chain: aggregates are the same however the input
//! is batched, and a restart that re-reads from the held-back cursor, with
//! the sink's durable events trimmed, yields the continuous run's aggregates
//! once each.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::HashMap;

use common::{Frames, SOURCE, message, run, topic, warm};
use thadam_core::{Event, SourceId, WalEpoch, WalPos};
use thadam_transform::{Chain, ChainLimits, ChainSpec, FrameInput, RawRecordView};
use serde_json::json;

const FRAMES: u64 = 90;

fn spec() -> ChainSpec {
    ChainSpec::from_value(json!({
        "datasets": [{
            "id": "d",
            "match": { "source": SOURCE },
            "decode": "json",
            "identity": [{ "robot": "{topic.3}" }],
            "fields": [{ "name": "v", "type": "f64" }],
        }],
        "transforms": [{
            "kind": "window", "dataset": "d", "size": "10s",
            "aggregates": [
                { "field": "v", "fn": "count", "as": "n" },
                { "field": "v", "fn": "sum", "as": "sum" },
                { "field": "v", "fn": "max", "as": "hi" },
            ],
        }],
    }))
    .unwrap()
}

fn chain() -> Chain {
    Chain::compile(&spec(), ChainLimits::default()).unwrap()
}

/// The same chain with `sample: every_n: 3` ahead of the window.
fn sampled_chain() -> Chain {
    let spec = json!({
        "datasets": [{
            "id": "d",
            "match": { "source": SOURCE },
            "decode": "json",
            "identity": [{ "robot": "{topic.3}" }],
            "fields": [{ "name": "v", "type": "f64" }],
        }],
        "transforms": [
            { "kind": "sample", "dataset": "d", "every_n": 3 },
            { "kind": "window", "dataset": "d", "size": "10s",
              "aggregates": [{ "field": "v", "fn": "sum", "as": "sum" }] },
        ],
    });
    Chain::compile(
        &ChainSpec::from_value(spec).unwrap(),
        ChainLimits::default(),
    )
    .unwrap()
}

/// 1-based WAL positions, as the runtime assigns them. Three robots speak at
/// different paces so their windows open and close at different frames.
fn script() -> Frames {
    let mut frames = Frames::new();
    let mut now_s = 0i64;
    let mut seed = 7u64;
    for i in 0..FRAMES {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let robot = ["a", "b", "c"][(seed >> 33) as usize % 3];
        now_s += ((seed >> 40) % 5) as i64;
        let payload = json!({ "v": i as f64 * 0.5 }).to_string();
        let msg = message(&topic(robot, "x"), payload, now_s * 1_000_000);
        frames.records.push((i + 1, 0, msg));
    }
    frames
}

fn key(e: &Event) -> (u128, u64, String) {
    (
        e.id().as_u128(),
        e.wal_pos().get(),
        format!("{:?}", e.payload()),
    )
}

fn keys(events: &[Event]) -> Vec<(u128, u64, String)> {
    events.iter().map(key).collect()
}

/// The frames `from..=to`.
fn range(frames: &Frames, from: u64, to: u64) -> Frames {
    Frames {
        source: SourceId::new(SOURCE),
        records: frames
            .records
            .iter()
            .filter(|(p, _, _)| (from..=to).contains(p))
            .cloned()
            .collect(),
    }
}

fn process_by_frame(chain: &mut Chain, frames: &Frames, from: u64, to: u64) -> Vec<Event> {
    let mut out = Vec::new();
    for pos in from..=to {
        out.extend(run(chain, &range(frames, pos, pos)).events("d").to_vec());
    }
    out
}

fn warm_through_windows(chain: &mut Chain, frames: &Frames, from: u64, to: u64) {
    let part = range(frames, from, to);
    let views: Vec<RawRecordView<'_>> = part
        .records
        .iter()
        .map(|(pos, idx, m)| {
            RawRecordView::new(
                &part.source,
                m,
                WalEpoch::new(common::EPOCH),
                WalPos::new(*pos),
                *idx,
            )
        })
        .collect();
    chain.warm_open_windows(&FrameInput::new(&views, true));
}

#[test]
fn batching_does_not_change_the_aggregates() {
    let frames = script();
    let whole = run(&mut chain(), &frames).events("d").to_vec();
    assert!(whole.len() > 10, "{} aggregates", whole.len());
    assert_eq!(
        keys(&whole),
        keys(&process_by_frame(&mut chain(), &frames, 1, FRAMES))
    );

    let mut halves = chain();
    let mut parts = run(&mut halves, &range(&frames, 1, 37))
        .events("d")
        .to_vec();
    parts.extend(
        run(&mut halves, &range(&frames, 38, FRAMES))
            .events("d")
            .to_vec(),
    );
    assert_eq!(keys(&whole), keys(&parts));
}

#[test]
fn hold_is_the_first_frame_of_the_oldest_open_window() {
    let frames = script();
    let mut open: HashMap<String, (i64, u64)> = HashMap::new();
    let mut c = chain();
    assert_eq!(c.hold(), None);
    assert!(c.holds_back());
    for (pos, _, msg) in &frames.records {
        let robot = msg
            .attrs
            .get("topic")
            .and_then(|t| t.split('/').nth(3))
            .unwrap()
            .to_owned();
        let window = msg.ingest_time.as_nanos().div_euclid(10_000_000_000);
        let entry = open.entry(robot).or_insert((window, *pos));
        if window > entry.0 {
            *entry = (window, *pos);
        }
        run(&mut c, &range(&frames, *pos, *pos));
        let want = open.values().map(|(_, first)| *first).min();
        assert_eq!(c.hold().map(WalPos::get), want, "after frame {pos}");
    }
}

#[test]
fn a_restart_from_any_allowed_cursor_loses_and_repeats_nothing() {
    let frames = script();
    let all = process_by_frame(&mut chain(), &frames, 1, FRAMES);
    assert!(all.len() > 10);

    // `holds[m]`: the hold after frames 1..=m, which caps the cursor once the
    // sink is durable through frame m.
    let mut c = chain();
    let mut holds = vec![None];
    for pos in 1..=FRAMES {
        run(&mut c, &range(&frames, pos, pos));
        holds.push(c.hold().map(WalPos::get));
    }

    let mut restarts = 0;
    for m in 0..=FRAMES {
        let allowed = holds[m as usize].map_or(m, |h| h - 1);
        for cursor in [allowed, allowed / 2, 0] {
            let mut second = chain();
            warm(&mut second, &range(&frames, 1, cursor));
            let rerun = process_by_frame(&mut second, &frames, cursor + 1, FRAMES);

            // What the sink holds is whatever was delivered through `m`; the
            // runtime trims everything the re-read yields at or before it,
            // including aggregates a low cursor leaves partial.
            let delivered = all.iter().filter(|e| e.wal_pos().get() <= m);
            let got: Vec<_> = delivered
                .into_iter()
                .chain(rerun.iter().filter(|e| e.wal_pos().get() > m))
                .map(key)
                .collect();
            assert_eq!(got, keys(&all), "m={m} cursor={cursor}");
            restarts += 1;
        }
    }
    assert_eq!(restarts, 3 * (FRAMES + 1));
}

#[test]
fn a_cursor_past_the_hold_would_lose_a_window() {
    // The reason the hold exists: committed past an open window, a restart
    // cannot rebuild it, and its aggregate comes out partial.
    let frames = script();
    let all = process_by_frame(&mut chain(), &frames, 1, FRAMES);
    let mut c = chain();
    process_by_frame(&mut c, &frames, 1, 60);
    let hold = c.hold().unwrap().get();
    assert!(hold > 1 && hold <= 60);

    let mut second = chain();
    warm(&mut second, &range(&frames, 1, 60));
    let rerun = process_by_frame(&mut second, &frames, 61, FRAMES);
    let delivered = all.iter().filter(|e| e.wal_pos().get() <= 60);
    let got: Vec<_> = delivered
        .chain(rerun.iter().filter(|e| e.wal_pos().get() > 60))
        .map(key)
        .collect();
    assert_ne!(got, keys(&all));
}

#[test]
fn warm_up_leaves_windows_alone() {
    let frames = script();
    let mut skipped = chain();
    warm(&mut skipped, &range(&frames, 1, 30));
    assert_eq!(
        skipped.hold(),
        None,
        "windows opened by warm-up would pin the cursor"
    );

    let mut fed = chain();
    warm_through_windows(&mut fed, &frames, 1, 30);
    assert!(fed.hold().is_some());
}

#[test]
fn a_chain_rebuilt_mid_run_replays_from_the_hold_and_continues_identically() {
    let frames = script();
    let all = process_by_frame(&mut chain(), &frames, 1, FRAMES);
    for k in 1..FRAMES {
        let mut before = chain();
        process_by_frame(&mut before, &frames, 1, k);
        let hold = before.hold().map_or(k + 1, WalPos::get);

        let mut rebuilt = chain();
        warm_through_windows(&mut rebuilt, &frames, hold, k);
        assert_eq!(rebuilt.hold(), before.hold(), "k={k}");
        let rest = process_by_frame(&mut rebuilt, &frames, k + 1, FRAMES);
        let want: Vec<_> = all
            .iter()
            .filter(|e| e.wal_pos().get() > k)
            .cloned()
            .collect();
        assert_eq!(keys(&rest), keys(&want), "k={k}");
    }
}

#[test]
fn hold_back_does_not_fix_the_sample_phase_gap() {
    // `sample every_n` counts from the asset's first event, and warm-up only
    // sees `warm_start_frames` of history. The hold-back rebuilds windows
    // exactly, but the events a window sees after a restart are whatever the
    // upstream step lets through, so a window behind `sample` can still differ
    // from the continuous run when warm-up is shorter than the history.
    let frames = script();
    let all = process_by_frame(&mut sampled_chain(), &frames, 1, FRAMES);
    let mut c = sampled_chain();
    process_by_frame(&mut c, &frames, 1, 60);
    let cursor = c.hold().unwrap().get() - 1;
    assert!(cursor > 6);

    let restart = |warm_from: u64| {
        let mut second = sampled_chain();
        warm(&mut second, &range(&frames, warm_from, cursor));
        let rerun = process_by_frame(&mut second, &frames, cursor + 1, FRAMES);
        let delivered = all.iter().filter(|e| e.wal_pos().get() <= 60);
        delivered
            .chain(rerun.iter().filter(|e| e.wal_pos().get() > 60))
            .map(key)
            .collect::<Vec<_>>()
    };
    assert_eq!(restart(1), keys(&all), "warm-up covering the history");
    assert_ne!(restart(cursor - 1), keys(&all), "warm-up shorter than it");
}
