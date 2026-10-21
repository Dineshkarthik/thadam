//! `window` through a whole chain: aggregates are the same however the input
//! is batched, and a restart that re-reads from the held-back cursor, with
//! the sink's durable events trimmed, yields the continuous run's aggregates
//! once each.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::HashMap;

use common::{Frames, SOURCE, message, run, topic, warm};
use serde_json::json;
use thadam_core::{Event, SourceId, WalEpoch, WalPos};
use thadam_transform::{Chain, ChainLimits, ChainSpec, FrameInput, RawRecordView};

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

#[test]
fn idle_close_releases_a_silent_asset_and_restarts_match_the_continuous_run() {
    let build = || {
        let mut s = spec();
        let thadam_transform::TransformSpec::Window(w) = &mut s.transforms[0] else {
            unreachable!()
        };
        w.idle_close = Some(serde_json::from_value(json!("20s")).unwrap());
        Chain::compile(&s, ChainLimits::default()).unwrap()
    };
    let mut frames = script();
    // Robot `silent` speaks only in the first two frames; `idle_close: 20s`
    // closes its window once the fleet moves 20s past frame 2 and releases the
    // hold past frame 1.
    for pos in [1usize, 2] {
        frames.records[pos - 1].2 = message(
            &topic("silent", "x"),
            r#"{"v":100.0}"#,
            pos as i64 * 1_000_000,
        );
    }
    let all = process_by_frame(&mut build(), &frames, 1, FRAMES);
    assert!(
        all.iter().any(|e| e.asset().get("robot") == Some("silent")),
        "the silent asset's window closed on another asset's event"
    );

    let mut c = build();
    let mut holds = vec![None];
    for pos in 1..=FRAMES {
        run(&mut c, &range(&frames, pos, pos));
        holds.push(c.hold().map(WalPos::get));
    }
    assert!(
        holds.last().unwrap().unwrap() > 2,
        "hold advanced past the silent asset's frames: {holds:?}"
    );

    for m in 0..=FRAMES {
        let allowed = holds[m as usize].map_or(m, |h| h - 1);
        for cursor in [allowed, allowed / 2, 0] {
            let mut second = build();
            warm(&mut second, &range(&frames, 1, cursor));
            let rerun = process_by_frame(&mut second, &frames, cursor + 1, FRAMES);
            let delivered = all.iter().filter(|e| e.wal_pos().get() <= m);
            let got: Vec<_> = delivered
                .chain(rerun.iter().filter(|e| e.wal_pos().get() > m))
                .map(key)
                .collect();
            assert_eq!(got, keys(&all), "m={m} cursor={cursor}");
        }
    }
}

fn event_time_chain() -> Chain {
    let spec = json!({
        "datasets": [{
            "id": "d",
            "match": { "source": SOURCE },
            "decode": "json",
            "identity": [{ "robot": "{topic.3}" }],
            "time": { "field": "/ts_s", "unit": "s", "domain": "utc" },
            "fields": [{ "name": "v", "type": "f64" }],
        }],
        "transforms": [{
            "kind": "window",
            "dataset": "d",
            "size": "10s",
            "time": "event",
            "allowed_lateness": "15s",
            "aggregates": [
                { "field": "v", "fn": "count", "as": "n" },
                { "field": "v", "fn": "sum", "as": "sum" },
                { "field": "v", "fn": "first", "as": "first" },
                { "field": "v", "fn": "last", "as": "last" },
                { "field": "v", "fn": "stddev", "as": "stddev" },
                { "field": "v", "fn": "range", "as": "range" },
                { "field": "v", "fn": "delta", "as": "delta" },
                { "field": "v", "fn": "rms", "as": "rms" },
            ],
        }],
    });
    Chain::compile(
        &ChainSpec::from_value(spec).unwrap(),
        ChainLimits::default(),
    )
    .unwrap()
}

/// Out-of-order event times across three robots: jitter up to +-18s around a
/// rising trend so each robot keeps multiple panes open at once and also emits
/// some late events beyond the 15s lateness budget.
fn out_of_order_script() -> Frames {
    let mut frames = Frames::new();
    let mut seed = 19u64;
    for i in 0..FRAMES {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let robot = ["a", "b", "c"][(seed >> 33) as usize % 3];
        let base_s = (i as i64) * 2 + 20;
        let jitter = ((seed >> 40) % 37) as i64 - 18;
        let ts_s = (base_s + jitter).max(0);
        let payload = json!({ "ts_s": ts_s, "v": (i + 1) as f64 }).to_string();
        let msg = message(&topic(robot, "x"), payload, (i as i64 + 1) * 1_000_000);
        frames.records.push((i + 1, 0, msg));
    }
    frames
}

#[test]
fn event_time_out_of_order_late_counts_and_restart_equivalence() {
    let frames = out_of_order_script();
    let mut whole_chain = event_time_chain();
    let all = process_by_frame(&mut whole_chain, &frames, 1, FRAMES);
    assert!(all.len() > 8, "{} aggregates", all.len());
    let stats = &whole_chain.stats().datasets[0].1;
    assert!(stats.late > 0, "script includes late events: {stats:?}");

    let mut c = event_time_chain();
    let mut holds = vec![None];
    for pos in 1..=FRAMES {
        run(&mut c, &range(&frames, pos, pos));
        holds.push(c.hold().map(WalPos::get));
    }

    for m in 0..=FRAMES {
        let allowed = holds[m as usize].map_or(m, |h| h - 1);
        for cursor in [allowed, allowed / 2, 0] {
            let mut second = event_time_chain();
            warm(&mut second, &range(&frames, 1, cursor));
            let rerun = process_by_frame(&mut second, &frames, cursor + 1, FRAMES);
            let delivered = all.iter().filter(|e| e.wal_pos().get() <= m);
            let got: Vec<_> = delivered
                .chain(rerun.iter().filter(|e| e.wal_pos().get() > m))
                .map(key)
                .collect();
            assert_eq!(got, keys(&all), "m={m} cursor={cursor}");
        }
    }
}

#[test]
fn event_time_excludes_non_utc_and_flags_gap_and_range_start() {
    let monotonic = ChainSpec::from_value(json!({
        "datasets": [{
            "id": "d",
            "match": { "source": SOURCE },
            "decode": "json",
            "identity": [{ "robot": "{topic.3}" }],
            "time": { "field": "/ts_s", "unit": "s", "domain": "monotonic" },
            "fields": [{ "name": "v", "type": "f64" }],
        }],
        "transforms": [{
            "kind": "window",
            "dataset": "d",
            "size": "10s",
            "time": "event",
            "aggregates": [{ "field": "v", "fn": "count", "as": "n" }],
        }],
    }))
    .unwrap();
    let mut mono_chain = Chain::compile(&monotonic, ChainLimits::default()).unwrap();
    let mut f = Frames::new();
    f.records
        .push((1, 0, message(&topic("a", "x"), r#"{"ts_s":1,"v":1.0}"#, 1)));
    f.records
        .push((2, 0, message(&topic("a", "x"), r#"{"ts_s":20,"v":2.0}"#, 2)));
    assert!(run(&mut mono_chain, &f).events("d").is_empty());
    assert_eq!(mono_chain.stats().datasets[0].1.filtered, 2);

    let mut c = chain();
    c.mark_range_start();
    let frames = script();
    let first = process_by_frame(&mut c, &frames, 1, 15);
    c.on_gap();
    let rest = process_by_frame(&mut c, &frames, 16, FRAMES);
    let all: Vec<_> = first.into_iter().chain(rest).collect();
    assert!(
        all.iter()
            .any(|e| e.quality().reason.as_deref() == Some("window_range_start"))
    );
    assert!(
        all.iter()
            .any(|e| e.quality().reason.as_deref() == Some("window_gap"))
    );
}

#[derive(Clone, Copy, Debug)]
struct RawSample {
    pos: u64,
    robot: &'static str,
    t_ns: i64,
    v: f64,
}

fn sliding_chain(event_time: bool) -> Chain {
    let mut ds = json!({
        "id": "d",
        "match": { "source": SOURCE },
        "decode": "json",
        "identity": [{ "robot": "{topic.3}" }],
        "fields": [{ "name": "v", "type": "f64" }],
    });
    let mut win = json!({
        "kind": "window",
        "dataset": "d",
        "size": "30s",
        "slide": "10s",
        "aggregates": [
            { "field": "v", "fn": "count", "as": "count" },
            { "field": "v", "fn": "sum", "as": "sum" },
            { "field": "v", "fn": "min", "as": "min" },
            { "field": "v", "fn": "max", "as": "max" },
            { "field": "v", "fn": "mean", "as": "mean" },
            { "field": "v", "fn": "first", "as": "first" },
            { "field": "v", "fn": "last", "as": "last" },
            { "field": "v", "fn": "stddev", "as": "stddev" },
            { "field": "v", "fn": "range", "as": "range" },
            { "field": "v", "fn": "delta", "as": "delta" },
            { "field": "v", "fn": "rms", "as": "rms" },
        ],
    });
    if event_time {
        ds["time"] = json!({ "field": "/ts_s", "unit": "s", "domain": "utc" });
        win["time"] = json!("event");
        win["allowed_lateness"] = json!("15s");
    }
    Chain::compile(
        &ChainSpec::from_value(json!({
            "datasets": [ds],
            "transforms": [win],
        }))
        .unwrap(),
        ChainLimits::default(),
    )
    .unwrap()
}

fn sliding_script(event_time: bool) -> (Frames, Vec<RawSample>) {
    let mut frames = Frames::new();
    let mut samples = Vec::new();
    let mut ingest_s = 0i64;
    let mut robot_base_s = [20i64, 20, 20];
    let mut seed = 43u64;
    for i in 0..FRAMES {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let r_idx = (seed >> 33) as usize % 3;
        let robot = ["a", "b", "c"][r_idx];
        ingest_s += ((seed >> 40) % 4) as i64 + 1;
        robot_base_s[r_idx] += ((seed >> 44) % 5) as i64 + 2;
        // Jitter in [-7, +7] keeps max_event_s - ts_s <= 14s <= 15s allowed_lateness
        // while reordering events across 10s pane boundaries.
        let jitter = ((seed >> 48) % 15) as i64 - 7;
        let ts_s = if event_time {
            robot_base_s[r_idx] + jitter
        } else {
            ingest_s
        };
        let v = ((i as i64 % 17) - 5) as f64 * 1.25 + (r_idx as f64) * 0.5;
        let payload = json!({ "ts_s": ts_s, "v": v }).to_string();
        let msg = message(&topic(robot, "x"), payload, ingest_s * 1_000_000);
        let pos = i + 1;
        frames.records.push((pos, 0, msg));
        samples.push(RawSample {
            pos,
            robot,
            t_ns: ts_s * 1_000_000_000,
            v,
        });
    }
    (frames, samples)
}

#[test]
fn sliding_window_pane_merge_equals_brute_force_and_restarts_match() {
    const SLIDE_NS: i64 = 10_000_000_000;
    const SIZE_NS: i64 = 30_000_000_000;
    const LATENESS_NS: i64 = 15_000_000_000;

    for event_time in [false, true] {
        let (frames, samples) = sliding_script(event_time);
        let mut whole = sliding_chain(event_time);
        let all = process_by_frame(&mut whole, &frames, 1, FRAMES);
        assert!(
            all.len() >= 15,
            "event_time={event_time}: {} rows",
            all.len()
        );
        assert_eq!(whole.stats().datasets[0].1.late, 0);

        for robot in ["a", "b", "c"] {
            let r_samples: Vec<RawSample> = samples
                .iter()
                .copied()
                .filter(|s| s.robot == robot)
                .collect();
            let cutoff_end_ns = if event_time {
                let max_t = r_samples.iter().map(|s| s.t_ns).max().unwrap();
                max_t - LATENESS_NS
            } else {
                let last_t = r_samples.last().unwrap().t_ns;
                last_t.div_euclid(SLIDE_NS) * SLIDE_NS
            };
            let min_t = r_samples.iter().map(|s| s.t_ns).min().unwrap();
            let first_w = min_t.div_euclid(SLIDE_NS) - 2;
            let last_w = (cutoff_end_ns - SIZE_NS).div_euclid(SLIDE_NS);

            let mut expected_windows = Vec::new();
            for w in first_w..=last_w {
                let start_ns = w * SLIDE_NS;
                let end_ns = start_ns + SIZE_NS;
                if end_ns > cutoff_end_ns {
                    continue;
                }
                let win_samples: Vec<RawSample> = r_samples
                    .iter()
                    .copied()
                    .filter(|s| s.t_ns >= start_ns && s.t_ns < end_ns)
                    .collect();
                if !win_samples.is_empty() {
                    expected_windows.push((start_ns, end_ns, win_samples));
                }
            }

            let actual: Vec<&Event> = all
                .iter()
                .filter(|e| e.asset().get("robot") == Some(robot))
                .collect();
            assert_eq!(
                actual.len(),
                expected_windows.len(),
                "event_time={event_time} robot={robot}"
            );

            for (ev, (start_ns, end_ns, win_samples)) in actual.into_iter().zip(expected_windows) {
                let rec = ev.payload().as_record().unwrap();
                let ts = |col: &str| rec.get(col).unwrap().as_ts().unwrap().as_nanos();
                let f = |col: &str| rec.get(col).unwrap().as_f64().unwrap();

                assert_eq!(ts("window_start"), start_ns);
                assert_eq!(ts("window_end"), end_ns);

                let n = win_samples.len() as u64;
                let sum: f64 = win_samples.iter().map(|s| s.v).sum();
                let min = win_samples
                    .iter()
                    .map(|s| s.v)
                    .fold(f64::INFINITY, f64::min);
                let max = win_samples
                    .iter()
                    .map(|s| s.v)
                    .fold(f64::NEG_INFINITY, f64::max);
                let mean = sum / n as f64;
                let first = win_samples
                    .iter()
                    .min_by_key(|s| (s.t_ns, s.pos))
                    .unwrap()
                    .v;
                let last = win_samples
                    .iter()
                    .max_by_key(|s| (s.t_ns, s.pos))
                    .unwrap()
                    .v;
                let stddev = (win_samples
                    .iter()
                    .map(|s| (s.v - mean).powi(2))
                    .sum::<f64>()
                    / n as f64)
                    .sqrt();
                let range_v = max - min;
                let delta = last - first;
                let rms = (win_samples.iter().map(|s| s.v * s.v).sum::<f64>() / n as f64).sqrt();

                assert_eq!(rec.get("count").unwrap().as_u64().unwrap(), n);
                assert!((f("sum") - sum).abs() < 1e-9, "{} vs {sum}", f("sum"));
                assert_eq!(f("min"), min);
                assert_eq!(f("max"), max);
                assert!((f("mean") - mean).abs() < 1e-9, "{} vs {mean}", f("mean"));
                assert_eq!(f("first"), first);
                assert_eq!(f("last"), last);
                assert!(
                    (f("stddev") - stddev).abs() < 1e-9,
                    "{} vs {stddev}",
                    f("stddev")
                );
                assert_eq!(f("range"), range_v);
                assert_eq!(f("delta"), delta);
                assert!((f("rms") - rms).abs() < 1e-9, "{} vs {rms}", f("rms"));
            }
        }

        let mut c = sliding_chain(event_time);
        let mut holds = vec![None];
        for pos in 1..=FRAMES {
            run(&mut c, &range(&frames, pos, pos));
            holds.push(c.hold().map(WalPos::get));
        }
        for m in 0..=FRAMES {
            let allowed = holds[m as usize].map_or(m, |h| h - 1);
            for cursor in [allowed, allowed / 2, 0] {
                let mut second = sliding_chain(event_time);
                warm(&mut second, &range(&frames, 1, cursor));
                let rerun = process_by_frame(&mut second, &frames, cursor + 1, FRAMES);
                let delivered = all.iter().filter(|e| e.wal_pos().get() <= m);
                let got: Vec<_> = delivered
                    .chain(rerun.iter().filter(|e| e.wal_pos().get() > m))
                    .map(key)
                    .collect();
                assert_eq!(
                    got,
                    keys(&all),
                    "event_time={event_time} m={m} cursor={cursor}"
                );
            }
        }
    }
}
