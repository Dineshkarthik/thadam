//! Warm-start deduplication across a restart, and replay row-set
//! equivalence.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::Path;

use common::*;
use thadam_core::{Shutdown, WalPos};
use thadam_runtime::{
    ExitStatus, Phase, Pipeline, ReplayOutput, ReplayRequest, RuntimeOptions, replay,
};
use thadam_testkit::{MemorySourceConfig, TempDataDir};

fn dedup_yaml(dir: &Path) -> String {
    yaml(
        dir,
        "  - { id: out, kind: recseg, config: { frames_per_file: 4 } }",
        "",
    )
    .replace(
        "    decode: json\n",
        "    decode: json\n    event_id: { key: [n] }\n    dedup: { window: 1h, max_keys: 10k }\n",
    )
}

fn per_frame() -> RuntimeOptions {
    let mut o = fast_opts();
    o.batch_limits.max_messages = 1;
    o
}

async fn redelivery_after_restart(warm_frames: u64) -> std::collections::BTreeMap<u64, usize> {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let y = dedup_yaml(dir.path());
    kit.sources.scripted("src", (1..=50).map(json_n));
    assert_eq!(
        run_to_end(&kit, spec(&y), per_frame()).await.exit,
        ExitStatus::Ok
    );
    // The broker redelivers 40..=50 after the restart (a lost session).
    kit.sources.scripted("src", (40..=80).map(json_n));
    let mut o = per_frame();
    o.warm_start_frames = warm_frames;
    assert_eq!(run_to_end(&kit, spec(&y), o).await.exit, ExitStatus::Ok);
    counts(&kit.rec.store("out").ns())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn warm_start_dedups_redelivery_across_a_restart() {
    let warm = redelivery_after_restart(10_000).await;
    assert_eq!(warm.len(), 80);
    assert!(warm.values().all(|c| *c == 1), "{warm:?}");

    let cold = redelivery_after_restart(0).await;
    assert_eq!(cold.len(), 80);
    assert!(
        (40..=50).all(|n| cold[&n] == 2),
        "without warm start the redelivery is duplicated: {cold:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_is_row_set_equivalent_to_the_continuous_run() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let y = dedup_yaml(dir.path());
    let payloads = (1..=60).chain(20..=30).chain(61..=70).map(json_n);
    kit.sources.scripted("src", payloads);
    let report = run_to_end(&kit, spec(&y), per_frame()).await;
    assert_eq!(report.exit, ExitStatus::Ok);
    let end = report.status.wal.as_ref().unwrap().durable_end;
    assert_eq!(end, 81);
    let mut original = kit.rec.store("out").rows();
    original.sort();
    assert_eq!(original.len(), 70, "the in-stream repeats are deduplicated");
    let cursor_before = cursor(&report.status, "out");
    let meta_before = thadam_runtime::read_thadam_json(dir.path()).unwrap();

    let full = replay(
        &spec(&y),
        &kit.registry(),
        per_frame(),
        ReplayRequest::new("out", WalPos::new(1), "full"),
    )
    .await
    .unwrap();
    assert_eq!(full.frames, 81);
    assert_eq!(full.events, 70);
    assert!(
        full.output_dir
            .as_ref()
            .unwrap()
            .ends_with("replays/full/out")
    );
    let mut replayed = kit.rec.store("replay.full.out").rows();
    replayed.sort();
    assert_eq!(replayed, original, "same event ids, values, and flags");

    // A sub-range: warm start sees frames 1..=60, so the repeats at 61..=71
    // are dropped exactly as the continuous run dropped them.
    let part = replay(
        &spec(&y),
        &kit.registry(),
        per_frame(),
        ReplayRequest::new("out", WalPos::new(55), "part").to(WalPos::new(75)),
    )
    .await
    .unwrap();
    assert_eq!(part.range, Some((55, 75)));
    let mut sub = kit.rec.store("replay.part.out").rows();
    sub.sort();
    let want: Vec<Row> = original
        .iter()
        .filter(|r| (55..=75).contains(&r.pos))
        .cloned()
        .collect();
    assert_eq!(sub, want);

    // Production state is untouched; a replay id is single-use.
    kit.sources.scripted("src", std::iter::empty());
    let after = run_to_end(&kit, spec(&y), per_frame()).await;
    assert_eq!(cursor(&after.status, "out"), cursor_before);
    assert_eq!(kit.rec.store("out").rows().len(), 70);
    assert_eq!(
        thadam_runtime::read_thadam_json(dir.path())
            .unwrap()
            .unwrap()
            .wal,
        meta_before.unwrap().wal
    );
    let again = replay(
        &spec(&y),
        &kit.registry(),
        per_frame(),
        ReplayRequest::new("out", WalPos::new(1), "full"),
    )
    .await;
    assert!(again.unwrap_err().message.contains("already holds data"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_into_a_caller_sink_and_refusal_while_running() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let y = dedup_yaml(dir.path());
    kit.sources.scripted("src", (1..=10).map(json_n));
    assert_eq!(
        run_to_end(&kit, spec(&y), per_frame()).await.exit,
        ExitStatus::Ok
    );

    let store = RecStore::default();
    let out = ReplayOutput::Sink(Box::new(RecSink::new(store.clone(), 2)));
    let r = replay(
        &spec(&y),
        &kit.registry(),
        per_frame(),
        ReplayRequest::new("out", WalPos::new(3), "mine").output(out),
    )
    .await
    .unwrap();
    assert_eq!(r.events, 8);
    assert_eq!(store.ns(), (3..=10).collect::<Vec<_>>());
    assert!(r.output_dir.is_none());

    kit.sources.memory("src", MemorySourceConfig::default());
    let p = Pipeline::build(spec(&y), &kit.registry(), per_frame()).unwrap();
    let handle = p.handle();
    let shutdown = Shutdown::new();
    let task = tokio::spawn(p.run(shutdown.clone()));
    handle.wait_for(Phase::Running).await;
    let err = replay(
        &spec(&y),
        &kit.registry(),
        per_frame(),
        ReplayRequest::new("out", WalPos::new(1), "busy"),
    )
    .await
    .unwrap_err();
    assert_eq!(
        thadam_runtime::ExitStatus::for_error(&err),
        ExitStatus::DataDir
    );
    shutdown.trigger();
    assert_eq!(task.await.unwrap().unwrap().exit, ExitStatus::Ok);
}
