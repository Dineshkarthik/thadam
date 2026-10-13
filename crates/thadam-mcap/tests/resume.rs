//! Resume tests: a source built with a resume directory acknowledges after
//! the (simulated) WAL commit and a restart on the same directory neither
//! re-emits committed messages nor loses uncommitted ones.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::{Msg, write_mcap};
use thadam_core::{
    AckOutcome, BatchLimits, RawMessage, Shutdown, Source, SourceContext, SourceId, SystemClock,
    codes,
};
use thadam_mcap::{McapConfig, McapSource, attr};
use serde_json::{Value, json};

fn limits() -> BatchLimits {
    BatchLimits {
        max_messages: 4,
        max_bytes: 1 << 24,
        linger: Duration::from_millis(50),
    }
}

fn log_times(messages: &[RawMessage]) -> Vec<u64> {
    messages
        .iter()
        .map(|m| m.attrs.get(attr::MCAP_LOG_TIME).unwrap().parse().unwrap())
        .collect()
}

/// Two log-time-ordered files whose messages interleave in the merge, each
/// alternating the `/joint_states` and `/annotations` topics so a topic
/// filter skips messages inside the files.
fn write_fixture(dir: &Path) -> (PathBuf, PathBuf) {
    let a = dir.join("a.mcap");
    let b = dir.join("b.mcap");
    let msgs = |first: u64| -> Vec<Msg> {
        (0..12u64)
            .map(|i| {
                let (time, seq) = (first + 20 * i, i as u32);
                if i % 2 == 0 {
                    Msg::joint(time, seq)
                } else {
                    Msg::json(time, seq)
                }
            })
            .collect()
    };
    write_mcap(&a, &msgs(100), true);
    write_mcap(&b, &msgs(110), false);
    (a, b)
}

fn config(files: &[&Path], extra: Value) -> Value {
    let mut cfg = json!({ "paths": files });
    cfg.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    cfg
}

async fn open(cfg: &Value, resume: Option<&Path>) -> McapSource {
    let mut source = McapSource::new(McapConfig::from_value(cfg).expect("valid config"));
    if let Some(dir) = resume {
        source = source.with_resume_dir(dir);
    }
    let ctx = SourceContext::new(
        SourceId::new("test"),
        Shutdown::new(),
        Arc::new(SystemClock::new()),
    );
    source.open(&ctx).await.expect("open");
    source
}

/// What an uninterrupted, resume-less replay emits.
async fn baseline(cfg: &Value) -> Vec<u64> {
    let mut source = open(cfg, None).await;
    let mut out = Vec::new();
    while let Some(batch) = source.next_batch(limits()).await.unwrap() {
        out.extend(log_times(&batch.batch.messages));
        batch.ack.complete(AckOutcome::Committed);
    }
    out
}

/// One run against a simulated WAL. The first `commit` batches are
/// committed (appended to `wal`, then acknowledged, as the runtime does);
/// then `inflight` more batches are pulled and never committed, and the
/// source is dropped without `close`: a crash. `usize::MAX` for `commit`
/// runs to the end.
async fn run(cfg: &Value, resume: &Path, wal: &mut Vec<u64>, commit: usize, inflight: usize) {
    let mut source = open(cfg, Some(resume)).await;
    let mut pulled = 0;
    let mut held = Vec::new();
    while let Some(batch) = source.next_batch(limits()).await.unwrap() {
        pulled += 1;
        if pulled <= commit {
            wal.extend(log_times(&batch.batch.messages));
            batch.ack.complete(AckOutcome::Committed);
        } else {
            held.push(batch);
            if held.len() == inflight {
                break;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_run_on_the_same_directory_emits_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = write_fixture(dir.path());
    let resume = dir.path().join("sources").join("bag");
    let cfg = config(&[&a, &b], json!({}));

    let mut wal = Vec::new();
    run(&cfg, &resume, &mut wal, usize::MAX, 0).await;
    assert_eq!(wal, baseline(&cfg).await);
    assert_eq!(wal.len(), 24);
    assert!(resume.join("resume.json").is_file());

    run(&cfg, &resume, &mut wal, usize::MAX, 0).await;
    assert_eq!(wal.len(), 24, "the second run re-emitted messages");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_mid_run_loses_nothing_and_duplicates_nothing() {
    for extra in [json!({}), json!({ "topics": ["/joint_states"] })] {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = write_fixture(dir.path());
        let resume = dir.path().join("sources").join("bag");
        let cfg = config(&[&a, &b], extra);

        let mut wal = Vec::new();
        // Crash with one batch committed and one in flight, twice, then finish.
        run(&cfg, &resume, &mut wal, 1, 1).await;
        assert_eq!(wal.len(), 4);
        run(&cfg, &resume, &mut wal, 1, 1).await;
        assert_eq!(wal.len(), 8);
        run(&cfg, &resume, &mut wal, usize::MAX, 0).await;

        assert_eq!(wal, baseline(&cfg).await, "{cfg}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_file_is_replayed_from_the_start() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = write_fixture(dir.path());
    let resume = dir.path().join("sources").join("bag");
    let cfg = config(&[&a, &b], json!({}));

    let mut wal = Vec::new();
    run(&cfg, &resume, &mut wal, usize::MAX, 0).await;
    wal.clear();

    let mut grown: Vec<Msg> = (0..12u64)
        .map(|i| Msg::joint(110 + 20 * i, i as u32))
        .collect();
    grown.push(Msg::joint(400, 12));
    write_mcap(&b, &grown, false);

    run(&cfg, &resume, &mut wal, usize::MAX, 0).await;
    let mut want: Vec<u64> = (0..12).map(|i| 110 + 20 * i).collect();
    want.push(400);
    assert_eq!(wal, want, "only the changed file replays, from its start");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_state_write_is_not_silent() {
    let dir = tempfile::tempdir().unwrap();
    let (a, _) = write_fixture(dir.path());
    let cfg = config(&[&a], json!({}));
    // The state directory is usable at open but breaks before the first ack.
    let resume = dir.path().join("sources");
    let mut source = open(&cfg, Some(&resume)).await;
    std::fs::write(&resume, b"now a file").unwrap();
    let batch = source.next_batch(limits()).await.unwrap().unwrap();
    batch.ack.complete(AckOutcome::Committed);
    let err = source.next_batch(limits()).await.unwrap_err();
    assert_eq!(err.code, codes::BUFFER_IO);
    assert_eq!(source.close().await.unwrap_err().code, codes::BUFFER_IO);
}
