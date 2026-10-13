//! `window` and the cursor: the sink cursor trails the oldest open window, and
//! a restart, a crash around the cursor commit, or a chain panic mid-window
//! loses no aggregate and repeats none.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use common::*;
use thadam_core::{
    BuildContext, ErrorDomain, Reject, ThadamError, SegmentBatch, SegmentSink, SegmentSinkFactory,
    Shutdown, SinkContext, WalPos, codes,
};
use thadam_runtime::{ExitStatus, Pipeline, Registry, RuntimeOptions, failpoints};
use thadam_testkit::TempDataDir;

fn config(dir: &Path) -> String {
    format!(
        r#"version: 1
pipeline: {{ name: test, node_id: node-1, data_dir: "{dir}" }}
sources:
  - {{ id: src, kind: memory }}
wal: {{ max_disk: 64MiB, segment_bytes: 1MiB, fsync: {{ interval: 1ms }}, on_full: block, retain_for: 1h }}
datasets:
  - id: d
    match: {{ source: src }}
    decode: json
    identity: {{ robot: {{ id: 100, template: "{{topic.1}}" }} }}
    fields:
      - {{ id: 1000, name: v, type: f64, from: /v }}
transforms:
  - {{ kind: window, dataset: d, size: 10s, aggregates: [{{ field: v, fn: count, as: n }}, {{ field: v, fn: sum, as: total }}] }}
sinks:
  - {{ id: out, kind: recseg, config: {{ frames_per_file: 3 }} }}
"#,
        dir = dir.display()
    )
}

fn per_frame() -> RuntimeOptions {
    let mut o = fast_opts();
    o.batch_limits.max_messages = 1;
    o
}

/// How many bytes the chain reads from the WAL at a time: one frame, a few
/// frames, or everything durable, so batch edges fall at different frames.
const READS: [usize; 3] = [1, 300, 1 << 20];

fn reading(bytes: usize) -> RuntimeOptions {
    let mut o = per_frame();
    o.read_batch_bytes = bytes;
    o
}

fn rows(kit: &Kit) -> Vec<Row> {
    let mut rows = kit.rec.store("out").rows();
    rows.sort();
    rows
}

/// `(pos, n, total)` of each aggregate row, in position order.
fn summary(kit: &Kit) -> Vec<(u64, String)> {
    let mut out: Vec<_> = kit
        .rec
        .store("out")
        .rows()
        .iter()
        .map(|r| {
            let col = |name: &str| {
                r.values
                    .split(',')
                    .find_map(|kv| kv.strip_prefix(&format!("{name}=")))
                    .unwrap()
                    .to_owned()
            };
            (r.pos, format!("{} {}", col("n"), col("total")))
        })
        .collect();
    out.sort();
    out
}

fn assert_no_duplicates(rows: &[Row]) {
    let mut ids: Vec<u128> = rows.iter().map(|r| r.id).collect();
    ids.sort_unstable();
    let n = ids.len();
    ids.dedup();
    assert_eq!(ids.len(), n, "an aggregate was delivered twice: {rows:#?}");
}

async fn run(kit: &Kit, dir: &Path, msgs: &[Msg], o: RuntimeOptions) -> thadam_runtime::RunReport {
    run_with(kit, dir, msgs, o, true).await
}

/// Without a quarantine sink, recovery takes a different branch than with
/// one, so the restart tests run both ways.
async fn run_with(
    kit: &Kit,
    dir: &Path,
    msgs: &[Msg],
    mut o: RuntimeOptions,
    quarantine: bool,
) -> thadam_runtime::RunReport {
    feed(kit, msgs);
    if quarantine {
        return run_to_end(kit, spec(&config(dir)), o).await;
    }
    let mut registry = Registry::new();
    registry
        .source(Box::new(SlotSourceFactory(kit.sources.clone())))
        .segment_sink(Box::new(RecFactory {
            kind: "recseg",
            stores: kit.rec.clone(),
        }));
    o.stop_when_sources_finish = true;
    let p = Pipeline::build(spec(&config(dir)), &registry, o).unwrap();
    tokio::time::timeout(Duration::from_secs(60), p.run(Shutdown::new()))
        .await
        .expect("run timed out")
        .expect("run failed to start")
}

/// A recording sink whose `fail_at`th `append` fails once with a transient
/// error, so the driver recovers the sink and rewinds the chain.
struct FlakySink {
    inner: RecSink,
    fail_at: u64,
    appends: Arc<AtomicU64>,
}

#[async_trait]
impl SegmentSink for FlakySink {
    async fn recover(&mut self, ctx: &SinkContext) -> Result<Option<WalPos>, ThadamError> {
        self.inner.recover(ctx).await
    }

    async fn append(&mut self, batch: SegmentBatch) -> Result<Vec<Reject>, ThadamError> {
        if self.appends.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_at {
            return Err(ThadamError::transient(
                ErrorDomain::Sink,
                codes::SINK_IO,
                "injected append failure",
            ));
        }
        self.inner.append(batch).await
    }

    async fn poll_commit(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.inner.poll_commit().await
    }

    async fn finalize_all(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.inner.finalize_all().await
    }

    fn cursor_committed(&mut self, pos: WalPos) {
        self.inner.cursor_committed(pos);
    }
}

struct FlakyFactory {
    store: RecStore,
    fail_at: u64,
    appends: Arc<AtomicU64>,
}

impl SegmentSinkFactory for FlakyFactory {
    fn kind(&self) -> &'static str {
        "flaky"
    }
    fn config_schema(&self) -> serde_json::Value {
        serde_json::json!({"type": "object"})
    }
    fn validate(&self, _cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        Ok(())
    }
    fn build(
        &self,
        _cfg: serde_json::Value,
        _ctx: &BuildContext,
    ) -> Result<Box<dyn SegmentSink>, ThadamError> {
        Ok(Box::new(FlakySink {
            inner: RecSink::new(self.store.clone(), 3),
            fail_at: self.fail_at,
            appends: Arc::clone(&self.appends),
        }))
    }
}

/// Robot `b` speaks once and never again: its window stays open for good.
const PINNED: [Msg; 10] = [
    ("b", 0, 100),
    ("a", 1, 1),
    ("a", 3, 2),
    ("a", 5, 3),
    ("a", 8, 4),
    ("a", 12, 5),
    ("a", 15, 6),
    ("a", 21, 7),
    ("a", 24, 8),
    ("a", 33, 9),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cursor_stays_behind_the_oldest_open_window_and_a_restart_repeats_nothing() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();

    let first = run(&kit, dir.path(), &PINNED, per_frame()).await;
    assert_eq!(first.exit, ExitStatus::Ok, "{:#?}", first.status);
    assert_eq!(first.status.wal.as_ref().unwrap().durable_end, 10);
    assert_eq!(
        summary(&kit),
        [
            (6, "U64(4) F64(10.0)".to_owned()),
            (8, "U64(2) F64(11.0)".to_owned()),
            (10, "U64(2) F64(15.0)".to_owned()),
        ],
        "a's three closed windows, each at the frame that closed it"
    );
    assert_eq!(
        cursor(&first.status, "out"),
        0,
        "b's window opened at frame 1 and is still open: the sink holds frames through 10, the cursor not one"
    );

    // Restart with nothing new: the chain re-reads from the cursor, rebuilds
    // both open windows, and everything it re-emits is already in the sink.
    let again = run(&kit, dir.path(), &[], per_frame()).await;
    assert_eq!(again.exit, ExitStatus::Ok, "{:#?}", again.status);
    assert_eq!(summary(&kit).len(), 3, "nothing was delivered twice");
    assert_eq!(cursor(&again.status, "out"), 0);

    // More data after the restart: a's open window (frame 10) gets its
    // aggregate, once, with the values of the window as it was before the
    // restart plus the new frame.
    let more = run(
        &kit,
        dir.path(),
        &[("a", 34, 10), ("a", 45, 11)],
        per_frame(),
    )
    .await;
    assert_eq!(more.exit, ExitStatus::Ok, "{:#?}", more.status);
    assert_eq!(
        summary(&kit).last().unwrap(),
        &(12, "U64(2) F64(19.0)".to_owned())
    );
    assert_eq!(summary(&kit).len(), 4);
    assert_no_duplicates(&rows(&kit));
    assert_eq!(cursor(&more.status, "out"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cursor_stops_one_frame_short_of_the_open_window() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let msgs: [Msg; 6] = [
        ("a", 0, 1),
        ("a", 1, 2),
        ("a", 2, 3),
        ("a", 11, 4),
        ("a", 12, 5),
        ("a", 25, 6),
    ];
    let first = run(&kit, dir.path(), &msgs, per_frame()).await;
    assert_eq!(first.exit, ExitStatus::Ok, "{:#?}", first.status);
    assert_eq!(first.status.wal.as_ref().unwrap().durable_end, 6);
    assert_eq!(
        summary(&kit),
        [
            (4, "U64(3) F64(6.0)".to_owned()),
            (6, "U64(2) F64(9.0)".to_owned())
        ]
    );
    assert_eq!(
        cursor(&first.status, "out"),
        5,
        "the window opened at frame 6 is still needed"
    );

    let more = run(&kit, dir.path(), &[("a", 26, 7), ("a", 39, 8)], per_frame()).await;
    assert_eq!(more.exit, ExitStatus::Ok, "{:#?}", more.status);
    assert_eq!(
        summary(&kit).last().unwrap(),
        &(8, "U64(2) F64(13.0)".to_owned()),
        "frame 6 was read again after the restart, so the window is whole"
    );
    assert_eq!(summary(&kit).len(), 3);
    assert_eq!(cursor(&more.status, "out"), 7);
}

/// Two robots at their own pace, windows of 10 s: `n` messages.
fn script(n: i64) -> Vec<Msg> {
    (0..n)
        .map(|i| (if i % 3 == 2 { "b" } else { "a" }, i * 3, i as u64))
        .collect()
}

async fn baseline(msgs: &[Msg]) -> Vec<Row> {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let report = run(&kit, dir.path(), msgs, per_frame()).await;
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    assert_eq!(
        report.status.wal.as_ref().unwrap().durable_end,
        msgs.len() as u64,
        "one frame per message"
    );
    let rows = rows(&kit);
    assert!(rows.len() > 10, "{} aggregates", rows.len());
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_around_the_cursor_commit_loses_and_repeats_no_aggregate() {
    let msgs = script(60);
    let want = baseline(&msgs).await;
    for point in [
        failpoints::SEGMENT_BEFORE_CURSOR_COMMIT,
        failpoints::SEGMENT_BEFORE_CURSOR_COMMITTED,
    ] {
        for (prior_run, read) in [false, true]
            .into_iter()
            .flat_map(|p| READS.map(|r| (p, r)))
        {
            let ctx = format!("{point} prior_run={prior_run} read={read}");
            let dir = TempDataDir::new().unwrap();
            let kit = Kit::default();
            let mut next = 0;
            if prior_run {
                let prior = run(&kit, dir.path(), &msgs[..23], reading(read)).await;
                assert_eq!(prior.exit, ExitStatus::Ok, "{ctx}: {:#?}", prior.status);
                next = 23;
            }
            let o = reading(read);
            o.failpoints.arm(point, 1);
            let crashed = run(&kit, dir.path(), &msgs[next..], o.clone()).await;
            assert_eq!(
                crashed.exit,
                ExitStatus::RuntimeFailure,
                "{ctx}: {:#?}",
                crashed.status
            );
            assert!(!o.failpoints.is_armed(point), "{ctx}: failpoint never hit");

            // Frames are one per message, so whatever the crash cut off before
            // the WAL took it is what the source would redeliver.
            let ingested = crashed.status.wal.as_ref().unwrap().durable_end as usize;
            assert!(ingested <= msgs.len(), "{ctx}");
            let restarted = run(&kit, dir.path(), &msgs[ingested..], reading(read)).await;
            assert_eq!(
                restarted.exit,
                ExitStatus::Ok,
                "{ctx}: {:#?}",
                restarted.status
            );
            let got = rows(&kit);
            assert_no_duplicates(&got);
            assert_eq!(got, want, "{ctx}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_at_every_cut_of_the_input_matches_the_continuous_run() {
    let msgs = script(40);
    let want = baseline(&msgs).await;
    for (cut, read, quarantine) in [1, 4, 5, 11, 12, 20, 33, 39]
        .into_iter()
        .flat_map(|c| READS.map(|r| (c, r)))
        .flat_map(|(c, r)| [(c, r, true), (c, r, false)])
    {
        let ctx = format!("cut={cut} read={read} quarantine={quarantine}");
        let dir = TempDataDir::new().unwrap();
        let kit = Kit::default();
        let first = run_with(&kit, dir.path(), &msgs[..cut], reading(read), quarantine).await;
        assert_eq!(first.exit, ExitStatus::Ok, "{ctx}: {:#?}", first.status);
        let second = run_with(&kit, dir.path(), &msgs[cut..], reading(read), quarantine).await;
        assert_eq!(second.exit, ExitStatus::Ok, "{ctx}: {:#?}", second.status);
        let got = rows(&kit);
        assert_no_duplicates(&got);
        assert_eq!(got, want, "{ctx}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sink_that_loses_an_append_rewinds_the_chain_through_its_open_windows() {
    let msgs = script(60);
    let want = baseline(&msgs).await;
    let mut rewound = 0;
    for (fail_at, read) in [1, 3, 9, 20]
        .into_iter()
        .flat_map(|f| READS.map(|r| (f, r)))
    {
        let ctx = format!("fail_at={fail_at} read={read}");
        let dir = TempDataDir::new().unwrap();
        let kit = Kit::default();
        feed(&kit, &msgs);
        let appends = Arc::new(AtomicU64::new(0));
        let mut registry = Registry::new();
        registry
            .source(Box::new(SlotSourceFactory(kit.sources.clone())))
            .segment_sink(Box::new(FlakyFactory {
                store: kit.rec.store("out"),
                fail_at,
                appends: Arc::clone(&appends),
            }))
            .quarantine_sink(Box::new(RecFactory {
                kind: "recquar",
                stores: kit.quarantine.clone(),
            }));
        let y = config(dir.path()).replace(
            "kind: recseg, config: { frames_per_file: 3 }",
            "kind: flaky",
        );
        let mut o = reading(read);
        o.stop_when_sources_finish = true;
        let p = Pipeline::build(spec(&y), &registry, o).unwrap();
        let report = tokio::time::timeout(Duration::from_secs(60), p.run(Shutdown::new()))
            .await
            .expect("run timed out")
            .expect("run failed to start");
        assert_eq!(report.exit, ExitStatus::Ok, "{ctx}: {:#?}", report.status);
        // Big reads make few appends, so not every failure point is reached.
        if appends.load(Ordering::SeqCst) >= fail_at {
            assert!(
                component(&report.status, "chain:out").counters.restarts >= 1,
                "{ctx}: the chain was never rewound: {:#?}",
                report.status
            );
            rewound += 1;
        }
        let got = rows(&kit);
        assert_no_duplicates(&got);
        assert_eq!(got, want, "{ctx}");
    }
    assert!(rewound >= 8, "only {rewound} runs hit their failure point");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chain_panic_mid_window_rebuilds_the_open_windows() {
    let msgs = script(60);
    let want = baseline(&msgs).await;
    for read in READS {
        let dir = TempDataDir::new().unwrap();
        let kit = Kit::default();
        let o = reading(read);
        o.failpoints.panic_chain_at(WalPos::new(30), 1);
        let report = run(&kit, dir.path(), &msgs, o).await;
        assert_eq!(
            report.exit,
            ExitStatus::Ok,
            "read={read}: {:#?}",
            report.status
        );
        assert!(
            component(&report.status, "chain:out").counters.panics >= 1,
            "read={read}: {:#?}",
            report.status
        );
        assert_eq!(rows(&kit), want, "read={read}");
    }
}

#[test]
fn a_window_on_a_batch_sink_is_refused_at_build() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    feed(&kit, &[]);
    let y = config(dir.path()).replace(
        "  - { id: out, kind: recseg, config: { frames_per_file: 3 } }",
        "  - { id: out, kind: membatch }",
    );
    let errs = thadam_runtime::Pipeline::build(spec(&y), &kit.registry(), per_frame())
        .err()
        .unwrap_or_else(|| panic!("a window on a batch sink must not build"));
    let text: Vec<String> = errs.iter().map(|e| e.message.clone()).collect();
    assert!(
        text.iter().any(|m| m.contains("needs a segment sink")),
        "{text:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_close_emits_the_silent_asset_and_releases_the_cursor() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let y = config(dir.path()).replace("size: 10s,", "size: 10s, idle_close: 30s,");
    let run_idle = |msgs: &[Msg]| {
        feed(&kit, msgs);
        run_to_end(&kit, spec(&y), per_frame())
    };

    // Frames 1..=9 reach 24s: robot `b` (at 0s, frame 1) is not yet 30s idle,
    // so its open window still pins the cursor at 0.
    let before = run_idle(&PINNED[..9]).await;
    assert_eq!(before.exit, ExitStatus::Ok, "{:#?}", before.status);
    assert_eq!(cursor(&before.status, "out"), 0);
    assert_eq!(
        summary(&kit),
        [
            (6, "U64(4) F64(10.0)".to_owned()),
            (8, "U64(2) F64(11.0)".to_owned()),
        ]
    );

    // Frame 10 arrives at 33s (>= 0s + 30s): both `a`'s [20, 30) window and
    // `b`'s idle [0, 10) window close at frame 10, and the cursor advances to
    // 9 (one frame short of `a`'s new [30, 40) window opened at frame 10).
    let after = run_idle(&PINNED[9..]).await;
    assert_eq!(after.exit, ExitStatus::Ok, "{:#?}", after.status);
    assert_eq!(cursor(&after.status, "out"), 9);
    assert_eq!(
        summary(&kit),
        [
            (6, "U64(4) F64(10.0)".to_owned()),
            (8, "U64(2) F64(11.0)".to_owned()),
            (10, "U64(1) F64(100.0)".to_owned()),
            (10, "U64(2) F64(15.0)".to_owned()),
        ]
    );
    assert_no_duplicates(&rows(&kit));

    let again = run_idle(&[]).await;
    assert_eq!(again.exit, ExitStatus::Ok, "{:#?}", again.status);
    assert_eq!(summary(&kit).len(), 4);
    assert_eq!(cursor(&again.status, "out"), 9);
}

type SeenRow = (u64, String, thadam_core::QualityStatus, Option<String>);

#[derive(Clone, Default)]
struct SeenQualities(Arc<std::sync::Mutex<Vec<SeenRow>>>);

struct QualitySink(SeenQualities);

#[async_trait]
impl SegmentSink for QualitySink {
    async fn recover(&mut self, _ctx: &SinkContext) -> Result<Option<WalPos>, ThadamError> {
        Ok(None)
    }

    async fn append(&mut self, batch: SegmentBatch) -> Result<Vec<Reject>, ThadamError> {
        let mut guard = lock(&self.0.0);
        for (_, e) in batch.events() {
            let robot = e.asset().get("robot").unwrap_or("").to_owned();
            let q = e.quality();
            guard.push((
                e.wal_pos().get(),
                robot,
                q.status,
                q.reason.as_ref().map(|s| s.to_string()),
            ));
        }
        Ok(Vec::new())
    }

    async fn poll_commit(&mut self) -> Result<Option<WalPos>, ThadamError> {
        Ok(None)
    }

    async fn finalize_all(&mut self) -> Result<Option<WalPos>, ThadamError> {
        Ok(None)
    }

    fn cursor_committed(&mut self, _pos: WalPos) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn range_replay_and_wal_gaps_flag_uncertain_windows() {
    use thadam_core::QualityStatus;
    use thadam_runtime::{ReplayOutput, ReplayRequest, replay};

    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let msgs: [Msg; 6] = [
        ("a", 1, 1),
        ("b", 2, 2),
        ("a", 3, 3),
        ("a", 12, 4),
        ("b", 13, 5),
        ("a", 25, 6),
    ];
    let y = format!(
        r#"version: 1
pipeline: {{ name: test, node_id: node-1, data_dir: "{dir}" }}
sources:
  - {{ id: src, kind: memory }}
wal: {{ max_disk: 64MiB, segment_bytes: 1MiB, fsync: {{ interval: 1ms }}, on_full: block, retain_for: 1h }}
datasets:
  - id: d
    match: {{ source: src }}
    decode: json
    identity: {{ robot: {{ id: 100, template: "{{topic.1}}" }} }}
    fields:
      - {{ id: 1000, name: v, type: f64, from: /v }}
transforms:
  - {{ kind: window, dataset: d, sinks: [out], size: 10s, aggregates: [{{ field: v, fn: count, as: n }}] }}
sinks:
  - {{ id: out, kind: recseg, config: {{ frames_per_file: 1 }} }}
  - {{ id: raw, kind: recseg, config: {{ frames_per_file: 1 }} }}
"#,
        dir = dir.path().display()
    );

    feed(&kit, &msgs[..3]);
    let first = run_to_end(&kit, spec(&y), per_frame()).await;
    assert_eq!(first.exit, ExitStatus::Ok, "{:#?}", first.status);
    assert_eq!(cursor(&first.status, "raw"), 3);

    // Tear the last byte of segment 1 (frame 3) as in an un-fsynced power loss;
    // `out` had not flushed anything yet (`files` cleared). On the next open,
    // recovery truncates frame 3 and skips position 3 because `raw`'s cursor is
    // at 3, leaving a Missing gap at frame 3 before frames 4..=6.
    lock(&kit.rec.store("out").0).files.clear();
    let seg1 = dir.path().join("wal/segments/00000000000000000001.seg");
    let mut bytes = std::fs::read(&seg1).unwrap();
    let last_idx = bytes.len() - 1;
    bytes[last_idx] ^= 0xff;
    std::fs::write(&seg1, &bytes).unwrap();

    feed(&kit, &msgs[3..]);
    let second = run_to_end(&kit, spec(&y), per_frame()).await;
    assert_eq!(second.exit, ExitStatus::Ok, "{:#?}", second.status);
    assert!(component(&second.status, "chain:out").counters.gaps >= 1);

    // Replay from frame 1 sees the gap at frame 3: windows open across the gap
    // (a's [0, 10) closed at frame 4, b's [0, 10) closed at frame 5) carry
    // `window_gap`, while a's [10, 20) opened at frame 4 after the gap is Good.
    let gap_seen = SeenQualities::default();
    let rep_gap = replay(
        &spec(&y),
        &kit.registry(),
        per_frame(),
        ReplayRequest::new("out", WalPos::new(1), "from-one")
            .output(ReplayOutput::Sink(Box::new(QualitySink(gap_seen.clone())))),
    )
    .await
    .unwrap();
    assert_eq!(rep_gap.gaps, 1);
    assert_eq!(
        lock(&gap_seen.0).as_slice(),
        &[
            (
                4,
                "a".to_owned(),
                QualityStatus::Uncertain,
                Some("window_gap".to_owned())
            ),
            (
                5,
                "b".to_owned(),
                QualityStatus::Uncertain,
                Some("window_gap".to_owned())
            ),
            (6, "a".to_owned(), QualityStatus::Good, None),
        ]
    );

    // On a clean WAL, replaying from `from > 1` flags the first window of each
    // asset with `window_range_start` and leaves subsequent windows `Good`.
    let clean_dir = TempDataDir::new().unwrap();
    let clean_kit = Kit::default();
    let clean_msgs: [Msg; 6] = [
        ("a", 1, 1),
        ("a", 2, 2),
        ("b", 3, 3),
        ("a", 12, 4),
        ("b", 13, 5),
        ("a", 25, 6),
    ];
    let clean = run(&clean_kit, clean_dir.path(), &clean_msgs, per_frame()).await;
    assert_eq!(clean.exit, ExitStatus::Ok);

    let range_seen = SeenQualities::default();
    let rep_range = replay(
        &spec(&config(clean_dir.path())),
        &clean_kit.registry(),
        per_frame(),
        ReplayRequest::new("out", WalPos::new(2), "from-two").output(ReplayOutput::Sink(Box::new(
            QualitySink(range_seen.clone()),
        ))),
    )
    .await
    .unwrap();
    assert_eq!(rep_range.gaps, 0);
    assert_eq!(
        lock(&range_seen.0).as_slice(),
        &[
            (
                4,
                "a".to_owned(),
                QualityStatus::Uncertain,
                Some("window_range_start".to_owned())
            ),
            (
                5,
                "b".to_owned(),
                QualityStatus::Uncertain,
                Some("window_range_start".to_owned())
            ),
            (6, "a".to_owned(), QualityStatus::Good, None),
        ]
    );
}
