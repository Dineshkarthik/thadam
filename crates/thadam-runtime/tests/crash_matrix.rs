//! Crash between every step of segment finalize and cursor commit (D18):
//! after a restart every WAL range lands in exactly one file, none goes
//! missing, and only files at or below the committed cursor are eligible.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::*;
use thadam_runtime::{ExitStatus, Registry, failpoints};
use thadam_testkit::{Failpoint, TempDataDir};

#[derive(Clone, Copy, Debug)]
enum Point {
    Sink(Failpoint),
    Runtime(&'static str),
}

const POINTS: [Point; 5] = [
    Point::Sink(Failpoint::BeforeFinalize),
    Point::Sink(Failpoint::AfterRenameBeforeCatalog),
    Point::Sink(Failpoint::AfterCatalogBeforeReturn),
    Point::Runtime(failpoints::SEGMENT_BEFORE_CURSOR_COMMIT),
    Point::Runtime(failpoints::SEGMENT_BEFORE_CURSOR_COMMITTED),
];

fn registry(kit: &Kit, quarantine: bool) -> Registry {
    if quarantine {
        return kit.registry();
    }
    let mut r = Registry::new();
    r.source(Box::new(SlotSourceFactory(kit.sources.clone())))
        .segment_sink(Box::new(MemSegFactory(kit.memseg.clone())));
    r
}

async fn run(
    kit: &Kit,
    quarantine: bool,
    y: &str,
    o: thadam_runtime::RuntimeOptions,
) -> thadam_runtime::RunReport {
    let mut o = o;
    o.stop_when_sources_finish = true;
    o.batch_limits.max_messages = 1;
    let p = thadam_runtime::Pipeline::build(spec(y), &registry(kit, quarantine), o).unwrap();
    p.run(thadam_core::Shutdown::new()).await.unwrap()
}

async fn crash_then_restart(point: Point, quarantine: bool, prior_run: bool) {
    let ctx = format!("{point:?} quarantine={quarantine} prior_run={prior_run}");
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let store = kit.memseg.store("out");
    let y = yaml(
        dir.path(),
        "  - { id: out, kind: memseg, config: { frames_per_segment: 3 } }",
        "",
    );
    let total = 45u64;
    let mut next = 1;
    if prior_run {
        kit.sources.scripted("src", (1..=15).map(json_n));
        assert_eq!(
            run(&kit, quarantine, &y, fast_opts()).await.exit,
            ExitStatus::Ok,
            "{ctx}"
        );
        assert!(!store.files().is_empty(), "{ctx}");
        next = 16;
    }

    kit.sources.scripted("src", (next..=total).map(json_n));
    let o = fast_opts();
    match point {
        Point::Sink(fp) => store.arm(fp),
        Point::Runtime(name) => o.failpoints.arm(name, 1),
    }
    let first = run(&kit, quarantine, &y, o.clone()).await;
    assert_eq!(
        first.exit,
        ExitStatus::RuntimeFailure,
        "{ctx}: {:#?}",
        first.status
    );
    match point {
        Point::Sink(_) => assert!(!store.is_armed(), "{ctx}: failpoint never hit"),
        Point::Runtime(name) => assert!(!o.failpoints.is_armed(name), "{ctx}: failpoint never hit"),
    }

    kit.sources.scripted("src", std::iter::empty());
    let second = run(&kit, quarantine, &y, fast_opts()).await;
    assert_eq!(second.exit, ExitStatus::Ok, "{ctx}: {:#?}", second.status);

    let end = second.status.wal.as_ref().unwrap().durable_end;
    assert_eq!(end, total, "{ctx}");
    let files = store.files();
    let ranges: Vec<_> = files.iter().map(|f| f.range).collect();
    assert_contiguous(&ranges, end);
    assert_eq!(
        files.iter().map(|f| f.events).sum::<usize>(),
        total as usize,
        "{ctx}"
    );
    assert_eq!(store.catalog(), files, "{ctx}");
    let c = cursor(&second.status, "out");
    assert_eq!(c, end, "{ctx}");
    let mut eligible = store.eligible_ranges();
    eligible.sort_by_key(|r| r.first());
    assert_eq!(
        eligible, ranges,
        "{ctx}: every file at or below the cursor is eligible, once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_matrix_with_quarantine() {
    for point in POINTS {
        for prior_run in [false, true] {
            crash_then_restart(point, true, prior_run).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_matrix_without_quarantine() {
    for point in POINTS {
        for prior_run in [false, true] {
            crash_then_restart(point, false, prior_run).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eligibility_never_passes_the_committed_cursor() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let store = kit.memseg.store("out");
    let y = yaml(
        dir.path(),
        "  - { id: out, kind: memseg, config: { frames_per_segment: 2 } }",
        "",
    );
    kit.sources.scripted("src", (1..=20).map(json_n));
    let o = fast_opts();
    o.failpoints
        .arm(failpoints::SEGMENT_BEFORE_CURSOR_COMMIT, 1);
    let first = run(&kit, false, &y, o).await;
    assert_eq!(first.exit, ExitStatus::RuntimeFailure);
    let c = cursor(&first.status, "out");
    assert!(!store.files().is_empty());
    for r in store.eligible_ranges() {
        assert!(r.last().get() <= c, "{r} eligible beyond cursor {c}");
    }
    assert!(store.eligible_ranges().len() < store.files().len());
}
