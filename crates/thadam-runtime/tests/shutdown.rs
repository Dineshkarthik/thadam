//! Shutdown order, readiness, and exit codes.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::time::Duration;

use async_trait::async_trait;
use common::*;
use thadam_core::{
    BatchLimits, ErrorDomain, ThadamError, Shutdown, Source, SourceBatch, SourceContext, codes,
};
use thadam_runtime::{ComponentState, ExitStatus, Phase, Pipeline};
use thadam_testkit::{MemorySource, MemorySourceConfig, TempDataDir};

/// Logs `source.close` so the order against sinks and services is visible.
struct LoggingSource {
    inner: MemorySource,
    log: EventLog,
}

#[async_trait]
impl Source for LoggingSource {
    async fn open(&mut self, ctx: &SourceContext) -> Result<(), ThadamError> {
        self.inner.open(ctx).await
    }
    async fn next_batch(&mut self, limits: BatchLimits) -> Result<Option<SourceBatch>, ThadamError> {
        self.inner.next_batch(limits).await
    }
    async fn close(&mut self) -> Result<(), ThadamError> {
        self.log.push("source.close");
        self.inner.close().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_runs_sources_then_wal_then_sinks_then_services() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let (inner, up) = MemorySource::new(MemorySourceConfig::default());
    kit.sources.put(
        "src",
        LoggingSource {
            inner,
            log: kit.log.clone(),
        },
    );
    lock(&kit.rec.store("out").0).log = Some(kit.log.clone());
    let sinks = "  - { id: out, kind: recseg, config: { frames_per_file: 1000 } }\n  - { id: ship, kind: recservice }";
    let p = Pipeline::build(
        spec(&yaml(dir.path(), sinks, "")),
        &kit.registry(),
        fast_opts(),
    )
    .unwrap();
    let handle = p.handle();
    let mut phases = handle.subscribe();
    let shutdown = Shutdown::new();
    let task = tokio::spawn(p.run(shutdown.clone()));
    wait_ready(&handle).await;
    for i in 1..=100 {
        up.publish(json_n(i));
    }
    while up.acked_seqs().len() < 100 {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    // Messages published after the signal are drained into the WAL, not lost.
    for i in 101..=120 {
        up.publish(json_n(i));
    }
    shutdown.trigger();
    let report = task.await.unwrap().unwrap();

    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    assert_eq!(up.acked_seqs().len(), 120);
    assert_eq!(kit.rec.store("out").ns(), (1..=120).collect::<Vec<_>>());
    let log = kit.log.entries();
    let close = kit.log.index_of("source.close").unwrap();
    let finalize = log.iter().rposition(|e| e == "sink.finalize").unwrap();
    let exit = kit.log.index_of("service.exit").unwrap();
    assert!(close < finalize && finalize < exit, "{log:?}");
    assert!(
        kit.log.index_of("service.start").unwrap() < close,
        "{log:?}"
    );
    assert!(dir.join("wal/clean_shutdown").exists());
    assert_eq!(handle.phase(), Phase::Stopped);
    assert!(!handle.is_ready());
    let _ = phases.borrow_and_update();
    for c in &report.status.components {
        assert_eq!(c.state, ComponentState::Stopped, "{c:#?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fatal_sink_error_fails_only_that_sink_and_exits_one() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted("src", (1..=60).map(json_n));
    lock(&kit.rec.store("bad").0).append_errors = vec![ThadamError::fatal(
        ErrorDomain::Auth,
        codes::AUTH_FAILED,
        "credentials rejected",
    )];
    let sinks = "  - { id: bad, kind: recseg }\n  - { id: good, kind: recseg }";
    let report = run_to_end(&kit, spec(&yaml(dir.path(), sinks, "")), fast_opts()).await;

    assert_eq!(report.exit, ExitStatus::RuntimeFailure);
    match &component(&report.status, "sink:bad").state {
        ComponentState::Failed { error } => assert_eq!(error.code, codes::AUTH_FAILED),
        other => panic!("sink:bad is {other:?}"),
    }
    assert_eq!(kit.rec.store("good").ns().len(), 60);
    assert_eq!(
        cursor(&report.status, "bad"),
        0,
        "the failed sink's data stays in the WAL"
    );
    assert!(report.status.wal.as_ref().unwrap().first_seq <= 1);

    // The data is still there: a restart with a healthy sink delivers it.
    kit.sources.scripted("src", std::iter::empty());
    let again = run_to_end(&kit, spec(&yaml(dir.path(), sinks, "")), fast_opts()).await;
    assert_eq!(again.exit, ExitStatus::Ok, "{:#?}", again.status);
    assert_eq!(kit.rec.store("bad").ns(), (1..=60).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stuck_sink_is_aborted_at_the_deadline_with_exit_130() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.scripted("src", (1..=20).map(json_n));
    lock(&kit.rec.store("out").0).stall = true;
    let mut o = fast_opts();
    o.drain_timeout = Duration::from_millis(100);
    o.shutdown_timeout = Duration::from_millis(500);
    let report = run_to_end(
        &kit,
        spec(&yaml(dir.path(), "  - { id: out, kind: recseg }", "")),
        o,
    )
    .await;
    assert_eq!(report.exit, ExitStatus::Interrupted);
    assert_eq!(report.exit.code(), 130);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn force_shutdown_aborts_at_once() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.memory("src", MemorySourceConfig::default());
    lock(&kit.rec.store("out").0).stall = true;
    let p = Pipeline::build(
        spec(&yaml(dir.path(), "  - { id: out, kind: recseg }", "")),
        &kit.registry(),
        fast_opts(),
    )
    .unwrap();
    let handle = p.handle();
    let task = tokio::spawn(p.run(Shutdown::new()));
    handle.wait_for(Phase::Running).await;
    handle.force_shutdown();
    let report = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(report.exit, ExitStatus::Interrupted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_locked_or_foreign_data_directory_exits_three() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    kit.sources.memory("src", MemorySourceConfig::default());
    let y = yaml(dir.path(), "  - { id: out, kind: recseg }", "");
    let p = Pipeline::build(spec(&y), &kit.registry(), fast_opts()).unwrap();
    let handle = p.handle();
    let shutdown = Shutdown::new();
    let task = tokio::spawn(p.run(shutdown.clone()));
    handle.wait_for(Phase::Running).await;

    kit.sources.memory("src", MemorySourceConfig::default());
    let second = Pipeline::build(spec(&y), &kit.registry(), fast_opts()).unwrap();
    let err = second.run(Shutdown::new()).await.unwrap_err();
    assert_eq!(ExitStatus::for_error(&err), ExitStatus::DataDir, "{err}");
    assert!(err.message.contains("locked"), "{err}");

    shutdown.trigger();
    assert_eq!(task.await.unwrap().unwrap().exit, ExitStatus::Ok);

    let renamed = y.replace("name: test", "name: other");
    kit.sources.scripted("src", std::iter::empty());
    let foreign = Pipeline::build(spec(&renamed), &kit.registry(), fast_opts()).unwrap();
    let err = foreign.run(Shutdown::new()).await.unwrap_err();
    assert_eq!(ExitStatus::for_error(&err), ExitStatus::DataDir);
    assert!(err.message.contains("belongs to pipeline `test`"), "{err}");

    kit.sources.scripted("src", std::iter::empty());
    let mut o = fast_opts();
    o.repair = true;
    let repaired = run_to_end(&kit, spec(&renamed), o).await;
    assert_eq!(repaired.exit, ExitStatus::Ok);
    assert_eq!(repaired.repairs.len(), 1);
    assert_eq!(
        thadam_runtime::read_thadam_json(dir.path())
            .unwrap()
            .unwrap()
            .pipeline,
        "other"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replaced_wal_is_refused_against_existing_sink_state() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let y = yaml(dir.path(), "  - { id: out, kind: recseg }", "");
    kit.sources.scripted("src", (1..=5).map(json_n));
    assert_eq!(
        run_to_end(&kit, spec(&y), fast_opts()).await.exit,
        ExitStatus::Ok
    );
    std::fs::remove_dir_all(dir.join("wal")).unwrap();

    kit.sources.scripted("src", std::iter::empty());
    let p = Pipeline::build(spec(&y), &kit.registry(), fast_opts()).unwrap();
    let err = p.run(Shutdown::new()).await.unwrap_err();
    assert_eq!(ExitStatus::for_error(&err), ExitStatus::DataDir);
    assert!(err.message.contains("WAL epoch"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_component_clears_readiness() {
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let up = kit.sources.memory("src", MemorySourceConfig::default());
    lock(&kit.rec.store("out").0).append_errors = vec![ThadamError::fatal(
        ErrorDomain::Sink,
        codes::SINK_IO,
        "disk gone",
    )];
    let p = Pipeline::build(
        spec(&yaml(dir.path(), "  - { id: out, kind: recseg }", "")),
        &kit.registry(),
        fast_opts(),
    )
    .unwrap();
    let handle = p.handle();
    let shutdown = Shutdown::new();
    let task = tokio::spawn(p.run(shutdown.clone()));
    wait_ready(&handle).await;
    up.publish(json_n(1));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while handle.is_ready() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(handle.status().phase, Phase::Running);
    shutdown.trigger();
    assert_eq!(
        task.await.unwrap().unwrap().exit,
        ExitStatus::RuntimeFailure
    );
}
