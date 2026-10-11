//! The Parquet segment sink plus the object-store shipper end to end in
//! process: an idle source must not stop age-based rotation, cursor commits,
//! eligibility, and shipping (the edge-capture chaos-run findings).
//!
//! The shipper runs over `object_store::memory::InMemory` — no Docker — as a
//! `kind: memship` service whose `from` names the Parquet sink, exactly like
//! the demo's `kind: object_store` entry.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use common::*;
use futures_util::StreamExt as _;
use object_store::ObjectStore as _;
use thadam_core::{BuildContext, ThadamError, Service, ServiceFactory, Shutdown};
use thadam_runtime::{ExitStatus, Pipeline, PipelineHandle, Registry, RunReport};
use thadam_testkit::TempDataDir;

/// `kind: memship`: a real `Shipper` over a shared in-memory object store,
/// with an upload counter for synchronous `run_until` conditions.
struct MemShipFactory {
    store: Arc<object_store::memory::InMemory>,
    uploads: Arc<AtomicU64>,
}

struct CountedUploads(Arc<AtomicU64>);

impl thadam_object_store::ShipMetrics for CountedUploads {
    fn uploaded(&self, _bytes: u64, _latency: Duration, _outcome: &str) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn retry(&self, _segment: &thadam_segments::SegmentId, _attempt: u32, _retry_age: Duration) {}
    fn conflict_verified(&self, _same_content: bool) {}
    fn released(&self, _files: u64, _bytes: u64) {}
    fn backlog(&self, _files: usize) {}
}

impl ServiceFactory for MemShipFactory {
    fn kind(&self) -> &'static str {
        "memship"
    }
    fn config_schema(&self) -> serde_json::Value {
        serde_json::json!({"type": "object"})
    }
    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        thadam_object_store::parse_ship_config(cfg).map(drop)
    }
    fn build(
        &self,
        cfg: serde_json::Value,
        ctx: &BuildContext,
    ) -> Result<Box<dyn Service>, ThadamError> {
        let config = thadam_object_store::parse_ship_config(&cfg)
            .map_err(|errs| errs.into_iter().next().expect("nonempty errors"))?;
        let store: Arc<dyn object_store::ObjectStore> = self.store.clone();
        Ok(Box::new(
            thadam_object_store::Shipper::new(ctx.component_id.as_ref(), config, store)
                .with_metrics(Arc::new(CountedUploads(self.uploads.clone()))),
        ))
    }
}

struct ShipKit {
    store: Arc<object_store::memory::InMemory>,
    uploads: Arc<AtomicU64>,
}

impl ShipKit {
    fn new() -> Self {
        ShipKit {
            store: Arc::new(object_store::memory::InMemory::new()),
            uploads: Arc::new(AtomicU64::new(0)),
        }
    }

    fn registry(&self, kit: &Kit) -> Registry {
        let mut r = Registry::new();
        r.source(Box::new(SlotSourceFactory(kit.sources.clone())))
            .segment_sink(Box::new(thadam_segments::ParquetSegmentSinkFactory::new()))
            .quarantine_sink(Box::new(thadam_segments::QuarantineSinkFactory::new()))
            .service(Box::new(MemShipFactory {
                store: self.store.clone(),
                uploads: self.uploads.clone(),
            }));
        r
    }

    fn upload_count(&self) -> u64 {
        self.uploads.load(Ordering::Relaxed)
    }
}

/// One `kind: parquet` sink `pq` with 1s age rotation and one `kind: memship`
/// service `ship` consuming its catalog.
const SINKS: &str = "  - id: pq\n    kind: parquet\n    config:\n      datasets: [d]\n      compression: none\n      rotation: { max_bytes: null, max_rows: null, max_age: 1 }\n  - id: ship\n    kind: memship\n    config:\n      from: pq\n      pipeline: test\n      url: s3://bucket/pfx\n      access_key_id: AK\n      secret_access_key: SK\n      conditional_put: auto\n      retry: { initial: 50ms, max: 200ms }\n      local_retention: { mode: keep_for, keep_for: 1h }";

fn init_logs() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
}

/// Polls `done` against the pipeline handle until it holds (30s deadline).
async fn wait_until(handle: &PipelineHandle, mut done: impl FnMut(&PipelineHandle) -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if done(handle) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not reached: {:#?}",
            handle.status()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn finish(pipeline: tokio::task::JoinHandle<Result<RunReport, ThadamError>>) -> RunReport {
    tokio::time::timeout(Duration::from_secs(60), pipeline)
        .await
        .expect("shutdown timed out")
        .expect("run task panicked")
        .expect("run failed to start")
}

fn parquet_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(parquet_files(&path));
            } else if path.extension().is_some_and(|e| e == "parquet") {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// A burst of rows, then nothing: age rotation must finalize the open
/// segment, the cursor must commit past it, and the shipper must upload it
/// while the pipeline is still running — not at shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_source_segments_finalize_commit_and_ship() {
    init_logs();
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let upstream = kit.sources.memory("src", Default::default());
    let ship = ShipKit::new();
    let y = yaml(dir.path(), SINKS, "");
    let p = Pipeline::build(spec(&y), &ship.registry(&kit), fast_opts())
        .unwrap_or_else(|e| panic!("build: {e:?}"));
    let handle = p.handle();
    let shutdown = Shutdown::new();
    let run = tokio::spawn(p.run(shutdown.clone()));

    // One burst, then the source goes idle (stays open, nothing published).
    for n in 1..=3 {
        upstream.publish(json_n(n));
    }

    // Before any shutdown: the segment age-rotates, the cursor commits, and
    // the shipper uploads it.
    wait_until(&handle, |_| ship.upload_count() >= 1).await;
    assert!(
        cursor(&handle.status(), "pq") >= 1,
        "cursor committed without traffic: {:#?}",
        handle.status().cursors
    );

    shutdown.trigger();
    let report: RunReport = finish(run).await;
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);

    // Every object ships under the url prefix from `s3://bucket/pfx`
    // (ADR-0013): `{prefix}/thadam/v1/{pipeline}/...`.
    let mut stream = ship
        .store
        .list(Some(&object_store::path::Path::from("pfx")));
    let mut keys = Vec::new();
    while let Some(meta) = stream.next().await {
        keys.push(meta.expect("list").location.to_string());
    }
    assert!(!keys.is_empty(), "expected uploaded objects, got none");
    for k in &keys {
        assert!(
            k.starts_with("pfx/thadam/v1/test/"),
            "key loses the url prefix: {k}"
        );
    }
}

/// The quarantine instance's data directory for sink `pq`.
fn quarantine_dir(root: &Path) -> PathBuf {
    root.join("quarantine/pq")
}

/// Size of the quarantine staging file (a bare header, 8 bytes, when empty).
fn staging_len(root: &Path) -> u64 {
    std::fs::metadata(quarantine_dir(root).join("rejects.staging")).map_or(0, |m| m.len())
}

/// Total rows across the quarantine Parquet segments, from their footers.
fn quarantine_rows(root: &Path) -> i64 {
    parquet_files(&quarantine_dir(root).join("ready"))
        .iter()
        .map(|path| {
            let file = std::fs::File::open(path).unwrap();
            let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
                .parse_and_finish(&file)
                .unwrap();
            (0..metadata.num_row_groups())
                .map(|i| metadata.row_group(i).num_rows())
                .sum::<i64>()
        })
        .sum()
}

const BAD: Bytes = Bytes::from_static(b"this is not json");

/// The `pq` sink alone (no shipper), 1s age rotation, default quarantine.
const PQ_ONLY: &str = "  - id: pq\n    kind: parquet\n    config:\n      datasets: [d]\n      compression: none\n      rotation: { max_bytes: null, max_rows: null, max_age: 1 }";

/// A reject releases its sink's cursor as soon as it is fsynced into the
/// quarantine staging file. The quarantine segment keeps its own (default
/// 300 second) rotation, so it is still open while the cursor passes the
/// reject and the wave after it uploads; shutdown then builds the quarantine
/// segment from staging, once, and empties staging.
///
/// This replaces the old "quarantine floor" behaviour, where one pending
/// reject pinned the cursor, and with it every upload, until the quarantine
/// segment rotated (the edge-capture chaos findings A and B).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pending_reject_does_not_hold_the_cursor_or_shipping() {
    init_logs();
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let upstream = kit.sources.memory("src", Default::default());
    let ship = ShipKit::new();
    let y = yaml(dir.path(), SINKS, "");
    let p = Pipeline::build(spec(&y), &ship.registry(&kit), fast_opts())
        .unwrap_or_else(|e| panic!("build: {e:?}"));
    let handle = p.handle();
    let shutdown = Shutdown::new();
    let run = tokio::spawn(p.run(shutdown.clone()));

    for n in 1..=3 {
        upstream.publish(json_n(n));
    }
    wait_until(&handle, |h| cursor(&h.status(), "pq") >= 1).await;
    wait_until(&handle, |_| ship.upload_count() >= 1).await;
    let before = cursor(&handle.status(), "pq");

    // Wave 2: an undecodable payload (a quarantined reject) plus good rows.
    upstream.publish(BAD);
    for n in 4..=6 {
        upstream.publish(json_n(n));
    }

    // The cursor passes the reject and the wave-2 file uploads, while the
    // quarantine segment is still open: nothing is in quarantine's `ready/`,
    // the reject sits in staging.
    wait_until(&handle, |_| ship.upload_count() >= 2).await;
    let status = handle.status();
    assert!(
        cursor(&status, "pq") > before,
        "the cursor moved past the reject: {:#?}",
        status.cursors
    );
    assert!(
        parquet_files(&quarantine_dir(dir.path()).join("ready")).is_empty(),
        "the quarantine segment is still open"
    );
    assert!(
        staging_len(dir.path()) > 8,
        "the reject is durable in staging"
    );

    shutdown.trigger();
    let report = finish(run).await;
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    assert_eq!(quarantine_rows(dir.path()), 1, "exactly one reject");
    assert_eq!(staging_len(dir.path()), 8, "staging shrinks once durable");
}

/// The quarantine segment rotates on its own cadence (`quarantine.rotation`),
/// independent of the sink cursor: it appears after its `max_age`, staging
/// shrinks to a bare header, and a later reject (after good rows, so the two
/// rejects' WAL ranges have a gap between them) becomes a second quarantine
/// segment rather than stalling the quarantine watermark.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quarantine_segments_rotate_on_their_own_cadence() {
    init_logs();
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let upstream = kit.sources.memory("src", Default::default());
    let ship = ShipKit::new();
    let y = yaml(
        dir.path(),
        SINKS,
        "quarantine: { format: parquet, max_disk: 16MiB, rotation: { max_age: 2 } }",
    );
    let p = Pipeline::build(spec(&y), &ship.registry(&kit), fast_opts())
        .unwrap_or_else(|e| panic!("build: {e:?}"));
    let handle = p.handle();
    let shutdown = Shutdown::new();
    let run = tokio::spawn(p.run(shutdown.clone()));
    let root = dir.path().to_path_buf();

    for n in 1..=3 {
        upstream.publish(json_n(n));
    }
    wait_until(&handle, |_| ship.upload_count() >= 1).await;
    upstream.publish(BAD);
    for n in 4..=6 {
        upstream.publish(json_n(n));
    }
    // Uploads do not wait for the quarantine segment...
    wait_until(&handle, |_| ship.upload_count() >= 2).await;
    // ...which then rotates by itself and frees its staged rows.
    let r = root.clone();
    wait_until(&handle, move |_| {
        quarantine_rows(&r) == 1 && staging_len(&r) == 8
    })
    .await;

    // A second reject, after the first segment became durable.
    for n in 7..=9 {
        upstream.publish(json_n(n));
    }
    wait_until(&handle, |_| ship.upload_count() >= 3).await;
    upstream.publish(BAD);
    let r = root.clone();
    wait_until(&handle, move |_| {
        quarantine_rows(&r) == 2 && staging_len(&r) == 8
    })
    .await;

    shutdown.trigger();
    let report = finish(run).await;
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
    assert_eq!(quarantine_rows(&root), 2);
}

/// Crash after the sink cursor passed a reject but before the quarantine
/// segment rotated: the reject exists only in staging. The restart recovers
/// it from staging into the next quarantine segment, exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_before_quarantine_rotation_recovers_the_reject_exactly_once() {
    init_logs();
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let upstream = kit.sources.memory("src", Default::default());
    let ship = ShipKit::new();
    let y = yaml(dir.path(), SINKS, "");
    let opts = fast_opts();
    let failpoints = opts.failpoints.clone();
    let p = Pipeline::build(spec(&y), &ship.registry(&kit), opts)
        .unwrap_or_else(|e| panic!("build: {e:?}"));
    let handle = p.handle();
    let shutdown = Shutdown::new();
    let run = tokio::spawn(p.run(shutdown.clone()));

    for n in 1..=3 {
        upstream.publish(json_n(n));
    }
    wait_until(&handle, |_| ship.upload_count() >= 1).await;
    let before = cursor(&handle.status(), "pq");
    upstream.publish(BAD);
    for n in 4..=6 {
        upstream.publish(json_n(n));
    }
    wait_until(&handle, |_| ship.upload_count() >= 2).await;
    assert!(cursor(&handle.status(), "pq") > before);
    assert!(parquet_files(&quarantine_dir(dir.path()).join("ready")).is_empty());

    // The next cursor commit "crashes" the sink driver, so its quarantine
    // segment is never finalized.
    failpoints.arm(thadam_runtime::failpoints::SEGMENT_BEFORE_CURSOR_COMMIT, 1);
    upstream.publish(json_n(7));
    wait_until(&handle, |_| {
        !failpoints.is_armed(thadam_runtime::failpoints::SEGMENT_BEFORE_CURSOR_COMMIT)
    })
    .await;
    shutdown.trigger();
    let first = finish(run).await;
    assert_eq!(
        first.exit,
        ExitStatus::RuntimeFailure,
        "{:#?}",
        first.status
    );
    assert_eq!(quarantine_rows(dir.path()), 0, "nothing was durable");
    assert!(staging_len(dir.path()) > 8, "the reject is only in staging");

    // Restart on the same data directory.
    let upstream = kit.sources.memory("src", Default::default());
    let p = Pipeline::build(spec(&y), &ship.registry(&kit), fast_opts())
        .unwrap_or_else(|e| panic!("build: {e:?}"));
    let handle = p.handle();
    let shutdown = Shutdown::new();
    let run = tokio::spawn(p.run(shutdown.clone()));
    upstream.publish(json_n(8));
    wait_until(&handle, |h| cursor(&h.status(), "pq") >= 1).await;
    shutdown.trigger();
    let second = finish(run).await;
    assert_eq!(second.exit, ExitStatus::Ok, "{:#?}", second.status);
    assert_eq!(quarantine_rows(dir.path()), 1, "the reject, exactly once");
    assert_eq!(staging_len(dir.path()), 8);
}

/// Crash between the staging fsync and the quarantine segment append (the
/// `quarantine::after_stage` failpoint). The owner's cursor never passed the
/// reject, so the restart re-reads it from the WAL; staging already holds it,
/// and the position lineage keeps the re-read from staging it twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_between_staging_and_the_segment_does_not_duplicate_the_reject() {
    init_logs();
    let dir = TempDataDir::new().unwrap();
    let kit = Kit::default();
    let ship = ShipKit::new();
    let y = yaml(dir.path(), PQ_ONLY, "");
    kit.sources
        .scripted("src", [json_n(1), BAD, json_n(2), json_n(3)]);
    let mut opts = fast_opts();
    opts.stop_when_sources_finish = true;
    let failpoints = opts.failpoints.clone();
    failpoints.arm(thadam_runtime::failpoints::QUARANTINE_AFTER_STAGE, 1);
    let p = Pipeline::build(spec(&y), &ship.registry(&kit), opts)
        .unwrap_or_else(|e| panic!("build: {e:?}"));
    let first = tokio::time::timeout(Duration::from_secs(60), p.run(Shutdown::new()))
        .await
        .expect("run timed out")
        .expect("run failed to start");
    assert_eq!(
        first.exit,
        ExitStatus::RuntimeFailure,
        "{:#?}",
        first.status
    );
    assert!(!failpoints.is_armed(thadam_runtime::failpoints::QUARANTINE_AFTER_STAGE));
    assert_eq!(quarantine_rows(dir.path()), 0);
    assert!(staging_len(dir.path()) > 8);

    kit.sources.scripted("src", std::iter::empty());
    let mut opts = fast_opts();
    opts.stop_when_sources_finish = true;
    let p = Pipeline::build(spec(&y), &ship.registry(&kit), opts)
        .unwrap_or_else(|e| panic!("build: {e:?}"));
    let second = tokio::time::timeout(Duration::from_secs(60), p.run(Shutdown::new()))
        .await
        .expect("run timed out")
        .expect("run failed to start");
    assert_eq!(second.exit, ExitStatus::Ok, "{:#?}", second.status);
    assert_eq!(quarantine_rows(dir.path()), 1, "the reject, exactly once");
    assert_eq!(staging_len(dir.path()), 8);
    assert!(cursor(&second.status, "pq") >= 1);
}
