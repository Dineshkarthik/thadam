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

/// The quarantine floor: while rejects wait for the quarantine segment to
/// become durable, the owner sink's cursor may not pass them. Files keep
/// finalizing on the age timer, the catalog watermark advances, but the
/// commit cursor — and with it ship eligibility — stops at the floor.
///
/// This encodes the edge-capture chaos observations: the simulator injects
/// out-of-range telemetry at `out_of_range_rate` (validate: status bad →
/// quarantine), so every demo run has rejects almost immediately; the
/// quarantine sink's rotation is the fixed default (`max_age` 300 seconds),
/// longer than any scenario, so nothing after the first reject is ever marked
/// eligible within the run — no uploads, no shipper log lines — and the
/// SIGTERM finalize is the first time the cursor catches up.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_rejects_cap_commits_and_shipping_at_the_quarantine_floor() {
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

    // Wave 1: three rows land, age-rotate, commit, ship. (The idle path.)
    for n in 1..=3 {
        upstream.publish(json_n(n));
    }
    wait_until(&handle, |h| cursor(&h.status(), "pq") >= 1).await;
    wait_until(&handle, |_| ship.upload_count() >= 1).await;

    // Wave 2: one undecodable payload (a quarantined reject) plus good rows.
    // The reject's WAL range opens a new segment; the quarantine floor is
    // first(pending) - 1 = the current cursor, so commits must stop here.
    upstream.publish(Bytes::from_static(b"this is not json"));
    for n in 4..=6 {
        upstream.publish(json_n(n));
    }

    // Several rotation waves pass: wave-2's segment finalizes (rotation keeps
    // working while the cursor is capped), but the cursor and the upload
    // count must not move: file [floor+1..] is not eligible.
    let ready_dir = dir.path().join("sinks/pq/ready");
    wait_until(&handle, move |_| parquet_files(&ready_dir).len() >= 2).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let status = handle.status();
    assert_eq!(
        cursor(&status, "pq"),
        1,
        "the quarantine floor caps the commit cursor: {:#?}",
        status.cursors
    );
    assert_eq!(
        ship.upload_count(),
        1,
        "nothing past the floor is ship-eligible"
    );

    shutdown.trigger();
    let report = finish(run).await;
    // The forced finalize settles the pending reject and commits everything:
    // exactly what the chaos helpers' "verify after SIGTERM is authoritative"
    // comment records.
    assert!(
        cursor(&report.status, "pq") >= 2,
        "shutdown commits past the settled floor: {:#?}",
        report.status.cursors
    );
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
}

/// The regression: with a short quarantine `max_age` (configurable via the
/// pipeline spec), a quarantined reject pauses commits only until the
/// quarantine segment rotates — seconds, not the 300-second default — and the
/// shipper uploads the wave after the reject while the pipeline runs.
///
/// Fails before the fix: `quarantine.rotation` was not expressible, so the
/// quarantine sink always took the default 300 seconds and the scenario
/// window closed first (findings A and B).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn short_quarantine_rotation_resumes_commits_and_shipping_after_a_reject() {
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

    // Wave 1 ships, then a reject lands: the floor caps the cursor at 1.
    for n in 1..=3 {
        upstream.publish(json_n(n));
    }
    wait_until(&handle, |_| ship.upload_count() >= 1).await;
    upstream.publish(Bytes::from_static(b"this is not json"));
    for n in 4..=6 {
        upstream.publish(json_n(n));
    }
    wait_until(&handle, |h| {
        cursor(&h.status(), "pq") == 1
            && parquet_files(&dir.path().join("sinks/pq/ready")).len() >= 2
    })
    .await;

    // The quarantine segment rotates two seconds after the reject: the floor
    // clears, the cursor commits past it, and the wave-2 file ships — no
    // shutdown involved.
    wait_until(&handle, |_| ship.upload_count() >= 2).await;
    assert!(
        cursor(&handle.status(), "pq") >= 2,
        "commits resumed after the quarantine segment rotated: {:#?}",
        handle.status().cursors
    );

    shutdown.trigger();
    let report = finish(run).await;
    assert_eq!(report.exit, ExitStatus::Ok, "{:#?}", report.status);
}
