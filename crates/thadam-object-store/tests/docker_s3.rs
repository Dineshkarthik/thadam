//! Docker integration tests against SeaweedFS (S3-compatible).
//!
//! These are `#[ignore]`d so `cargo nextest run` passes without Docker; run
//! them with
//!
//! ```text
//! cargo nextest run -p thadam-object-store --run-ignored only -E 'binary(docker_s3)'
//! ```
//!
//! Images: public Docker Hub only, pinned by tag (never a private mirror);
//! re-pin with `THADAM_TEST_S3_IMAGE` when updating. SeaweedFS provides an
//! S3-compatible gateway via `weed server -s3 -filer`. The bucket is created
//! with a Signature V4 HTTP call. `curl` and `docker` CLIs must be available.
//!
//! MinIO note: MinIO stopped publishing community images to Docker Hub in
//! late 2025. SeaweedFS is the drop-in S3-compatible replacement.
//!
//! Capability note (verified against `chrislusf/seaweedfs:3.81`): the S3
//! gateway ignores `If-None-Match: *` on PutObject — a second create for an
//! existing key returns `200 OK` and silently overwrites (real S3 answers
//! `412 Precondition Failed`). The shipper's startup probe therefore reports
//! `conditional_create = false` and `conditional_put: auto` falls back to
//! overwrite, which is only safe because finalized files are immutable and
//! keys deterministic. The collision test below asserts the probed behavior
//! per store capability; the always-fatal guarantee for stores that DO
//! enforce conditional writes is covered by the in-memory crate tests
//! (`conflict_with_different_content_is_a_fatal_collision`).
//!
//! Outage note: `docker stop` + `docker start` re-allocates ephemeral host
//! ports (`-p 127.0.0.1::9000` landed on a different host port after every
//! restart in testing). The S3 port is therefore pinned explicitly; the
//! shipper's S3 client keeps the endpoint baked in, and a remapped port
//! would leave it retrying a dead endpoint forever (the outage test's
//! original 180 s "ready" timeout).
//!
//! Containers are labelled `thadam-object-store-test` and removed when a test
//! ends, including on panic. Logs: run with `RUST_LOG=debug` (the harness
//! installs a tracing subscriber).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use object_store::{ObjectStore, ObjectStoreExt};
use thadam_core::{
    BudgetCategory, BuildContext, DiskBudget, ServiceContext, ServiceFactory, SharedObjects,
    Shutdown, SystemClock, WalEpoch, WalPos, WalRange, codes,
};
use thadam_segments::{SegmentCatalog, SegmentId, SegmentRecord};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use thadam_object_store::{ObjectStoreShipperFactory, ShipMetrics};

/// The pinned SeaweedFS image (public Docker Hub only, S3-compatible).
/// MinIO stopped publishing to Docker Hub in late 2025; SeaweedFS provides
/// an S3-compatible gateway via `weed server -s3`. Re-pin with
/// `THADAM_TEST_S3_IMAGE` when updating.
const S3_IMAGE: &str = "chrislusf/seaweedfs:3.81";
const LABEL: &str = "thadam-object-store-test";
const ROOT_USER: &str = "thadam-test";
const ROOT_SECRET: &str = "thadam-test-secret-16+bytes";
const TIMEOUT: Duration = Duration::from_secs(180);

fn s3_image() -> String {
    std::env::var("THADAM_TEST_S3_IMAGE").unwrap_or_else(|_| S3_IMAGE.to_owned())
}

fn docker(args: &[&str]) -> Output {
    Command::new("docker")
        .args(args)
        .output()
        .expect("docker cli")
}

fn err(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// A free loopback port to pin the container's S3 mapping onto.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    listener.local_addr().expect("local addr").port()
}

/// A running SeaweedFS container; removed on Drop. The S3 port is pinned on
/// the host so a `stop`/`start` cycle (the simulated outage) keeps the
/// endpoint valid — Docker re-allocates ephemeral ports on restart, and the
/// shipper's S3 client bakes the endpoint in at construction.
struct Server {
    name: String,
    endpoint: String,
    bucket: String,
}

impl Server {
    async fn start(bucket: &str) -> Server {
        let bucket = bucket.to_owned();
        tokio::task::spawn_blocking(move || Self::start_blocking(&bucket))
            .await
            .expect("docker task")
    }

    fn start_blocking(bucket: &str) -> Server {
        let mut last_err = String::new();
        // Retry the run a few times: another test process can claim the port
        // between `free_port` and `docker run`.
        for _ in 0..8 {
            let port = free_port();
            let name = format!(
                "thadam-objstore-{}-{:x}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            );
            // SeaweedFS: weed server -s3 -s3.port=9000 -filer (small footprint)
            let out = docker(&[
                "run",
                "-d",
                "--name",
                &name,
                "--label",
                LABEL,
                "-p",
                &format!("127.0.0.1:{port}:9000"),
                &s3_image(),
                "server",
                "-s3",
                "-s3.port=9000",
                "-filer",
                // Keep the container small: no 1GiB-per-volume preallocation
                // (the default, x7 volumes, exhausts the Docker Desktop disk)
                // and a small volume size limit; the tests write <= 17MiB.
                "-master.volumePreallocate=false",
                "-master.volumeSizeLimitMB=64",
                "-volume.max=2",
            ]);
            if !out.status.success() {
                last_err = err(&out);
                let _ = docker(&["rm", "-f", "-v", &name]);
                if last_err.contains("port is already allocated")
                    || last_err.contains("address already in use")
                {
                    continue;
                }
                panic!("docker run: {last_err}");
            }
            let server = Server {
                name,
                endpoint: format!("http://127.0.0.1:{port}"),
                bucket: bucket.to_owned(),
            };
            server.wait_ready();
            create_bucket(&server.endpoint, bucket, ROOT_USER, ROOT_SECRET);
            return server;
        }
        panic!("docker run after port retries: {last_err}");
    }

    fn wait_ready(&self) {
        let started = Instant::now();
        loop {
            // SeaweedFS S3 gateway: GET / answers 200 once the S3 API is up
            // (it starts several seconds after the container: master
            // election, then filer connect).
            let ok = Command::new("curl")
                .args(["-sf", "--max-time", "5", &format!("{}/", self.endpoint)])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            if ok {
                return;
            }
            assert!(
                started.elapsed() < TIMEOUT,
                "seaweedfs did not become ready"
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    fn stop_container(&self) {
        let out = docker(&["stop", &self.name]);
        assert!(out.status.success(), "docker stop: {}", err(&out));
    }

    fn start_container(&self) {
        let out = docker(&["start", &self.name]);
        assert!(out.status.success(), "docker start: {}", err(&out));
        self.wait_ready();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // -v: the image declares VOLUME /data; drop the anonymous volume too
        let _ = docker(&["rm", "-f", "-v", &self.name]);
    }
}

/// An independent S3 client toward the test SeaweedFS.
fn s3_for_server(server: &Server) -> object_store::aws::AmazonS3 {
    object_store::aws::AmazonS3Builder::new()
        .with_url(format!("s3://{}", server.bucket))
        .with_endpoint(&server.endpoint)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_access_key_id(ROOT_USER)
        .with_secret_access_key(ROOT_SECRET)
        // SeaweedFS requires path-style access (not virtual-hosted)
        .with_virtual_hosted_style_request(false)
        .build()
        .expect("builder")
}

/// Mirrors the shipper's startup probe: does the store reject a second
/// create for an existing key (enforce `If-None-Match: *`)?
async fn store_supports_conditional_create(store: &object_store::aws::AmazonS3) -> bool {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let key = format!(".thadam-shipper-probe/harness-{nanos:x}");
    let path = object_store::path::Path::from(key.as_str());
    store
        .put_opts(&path, "probe".into(), object_store::PutMode::Create.into())
        .await
        .expect("probe put");
    let conflict = store
        .put_opts(
            &path,
            "probe-conflict".into(),
            object_store::PutMode::Create.into(),
        )
        .await;
    let supported = matches!(conflict, Err(object_store::Error::AlreadyExists { .. }));
    store.delete(&path).await.expect("probe cleanup");
    supported
}

/// Every object in the bucket (store-relative keys), sorted.
async fn object_keys(server: &Server) -> Vec<String> {
    use futures_util::StreamExt;
    let store = s3_for_server(server);
    let mut out = Vec::new();
    let mut stream = store.list(None);
    while let Some(meta) = stream.next().await {
        out.push(meta.expect("list").location.to_string());
    }
    out.sort();
    out
}

/// Object count, zero when the store is unreachable (the outage window).
async fn object_count(server: &Server) -> usize {
    use futures_util::StreamExt;
    let store = s3_for_server(server);
    let mut n = 0;
    let mut stream = store.list(None);
    while let Some(meta) = stream.next().await {
        if meta.is_ok() {
            n += 1;
        }
    }
    n
}

fn config_value(server: &Server) -> serde_json::Value {
    serde_json::json!({
        "from": "local",
        "pipeline": "edge-capture",
        "url": format!("s3://{}", server.bucket),
        "endpoint": server.endpoint,
        "region": "us-east-1",
        "allow_http": true,
        "access_key_id": ROOT_USER,
        "secret_access_key": ROOT_SECRET,
        "retry": { "initial": "100ms", "max": "1500ms", "jitter": true },
    })
}

/// A shipper over the test store through the public entry points; `extra`
/// overrides config keys (for example `multipart_threshold`).
fn build_shipper(
    server: &Server,
    metrics: Arc<dyn ShipMetrics>,
    extra: serde_json::Value,
) -> Box<dyn thadam_core::Service> {
    let mut value = config_value(server);
    if let Some(map) = extra.as_object() {
        for (k, v) in map {
            value[k] = v.clone();
        }
    }
    let config = thadam_object_store::parse_ship_config(&value).expect("config");
    let shipper = thadam_object_store::Shipper::new("s3", config, Arc::new(s3_for_server(server)))
        .with_metrics(metrics);
    Box::new(shipper)
}

/// The factory-built service (exercises the CLI registration path).
fn build_service(server: &Server) -> Box<dyn thadam_core::Service> {
    let factory = ObjectStoreShipperFactory::new();
    let ctx = BuildContext::new(
        "s3",
        Arc::new(SystemClock::new()),
        std::path::PathBuf::from("/tmp/thadam-s3-test-service"),
    );
    factory
        .build(config_value(server), &ctx)
        .expect("build service")
}

/// Metrics recorded to memory for assertions.
#[derive(Default)]
struct Recording {
    uploaded_ok: AtomicU64,
    retries: AtomicU64,
    outcomes: Mutex<Vec<String>>,
    conflicts: Mutex<Vec<bool>>,
}

impl ShipMetrics for Recording {
    fn uploaded(&self, _bytes: u64, _latency: Duration, outcome: &str) {
        self.uploaded_ok.fetch_add(1, Ordering::SeqCst);
        self.outcomes.lock().unwrap().push(outcome.to_owned());
    }
    fn retry(&self, _segment: &SegmentId, _attempt: u32, _retry_age: Duration) {
        self.retries.fetch_add(1, Ordering::SeqCst);
    }
    fn conflict_verified(&self, same_content: bool) {
        self.conflicts.lock().unwrap().push(same_content);
    }
    fn released(&self, _files: u64, _bytes: u64) {}
    fn backlog(&self, _files: usize) {}
}

/// A catalog in a tempdir with seeded, eligible segments.
struct Sink {
    dir: TempDir,
    catalog: SegmentCatalog,
    dataset: String,
    node_id: String,
    records: Vec<SegmentRecord>,
}

impl Sink {
    async fn open(dataset: &str) -> Sink {
        let dir = TempDir::new().expect("tempdir");
        let datasets = vec![dataset.to_owned()];
        let root = dir.path().to_owned();
        let node = "edge01".to_owned();
        let ds = datasets.clone();
        let catalog = tokio::task::spawn_blocking(move || {
            SegmentCatalog::open(root, node, None, BudgetCategory::Segments, &ds)
        })
        .await
        .expect("open task")
        .expect("catalog open")
        .0;
        Sink {
            dir,
            catalog,
            dataset: dataset.to_owned(),
            node_id: "edge01".to_owned(),
            records: Vec::new(),
        }
    }

    fn root(&self) -> std::path::PathBuf {
        self.dir.path().to_owned()
    }

    /// Writes `size` bytes (deterministic from `first`) and catalogs the
    /// segment as finalized + eligible.
    async fn seed(&mut self, first: u64, last: u64, size: usize) -> SegmentRecord {
        let name = format!(
            "{}_{:016x}_{:020}_{:020}.parquet",
            self.node_id, 7u64, first, last
        );
        let rel = format!("ready/{}/date=2026-10-08/hour=13/{name}", self.dataset);
        let abs = self.root().join(&rel);
        std::fs::create_dir_all(abs.parent().expect("parent")).expect("mkdir");
        let content: Vec<u8> = (0..size)
            .map(|i| (first.wrapping_add(i as u64) % 251) as u8)
            .collect();
        std::fs::write(&abs, &content).expect("write");
        let mut hasher = Sha256::new();
        hasher.update(&content);
        let mut checksum = [0u8; 32];
        checksum.copy_from_slice(&hasher.finalize());
        let record = SegmentRecord::new(
            SegmentId(rel.clone()),
            self.dataset.clone(),
            Some(std::path::PathBuf::from(rel)),
            WalRange::new(WalEpoch::new(7), WalPos::new(first), WalPos::new(last)).expect("range"),
            10,
            size as u64,
            None,
            None,
            checksum,
            true,
        );
        let pos = record.range.last();
        self.catalog
            .append_finalized(record.clone())
            .await
            .expect("finalized");
        self.catalog.mark_eligible(pos).await.expect("eligible");
        self.records.push(record.clone());
        record
    }
}

struct ServiceHandle {
    shutdown: Shutdown,
    task: tokio::task::JoinHandle<Result<(), thadam_core::ThadamError>>,
}

/// Starts the service over `catalog` with the pipeline `SharedObjects` wired
/// the way the runtime does it.
fn start_service(
    mut service: Box<dyn thadam_core::Service>,
    catalog: &SegmentCatalog,
) -> ServiceHandle {
    let shutdown = Shutdown::new();
    let shared = SharedObjects::default();
    shared.publish("local", Arc::new(catalog.clone()));
    let ctx = ServiceContext::new(
        "s3",
        shutdown.clone(),
        Arc::new(SystemClock::new()),
        std::path::PathBuf::from("/tmp/thadam-s3-test-service-ctx"),
        Arc::new(DiskBudget::new()),
    )
    .with_shared(shared);
    let task = tokio::spawn(async move { service.run(&ctx).await });
    ServiceHandle { shutdown, task }
}

async fn wait_until(what: &str, mut check: impl FnMut() -> bool) {
    let started = Instant::now();
    loop {
        if check() {
            return;
        }
        assert!(started.elapsed() < TIMEOUT, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

/// A wait loop for async conditions (store round-trips).
async fn wait_until_fn<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = Instant::now();
    loop {
        if check().await {
            return;
        }
        assert!(started.elapsed() < TIMEOUT, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

fn keys_for(record: &SegmentRecord, pipeline: &str) -> String {
    let rel = record.path.as_ref().expect("file");
    let text = rel.to_string_lossy();
    let rest = text
        .strip_prefix(&format!("ready/{}/", record.dataset))
        .expect("ready path");
    // Shipper adds thadam/v1/ prefix per ADR-0013
    format!("thadam/v1/{pipeline}/{}/{rest}", record.dataset)
}

fn sha256_meta() -> object_store::Attribute {
    object_store::Attribute::Metadata(std::borrow::Cow::Borrowed("thadam-sha256"))
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (SeaweedFS)"]
async fn uploads_oldest_first_with_metadata() {
    init_test_runtime();
    let server = Server::start("thadam-ship-basic").await;
    let mut sink = Sink::open("telemetry").await;
    for (first, last) in [(0u64, 9u64), (10, 19), (20, 29)] {
        sink.seed(first, last, 64 * 1024).await;
    }

    let handle = start_service(build_service(&server), &sink.catalog);
    wait_until("all marked uploaded", || {
        sink.catalog.eligible_for_upload().is_empty()
    })
    .await;
    handle.shutdown.trigger();
    handle.task.await.expect("service").expect("run");

    let keys = object_keys(&server).await;
    assert_eq!(
        keys,
        sink.records
            .iter()
            .map(|r| keys_for(r, "edge-capture"))
            .collect::<Vec<_>>(),
        "deterministic keys (no url prefix): {keys:?}"
    );

    let store = s3_for_server(&server);
    for (i, record) in sink.records.iter().enumerate() {
        let path = object_store::path::Path::from(keys[i].as_str());
        let get = store
            .get_opts(&path, Default::default())
            .await
            .expect("get");
        let sha = get
            .attributes
            .get(&sha256_meta())
            .map(|v| v.to_string())
            .expect("thadam-sha256 metadata");
        assert_eq!(
            sha,
            hex_bytes(&record.checksum),
            "sha metadata on {}",
            keys[i]
        );
        assert_eq!(get.meta.size, record.bytes);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (SeaweedFS)"]
async fn destination_outage_then_recovery_drains_backlog() {
    init_test_runtime();
    let server = Server::start("thadam-ship-outage").await;
    let mut sink = Sink::open("telemetry").await;
    sink.seed(0, 9, 64 * 1024).await;

    // Upload the first file, then take the store out mid-backlog.
    let metrics = Arc::new(Recording::default());
    let handle = start_service(
        build_shipper(&server, metrics.clone(), serde_json::json!({})),
        &sink.catalog,
    );
    wait_until_fn("first object landed", || async {
        object_count(&server).await > 0
    })
    .await;

    // Stop BEFORE seeding the second segment: `docker stop` returns once the
    // container is down, so every seg2 attempt is guaranteed to fail against
    // the dead port. (Seeding first races the shipper: seg2 can be fully
    // uploaded during the stop grace window and no retry ever fires.)
    server.stop_container();
    sink.seed(10, 19, 64 * 1024).await;
    wait_until("retry attempts accumulate", || {
        metrics.retries.load(Ordering::SeqCst) > 0
    })
    .await;

    server.start_container();
    wait_until("backlog drained", || {
        sink.catalog.eligible_for_upload().is_empty()
    })
    .await;
    handle.shutdown.trigger();
    handle.task.await.expect("service").expect("run");

    assert_eq!(
        object_keys(&server).await.len(),
        2,
        "one object per segment"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (SeaweedFS)"]
async fn conditional_put_conflict_and_collision() {
    init_test_runtime();
    let server = Server::start("thadam-ship-conflict").await;
    let mut sink = Sink::open("telemetry").await;
    let record = sink.seed(0, 9, 4096).await;
    let key = keys_for(&record, "edge-capture");
    let path = object_store::path::Path::from(key.as_str());
    let store = s3_for_server(&server);

    // What does the store enforce? SeaweedFS 3.81 ignores If-None-Match
    // (verified: a second create returns 200 and overwrites), so
    // `conditional_put: auto` documentedly falls back to overwrite.
    // Copy the segment bytes now: the service runs default
    // delete_after_upload retention, which removes the local file.
    let abs = sink.root().join(record.path.as_ref().unwrap());
    let segment_body = std::fs::read(&abs).expect("local segment");

    let conditional_create = store_supports_conditional_create(&store).await;

    // Another writer owns the key with different content.
    store
        .put_opts(
            &path,
            vec![0xabu8; 4097].into(),
            object_store::PutMode::Overwrite.into(),
        )
        .await
        .expect("out-of-band put");
    let metrics = Arc::new(Recording::default());
    let handle = start_service(
        build_shipper(&server, metrics.clone(), serde_json::json!({})),
        &sink.catalog,
    );

    if conditional_create {
        // Stores that enforce If-None-Match: the collision is Fatal and the
        // foreign object is never silently overwritten.
        wait_until("fatal surfaces", || handle.task.is_finished()).await;
        let error = handle
            .task
            .await
            .expect("service")
            .expect_err("collision expected");
        assert_eq!(error.code, codes::SINK_OBJECT_KEY_COLLISION, "{error}");
        assert!(!sink.catalog.eligible_for_upload().is_empty(), "not marked");
        assert_eq!(*metrics.conflicts.lock().unwrap(), vec![false]);

        // Same bytes and sha metadata: verification-success path.
        let mut attrs = object_store::Attributes::new();
        attrs.insert(sha256_meta(), hex_bytes(&record.checksum).into());
        store
            .put_opts(
                &path,
                segment_body.clone().into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Overwrite,
                    attributes: attrs,
                    ..Default::default()
                },
            )
            .await
            .expect("equal-content put");
        let metrics = Arc::new(Recording::default());
        let handle = start_service(
            build_shipper(&server, metrics.clone(), serde_json::json!({})),
            &sink.catalog,
        );
        wait_until("verified and marked", || {
            sink.catalog.eligible_for_upload().is_empty()
        })
        .await;
        handle.shutdown.trigger();
        handle.task.await.expect("service").expect("run");
        assert_eq!(*metrics.conflicts.lock().unwrap(), vec![true]);
        assert_eq!(object_keys(&server).await, vec![key]);
        return;
    }

    // Documented fallback (`resolve_put_mode` warns): the store ignores
    // If-None-Match, so `auto` ships with overwrite puts. Collisions cannot
    // be detected; the guarantee narrows to "deterministic keys over
    // immutable files", and the final object must be exactly the segment's
    // payload with its metadata.
    wait_until("backlog drained (overwrite fallback)", || {
        sink.catalog.eligible_for_upload().is_empty()
    })
    .await;
    handle.shutdown.trigger();
    handle.task.await.expect("service").expect("run");

    assert_eq!(
        metrics.uploaded_ok.load(Ordering::SeqCst),
        1,
        "one upload finished"
    );
    assert_eq!(metrics.outcomes.lock().unwrap().as_slice(), ["put"]);
    assert!(
        metrics.conflicts.lock().unwrap().is_empty(),
        "overwrite fallback runs no conflict verification"
    );
    assert_eq!(object_keys(&server).await, vec![key.clone()]);
    let get = store
        .get_opts(&path, Default::default())
        .await
        .expect("get after fallback");
    assert_eq!(get.meta.size, record.bytes, "foreign object overwritten");
    let sha = get
        .attributes
        .get(&sha256_meta())
        .map(|v| v.to_string())
        .expect("thadam-sha256 metadata after fallback put");
    assert_eq!(sha, hex_bytes(&record.checksum));
    use futures_util::StreamExt;
    let mut body = Vec::new();
    let mut stream = get.into_stream();
    while let Some(chunk) = stream.next().await {
        body.extend_from_slice(&chunk.expect("chunk"));
    }
    assert_eq!(body, segment_body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (SeaweedFS)"]
async fn multipart_upload_round_trip() {
    init_test_runtime();
    let server = Server::start("thadam-ship-multipart").await;
    let mut sink = Sink::open("telemetry").await;
    let size = 17 * 1024 * 1024 + 1024;
    let record = sink.seed(0, 9, size).await;

    let handle = start_service(
        build_shipper(
            &server,
            Arc::new(Recording::default()),
            serde_json::json!({ "multipart_threshold": "8MiB" }),
        ),
        &sink.catalog,
    );
    wait_until("multipart uploaded and marked", || {
        sink.catalog.eligible_for_upload().is_empty()
    })
    .await;
    handle.shutdown.trigger();
    handle.task.await.expect("service").expect("run");

    let key = keys_for(&record, "edge-capture");
    let store = s3_for_server(&server);
    let get = store
        .get_opts(
            &object_store::path::Path::from(key.as_str()),
            Default::default(),
        )
        .await
        .expect("get");
    assert_eq!(get.meta.size, size as u64);
    let sha = get
        .attributes
        .get(&sha256_meta())
        .map(|v| v.to_string())
        .expect("sha metadata on multipart object");
    assert_eq!(sha, hex_bytes(&record.checksum));
    if let Some(etag) = &get.meta.e_tag {
        assert!(etag.contains('-'), "multipart etag shape: {etag}");
    }
    use futures_util::StreamExt;
    let mut body = Vec::new();
    let mut stream = get.into_stream();
    while let Some(chunk) = stream.next().await {
        body.extend_from_slice(&chunk.expect("chunk"));
    }
    let mut hasher = Sha256::new();
    hasher.update(&body);
    assert_eq!(hex_bytes(&hasher.finalize()), hex_bytes(&record.checksum));
    assert_eq!(object_keys(&server).await, vec![key]);
}

/// Installs the ring rustls crypto provider (reqwest is built
/// `rustls-no-provider`; ADR-0020 bans aws-lc) and, when `RUST_LOG` is set,
/// a tracing subscriber so shipper logs are visible.
fn init_test_runtime() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// HMAC-SHA256 (RFC 2104) over sha2; block size 64.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    const BLOCK: usize = 64;
    let mut key_block = [0u8; BLOCK];
    if key.len() > BLOCK {
        let digest = Sha256::digest(key);
        key_block[..32].copy_from_slice(&digest);
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }
    let mut ipad = vec![0x36u8; BLOCK];
    let mut opad = vec![0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= key_block[i];
        opad[i] ^= key_block[i];
    }
    let mut inner = Sha256::new();
    inner.update(&ipad);
    inner.update(data);
    let inner_hash = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(&opad);
    outer.update(inner_hash);
    outer.finalize().to_vec()
}

/// `YYYYmmddTHHMMSSZ` and `YYYYmmdd` without a datetime crate (Howard
/// Hinnant's civil_from_days plus clock decomposition).
fn amz_times_from_secs(secs: u64) -> (String, String) {
    let days = (secs / 86_400) as i64;
    let day_secs = secs % 86_400;
    let (hour, min, sec) = (day_secs / 3600, (day_secs % 3600) / 60, day_secs % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (
        format!("{y:04}{m:02}{d:02}T{hour:02}{min:02}{sec:02}Z"),
        format!("{y:04}{m:02}{d:02}"),
    )
}

fn amz_times() -> (String, String) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    amz_times_from_secs(secs)
}

#[test]
fn amz_times_known_values() {
    // Unix 0 -> 1970-01-01T00:00:00Z
    let (ts, date) = amz_times_from_secs(0);
    assert_eq!(ts, "19700101T000000Z");
    assert_eq!(date, "19700101");

    // 951782400 -> 2000-02-29T00:00:00Z (leap year)
    let (ts, date) = amz_times_from_secs(951_782_400);
    assert_eq!(ts, "20000229T000000Z");
    assert_eq!(date, "20000229");

    // 1767225599 -> 2025-12-31T23:59:59Z
    let (ts, date) = amz_times_from_secs(1_767_225_599);
    assert_eq!(ts, "20251231T235959Z");
    assert_eq!(date, "20251231");
}

/// Creates the bucket with a Signature V4 PUT (no helper container needed).
fn create_bucket(endpoint: &str, bucket: &str, ak: &str, sk: &str) {
    let (amz_date, date) = amz_times();
    let host = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .expect("endpoint");
    let payload_hash = hex_bytes(&Sha256::digest(b""));
    let region = "us-east-1";
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let canonical = format!(
        "PUT\n/{bucket}\n\n{canonical_headers}\nhost;x-amz-content-sha256;x-amz-date\n{payload_hash}"
    );
    let scope = format!("{date}/{region}/s3/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex_bytes(&Sha256::digest(canonical.as_bytes()))
    );
    let k_date = hmac_sha256(format!("AWS4{sk}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let signature = hex_bytes(&hmac_sha256(&k_signing, to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={ak}/{date}/{region}/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
    );

    let out = Command::new("curl")
        .args([
            "-s",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "--max-time",
            "10",
            "-X",
            "PUT",
            "-H",
            &format!("x-amz-date: {amz_date}"),
            "-H",
            &format!("x-amz-content-sha256: {payload_hash}"),
            "-H",
            &format!("Authorization: {authorization}"),
            &format!("{endpoint}/{bucket}"),
        ])
        .output()
        .expect("curl");
    let code = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    assert!(
        out.status.success() && matches!(code.as_str(), "200" | "409"),
        "create bucket http {code}: {}",
        err(&out)
    );
}
