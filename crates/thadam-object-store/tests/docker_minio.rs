//! Docker integration tests against MinIO.
//!
//! These are `#[ignore]`d so `cargo nextest run` passes without Docker; run
//! them with
//!
//! ```text
//! cargo nextest run -p thadam-object-store --run-ignored only -E 'binary(docker_minio)'
//! ```
//!
//! Images: public Docker Hub only, pinned by tag (never a corporate mirror);
//! re-pin with `THADAM_TEST_MINIO_IMAGE` when updating. Only `minio/minio` is
//! needed: the bucket is created with a Signature V4 HTTP call, so no
//! `minio/mc` helper container. `curl` and `docker` CLIs must be available.
//!
//! Containers are labelled `thadam-object-store-test` and removed when a test
//! ends, including on panic.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use object_store::ObjectStore;
use thadam_core::{
    BudgetCategory, BuildContext, DiskBudget, ServiceContext, ServiceFactory, SharedObjects,
    Shutdown, SystemClock, WalEpoch, WalPos, WalRange, codes,
};
use thadam_segments::{SegmentCatalog, SegmentId, SegmentRecord};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use thadam_object_store::{ObjectStoreShipperFactory, ShipMetrics};

/// The pinned MinIO image (public Docker Hub only; the 2025 public release
/// line). Re-pin with `THADAM_TEST_MINIO_IMAGE` when updating.
const MINIO_IMAGE: &str = "minio/minio:RELEASE.2025-10-15T17-29-55Z";
const LABEL: &str = "thadam-object-store-test";
const ROOT_USER: &str = "thadam-test";
const ROOT_SECRET: &str = "thadam-test-secret-16+bytes";
const TIMEOUT: Duration = Duration::from_secs(180);

fn minio_image() -> String {
    std::env::var("THADAM_TEST_MINIO_IMAGE").unwrap_or_else(|_| MINIO_IMAGE.to_owned())
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

/// A running MinIO container; removed on Drop.
struct Minio {
    name: String,
    endpoint: String,
    bucket: String,
}

impl Minio {
    async fn start(bucket: &str) -> Minio {
        let bucket = bucket.to_owned();
        tokio::task::spawn_blocking(move || Self::start_blocking(&bucket))
            .await
            .expect("docker task")
    }

    fn start_blocking(bucket: &str) -> Minio {
        let name = format!(
            "thadam-objstore-{}-{:x}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        let out = docker(&[
            "run",
            "-d",
            "--rm",
            "--label",
            LABEL,
            "-p",
            "127.0.0.1::9000",
            "-e",
            &format!("MINIO_ROOT_USER={ROOT_USER}"),
            "-e",
            &format!("MINIO_ROOT_PASSWORD={ROOT_SECRET}"),
            &minio_image(),
            "server",
            "/data",
        ]);
        assert!(out.status.success(), "docker run: {}", err(&out));

        let port = {
            let started = Instant::now();
            loop {
                let out = docker(&["port", &name, "9000/tcp"]);
                if out.status.success() {
                    let text = String::from_utf8_lossy(&out.stdout);
                    if let Some((_, port)) = text.trim().rsplit_once(':') {
                        break port.to_owned();
                    }
                }
                assert!(started.elapsed() < TIMEOUT, "waiting for the mapped port");
                std::thread::sleep(Duration::from_millis(150));
            }
        };
        let endpoint = format!("http://127.0.0.1:{port}");

        let minio = Minio {
            name,
            endpoint,
            bucket: bucket.to_owned(),
        };
        minio.wait_ready();
        create_bucket(&minio.endpoint, bucket, ROOT_USER, ROOT_SECRET);
        minio
    }

    fn wait_ready(&self) {
        let started = Instant::now();
        loop {
            let ok = Command::new("curl")
                .args([
                    "-sf",
                    "--max-time",
                    "5",
                    &format!("{}/minio/health/ready", self.endpoint),
                ])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            if ok {
                return;
            }
            assert!(started.elapsed() < TIMEOUT, "minio did not become ready");
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

impl Drop for Minio {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", &self.name]);
    }
}

/// An independent S3 client toward the test MinIO.
fn s3_for_test(minio: &Minio) -> object_store::aws::AmazonS3 {
    object_store::aws::AmazonS3Builder::new()
        .with_url(format!("s3://{}", minio.bucket))
        .with_endpoint(&minio.endpoint)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_access_key_id(ROOT_USER)
        .with_secret_access_key(ROOT_SECRET)
        .build()
        .expect("builder")
}

/// Every object in the bucket (store-relative keys), sorted.
async fn object_keys(minio: &Minio) -> Vec<String> {
    use futures_util::StreamExt;
    let store = s3_for_test(minio);
    let mut out = Vec::new();
    let mut stream = store.list(None);
    while let Some(meta) = stream.next().await {
        out.push(meta.expect("list").location.to_string());
    }
    out.sort();
    out
}

/// Object count, zero when the store is unreachable (the outage window).
async fn object_count(minio: &Minio) -> usize {
    use futures_util::StreamExt;
    let store = s3_for_test(minio);
    let mut n = 0;
    let mut stream = store.list(None);
    while let Some(meta) = stream.next().await {
        if meta.is_ok() {
            n += 1;
        }
    }
    n
}

fn config_value(minio: &Minio) -> serde_json::Value {
    serde_json::json!({
        "from": "local",
        "pipeline": "edge-capture",
        "url": format!("s3://{}", minio.bucket),
        "endpoint": minio.endpoint,
        "region": "us-east-1",
        "allow_http": true,
        "access_key_id": ROOT_USER,
        "secret_access_key": ROOT_SECRET,
        "retry": { "initial": "100ms", "max": "1500ms", "jitter": true },
    })
}

/// A shipper over the test MinIO through the public entry points; `extra`
/// overrides config keys (for example `multipart_threshold`).
fn build_shipper(
    minio: &Minio,
    metrics: Arc<dyn ShipMetrics>,
    extra: serde_json::Value,
) -> Box<dyn thadam_core::Service> {
    let mut value = config_value(minio);
    if let Some(map) = extra.as_object() {
        for (k, v) in map {
            value[k] = v.clone();
        }
    }
    let config = thadam_object_store::parse_ship_config(&value).expect("config");
    let shipper = thadam_object_store::Shipper::new("s3", config, Arc::new(s3_for_test(minio)))
        .with_metrics(metrics);
    Box::new(shipper)
}

/// The factory-built service (exercises the CLI registration path).
fn build_service(minio: &Minio) -> Box<dyn thadam_core::Service> {
    let factory = ObjectStoreShipperFactory::new();
    let ctx = BuildContext::new(
        "s3",
        Arc::new(SystemClock::new()),
        std::path::PathBuf::from("/tmp/thadam-minio-test-service"),
    );
    factory
        .build(config_value(minio), &ctx)
        .expect("build service")
}

/// Metrics recorded to memory for assertions.
#[derive(Default)]
struct Recording {
    uploaded_ok: AtomicU64,
    retries: AtomicU64,
    conflicts: Mutex<Vec<bool>>,
}

impl ShipMetrics for Recording {
    fn uploaded(&self, _bytes: u64, _latency: Duration, _outcome: &str) {
        self.uploaded_ok.fetch_add(1, Ordering::SeqCst);
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
        std::path::PathBuf::from("/tmp/thadam-minio-test-service-ctx"),
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
    format!("{pipeline}/{}/{rest}", record.dataset)
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
#[ignore = "needs Docker (MinIO)"]
async fn uploads_oldest_first_with_metadata() {
    let minio = Minio::start("thadam-ship-basic").await;
    let mut sink = Sink::open("telemetry").await;
    for (first, last) in [(0u64, 9u64), (10, 19), (20, 29)] {
        sink.seed(first, last, 64 * 1024).await;
    }

    let handle = start_service(build_service(&minio), &sink.catalog);
    wait_until("all marked uploaded", || {
        sink.catalog.eligible_for_upload().is_empty()
    })
    .await;
    handle.shutdown.trigger();
    handle.task.await.expect("service").expect("run");

    let keys = object_keys(&minio).await;
    assert_eq!(
        keys,
        sink.records
            .iter()
            .map(|r| keys_for(r, "edge-capture"))
            .collect::<Vec<_>>(),
        "deterministic keys (no url prefix): {keys:?}"
    );

    let store = s3_for_test(&minio);
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
#[ignore = "needs Docker (MinIO)"]
async fn destination_outage_then_recovery_drains_backlog() {
    let minio = Minio::start("thadam-ship-outage").await;
    let mut sink = Sink::open("telemetry").await;
    sink.seed(0, 9, 64 * 1024).await;

    // Upload the first file, then take the store out mid-backlog.
    let metrics = Arc::new(Recording::default());
    let handle = start_service(
        build_shipper(&minio, metrics.clone(), serde_json::json!({})),
        &sink.catalog,
    );
    wait_until_fn("first object landed", || async {
        object_count(&minio).await > 0
    })
    .await;

    sink.seed(10, 19, 64 * 1024).await;
    minio.stop_container();
    wait_until("retry attempts accumulate", || {
        metrics.retries.load(Ordering::SeqCst) > 0
    })
    .await;

    minio.start_container();
    wait_until("backlog drained", || {
        sink.catalog.eligible_for_upload().is_empty()
    })
    .await;
    handle.shutdown.trigger();
    handle.task.await.expect("service").expect("run");

    assert_eq!(object_keys(&minio).await.len(), 2, "one object per segment");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (MinIO)"]
async fn conditional_put_conflict_and_collision() {
    let minio = Minio::start("thadam-ship-conflict").await;
    let mut sink = Sink::open("telemetry").await;
    let record = sink.seed(0, 9, 4096).await;
    let key = keys_for(&record, "edge-capture");
    let path = object_store::path::Path::from(key.as_str());
    let store = s3_for_test(&minio);

    // Another writer owns the key with different content: Fatal collision.
    store
        .put_opts(
            &path,
            vec![0xabu8; 4097].into(),
            object_store::PutMode::Overwrite.into(),
        )
        .await
        .expect("out-of-band put");
    let handle = start_service(
        build_shipper(
            &minio,
            Arc::new(Recording::default()),
            serde_json::json!({}),
        ),
        &sink.catalog,
    );
    wait_until("fatal surfaces", || handle.task.is_finished()).await;
    let error = handle
        .task
        .await
        .expect("service")
        .expect_err("collision expected");
    assert_eq!(error.code, codes::SINK_OBJECT_KEY_COLLISION, "{error}");
    assert!(!sink.catalog.eligible_for_upload().is_empty(), "not marked");

    // Same bytes and sha metadata: verification-success path.
    let abs = sink.root().join(record.path.as_ref().unwrap());
    let body = std::fs::read(&abs).expect("local");
    let mut attrs = object_store::Attributes::new();
    attrs.insert(sha256_meta(), hex_bytes(&record.checksum).into());
    store
        .put_opts(
            &path,
            body.clone().into(),
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
        build_shipper(&minio, metrics.clone(), serde_json::json!({})),
        &sink.catalog,
    );
    wait_until("verified and marked", || {
        sink.catalog.eligible_for_upload().is_empty()
    })
    .await;
    handle.shutdown.trigger();
    handle.task.await.expect("service").expect("run");
    assert_eq!(*metrics.conflicts.lock().unwrap(), vec![true]);
    assert_eq!(object_keys(&minio).await, vec![key]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (MinIO)"]
async fn multipart_upload_round_trip() {
    let minio = Minio::start("thadam-ship-multipart").await;
    let mut sink = Sink::open("telemetry").await;
    let size = 17 * 1024 * 1024 + 1024;
    let record = sink.seed(0, 9, size).await;

    let handle = start_service(
        build_shipper(
            &minio,
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
    let store = s3_for_test(&minio);
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
    assert_eq!(object_keys(&minio).await, vec![key]);
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
fn amz_times() -> (String, String) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = (secs / 86_400) as i64;
    let day_secs = secs % 86_400;
    let (hour, min, sec) = (day_secs / 3600, (day_secs % 3600) / 60, day_secs % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u64;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u64;
    let y = if m <= 2 { y + 1 } else { y } as u64;
    (
        format!("{y:04}{m:02}{d:02}T{hour:02}{min:02}{sec:02}Z"),
        format!("{y:04}{m:02}{d:02}"),
    )
}

/// Creates the bucket with a Signature V4 PUT (no mc container needed).
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
