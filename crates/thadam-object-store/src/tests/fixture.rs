//! Shared fixture pieces for shipper tests: a seeded catalog on disk, a
//! deterministic segment record + file, an instrumented `ObjectStore`
//! wrapper, and a recording metrics sink.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fmt;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
};
use thadam_core::{
    BudgetCategory, DiskBudget, Service, ServiceContext, SharedObjects, Shutdown, SystemClock,
    WalEpoch, WalPos, WalRange,
};
use thadam_segments::{SegmentCatalog, SegmentId, SegmentRecord};
use sha2::Digest;
use tempfile::TempDir;

use crate::config::{
    ConditionalPut, Credentials, RetentionConfig, RetentionMode, RetryConfig, ShipConfig,
};
use crate::metrics::ShipMetrics;
use crate::shipper::Shipper;

/// A catalog opened in a fresh tempdir, plus the tempdir handle.
pub(crate) struct TestSink {
    /// The tempdir (kept alive for the test).
    pub(crate) dir: TempDir,
    /// The sink's node id.
    pub(crate) node_id: String,
    /// The dataset the records claim.
    pub(crate) dataset: String,
}

impl TestSink {
    /// A fresh tempdir; no catalog yet.
    pub(crate) fn new() -> Self {
        TestSink {
            dir: TempDir::new().expect("tempdir"),
            node_id: "edge01".to_owned(),
            dataset: "telemetry".to_owned(),
        }
    }

    /// The sink root (the catalog's `root()`).
    pub(crate) fn root(&self) -> std::path::PathBuf {
        self.dir.path().join("sink")
    }
}

/// Opens (or re-opens) the catalog for `sink`.
pub(crate) async fn open_catalog(sink: &TestSink) -> SegmentCatalog {
    let root = sink.root();
    let datasets = vec![sink.dataset.clone()];
    let node = sink.node_id.clone();
    tokio::task::spawn_blocking(move || {
        SegmentCatalog::open(root, node, None, BudgetCategory::Segments, &datasets)
    })
    .await
    .expect("open task")
    .expect("catalog open")
    .0
}

/// `edge01_{epoch:016x}_{first:020}_{last:020}.parquet` (mirrors the
/// `ready/` layout).
pub(crate) fn segment_name(node_id: &str, epoch: u64, first: u64, last: u64) -> String {
    format!("{node_id}_{epoch:016x}_{first:020}_{last:020}.parquet")
}

/// Writes the segment file under `ready/` and returns its catalog record,
/// SHA-256 filled in. Content is deterministic from `first`.
pub(crate) fn make_record(
    sink: &TestSink,
    epoch: u64,
    first: u64,
    last: u64,
    size: usize,
    synced: bool,
    partition_dir: &str,
) -> SegmentRecord {
    let name = segment_name(&sink.node_id, epoch, first, last);
    let rel = format!("ready/{}/{partition_dir}/{name}", sink.dataset);
    let abs = sink.root().join(&rel);
    std::fs::create_dir_all(abs.parent().expect("parent")).expect("mkdir");
    let content: Vec<u8> = (0..size)
        .map(|i| (first.wrapping_add(i as u64) % 251) as u8)
        .collect();
    std::fs::write(&abs, &content).expect("write segment");
    let mut hasher = sha2::Sha256::new();
    hasher.update(&content);
    let digest = hasher.finalize();
    let mut checksum = [0u8; 32];
    checksum.copy_from_slice(&digest);
    SegmentRecord::new(
        SegmentId(rel.clone()),
        sink.dataset.clone(),
        Some(std::path::PathBuf::from(rel)),
        WalRange::new(WalEpoch::new(epoch), WalPos::new(first), WalPos::new(last)).expect("range"),
        10,
        size as u64,
        None,
        None,
        checksum,
        synced,
    )
}

/// Catalogs a record as finalized and makes it ship-eligible.
pub(crate) async fn catalog_segment(catalog: &SegmentCatalog, record: SegmentRecord) {
    let pos = record.range.last();
    catalog
        .append_finalized(record)
        .await
        .expect("append_finalized");
    catalog.mark_eligible(pos).await.expect("mark_eligible");
}

/// A config with tiny sizes/times; validation is bypassed on purpose.
pub(crate) fn make_config(pipeline: &str, prefix: &str, mode: RetentionMode) -> ShipConfig {
    ShipConfig {
        from: "local".to_owned(),
        pipeline: pipeline.to_owned(),
        url: format!("s3://bucket/{prefix}"),
        prefix: prefix.to_owned(),
        endpoint: None,
        region: "us-east-1".to_owned(),
        allow_http: true,
        credentials: Credentials::new("test", "test"),
        upload_concurrency: 1,
        multipart_threshold: u64::MAX,
        part_size: 8 << 20,
        conditional_put: ConditionalPut::Create,
        retry: RetryConfig {
            initial: Duration::from_millis(5),
            max: Duration::from_millis(50),
            jitter: false,
        },
        retention: RetentionConfig {
            mode,
            keep_for: Some(Duration::from_secs(3600)),
        },
    }
}

/// Runs the shipper until `done` holds (or the timeout expires), then
/// triggers shutdown and returns the service outcome.
pub(crate) async fn run_service_until(
    mut shipper: Shipper,
    catalog: &SegmentCatalog,
    done: impl Fn(&SegmentCatalog, &Arc<RecordingMetrics>) -> bool,
    metrics: &Arc<RecordingMetrics>,
    timeout: Duration,
) -> Result<(), thadam_core::ThadamError> {
    let shutdown = Shutdown::new();
    let shared = SharedObjects::default();
    shared.publish("local", Arc::new(catalog.clone()));
    let ctx = ServiceContext::new(
        "s3",
        shutdown.clone(),
        Arc::new(SystemClock::new()),
        std::path::PathBuf::from("/tmp/thadam-ship-test-service"),
        Arc::new(DiskBudget::new()),
    )
    .with_shared(shared);
    let task = tokio::spawn(async move { shipper.run(&ctx).await });
    let start = std::time::Instant::now();
    loop {
        if task.is_finished() {
            break;
        }
        if done(catalog, metrics) {
            shutdown.trigger();
            break;
        }
        if start.elapsed() > timeout {
            shutdown.trigger();
            panic!("shipper did not finish in {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    task.await.expect("service task")
}

/// Metrics recorded to memory for assertions.
#[derive(Debug, Default)]
pub(crate) struct RecordingMetrics {
    /// Recorded `(bytes, outcome)` uploads.
    pub(crate) uploaded: Mutex<Vec<(u64, String)>>,
    /// Recorded `(segment, attempt)` retries.
    pub(crate) retries: Mutex<Vec<(String, u32)>>,
    /// Conflict-verification outcomes.
    pub(crate) conflicts: Mutex<Vec<bool>>,
    /// Retention deletions `(files, bytes)`.
    pub(crate) released: Mutex<Vec<(u64, u64)>>,
}

impl RecordingMetrics {
    /// A fresh recorder.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(RecordingMetrics::default())
    }

    pub(crate) fn uploaded_count(&self) -> usize {
        self.uploaded
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }
}

/// Connector-metric events from the service-context hook (what the
/// runtime would see), debug-formatted in order.
#[derive(Debug, Default)]
pub(crate) struct RecordingConnectorMetrics {
    events: Mutex<Vec<String>>,
}

impl RecordingConnectorMetrics {
    /// A fresh recorder.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(RecordingConnectorMetrics::default())
    }

    /// Recorded events whose text contains `needle`.
    pub(crate) fn matching(&self, needle: &str) -> Vec<String> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|e| e.contains(needle))
            .cloned()
            .collect()
    }
}

impl thadam_core::MetricsRecorder for RecordingConnectorMetrics {
    fn record(&self, metric: thadam_core::ConnectorMetric<'_>) {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(format!("{metric:?}"));
    }
}

/// Like [`run_service_until`], but the shipper reports through the service
/// context's metrics hook (no `with_metrics`) and `done` sees the
/// connector events.
pub(crate) async fn run_service_until_hooked(
    mut shipper: Shipper,
    catalog: &SegmentCatalog,
    done: impl Fn(&SegmentCatalog, &Arc<RecordingConnectorMetrics>) -> bool,
    metrics: &Arc<RecordingConnectorMetrics>,
    timeout: Duration,
) -> Result<(), thadam_core::ThadamError> {
    let shutdown = Shutdown::new();
    let shared = SharedObjects::default();
    shared.publish("local", Arc::new(catalog.clone()));
    let ctx = ServiceContext::new(
        "s3",
        shutdown.clone(),
        Arc::new(SystemClock::new()),
        std::path::PathBuf::from("/tmp/thadam-ship-test-service"),
        Arc::new(DiskBudget::new()),
    )
    .with_shared(shared)
    .with_metrics(metrics.clone());
    let task = tokio::spawn(async move { shipper.run(&ctx).await });
    let start = std::time::Instant::now();
    loop {
        if task.is_finished() {
            break;
        }
        if done(catalog, metrics) {
            shutdown.trigger();
            break;
        }
        if start.elapsed() > timeout {
            shutdown.trigger();
            panic!("shipper did not finish in {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    task.await.expect("service task")
}

impl ShipMetrics for RecordingMetrics {
    fn uploaded(&self, bytes: u64, _latency: Duration, outcome: &str) {
        self.uploaded
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((bytes, outcome.to_owned()));
    }

    fn retry(&self, segment: &SegmentId, attempt: u32, _retry_age: Duration) {
        self.retries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((segment.to_string(), attempt));
    }

    fn conflict_verified(&self, same_content: bool) {
        self.conflicts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(same_content);
    }

    fn released(&self, files: u64, bytes: u64) {
        self.released
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((files, bytes));
    }

    fn backlog(&self, _files: usize) {}
}

/// A scripted `ObjectStore` around `InMemory`: failure injection, latency,
/// order capture, and concurrency tracking.
pub(crate) struct FlakyStore {
    inner: object_store::memory::InMemory,
    put_failures: AtomicU64,
    put_delay: Duration,
    /// Successful put keys, in order.
    pub(crate) order: Mutex<Vec<String>>,
    current: AtomicU64,
    /// The largest number of puts in flight at once.
    pub(crate) max_concurrent: AtomicU64,
}

impl FlakyStore {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(FlakyStore {
            inner: object_store::memory::InMemory::new(),
            put_failures: AtomicU64::new(0),
            put_delay: Duration::ZERO,
            order: Mutex::new(Vec::new()),
            current: AtomicU64::new(0),
            max_concurrent: AtomicU64::new(0),
        })
    }

    /// Artificial per-put latency (mutated once before the store is shared).
    pub(crate) fn with_delay(mut self: Arc<Self>, delay: Duration) -> Arc<Self> {
        if let Some(this) = Arc::get_mut(&mut self) {
            this.put_delay = delay;
        }
        self
    }

    pub(crate) fn fail_puts(self: &Arc<Self>, count: u64) -> &Arc<Self> {
        self.put_failures.store(count, Ordering::Relaxed);
        self
    }

    pub(crate) fn store(self: &Arc<Self>) -> Arc<dyn ObjectStore> {
        self.clone()
    }
}

impl fmt::Display for FlakyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("flaky-memory")
    }
}

impl fmt::Debug for FlakyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("flaky-memory")
    }
}

fn transient() -> object_store::Error {
    object_store::Error::Generic {
        store: "flaky-memory",
        source: "injected transient put failure".into(),
    }
}

/// An `InMemory` store that, like SeaweedFS 3.81, ignores `If-None-Match`:
/// a create-mode put is executed as an overwrite. The shipper's startup
/// probe must detect this and fall `conditional_put: auto` back to
/// overwrite.
pub(crate) struct LaxCreateStore {
    inner: object_store::memory::InMemory,
}

impl LaxCreateStore {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(LaxCreateStore {
            inner: object_store::memory::InMemory::new(),
        })
    }

    pub(crate) fn store(self: &Arc<Self>) -> Arc<dyn ObjectStore> {
        self.clone()
    }
}

impl fmt::Display for LaxCreateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("lax-create-memory")
    }
}

impl fmt::Debug for LaxCreateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("lax-create-memory")
    }
}

#[async_trait::async_trait]
impl ObjectStore for LaxCreateStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        let opts = PutOptions {
            mode: object_store::PutMode::Overwrite,
            ..opts
        };
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
}

#[async_trait::async_trait]
impl ObjectStore for FlakyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        let current = self.current.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_concurrent.fetch_max(current, Ordering::Relaxed);
        if self.put_delay != Duration::ZERO {
            tokio::time::sleep(self.put_delay).await;
        }
        let is_segment_put = location.to_string().starts_with("thadam/v1/");
        let injected = is_segment_put && {
            let prev = self.put_failures.load(Ordering::Relaxed);
            if prev > 0 {
                self.put_failures.store(prev - 1, Ordering::Relaxed);
                true
            } else {
                false
            }
        };
        self.current.fetch_sub(1, Ordering::Relaxed);
        if injected {
            return Err(transient());
        }
        let result = self.inner.put_opts(location, payload, opts).await;
        if result.is_ok() {
            self.order
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(location.to_string());
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
}
