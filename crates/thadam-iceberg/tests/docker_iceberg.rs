//! Docker integration tests: real Parquet segments shipped to SeaweedFS
//! (S3) and registered in an Iceberg REST catalog
//! (`apache/iceberg-rest-fixture`).
//!
//! These are `#[ignore]`d so `cargo nextest run` passes without Docker; run
//! them with
//!
//! ```text
//! cargo nextest run -p thadam-iceberg --run-ignored only -E 'binary(docker_iceberg)'
//! ```
//!
//! Images: public Docker Hub only, pinned by tag (never a corporate mirror);
//! re-pin with `THADAM_TEST_S3_IMAGE` / `THADAM_TEST_ICEBERG_REST_IMAGE`.
//! `docker` and `curl` CLIs must be available. Containers and the per-test
//! network are labelled `thadam-iceberg-test` and removed on Drop, including
//! on panic. Logs: run with `RUST_LOG=debug` (the harness installs a
//! subscriber).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::{Command, Output};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use thadam_core::{
    BudgetCategory, DiskBudget, FieldDef, FieldType, ServiceContext, SharedObjects, Shutdown,
    SystemClock, WalEpoch, WalPos, WalRange,
};
use thadam_segments::{SegmentCatalog, SegmentId, SegmentRecord};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use thadam_arrow::{ArrowOptions, BatchContext, BatchEncoder, DatasetSchema, UnsignedPolicy};
use thadam_iceberg::IcebergRegistrarFactory;

/// The pinned SeaweedFS image (same as the shipper's tests).
const S3_IMAGE: &str = "chrislusf/seaweedfs:3.81";
/// The pinned Iceberg REST catalog fixture (in-memory catalog implementing
/// Iceberg REST spec 0.14; verified on Docker Hub).
const REST_IMAGE: &str = "apache/iceberg-rest-fixture:1.10.1";
const LABEL: &str = "thadam-iceberg-test";
const ROOT_USER: &str = "thadam-test";
const ROOT_SECRET: &str = "thadam-test-secret-16+bytes";
const TIMEOUT: Duration = Duration::from_secs(180);
/// SeaweedFS S3 auth config: one static identity. Auth must be **enabled**
/// for the Iceberg REST fixture: SeaweedFS 3.81 rejects classic
/// aws-chunked signed writes when auth is off (`ErrAuthNotSetup`), and does
/// not strip the newer `...-PAYLOAD-TRAILER` form AWS SDK v2 uses with
/// trailing checksums — which is why the fixture also runs with
/// `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` (classic chunked form,
/// decoded server-side).
const S3_AUTH_JSON: &str = r#"{"identities": [{"name": "admin", "credentials": [{"accessKey": "thadam-test", "secretKey": "thadam-test-secret-16+bytes"}], "actions": ["Admin", "Read", "Write", "List"]}]}"#;

/// The dataset used everywhere in this file.
const DATASET: &str = "telemetry";
/// The WAL epoch the seeded segment claims.
const EPOCH: u64 = 7;

fn s3_image() -> String {
    std::env::var("THADAM_TEST_S3_IMAGE").unwrap_or_else(|_| S3_IMAGE.to_owned())
}

fn rest_image() -> String {
    std::env::var("THADAM_TEST_ICEBERG_REST_IMAGE").unwrap_or_else(|_| REST_IMAGE.to_owned())
}

fn init_test_runtime() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("ring provider");
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
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

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    listener.local_addr().expect("local addr").port()
}

fn unique(tag: &str) -> String {
    format!(
        "thadam-ice-{tag}-{}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0)
    )
}

fn wait_http_ready(what: &str, url: &str) {
    let started = Instant::now();
    loop {
        // Any HTTP answer counts as up: SeaweedFS answers the unsigned
        // readiness probe with 403 now that S3 auth is configured.
        let ok = Command::new("curl")
            .args(["-s", "-o", "/dev/null", "--max-time", "5", url])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if ok {
            return;
        }
        assert!(started.elapsed() < TIMEOUT, "{what} did not become ready");
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// SeaweedFS (S3) plus the Iceberg REST fixture on one per-test network.
/// The REST fixture reads table metadata itself on some code paths, so it
/// gets its own S3 view of the same store over the network.
struct Stack {
    network: String,
    seaweed: String,
    rest: String,
    /// Owns the mounted S3 auth config file.
    _auth_dir: tempfile::TempDir,
    /// `http://127.0.0.1:PORT` of the S3 endpoint (host side).
    s3_endpoint: String,
    /// `http://127.0.0.1:PORT` of the REST catalog (host side).
    rest_uri: String,
    bucket: String,
}

/// Cleans up whatever started so far when `start_blocking` panics before a
/// full [`Stack`] exists (leaked containers otherwise poison later runs).
struct StackParts {
    network: String,
    seaweed: Option<String>,
    rest: Option<String>,
}

impl Drop for StackParts {
    fn drop(&mut self) {
        if let Some(rest) = &self.rest {
            let _ = docker(&["rm", "-f", "-v", rest]);
        }
        if let Some(seaweed) = &self.seaweed {
            let _ = docker(&["rm", "-f", "-v", seaweed]);
        }
        let _ = docker(&["network", "rm", &self.network]);
    }
}

impl Stack {
    async fn start(bucket: &str) -> Stack {
        let bucket = bucket.to_owned();
        tokio::task::spawn_blocking(move || Self::start_blocking(&bucket))
            .await
            .expect("docker task")
    }

    fn start_blocking(bucket: &str) -> Stack {
        let network = unique("net");
        let out = docker(&["network", "create", "--label", LABEL, &network]);
        assert!(out.status.success(), "docker network create: {}", err(&out));
        let mut guard = StackParts {
            network: network.clone(),
            seaweed: None,
            rest: None,
        };

        let seaweed = unique("s3");
        let mut stack: Option<Stack> = None;
        for _ in 0..8 {
            let port = free_port();
            let auth_dir = tempfile::TempDir::new().expect("auth dir");
            let auth_file = auth_dir.path().join("s3_auth.json");
            std::fs::write(&auth_file, S3_AUTH_JSON).expect("auth file");
            let auth_mount = format!("{}:/etc/seaweed/s3_auth.json:ro", auth_file.display());
            let out = docker(&[
                "run",
                "-d",
                "--name",
                &seaweed,
                "--hostname",
                "seaweed",
                "--label",
                LABEL,
                "--network",
                &network,
                "-p",
                &format!("127.0.0.1:{port}:9000"),
                "-v",
                &auth_mount,
                &s3_image(),
                "server",
                "-s3",
                "-s3.port=9000",
                "-s3.config=/etc/seaweed/s3_auth.json",
                "-filer",
                "-master.volumePreallocate=false",
                "-master.volumeSizeLimitMB=64",
                "-volume.max=2",
            ]);
            if !out.status.success() {
                let e = err(&out);
                let _ = docker(&["rm", "-f", "-v", &seaweed]);
                if e.contains("port is already allocated") || e.contains("address already in use") {
                    continue;
                }
                panic!("docker run seaweedfs: {e}");
            }
            guard.seaweed = Some(seaweed.clone());
            let s3_endpoint = format!("http://127.0.0.1:{port}");
            wait_http_ready("seaweedfs", &format!("{s3_endpoint}/"));
            create_bucket(&s3_endpoint, bucket, ROOT_USER, ROOT_SECRET);

            let rest = unique("rest");
            let rest_port = free_port();
            let out = docker(&[
                "run",
                "-d",
                "--name",
                &rest,
                "--label",
                LABEL,
                "--network",
                &network,
                "-p",
                &format!("127.0.0.1:{rest_port}:8181"),
                "-e",
                "CATALOG_WAREHOUSE=s3://warehouse/",
                "-e",
                "CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO",
                "-e",
                "CATALOG_S3_ENDPOINT=http://seaweed:9000",
                "-e",
                "CATALOG_S3_PATH__STYLE__ACCESS=true",
                "-e",
                &format!("CATALOG_S3_ACCESS__KEY__ID={ROOT_USER}"),
                "-e",
                &format!("CATALOG_S3_SECRET__ACCESS__KEY={ROOT_SECRET}"),
                "-e",
                "AWS_REGION=us-east-1",
                // SeaweedFS 3.81 cannot unwrap SDK v2 trailing-checksum
                // chunked writes (the `...-PAYLOAD-TRAILER` form); with
                // checksums only "when_required" the fixture writes plain or
                // classic-chunked PUTs, which SeaweedFS-with-auth decodes.
                "-e",
                "AWS_REQUEST_CHECKSUM_CALCULATION=when_required",
                "-e",
                "AWS_RESPONSE_CHECKSUM_VALIDATION=when_required",
                &rest_image(),
            ]);
            if !out.status.success() {
                let _ = docker(&["rm", "-f", "-v", &rest]);
                let _ = docker(&["rm", "-f", "-v", &seaweed]);
                let e = err(&out);
                if e.contains("port is already allocated") || e.contains("address already in use") {
                    continue;
                }
                panic!("docker run iceberg-rest-fixture: {e}");
            }
            guard.rest = Some(rest.clone());
            let rest_uri = format!("http://127.0.0.1:{rest_port}");
            wait_http_ready("iceberg-rest-fixture", &format!("{rest_uri}/v1/config"));
            std::mem::forget(guard);
            stack = Some(Stack {
                network: network.clone(),
                seaweed: seaweed.clone(),
                rest,
                _auth_dir: auth_dir,
                s3_endpoint,
                rest_uri,
                bucket: bucket.to_owned(),
            });
            break;
        }
        match stack {
            Some(s) => s,
            None => panic!("stack did not start after port retries"),
        }
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        let _ = docker(&["rm", "-f", "-v", &self.rest]);
        let _ = docker(&["rm", "-f", "-v", &self.seaweed]);
        let _ = docker(&["network", "rm", &self.network]);
    }
}

/// An independent S3 client toward the test SeaweedFS (bucket `s3://BUCKET`).
fn s3_client(endpoint: &str, bucket: &str) -> object_store::aws::AmazonS3 {
    object_store::aws::AmazonS3Builder::new()
        .with_url(format!("s3://{bucket}"))
        .with_endpoint(endpoint)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_access_key_id(ROOT_USER)
        .with_secret_access_key(ROOT_SECRET)
        .with_virtual_hosted_style_request(false)
        .build()
        .expect("s3 builder")
}

/// The `unsigned: signed` dataset schema the Iceberg-compatible segment is
/// written with (Iceberg has no unsigned types; docs/design/iceberg-append.md).
fn dataset_schema() -> Arc<DatasetSchema> {
    DatasetSchema::builder(DATASET)
        .options(ArrowOptions::default().with_unsigned(UnsignedPolicy::Signed))
        .field(FieldDef::new("n", FieldType::I64).with_field_id(1000))
        .build()
        .map(Arc::new)
        .expect("dataset schema")
}

/// Writes a real Parquet segment of `count` rows through `thadam-arrow`
/// (same writer the production sink uses, minus its WriterProperties
/// customization that is irrelevant here) and catalogs it Finalized +
/// Eligible.
async fn write_segment(
    catalog: &SegmentCatalog,
    root: &std::path::Path,
    count: u64,
) -> SegmentRecord {
    let schema = dataset_schema();
    let mut encoder = BatchEncoder::new(Arc::clone(&schema), count as usize);
    encoder.set_context(BatchContext {
        epoch: WalEpoch::new(EPOCH),
        clock_synced: true,
    });
    let batch = thadam_testkit::test_batch(EPOCH, 0, count - 1);
    let rejects = encoder.push_batch(&batch);
    assert!(rejects.is_empty(), "encoder rejects: {rejects:?}");
    let record_batch = encoder.finish();

    let name = format!("edge01_{EPOCH:016x}_{:020}_{:020}.parquet", 0u64, count - 1);
    let rel = format!("ready/{DATASET}/date=2026-10-12/hour=13/{name}");
    let abs = root.join(&rel);
    std::fs::create_dir_all(abs.parent().expect("parent")).expect("mkdir");
    {
        let mut file = std::fs::File::create(&abs).expect("create");
        let props = parquet::file::properties::WriterProperties::builder().build();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut file, schema.arrow_schema(), Some(props))
                .expect("arrow writer");
        writer.write(&record_batch).expect("write batch");
        writer.close().expect("close writer");
    }

    let content = std::fs::read(&abs).expect("read back");
    let mut checksum = [0u8; 32];
    checksum.copy_from_slice(&Sha256::digest(&content));
    let record = SegmentRecord::new(
        SegmentId(rel.clone()),
        DATASET,
        Some(std::path::PathBuf::from(&rel)),
        WalRange::new(WalEpoch::new(EPOCH), WalPos::new(0), WalPos::new(count - 1)).expect("range"),
        count,
        content.len() as u64,
        None,
        None,
        checksum,
        true,
    );
    catalog
        .append_finalized(record.clone())
        .await
        .expect("finalized");
    catalog
        .mark_eligible(record.range.last())
        .await
        .expect("eligible");
    record
}

/// The segment catalog in a tempdir (mirrors the shipper tests' `Sink`).
struct Sink {
    dir: TempDir,
    catalog: SegmentCatalog,
}

impl Sink {
    async fn open() -> Sink {
        let dir = TempDir::new().expect("tempdir");
        let root = dir.path().to_owned();
        let datasets = vec![DATASET.to_owned()];
        let catalog = tokio::task::spawn_blocking(move || {
            SegmentCatalog::open(root, "edge01", None, BudgetCategory::Segments, &datasets)
        })
        .await
        .expect("open task")
        .expect("catalog open")
        .0;
        Sink { dir, catalog }
    }
}

struct ServiceHandle {
    shutdown: Shutdown,
    task: tokio::task::JoinHandle<Result<(), thadam_core::ThadamError>>,
}

fn start_service(
    mut service: Box<dyn thadam_core::Service>,
    id: &'static str,
    catalog: &SegmentCatalog,
) -> ServiceHandle {
    let shutdown = Shutdown::new();
    let shared = SharedObjects::default();
    shared.publish("local", Arc::new(catalog.clone()));
    let ctx = ServiceContext::new(
        id,
        shutdown.clone(),
        Arc::new(SystemClock::new()),
        std::path::PathBuf::from("/tmp/thadam-iceberg-test-service-ctx"),
        Arc::new(DiskBudget::new()),
    )
    .with_shared(shared);
    let task = tokio::spawn(async move { service.run(&ctx).await });
    ServiceHandle { shutdown, task }
}

async fn stop_service(handle: ServiceHandle) -> Result<(), thadam_core::ThadamError> {
    handle.shutdown.trigger();
    tokio::time::timeout(TIMEOUT, handle.task)
        .await
        .expect("service join timeout")
        .expect("service task")
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

fn config_json(stack: &Stack, create_table: bool) -> serde_json::Value {
    serde_json::json!({
        "from": "local",
        "pipeline": "edge-capture",
        "catalog_uri": stack.rest_uri,
        "url": format!("s3://{}/lake", stack.bucket),
        "namespace": "robotics",
        "create_table": create_table,
        "store": {
            "endpoint": stack.s3_endpoint,
            "region": "us-east-1",
            "allow_http": true,
            "access_key_id": ROOT_USER,
            "secret_access_key": ROOT_SECRET,
        },
        "retry": { "initial": "100ms", "max": "1500ms", "jitter": true },
    })
}

fn build_registrar(stack: &Stack, create_table: bool) -> Box<dyn thadam_core::Service> {
    use thadam_core::ServiceFactory;
    let factory = IcebergRegistrarFactory::new();
    let ctx = thadam_core::BuildContext::new(
        "iceberg",
        Arc::new(SystemClock::new()),
        std::path::PathBuf::from("/tmp/thadam-iceberg-test-factory-ctx"),
    );
    factory
        .build(config_json(stack, create_table), &ctx)
        .expect("build registrar")
}

fn shipper_config(stack: &Stack) -> serde_json::Value {
    serde_json::json!({
        "from": "local",
        "pipeline": "edge-capture",
        "url": format!("s3://{}/lake", stack.bucket),
        "endpoint": stack.s3_endpoint,
        "region": "us-east-1",
        "allow_http": true,
        "access_key_id": ROOT_USER,
        "secret_access_key": ROOT_SECRET,
        "retry": { "initial": "100ms", "max": "1500ms", "jitter": true },
    })
}

fn build_shipper(stack: &Stack) -> Box<dyn thadam_core::Service> {
    let config =
        thadam_object_store::parse_ship_config(&shipper_config(stack)).expect("ship config");
    Box::new(thadam_object_store::Shipper::new(
        "s3",
        config,
        Arc::new(s3_client(&stack.s3_endpoint, &stack.bucket)),
    ))
}

/// A catalog client for read-side assertions (independent of the registrar).
async fn connect_catalog(stack: &Stack) -> Arc<dyn iceberg::Catalog> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_rest::RestCatalogBuilder;

    let cfg = thadam_iceberg::parse_iceberg_config(&config_json(stack, true)).expect("config");
    let store = thadam_iceberg::build_s3(&cfg.url, &cfg.store).expect("s3 build");
    let io = Arc::new(thadam_iceberg::S3Io::new(
        Arc::new(store),
        stack.bucket.as_str(),
    ));
    let props = std::collections::HashMap::from([
        (
            iceberg_catalog_rest::REST_CATALOG_PROP_URI.to_owned(),
            stack.rest_uri.clone(),
        ),
        (
            iceberg_catalog_rest::REST_CATALOG_PROP_WAREHOUSE.to_owned(),
            format!("s3://{}", stack.bucket),
        ),
    ]);
    let builder = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(thadam_iceberg::S3IoFactory::new(io)))
        .with_runtime(iceberg::Runtime::current());
    let catalog = builder
        .load("thadam-test", props)
        .await
        .expect("rest catalog");
    Arc::new(catalog)
}

fn table_ident() -> iceberg::TableIdent {
    iceberg::TableIdent::new(
        iceberg::NamespaceIdent::from_vec(vec!["robotics".to_owned()]).expect("namespace"),
        DATASET.to_owned(),
    )
}

/// The sum of the `n` column over a full scan (None when the table is
/// missing). u64 sums to i64 here: rows are 0..=24.
async fn sum_column_n(catalog: &Arc<dyn iceberg::Catalog>) -> Option<i64> {
    use arrow_array58::{Array, Int64Array};

    let table = catalog.load_table(&table_ident()).await.ok()?;
    let scan = table.scan().build().expect("scan");
    let mut stream = scan.to_arrow().await.expect("to_arrow");
    let mut sum = 0i64;
    let mut cols = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("batch");
        let col = batch
            .column_by_name("n")
            .expect("column n in the scanned batch")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("n is long");
        assert_eq!(col.null_count(), 0, "n must read back non-null");
        cols += 1;
        for v in col.iter().flatten() {
            sum += v;
        }
    }
    assert!(cols > 0, "at least one batch");
    Some(sum)
}

/// Row count of the table, or None when it does not exist (yet).
async fn table_rows(catalog: &Arc<dyn iceberg::Catalog>) -> Option<u64> {
    let table = catalog.load_table(&table_ident()).await.ok()?;
    let scan = table.scan().build().expect("scan");
    let mut stream = scan.to_arrow().await.expect("to_arrow");
    let mut rows = 0u64;
    while let Some(batch) = stream.next().await {
        rows += batch.expect("batch").num_rows() as u64;
    }
    Some(rows)
}

async fn wait_table_rows(what: &str, catalog: &Arc<dyn iceberg::Catalog>, want: u64) {
    let started = Instant::now();
    loop {
        let got = table_rows(catalog).await;
        if got == Some(want) {
            return;
        }
        assert!(
            started.elapsed() < TIMEOUT,
            "timed out waiting for {what} (rows now: {got:?}, want {want})"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (SeaweedFS + apache/iceberg-rest-fixture)"]
async fn registers_segments_and_survives_registrar_restart() {
    init_test_runtime();
    let stack = Stack::start("warehouse").await;
    let sink = Sink::open().await;
    let rows: u64 = 25;
    write_segment(&sink.catalog, sink.dir.path(), rows).await;

    // 1. Ship the segment.
    let shipper = start_service(build_shipper(&stack), "s3", &sink.catalog);
    wait_until("segment uploaded", || !sink.catalog.uploaded().is_empty()).await;
    stop_service(shipper).await.expect("shipper stops clean");

    // 2. First registrar: creates the table and registers the segment.
    let reader = connect_catalog(&stack).await;
    let registrar = start_service(build_registrar(&stack, true), "iceberg", &sink.catalog);
    wait_table_rows("first registration", &reader, rows).await;
    stop_service(registrar)
        .await
        .expect("registrar stops clean");

    let table = reader.load_table(&table_ident()).await.expect("load table");
    assert_eq!(table.metadata().snapshots().len(), 1, "one append snapshot");

    // The table keeps the file's own Parquet field IDs (client-authored
    // metadata + register; see the crate docs comment above create()).
    // envelope IDs fixed by thadam-arrow format v1 (sample_idx 12 absent:
    // the dataset is not exploded), plus the declared field's configured ID.
    let ids: std::collections::BTreeSet<i32> = table
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.id)
        .collect();
    let expected: std::collections::BTreeSet<i32> = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 1000]
        .into_iter()
        .collect();
    assert_eq!(ids, expected, "Iceberg table field ids");

    // Data reads: the scan maps file field id 1000 straight onto the
    // table's `n` column — values, not just counts.
    assert_eq!(sum_column_n(&reader).await, Some(300), "0+..+24 through n");

    // 3. "Crash/restart": a brand-new registrar process view over the same
    // catalog must NOT append a second data file — the committed key comes
    // back from the table's snapshot summaries.
    let registrar2 = start_service(build_registrar(&stack, true), "iceberg", &sink.catalog);
    // Settle: two poll intervals are enough for a full pass and a skip.
    tokio::time::sleep(Duration::from_secs(8)).await;
    stop_service(registrar2)
        .await
        .expect("second registrar stops clean");

    let table = reader
        .load_table(&table_ident())
        .await
        .expect("reload table");
    assert_eq!(
        table.metadata().snapshots().len(),
        1,
        "a restarted registrar commits nothing twice"
    );
    assert_eq!(
        table_rows(&reader).await,
        Some(rows),
        "row count unchanged after restart"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs Docker (SeaweedFS + apache/iceberg-rest-fixture)"]
async fn existing_table_schema_mismatch_is_a_fatal_error() {
    init_test_runtime();
    let stack = Stack::start("warehouse").await;
    let sink = Sink::open().await;
    write_segment(&sink.catalog, sink.dir.path(), 3).await;

    let shipper = start_service(build_shipper(&stack), "s3", &sink.catalog);
    wait_until("segment uploaded", || !sink.catalog.uploaded().is_empty()).await;
    stop_service(shipper).await.expect("shipper stops clean");

    // Pre-create the table deliberately missing the declared `n` column:
    // the segment's footer has a field (id 1000) the table does not.
    let reader = connect_catalog(&stack).await;
    let ns = iceberg::NamespaceIdent::from_vec(vec!["robotics".to_owned()]).expect("namespace");
    reader
        .create_namespace(&ns, std::collections::HashMap::new())
        .await
        .expect("create namespace");
    let schema = iceberg::spec::Schema::builder()
        .with_fields([std::sync::Arc::new(iceberg::spec::NestedField::required(
            2,
            "_ingest_time",
            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Timestamptz),
        ))])
        .build()
        .expect("schema");
    let creation = iceberg::TableCreation::builder()
        .name(DATASET.to_owned())
        .location(format!(
            "s3://{}/lake/thadam/v1/edge-capture/{DATASET}",
            stack.bucket
        ))
        .schema(schema)
        .build();
    reader
        .create_table(&ns, creation)
        .await
        .expect("pre-create table");

    // create_table: false -> the registrar must refuse the mismatched table
    // with the fatal schema code rather than registering into a bad table.
    let handle = start_service(build_registrar(&stack, false), "iceberg", &sink.catalog);
    let result = stop_service(handle).await;
    let error = result.expect_err("the mismatch must fail the service");
    assert_eq!(
        error.code, "sink.iceberg_schema_mismatch",
        "unexpected error: {error}"
    );
    assert_eq!(error.retry, thadam_core::Retry::Fatal);
}

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
    let mut outer = Sha256::new();
    outer.update(&opad);
    outer.update(inner.finalize());
    outer.finalize().to_vec()
}

fn amz_times() -> (String, String) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = (secs / 86_400) as i64;
    let day_secs = secs % 86_400;
    let (hour, min, sec) = (day_secs / 3600, (day_secs % 3600) / 60, day_secs % 60);
    // Howard Hinnant's civil-from-days, including the day-of-year step the
    // docker_s3 copy this is based on skips (its signatures are never
    // validated against auth-off SeaweedFS; with auth on the date is real).
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

fn hex_bytes(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// SeaweedFS buckets must exist before use; `PUT /{bucket}` with SigV4.
/// Copied from thadam-object-store's docker tests; with S3 auth configured
/// the gateway can still be settling when it first answers (it accepts a
/// connection, then resets it — curl exit 52), so retry briefly.
fn create_bucket(endpoint: &str, bucket: &str, ak: &str, sk: &str) {
    let started = Instant::now();
    loop {
        match put_bucket(endpoint, bucket, ak, sk) {
            Ok(code) if matches!(code.as_str(), "200" | "409") => return,
            Ok(code) => panic!("create bucket unexpected http {code}"),
            Err(e) => {
                assert!(
                    started.elapsed() < Duration::from_secs(60),
                    "create bucket kept failing: {e}"
                );
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

fn put_bucket(endpoint: &str, bucket: &str, ak: &str, sk: &str) -> Result<String, String> {
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
    if !out.status.success() {
        return Err(format!(
            "curl exited {:?}: {}",
            out.status.code(),
            err(&out)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}
