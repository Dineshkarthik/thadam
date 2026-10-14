//! The registrar service: watches the segment catalog and appends every
//! uploaded segment to its dataset's Iceberg table, oldest first.
//!
//! Durable order (docs/design/iceberg-append.md): a segment enters the list
//! only after its `Uploaded` record is fsynced in the segment catalog (the
//! shipper has already verified the object). Registration with the Iceberg
//! catalog is idempotent across crashes and retries through the snapshot
//! summary markers (see [`crate::table`]).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use iceberg::Catalog;
use thadam_core::{Classify, Retry, ThadamError, ServiceContext};
use thadam_segments::SegmentCatalog;

use crate::config::{CatalogAuth, IcebergConfig};
use crate::error::{IcebergError, is_expired_oauth_token};
use crate::rest::{RestCommitter, http_client};
use crate::storage::{S3Io, S3IoFactory};
use crate::table::TableHandle;

/// Wake fallback when the catalog subscription misses an event (matches the
/// shipper's).
const POLL_IDLE: Duration = Duration::from_secs(5);

/// The registrar: one Iceberg table per dataset of the named segment sink.
pub struct Registrar {
    id: String,
    config: IcebergConfig,
    storage: S3IoFactory,
}

impl Registrar {
    /// A registrar over `storage` (built by the factory; tests build one
    /// over an in-memory store).
    pub(crate) fn new(id: impl Into<String>, config: IcebergConfig, storage: S3IoFactory) -> Self {
        Registrar {
            id: id.into(),
            config,
            storage,
        }
    }

    async fn run_inner(&mut self, ctx: &ServiceContext) -> Result<(), IcebergError> {
        if self.config.sends_credentials_unsecured() {
            tracing::warn!(
                id = %self.id,
                catalog_uri = %self.config.catalog_uri,
                "Iceberg catalog credentials are sent over plain http://; use https:// outside local setups"
            );
        }
        let (mut catalog_client, mut committer) = self.connect(ctx).await?;
        let catalog = self.await_catalog(ctx).await?;
        let mut watch_rx = catalog.subscribe();
        let mut handles: HashMap<String, TableHandle> = HashMap::new();
        let io = Arc::clone(self.storage.io());
        let mut backoff = Backoff::new(self.config.retry, seed_of(&self.id));
        let mut first_failed: Option<std::time::Instant> = None;

        loop {
            let mut outcome = self
                .run_pass(&catalog_client, &committer, &io, &catalog, &mut handles)
                .await;
            if let Err(ref e) = outcome
                && matches!(self.config.catalog_auth, CatalogAuth::Credential(_))
                && is_expired_oauth_token(e)
            {
                tracing::info!(
                    id = %self.id,
                    "iceberg catalog answered 401; re-exchanging OAuth2 credential and retrying once"
                );
                (catalog_client, committer) = self.connect(ctx).await?;
                handles.clear();
                outcome = self
                    .run_pass(&catalog_client, &committer, &io, &catalog, &mut handles)
                    .await;
            }
            match outcome {
                Ok(()) => {
                    backoff = Backoff::new(self.config.retry, seed_of(&self.id));
                    first_failed = None;
                }
                Err(IcebergError::Cancelled) => return Ok(()),
                Err(e) => {
                    if e.retry() != Retry::Transient {
                        return Err(e);
                    }
                    let since = first_failed.get_or_insert_with(std::time::Instant::now);
                    let delay = backoff.next_delay();
                    tracing::warn!(
                        error = %e,
                        retry_age_seconds = since.elapsed().as_secs(),
                        delay_ms = delay.as_millis() as u64,
                        "iceberg registration failed; retrying"
                    );
                    tokio::select! {
                        () = ctx.shutdown.triggered() => return Ok(()),
                        () = tokio::time::sleep(delay) => continue,
                    }
                }
            }

            tokio::select! {
                () = ctx.shutdown.triggered() => return Ok(()),
                _ = watch_rx.changed() => {}
                () = tokio::time::sleep(POLL_IDLE) => {}
            }
        }
    }

    /// One registration pass over the catalog: every uploaded segment not
    /// yet committed. Ordered oldest-first per dataset. Transient failures
    /// abort the pass (the caller waits and retries); fatal ones end it.
    async fn run_pass(
        &mut self,
        catalog_client: &Arc<dyn Catalog>,
        committer: &RestCommitter,
        io: &Arc<S3Io>,
        catalog: &SegmentCatalog,
        handles: &mut HashMap<String, TableHandle>,
    ) -> Result<(), IcebergError> {
        for segment in catalog.uploaded() {
            let dataset = &segment.record.dataset;
            if !handles.contains_key(dataset) {
                if !table_name_ok(dataset) {
                    return Err(IcebergError::BadTableName {
                        dataset: dataset.clone(),
                    });
                }
                let uri = io.uri_of(&segment.key);
                let handle =
                    TableHandle::open(catalog_client, io, &self.config, dataset, &uri).await?;
                handles.insert(dataset.clone(), handle);
            }
            let handle = handles
                .get_mut(dataset)
                .unwrap_or_else(|| unreachable!("inserted or present: {dataset}"));
            if handle.is_committed(&segment.key) {
                tracing::debug!(
                    dataset = %dataset,
                    segment = %segment.key,
                    "segment already registered"
                );
                continue;
            }
            handle
                .evolve_schema(
                    catalog_client,
                    committer,
                    io,
                    &io.uri_of(&segment.key),
                    &segment.key,
                )
                .await?;
            let wal_range = format!(
                "{:016x}.{:020}.{:020}",
                segment.record.range.epoch().get(),
                segment.record.range.first().get(),
                segment.record.range.last().get(),
            );
            let committed = handle
                .append(
                    catalog_client,
                    &segment.key,
                    segment.record.rows,
                    segment.record.bytes,
                    &wal_range,
                )
                .await?;
            if committed {
                tracing::info!(
                    id = %self.id,
                    dataset = %dataset,
                    segment = %segment.key,
                    rows = segment.record.rows,
                    table = %handle.name,
                    "registered segment in the Iceberg table"
                );
            } else {
                tracing::debug!(
                    dataset = %dataset,
                    segment = %segment.key,
                    "segment already registered"
                );
            }
        }
        Ok(())
    }

    /// Connects to the REST catalog, retrying transient failures forever
    /// (the catalog may simply be down at startup) and failing fast on
    /// fatal ones.
    async fn connect(
        &self,
        ctx: &ServiceContext,
    ) -> Result<(Arc<dyn Catalog>, RestCommitter), IcebergError> {
        use iceberg::CatalogBuilder;
        use iceberg_catalog_rest::RestCatalogBuilder;

        let warehouse = format!("s3://{}", self.config.bucket);
        let identity = self
            .config
            .catalog_client_cert
            .as_deref()
            .zip(self.config.catalog_client_key.as_deref());
        let http = http_client(self.config.catalog_ca_file.as_deref(), identity)?;
        let mut backoff = Backoff::new(self.config.retry, seed_of(&self.id));
        let mut first_failed: Option<std::time::Instant> = None;
        loop {
            let mut props = HashMap::from([
                (
                    iceberg_catalog_rest::REST_CATALOG_PROP_URI.to_owned(),
                    self.config.catalog_uri.clone(),
                ),
                (
                    iceberg_catalog_rest::REST_CATALOG_PROP_WAREHOUSE.to_owned(),
                    warehouse.clone(),
                ),
            ]);
            props.extend(
                catalog_auth_props(&self.config.catalog_auth)
                    .into_iter()
                    .map(|(key, value)| (key.to_owned(), value)),
            );
            let runtime = iceberg::Runtime::try_current().map_err(|e| {
                IcebergError::Internal(format!("no tokio runtime for the iceberg client: {e}"))
            })?;
            let builder = RestCatalogBuilder::default()
                .with_storage_factory(Arc::new(self.storage.clone()))
                .with_client(http.clone())
                .with_runtime(runtime);
            let connected = async {
                let catalog = builder
                    .load("thadam", props)
                    .await
                    .map_err(IcebergError::Catalog)?;
                let committer = RestCommitter::connect(
                    http.clone(),
                    &self.config.catalog_uri,
                    &self.config.catalog_auth,
                )
                .await?;
                Ok::<_, IcebergError>((catalog, committer))
            }
            .await;
            match connected {
                Ok((catalog, committer)) => {
                    let catalog: Arc<dyn Catalog> = Arc::new(catalog);
                    tracing::info!(
                        id = %self.id,
                        uri = %self.config.catalog_uri,
                        warehouse = %warehouse,
                        "connected to the Iceberg REST catalog"
                    );
                    return Ok((catalog, committer));
                }
                Err(error) => {
                    if error.retry() != Retry::Transient {
                        return Err(error);
                    }
                    let since = first_failed.get_or_insert_with(std::time::Instant::now);
                    let delay = backoff.next_delay();
                    tracing::warn!(
                        error = %error,
                        retry_age_seconds = since.elapsed().as_secs(),
                        delay_ms = delay.as_millis() as u64,
                        uri = %self.config.catalog_uri,
                        "iceberg catalog unreachable; retrying"
                    );
                    tokio::select! {
                        () = ctx.shutdown.triggered() => return Err(IcebergError::Cancelled),
                        () = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
    }

    /// The catalog for `config.from`, waiting (with warnings) until the
    /// named sink publishes it — mirrors the shipper's behavior.
    async fn await_catalog(
        &self,
        ctx: &ServiceContext,
    ) -> Result<Arc<SegmentCatalog>, IcebergError> {
        let mut warned = false;
        loop {
            if let Some(catalog) = ctx.shared.get::<SegmentCatalog>(&self.config.from) {
                return Ok(catalog);
            }
            if !warned {
                tracing::warn!(
                    from = %self.config.from,
                    "no segment sink published a catalog under that id; check `from` (services start after sinks recover)"
                );
                warned = true;
            }
            tokio::select! {
                () = ctx.shutdown.triggered() => return Err(IcebergError::Cancelled),
                () = tokio::time::sleep(POLL_IDLE) => {}
            }
        }
    }
}

#[async_trait::async_trait]
impl thadam_core::Service for Registrar {
    async fn run(&mut self, ctx: &ServiceContext) -> Result<(), ThadamError> {
        match self.run_inner(ctx).await {
            Ok(()) | Err(IcebergError::Cancelled) => Ok(()),
            Err(e) => Err(ThadamError::from(e)),
        }
    }
}

/// The `iceberg-catalog-rest` properties that carry the catalog's
/// authentication. The crate runs the client-credentials exchange itself and
/// refuses a `token` and a `credential` together, which config validation
/// already prevents.
fn catalog_auth_props(auth: &CatalogAuth) -> Vec<(&'static str, String)> {
    match auth {
        CatalogAuth::None => Vec::new(),
        CatalogAuth::Token(token) => vec![("token", token.expose().to_owned())],
        CatalogAuth::Credential(credential) => {
            let mut props = vec![
                (
                    "credential",
                    format!(
                        "{}:{}",
                        credential.client_id,
                        credential.client_secret.expose()
                    ),
                ),
                ("oauth2-server-uri", credential.token_url.clone()),
            ];
            if let Some(scope) = &credential.scope {
                props.push(("scope", scope.clone()));
            }
            props
        }
    }
}

/// Dataset names that are usable as Iceberg table names (the common
/// conservative subset; dots are excluded so the table name stays a single
/// identifier).
fn table_name_ok(dataset: &str) -> bool {
    !dataset.is_empty()
        && dataset.len() <= 255
        && dataset
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Copied from thadam-object-store: unbounded, capped, jittered backoff
/// (SplitMix64, deterministic per seed).
#[derive(Clone, Debug)]
struct Backoff {
    initial: Duration,
    max: Duration,
    jitter: bool,
    attempt: u32,
    seed: u64,
}

impl Backoff {
    fn new(retry: crate::config::RetryConfig, seed: u64) -> Self {
        Backoff {
            initial: retry.initial,
            max: retry.max,
            jitter: retry.jitter,
            attempt: 0,
            seed: seed | 1,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let shift = self.attempt.min(20);
        let base = self.initial.saturating_mul(1u32 << shift).min(self.max);
        self.attempt = self.attempt.saturating_add(1);
        if !self.jitter || base.is_zero() {
            return base;
        }
        let base_ns = base.as_nanos() as u64;
        let jittered = base_ns / 2 + (self.next_rand() % (base_ns / 2 + 1));
        Duration::from_nanos(jittered)
    }

    fn next_rand(&mut self) -> u64 {
        self.seed = self.seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// A deterministic seed for a component's backoff jitter (copied from
/// thadam-object-store).
fn seed_of(text: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in text.as_bytes() {
        h = (h ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    h | 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_reject_paths_and_dots() {
        assert!(table_name_ok("telemetry"));
        assert!(table_name_ok("fleet-core_v2"));
        assert!(!table_name_ok("a/b"));
        assert!(!table_name_ok("a.b"));
        assert!(!table_name_ok(""));
        assert!(!table_name_ok("has space"));
    }

    #[test]
    fn backoff_is_capped_jittered_and_seeded() {
        let retry = crate::config::RetryConfig {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(40),
            jitter: true,
        };
        let mut b = Backoff::new(retry, 7);
        let mut b2 = Backoff::new(retry, 7);
        for _ in 0..16 {
            let (d, d2) = (b.next_delay(), b2.next_delay());
            assert!(d <= Duration::from_millis(40), "capped: {d:?}");
            assert_eq!(d, d2, "same seed, same schedule");
        }
        let mut calm = Backoff::new(
            crate::config::RetryConfig {
                jitter: false,
                ..retry
            },
            7,
        );
        assert_eq!(calm.next_delay(), Duration::from_millis(10));
        assert_eq!(calm.next_delay(), Duration::from_millis(20));
        assert_eq!(calm.next_delay(), Duration::from_millis(40));
        assert_eq!(calm.next_delay(), Duration::from_millis(40));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registrar_rebuilds_catalog_once_on_oauth2_401_and_fails_when_retry_still_401() {
        use thadam_core::{
            BudgetCategory, DiskBudget, Retry, Service, SharedObjects, Shutdown, SystemClock,
            WalEpoch, WalPos, WalRange, codes,
        };
        use thadam_segments::{SegmentId, SegmentRecord};
        use std::io::{Read, Write};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let _ = rustls::crypto::ring::default_provider().install_default();
        let seg_dir = tempfile::tempdir().expect("seg dir");
        let root = seg_dir.path().to_owned();
        let (catalog, _) = tokio::task::spawn_blocking(move || {
            SegmentCatalog::open(
                root,
                "edge01",
                None,
                BudgetCategory::Segments,
                &["telemetry".to_owned()],
            )
        })
        .await
        .expect("open task")
        .expect("catalog open");
        let rel = "ready/telemetry/seg1.parquet";
        let abs = seg_dir.path().join(rel);
        std::fs::create_dir_all(abs.parent().expect("parent")).expect("mkdir");
        std::fs::write(&abs, b"parquet").expect("write");
        let record = SegmentRecord::new(
            SegmentId(rel.to_owned()),
            "telemetry",
            Some(std::path::PathBuf::from(rel)),
            WalRange::new(WalEpoch::new(1), WalPos::new(1), WalPos::new(2)).expect("range"),
            2,
            7,
            None,
            None,
            [0u8; 32],
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
        catalog
            .mark_uploaded(&record.id, "lake/seg1.parquet", Some("e1"))
            .await
            .expect("uploaded");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock");
        let addr = listener.local_addr().expect("local addr");
        let token_calls = Arc::new(AtomicUsize::new(0));
        let load_calls = Arc::new(AtomicUsize::new(0));
        let token_calls_srv = Arc::clone(&token_calls);
        let load_calls_srv = Arc::clone(&load_calls);

        let server = std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut stream = stream;
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let mut buf = Vec::new();
                let mut chunk = [0u8; 1024];
                let header_end = loop {
                    let Ok(n) = stream.read(&mut chunk) else {
                        break 0;
                    };
                    if n == 0 {
                        break 0;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };
                if header_end == 0 {
                    continue;
                }
                let headers = String::from_utf8_lossy(&buf[..header_end]).into_owned();
                let req_line = headers.lines().next().unwrap_or_default().to_owned();
                let content_length: usize = headers
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse().ok())?
                    })
                    .unwrap_or(0);
                while buf.len() < header_end + content_length {
                    let Ok(n) = stream.read(&mut chunk) else {
                        break;
                    };
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let respond = |stream: &mut std::net::TcpStream, status: u16, body: &str| {
                    let reason = match status {
                        200 => "OK",
                        401 => "Unauthorized",
                        404 => "Not Found",
                        _ => "Error",
                    };
                    let msg = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(msg.as_bytes());
                    let _ = stream.flush();
                };
                if req_line.starts_with("POST /v1/oauth/tokens ") {
                    let n = token_calls_srv.fetch_add(1, Ordering::SeqCst) + 1;
                    respond(
                        &mut stream,
                        200,
                        &format!(r#"{{"access_token":"tok-{n}","token_type":"bearer"}}"#),
                    );
                } else if req_line.starts_with("GET /v1/config") {
                    respond(&mut stream, 200, r#"{"overrides":{},"defaults":{}}"#);
                } else if req_line.starts_with("GET /v1/namespaces/robotics/tables/telemetry ") {
                    let c = load_calls_srv.fetch_add(1, Ordering::SeqCst) + 1;
                    match c {
                        // Run 1: first load gets 401 -> Registrar refreshes once -> retry gets 404.
                        1 => respond(
                            &mut stream,
                            401,
                            r#"{"error":{"message":"expired token","type":"NotAuthorizedException","code":401}}"#,
                        ),
                        2 => respond(
                            &mut stream,
                            404,
                            r#"{"error":{"message":"missing","type":"NoSuchTableException","code":404}}"#,
                        ),
                        // Run 2: both first load and immediate retry get 401 -> fatal auth.failed.
                        _ => respond(
                            &mut stream,
                            401,
                            r#"{"error":{"message":"still unauthorized","type":"NotAuthorizedException","code":401}}"#,
                        ),
                    }
                }
                if load_calls_srv.load(Ordering::SeqCst) >= 4 {
                    break;
                }
            }
        });

        let config = crate::config::parse_iceberg_config(&serde_json::json!({
            "from": "local",
            "pipeline": "edge-capture",
            "catalog_uri": format!("http://{addr}"),
            "catalog_credential": {
                "client_id": "thadam",
                "client_secret": "s3cret",
            },
            "url": "s3://warehouse/lake",
            "namespace": "robotics",
            "create_table": false,
            "store": {
                "endpoint": "http://127.0.0.1:9000",
                "allow_http": true,
                "access_key_id": "k",
                "secret_access_key": "s",
            },
        }))
        .expect("config");
        let io = Arc::new(S3Io::new(
            Arc::new(object_store::memory::InMemory::new()),
            "warehouse",
        ));
        let storage = S3IoFactory::new(io);
        let shared = SharedObjects::default();
        shared.publish("local", Arc::new(catalog));
        let ctx = ServiceContext::new(
            "iceberg",
            Shutdown::new(),
            Arc::new(SystemClock::new()),
            seg_dir.path().to_path_buf(),
            Arc::new(DiskBudget::new()),
        )
        .with_shared(shared);

        // Run 1: 401 on load_table triggers one OAuth2 re-exchange + retry, reaching 404 TableMissing.
        let mut registrar = Registrar::new("iceberg", config.clone(), storage.clone());
        let err = registrar
            .run(&ctx)
            .await
            .expect_err("table missing after token refresh");
        assert_eq!(err.code, crate::error::CODE_TABLE_MISSING, "{err}");
        assert_eq!(load_calls.load(Ordering::SeqCst), 2);

        // Run 2: persistent 401 after the one OAuth2 re-exchange stops fatally with auth.failed.
        let mut registrar2 = Registrar::new("iceberg", config, storage);
        let err = registrar2
            .run(&ctx)
            .await
            .expect_err("persistent 401 must fail with auth.failed");
        assert_eq!(
            (err.retry, err.code),
            (Retry::Fatal, codes::AUTH_FAILED),
            "{err}"
        );
        assert_eq!(load_calls.load(Ordering::SeqCst), 4);

        server.join().expect("mock server thread");
    }
}
