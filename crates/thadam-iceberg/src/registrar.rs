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

use crate::config::IcebergConfig;
use crate::error::IcebergError;
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
        let catalog_client = self.connect(ctx).await?;
        let catalog = self.await_catalog(ctx).await?;
        let mut watch_rx = catalog.subscribe();
        let mut handles: HashMap<String, TableHandle> = HashMap::new();
        let io = Arc::clone(self.storage.io());
        let mut backoff = Backoff::new(self.config.retry, seed_of(&self.id));
        let mut first_failed: Option<std::time::Instant> = None;

        loop {
            match self
                .run_pass(&catalog_client, &io, &catalog, &mut handles)
                .await
            {
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
    async fn connect(&self, ctx: &ServiceContext) -> Result<Arc<dyn Catalog>, IcebergError> {
        use iceberg::CatalogBuilder;
        use iceberg_catalog_rest::RestCatalogBuilder;

        let warehouse = format!("s3://{}", self.config.bucket);
        let mut backoff = Backoff::new(self.config.retry, seed_of(&self.id));
        let mut first_failed: Option<std::time::Instant> = None;
        loop {
            let props = HashMap::from([
                (
                    iceberg_catalog_rest::REST_CATALOG_PROP_URI.to_owned(),
                    self.config.catalog_uri.clone(),
                ),
                (
                    iceberg_catalog_rest::REST_CATALOG_PROP_WAREHOUSE.to_owned(),
                    warehouse.clone(),
                ),
            ]);
            let runtime = iceberg::Runtime::try_current().map_err(|e| {
                IcebergError::Internal(format!("no tokio runtime for the iceberg client: {e}"))
            })?;
            let builder = RestCatalogBuilder::default()
                .with_storage_factory(Arc::new(self.storage.clone()))
                .with_runtime(runtime);
            match builder.load("thadam", props).await {
                Ok(catalog) => {
                    let catalog: Arc<dyn Catalog> = Arc::new(catalog);
                    tracing::info!(
                        id = %self.id,
                        uri = %self.config.catalog_uri,
                        warehouse = %warehouse,
                        "connected to the Iceberg REST catalog"
                    );
                    return Ok(catalog);
                }
                Err(e) => {
                    let kind = e.kind();
                    let error = IcebergError::Catalog(e);
                    if error.retry() != Retry::Transient {
                        return Err(error);
                    }
                    let since = first_failed.get_or_insert_with(std::time::Instant::now);
                    let delay = backoff.next_delay();
                    tracing::warn!(
                        kind = ?kind,
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
}
