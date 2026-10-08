//! The shipper task: one service uploading eligible segments from the
//! catalog, oldest first, with bounded concurrency.
//!
//! Per file (plan 5.8):
//!
//! 1. At or below `multipart_threshold`: read into memory, one put. Above:
//!    multipart with `part_size` chunks read sequentially (never more than
//!    `upload_concurrency x part_size` in memory).
//! 2. `conditional_put: auto|create` uses `PutMode::Create` for single puts.
//!    Multipart uploads cannot be conditional, so they pre-HEAD and verify.
//! 3. An AlreadyExists conflict settles by comparing size and `thadam-sha256`
//!    metadata: equal is success (the crash-between-put-and-catalog-record
//!    path), different is the Fatal `OBJECT_KEY_COLLISION`.
//! 4. Failures retry with unbounded jittered backoff; the first-failure age
//!    is logged so a stuck upload stays visible.
//! 5. After an upload is marked, local retention runs (`release_local`).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::{
    Error as StoreError, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
};
use thadam_core::{Classify, ThadamError, ServiceContext, Shutdown};
use thadam_segments::{LocalRetention, SegmentCatalog, SegmentId, SegmentRecord};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::task::{JoinError, JoinSet};

use crate::backoff::Backoff;
use crate::config::{ConditionalPut, RetentionMode, ShipConfig};
use crate::error::ShipError;
use crate::failpoints::{Failpoint, Failpoints};
use crate::keys::{
    Verification, display_key, object_key, put_attributes, sha256_attr, store_path, verify_remote,
};
use crate::metrics::{NoopShipMetrics, ShipMetrics};
use crate::probe::{StoreCapabilities, probe};

/// Wake fallback when the catalog subscription misses an event.
const POLL_IDLE: Duration = Duration::from_secs(5);
/// How often local retention is reconsidered.
const RETENTION_TICK: Duration = Duration::from_secs(30);
/// Above this reported skew, signed requests can be rejected; the warning
/// names `thadam_clock_synced` so operators correlate.
const SKEW_WARN: Duration = Duration::from_secs(300);

/// The resolved put mode after the startup probe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EffectivePut {
    Create,
    Overwrite,
}

/// What the upload loop gets back for one finished segment.
struct Finished {
    record: SegmentRecord,
    display_key: String,
    etag: Option<String>,
    outcome: &'static str,
    latency: Duration,
}

/// A shipper service: uploads eligible segments from one segment sink's
/// catalog to one object store. The factory builds it for S3-compatible
/// stores; tests construct it directly over an in-memory store.
pub struct Shipper {
    id: String,
    config: ShipConfig,
    store: Arc<dyn ObjectStore>,
    metrics: Arc<dyn ShipMetrics>,
    failpoints: Failpoints,
}

impl Shipper {
    /// A shipper over `store` with no metrics and no armed failpoints.
    pub fn new(
        component_id: impl Into<String>,
        config: ShipConfig,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        Shipper {
            id: component_id.into(),
            config,
            store,
            metrics: Arc::new(NoopShipMetrics),
            failpoints: Failpoints::default(),
        }
    }

    /// Plumbs a metrics receiver.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn ShipMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Arms crash failpoints (unit tests and the `failpoints` feature).
    #[cfg(any(test, feature = "failpoints"))]
    #[must_use]
    pub fn with_failpoints(mut self, failpoints: Failpoints) -> Self {
        self.failpoints = failpoints;
        self
    }
}

#[async_trait::async_trait]
impl thadam_core::Service for Shipper {
    async fn run(&mut self, ctx: &ServiceContext) -> Result<(), ThadamError> {
        match self.run_inner(ctx).await {
            Ok(()) | Err(ShipError::Cancelled) => Ok(()),
            Err(e) => Err(ThadamError::from(e)),
        }
    }
}

/// Everything an upload task needs; built once per spawn.
struct UploadCtx {
    store: Arc<dyn ObjectStore>,
    metrics: Arc<dyn ShipMetrics>,
    failpoints: Failpoints,
    root: std::path::PathBuf,
    node_id: String,
    pipeline: String,
    prefix: String,
    put: EffectivePut,
    capabilities: StoreCapabilities,
    retry: crate::config::RetryConfig,
    multipart_threshold: u64,
    part_size: usize,
    started: Instant,
}

impl Shipper {
    async fn run_inner(&mut self, ctx: &ServiceContext) -> Result<(), ShipError> {
        let caps = self.probe_with_retry(ctx).await?;
        tracing::info!(
            conditional_create = caps.conditional_create,
            returns_metadata = caps.returns_metadata,
            clock_skew_ms = caps.clock_skew.map_or(0u64, |d| d.as_millis() as u64),
            "store capabilities probed"
        );
        if let Some(skew) = caps.clock_skew
            && skew > SKEW_WARN
        {
            tracing::warn!(
                skew_seconds = skew.as_secs(),
                "store clock differs from the local clock; signed requests may be rejected until synced (thadam_clock_synced)"
            );
        }
        let effective = resolve_put_mode(self.config.conditional_put, caps)?;

        let catalog = self.await_catalog(ctx).await?;
        let node_id = catalog.node_id().to_owned();

        let mut in_flight: JoinSet<Result<Finished, ShipError>> = JoinSet::new();
        let mut scheduled: HashSet<SegmentId> = HashSet::new();
        let mut retention = tokio::time::interval(RETENTION_TICK);
        retention.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            // Re-resolve the catalog each pass: a re-recovered sink publishes
            // a fresh handle, and the subscription must follow it. A missing
            // handle here is a misconfiguration or a re-recovery in
            // progress; either way waiting is right.
            let Some(catalog) = ctx.shared.get::<SegmentCatalog>(&self.config.from) else {
                tracing::warn!(
                    from = %self.config.from,
                    "the named segment sink no longer publishes a catalog; waiting"
                );
                tokio::select! {
                    () = ctx.shutdown.triggered() => return Ok(()),
                    () = tokio::time::sleep(POLL_IDLE) => continue,
                }
            };
            let mut watch_rx = catalog.subscribe();

            let eligible = catalog.eligible_for_upload();
            self.metrics.backlog(eligible.len());
            for record in eligible {
                if in_flight.len() >= self.config.upload_concurrency {
                    break;
                }
                if !scheduled.insert(record.id.clone()) {
                    continue;
                }
                let uctx = UploadCtx {
                    store: Arc::clone(&self.store),
                    metrics: Arc::clone(&self.metrics),
                    failpoints: self.failpoints.clone(),
                    root: catalog.root().to_path_buf(),
                    node_id: node_id.clone(),
                    pipeline: self.config.pipeline.clone(),
                    prefix: self.config.prefix.clone(),
                    put: effective,
                    capabilities: caps,
                    retry: self.config.retry,
                    multipart_threshold: self.config.multipart_threshold,
                    part_size: self.config.part_size as usize,
                    started: Instant::now(),
                };
                let stop = ctx.shutdown.child();
                in_flight.spawn(async move { upload_with_retry(uctx, record, stop).await });
            }

            tokio::select! {
                () = ctx.shutdown.triggered() => {
                    // Graceful: no new tasks; the running ones see the child
                    // signal and abort multipart uploads at the next part.
                    while let Some(outcome) = in_flight.join_next().await {
                        log_task_exit(outcome);
                    }
                    return Ok(());
                }
                Some(outcome) = in_flight.join_next() => {
                    let finished = self.handle_outcome(outcome, &mut scheduled)?;
                    catalog
                        .mark_uploaded(
                            &finished.record.id,
                            &finished.display_key,
                            finished.etag.as_deref(),
                        )
                        .await?;
                    self.metrics.uploaded(
                        finished.record.bytes,
                        finished.latency,
                        finished.outcome,
                    );
                    self.retention_pass(&catalog).await;
                }
                _ = watch_rx.changed() => {}
                () = tokio::time::sleep(POLL_IDLE) => {}
                _ = retention.tick() => self.retention_pass(&catalog).await,
            }
        }
    }

    /// The catalog for `config.from`, waiting (with warnings) until the
    /// named sink publishes it. Returns `Err(Cancelled)` only on shutdown.
    async fn await_catalog(&self, ctx: &ServiceContext) -> Result<Arc<SegmentCatalog>, ShipError> {
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
                () = ctx.shutdown.triggered() => return Err(ShipError::Cancelled),
                () = tokio::time::sleep(POLL_IDLE) => {}
            }
        }
    }

    /// Runs the startup probe, retrying transient failures forever (the
    /// store may simply be offline) and failing fast on fatal ones.
    async fn probe_with_retry(&self, ctx: &ServiceContext) -> Result<StoreCapabilities, ShipError> {
        let mut backoff = Backoff::new(self.config.retry, seed_of(self.id.as_str()));
        let mut first_failed: Option<Instant> = None;
        loop {
            match probe(self.store.as_ref(), &self.id).await {
                Ok(caps) => return Ok(caps),
                Err(e) => {
                    if e.retry() == thadam_core::Retry::Fatal {
                        return Err(e);
                    }
                    let since = first_failed.get_or_insert_with(Instant::now);
                    let delay = backoff.next_delay();
                    tracing::warn!(
                        error = %e,
                        retry_age_seconds = since.elapsed().as_secs(),
                        "store capability probe failed; retrying"
                    );
                    tokio::select! {
                        () = ctx.shutdown.triggered() => return Err(ShipError::Cancelled),
                        () = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
    }

    /// Handles one task result: removes the segment from the scheduled set,
    /// extracts the `Finished`, and propagates panics and fatal errors.
    fn handle_outcome(
        &self,
        outcome: Result<Result<Finished, ShipError>, JoinError>,
        scheduled: &mut HashSet<SegmentId>,
    ) -> Result<Finished, ShipError> {
        let result = match outcome {
            Ok(result) => result,
            Err(join) => {
                let detail = if join.is_panic() {
                    "upload task panicked"
                } else {
                    "upload task aborted externally"
                };
                return Err(ShipError::Internal(format!("{detail}: {join}")));
            }
        };
        match result {
            Ok(finished) => {
                scheduled.remove(&finished.record.id);
                Ok(finished)
            }
            Err(e) => Err(e),
        }
    }

    /// Applies the configured local-retention policy; failures only log
    /// (a failed deletion retries on the next tick or upload).
    async fn retention_pass(&self, catalog: &SegmentCatalog) {
        let policy = retention_policy(&self.config);
        match catalog.release_local(&policy).await {
            Ok(report) => {
                if report.deleted_files > 0 {
                    self.metrics
                        .released(report.deleted_files, report.deleted_bytes);
                    tracing::info!(
                        files = report.deleted_files,
                        bytes = report.deleted_bytes,
                        "local retention released uploaded segments"
                    );
                }
                if report.pressure {
                    tracing::warn!("segment budget still under pressure after uploaded eviction");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "local retention pass failed; will retry");
            }
        }
    }
}

fn log_task_exit(outcome: Result<Result<Finished, ShipError>, JoinError>) {
    match outcome {
        Ok(Ok(finished)) => {
            tracing::debug!(segment = %finished.record.id, "upload finished during shutdown")
        }
        Ok(Err(ShipError::Cancelled)) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "upload aborted during shutdown"),
        Err(join) => tracing::warn!(error = %join, "upload task did not exit cleanly"),
    }
}

/// The retention policy from configuration, as `thadam-segments` sees it.
fn retention_policy(config: &ShipConfig) -> LocalRetention {
    match config.retention.mode {
        RetentionMode::DeleteAfterUpload => LocalRetention::delete_after_upload(),
        RetentionMode::KeepFor => {
            LocalRetention::keep_for(config.retention.keep_for.unwrap_or(Duration::ZERO))
        }
        RetentionMode::KeepUntilPressure => LocalRetention::keep_until_disk_pressure(),
    }
}

/// What the probe means for `conditional_put`.
fn resolve_put_mode(
    configured: ConditionalPut,
    caps: StoreCapabilities,
) -> Result<EffectivePut, ShipError> {
    match (configured, caps.conditional_create) {
        (ConditionalPut::Auto, true) | (ConditionalPut::Create, true) => Ok(EffectivePut::Create),
        (ConditionalPut::Auto, false) => {
            tracing::warn!(
                "the store ignores If-None-Match; falling back to overwrite uploads \
                 (safe only because finalized files are immutable and keys deterministic)"
            );
            Ok(EffectivePut::Overwrite)
        }
        (ConditionalPut::Create, false) => Err(ShipError::UnsupportedConditional),
        (ConditionalPut::Overwrite, _) => Ok(EffectivePut::Overwrite),
    }
}

/// One file with unbounded jittered retries; returns only on success,
/// fatal failure, or cancellation.
async fn upload_with_retry(
    uctx: UploadCtx,
    record: SegmentRecord,
    stop: Shutdown,
) -> Result<Finished, ShipError> {
    let mut backoff = Backoff::new(uctx.retry, seed_of(record.id.as_str()));
    let mut first_failed: Option<Instant> = None;
    loop {
        if stop.is_triggered() {
            return Err(ShipError::Cancelled);
        }
        match upload_once(&uctx, &record, &stop).await {
            Ok(finished) => return Ok(finished),
            Err(e) => {
                if e.retry() != thadam_core::Retry::Transient {
                    return Err(e);
                }
                let since = first_failed.get_or_insert_with(Instant::now);
                let attempt = backoff.attempt();
                let delay = backoff.next_delay();
                uctx.metrics.retry(&record.id, attempt, since.elapsed());
                let skew_note = if e.to_string().contains("RequestTimeTooSkewed") {
                    " (device clock needs synchronization; see thadam_clock_synced)"
                } else {
                    ""
                };
                tracing::warn!(
                    segment = %record.id,
                    attempt,
                    retry_age_seconds = since.elapsed().as_secs(),
                    delay_ms = delay.as_millis() as u64,
                    error = %e,
                    "upload failed; retrying{skew_note}"
                );
                tokio::select! {
                    () = stop.triggered() => return Err(ShipError::Cancelled),
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
    }
}

/// A single upload attempt for one segment (one conditional put, or one
/// multipart lifecycle).
async fn upload_once(
    uctx: &UploadCtx,
    record: &SegmentRecord,
    stop: &Shutdown,
) -> Result<Finished, ShipError> {
    let key = object_key(&uctx.pipeline, record).ok_or_else(|| ShipError::LocalFile {
        path: format!("{:?}", record.path),
        message: format!("segment {} has no ship-able path", record.id),
    })?;
    let path = store_path(&key);
    let display = display_key(&uctx.prefix, &key);
    let Some(abs) = record.path.as_ref().map(|rel| uctx.root.join(rel)) else {
        return Err(ShipError::Cancelled);
    };

    let (etag, outcome) = if record.bytes <= uctx.multipart_threshold {
        let (body, sha_hex) = read_file(&abs).await?;
        if stop.is_triggered() {
            return Err(ShipError::Cancelled);
        }
        let mode = match uctx.put {
            EffectivePut::Create => PutMode::Create,
            EffectivePut::Overwrite => PutMode::Overwrite,
        };
        let create_mode = matches!(mode, PutMode::Create);
        let opts = PutOptions {
            mode,
            attributes: put_attributes(record, &sha_hex, &uctx.node_id),
            ..PutOptions::default()
        };
        match uctx.store.put_opts(&path, body.into(), opts).await {
            Ok(put) => {
                uctx.failpoints.hit(Failpoint::AfterPutBeforeMark)?;
                (put.e_tag, "put")
            }
            Err(StoreError::AlreadyExists { .. }) if create_mode => {
                let etag = verify_and_finish(uctx, record, &path, &sha_hex).await?;
                (etag, "verified")
            }
            Err(e) => return Err(ShipError::Store(e)),
        }
    } else {
        multipart_upload(uctx, record, &path, &abs, stop).await?
    };

    Ok(Finished {
        record: record.clone(),
        display_key: display,
        etag,
        outcome,
        latency: uctx.started.elapsed(),
    })
}

/// Multipart for files above the threshold: pre-HEAD existence check (the
/// only conditional-write substitute multipart has), `part_size` chunks read
/// sequentially, abort on failure.
async fn multipart_upload(
    uctx: &UploadCtx,
    record: &SegmentRecord,
    path: &object_store::path::Path,
    abs: &std::path::Path,
    stop: &Shutdown,
) -> Result<(Option<String>, &'static str), ShipError> {
    match uctx.store.head(path).await {
        Ok(_) => {
            // Already there: same settlement as an AlreadyExists conflict.
            let sha_hex = local_sha_hex(record, abs).await?;
            let etag = verify_and_finish(uctx, record, path, &sha_hex).await?;
            return Ok((etag, "verified"));
        }
        Err(StoreError::NotFound { .. }) => {}
        Err(e) => return Err(ShipError::Store(e)),
    }

    // S3 attaches user metadata at initiate and carries it onto the object
    // at complete, so the checksum is needed up front: from the catalog
    // record, or from one hash pass for a footer-rebuilt record.
    let init_sha = local_sha_hex(record, abs).await?;
    let multipart_opts = object_store::PutMultipartOptions {
        attributes: put_attributes(record, &init_sha, &uctx.node_id),
        ..Default::default()
    };
    let mut upload = uctx.store.put_multipart_opts(path, multipart_opts).await?;

    let mut file = match tokio::fs::File::open(abs).await {
        Ok(f) => f,
        Err(e) => {
            abort_upload(&mut upload, "local open failure").await;
            return Err(ShipError::LocalFile {
                path: abs.display().to_string(),
                message: e.to_string(),
            });
        }
    };

    let mut hasher = Sha256::new();
    let mut first_part = true;
    loop {
        if stop.is_triggered() {
            abort_upload(&mut upload, "shutdown during multipart").await;
            return Err(ShipError::Cancelled);
        }
        let mut buf = vec![0u8; uctx.part_size];
        let mut filled = 0usize;
        while filled < uctx.part_size {
            match file.read(&mut buf[filled..]).await {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) => {
                    abort_upload(&mut upload, "local read failure").await;
                    return Err(ShipError::LocalFile {
                        path: abs.display().to_string(),
                        message: e.to_string(),
                    });
                }
            }
        }
        if filled == 0 {
            break;
        }
        buf.truncate(filled);
        hasher.update(&buf);
        if let Err(e) = upload.put_part(buf.into()).await {
            let err = ShipError::Store(e);
            abort_upload(&mut upload, "part failure").await;
            return Err(err);
        }
        if first_part {
            first_part = false;
            if let Err(e) = uctx.failpoints.hit(Failpoint::MidMultipart) {
                abort_upload(&mut upload, "crash failpoint").await;
                return Err(e);
            }
        }
    }

    // Immutable files do not change mid-upload; a checksum drift means the
    // file changed on disk, which must stop the pipeline visibly.
    let streamed_sha = hex32(&hasher.finalize());
    if streamed_sha != init_sha {
        abort_upload(&mut upload, "on-disk change during upload").await;
        return Err(ShipError::LocalFile {
            path: abs.display().to_string(),
            message: "file content changed while uploading".to_owned(),
        });
    }

    match upload.complete().await {
        Ok(put) => {
            uctx.failpoints.hit(Failpoint::AfterPutBeforeMark)?;
            Ok((put.e_tag, "multipart"))
        }
        Err(e) => {
            abort_upload(&mut upload, "complete failure").await;
            Err(ShipError::Store(e))
        }
    }
}

async fn abort_upload(upload: &mut Box<dyn object_store::MultipartUpload>, why: &str) {
    if let Err(e) = upload.abort().await {
        tracing::warn!(error = %e, why, "multipart abort failed; a lifecycle rule should clean up");
    }
}

/// The segment's SHA-256: the catalog record's checksum when present
/// (sink-finalized segments always have one), else one hash pass over the
/// file (footer-rebuilt records).
async fn local_sha_hex(record: &SegmentRecord, abs: &std::path::Path) -> Result<String, ShipError> {
    if record.checksum != [0; 32] {
        return Ok(hex32(&record.checksum));
    }
    let (_, sha_hex) = read_file(abs).await?;
    Ok(sha_hex)
}

/// Compares the remote object with the segment and either returns its etag
/// (equal content: success) or raises the Fatal collision.
async fn verify_and_finish(
    uctx: &UploadCtx,
    record: &SegmentRecord,
    path: &object_store::path::Path,
    sha_hex: &str,
) -> Result<Option<String>, ShipError> {
    let meta = uctx.store.head(path).await.map_err(ShipError::Store)?;
    let remote_sha = remote_sha256(uctx, path).await?;
    let verification = verify_remote(
        meta.size,
        remote_sha.as_deref(),
        record.bytes,
        sha_hex,
        uctx.capabilities.returns_metadata,
    );
    match verification {
        Verification::SameContent => {
            uctx.metrics.conflict_verified(true);
            tracing::info!(
                key = %display_key(&uctx.prefix, path.as_ref()),
                "object already exists with identical content (the crash-recovery path); marking uploaded"
            );
            Ok(meta.e_tag)
        }
        Verification::Differs(detail) => {
            uctx.metrics.conflict_verified(false);
            Err(ShipError::Collision {
                key: display_key(&uctx.prefix, path.as_ref()),
                detail,
            })
        }
    }
}

/// The remote `thadam-sha256` user metadata, read with a 1-byte ranged GET
/// (`ObjectMeta` carries no attributes).
async fn remote_sha256(
    uctx: &UploadCtx,
    path: &object_store::path::Path,
) -> Result<Option<String>, ShipError> {
    if !uctx.capabilities.returns_metadata {
        return Ok(None);
    }
    let opts = GetOptions {
        range: Some(GetRange::Bounded(0..1)),
        ..Default::default()
    };
    let result = uctx
        .store
        .get_opts(path, opts)
        .await
        .map_err(ShipError::Store)?;
    Ok(result
        .attributes
        .get(&sha256_attr())
        .map(|v| AsRef::<str>::as_ref(v).to_owned()))
}

/// Reads the whole segment file and its SHA-256.
async fn read_file(abs: &std::path::Path) -> Result<(Vec<u8>, String), ShipError> {
    let body = tokio::fs::read(abs)
        .await
        .map_err(|e| ShipError::LocalFile {
            path: abs.display().to_string(),
            message: e.to_string(),
        })?;
    let mut hasher = Sha256::new();
    hasher.update(&body);
    let digest = hasher.finalize();
    Ok((body, hex32(&digest)))
}

/// Lowercase hex of a digest.
fn hex32(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// A deterministic seed for a component's backoff jitter.
fn seed_of(text: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in text.as_bytes() {
        h = (h ^ u64::from(*b)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    h | 1
}
