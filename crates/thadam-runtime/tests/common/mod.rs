//! Shared fixtures: factories over the testkit memory connectors, a recording
//! segment sink, configuration text, and fast runtime options.

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use thadam_config::{MapEnv, PipelineSpec};
use thadam_core::{
    BatchSink, BatchSinkFactory, BuildContext, ErrorDomain, Event, EventBatch, Reject, ThadamError,
    SegmentSink, SegmentSinkFactory, Service, ServiceContext, ServiceFactory, SinkContext, Source,
    SourceFactory, Value, WalPos, WalRange, codes,
};
use thadam_runtime::{BackoffPolicy, Pipeline, Registry, RunReport, RuntimeOptions};
use thadam_testkit::{
    MemoryBatchSink, MemoryBatchSinkConfig, MemoryBatchSinkHandle, MemorySegmentSink,
    MemorySegmentStore, MemorySource, MemorySourceConfig, MemoryUpstream,
};

pub fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// `{"n": <n>}`.
pub fn json_n(n: u64) -> Bytes {
    Bytes::from(format!("{{\"n\":{n}}}"))
}

/// The value of field `n` in an event.
pub fn event_n(e: &Event) -> Option<u64> {
    match e.payload().as_record()?.get("n")? {
        Value::U64(v) => Some(*v),
        _ => None,
    }
}

/// A shared, ordered log of lifecycle events for order assertions.
#[derive(Clone, Default)]
pub struct EventLog(pub Arc<Mutex<Vec<String>>>);

impl EventLog {
    pub fn push(&self, s: impl Into<String>) {
        lock(&self.0).push(s.into());
    }
    pub fn entries(&self) -> Vec<String> {
        lock(&self.0).clone()
    }
    pub fn index_of(&self, s: &str) -> Option<usize> {
        lock(&self.0).iter().position(|e| e == s)
    }
}

// ---------------------------------------------------------------- sources

/// Hands out pre-built sources by id (`kind: memory`).
#[derive(Clone, Default)]
pub struct SourceSlots(pub Arc<Mutex<HashMap<String, Box<dyn Source>>>>);

impl SourceSlots {
    pub fn put(&self, id: &str, s: impl Source) {
        lock(&self.0).insert(id.to_owned(), Box::new(s));
    }

    /// Puts a fresh memory source under `id` and returns its upstream.
    pub fn memory(&self, id: &str, cfg: MemorySourceConfig) -> MemoryUpstream {
        let (s, up) = MemorySource::new(cfg);
        self.put(id, s);
        up
    }

    /// A finished source over `payloads`.
    pub fn scripted(&self, id: &str, payloads: impl IntoIterator<Item = Bytes>) -> MemoryUpstream {
        let (s, up) = MemorySource::scripted(payloads, MemorySourceConfig::default());
        self.put(id, s);
        up
    }
}

pub struct SlotSourceFactory(pub SourceSlots);

impl SourceFactory for SlotSourceFactory {
    fn kind(&self) -> &'static str {
        "memory"
    }
    fn config_schema(&self) -> serde_json::Value {
        serde_json::json!({"type": "object"})
    }
    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        if cfg.get("bad").is_some() {
            return Err(vec![
                ThadamError::permanent(
                    ErrorDomain::Config,
                    codes::CONFIG_INVALID,
                    "`bad` is not allowed",
                )
                .with_context("path", "/bad"),
            ]);
        }
        Ok(())
    }
    fn build(
        &self,
        _cfg: serde_json::Value,
        ctx: &BuildContext,
    ) -> Result<Box<dyn Source>, ThadamError> {
        lock(&self.0.0)
            .remove(ctx.component_id.as_ref())
            .ok_or_else(|| {
                ThadamError::permanent(
                    ErrorDomain::Config,
                    codes::CONFIG_INVALID,
                    format!("no test source for {}", ctx.component_id),
                )
            })
    }
}

// ------------------------------------------------- testkit segment sink

/// `kind: memseg`: testkit `MemorySegmentSink`s over a store per sink id.
#[derive(Clone, Default)]
pub struct MemSegStores(pub Arc<Mutex<HashMap<String, MemorySegmentStore>>>);

impl MemSegStores {
    pub fn store(&self, id: &str) -> MemorySegmentStore {
        lock(&self.0).entry(id.to_owned()).or_default().clone()
    }
}

pub struct MemSegFactory(pub MemSegStores);

impl SegmentSinkFactory for MemSegFactory {
    fn kind(&self) -> &'static str {
        "memseg"
    }
    fn config_schema(&self) -> serde_json::Value {
        serde_json::json!({"type": "object"})
    }
    fn validate(&self, _cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        Ok(())
    }
    fn build(
        &self,
        cfg: serde_json::Value,
        ctx: &BuildContext,
    ) -> Result<Box<dyn SegmentSink>, ThadamError> {
        let frames = cfg
            .get("frames_per_segment")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(4);
        Ok(Box::new(MemorySegmentSink::new(
            self.0.store(&ctx.component_id),
            frames as usize,
        )))
    }
}

// ----------------------------------------------------- recording sink

/// One delivered event, reduced to what row-set equivalence compares.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Row {
    pub id: u128,
    pub pos: u64,
    pub values: String,
    pub flags: u32,
}

impl Row {
    pub fn of(e: &Event) -> Row {
        let values = match e.payload().as_record() {
            Some(r) => r
                .iter()
                .map(|(k, v)| format!("{k}={v:?}"))
                .collect::<Vec<_>>()
                .join(","),
            None => String::new(),
        };
        Row {
            id: e.id().as_u128(),
            pos: e.wal_pos().get(),
            values,
            flags: e.quality().flags.bits(),
        }
    }
}

#[derive(Default)]
pub struct RecInner {
    /// Finalized "files": range and rows.
    pub files: Vec<(WalRange, Vec<Row>)>,
    /// Ranges made eligible through `cursor_committed`.
    pub eligible: Vec<WalRange>,
    pub log: Option<EventLog>,
    /// Makes `append` stall forever (a stuck sink).
    pub stall: bool,
    /// Makes `append` panic once on a batch containing this `n`.
    pub panic_on_n: Option<u64>,
    /// Makes the next `append` calls fail with this error.
    pub append_errors: Vec<ThadamError>,
    pub appends: u64,
}

/// A durable-in-memory segment sink that keeps every row (`kind: recseg`,
/// also used as the quarantine sink, `kind: recquar`).
#[derive(Clone, Default)]
pub struct RecStore(pub Arc<Mutex<RecInner>>);

impl RecStore {
    pub fn rows(&self) -> Vec<Row> {
        lock(&self.0)
            .files
            .iter()
            .flat_map(|(_, r)| r.clone())
            .collect()
    }
    pub fn ranges(&self) -> Vec<WalRange> {
        lock(&self.0).files.iter().map(|(r, _)| *r).collect()
    }
    pub fn eligible(&self) -> Vec<WalRange> {
        lock(&self.0).eligible.clone()
    }
    pub fn ns(&self) -> Vec<u64> {
        let mut v: Vec<u64> = self
            .rows()
            .iter()
            .filter_map(|r| {
                r.values
                    .split(',')
                    .find_map(|kv| kv.strip_prefix("n=U64("))
                    .and_then(|x| x.trim_end_matches(')').parse().ok())
            })
            .collect();
        v.sort_unstable();
        v
    }
}

pub struct RecSink {
    store: RecStore,
    frames_per_file: u64,
    buffered: Vec<(WalRange, Vec<Row>)>,
    last: Option<WalPos>,
}

impl RecSink {
    pub fn new(store: RecStore, frames_per_file: u64) -> Self {
        RecSink {
            store,
            frames_per_file: frames_per_file.max(1),
            buffered: Vec::new(),
            last: None,
        }
    }

    fn finalize(&mut self) -> Option<WalPos> {
        let (first, last) = (self.buffered.first()?.0, self.buffered.last()?.0);
        let range = WalRange::new(first.epoch(), first.first(), last.last()).unwrap();
        let rows = self.buffered.drain(..).flat_map(|(_, r)| r).collect();
        let mut st = lock(&self.store.0);
        st.files.push((range, rows));
        if let Some(l) = &st.log {
            l.push("sink.finalize");
        }
        Some(range.last())
    }
}

#[async_trait]
impl SegmentSink for RecSink {
    async fn recover(&mut self, _ctx: &SinkContext) -> Result<Option<WalPos>, ThadamError> {
        self.buffered.clear();
        let last = lock(&self.store.0).files.last().map(|(r, _)| r.last());
        self.last = last;
        Ok(last)
    }

    async fn append(&mut self, batch: EventBatch) -> Result<Vec<Reject>, ThadamError> {
        let (stall, panic_n, err) = {
            let mut st = lock(&self.store.0);
            st.appends += 1;
            let err = if st.append_errors.is_empty() {
                None
            } else {
                Some(st.append_errors.remove(0))
            };
            (st.stall, st.panic_on_n, err)
        };
        if stall {
            std::future::pending::<()>().await;
        }
        if let Some(e) = err {
            return Err(e);
        }
        if let Some(n) = panic_n
            && batch.events.iter().any(|e| event_n(e) == Some(n))
        {
            lock(&self.store.0).panic_on_n = None;
            panic!("test sink panics on n = {n}");
        }
        if self.last.is_some_and(|l| batch.origin.first() <= l) {
            return Err(ThadamError::permanent(
                ErrorDomain::Sink,
                codes::SINK_NON_MONOTONIC_APPEND,
                format!("batch {} is not after {:?}", batch.origin, self.last),
            ));
        }
        self.last = Some(batch.origin.last());
        let rows = batch.events.iter().map(Row::of).collect();
        self.buffered.push((batch.origin, rows));
        Ok(Vec::new())
    }

    async fn poll_commit(&mut self) -> Result<Option<WalPos>, ThadamError> {
        let frames: u64 = self.buffered.iter().map(|(r, _)| r.len()).sum();
        if frames < self.frames_per_file {
            return Ok(None);
        }
        Ok(self.finalize())
    }

    async fn finalize_all(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.finalize();
        Ok(lock(&self.store.0).files.last().map(|(r, _)| r.last()))
    }

    fn cursor_committed(&mut self, pos: WalPos) {
        let mut st = lock(&self.store.0);
        let ranges: Vec<WalRange> = st
            .files
            .iter()
            .map(|(r, _)| *r)
            .filter(|r| r.last() <= pos)
            .collect();
        for r in ranges {
            if !st.eligible.contains(&r) {
                st.eligible.push(r);
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct RecStores(pub Arc<Mutex<HashMap<String, RecStore>>>);

impl RecStores {
    pub fn store(&self, id: &str) -> RecStore {
        lock(&self.0).entry(id.to_owned()).or_default().clone()
    }
}

pub struct RecFactory {
    pub kind: &'static str,
    pub stores: RecStores,
}

impl SegmentSinkFactory for RecFactory {
    fn kind(&self) -> &'static str {
        self.kind
    }
    fn config_schema(&self) -> serde_json::Value {
        serde_json::json!({"type": "object"})
    }
    fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        match cfg.get("frames_per_file") {
            Some(v) if !v.is_u64() => Err(vec![ThadamError::permanent(
                ErrorDomain::Config,
                codes::CONFIG_INVALID,
                "frames_per_file must be a number",
            )]),
            _ => Ok(()),
        }
    }
    fn build(
        &self,
        cfg: serde_json::Value,
        ctx: &BuildContext,
    ) -> Result<Box<dyn SegmentSink>, ThadamError> {
        let frames = cfg
            .get("frames_per_file")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(4);
        Ok(Box::new(RecSink::new(
            self.stores.store(&ctx.component_id),
            frames,
        )))
    }
}

// ------------------------------------------------------- batch sinks

/// `kind: membatch`: testkit `MemoryBatchSink` per id.
#[derive(Clone, Default)]
pub struct BatchSlots(pub Arc<Mutex<HashMap<String, Box<dyn BatchSink>>>>);

impl BatchSlots {
    pub fn memory(&self, id: &str, cfg: MemoryBatchSinkConfig) -> MemoryBatchSinkHandle {
        let (s, h) = MemoryBatchSink::new(cfg);
        lock(&self.0).insert(id.to_owned(), Box::new(s));
        h
    }
    pub fn put(&self, id: &str, s: impl BatchSink) {
        lock(&self.0).insert(id.to_owned(), Box::new(s));
    }
}

pub struct BatchSlotFactory(pub BatchSlots);

impl BatchSinkFactory for BatchSlotFactory {
    fn kind(&self) -> &'static str {
        "membatch"
    }
    fn config_schema(&self) -> serde_json::Value {
        serde_json::json!({"type": "object"})
    }
    fn validate(&self, _cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        Ok(())
    }
    fn build(
        &self,
        _cfg: serde_json::Value,
        ctx: &BuildContext,
    ) -> Result<Box<dyn BatchSink>, ThadamError> {
        lock(&self.0.0)
            .remove(ctx.component_id.as_ref())
            .ok_or_else(|| {
                ThadamError::permanent(
                    ErrorDomain::Config,
                    codes::CONFIG_INVALID,
                    "no test batch sink",
                )
            })
    }
}

// ------------------------------------------------------------ services

/// `kind: recservice`: runs until shutdown and logs when it exits.
pub struct LogServiceFactory(pub EventLog);

struct LogService(EventLog);

#[async_trait]
impl Service for LogService {
    async fn run(&mut self, ctx: &ServiceContext) -> Result<(), ThadamError> {
        self.0.push("service.start");
        ctx.shutdown.triggered().await;
        self.0.push("service.exit");
        Ok(())
    }
}

impl ServiceFactory for LogServiceFactory {
    fn kind(&self) -> &'static str {
        "recservice"
    }
    fn config_schema(&self) -> serde_json::Value {
        serde_json::json!({"type": "object"})
    }
    fn validate(&self, _cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>> {
        Ok(())
    }
    fn build(
        &self,
        _cfg: serde_json::Value,
        _ctx: &BuildContext,
    ) -> Result<Box<dyn Service>, ThadamError> {
        Ok(Box::new(LogService(self.0.clone())))
    }
}

// ------------------------------------------------------------- harness

/// Everything a test registers, shared across pipeline builds so "restarts"
/// see the same durable stores.
#[derive(Clone, Default)]
pub struct Kit {
    pub sources: SourceSlots,
    pub memseg: MemSegStores,
    pub rec: RecStores,
    pub quarantine: RecStores,
    pub batch: BatchSlots,
    pub log: EventLog,
}

impl Kit {
    pub fn registry(&self) -> Registry {
        let mut r = Registry::new();
        r.source(Box::new(SlotSourceFactory(self.sources.clone())))
            .segment_sink(Box::new(MemSegFactory(self.memseg.clone())))
            .segment_sink(Box::new(RecFactory {
                kind: "recseg",
                stores: self.rec.clone(),
            }))
            .batch_sink(Box::new(BatchSlotFactory(self.batch.clone())))
            .service(Box::new(LogServiceFactory(self.log.clone())))
            .quarantine_sink(Box::new(RecFactory {
                kind: "recquar",
                stores: self.quarantine.clone(),
            }));
        r
    }

    pub fn quarantine_rows(&self, sink: &str) -> Vec<Row> {
        self.quarantine.store(&format!("quarantine.{sink}")).rows()
    }
}

pub fn spec(yaml: &str) -> PipelineSpec {
    match thadam_config::load_str(yaml, &MapEnv::new()) {
        Ok(s) => s,
        Err(report) => panic!("invalid test config:\n{}", report.render(yaml)),
    }
}

/// One source `src` (kind memory), one dataset `d` with a required `n: u64`,
/// and the given `sinks:` block (YAML list items, already indented).
pub fn yaml(data_dir: &Path, sinks: &str, extra: &str) -> String {
    format!(
        r#"version: 1
pipeline: {{ name: test, node_id: node-1, data_dir: "{dir}" }}
sources:
  - {{ id: src, kind: memory }}
wal: {{ max_disk: 64MiB, segment_bytes: 1MiB, fsync: {{ interval: 1ms }}, on_full: block, retain_for: 1h }}
datasets:
  - id: d
    match: {{ source: src }}
    decode: json
    fields:
      - {{ id: 1000, name: n, type: u64, from: /n, required: true }}
{extra}
sinks:
{sinks}
"#,
        dir = data_dir.display()
    )
}

pub fn fast_opts() -> RuntimeOptions {
    let mut o = RuntimeOptions::default();
    o.poll_commit_interval = Duration::from_millis(10);
    o.cursor_commit_interval = Duration::from_millis(10);
    o.maintenance_interval = Duration::from_millis(50);
    o.drain_timeout = Duration::from_secs(10);
    o.shutdown_timeout = Duration::from_secs(30);
    o.sink_retry = BackoffPolicy::new(Duration::from_millis(5), Duration::from_millis(50));
    o.source_restart = BackoffPolicy::new(Duration::from_millis(5), Duration::from_millis(50));
    o.batch_max_age = Duration::from_millis(20);
    o.clock_sync_probe = None;
    o.batch_limits.linger = Duration::from_millis(1);
    o
}

/// Builds and runs to completion with `stop_when_sources_finish`.
pub async fn run_to_end(kit: &Kit, spec: PipelineSpec, mut opts: RuntimeOptions) -> RunReport {
    opts.stop_when_sources_finish = true;
    let p = Pipeline::build(spec, &kit.registry(), opts).unwrap_or_else(|e| panic!("build: {e:?}"));
    tokio::time::timeout(Duration::from_secs(60), p.run(thadam_core::Shutdown::new()))
        .await
        .expect("run timed out")
        .expect("run failed to start")
}

/// Runs until `done` holds for the status, then triggers shutdown.
pub async fn run_until(
    p: Pipeline,
    mut done: impl FnMut(&thadam_runtime::StatusSnapshot) -> bool,
) -> RunReport {
    let handle = p.handle();
    let shutdown = thadam_core::Shutdown::new();
    let task = tokio::spawn(p.run(shutdown.clone()));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        if done(&handle.status()) || task.is_finished() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not reached: {:#?}",
            handle.status()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    shutdown.trigger();
    tokio::time::timeout(Duration::from_secs(60), task)
        .await
        .expect("shutdown timed out")
        .expect("run task panicked")
        .expect("run failed to start")
}

pub fn component<'a>(
    s: &'a thadam_runtime::StatusSnapshot,
    id: &str,
) -> &'a thadam_runtime::ComponentStatus {
    s.components
        .iter()
        .find(|c| c.id == id)
        .unwrap_or_else(|| panic!("no component {id} in {:#?}", s.components))
}

pub fn cursor(s: &thadam_runtime::StatusSnapshot, sink: &str) -> u64 {
    s.cursors
        .iter()
        .find(|c| c.sink == sink)
        .map_or(0, |c| c.committed)
}

/// The ranges cover `1..=last` exactly once, in order.
pub fn assert_contiguous(ranges: &[WalRange], last: u64) {
    let mut expect = 1;
    for r in ranges {
        assert_eq!(r.first().get(), expect, "ranges {ranges:?}");
        expect = r.last().get() + 1;
    }
    assert_eq!(expect, last + 1, "ranges {ranges:?} do not reach {last}");
}

pub fn counts(ns: &[u64]) -> BTreeMap<u64, usize> {
    let mut m = BTreeMap::new();
    for n in ns {
        *m.entry(*n).or_default() += 1;
    }
    m
}

/// Waits until every component has started (sources open after `Running`).
pub async fn wait_ready(handle: &thadam_runtime::PipelineHandle) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !handle.is_ready() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "never ready: {:#?}",
            handle.status()
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
