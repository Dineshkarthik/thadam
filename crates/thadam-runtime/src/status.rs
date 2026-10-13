//! Component state machine, counters, and the status snapshot behind
//! [`PipelineHandle`].

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use thadam_core::{
    BudgetCategory, Clock, DeliveryClass, DiskBudget, ThadamError, Shutdown, SyncFlag,
};
use thadam_wal::{CursorState, MaxLag, Wal};
use serde::Serialize;
use tokio::sync::watch;

use crate::metrics::{MetricEvent, MetricsSink};

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Where the run is in its lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Phase {
    /// Built, `run` not called yet.
    Built,
    /// Running the startup recovery sequence.
    Starting,
    /// Every component started; sources are pulling.
    Running,
    /// Shutdown began: intake stopped, sinks draining.
    Draining,
    /// `run` returned.
    Stopped,
}

/// What a component is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ComponentKind {
    /// A source and its driver.
    Source,
    /// A sink's chain task.
    Chain,
    /// A segment sink and its driver.
    SegmentSink,
    /// A batch sink and its driver.
    BatchSink,
    /// A service such as the shipper.
    Service,
    /// The maintenance task.
    Maintenance,
}

/// An error in a status snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ErrorInfo {
    /// The error domain.
    pub domain: String,
    /// The stable code.
    pub code: String,
    /// The message (never payloads or secrets).
    pub message: String,
}

impl From<&ThadamError> for ErrorInfo {
    fn from(e: &ThadamError) -> Self {
        ErrorInfo {
            domain: e.domain.as_str().to_owned(),
            code: e.code.to_owned(),
            message: e.message.clone(),
        }
    }
}

/// Component states (ARCHITECTURE.md section 13).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ComponentState {
    /// Opening or recovering.
    Starting,
    /// Working normally.
    Running,
    /// Retrying after transient errors.
    Degraded {
        /// When it became degraded, in Unix milliseconds.
        since_unix_ms: i64,
        /// The most recent error.
        last_error: ErrorInfo,
    },
    /// Finishing its work during shutdown.
    Draining,
    /// Finished normally.
    Stopped,
    /// Stopped by an error; the WAL keeps its data.
    Failed {
        /// Why.
        error: ErrorInfo,
    },
}

impl ComponentState {
    /// The stable lowercase name used in metrics.
    pub fn name(&self) -> &'static str {
        match self {
            ComponentState::Starting => "starting",
            ComponentState::Running => "running",
            ComponentState::Degraded { .. } => "degraded",
            ComponentState::Draining => "draining",
            ComponentState::Stopped => "stopped",
            ComponentState::Failed { .. } => "failed",
        }
    }
}

/// Counters of one component. Which ones move depends on the kind.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct ComponentCounters {
    /// Messages pulled (sources) or records read (chains).
    pub messages: u64,
    /// Payload bytes pulled or read.
    pub bytes: u64,
    /// Batches submitted, read, appended, or written.
    pub batches: u64,
    /// Events produced (chains) or delivered (sinks).
    pub events: u64,
    /// Records or events rejected.
    pub rejects: u64,
    /// Records no dataset matched (chains). They are not rejects.
    pub unmatched: u64,
    /// Rejects written to quarantine.
    pub quarantined: u64,
    /// Retried operations.
    pub retries: u64,
    /// Source reopens and chain rebuilds.
    pub restarts: u64,
    /// WAL gaps received.
    pub gaps: u64,
    /// Panics caught.
    pub panics: u64,
    /// Batches bisected.
    pub bisects: u64,
    /// Bytes queued between the chain and the sink driver (gauge).
    pub queued_bytes: u64,
    /// Source batches submitted and not yet acknowledged (gauge).
    pub uncommitted_batches: u64,
}

#[derive(Default)]
pub(crate) struct Counters {
    pub messages: AtomicU64,
    pub bytes: AtomicU64,
    pub batches: AtomicU64,
    pub events: AtomicU64,
    pub rejects: AtomicU64,
    pub unmatched: AtomicU64,
    pub quarantined: AtomicU64,
    pub retries: AtomicU64,
    pub restarts: AtomicU64,
    pub gaps: AtomicU64,
    pub panics: AtomicU64,
    pub bisects: AtomicU64,
    pub queued_bytes: AtomicU64,
    pub uncommitted_batches: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> ComponentCounters {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        ComponentCounters {
            messages: g(&self.messages),
            bytes: g(&self.bytes),
            batches: g(&self.batches),
            events: g(&self.events),
            rejects: g(&self.rejects),
            unmatched: g(&self.unmatched),
            quarantined: g(&self.quarantined),
            retries: g(&self.retries),
            restarts: g(&self.restarts),
            gaps: g(&self.gaps),
            panics: g(&self.panics),
            bisects: g(&self.bisects),
            queued_bytes: g(&self.queued_bytes),
            uncommitted_batches: g(&self.uncommitted_batches),
        }
    }
}

pub(crate) fn add(a: &AtomicU64, n: u64) {
    a.fetch_add(n, Ordering::Relaxed);
}

/// One component as the supervisor and its own task see it.
pub(crate) struct Component {
    pub id: String,
    pub kind: ComponentKind,
    pub counters: Counters,
    state: Mutex<ComponentState>,
    delivery: Mutex<Option<DeliveryClass>>,
    oldest_pending: Mutex<Option<Instant>>,
    metrics: Arc<dyn MetricsSink>,
    clock: Arc<dyn Clock>,
}

impl Component {
    pub fn new(
        id: String,
        kind: ComponentKind,
        metrics: Arc<dyn MetricsSink>,
        clock: Arc<dyn Clock>,
    ) -> Arc<Self> {
        Arc::new(Component {
            id,
            kind,
            counters: Counters::default(),
            state: Mutex::new(ComponentState::Starting),
            delivery: Mutex::new(None),
            oldest_pending: Mutex::new(None),
            metrics,
            clock,
        })
    }

    pub fn state(&self) -> ComponentState {
        lock(&self.state).clone()
    }

    fn set(&self, next: ComponentState) {
        let name = next.name();
        let changed = {
            let mut st = lock(&self.state);
            if matches!(*st, ComponentState::Failed { .. }) {
                return;
            }
            let changed = st.name() != name;
            *st = next;
            changed
        };
        if changed {
            self.metrics.record(MetricEvent::ComponentState {
                component: &self.id,
                state: name,
            });
        }
    }

    pub fn running(&self) {
        self.set(ComponentState::Running);
    }

    pub fn draining(&self) {
        self.set(ComponentState::Draining);
    }

    pub fn stopped(&self) {
        self.set(ComponentState::Stopped);
    }

    pub fn degraded(&self, err: &ThadamError) {
        let since = match &*lock(&self.state) {
            ComponentState::Degraded { since_unix_ms, .. } => *since_unix_ms,
            _ => self.clock.now().as_millis(),
        };
        add(&self.counters.retries, 1);
        self.metrics.record(MetricEvent::Retry {
            component: &self.id,
        });
        self.set(ComponentState::Degraded {
            since_unix_ms: since,
            last_error: err.into(),
        });
    }

    pub fn failed(&self, err: &ThadamError) {
        tracing::error!(component = %self.id, error = %err, "component failed");
        self.set(ComponentState::Failed { error: err.into() });
    }

    pub fn is_failed(&self) -> bool {
        matches!(*lock(&self.state), ComponentState::Failed { .. })
    }

    pub fn set_delivery(&self, d: DeliveryClass) {
        *lock(&self.delivery) = Some(d);
    }

    pub fn set_oldest_pending(&self, at: Option<Instant>) {
        *lock(&self.oldest_pending) = at;
    }

    pub fn metrics(&self) -> &dyn MetricsSink {
        self.metrics.as_ref()
    }

    pub fn metrics_arc(&self) -> Arc<dyn MetricsSink> {
        Arc::clone(&self.metrics)
    }

    fn snapshot(&self) -> ComponentStatus {
        ComponentStatus {
            id: self.id.clone(),
            kind: self.kind,
            state: self.state(),
            counters: self.counters.snapshot(),
            delivery: lock(&self.delivery).map(DeliveryClass::as_str),
            oldest_unacked_secs: lock(&self.oldest_pending).map(|t| t.elapsed().as_secs_f64()),
        }
    }
}

/// One component in a [`StatusSnapshot`].
#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct ComponentStatus {
    /// `source:<id>`, `chain:<sink>`, `sink:<id>`, `service:<id>`, or `maintenance`.
    pub id: String,
    /// What it is.
    pub kind: ComponentKind,
    /// Its state.
    pub state: ComponentState,
    /// Its counters.
    pub counters: ComponentCounters,
    /// A batch sink's effective delivery class.
    pub delivery: Option<&'static str>,
    /// Age of the oldest batch submitted and not acknowledged (sources; the
    /// lost-token watchdog reads this).
    pub oldest_unacked_secs: Option<f64>,
}

/// A sink's `max_lag` as configured.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum MaxLagStatus {
    /// WAL bytes between the sink and the head.
    Bytes(u64),
    /// Seconds of age of the oldest data the sink has not consumed.
    AgeSecs(f64),
}

impl From<MaxLag> for MaxLagStatus {
    fn from(lag: MaxLag) -> Self {
        match lag {
            MaxLag::Bytes(b) => MaxLagStatus::Bytes(b),
            MaxLag::Age(a) => MaxLagStatus::AgeSecs(a.as_secs_f64()),
        }
    }
}

/// How a sink takes part in WAL retention, from its configuration.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct SinkPolicy {
    pub required: bool,
    pub max_lag: Option<MaxLagStatus>,
    pub start_from: &'static str,
}

impl Default for SinkPolicy {
    fn default() -> Self {
        SinkPolicy {
            required: true,
            max_lag: None,
            start_from: "earliest",
        }
    }
}

/// A sink's durable progress.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[non_exhaustive]
pub struct CursorStatus {
    /// The sink id.
    pub sink: String,
    /// The WAL cursor name.
    pub cursor: String,
    /// Every frame at or below this is delivered.
    pub committed: u64,
    /// A batch-sink range whose outcome is not yet known.
    pub inflight: Option<(u64, u64)>,
    /// Durable frames not yet committed.
    pub lag_frames: u64,
    /// Whether the WAL keeps data until this sink has consumed it.
    pub required: bool,
    /// The configured lag limit, if any.
    pub max_lag: Option<MaxLagStatus>,
    /// Where the sink starts without a committed cursor: `earliest` or `latest`.
    pub start_from: &'static str,
}

/// The WAL in a [`StatusSnapshot`].
#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct WalStatus {
    /// The epoch, hexadecimal.
    pub epoch: String,
    /// The oldest retained frame.
    pub first_seq: u64,
    /// The last durable frame (0: none).
    pub durable_end: u64,
    /// Retained segment files.
    pub segments: usize,
    /// Bytes held by retained segments.
    pub disk_bytes: u64,
    /// Bytes evicted under `drop_oldest` since open.
    pub evicted_bytes: u64,
    /// Writer thread state.
    pub writer: String,
}

/// One disk budget category.
#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct BudgetStatus {
    /// `wal`, `segments`, or `quarantine`.
    pub category: &'static str,
    /// Bytes in use.
    pub used: u64,
    /// The limit, if any.
    pub limit: Option<u64>,
}

/// A point-in-time view of a run, for `/status`, `/readyz`, and gauges.
#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct StatusSnapshot {
    /// The pipeline name.
    pub pipeline: String,
    /// The node identity (`pipeline.node_id`).
    pub node_id: String,
    /// The thadam version running the pipeline.
    pub thadam_version: &'static str,
    /// The config hash, hexadecimal.
    pub config_hash: String,
    /// The version of the config hash algorithm.
    pub config_hash_version: u32,
    /// The config hash of the run before, when the configuration changed
    /// (`previous_config_hash` in `thadam.json`).
    pub previous_config_hash: Option<String>,
    /// When the run began, Unix milliseconds; `None` before `run` starts.
    pub started_at: Option<i64>,
    /// The lifecycle phase.
    pub phase: Phase,
    /// Readiness: running, nothing starting, nothing failed.
    pub ready: bool,
    /// Every component.
    pub components: Vec<ComponentStatus>,
    /// Every sink cursor.
    pub cursors: Vec<CursorStatus>,
    /// The WAL, while it is open.
    pub wal: Option<WalStatus>,
    /// Disk budgets.
    pub budgets: Vec<BudgetStatus>,
    /// The last recorded clock-sync state.
    pub clock_synced: bool,
}

pub(crate) struct Shared {
    pub pipeline: String,
    pub node_id: String,
    pub config_hash: String,
    pub previous_config_hash: Mutex<Option<String>>,
    pub started_at: Mutex<Option<i64>>,
    pub phase: watch::Sender<Phase>,
    pub components: Mutex<Vec<Arc<Component>>>,
    pub cursors: Mutex<BTreeMap<String, (String, CursorState)>>,
    pub policies: BTreeMap<String, SinkPolicy>,
    pub wal: Mutex<Option<Wal>>,
    pub budget: Mutex<Arc<DiskBudget>>,
    pub sync_flag: SyncFlag,
    pub force: Shutdown,
}

/// A cloneable view of a pipeline: status snapshots, readiness, and the
/// forced-shutdown switch. Obtain it with [`Pipeline::handle`](crate::Pipeline::handle)
/// before calling `run`.
#[derive(Clone)]
pub struct PipelineHandle(pub(crate) Arc<Shared>);

impl std::fmt::Debug for PipelineHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineHandle")
            .field("pipeline", &self.0.pipeline)
            .field("phase", &self.phase())
            .finish()
    }
}

impl PipelineHandle {
    pub(crate) fn new(
        pipeline: String,
        node_id: String,
        config_hash: String,
        sync_flag: SyncFlag,
        policies: BTreeMap<String, SinkPolicy>,
    ) -> Self {
        PipelineHandle(Arc::new(Shared {
            pipeline,
            node_id,
            config_hash,
            previous_config_hash: Mutex::new(None),
            started_at: Mutex::new(None),
            phase: watch::Sender::new(Phase::Built),
            components: Mutex::new(Vec::new()),
            cursors: Mutex::new(BTreeMap::new()),
            policies,
            wal: Mutex::new(None),
            budget: Mutex::new(Arc::new(DiskBudget::new())),
            sync_flag,
            force: Shutdown::new(),
        }))
    }

    /// The current phase.
    pub fn phase(&self) -> Phase {
        *self.0.phase.borrow()
    }

    /// A receiver that observes phase changes.
    pub fn subscribe(&self) -> watch::Receiver<Phase> {
        self.0.phase.subscribe()
    }

    /// Waits until the run reaches `phase` or a later one.
    pub async fn wait_for(&self, phase: Phase) {
        let mut rx = self.subscribe();
        let rank = |p: Phase| match p {
            Phase::Built => 0,
            Phase::Starting => 1,
            Phase::Running => 2,
            Phase::Draining => 3,
            Phase::Stopped => 4,
        };
        let _ = rx.wait_for(|p| rank(*p) >= rank(phase)).await;
    }

    /// Readiness (`/readyz`): running, with no component starting or failed.
    pub fn is_ready(&self) -> bool {
        self.phase() == Phase::Running
            && lock(&self.0.components).iter().all(|c| {
                !matches!(
                    c.state(),
                    ComponentState::Starting | ComponentState::Failed { .. }
                )
            })
    }

    /// Aborts every task now, as on a second signal. `run` returns with
    /// [`ExitStatus::Interrupted`](crate::ExitStatus::Interrupted); the WAL and
    /// recovery cover anything unfinished.
    pub fn force_shutdown(&self) {
        self.0.force.trigger();
    }

    /// A snapshot of components, cursors, WAL, and budgets.
    pub fn status(&self) -> StatusSnapshot {
        let wal = lock(&self.0.wal).clone();
        let durable = wal
            .as_ref()
            .and_then(Wal::durable_end)
            .map_or(0, |p| p.get());
        let cursors = lock(&self.0.cursors)
            .iter()
            .map(|(sink, (cursor, state))| {
                let policy = self.0.policies.get(sink).copied().unwrap_or_default();
                CursorStatus {
                    sink: sink.clone(),
                    cursor: cursor.clone(),
                    committed: state.committed.get(),
                    inflight: state.inflight.map(|r| (r.first().get(), r.last().get())),
                    lag_frames: durable.saturating_sub(state.committed.get()),
                    required: policy.required,
                    max_lag: policy.max_lag,
                    start_from: policy.start_from,
                }
            })
            .collect();
        let wal = wal.map(|w| {
            let s = w.stats();
            WalStatus {
                epoch: format!("{:016x}", s.epoch.get()),
                first_seq: s.first_seq.get(),
                durable_end: s.durable_end.get(),
                segments: s.segments,
                disk_bytes: s.disk_bytes,
                evicted_bytes: s.evicted_bytes,
                writer: format!("{:?}", s.writer).to_lowercase(),
            }
        });
        let budget = Arc::clone(&lock(&self.0.budget));
        let budgets = BudgetCategory::ALL
            .into_iter()
            .map(|c| {
                let u = budget.usage(c);
                BudgetStatus {
                    category: c.as_str(),
                    used: u.used,
                    limit: u.limit,
                }
            })
            .collect();
        StatusSnapshot {
            pipeline: self.0.pipeline.clone(),
            node_id: self.0.node_id.clone(),
            thadam_version: env!("CARGO_PKG_VERSION"),
            config_hash: self.0.config_hash.clone(),
            config_hash_version: thadam_config::CONFIG_HASH_VERSION,
            previous_config_hash: lock(&self.0.previous_config_hash).clone(),
            started_at: *lock(&self.0.started_at),
            phase: self.phase(),
            ready: self.is_ready(),
            components: lock(&self.0.components)
                .iter()
                .map(|c| c.snapshot())
                .collect(),
            cursors,
            wal,
            budgets,
            clock_synced: self.0.sync_flag.get(),
        }
    }

    pub(crate) fn set_phase(&self, phase: Phase) {
        self.0.phase.send_replace(phase);
    }

    pub(crate) fn set_started_at(&self, unix_ms: i64) {
        *lock(&self.0.started_at) = Some(unix_ms);
    }

    pub(crate) fn set_previous_config_hash(&self, previous: Option<String>) {
        *lock(&self.0.previous_config_hash) = previous;
    }

    pub(crate) fn register(&self, c: Arc<Component>) {
        lock(&self.0.components).push(c);
    }

    pub(crate) fn set_cursor(&self, sink: &str, cursor: &str, state: CursorState) {
        lock(&self.0.cursors).insert(sink.to_owned(), (cursor.to_owned(), state));
    }

    pub(crate) fn components(&self) -> Vec<Arc<Component>> {
        lock(&self.0.components).clone()
    }
}
