//! Runtime knobs that are not (yet) part of the pipeline configuration.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use thadam_core::{BatchLimits, Clock, SyncFlag, SystemClock};
use thadam_transform::ChainLimits;

use crate::failpoints::Failpoints;
use crate::metrics::{MetricsSink, NoopMetrics};

/// Capped exponential backoff with full jitter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackoffPolicy {
    /// The first delay.
    pub initial: Duration,
    /// The largest delay.
    pub max: Duration,
}

impl BackoffPolicy {
    /// A policy from `initial` doubling up to `max`.
    pub const fn new(initial: Duration, max: Duration) -> Self {
        BackoffPolicy { initial, max }
    }
}

/// Reads the kernel's clock-sync state. `None` means unknown, which leaves the
/// last recorded state in place.
pub type ClockSyncProbe = Arc<dyn Fn() -> Option<bool> + Send + Sync>;

/// How a run behaves beyond what the configuration file says.
///
/// Every field has a default ([`RuntimeOptions::default`]); callers change the
/// ones they need. Values follow ARCHITECTURE.md sections 10 and 11.
#[derive(Clone)]
#[non_exhaustive]
pub struct RuntimeOptions {
    /// Overrides `pipeline.data_dir` from the configuration.
    pub data_dir: Option<PathBuf>,
    /// The directory relative enrichment paths resolve against (the config
    /// file's directory).
    pub config_dir: Option<PathBuf>,
    /// Accept a `thadam.json` mismatch (pipeline name or WAL binding) and
    /// rewrite it. Never accepts a format newer than this binary knows.
    pub repair: bool,
    /// Bound into `wal.json` when the WAL is created; a later open on another
    /// machine is refused.
    pub machine_id: Option<String>,
    /// The clock handed to connectors. Its sync state should follow
    /// [`RuntimeOptions::sync_flag`].
    pub clock: Arc<dyn Clock>,
    /// The flag the maintenance task updates from [`RuntimeOptions::clock_sync_probe`].
    pub sync_flag: SyncFlag,
    /// Reads the clock-sync state; the default reads systemd-timesyncd's
    /// marker file on Linux and reports unknown elsewhere.
    pub clock_sync_probe: Option<ClockSyncProbe>,
    /// Where counters and gauges go (PR11 attaches Prometheus here).
    pub metrics: Arc<dyn MetricsSink>,
    /// From the first shutdown signal to aborting every task (exit 130).
    pub shutdown_timeout: Duration,
    /// How long chains keep draining the WAL after intake stopped.
    pub drain_timeout: Duration,
    /// Shortest interval between durable cursor commits of one sink.
    pub cursor_commit_interval: Duration,
    /// How often segment sinks are polled for finalized files.
    pub poll_commit_interval: Duration,
    /// How often the maintenance task runs retention and reads the clock state.
    pub maintenance_interval: Duration,
    /// Limits for every `next_batch` call.
    pub batch_limits: BatchLimits,
    /// Batches submitted to the WAL and not yet acknowledged, per source.
    pub max_uncommitted_batches: usize,
    /// Bytes of whole frames one chain read takes from the WAL.
    pub read_batch_bytes: usize,
    /// Chain-to-driver channel capacity, in batches. The byte cap is twice
    /// `read_batch_bytes`.
    pub chain_channel_batches: usize,
    /// Backoff before reopening a source after a transient error.
    pub source_restart: BackoffPolicy,
    /// Backoff between retries of a failed sink operation.
    pub sink_retry: BackoffPolicy,
    /// Transient write failures of a multi-frame batch before it is bisected.
    pub max_attempts_before_bisect: u32,
    /// A batch-sink batch closes at this many events.
    pub batch_max_events: usize,
    /// A batch-sink batch closes at this many payload bytes.
    pub batch_max_bytes: usize,
    /// A batch-sink batch closes when its first frame is this old.
    pub batch_max_age: Duration,
    /// Times a frame whose processing panicked is retried before its records
    /// are quarantined.
    pub poison_retries: u32,
    /// Most WAL frames read before a chain's cursor to warm stateful steps.
    pub warm_start_frames: u64,
    /// Limits for every compiled chain.
    pub chain_limits: ChainLimits,
    /// Trigger shutdown once every source has reached end of stream (offline
    /// ingestion and tests). Off by default: live sources never end.
    pub stop_when_sources_finish: bool,
    /// Fault injection for crash tests.
    pub failpoints: Failpoints,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        let sync_flag = SyncFlag::default();
        RuntimeOptions {
            data_dir: None,
            config_dir: None,
            repair: false,
            machine_id: None,
            clock: Arc::new(SystemClock::with_sync_flag(sync_flag.clone())),
            sync_flag,
            clock_sync_probe: Some(Arc::new(default_clock_sync_probe)),
            metrics: Arc::new(NoopMetrics),
            shutdown_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(10),
            cursor_commit_interval: Duration::from_millis(200),
            poll_commit_interval: Duration::from_millis(200),
            maintenance_interval: Duration::from_secs(1),
            batch_limits: BatchLimits::default(),
            max_uncommitted_batches: 64,
            read_batch_bytes: 1 << 20,
            chain_channel_batches: 2,
            source_restart: BackoffPolicy::new(Duration::from_millis(100), Duration::from_secs(30)),
            sink_retry: BackoffPolicy::new(Duration::from_secs(1), Duration::from_secs(60)),
            max_attempts_before_bisect: 5,
            batch_max_events: 10_000,
            batch_max_bytes: 8 << 20,
            batch_max_age: Duration::from_secs(1),
            poison_retries: 2,
            warm_start_frames: 10_000,
            chain_limits: ChainLimits::default(),
            stop_when_sources_finish: false,
            failpoints: Failpoints::default(),
        }
    }
}

impl std::fmt::Debug for RuntimeOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeOptions")
            .field("data_dir", &self.data_dir)
            .field("repair", &self.repair)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("drain_timeout", &self.drain_timeout)
            .finish_non_exhaustive()
    }
}

/// systemd-timesyncd creates this file once the clock is synchronized. Reading
/// the kernel's `adjtimex` state needs `unsafe` (forbidden here) or a crate
/// that wraps it; the probe is replaceable through the options.
fn default_clock_sync_probe() -> Option<bool> {
    if cfg!(target_os = "linux") {
        std::path::Path::new("/run/systemd/timesync/synchronized")
            .exists()
            .then_some(true)
    } else {
        None
    }
}
