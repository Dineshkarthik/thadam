//! Pipeline assembly and execution.
//!
//! # Responsibility
//!
//! - The connector registry and build-time validation ([`Registry`],
//!   [`Pipeline::check`], [`Pipeline::build`]), and the read-only look at a
//!   data directory that `thadam validate --against-data-dir` uses
//!   ([`check_data_dir`]).
//! - Drivers: source, chain, segment sink, batch sink, and service tasks.
//! - The supervisor, component state machine, and shutdown tree.
//! - Startup recovery, maintenance, and WAL-range [`replay`].
//! - Hooks for observability: [`PipelineHandle::status`] snapshots and the
//!   [`MetricsSink`] callback. The metrics registry, tracing setup, and admin
//!   HTTP API are PR11 and attach to these; this crate depends on no metrics or
//!   HTTP library.
//!
//! # Deliberately not exposed
//!
//! - Driver internals and channels.
//! - Restart and backoff knobs beyond [`RuntimeOptions`].
//! - Hot reload and multiple pipelines.
//! - A dynamic plugin loader.
//! - A gRPC API (post-MVP).
//!
//! # Task and thread topology
//!
//! ```text
//! per source:   SourceDriver task ──submit──> WAL writer OS thread (thadam-wal)
//!                 └─ select! { next_batch (cancel-safe), FIFO-head CommitTicket }
//! per sink:     ChainTask ──byte-bounded mpsc──> SegmentDriver | BatchDriver
//!                 (WalReader, Chain)              (owns the sink and its quarantine instance)
//! per service:  Service task (the shipper)
//! singletons:   maintenance task; the supervisor is the `run` future (JoinSet)
//! blocking pool: WAL open, reads, cursor commits, retention (inside thadam-wal)
//! ```
//!
//! Connector protocol tasks (for MQTT, the poll loop) belong to the connector.
//! The channel from a chain to its driver holds at most
//! [`RuntimeOptions::chain_channel_batches`] batches and twice
//! [`RuntimeOptions::read_batch_bytes`] bytes; the driver releases the bytes
//! only after the sink took the batch, so a stalled sink stops its chain and
//! nothing else (the WAL keeps capturing within its budget).
//!
//! # Delivery rules the drivers implement
//!
//! - **Ordered acknowledgement.** A source's batches are acknowledged strictly
//!   in receive order after their WAL commit, at most
//!   [`RuntimeOptions::max_uncommitted_batches`] in flight. An empty batch is
//!   acknowledged after the batches before it. On shutdown or a WAL failure
//!   pending handles complete as `Abandoned`, so the peer redelivers.
//! - **Commit, then ship (D18).** A segment driver commits the cursor only to
//!   a watermark the sink reported, then calls `cursor_committed`. Cursor
//!   commits are batched to [`RuntimeOptions::cursor_commit_interval`] and
//!   forced on finalize.
//! - **Same batch id on retry.** A batch driver persists the in-flight range in
//!   the cursor before writing, retries the same `SinkBatch`, re-sends the
//!   in-flight range first after a restart, bisects on frame boundaries after
//!   [`RuntimeOptions::max_attempts_before_bisect`] transient failures (or at
//!   once on a permanent one), and quarantines a single frame only on a
//!   permanent error, never on transient ones (an outage must not turn into
//!   quarantine).
//! - **Quarantine.** Chain rejects (from the primary chain only,
//!   ARCHITECTURE.md O3: the first chain sink in configuration order) and
//!   sink rejects go to that sink's quarantine instance. A sink's cursor never
//!   passes a reject until it is fsynced into the quarantine instance's
//!   staging file; the quarantine Parquet segment is built from staging on its
//!   own rotation cadence and no longer holds the cursor.
//!
//! # `thadam.json` and `wal.json`
//!
//! Decision (closes the PR10 open item in ARCHITECTURE.md section 11 and the
//! version inventory): **identity lives once, in `wal/wal.json`; `thadam.json`
//! references it.**
//!
//! - `wal/wal.json` (owned by `thadam-wal`) keeps the WAL kind, its format
//!   versions (WAL, frame, cursor file), the `epoch`, and the `node_id` and
//!   `machine_id` binding. The runtime passes `pipeline.node_id` and
//!   [`RuntimeOptions::machine_id`] into the WAL options; the WAL refuses an
//!   open with different values. The runtime never copies these fields.
//! - `data_dir/thadam.json` (owned by the runtime, [`ThadamJson`]) keeps the
//!   pipeline name, the config hash (and its algorithm version, plus the
//!   previous hash when the configuration changed), the runtime format version
//!   (this file and the data-directory layout), the cursor layout version (how
//!   sinks map to WAL cursors and what `inflight` means), the catalog format
//!   version expected of `thadam-segments`, and a [`WalBinding`]: the WAL
//!   directory and the epoch the sink state was written against.
//! - The epoch in the binding is a reference, not a second source of truth.
//!   There is a concrete reason to keep it: sink state stores bare WAL
//!   positions (`SegmentSink::recover` returns a `WalPos` without an epoch), so
//!   if `wal/` were replaced while `sinks/` survived, resuming at the old
//!   positions would silently skip new data. A mismatch is refused (exit 3)
//!   unless [`RuntimeOptions::repair`] is set.
//! - A different pipeline name is refused unless repairing. A different config
//!   hash is accepted (restart applies the new configuration from each cursor)
//!   and recorded. A newer format version is always refused.
//!
//! # Contract gaps found while building this crate
//!
//! - Closed in wave 3: `SegmentSink::append` takes a [`thadam_core::SegmentBatch`],
//!   so the segment driver passes each chain read as one batch with events
//!   grouped by dataset (declaration order, empty datasets included) over a
//!   single WAL range. Sink rejects index the events in that order. The
//!   quarantine instance receives its rows under
//!   [`thadam_core::QUARANTINE_DATASET`].
//! - Still open: `BatchSink::write` receives a [`thadam_core::SinkBatch`] with no
//!   dataset id, so the batch driver concatenates the datasets' events.
//!   `thadam-clickhouse` (one table per dataset) needs the same grouping before
//!   PR20.
//! - Closed: each sink takes `required` (default `true`), `max_lag`
//!   (`{ bytes: <size> }` or `{ age: <duration> }`), and `start_from`
//!   (`earliest`, the default, or `latest`). A `required: false` sink is an
//!   optional WAL consumer that never pins retention, so a reader that falls
//!   behind eviction sees a WAL gap as before; `max_lag` bounds how long a
//!   required sink may pin it. `start_from` applies only to a sink with no
//!   committed cursor, and `latest` commits that cursor at the current durable
//!   end during startup so a restart cannot skip data. The optional `datasets`
//!   list in a sink's `config` restricts its chain.
//! - Without a registered quarantine sink, rejects are counted
//!   ([`MetricEvent::RejectsDropped`]) and dropped.
//!
//! # Startup and shutdown
//!
//! Startup: data-directory lock, `thadam.json` checks, WAL open and recovery on
//! the blocking pool, epoch binding, cursors, sink recovery (segment sinks
//! advance the cursor to their recovered watermark when quarantine (staged or
//! durable) already holds every reject up to it; batch sinks re-send the in-flight range first), chain warm start
//! from up to [`RuntimeOptions::warm_start_frames`] frames before the cursor,
//! then maintenance, sinks and chains, services, and sources last.
//!
//! Shutdown (ARCHITECTURE.md section 11): the shutdown token stops intake;
//! sources drain their queues into the WAL, wait for every ticket, acknowledge
//! in order, and close; the WAL writer does its final group commit and writes
//! its clean-shutdown marker; chains drain to the WAL end (bounded by
//! [`RuntimeOptions::drain_timeout`]); drivers finalize, commit cursors, and
//! close sinks; services stop; maintenance stops; the lock is released.
//! [`RuntimeOptions::shutdown_timeout`] after the signal, or on
//! [`PipelineHandle::force_shutdown`], every task is aborted and the exit
//! status is [`ExitStatus::Interrupted`]. A WAL writer failure stops the run
//! with [`ExitStatus::RuntimeFailure`]; reopening re-verifies the WAL.

mod admin;
mod backoff;
mod batch;
mod chain;
mod datadir;
mod exit;
pub mod failpoints;
mod lane;
pub mod logging;
mod metrics;
mod observability;
mod options;
mod pipeline;
mod plan;
mod quarantine;
mod registry;
mod replay;
mod segment;
mod source;
mod staging;
mod status;

pub use admin::{AdminConfig, router, serve, validate_admin_config};
pub use datadir::{
    BIRTHS_DIR, CATALOG_FORMAT_VERSION, CURSOR_LAYOUT_VERSION, RUNTIME_FORMAT_VERSION, THADAM_JSON,
    THADAM_JSON_KIND, ThadamJson, WAL_DIR, WalBinding, births_dir, check_data_dir,
    lock as lock_data_dir, read_thadam_json, schemas_dir,
};
pub use exit::ExitStatus;
pub use failpoints::Failpoints;
pub use logging::{LogFormat, LoggingGuard, init_logging};
pub use metrics::{MetricEvent, MetricsSink, NoopMetrics};
pub use observability::PrometheusMetrics;
pub use options::{BackoffPolicy, ClockSyncProbe, RuntimeOptions};
pub use pipeline::{Pipeline, RecoverySummary, RunReport};
pub use quarantine::{QUARANTINE_RAW_MAX, quarantine_shape};
pub use registry::Registry;
pub use replay::{ReplayOutput, ReplayReport, ReplayRequest, replay};
pub use status::{
    BudgetStatus, ComponentCounters, ComponentKind, ComponentState, ComponentStatus, CursorStatus,
    ErrorInfo, MaxLagStatus, Phase, PipelineHandle, StatusSnapshot, WalStatus,
};
