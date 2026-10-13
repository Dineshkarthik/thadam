//! `McapSource`: a finite, replayed stream merged out of one or more files.
//!
//! # Replay semantics
//!
//! - **Ordering.** Files are merged by MCAP log time; ties break by sorted
//!   file path, then by file order. Within a file, messages follow file
//!   order (recorders write log-time-ordered streams).
//! - **Ingest time.** With `pace: max` each message's ingest time is its MCAP
//!   log time, so a replay of the same inputs lands byte-identical in the
//!   WAL — the choice that makes replays deterministic. With a paced replay
//!   the ingest time is the wall clock at emission, like a live capture.
//! - **Acknowledgement and resume.** Re-reading a file is not idempotent
//!   for the pipeline: every re-read message lands at a new WAL position,
//!   so its default event id differs and sinks see duplicates. With a data
//!   directory the source acknowledges after the WAL commit
//!   ([`AckMode::OnCommit`]) and, on each ack, durably records per file how
//!   many messages the committed batches cover (see the `resume` module). A
//!   restart skips files that are complete and resumes the others after
//!   their last committed message; batches that were not yet acknowledged
//!   are read again, so no uncommitted message is skipped. A crash between the WAL commit and
//!   the state write re-reads at most the batches in that window
//!   (at-least-once). A file whose size or modification time changed, or a
//!   changed `topics`/`start`/`end`, is read again from the start with a
//!   warning. Without a data directory (direct use) there is no resume and
//!   acks are no-ops ([`AckMode::None`]).
//! - **Finite.** `next_batch` returns `Ok(None)` once every file is
//!   exhausted, and keeps returning `None` until `close`.
//! - **Cancel safety.** Messages a paced batch has already consumed live in
//!   the source's `staged` buffer, not in the in-flight future: dropping a
//!   `next_batch` future mid-sleep leaves them in the source, and the next
//!   call resumes the same batch. The future only *takes* the staged
//!   messages at its synchronous return, never across an await.
//! - **Read errors.** A parse or I/O failure is sticky: staged messages are
//!   flushed as one last batch, then every later `next_batch` returns the
//!   same classified error until `close`.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use thadam_core::{
    AckHandle, AckMode, AckOutcome, AckTarget, AckToken, Attributes, BatchLimits, Clock,
    ConnectionEpoch, EpochGate, ErrorDomain, RawBatch, RawMessage, ThadamError, SchemaStore,
    Shutdown, Source, SourceBatch, SourceCapabilities, SourceContext, SourceId, Timestamp, codes,
};
use thadam_ros2_msg::{CompiledMessage, Registry};

use crate::config::{DecodeMode, McapConfig, Pace};
use crate::error::McapError;
use crate::file_reader::{ChannelInfo, FileReader, Item, SchemaInfo};
use crate::resume::{self, FileState, ResumeStore};

/// The longest a pacing sleep ever waits. Gaps beyond a day in a recording
/// are pathological; this also keeps `tokio::time::sleep` far from overflow.
const MAX_PACE_DELAY: Duration = Duration::from_secs(24 * 60 * 60);

/// Attribute keys written on every message.
pub mod attr {
    /// The normalized topic, also read by `match.topic` filters and
    /// `{topic.N}` identity templates (same value as `mcap.topic`).
    pub const TOPIC: &str = "topic";
    /// The MCAP channel's topic.
    pub const MCAP_TOPIC: &str = "mcap.topic";
    /// The channel's schema name ("" when the channel has no schema).
    pub const MCAP_SCHEMA_NAME: &str = "mcap.schema_name";
    /// The schema encoding, for example `ros2msg` or `jsonschema` ("" when
    /// the channel has no schema).
    pub const MCAP_SCHEMA_ENCODING: &str = "mcap.schema_encoding";
    /// The message encoding, for example `cdr` or `json`.
    pub const MCAP_MESSAGE_ENCODING: &str = "mcap.message_encoding";
    /// The channel id, decimal.
    pub const MCAP_CHANNEL_ID: &str = "mcap.channel_id";
    /// The recorder's sequence number, decimal.
    pub const MCAP_SEQUENCE: &str = "mcap.sequence";
    /// The MCAP log time, nanoseconds since the epoch, decimal.
    pub const MCAP_LOG_TIME: &str = "mcap.log_time";
    /// The MCAP publish time, nanoseconds since the epoch, decimal.
    pub const MCAP_PUBLISH_TIME: &str = "mcap.publish_time";
    /// With `decode: ros2`: the comma-separated flattened field names of the
    /// compiled `ros2msg` shape.
    pub const MCAP_ROS2_FIELDS: &str = "mcap.ros2.fields";
    /// The schema-store hash of the channel's schema, set on every message
    /// of a `ros2msg` channel whose schema text was durably stored (see
    /// [`SchemaStore`](thadam_core::SchemaStore)). The transform chain's
    /// `decode: ros2` resolves the schema text from the store with this key.
    pub const MCAP_SCHEMA_HASH: &str = thadam_core::SCHEMA_HASH_ATTR;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Created,
    Open,
    Closed,
}

/// One heap entry: a message and where it came from. The heap is a min-heap
/// on `(log_time, file, order)`.
struct HeapEntry {
    log_time: u64,
    file: usize,
    order: u64,
    /// The file's `consumed()` count right after this message was read: the
    /// resume position once the message is emitted.
    next_index: u64,
    item: Item,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        (self.log_time, self.file, self.order) == (other.log_time, other.file, other.order)
    }
}
impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.log_time, other.file, other.order).cmp(&(self.log_time, self.file, self.order))
    }
}

/// A compiled `ros2msg` channel, cached after pre-flight.
struct CachedShape {
    #[allow(dead_code)]
    compiled: Arc<CompiledMessage>,
    fields_csv: String,
}

/// The `ros2` decode mode's registry and per-channel compiled shapes.
struct Ros2State {
    registry: Registry,
    /// Keyed by `(file index, channel id)`: ids are per-file.
    channels: HashMap<(usize, u16), CachedShape>,
}

/// Persists the resume state as batches are acknowledged. The runtime
/// completes handles in receive order after the WAL commit, so the state of
/// the newest committed batch covers every earlier one.
struct Checkpoint {
    store: ResumeStore,
    selection: String,
    inner: Mutex<CheckpointInner>,
}

#[derive(Default)]
struct CheckpointInner {
    /// The per-file state at the end of each unacknowledged batch, by batch
    /// number.
    pending: BTreeMap<u64, Vec<FileState>>,
    /// The first failed state write. A failed write only means a restart
    /// re-reads data, but it must not pass silently: `next_batch` and
    /// `close` return it.
    error: Option<ThadamError>,
}

impl Checkpoint {
    fn lock(&self) -> MutexGuard<'_, CheckpointInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl AckTarget for Checkpoint {
    fn ack(&self, _epoch: ConnectionEpoch, token: AckToken, outcome: AckOutcome) {
        let batch = token.a();
        let files = {
            let mut inner = self.lock();
            let files = inner.pending.remove(&batch);
            inner.pending = inner.pending.split_off(&batch);
            files
        };
        let Some(files) = files.filter(|_| outcome == AckOutcome::Committed) else {
            return;
        };
        if let Err(error) = self.store.save(&self.selection, &files) {
            tracing::error!(%error, "cannot persist the MCAP resume state; a restart will re-read data");
            self.lock().error.get_or_insert(error);
        }
    }
}

/// A finite source replaying MCAP files (ADR-0007 stage 2).
///
/// Build with [`McapSource::new`] (or through the factory), then drive it
/// with the [`Source`] methods. Files are opened in `open`, which also reads
/// far enough to prime the merge: a missing file, bad magic, or a schema
/// that fails `ros2` pre-flight fails `open`, not the first batch.
pub struct McapSource {
    config: McapConfig,
    phase: Phase,
    source_id: Option<SourceId>,
    clock: Option<Arc<dyn Clock>>,
    shutdown: Option<Shutdown>,
    files: Vec<FileReader>,
    live: BinaryHeap<HeapEntry>,
    counter: u64,
    /// Log time of the last staged message: the pacing reference.
    prev_log_time: Option<u64>,
    /// Messages consumed toward the batch being built (the connector-side
    /// queue that keeps dropped futures lossless, see the module docs).
    staged: Vec<RawMessage>,
    /// Payload byte total of `staged`.
    staged_bytes: usize,
    ros2: Option<Ros2State>,
    /// The pipeline's durable schema store, when the runtime planned this
    /// source against a data directory.
    schema_store: Option<SchemaStore>,
    /// The store hash of each seen `ros2msg` channel's schema, keyed like
    /// `Ros2State.channels` by `(file index, channel id)`.
    schema_hashes: HashMap<(usize, u16), Arc<str>>,
    /// The source's component directory, where the resume state lives; `None`
    /// disables resume.
    resume_dir: Option<PathBuf>,
    /// Set in `open` when `resume_dir` is.
    checkpoint: Option<Arc<Checkpoint>>,
    /// Per file, parallel to `files`: its identity and how much of it the
    /// messages emitted so far cover.
    progress: Vec<FileState>,
    gate: EpochGate,
    batch_no: u64,
    /// The first read error, returned on every call after the staged flush.
    error: Option<ThadamError>,
    /// Set once every file is exhausted; makes `None` stable.
    exhausted: bool,
}

impl McapSource {
    /// A source for `config`. No file is touched until `open`.
    pub fn new(config: McapConfig) -> Self {
        McapSource {
            config,
            phase: Phase::Created,
            source_id: None,
            clock: None,
            shutdown: None,
            files: Vec::new(),
            live: BinaryHeap::new(),
            counter: 0,
            prev_log_time: None,
            staged: Vec::new(),
            staged_bytes: 0,
            ros2: None,
            schema_store: None,
            schema_hashes: HashMap::new(),
            resume_dir: None,
            checkpoint: None,
            progress: Vec::new(),
            gate: EpochGate::new(),
            batch_no: 0,
            error: None,
            exhausted: false,
        }
    }

    /// Makes the source resumable: it acknowledges after the WAL commit and
    /// keeps its replay position in `dir` (the runtime passes the source's
    /// component directory), so a restart does not re-emit committed
    /// messages. Without it every run reads the files from the start.
    #[must_use]
    pub fn with_resume_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.resume_dir = Some(dir.into());
        self
    }

    /// Sets the schema store `ros2msg` channel schemas are written to
    /// (before the first message that references them), and whose hashes
    /// messages carry as `mcap.schema_hash`. The runtime's factory always
    /// sets one; a source without a store passes schemas through
    /// unreferenced, so a chain `decode: ros2` then rejects affected
    /// messages with `decode.schema_ref_missing`.
    #[must_use]
    pub fn with_schema_store(mut self, store: SchemaStore) -> Self {
        self.schema_store = Some(store);
        self
    }

    /// Expands directories one level deep, keeps `*.mcap`, sorts and
    /// deduplicates. I/O failures are classified `connector.connect_failed`.
    fn expand_paths(paths: &[PathBuf]) -> Result<Vec<PathBuf>, ThadamError> {
        let mut files = Vec::new();
        for path in paths {
            let metadata = std::fs::metadata(path).map_err(|e| McapError::open(path, e))?;
            if metadata.is_dir() {
                let mut inside = Vec::new();
                let entries = std::fs::read_dir(path).map_err(|e| McapError::open(path, e))?;
                for entry in entries {
                    let entry = entry.map_err(|e| McapError::open(path, e))?;
                    let is_file = entry
                        .file_type()
                        .map_err(|e| McapError::open(path, e))?
                        .is_file();
                    let is_mcap = entry
                        .path()
                        .extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("mcap"));
                    if is_file && is_mcap {
                        inside.push(entry.path());
                    }
                }
                inside.sort();
                files.extend(inside);
            } else {
                files.push(path.clone());
            }
        }
        files.sort();
        files.dedup();
        Ok(files)
    }

    /// Drains a file's newly seen channels through schema storing and the
    /// `ros2` pre-flight.
    fn preflight(&mut self, file: usize) -> Result<(), ThadamError> {
        let path = self.files[file].path().to_path_buf();
        for channel in self.files[file].drain_new_channels() {
            self.preflight_channel(file, &channel, &path)?;
        }
        Ok(())
    }

    /// Writes a `ros2msg` channel's schema text to the schema store the
    /// first time the channel is seen, so the write is durable before any
    /// message of the channel can be committed to the WAL (`refill` runs
    /// this before the channel's first message is staged). A write failure
    /// is classified and sticks like any read error.
    fn store_channel_schema(
        &mut self,
        file: usize,
        channel_id: u16,
        schema: &SchemaInfo,
    ) -> Result<(), ThadamError> {
        if self.schema_hashes.contains_key(&(file, channel_id)) {
            return Ok(());
        }
        let Some(store) = &self.schema_store else {
            return Ok(());
        };
        let Ok(text) = std::str::from_utf8(&schema.data) else {
            // Bytes that are not valid `ros2msg` text cannot be stored, so
            // no message can reference them. With `decode: ros2` the
            // pre-flight below reports the same schema as
            // `schema.invalid_shape`; `decode: raw` passes it through
            // unreferenced.
            return Ok(());
        };
        let hash = store.put(&schema.name, &schema.encoding, text)?;
        self.schema_hashes
            .insert((file, channel_id), Arc::from(hash.as_str()));
        Ok(())
    }

    fn preflight_channel(
        &mut self,
        file: usize,
        channel: &Arc<ChannelInfo>,
        path: &Path,
    ) -> Result<(), ThadamError> {
        let Some(schema) = channel.schema.clone() else {
            return Ok(());
        };
        if schema.encoding != "ros2msg" {
            return Ok(());
        }
        self.store_channel_schema(file, channel.id, &schema)?;
        let Some(ros2) = &mut self.ros2 else {
            return Ok(());
        };
        let file_ctx = || path.display().to_string();
        let text = std::str::from_utf8(&schema.data).map_err(|_| {
            ThadamError::permanent(
                ErrorDomain::Schema,
                codes::SCHEMA_INVALID_SHAPE,
                format!("schema `{}` is not valid UTF-8 text", schema.name),
            )
            .with_context("file", file_ctx())
        })?;
        let main = ros2
            .registry
            .register_ros2msg(&schema.name, text)
            .map_err(|e| e.with_context("file", file_ctx()))?;
        let compiled = ros2
            .registry
            .compile(&main.to_string())
            .map_err(|e| e.with_context("file", file_ctx()))?;
        let fields_csv = compiled
            .shape()
            .fields()
            .iter()
            .map(|f| &*f.name)
            .collect::<Vec<_>>()
            .join(",");
        ros2.channels.insert(
            (file, channel.id),
            CachedShape {
                compiled,
                fields_csv,
            },
        );
        Ok(())
    }

    /// Pulls the next filter-passing message from `file` onto the merge heap,
    /// or marks the file exhausted by pushing nothing.
    fn refill(&mut self, file: usize) -> Result<(), ThadamError> {
        loop {
            let item = self.files[file].next_item()?;
            self.preflight(file)?;
            let Some(item) = item else {
                self.progress[file].done = true;
                return Ok(());
            };
            if !self.config.topics.matches(&item.channel.topic) {
                continue;
            }
            if !self.within_time_bounds(item.log_time) {
                continue;
            }
            self.counter += 1;
            let order = self.counter;
            let entry = HeapEntry {
                log_time: item.log_time,
                file,
                order,
                next_index: self.files[file].consumed(),
                item,
            };
            self.live.push(entry);
            return Ok(());
        }
    }

    fn within_time_bounds(&self, log_time: u64) -> bool {
        let as_u64 = |t: Timestamp| u64::try_from(t.as_nanos().max(0)).unwrap_or(u64::MAX);
        if let Some(start) = self.config.start
            && log_time < as_u64(start.0)
        {
            return false;
        }
        if let Some(end) = self.config.end
            && log_time > as_u64(end.0)
        {
            return false;
        }
        true
    }

    /// The wait before the next message, from the gap to the previously
    /// *staged* message. The first message of a run never waits.
    fn pace_delay(&self, log_time: u64) -> Duration {
        let Some(prev) = self.prev_log_time else {
            return Duration::ZERO;
        };
        let gap = log_time.saturating_sub(prev);
        let delay = match self.config.pace {
            Pace::Max => return Duration::ZERO,
            Pace::Realtime => Duration::from_nanos(gap),
            Pace::Factor(factor) => {
                let scaled = gap as f64 / factor;
                if !scaled.is_finite() || scaled >= u64::MAX as f64 {
                    MAX_PACE_DELAY
                } else {
                    Duration::from_nanos(scaled as u64)
                }
            }
        };
        delay.min(MAX_PACE_DELAY)
    }

    fn raw_message(&self, file: usize, item: Item) -> RawMessage {
        // With `pace: max` the ingest time is the message's log time, making
        // replay deterministic (see the module docs). Paced replays stamp
        // the wall clock at emission, like a live capture.
        let ingest_time = match self.config.pace {
            Pace::Max => Timestamp::from_nanos(i64::try_from(item.log_time).unwrap_or(i64::MAX)),
            _ => self
                .clock
                .as_ref()
                .map(|clock| clock.now())
                .unwrap_or(Timestamp::EPOCH),
        };
        let channel = &item.channel;
        let mut attrs = Attributes::new();
        attrs.insert(attr::TOPIC, channel.topic.clone());
        attrs.insert(attr::MCAP_TOPIC, channel.topic.clone());
        match &channel.schema {
            Some(schema) => {
                attrs.insert(attr::MCAP_SCHEMA_NAME, schema.name.clone());
                attrs.insert(attr::MCAP_SCHEMA_ENCODING, schema.encoding.clone());
            }
            None => {
                attrs.insert(attr::MCAP_SCHEMA_NAME, "");
                attrs.insert(attr::MCAP_SCHEMA_ENCODING, "");
            }
        }
        attrs.insert(
            attr::MCAP_MESSAGE_ENCODING,
            channel.message_encoding.clone(),
        );
        if let Some(hash) = self.schema_hashes.get(&(file, channel.id)) {
            attrs.insert(attr::MCAP_SCHEMA_HASH, hash.as_ref());
        }
        attrs.insert(attr::MCAP_CHANNEL_ID, channel.id.to_string());
        attrs.insert(attr::MCAP_SEQUENCE, item.sequence.to_string());
        attrs.insert(attr::MCAP_LOG_TIME, item.log_time.to_string());
        attrs.insert(attr::MCAP_PUBLISH_TIME, item.publish_time.to_string());
        if let Some(shape) = self
            .ros2
            .as_ref()
            .and_then(|r| r.channels.get(&(file, channel.id)))
        {
            attrs.insert(attr::MCAP_ROS2_FIELDS, shape.fields_csv.clone());
        }
        RawMessage {
            ingest_time,
            attrs,
            payload: item.payload,
        }
    }

    /// Empties the staging buffer into a batch (the synchronous commit point
    /// of `next_batch`).
    fn take_staged(&mut self, source_id: &SourceId, clock: &Arc<dyn Clock>) -> SourceBatch {
        debug_assert!(!self.staged.is_empty());
        let messages = std::mem::take(&mut self.staged);
        self.staged_bytes = 0;
        let ack = match &self.checkpoint {
            Some(checkpoint) => {
                self.batch_no += 1;
                checkpoint
                    .lock()
                    .pending
                    .insert(self.batch_no, self.progress.clone());
                self.gate
                    .issue(AckToken::new(self.batch_no, 0), checkpoint.clone())
            }
            None => AckHandle::noop(),
        };
        SourceBatch {
            batch: RawBatch::new(source_id.clone(), messages, clock.synced()),
            ack,
        }
    }

    /// The first failed resume-state write, if any.
    fn checkpoint_error(&self) -> Option<ThadamError> {
        self.checkpoint.as_ref()?.lock().error.clone()
    }

    /// What identifies the replay selection for resume: files are only
    /// skipped when the messages they were skipped for would be selected
    /// again.
    fn selection(&self) -> String {
        let nanos = |bound: Option<crate::config::TimeBound>| bound.map(|b| b.0.as_nanos());
        serde_json::json!({
            "topics": self.config.topics.0,
            "start": nanos(self.config.start),
            "end": nanos(self.config.end),
        })
        .to_string()
    }
}

#[async_trait]
impl Source for McapSource {
    async fn open(&mut self, ctx: &SourceContext) -> Result<(), ThadamError> {
        if self.phase != Phase::Created {
            return Err(McapError::State("the MCAP source is already open or closed").into());
        }
        let files = Self::expand_paths(&self.config.paths)?;
        if files.is_empty() {
            return Err(McapError::Config(
                "no *.mcap files found under the configured `paths`".into(),
            )
            .into());
        }
        self.source_id = Some(ctx.source_id.clone());
        self.clock = Some(Arc::clone(&ctx.clock));
        self.shutdown = Some(ctx.shutdown.clone());
        if self.config.decode == DecodeMode::Ros2 {
            self.ros2 = Some(Ros2State {
                registry: Registry::new(),
                channels: HashMap::new(),
            });
        }
        let selection = self.selection();
        let mut progress = files
            .iter()
            .map(|path| FileState::fresh(path))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(dir) = &self.resume_dir {
            let store = ResumeStore::new(dir);
            progress = resume::plan(store.load()?, &selection, progress);
            self.checkpoint = Some(Arc::new(Checkpoint {
                store,
                selection,
                inner: Mutex::new(CheckpointInner::default()),
            }));
        }
        self.progress = progress;
        for path in files {
            self.files.push(FileReader::start(path)?);
        }
        // Prime the merge: one filter-passing message per live file, after
        // the position the resume state saved. This also surfaces unreadable
        // headers and failing `ros2` schemas here, where configuration
        // errors belong. Files the state marks complete are not read.
        for file in 0..self.files.len() {
            if self.progress[file].done {
                continue;
            }
            self.files[file].skip(self.progress[file].next_index)?;
            self.refill(file)?;
        }
        self.exhausted = self.live.is_empty();
        self.phase = Phase::Open;
        Ok(())
    }

    async fn next_batch(&mut self, limits: BatchLimits) -> Result<Option<SourceBatch>, ThadamError> {
        match self.phase {
            Phase::Created => return Err(McapError::State("next_batch called before open").into()),
            Phase::Closed => return Ok(None),
            Phase::Open => {}
        }
        let (source_id, clock, shutdown) = match (
            self.source_id.clone(),
            self.clock.clone(),
            self.shutdown.clone(),
        ) {
            (Some(id), Some(clock), Some(shutdown)) => (id, clock, shutdown),
            _ => return Err(McapError::State("next_batch called before open").into()),
        };
        // A failed resume-state write stops the replay: staged messages are
        // unacknowledged, so the next run reads them again.
        if let Some(error) = self.checkpoint_error() {
            return Err(error);
        }
        // A sticky read error lets the staged remainder drain first, then
        // blocks every later call.
        if let Some(error) = &self.error {
            if self.staged.is_empty() {
                return Err(error.clone());
            }
            return Ok(Some(self.take_staged(&source_id, &clock)));
        }

        let paced = self.config.pace != Pace::Max;
        // Linger bounds how long a *paced* batch may take to fill once it
        // holds a message. With `pace: max` nothing ever waits, so the batch
        // fills to the limits immediately.
        let deadline = tokio::time::Instant::now() + limits.linger;

        loop {
            if self.staged.len() >= limits.max_messages || self.staged_bytes >= limits.max_bytes {
                break;
            }
            let Some(top) = self.live.peek() else {
                break;
            };
            let delay = self.pace_delay(top.log_time);
            if paced && !self.staged.is_empty() && tokio::time::Instant::now() + delay > deadline {
                break;
            }
            if delay > Duration::ZERO {
                // The staged messages stay in the source across this await;
                // dropping the future here loses nothing.
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = shutdown.triggered() => break,
                }
            }
            let Some(entry) = self.live.pop() else { break };
            let file = entry.file;
            self.progress[file].next_index = entry.next_index;
            self.prev_log_time = Some(entry.log_time);
            self.staged_bytes += entry.item.payload.len();
            self.staged.push(self.raw_message(file, entry.item));
            // Refill after the pop. On error the staged messages are still
            // returned below — nothing popped is lost — and the sticky error
            // fails every call after.
            if let Err(error) = self.refill(file) {
                self.error = Some(error);
                break;
            }
        }

        if self.staged.is_empty() {
            if let Some(error) = &self.error {
                return Err(error.clone());
            }
            // Every file is drained, or shutdown arrived before the first
            // paced message: the stream is over.
            self.exhausted = self.live.is_empty() || shutdown.is_triggered();
            return Ok(None);
        }
        Ok(Some(self.take_staged(&source_id, &clock)))
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        if self.phase == Phase::Closed {
            return Ok(());
        }
        self.phase = Phase::Closed;
        self.files.clear();
        self.live.clear();
        self.staged.clear();
        self.staged_bytes = 0;
        match self.checkpoint_error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn capabilities(&self) -> SourceCapabilities {
        // With a resume directory the ack is what advances the saved
        // position, after the WAL commit; without one there is nothing to
        // acknowledge and batches carry no-op handles.
        SourceCapabilities::new(if self.resume_dir.is_some() {
            AckMode::OnCommit
        } else {
            AckMode::None
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_is_a_min_heap_on_log_time_then_file_then_order() {
        let mut heap = BinaryHeap::new();
        let entry = |log_time: u64, file: usize, order: u64| HeapEntry {
            log_time,
            file,
            order,
            next_index: 0,
            item: Item {
                channel: Arc::new(ChannelInfo {
                    id: 1,
                    topic: "/t".into(),
                    message_encoding: "cdr".into(),
                    schema: None,
                }),
                sequence: 0,
                log_time,
                publish_time: 0,
                payload: bytes::Bytes::new(),
            },
        };
        for (log_time, file, order) in [(30, 1, 3), (10, 0, 1), (10, 1, 2), (20, 0, 4)] {
            heap.push(entry(log_time, file, order));
        }
        let popped: Vec<(u64, usize, u64)> = (0..4)
            .map(|_| {
                let e = heap.pop().expect("entry");
                (e.log_time, e.file, e.order)
            })
            .collect();
        assert_eq!(popped, vec![(10, 0, 1), (10, 1, 2), (20, 0, 4), (30, 1, 3)]);
    }

    #[test]
    fn pace_delays_follow_emitted_gaps() {
        let config = McapConfig {
            pace: Pace::Factor(2.0),
            ..McapConfig::default()
        };
        let mut source = McapSource::new(config);
        assert_eq!(source.pace_delay(100), Duration::ZERO);
        source.prev_log_time = Some(100);
        assert_eq!(source.pace_delay(300), Duration::from_nanos(100));
        // Out-of-order within a file never sleeps negative.
        assert_eq!(source.pace_delay(50), Duration::ZERO);

        source.config.pace = Pace::Realtime;
        assert_eq!(
            source.pace_delay(1_000_000_100),
            Duration::from_nanos(1_000_000_000)
        );

        source.config.pace = Pace::Max;
        assert_eq!(source.pace_delay(u64::MAX), Duration::ZERO);
    }
}
