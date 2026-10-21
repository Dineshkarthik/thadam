//! kill -9 restart test: no acknowledged loss through source -> WAL -> chain ->
//! segment sink.
//!
//! The parent re-executes this test binary as a child running
//! `runtime_crash_child`, a full [`Pipeline`] over two file-backed connectors:
//!
//! - `filebroker` (source): replays ids `1..=N` from `broker/messages`, minus
//!   the ids in `broker/acked.log`. It appends a batch's ids to that log (and
//!   `fsync`s it) only when the runtime reports the batch `Committed`, then
//!   prints `@@ACK <max id>`. The log is the "broker's" durable record of what
//!   the runtime acknowledged.
//! - `filesink` (segment sink): every three WAL frames it durably writes
//!   `seg-{first}-{last}.json` (tmp file, `fsync`, rename, directory `fsync`).
//!
//! The child prints `@@READY` once the pipeline is running. The parent anchors
//! every kill to that signal (and optionally to the first `@@ACK`), never to
//! the spawn time, so load cannot turn each kill into a startup kill. After
//! each SIGKILL the parent opens the WAL in-process and checks that every id in
//! `acked.log` is in it. A final clean run (`THADAM_RT_CRASH_FINAL=1`) must end
//! by itself with exit code 0, and then:
//!
//! - the segment files hold every id `1..=N` (duplicates are allowed: ids
//!   that were committed to the WAL but not acknowledged are redelivered);
//! - the segment files' WAL ranges are contiguous with no gap or overlap;
//! - `acked.log` holds every id `1..=N`.
//!
//! Iterations: 4 by default; set `THADAM_RUNTIME_CRASH_ITERS` for more.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use common::{fast_opts, lock, spec};
use thadam_core::{
    AckHandle, AckOutcome, AckTarget, AckToken, BatchLimits, BuildContext, Clock, ConnectionEpoch,
    DiskBudget, EpochGate, ErrorDomain, RawBatch, RawMessage, Reject, SegmentBatch, SegmentSink,
    SegmentSinkFactory, Shutdown, SinkContext, Source, SourceBatch, SourceContext, SourceFactory,
    SourceId, SystemClock, ThadamError, Value, WalPos, WalRange, codes,
};
use thadam_runtime::{Phase, Pipeline, Registry};
use thadam_testkit::SplitMix64;
use thadam_wal::{ConsumerSpec, Gap, OnFull, ReadItem, StartFrom, WalOptions};

const DIR_ENV: &str = "THADAM_RT_CRASH_DIR";
const FINAL_ENV: &str = "THADAM_RT_CRASH_FINAL";
const ITERS_ENV: &str = "THADAM_RUNTIME_CRASH_ITERS";
const DEFAULT_ITERS: u64 = 4;

/// Ids `1..=N` are published to the broker.
const N: u64 = 4000;
/// Frames the sink buffers before it finalizes a segment file.
const FRAMES_PER_FILE: u64 = 3;
const NODE_ID: &str = "node-1";
const SINK_ID: &str = "out";

/// Generous upper bound for the child to start (or finish the final run). It
/// only bounds a hung child; a kill waits for the child's signal, never for a
/// fixed delay from spawn.
const READY_TIMEOUT: Duration = Duration::from_secs(120);

// ------------------------------------------------------------ file helpers

/// The bytes up to and including the last `\n`: a torn last line is dropped.
fn complete_lines(bytes: &[u8]) -> &[u8] {
    bytes
        .iter()
        .rposition(|b| *b == b'\n')
        .map_or(&[][..], |at| &bytes[..=at])
}

fn parse_ids(bytes: &[u8]) -> Result<Vec<u64>, String> {
    let text = std::str::from_utf8(complete_lines(bytes)).map_err(|e| e.to_string())?;
    text.lines()
        .map(|l| {
            l.trim()
                .parse()
                .map_err(|e| format!("bad id line {l:?}: {e}"))
        })
        .collect()
}

fn read_ids(path: &Path) -> Result<Vec<u64>, String> {
    match fs::read(path) {
        Ok(bytes) => parse_ids(&bytes),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(format!("read {}: {e}", path.display())),
    }
}

fn segment_name(first: u64, last: u64) -> String {
    format!("seg-{first:020}-{last:020}.json")
}

/// Finalized segment files as `(first, last, path)`, ordered by position.
fn segment_files(dir: &Path) -> io::Result<Vec<(u64, u64, PathBuf)>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(rest) = name
            .strip_prefix("seg-")
            .and_then(|r| r.strip_suffix(".json"))
        else {
            continue;
        };
        let Some((first, last)) = rest.split_once('-') else {
            continue;
        };
        if let (Ok(first), Ok(last)) = (first.parse(), last.parse()) {
            files.push((first, last, entry.path()));
        }
    }
    files.sort();
    Ok(files)
}

fn sink_io(what: &str, e: &io::Error) -> ThadamError {
    ThadamError::fatal(ErrorDomain::Sink, codes::SINK_IO, format!("{what}: {e}"))
}

// ------------------------------------------------------------ file broker

/// The acknowledgement side of the file broker: the durable `acked.log`.
struct BrokerAcks {
    log: Mutex<File>,
    pending: Mutex<HashMap<u64, Vec<u64>>>,
}

impl AckTarget for BrokerAcks {
    fn ack(&self, _epoch: ConnectionEpoch, token: AckToken, outcome: AckOutcome) {
        let Some(ids) = lock(&self.pending).remove(&token.a()) else {
            return;
        };
        if outcome != AckOutcome::Committed {
            return;
        }
        let text: String = ids.iter().map(|id| format!("{id}\n")).collect();
        {
            let mut log = lock(&self.log);
            log.write_all(text.as_bytes()).expect("append to acked.log");
            log.sync_data().expect("sync acked.log");
        }
        let max = ids.iter().max().copied().unwrap_or(0);
        println!("@@ACK {max}");
    }
}

struct FileBrokerSource {
    dir: PathBuf,
    queue: VecDeque<u64>,
    acks: Option<Arc<BrokerAcks>>,
    gate: EpochGate,
    source_id: Option<SourceId>,
    clock: Option<Arc<dyn Clock>>,
    next_batch_no: u64,
}

impl FileBrokerSource {
    fn new(dir: PathBuf) -> Self {
        FileBrokerSource {
            dir,
            queue: VecDeque::new(),
            acks: None,
            gate: EpochGate::new(),
            source_id: None,
            clock: None,
            next_batch_no: 1,
        }
    }

    fn io_error(what: &str, e: &dyn std::fmt::Display) -> ThadamError {
        ThadamError::fatal(
            ErrorDomain::Connector,
            codes::CONNECTOR_PROTOCOL,
            format!("{what}: {e}"),
        )
    }
}

#[async_trait]
impl Source for FileBrokerSource {
    async fn open(&mut self, ctx: &SourceContext) -> Result<(), ThadamError> {
        let messages =
            fs::read(self.dir.join("messages")).map_err(|e| Self::io_error("read messages", &e))?;
        let messages = parse_ids(&messages).map_err(|e| Self::io_error("parse messages", &e))?;

        let log_path = self.dir.join("acked.log");
        let existing = match fs::read(&log_path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(Self::io_error("read acked.log", &e)),
        };
        let acked: BTreeSet<u64> = parse_ids(&existing)
            .map_err(|e| Self::io_error("parse acked.log", &e))?
            .into_iter()
            .collect();
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| Self::io_error("open acked.log", &e))?;
        // A torn last line would otherwise be glued to the next append.
        log.set_len(complete_lines(&existing).len() as u64)
            .map_err(|e| Self::io_error("truncate acked.log", &e))?;

        self.queue = messages
            .into_iter()
            .filter(|id| !acked.contains(id))
            .collect();
        self.acks = Some(Arc::new(BrokerAcks {
            log: Mutex::new(log),
            pending: Mutex::new(HashMap::new()),
        }));
        self.source_id = Some(ctx.source_id.clone());
        self.clock = Some(Arc::clone(&ctx.clock));
        Ok(())
    }

    async fn next_batch(
        &mut self,
        limits: BatchLimits,
    ) -> Result<Option<SourceBatch>, ThadamError> {
        // Pacing. Sleeping before dequeuing keeps the future cancel-safe.
        tokio::time::sleep(Duration::from_millis(1)).await;
        let (Some(acks), Some(source_id), Some(clock)) = (&self.acks, &self.source_id, &self.clock)
        else {
            return Err(Self::io_error("next_batch", &"called before open"));
        };
        let take = limits.max_messages.clamp(1, 8);
        let ids: Vec<u64> = (0..take).map_while(|_| self.queue.pop_front()).collect();
        if ids.is_empty() {
            return Ok(None);
        }
        let messages = ids
            .iter()
            .map(|id| RawMessage::new(clock.now(), Bytes::from(format!("{{\"n\":{id}}}"))))
            .collect();
        let batch_no = self.next_batch_no;
        self.next_batch_no += 1;
        let token = AckToken::new(batch_no, ids.len() as u64);
        lock(&acks.pending).insert(batch_no, ids);
        let target: Arc<dyn AckTarget> = Arc::clone(acks) as Arc<dyn AckTarget>;
        let ack: AckHandle = self.gate.issue(token, target);
        Ok(Some(SourceBatch {
            batch: RawBatch::new(source_id.clone(), messages, true),
            ack,
        }))
    }

    async fn close(&mut self) -> Result<(), ThadamError> {
        Ok(())
    }
}

struct FileBrokerFactory {
    dir: PathBuf,
}

impl SourceFactory for FileBrokerFactory {
    fn kind(&self) -> &'static str {
        "filebroker"
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
    ) -> Result<Box<dyn Source>, ThadamError> {
        Ok(Box::new(FileBrokerSource::new(self.dir.clone())))
    }
}

// -------------------------------------------------------------- file sink

#[derive(Default)]
struct FileSegmentSink {
    dir: PathBuf,
    buffered: Vec<(WalRange, Vec<u64>)>,
    last: Option<WalPos>,
}

impl FileSegmentSink {
    fn watermark(&self) -> Result<Option<WalPos>, ThadamError> {
        let files = segment_files(&self.dir).map_err(|e| sink_io("list segments", &e))?;
        Ok(files
            .iter()
            .map(|(_, last, _)| *last)
            .max()
            .map(WalPos::new))
    }

    fn finalize(&mut self) -> Result<Option<WalPos>, ThadamError> {
        let (Some(first), Some(last)) = (self.buffered.first(), self.buffered.last()) else {
            return Ok(None);
        };
        let (first, last) = (first.0.first().get(), last.0.last().get());
        let ns: Vec<u64> = self
            .buffered
            .iter()
            .flat_map(|(_, ns)| ns.iter().copied())
            .collect();
        let body = serde_json::to_vec(&ns).expect("serialize ids");

        let tmp = self.dir.join("x.tmp");
        let mut file = File::create(&tmp).map_err(|e| sink_io("create tmp", &e))?;
        file.write_all(&body)
            .map_err(|e| sink_io("write tmp", &e))?;
        file.sync_all().map_err(|e| sink_io("sync tmp", &e))?;
        drop(file);
        fs::rename(&tmp, self.dir.join(segment_name(first, last)))
            .map_err(|e| sink_io("rename segment", &e))?;
        File::open(&self.dir)
            .and_then(|d| d.sync_all())
            .map_err(|e| sink_io("sync directory", &e))?;
        self.buffered.clear();
        Ok(Some(WalPos::new(last)))
    }
}

#[async_trait]
impl SegmentSink for FileSegmentSink {
    async fn recover(&mut self, ctx: &SinkContext) -> Result<Option<WalPos>, ThadamError> {
        self.dir.clone_from(&ctx.data_dir);
        fs::create_dir_all(&self.dir).map_err(|e| sink_io("create sink dir", &e))?;
        for entry in fs::read_dir(&self.dir).map_err(|e| sink_io("list sink dir", &e))? {
            let path = entry.map_err(|e| sink_io("list sink dir", &e))?.path();
            if path.extension().is_some_and(|ext| ext == "tmp") {
                fs::remove_file(&path).map_err(|e| sink_io("remove tmp", &e))?;
            }
        }
        self.buffered.clear();
        self.last = self.watermark()?;
        Ok(self.last)
    }

    async fn append(&mut self, batch: SegmentBatch) -> Result<Vec<Reject>, ThadamError> {
        if self.last.is_some_and(|last| batch.origin.first() <= last) {
            return Err(ThadamError::permanent(
                ErrorDomain::Sink,
                codes::SINK_NON_MONOTONIC_APPEND,
                format!("batch {} is not after {:?}", batch.origin, self.last),
            ));
        }
        let ns = batch
            .events()
            .filter_map(|(_, e)| match e.payload().as_record()?.get("n")? {
                Value::U64(n) => Some(*n),
                _ => None,
            })
            .collect();
        self.last = Some(batch.origin.last());
        self.buffered.push((batch.origin, ns));
        Ok(Vec::new())
    }

    async fn poll_commit(&mut self) -> Result<Option<WalPos>, ThadamError> {
        let frames: u64 = self.buffered.iter().map(|(origin, _)| origin.len()).sum();
        if frames < FRAMES_PER_FILE {
            return Ok(None);
        }
        self.finalize()
    }

    async fn finalize_all(&mut self) -> Result<Option<WalPos>, ThadamError> {
        self.finalize()?;
        self.watermark()
    }

    fn cursor_committed(&mut self, _pos: WalPos) {}
}

struct FileSinkFactory;

impl SegmentSinkFactory for FileSinkFactory {
    fn kind(&self) -> &'static str {
        "filesink"
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
    ) -> Result<Box<dyn SegmentSink>, ThadamError> {
        Ok(Box::new(FileSegmentSink::default()))
    }
}

// ------------------------------------------------------------------ child

fn yaml_text(data_dir: &Path) -> String {
    format!(
        r#"version: 1
pipeline: {{ name: crash, node_id: {NODE_ID}, data_dir: "{dir}" }}
sources:
  - {{ id: src, kind: filebroker }}
wal: {{ max_disk: 256MiB, segment_bytes: 1MiB, fsync: {{ interval: 2ms }}, on_full: block, retain_for: 1h }}
datasets:
  - id: d
    match: {{ source: src }}
    decode: json
    fields:
      - {{ id: 1000, name: n, type: u64, from: /n, required: true }}
sinks:
  - {{ id: {SINK_ID}, kind: filesink }}
"#,
        dir = data_dir.display()
    )
}

async fn run_child(dir: &Path, final_run: bool) -> u8 {
    let mut registry = Registry::new();
    registry
        .source(Box::new(FileBrokerFactory {
            dir: dir.join("broker"),
        }))
        .segment_sink(Box::new(FileSinkFactory));
    let mut opts = fast_opts();
    opts.stop_when_sources_finish = final_run;

    let pipeline = Pipeline::build(spec(&yaml_text(&dir.join("data"))), &registry, opts)
        .unwrap_or_else(|e| panic!("build: {e:#?}"));
    let handle = pipeline.handle();
    let task = tokio::spawn(pipeline.run(Shutdown::new()));
    handle.wait_for(Phase::Running).await;
    if handle.phase() == Phase::Running {
        println!("@@READY");
        io::stdout().flush().expect("flush stdout");
    }
    let report = task
        .await
        .expect("run task panicked")
        .unwrap_or_else(|e| panic!("run failed to start: {e:#?}"));
    report.exit.code()
}

/// The child half. A no-op unless the parent set `THADAM_RT_CRASH_DIR`.
#[test]
fn runtime_crash_child() {
    let Some(dir) = std::env::var_os(DIR_ENV).map(PathBuf::from) else {
        return;
    };
    let final_run = std::env::var(FINAL_ENV).is_ok_and(|v| v == "1");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let code = rt.block_on(run_child(&dir, final_run));
    println!("@@DONE {code}");
    io::stdout().flush().expect("flush stdout");
}

// ----------------------------------------------------------------- parent

#[derive(Debug, PartialEq, Eq)]
enum Wait {
    Seen,
    /// The child's stdout closed first (it exited); stderr says why.
    Closed,
    TimedOut,
}

struct ChildRun {
    child: std::process::Child,
    rx: mpsc::Receiver<String>,
    lines: Vec<String>,
    stdout_thread: JoinHandle<()>,
    stderr_thread: JoinHandle<String>,
}

struct Finished {
    lines: Vec<String>,
    stderr: String,
    status: std::process::ExitStatus,
}

impl ChildRun {
    fn spawn(exe: &Path, dir: &Path, final_run: bool) -> ChildRun {
        let mut cmd = Command::new(exe);
        cmd.args([
            "--exact",
            "runtime_crash_child",
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env(DIR_ENV, dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
        if final_run {
            cmd.env(FINAL_ENV, "1");
        } else {
            cmd.env_remove(FINAL_ENV);
        }
        let mut child = cmd.spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let (tx, rx) = mpsc::channel::<String>();
        let stdout_thread = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        let stderr_thread = std::thread::spawn(move || {
            let mut text = String::new();
            let _ = stderr.read_to_string(&mut text);
            text
        });
        ChildRun {
            child,
            rx,
            lines: Vec::new(),
            stdout_thread,
            stderr_thread,
        }
    }

    /// Collects output until a line contains `needle`.
    fn wait_for(&mut self, deadline: Instant, needle: &str) -> Wait {
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.rx.recv_timeout(left) {
                Ok(line) => {
                    let hit = line.contains(needle);
                    self.lines.push(line);
                    if hit {
                        return Wait::Seen;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => return Wait::TimedOut,
                Err(mpsc::RecvTimeoutError::Disconnected) => return Wait::Closed,
            }
        }
    }

    /// Waits up to `grace` for the child to exit on its own, then SIGKILLs it.
    fn finish(mut self, grace: Duration) -> Finished {
        let until = Instant::now() + grace;
        while Instant::now() < until && self.child.try_wait().unwrap().is_none() {
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.child.kill();
        let status = self.child.wait().unwrap();
        self.stdout_thread.join().unwrap();
        let stderr = self.stderr_thread.join().unwrap();
        self.lines.extend(self.rx.try_iter());
        Finished {
            lines: self.lines,
            stderr,
            status,
        }
    }
}

/// The value after `@@<kind> ` in a child output line.
fn marker(line: &str, kind: &str) -> Option<u64> {
    let at = line.find("@@")?;
    let mut it = line[at + 2..].split_whitespace();
    (it.next()? == kind).then(|| it.next()?.parse().ok())?
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

#[derive(Default)]
struct WalScan {
    ids: BTreeSet<u64>,
    frames: u64,
    missing_gaps: u64,
}

/// Opens the WAL in-process (running recovery) and collects every record's
/// id. Missing gaps are allowed; anything else that is not a frame is not.
fn scan_wal(wal_dir: &Path) -> Result<WalScan, String> {
    let mut o = WalOptions::new(OnFull::Block);
    o.node_id = Some(NODE_ID.into());
    o.segment_bytes = 1 << 20;
    o.frame_max_bytes = 512 << 10;
    o.group_max_bytes = 512 << 10;
    o.consumers = vec![ConsumerSpec::required(format!("sink.{SINK_ID}"))];
    let (wal, _report) = thadam_wal::open(
        wal_dir,
        o,
        Arc::new(DiskBudget::new()),
        Arc::new(SystemClock::new()),
    )
    .map_err(|e| format!("wal open failed: {e}"))?;
    let mut reader = wal
        .reader("verify", StartFrom::Earliest)
        .map_err(|e| format!("wal reader: {e}"))?;

    let mut scan = WalScan::default();
    while let Some(item) = reader
        .try_next(4 << 20)
        .map_err(|e| format!("wal read: {e}"))?
    {
        match item {
            ReadItem::Frames(chunk) => {
                for frame in chunk.frames() {
                    scan.frames += 1;
                    for record in frame.records() {
                        let v: serde_json::Value = serde_json::from_slice(record.payload())
                            .map_err(|e| format!("frame {}: bad payload: {e}", frame.seq()))?;
                        let n = v
                            .get("n")
                            .and_then(serde_json::Value::as_u64)
                            .ok_or_else(|| format!("frame {}: no n in {v}", frame.seq()))?;
                        scan.ids.insert(n);
                    }
                }
            }
            ReadItem::Gap(Gap::Missing { .. }) => scan.missing_gaps += 1,
            ReadItem::Gap(gap) => return Err(format!("wal gap that is not allowed: {gap:?}")),
        }
    }
    Ok(scan)
}

fn missing_from_wal(acked: &BTreeSet<u64>, wal: &BTreeSet<u64>) -> Vec<u64> {
    acked.difference(wal).copied().collect()
}

/// Checks after the clean final run; returns the failures and the duplicate count.
fn verify_final(dir: &Path) -> (Vec<String>, u64) {
    let mut failures = Vec::new();
    let sink_dir = dir.join("data").join("sinks").join(SINK_ID);
    let files = match segment_files(&sink_dir) {
        Ok(f) => f,
        Err(e) => return (vec![format!("final: list {}: {e}", sink_dir.display())], 0),
    };
    if sink_dir.join("x.tmp").exists() {
        failures.push("final: stale x.tmp after a clean run".into());
    }

    let mut seen = BTreeSet::new();
    let mut total = 0u64;
    for (first, last, path) in &files {
        match fs::read(path)
            .map_err(|e| e.to_string())
            .and_then(|b| serde_json::from_slice::<Vec<u64>>(&b).map_err(|e| e.to_string()))
        {
            Ok(ns) => {
                total += ns.len() as u64;
                seen.extend(ns);
            }
            Err(e) => failures.push(format!("final: segment {first}-{last}: {e}")),
        }
    }
    let lost: Vec<u64> = (1..=N).filter(|n| !seen.contains(n)).collect();
    if !lost.is_empty() {
        failures.push(format!(
            "final: {} ids missing from the sink files, first {:?}",
            lost.len(),
            &lost[..lost.len().min(10)]
        ));
    }
    let duplicates = total.saturating_sub(seen.len() as u64);
    eprintln!(
        "final: {} sink files, {total} rows, {} distinct ids, {duplicates} duplicates",
        files.len(),
        seen.len()
    );

    for pair in files.windows(2) {
        let (prev, next) = (&pair[0], &pair[1]);
        if next.0 != prev.1 + 1 {
            failures.push(format!(
                "final: segment ranges not contiguous: {}-{} then {}-{}",
                prev.0, prev.1, next.0, next.1
            ));
        }
    }

    match read_ids(&dir.join("broker").join("acked.log")) {
        Ok(ids) => {
            let acked: BTreeSet<u64> = ids.into_iter().collect();
            let unacked = (1..=N).filter(|n| !acked.contains(n)).count();
            if unacked > 0 {
                failures.push(format!("final: {unacked} ids never acknowledged"));
            }
        }
        Err(e) => failures.push(format!("final: {e}")),
    }
    (failures, duplicates)
}

#[test]
fn kill_dash_nine_runtime_restart() {
    if std::env::var_os(DIR_ENV).is_some() {
        return;
    }
    let iters = env_u64(ITERS_ENV).unwrap_or(DEFAULT_ITERS);
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let broker = dir.join("broker");
    fs::create_dir_all(&broker).unwrap();
    let messages: String = (1..=N).map(|id| format!("{id}\n")).collect();
    fs::write(broker.join("messages"), messages).unwrap();
    let acked_path = broker.join("acked.log");
    let wal_dir = dir.join("data").join("wal");
    let exe = std::env::current_exe().unwrap();

    let mut rng = SplitMix64::new(0x7269_7665_7431);
    let mut failures: Vec<String> = Vec::new();
    let mut acks_seen = 0u64;
    let mut gated_iters = 0u64;
    let mut max_wal_frames = 0u64;

    for i in 0..iters {
        let acked_before = match read_ids(&acked_path) {
            Ok(ids) => ids.into_iter().collect::<BTreeSet<_>>().len() as u64,
            Err(e) => {
                failures.push(format!("iter {i}: {e}"));
                break;
            }
        };
        // Always draw, so the sequence does not depend on the broker's state.
        let want_ack = rng.chance(70);
        let delay = rng.between(5, 150);
        let gate_on_ack = want_ack && acked_before < N;

        let mut run = ChildRun::spawn(&exe, dir, false);
        let deadline = Instant::now() + READY_TIMEOUT;
        let mut ready = run.wait_for(deadline, "@@READY");
        if ready == Wait::Seen && gate_on_ack {
            gated_iters += 1;
            ready = run.wait_for(deadline, "@@ACK");
        }
        let healthy = ready == Wait::Seen;
        if !healthy {
            failures.push(format!(
                "iter {i}: child not ready ({ready:?}, gate_on_ack: {gate_on_ack}, \
                 within {READY_TIMEOUT:?})"
            ));
        }
        std::thread::sleep(Duration::from_millis(delay));
        let done = run.finish(Duration::ZERO);

        let max_acked_line = done.lines.iter().filter_map(|l| marker(l, "ACK")).max();
        acks_seen += done
            .lines
            .iter()
            .filter(|l| marker(l, "ACK").is_some())
            .count() as u64;
        if done.stderr.contains("panicked") {
            failures.push(format!(
                "iter {i}: child failed ({}): {}",
                done.status,
                done.stderr.trim()
            ));
        }
        if !healthy {
            if failures.len() >= 10 {
                break;
            }
            continue;
        }

        let acked: BTreeSet<u64> = match read_ids(&acked_path) {
            Ok(ids) => ids.into_iter().collect(),
            Err(e) => {
                failures.push(format!("iter {i}: {e}"));
                break;
            }
        };
        if let Some(max) = max_acked_line
            && !acked.contains(&max)
        {
            failures.push(format!(
                "iter {i}: printed @@ACK {max} but acked.log lacks it"
            ));
        }
        match scan_wal(&wal_dir) {
            Ok(scan) => {
                max_wal_frames = max_wal_frames.max(scan.frames);
                let lost = missing_from_wal(&acked, &scan.ids);
                if !lost.is_empty() {
                    failures.push(format!(
                        "iter {i}: acknowledged loss: {} acked ids are not in the WAL, first {:?}",
                        lost.len(),
                        &lost[..lost.len().min(10)]
                    ));
                }
                if scan.missing_gaps > 0 {
                    eprintln!("iter {i}: {} Missing gaps in the WAL", scan.missing_gaps);
                }
            }
            Err(e) => {
                failures.push(format!("iter {i}: {e}"));
                break;
            }
        }
        if failures.len() >= 10 {
            break;
        }
    }

    let mut run = ChildRun::spawn(&exe, dir, true);
    let done_wait = run.wait_for(Instant::now() + READY_TIMEOUT, "@@DONE");
    let done = run.finish(Duration::from_secs(30));
    acks_seen += done
        .lines
        .iter()
        .filter(|l| marker(l, "ACK").is_some())
        .count() as u64;
    let done_code = done.lines.iter().rev().find_map(|l| marker(l, "DONE"));
    if done_wait != Wait::Seen || done_code != Some(0) || done.stderr.contains("panicked") {
        failures.push(format!(
            "final run: wait {done_wait:?}, DONE {done_code:?}, status {}, stderr: {}",
            done.status,
            done.stderr.trim()
        ));
    } else {
        let (more, duplicates) = verify_final(dir);
        failures.extend(more);
        eprintln!(
            "runtime crash loop: {iters} iterations, {acks_seen} acks seen, max {max_wal_frames} \
             WAL frames at a verification, {duplicates} duplicates, {} failures",
            failures.len()
        );
    }

    assert!(failures.is_empty(), "{failures:#?}");
    assert!(
        gated_iters == 0 || acks_seen > 0,
        "no child ever acknowledged a batch"
    );
}
