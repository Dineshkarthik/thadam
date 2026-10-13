//! Replaying a WAL range through a sink's chain into a separate output.
//!
//! Replay is offline: it takes the data-directory lock, so it refuses to run
//! while the pipeline does. It never commits a WAL cursor (production cursors
//! and retention are untouched) and never writes to the production sink
//! directories. Chains use the current configuration (ARCHITECTURE.md
//! section 7), and stateful steps are warm-started from the frames before
//! `from`, so the output is row-set equivalent to what a continuous run
//! produced for the same range and configuration. Rejects are counted, not
//! quarantined.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use thadam_config::PipelineSpec;
use thadam_core::{
    BuildContext, ErrorDomain, ThadamError, SegmentBatch, SegmentSink, Shutdown, SinkContext,
    SinkId, WalPos, codes,
};
use thadam_transform::Chain;
use thadam_wal::{ReadItem, StartFrom};
use serde::Serialize;

use crate::chain::{clock_groups, group_range, read_now, try_process, wal_error, warm};
use crate::datadir::{self, Layout};
use crate::lane::ChainBatch;
use crate::metrics::connector_metrics;
use crate::options::RuntimeOptions;
use crate::plan::{self, config_error};
use crate::registry::{Registry, SinkFactory};
use crate::segment::split;

/// Where replayed events go.
#[non_exhaustive]
pub enum ReplayOutput {
    /// A fresh instance of the sink's kind, built from its configuration in
    /// `data_dir/replays/<replay_id>/<sink>`.
    SeparateDir,
    /// A fresh instance of the sink's kind, built from its configuration in
    /// the given directory. Must not be inside the data directory's `sinks/`
    /// or `wal/` trees, which hold production state.
    SeparateDirAt(PathBuf),
    /// A segment sink the caller built.
    Sink(Box<dyn SegmentSink>),
}

impl std::fmt::Debug for ReplayOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayOutput::SeparateDir => f.write_str("SeparateDir"),
            ReplayOutput::SeparateDirAt(dir) => f.debug_tuple("SeparateDirAt").field(dir).finish(),
            ReplayOutput::Sink(_) => f.write_str("Sink(..)"),
        }
    }
}

/// What to replay.
#[derive(Debug)]
#[non_exhaustive]
pub struct ReplayRequest {
    /// The sink whose chain (datasets and transforms) processes the frames.
    pub sink: String,
    /// The first WAL position, inclusive.
    pub from: WalPos,
    /// The last WAL position, inclusive; the durable end if absent.
    pub to: Option<WalPos>,
    /// Names the output directory; `[A-Za-z0-9_.-]`.
    pub replay_id: String,
    /// Where the events go.
    pub output: ReplayOutput,
}

impl ReplayRequest {
    /// Replays from `from` to the durable end through `sink`'s chain into a
    /// separate directory named by `replay_id`.
    pub fn new(sink: impl Into<String>, from: WalPos, replay_id: impl Into<String>) -> Self {
        ReplayRequest {
            sink: sink.into(),
            from,
            to: None,
            replay_id: replay_id.into(),
            output: ReplayOutput::SeparateDir,
        }
    }

    /// Stops after `to`, inclusive.
    #[must_use]
    pub fn to(mut self, to: WalPos) -> Self {
        self.to = Some(to);
        self
    }

    /// Sends the events to `output`.
    #[must_use]
    pub fn output(mut self, output: ReplayOutput) -> Self {
        self.output = output;
        self
    }
}

/// What a replay did.
#[derive(Clone, Debug, Serialize)]
#[non_exhaustive]
pub struct ReplayReport {
    /// The replay id.
    pub replay_id: String,
    /// The sink whose chain was used.
    pub sink: String,
    /// The first and last frame read.
    pub range: Option<(u64, u64)>,
    /// Frames read.
    pub frames: u64,
    /// Events written to the output.
    pub events: u64,
    /// Records the chain rejected (counted, not quarantined).
    pub rejects: u64,
    /// Gaps met in the range.
    pub gaps: u64,
    /// The output's final watermark.
    pub watermark: Option<u64>,
    /// The output directory, for [`ReplayOutput::SeparateDir`].
    pub output_dir: Option<PathBuf>,
}

/// Makes `path` absolute against the working directory and resolves `.` and
/// `..` lexically. Symbolic links are not followed.
fn lexical_absolute(path: &Path) -> PathBuf {
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_or_else(|_| path.to_path_buf(), |cwd| cwd.join(path))
    };
    let mut out = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other),
        }
    }
    out
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 100
        && !id.starts_with('.')
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

/// Replays `req.from..=req.to` of the WAL through a sink's chain into a
/// separate output. Requires a Tokio runtime; the pipeline must not be running.
pub async fn replay(
    spec: &PipelineSpec,
    registry: &Registry,
    opts: RuntimeOptions,
    req: ReplayRequest,
) -> Result<ReplayReport, ThadamError> {
    if !valid_id(&req.replay_id) {
        return Err(config_error(
            "replay_id",
            "a replay id must be 1 to 100 characters of [A-Za-z0-9_.-]",
        ));
    }
    let Some((index, sink_spec)) = spec
        .sinks
        .iter()
        .enumerate()
        .find(|(_, s)| s.id == req.sink)
    else {
        return Err(config_error(
            "sink",
            format!("no sink `{}` in the configuration", req.sink),
        ));
    };
    let factory = registry.sink_factories(&sink_spec.kind).into_iter().next();
    if matches!(factory, None | Some(SinkFactory::Service(_))) {
        return Err(config_error(
            "sink",
            format!("`{}` is not a sink with a chain", req.sink),
        ));
    }
    let layout = Layout {
        root: opts
            .data_dir
            .clone()
            .unwrap_or_else(|| spec.pipeline.data_dir.clone()),
    };
    let only = plan::sink_datasets(spec, index, &sink_spec.config)?;
    let chain_spec = plan::chain_spec(
        spec,
        &req.sink,
        only.as_deref(),
        opts.config_dir.as_deref(),
        Some(layout.schemas().as_path()),
    )?;
    let chain_spec = plan::with_births(chain_spec, &layout.births(), &req.sink, true);
    let mut chain = Chain::compile(&chain_spec, opts.chain_limits.clone()).map_err(|es| {
        es.into_iter()
            .next()
            .unwrap_or_else(|| config_error("sink", "chain compile failed"))
    })?;

    let _lock = datadir::lock(&layout.root)?;
    let existing = datadir::read_thadam_json(&layout.root)?;
    datadir::check_before_wal(existing.as_ref(), &spec.pipeline.name, opts.repair)?;
    let budget = Arc::new(plan::budget(spec));
    let chain_sinks: Vec<_> = spec
        .sinks
        .iter()
        .enumerate()
        .filter(|(_, s)| {
            registry
                .sink_factories(&s.kind)
                .iter()
                .any(|f| !matches!(f, SinkFactory::Service(_)))
        })
        .collect();
    let wal_opts = plan::wal_options(spec, &chain_sinks, opts.machine_id.clone())?;
    let (wal_dir, b, clock) = (layout.wal(), Arc::clone(&budget), Arc::clone(&opts.clock));
    let (wal, _) =
        tokio::task::spawn_blocking(move || thadam_wal::open(&wal_dir, wal_opts, b, clock))
            .await
            .map_err(|_| {
                ThadamError::fatal(
                    ErrorDomain::Buffer,
                    codes::BUFFER_IO,
                    "wal open task panicked",
                )
            })?
            .map_err(|e| wal_error(&e))?;
    datadir::check_wal_binding(existing.as_ref(), wal.epoch(), opts.repair)?;

    let out_id = format!("replay.{}.{}", req.replay_id, req.sink);
    let config_hash = spec.config_hash_hex();
    let enrichment_hash = chain.enrichment_hash();
    let protected = [
        lexical_absolute(&layout.root.join("sinks")),
        lexical_absolute(&layout.wal()),
    ];
    let build_at = |dir: PathBuf| -> Result<(Box<dyn SegmentSink>, Option<PathBuf>), ThadamError> {
        let abs = lexical_absolute(&dir);
        if protected.iter().any(|p| abs.starts_with(p)) {
            return Err(config_error(
                "replay_output",
                format!(
                    "replay output `{}` is inside production state; choose another directory",
                    dir.display()
                ),
            ));
        }
        let ctx = BuildContext::new(out_id.as_str(), Arc::clone(&opts.clock), dir.clone())
            .with_datasets(plan::dataset_decls(spec, only.as_deref()))
            .with_config_hashes(&config_hash, &enrichment_hash)
            .with_node_id(&spec.pipeline.node_id);
        let built = match &factory {
            Some(SinkFactory::Segment(f)) => f.build(sink_spec.config.clone(), &ctx)?,
            _ => {
                return Err(config_error(
                    "sink",
                    "replay into a separate directory needs a segment sink; pass ReplayOutput::Sink",
                ));
            }
        };
        Ok((built, Some(dir)))
    };
    let (mut out, output_dir) = match req.output {
        ReplayOutput::Sink(s) => (s, None),
        ReplayOutput::SeparateDir => build_at(layout.replay(&req.replay_id, &req.sink))?,
        ReplayOutput::SeparateDirAt(dir) => build_at(dir)?,
    };
    let ctx = SinkContext::new(
        SinkId::new(out_id.as_str()),
        Shutdown::new(),
        Arc::clone(&opts.clock),
        output_dir
            .clone()
            .unwrap_or_else(|| layout.replay(&req.replay_id, &req.sink)),
        Arc::clone(&budget),
    )
    .with_metrics(connector_metrics(Arc::clone(&opts.metrics)));
    if let Some(p) = out.recover(&ctx).await?
        && p >= req.from
    {
        return Err(config_error(
            "replay_id",
            format!(
                "replay output `{}` already holds data through {p}; use a new replay id",
                req.replay_id
            ),
        ));
    }

    let from = req.from.get().max(1);
    let to = req
        .to
        .map(WalPos::get)
        .or_else(|| wal.durable_end().map(WalPos::get))
        .unwrap_or(0);
    if plan::chain_has_state(spec, &req.sink, only.as_deref()) {
        warm(
            &mut chain,
            &wal,
            &format!("replay.warm.{}", req.replay_id),
            from - 1,
            opts.warm_start_frames,
            None,
            opts.read_batch_bytes,
        )
        .await?;
    }
    if from > 1 {
        chain.mark_range_start();
    }
    let mut report = ReplayReport {
        replay_id: req.replay_id.clone(),
        sink: req.sink.clone(),
        range: None,
        frames: 0,
        events: 0,
        rejects: 0,
        gaps: 0,
        watermark: None,
        output_dir,
    };
    let epoch = wal.epoch();
    let mut reader = wal
        .reader(
            &format!("replay.{}", req.replay_id),
            StartFrom::At(WalPos::new(from)),
        )
        .map_err(|e| wal_error(&e))?;
    reader.seek(WalPos::new(from));
    let mut appended = 0u64;
    loop {
        if from > to {
            break;
        }
        let (r, item) = read_now(reader, opts.read_batch_bytes).await?;
        reader = r;
        match item {
            None => break,
            Some(ReadItem::Gap(g)) => {
                report.gaps += 1;
                chain.on_gap();
                tracing::warn!(gap = ?g, "gap in the replayed range");
                if g.range().last().get() >= to {
                    break;
                }
            }
            Some(ReadItem::Frames(chunk)) => {
                let past = chunk.range().last().get() >= to;
                let frames = chunk.into_frames();
                let keep = frames.partition_point(|f| f.seq().get() <= to);
                for group in clock_groups(&frames[..keep]) {
                    let Some(origin) = group_range(epoch, group) else {
                        continue;
                    };
                    let Some((output, rejects)) =
                        try_process(&mut chain, epoch, group, &opts.failpoints)
                    else {
                        return Err(ThadamError::fatal(
                            ErrorDomain::Transform,
                            codes::TRANSFORM_PANIC,
                            format!("the chain panicked replaying frames {origin}"),
                        ));
                    };
                    report.frames += group.len() as u64;
                    report.rejects += rejects.len() as u64;
                    report.range = Some((
                        report.range.map_or(origin.first().get(), |r| r.0),
                        origin.last().get(),
                    ));
                    let synced = group.first().is_some_and(thadam_wal::Frame::clock_synced);
                    let mut cb = ChainBatch {
                        origin,
                        clock_synced: synced,
                        datasets: output.batches,
                        rejects: Vec::new(),
                        hold: None,
                        generation: 0,
                    };
                    let (datasets, meta) = split(&mut cb);
                    report.events += meta.len() as u64;
                    out.append(SegmentBatch::new(origin, synced, datasets))
                        .await?;
                    appended += 1;
                    if appended.is_multiple_of(16)
                        && let Some(w) = out.poll_commit().await?
                    {
                        report.watermark = Some(w.get());
                    }
                }
                if past {
                    break;
                }
            }
        }
    }
    if let Some(w) = out.finalize_all().await? {
        report.watermark = Some(w.get());
        out.cursor_committed(w);
    }
    tracing::info!(replay = %report.replay_id, frames = report.frames, events = report.events, "replay finished");
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parent_components_resolve_before_the_protected_check() {
        let p = lexical_absolute(Path::new("/data/out/../sinks/pq"));
        assert_eq!(p, PathBuf::from("/data/sinks/pq"));
        assert!(p.starts_with("/data/sinks"));
    }

    #[test]
    fn relative_paths_are_anchored_to_the_working_directory() {
        let p = lexical_absolute(Path::new("./wal/segments"));
        assert!(p.is_absolute());
        assert!(p.ends_with("wal/segments"));
    }
}
