//! `thadam export`: writes a retained WAL range to one MCAP file (ADR-0015,
//! draft).
//!
//! Like `thadam replay`, the export takes the data-directory lock and opens the
//! WAL the way the runtime does, so it refuses to run while a pipeline holds the
//! directory. It reads the WAL only (never Parquet) and never commits a cursor.
//! The file is written as `<OUT>.tmp`, synced, and renamed into place, so an
//! interrupted export never leaves a partial file under the final name.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, BufWriter, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Args;
use thadam_config::{DecodeFormat, PipelineSpec, ProcessEnv, load_file};
use thadam_core::{DiskBudget, SystemClock, WalPos};
use thadam_mcap::{
    DefaultEncoding, ExportOptions, ExportReport, GapKind, McapError, McapExporter, Provenance,
};
use thadam_wal::{Gap, OnFull, ReadItem, StartFrom, WalError, WalOptions, WalReader};

use crate::data_dir;
use crate::exit::Exit;
use crate::point::{Bound, now_nanos, resolve};
use crate::schemas;

/// Bytes requested per WAL read.
const READ_BYTES: usize = 1 << 20;
/// The reader's cursor name. Never committed, so production cursors are untouched.
const READER_NAME: &str = "export";

/// Arguments of `thadam export`.
#[derive(Args)]
pub struct ExportArgs {
    /// The pipeline configuration file.
    path: PathBuf,
    /// The MCAP file to write. It must not exist unless `--force` is given.
    #[arg(long, value_name = "OUT")]
    mcap: PathBuf,
    /// The first point to export, inclusive: a WAL position, a duration before
    /// now (`-45m`, `-1h30m`), or an RFC 3339 time.
    #[arg(long, value_name = "POINT", allow_hyphen_values = true)]
    from: String,
    /// The last point to export, inclusive; the durable end when absent. Takes
    /// the same forms as `--from`.
    #[arg(long, value_name = "POINT", allow_hyphen_values = true)]
    to: Option<String>,
    /// Export only messages from this source. Repeat to select several; all
    /// sources when absent.
    #[arg(long = "source", value_name = "ID")]
    sources: Vec<String>,
    /// The encoding of a source's messages that carry no `mcap.message_encoding`:
    /// `json`, or `raw` for `application/octet-stream`. Repeat once per source.
    /// The default is `json` when every dataset reading the source decodes JSON,
    /// and `raw` otherwise.
    #[arg(long = "encoding", value_name = "SOURCE=ENCODING")]
    encodings: Vec<String>,
    /// An MCAP file whose schemas supply schema text the WAL does not carry.
    /// Repeat for several files. Schemas of the configuration's `mcap` sources
    /// are read automatically.
    #[arg(long = "schemas", value_name = "FILE.mcap")]
    schemas: Vec<PathBuf>,
    /// Replace `<OUT>` if it exists.
    #[arg(long)]
    force: bool,
}

/// Runs `thadam export`.
pub fn run(args: &ExportArgs, out: &mut dyn Write, err: &mut dyn Write) -> Exit {
    let spec = match load_file(&args.path, &ProcessEnv) {
        Ok(spec) => spec,
        Err(report) => {
            let _ = write!(err, "{}", report.render(&args.path.display().to_string()));
            return Exit::InvalidConfig;
        }
    };
    match export(args, &spec, out, err) {
        Ok(()) => Exit::Ok,
        Err(failure) => {
            let _ = writeln!(err, "error: {}", failure.message);
            failure.exit
        }
    }
}

/// A failed export: the exit code and the message for stderr.
struct Failure {
    exit: Exit,
    message: String,
}

impl Failure {
    fn usage(message: impl Into<String>) -> Self {
        Failure {
            exit: Exit::InvalidConfig,
            message: message.into(),
        }
    }

    fn runtime(message: impl Into<String>) -> Self {
        Failure {
            exit: Exit::RuntimeFailure,
            message: message.into(),
        }
    }

    fn data_dir(message: impl Into<String>) -> Self {
        Failure {
            exit: Exit::DataDir,
            message: message.into(),
        }
    }
}

/// The resolved bounds and selections of an export.
#[derive(Debug, PartialEq, Eq)]
struct Plan {
    from: WalPos,
    to: Option<WalPos>,
    sources: Option<BTreeSet<String>>,
    encodings: BTreeMap<String, DefaultEncoding>,
}

fn export(
    args: &ExportArgs,
    spec: &PipelineSpec,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Result<(), Failure> {
    let plan = plan(args, spec, now_nanos()).map_err(Failure::usage)?;
    check_output(&args.mcap, args.force).map_err(Failure::usage)?;
    let schemas = schemas::load(spec, &args.schemas, err).map_err(Failure::usage)?;

    let root = spec.pipeline.data_dir.as_path();
    let wal_dir = data_dir::wal_dir(root).map_err(Failure::usage)?;
    let _lock = thadam_runtime::lock_data_dir(root).map_err(|e| Failure::data_dir(e.to_string()))?;
    check_pipeline(root, &spec.pipeline.name)?;
    let inspection = thadam_wal::inspect(&wal_dir).map_err(|e| wal_failure(&wal_dir, &e))?;
    let node_id = inspection
        .node_id
        .clone()
        .unwrap_or_else(|| spec.pipeline.node_id.clone());

    let mut wal_opts = WalOptions::new(OnFull::Block);
    wal_opts.node_id = Some(spec.pipeline.node_id.clone());
    let (wal, _) = thadam_wal::open(
        &wal_dir,
        wal_opts,
        Arc::new(DiskBudget::new()),
        Arc::new(SystemClock::new()),
    )
    .map_err(|e| wal_failure(&wal_dir, &e))?;
    let durable = wal.durable_end().map_or(0, WalPos::get);
    let to = plan.to.map_or(durable, WalPos::get);
    let mut reader = wal
        .reader(READER_NAME, StartFrom::At(plan.from))
        .map_err(|e| wal_failure(&wal_dir, &e))?;
    reader.seek(plan.from);

    let mut options = ExportOptions::new(DefaultEncoding::OctetStream, schemas.into_resolver());
    for (source, encoding) in source_encodings(spec, &plan.encodings) {
        options = options.with_source_encoding(source, encoding);
    }

    let tmp = temp_path(&args.mcap);
    let job = Job {
        options,
        reader: &mut reader,
        from: plan.from.get(),
        to,
        sources: plan.sources.as_ref(),
        epoch: wal.epoch().get(),
        node_id,
    };
    let written = write_file(&tmp, job)?;
    fs::rename(&tmp, &args.mcap).map_err(|e| {
        let _ = fs::remove_file(&tmp);
        io_failure("rename", &tmp, &e)
    })?;
    sync_parent(&args.mcap);

    for unresolved in &written.report.unresolved_schemas {
        let _ = writeln!(
            err,
            "warning: no schema for `{}` ({}) on source `{}` topic `{}`; its channel is written without schema text",
            unresolved.schema_name, unresolved.schema_encoding, unresolved.source, unresolved.topic
        );
    }
    print_report(out, &args.mcap, &written);
    Ok(())
}

fn plan(args: &ExportArgs, spec: &PipelineSpec, now: i64) -> Result<Plan, String> {
    let data_dir = &spec.pipeline.data_dir;
    let from = resolve(&args.from, "--from", data_dir, Bound::First, now)?;
    let to = args
        .to
        .as_deref()
        .map(|text| resolve(text, "--to", data_dir, Bound::Last, now))
        .transpose()?;
    if let Some(to) = to
        && to < from
    {
        return Err(format!("--to {to} is before --from {from}"));
    }

    let known: BTreeSet<&str> = spec.sources.iter().map(|s| s.id.as_str()).collect();
    for id in &args.sources {
        if !known.contains(id.as_str()) {
            return Err(format!(
                "--source `{id}` is not a source in the configuration"
            ));
        }
    }
    let sources = (!args.sources.is_empty()).then(|| args.sources.iter().cloned().collect());

    let mut encodings = BTreeMap::new();
    for text in &args.encodings {
        let (source, encoding) = parse_encoding(text)?;
        if !known.contains(source.as_str()) {
            return Err(format!(
                "--encoding `{text}`: `{source}` is not a source in the configuration"
            ));
        }
        if encodings.insert(source.clone(), encoding).is_some() {
            return Err(format!("--encoding is given twice for source `{source}`"));
        }
    }
    Ok(Plan {
        from,
        to,
        sources,
        encodings,
    })
}

/// Parses `SOURCE=json` or `SOURCE=raw`.
fn parse_encoding(text: &str) -> Result<(String, DefaultEncoding), String> {
    let (source, value) = text.split_once('=').ok_or_else(|| {
        format!("invalid --encoding `{text}`: expected SOURCE=json or SOURCE=raw")
    })?;
    if source.is_empty() {
        return Err(format!(
            "invalid --encoding `{text}`: the source id is empty"
        ));
    }
    let encoding = match value {
        "json" => DefaultEncoding::Json,
        "raw" => DefaultEncoding::OctetStream,
        _ => {
            return Err(format!(
                "invalid --encoding `{text}`: the encoding must be `json` or `raw`"
            ));
        }
    };
    Ok((source.to_owned(), encoding))
}

/// Each configured source's default encoding, for messages without
/// `mcap.message_encoding`: `json` when at least one dataset reads the source
/// and every such dataset decodes JSON, otherwise `application/octet-stream`.
/// An explicit `--encoding` wins.
fn source_encodings(
    spec: &PipelineSpec,
    overrides: &BTreeMap<String, DefaultEncoding>,
) -> BTreeMap<String, DefaultEncoding> {
    spec.sources
        .iter()
        .map(|source| {
            let readers: Vec<_> = spec
                .datasets
                .iter()
                .filter(|d| d.match_.source == source.id)
                .collect();
            let derived = if !readers.is_empty()
                && readers
                    .iter()
                    .all(|d| matches!(d.decode, DecodeFormat::Json))
            {
                DefaultEncoding::Json
            } else {
                DefaultEncoding::OctetStream
            };
            let encoding = overrides.get(&source.id).copied().unwrap_or(derived);
            (source.id.clone(), encoding)
        })
        .collect()
}

fn check_output(path: &Path, force: bool) -> Result<(), String> {
    if path.exists() && !force {
        return Err(format!(
            "`{}` already exists; pass --force to replace it",
            path.display()
        ));
    }
    Ok(())
}

fn temp_path(out: &Path) -> PathBuf {
    let mut name = out.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}

fn check_pipeline(root: &Path, pipeline: &str) -> Result<(), Failure> {
    match thadam_runtime::read_thadam_json(root) {
        Ok(Some(meta)) if meta.pipeline != pipeline => Err(Failure::data_dir(format!(
            "the data directory belongs to pipeline `{}`, not `{pipeline}`",
            meta.pipeline
        ))),
        Ok(_) => Ok(()),
        Err(e) => Err(Failure::data_dir(e.to_string())),
    }
}

fn wal_failure(dir: &Path, e: &WalError) -> Failure {
    let message = format!("cannot read the WAL in {}: {e}", dir.display());
    match e {
        WalError::Incompatible(_) | WalError::Locked { .. } => Failure::data_dir(message),
        _ => Failure::runtime(message),
    }
}

fn io_failure(op: &str, path: &Path, e: &io::Error) -> Failure {
    Failure::runtime(format!("cannot {op} {}: {e}", path.display()))
}

fn export_failure(e: McapError) -> Failure {
    let message = match &e {
        McapError::Export { detail: _ } => {
            format!(
                "export failed: {e}\nhint: export the ranges separately with --from/--to, or select sources with --source"
            )
        }
        _ => format!("export failed: {e}"),
    };
    Failure::runtime(message)
}

/// The inputs of one write: the exporter's options, where to read frames from,
/// the inclusive position range, the source selection, and the provenance.
struct Job<'a> {
    options: ExportOptions,
    reader: &'a mut WalReader,
    from: u64,
    to: u64,
    sources: Option<&'a BTreeSet<String>>,
    epoch: u64,
    node_id: String,
}

/// What a write produced: the exporter's report and the positions of the
/// frames that contributed messages.
struct Written {
    report: ExportReport,
    first: Option<u64>,
    last: Option<u64>,
}

/// Writes `job` to `path` and syncs it. On failure the partial file is removed.
fn write_file(path: &Path, job: Job<'_>) -> Result<Written, Failure> {
    let file = File::create(path).map_err(|e| io_failure("create", path, &e))?;
    let mut sink = BufWriter::new(file);
    let written = write_mcap(&mut sink, job).and_then(|written| {
        sink.flush().map_err(|e| io_failure("write", path, &e))?;
        sink.get_ref()
            .sync_all()
            .map_err(|e| io_failure("sync", path, &e))?;
        Ok(written)
    });
    drop(sink);
    if written.is_err() {
        let _ = fs::remove_file(path);
    }
    written
}

fn write_mcap<W: Write + Seek>(sink: W, job: Job<'_>) -> Result<Written, Failure> {
    let mut exporter = McapExporter::new(sink, job.options).map_err(export_failure)?;
    let span = copy_range(&mut exporter, job.reader, job.from, job.to, job.sources)?;
    let report = exporter
        .finish(Provenance {
            thadam_version: env!("CARGO_PKG_VERSION").to_owned(),
            wal_epoch: job.epoch,
            first_pos: span.first.unwrap_or(0),
            last_pos: span.last.unwrap_or(0),
            node_id: job.node_id,
        })
        .map_err(export_failure)?;
    Ok(Written {
        report,
        first: span.first,
        last: span.last,
    })
}

/// The positions of the frames that contributed messages.
#[derive(Default)]
struct Span {
    first: Option<u64>,
    last: Option<u64>,
}

impl Span {
    fn include(&mut self, pos: u64) {
        self.first.get_or_insert(pos);
        self.last = Some(pos);
    }
}

/// Copies the frames at `from..=to` from `sources` (all when `None`) into
/// `exporter`, recording each WAL gap in the range as a gap record.
fn copy_range<W: Write + Seek>(
    exporter: &mut McapExporter<W>,
    reader: &mut WalReader,
    from: u64,
    to: u64,
    sources: Option<&BTreeSet<String>>,
) -> Result<Span, Failure> {
    let mut span = Span::default();
    'read: loop {
        match reader.try_next(READ_BYTES).map_err(|e| read_failure(&e))? {
            None => break,
            Some(ReadItem::Gap(gap)) => {
                let range = gap.range();
                let first = range.first().get().max(from);
                let last = range.last().get().min(to);
                if first <= last {
                    add_gap(exporter, &gap, first, last)?;
                }
                if range.last().get() >= to {
                    break;
                }
            }
            Some(ReadItem::Frames(chunk)) => {
                let past = chunk.range().last().get() >= to;
                for frame in chunk.into_frames() {
                    if frame.seq().get() > to {
                        break 'read;
                    }
                    let source = frame.source().as_str();
                    if sources.is_some_and(|s| !s.contains(source)) {
                        continue;
                    }
                    let batch = frame.to_raw_batch();
                    for message in &batch.messages {
                        exporter
                            .write_raw(source, message)
                            .map_err(export_failure)?;
                    }
                    span.include(frame.seq().get());
                }
                if past {
                    break;
                }
            }
        }
    }
    Ok(span)
}

fn add_gap<W: Write + Seek>(
    exporter: &mut McapExporter<W>,
    gap: &Gap,
    first: u64,
    last: u64,
) -> Result<(), Failure> {
    let kind =
        gap_kind(gap).ok_or_else(|| Failure::runtime("the WAL reported an unknown gap kind"))?;
    exporter
        .add_gap(kind, gap.range().epoch().get(), first, last)
        .map_err(export_failure)
}

fn gap_kind(gap: &Gap) -> Option<GapKind> {
    match gap {
        Gap::Corrupt { .. } => Some(GapKind::Corrupt),
        Gap::Evicted { .. } => Some(GapKind::Evicted),
        Gap::Missing { .. } => Some(GapKind::Missing),
        _ => None,
    }
}

fn read_failure(e: &WalError) -> Failure {
    Failure::runtime(format!("cannot read the WAL: {e}"))
}

fn print_report(out: &mut dyn Write, path: &Path, written: &Written) {
    let _ = writeln!(out, "output: {}", path.display());
    match (written.first, written.last) {
        (Some(first), Some(last)) => {
            let _ = writeln!(out, "positions: {first}..={last}");
        }
        _ => {
            let _ = writeln!(out, "positions: none exported");
        }
    }
    let report = &written.report;
    let _ = writeln!(
        out,
        "messages: {}, channels: {}, gaps: {}, unresolved schemas: {}",
        report.messages,
        report.channels,
        report.gaps,
        report.unresolved_schemas.len()
    );
}

/// Flushes the directory entry of the renamed file. Best effort: the file is
/// already durable, and some platforms cannot open a directory to sync it.
fn sync_parent(out: &Path) {
    if let Some(dir) = out.parent().filter(|d| !d.as_os_str().is_empty())
        && let Ok(handle) = File::open(dir)
    {
        let _ = handle.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: &str = r#"version: 1
pipeline: { name: test, node_id: node-1, data_dir: "data" }
sources:
  - { id: plc, kind: memory }
  - { id: bag, kind: mcap, config: { paths: ["a.mcap"] } }
wal: { max_disk: 64MiB, segment_bytes: 1MiB, fsync: { interval: 1ms }, on_full: block, retain_for: 1h }
datasets:
  - id: d
    match: { source: plc }
    decode: json
    fields:
      - { id: 1000, name: n, type: u64, from: /n, required: true }
sinks:
  - id: pq
    kind: parquet
    config: { datasets: [d], compression: none, rotation: { max_bytes: null, max_rows: 2, max_age: null } }
"#;

    fn spec() -> PipelineSpec {
        thadam_config::load_str(CONFIG, &thadam_config::MapEnv::new()).expect("valid config")
    }

    fn args(from: &str, to: Option<&str>) -> ExportArgs {
        ExportArgs {
            path: PathBuf::from("pipeline.yaml"),
            mcap: PathBuf::from("out.mcap"),
            from: from.to_owned(),
            to: to.map(str::to_owned),
            sources: Vec::new(),
            encodings: Vec::new(),
            schemas: Vec::new(),
            force: false,
        }
    }

    #[test]
    fn positions_resolve_without_reading_the_wal() {
        let plan = plan(&args("2", Some("5")), &spec(), 0).unwrap();
        assert_eq!(plan.from, WalPos::new(2));
        assert_eq!(plan.to, Some(WalPos::new(5)));
        assert_eq!(plan.sources, None);
        assert!(plan.encodings.is_empty());
    }

    #[test]
    fn to_before_from_is_refused() {
        let err = plan(&args("5", Some("2")), &spec(), 0).unwrap_err();
        assert!(err.contains("before"), "{err}");
    }

    #[test]
    fn unknown_sources_are_refused_in_both_flags() {
        let mut a = args("1", None);
        a.sources = vec!["nope".into()];
        assert!(plan(&a, &spec(), 0).unwrap_err().contains("`nope`"));

        let mut a = args("1", None);
        a.encodings = vec!["nope=json".into()];
        assert!(plan(&a, &spec(), 0).unwrap_err().contains("`nope`"));
    }

    #[test]
    fn source_filter_is_a_set() {
        let mut a = args("1", None);
        a.sources = vec!["plc".into(), "plc".into()];
        let plan = plan(&a, &spec(), 0).unwrap();
        assert_eq!(plan.sources, Some(BTreeSet::from(["plc".to_owned()])));
    }

    #[test]
    fn encoding_flags_parse_json_and_raw() {
        assert_eq!(
            parse_encoding("plc=json").unwrap(),
            ("plc".to_owned(), DefaultEncoding::Json)
        );
        assert_eq!(
            parse_encoding("bag=raw").unwrap(),
            ("bag".to_owned(), DefaultEncoding::OctetStream)
        );
        assert!(parse_encoding("bag").is_err());
        assert!(parse_encoding("=json").is_err());
        assert!(parse_encoding("bag=xml").is_err());
        assert!(parse_encoding("bag=application/octet-stream").is_err());
    }

    #[test]
    fn a_source_given_two_encodings_is_refused() {
        let mut a = args("1", None);
        a.encodings = vec!["plc=json".into(), "plc=raw".into()];
        assert!(plan(&a, &spec(), 0).unwrap_err().contains("twice"));
    }

    #[test]
    fn default_encoding_is_json_only_when_every_reader_decodes_json() {
        let spec = spec();
        let defaults = source_encodings(&spec, &BTreeMap::new());
        assert_eq!(defaults["plc"], DefaultEncoding::Json);
        assert_eq!(defaults["bag"], DefaultEncoding::OctetStream);

        let overrides = BTreeMap::from([("plc".to_owned(), DefaultEncoding::OctetStream)]);
        assert_eq!(
            source_encodings(&spec, &overrides)["plc"],
            DefaultEncoding::OctetStream
        );
    }

    #[test]
    fn an_existing_output_needs_force() {
        let dir = thadam_testkit::TempDataDir::new().unwrap();
        let out = dir.join("out.mcap");
        assert!(check_output(&out, false).is_ok());
        fs::write(&out, b"old").unwrap();
        assert!(check_output(&out, false).unwrap_err().contains("--force"));
        assert!(check_output(&out, true).is_ok());
    }

    #[test]
    fn the_temporary_file_sits_next_to_the_output() {
        assert_eq!(
            temp_path(Path::new("/tmp/run.mcap")),
            PathBuf::from("/tmp/run.mcap.tmp")
        );
    }

    #[test]
    fn gap_variants_map_to_their_mcap_kinds() {
        use thadam_core::{WalEpoch, WalRange};
        let range = WalRange::single(WalEpoch::new(1), WalPos::new(3));
        assert_eq!(gap_kind(&Gap::Corrupt { range }), Some(GapKind::Corrupt));
        assert_eq!(
            gap_kind(&Gap::Evicted {
                range,
                cursor: "c".into()
            }),
            Some(GapKind::Evicted)
        );
        assert_eq!(gap_kind(&Gap::Missing { range }), Some(GapKind::Missing));
    }

    #[test]
    fn span_tracks_first_and_last_included_frames() {
        let mut span = Span::default();
        span.include(4);
        span.include(9);
        assert_eq!((span.first, span.last), (Some(4), Some(9)));
    }
}
