//! `thadam inspect`: read-only views of a data directory.
//!
//! None of these commands takes the data-directory lock or the WAL's `LOCK`,
//! so they run while `thadam run` holds the directory. Each reads a snapshot;
//! positions appended after the read are not shown.

use std::io::Write;
use std::path::{Path, PathBuf};

use clap::{Args, Subcommand};
use thadam_core::{WalPos, WalRange};
use thadam_segments::catalog::{CatalogInspection, SegmentFile};
use thadam_wal::{CursorInfo, FrameInfo, RecordInfo, WalError, WalInspection};
use serde_json::{Value, json};

use crate::data_dir;
use crate::exit::Exit;

/// Payload bytes printed per record before truncation.
const PAYLOAD_LIMIT: usize = 200;
/// Records printed per frame.
const RECORDS_PER_FRAME: usize = 5;
/// Segment files listed in text output; JSON lists them all.
const SEGMENTS_LISTED: usize = 20;

/// Arguments of `thadam inspect`.
#[derive(Args)]
pub struct InspectArgs {
    #[command(subcommand)]
    command: InspectCommand,
}

#[derive(Subcommand)]
enum InspectCommand {
    /// Show the WAL's segments, durable end, epoch, and retained bytes.
    Wal(DataDirArgs),
    /// Show each durable cursor's committed position and in-flight range.
    Cursors(DataDirArgs),
    /// Show a sink's file counts and bytes per catalog state, newest files first.
    Segments(SegmentsArgs),
    /// Print the raw payloads of the newest frames, truncated. Prints at most
    /// the first 5 records of each frame.
    Sample(SampleArgs),
}

#[derive(Args)]
struct DataDirArgs {
    /// The data directory (`pipeline.data_dir` in the configuration).
    data_dir: PathBuf,
    /// Print JSON instead of text.
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct SegmentsArgs {
    /// The data directory (`pipeline.data_dir` in the configuration).
    data_dir: PathBuf,
    /// The sink whose catalog to read.
    #[arg(long, value_name = "ID")]
    sink: String,
    /// Print JSON instead of text.
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct SampleArgs {
    /// The data directory (`pipeline.data_dir` in the configuration).
    data_dir: PathBuf,
    /// How many of the newest frames to print.
    #[arg(short = 'n', long, value_name = "N", default_value_t = 5)]
    count: usize,
    /// Print JSON instead of text.
    #[arg(long)]
    json: bool,
}

/// A failed inspection: the exit code and the message for stderr.
struct Failure {
    exit: Exit,
    message: String,
}

type Outcome = Result<(), Failure>;

impl Failure {
    fn usage(message: impl Into<String>) -> Self {
        Failure {
            exit: Exit::InvalidConfig,
            message: message.into(),
        }
    }

    fn read(message: impl Into<String>) -> Self {
        Failure {
            exit: Exit::RuntimeFailure,
            message: message.into(),
        }
    }

    fn wal(dir: &Path, e: &WalError) -> Self {
        let exit = match e {
            WalError::Incompatible(_) => Exit::DataDir,
            _ => Exit::RuntimeFailure,
        };
        Failure {
            exit,
            message: format!("cannot read the WAL in {}: {e}", dir.display()),
        }
    }
}

impl From<std::io::Error> for Failure {
    fn from(e: std::io::Error) -> Self {
        Failure::read(format!("cannot write output: {e}"))
    }
}

/// Runs `thadam inspect`.
pub fn run(args: &InspectArgs, out: &mut dyn Write, err: &mut dyn Write) -> Exit {
    let result = match &args.command {
        InspectCommand::Wal(a) => wal(a, out),
        InspectCommand::Cursors(a) => cursors(a, out),
        InspectCommand::Segments(a) => segments(a, out),
        InspectCommand::Sample(a) => sample(a, out),
    };
    match result {
        Ok(()) => Exit::Ok,
        Err(failure) => {
            let _ = writeln!(err, "error: {}", failure.message);
            failure.exit
        }
    }
}

fn open_wal(data_dir: &Path) -> Result<WalInspection, Failure> {
    let dir = data_dir::wal_dir(data_dir).map_err(Failure::usage)?;
    thadam_wal::inspect(&dir).map_err(|e| Failure::wal(&dir, &e))
}

fn wal(args: &DataDirArgs, out: &mut dyn Write) -> Outcome {
    let w = open_wal(&args.data_dir)?;
    if args.json {
        return emit_json(out, &wal_json(&w));
    }
    writeln!(out, "wal: {}", w.dir.display())?;
    writeln!(out, "epoch: {}", w.epoch)?;
    writeln!(out, "node: {}", w.node_id.as_deref().unwrap_or("-"))?;
    writeln!(out, "machine: {}", w.machine_id.as_deref().unwrap_or("-"))?;
    writeln!(out, "clean shutdown: {}", yes_no(w.clean_shutdown))?;
    writeln!(out, "first position: {}", w.first_seq)?;
    writeln!(out, "next position: {}", w.next_seq)?;
    match w.durable_end {
        Some(end) => writeln!(out, "durable end: {end}")?,
        None => writeln!(out, "durable end: none")?,
    }
    writeln!(out, "retained bytes: {}", w.retained_bytes)?;
    writeln!(out, "torn tail: {}", yes_no(w.torn_tail))?;
    writeln!(out, "zero tail: {}", yes_no(w.zero_tail))?;
    writeln!(out, "segments: {}", w.segments.len())?;
    for seg in &w.segments {
        writeln!(
            out,
            "  base {:<12} bytes {:<12} {}",
            seg.base.get(),
            seg.bytes,
            seg.path.display()
        )?;
    }
    writeln!(out, "cursors: {}", w.cursors.len())?;
    Ok(())
}

fn wal_json(w: &WalInspection) -> Value {
    json!({
        "dir": w.dir.display().to_string(),
        "epoch": w.epoch.get(),
        "node_id": w.node_id,
        "machine_id": w.machine_id,
        "clean_shutdown": w.clean_shutdown,
        "first_seq": w.first_seq.get(),
        "next_seq": w.next_seq.get(),
        "durable_end": w.durable_end.map(WalPos::get),
        "retained_bytes": w.retained_bytes,
        "torn_tail": w.torn_tail,
        "zero_tail": w.zero_tail,
        "segments": w
            .segments
            .iter()
            .map(|s| json!({
                "base": s.base.get(),
                "path": s.path.display().to_string(),
                "bytes": s.bytes,
            }))
            .collect::<Vec<_>>(),
        "cursor_count": w.cursors.len(),
    })
}

fn cursors(args: &DataDirArgs, out: &mut dyn Write) -> Outcome {
    let w = open_wal(&args.data_dir)?;
    if args.json {
        let list: Vec<Value> = w.cursors.iter().map(cursor_json).collect();
        return emit_json(out, &json!({ "cursors": list }));
    }
    if w.cursors.is_empty() {
        writeln!(out, "no cursors")?;
    }
    for c in &w.cursors {
        let inflight = match c.state.inflight {
            Some(range) => format!("inflight {}..={}", range.first(), range.last()),
            None => "inflight none".to_owned(),
        };
        writeln!(
            out,
            "{:<24} committed {:<12} {}",
            c.name,
            c.state.committed.get(),
            inflight
        )?;
    }
    Ok(())
}

fn cursor_json(c: &CursorInfo) -> Value {
    json!({
        "name": c.name,
        "sink": c.name.strip_prefix("sink."),
        "committed": c.state.committed.get(),
        "inflight": c.state.inflight.map(range_json),
    })
}

fn range_json(r: WalRange) -> Value {
    json!({
        "first": r.first().get(),
        "last": r.last().get(),
        "frames": r.len(),
    })
}

fn segments(args: &SegmentsArgs, out: &mut dyn Write) -> Outcome {
    let dir = data_dir::sink_dir(&args.data_dir, &args.sink).map_err(Failure::usage)?;
    let catalog = thadam_segments::catalog::inspect(&dir)
        .map_err(|e| Failure::read(format!("cannot read the catalog in {}: {e}", dir.display())))?;
    if args.json {
        return emit_json(out, &segments_json(&args.sink, &catalog));
    }
    let t = catalog.totals;
    writeln!(out, "sink: {}", args.sink)?;
    writeln!(out, "{:<10} {:>8} {:>16}", "state", "files", "bytes")?;
    for (state, total) in [
        ("finalized", t.finalized),
        ("eligible", t.eligible),
        ("uploaded", t.uploaded),
        ("deleted", t.deleted),
    ] {
        writeln!(out, "{state:<10} {:>8} {:>16}", total.files, total.bytes)?;
    }
    if catalog.files.is_empty() {
        writeln!(out, "no segment files")?;
        return Ok(());
    }
    writeln!(
        out,
        "newest files (showing {} of {}):",
        catalog.files.len().min(SEGMENTS_LISTED),
        catalog.files.len()
    )?;
    for f in catalog.files.iter().take(SEGMENTS_LISTED) {
        writeln!(
            out,
            "  {:<10} {:<16} wal {}..={} rows {} bytes {} {}",
            f.status.as_str(),
            f.dataset,
            f.range.first(),
            f.range.last(),
            f.rows,
            f.bytes,
            f.path.display()
        )?;
    }
    Ok(())
}

fn segments_json(sink: &str, catalog: &CatalogInspection) -> Value {
    let t = catalog.totals;
    let total = |s: thadam_segments::StateTotal| json!({ "files": s.files, "bytes": s.bytes });
    json!({
        "sink": sink,
        "totals": {
            "finalized": total(t.finalized),
            "eligible": total(t.eligible),
            "uploaded": total(t.uploaded),
            "deleted": total(t.deleted),
        },
        "files": catalog.files.iter().map(segment_json).collect::<Vec<_>>(),
    })
}

fn segment_json(f: &SegmentFile) -> Value {
    json!({
        "id": f.id,
        "dataset": f.dataset,
        "status": f.status.as_str(),
        "path": f.path.display().to_string(),
        "first": f.range.first().get(),
        "last": f.range.last().get(),
        "rows": f.rows,
        "bytes": f.bytes,
    })
}

fn sample(args: &SampleArgs, out: &mut dyn Write) -> Outcome {
    let w = open_wal(&args.data_dir)?;
    let frames = w
        .newest_frames(args.count)
        .map_err(|e| Failure::wal(&w.dir, &e))?;
    if args.json {
        let list: Vec<Value> = frames.iter().map(frame_json).collect();
        return emit_json(out, &json!({ "frames": list }));
    }
    if frames.is_empty() {
        writeln!(out, "no frames retained")?;
    }
    for f in &frames {
        writeln!(
            out,
            "frame {} source {} clock_synced {} records {}",
            f.seq,
            f.source,
            yes_no(f.clock_synced),
            f.records.len()
        )?;
        for r in f.records.iter().take(RECORDS_PER_FRAME) {
            let (text, truncated) = payload_text(&r.payload);
            let mark = if truncated { " (truncated)" } else { "" };
            writeln!(out, "  {} {text}{mark}", r.ingest_time.as_nanos())?;
        }
        if f.records.len() > RECORDS_PER_FRAME {
            writeln!(out, "  ... {} more", f.records.len() - RECORDS_PER_FRAME)?;
        }
    }
    Ok(())
}

fn frame_json(f: &FrameInfo) -> Value {
    json!({
        "seq": f.seq.get(),
        "source": f.source,
        "clock_synced": f.clock_synced,
        "record_count": f.records.len(),
        "records": f.records.iter().take(RECORDS_PER_FRAME).map(record_json).collect::<Vec<_>>(),
    })
}

fn record_json(r: &RecordInfo) -> Value {
    let (text, truncated) = payload_text(&r.payload);
    json!({
        "ingest_time_ns": r.ingest_time.as_nanos(),
        "payload_bytes": r.payload.len(),
        "payload": text,
        "truncated": truncated,
    })
}

/// The payload as text, cut to `PAYLOAD_LIMIT` bytes at a character boundary,
/// and whether it was cut. Bytes that are not UTF-8 are shown as U+FFFD.
fn payload_text(bytes: &[u8]) -> (String, bool) {
    let text = String::from_utf8_lossy(bytes);
    if text.len() <= PAYLOAD_LIMIT {
        return (text.into_owned(), false);
    }
    let mut end = PAYLOAD_LIMIT;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (format!("{}...", &text[..end]), true)
}

fn yes_no(flag: bool) -> &'static str {
    if flag { "yes" } else { "no" }
}

fn emit_json(out: &mut dyn Write, value: &Value) -> Outcome {
    let text = serde_json::to_string_pretty(value)
        .map_err(|e| Failure::read(format!("cannot render JSON: {e}")))?;
    writeln!(out, "{text}")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payloads_are_cut_at_a_character_boundary() {
        let long = "é".repeat(PAYLOAD_LIMIT);
        let (text, truncated) = payload_text(long.as_bytes());
        assert!(truncated);
        assert!(text.ends_with("..."));
        assert!(text.len() <= PAYLOAD_LIMIT + 3);
    }

    #[test]
    fn short_payloads_are_printed_whole() {
        assert_eq!(payload_text(b"{\"n\":1}"), ("{\"n\":1}".to_owned(), false));
    }

    #[test]
    fn invalid_utf8_is_shown_lossily() {
        let (text, truncated) = payload_text(&[0x66, 0xff, 0x6f]);
        assert_eq!(text, "f\u{fffd}o");
        assert!(!truncated);
    }
}
