//! `thadam replay`: re-run a sink's chain over a retained WAL range into a
//! separate output directory.

use std::io::Write;
use std::path::{Path, PathBuf};

use clap::Args;
use thadam_config::{ProcessEnv, load_file};
use thadam_core::WalPos;
use thadam_runtime::{ExitStatus, ReplayOutput, ReplayReport, ReplayRequest, RuntimeOptions};
use tokio::runtime::Builder;

use crate::exit::Exit;
use crate::point::{Bound, now_nanos, resolve};
use crate::registry;

/// Arguments of `thadam replay`.
#[derive(Args)]
pub struct ReplayArgs {
    /// The pipeline configuration file.
    path: PathBuf,
    /// The first point to replay, inclusive: a WAL position, a duration before
    /// now (`-45m`, `-1h30m`), or an RFC 3339 time.
    #[arg(long, value_name = "POINT", allow_hyphen_values = true)]
    from: String,
    /// The last point to replay, inclusive; the durable end when absent. Takes
    /// the same forms as `--from`.
    #[arg(long, value_name = "POINT", allow_hyphen_values = true)]
    to: Option<String>,
    /// The sink whose chain processes the replayed frames.
    #[arg(long, value_name = "ID")]
    sink: String,
    /// Where to write the output. Must lie outside the data directory's
    /// `sinks/` and `wal/`. Defaults to `<data_dir>/replays/<id>/<sink>`.
    #[arg(long, value_name = "DIR")]
    output: Option<PathBuf>,
    /// Names this replay: `[A-Za-z0-9_.-]`, 1 to 100 characters. Defaults to
    /// `replay-<unix milliseconds>`.
    #[arg(long, value_name = "ID")]
    replay_id: Option<String>,
}

/// Runs `thadam replay`.
pub fn run(args: &ReplayArgs, out: &mut dyn Write, err: &mut dyn Write) -> Exit {
    let spec = match load_file(&args.path, &ProcessEnv) {
        Ok(spec) => spec,
        Err(report) => {
            let _ = write!(err, "{}", report.render(&args.path.display().to_string()));
            return Exit::InvalidConfig;
        }
    };
    let now = now_nanos();
    let plan = match plan(args, &spec.pipeline.data_dir, now) {
        Ok(plan) => plan,
        Err(message) => {
            let _ = writeln!(err, "error: {message}");
            return Exit::InvalidConfig;
        }
    };

    let registry = registry::build_registry();
    let runtime = match Builder::new_multi_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            let _ = writeln!(err, "failed to build Tokio runtime: {e}");
            return Exit::RuntimeFailure;
        }
    };

    let mut req = ReplayRequest::new(args.sink.clone(), plan.from, plan.replay_id.clone());
    if let Some(to) = plan.to {
        req = req.to(to);
    }
    if let Some(dir) = &args.output {
        req = req.output(ReplayOutput::SeparateDirAt(dir.clone()));
    }
    let result = runtime.block_on(thadam_runtime::replay(
        &spec,
        &registry,
        RuntimeOptions::default(),
        req,
    ));
    match result {
        Ok(report) => {
            print_report(out, &report);
            Exit::Ok
        }
        Err(e) => {
            let _ = writeln!(err, "error: replay failed: {e}");
            Exit::from(ExitStatus::for_error(&e))
        }
    }
}

/// The resolved bounds of a replay.
#[derive(Debug, PartialEq, Eq)]
struct Plan {
    from: WalPos,
    to: Option<WalPos>,
    replay_id: String,
}

fn plan(args: &ReplayArgs, data_dir: &Path, now: i64) -> Result<Plan, String> {
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
    let replay_id = args
        .replay_id
        .clone()
        .unwrap_or_else(|| format!("replay-{}", now / 1_000_000));
    Ok(Plan {
        from,
        to,
        replay_id,
    })
}

fn print_report(out: &mut dyn Write, report: &ReplayReport) {
    let _ = writeln!(out, "replay id: {}", report.replay_id);
    match &report.output_dir {
        Some(dir) => {
            let _ = writeln!(out, "output: {}", dir.display());
        }
        None => {
            let _ = writeln!(out, "output: (caller sink)");
        }
    }
    let _ = writeln!(out, "sink: {}", report.sink);
    match report.range {
        Some((first, last)) => {
            let _ = writeln!(out, "frames: {first}..={last}");
        }
        None => {
            let _ = writeln!(out, "frames: none in range");
        }
    }
    let _ = writeln!(
        out,
        "frames read: {}, events: {}, rejects: {}, gaps: {}",
        report.frames, report.events, report.rejects, report.gaps
    );
}
