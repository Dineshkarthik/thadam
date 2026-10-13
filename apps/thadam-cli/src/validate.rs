//! `thadam validate`: check a configuration and print the memory estimate.
//! With `--against-data-dir`, also check it the way `thadam run` starts it.

use std::io::Write;
use std::path::Path;

use thadam_config::{
    CONFIG_HASH_VERSION, CheckOptions, EnvSource, PipelineSpec, UnresolvedPolicy, check_file,
};
use thadam_core::ThadamError;
use thadam_runtime::{ExitStatus, Pipeline, Registry, RuntimeOptions, check_data_dir};

use crate::exit::Exit;
use crate::registry;

fn count(n: usize, noun: &str) -> String {
    if n == 1 {
        format!("{n} {noun}")
    } else {
        format!("{n} {noun}s")
    }
}

/// Checks `path`. Diagnostics go to `err`; on success a summary and the memory
/// estimate go to `out`. Returns [`Exit::InvalidConfig`] when any error was found.
pub fn run(
    path: &Path,
    allow_unresolved: bool,
    against_data_dir: bool,
    env: &dyn EnvSource,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Exit {
    if against_data_dir && allow_unresolved {
        let _ = writeln!(
            err,
            "error: --against-data-dir needs the node's real secrets; drop --allow-unresolved"
        );
        return Exit::InvalidConfig;
    }
    let policy = if allow_unresolved {
        UnresolvedPolicy::Placeholder
    } else {
        UnresolvedPolicy::Error
    };
    let registry = registry::build_registry();
    let opts = CheckOptions::new()
        .unresolved(policy)
        .kinds(registry.kind_policy());
    let checked = check_file(path, env, &opts);
    let name = path.display().to_string();

    if !checked.report.errors.is_empty() || !checked.report.warnings.is_empty() {
        let _ = write!(err, "{}", checked.report.render(&name));
    }
    let Some(spec) = checked.spec else {
        let _ = writeln!(err, "{name}: configuration is invalid");
        return Exit::InvalidConfig;
    };

    let data_dir_notes = if against_data_dir {
        match check_against_data_dir(&spec, &registry) {
            Ok(notes) => Some(notes),
            Err(errs) => {
                for e in &errs {
                    let _ = writeln!(err, "{}", render(e));
                }
                let _ = writeln!(
                    err,
                    "{name}: configuration cannot start against this data directory"
                );
                return Exit::from(ExitStatus::for_errors(&errs));
            }
        }
    } else {
        None
    };

    let _ = writeln!(out, "{name}: configuration is valid");
    let _ = writeln!(
        out,
        "  pipeline `{}` (node `{}`): {}, {}, {}, {}",
        spec.pipeline.name,
        spec.pipeline.node_id,
        count(spec.sources.len(), "source"),
        count(spec.datasets.len(), "dataset"),
        count(spec.transforms.len(), "transform"),
        count(spec.sinks.len(), "sink"),
    );
    let _ = writeln!(
        out,
        "  config hash: {} (hash version {CONFIG_HASH_VERSION})",
        spec.config_hash_hex()
    );
    let _ = write!(out, "{}", spec.memory_estimate());
    if let Some(notes) = data_dir_notes {
        let _ = writeln!(out, "data directory: {}", spec.pipeline.data_dir.display());
        for note in notes {
            let _ = writeln!(out, "  {note}");
        }
    }
    Exit::Ok
}

/// The checks `--against-data-dir` adds to the file's own: `Pipeline::check`
/// (what `run` checks before it starts) and the read-only look at the data
/// directory the file names. Returns every error found, or the notes.
fn check_against_data_dir(
    spec: &PipelineSpec,
    registry: &Registry,
) -> Result<Vec<String>, Vec<ThadamError>> {
    let mut errs = Pipeline::check(spec, registry, &RuntimeOptions::default())
        .err()
        .unwrap_or_default();
    let notes = match check_data_dir(&spec.pipeline.data_dir, spec, registry) {
        Ok(notes) => notes,
        Err(e) => {
            errs.push(e);
            Vec::new()
        }
    };
    if errs.is_empty() {
        Ok(notes)
    } else {
        Err(errs)
    }
}

/// One error for stderr, with its structured context (the path or component)
/// appended.
fn render(e: &ThadamError) -> String {
    let at: Vec<String> = e.context.iter().map(|(k, v)| format!("{k}={v}")).collect();
    if at.is_empty() {
        format!("error: {e}")
    } else {
        format!("error: {e} ({})", at.join(", "))
    }
}
