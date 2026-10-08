//! `thadam validate`: check a configuration and print the memory estimate.

use std::io::Write;
use std::path::Path;

use thadam_config::{CONFIG_HASH_VERSION, CheckOptions, EnvSource, UnresolvedPolicy, check_file};

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
    env: &dyn EnvSource,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> Exit {
    let policy = if allow_unresolved {
        UnresolvedPolicy::Placeholder
    } else {
        UnresolvedPolicy::Error
    };
    let opts = CheckOptions::new()
        .unresolved(policy)
        .kinds(registry::build_registry().kind_policy());
    let checked = check_file(path, env, &opts);
    let name = path.display().to_string();

    if !checked.report.errors.is_empty() || !checked.report.warnings.is_empty() {
        let _ = write!(err, "{}", checked.report.render(&name));
    }
    let Some(spec) = checked.spec else {
        let _ = writeln!(err, "{name}: configuration is invalid");
        return Exit::InvalidConfig;
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
    Exit::Ok
}
