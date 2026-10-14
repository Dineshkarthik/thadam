//! `thadam init`: write a commented starter configuration file.

use std::io::Write;
use std::path::{Path, PathBuf};

use clap::Args;
use thadam_core::write_atomic;
use thadam_runtime::ExitStatus;

use crate::exit::Exit;

/// The starter pipeline configuration written by `thadam init`.
const STARTER: &str = include_str!("starter.yaml");

/// Arguments of `thadam init`.
#[derive(Args)]
pub struct InitArgs {
    /// Where to write the starter configuration file.
    #[arg(default_value = "pipeline.yaml")]
    pub path: PathBuf,
}

/// Writes the starter configuration to `path`, refusing to overwrite an
/// existing file.
pub fn run(path: &Path, out: &mut dyn Write, err: &mut dyn Write) -> Exit {
    if path.exists() || path.symlink_metadata().is_ok() {
        let _ = writeln!(
            err,
            "error: `{}` already exists; refusing to overwrite",
            path.display()
        );
        return Exit::InvalidConfig;
    }
    match write_atomic(path, STARTER.as_bytes()) {
        Ok(()) => {
            let _ = writeln!(out, "wrote {}", path.display());
            Exit::Ok
        }
        Err(e) => {
            let _ = writeln!(err, "error: {e}");
            Exit::from(ExitStatus::for_error(&e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_writes_starter_and_refuses_to_overwrite() {
        let dir = thadam_testkit::TempDataDir::new().unwrap();
        let path = dir.join("pipeline.yaml");
        let (mut out, mut err) = (Vec::new(), Vec::new());

        assert_eq!(run(&path, &mut out, &mut err), Exit::Ok);
        assert!(err.is_empty());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), STARTER);

        std::fs::write(&path, "custom\n").unwrap();
        out.clear();
        err.clear();
        assert_eq!(run(&path, &mut out, &mut err), Exit::InvalidConfig);
        assert!(String::from_utf8_lossy(&err).contains("already exists"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "custom\n");
    }
}
