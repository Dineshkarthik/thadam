//! Finds the workspace root and the `thadam` and `thadam-simulator` binaries.
//! Lookup order for each binary: an environment override, the sibling of the
//! running `thadam-bench` executable (so `target/release` pairs with
//! `target/release`), then `<root>/target/release`.

use std::path::{Path, PathBuf};

use crate::error::{BenchError, Result};

/// The two binaries a run executes, resolved to absolute paths.
#[derive(Debug, Clone, PartialEq)]
pub struct Binaries {
    /// `thadam`, the runtime under test.
    pub thadam: PathBuf,
    /// `thadam-simulator`, the fleet publisher.
    pub simulator: PathBuf,
}

/// Walks up from `start` to the directory whose `Cargo.toml` declares a
/// `[workspace]`.
///
/// # Errors
///
/// Returns [`BenchError::Missing`] when no ancestor is a workspace root.
pub fn workspace_root(start: &Path) -> Result<PathBuf> {
    for dir in start.ancestors() {
        let manifest = dir.join("Cargo.toml");
        if let Ok(text) = std::fs::read_to_string(&manifest)
            && text.lines().any(|l| l.trim() == "[workspace]")
        {
            return Ok(dir.to_path_buf());
        }
    }
    Err(BenchError::Missing(format!(
        "no workspace root (Cargo.toml with [workspace]) above {}",
        start.display()
    )))
}

impl Binaries {
    /// Resolves both binaries for a workspace at `root`.
    ///
    /// # Errors
    ///
    /// Returns [`BenchError::Missing`] naming the build command when a binary
    /// is absent.
    pub fn resolve(root: &Path) -> Result<Binaries> {
        let current = std::env::current_exe().ok();
        let sibling_dir = current.as_deref().and_then(Path::parent);
        Ok(Binaries {
            thadam: locate(
                "thadam",
                "THADAM_BIN",
                sibling_dir,
                root,
                "cargo build --release -p thadam-cli",
            )?,
            simulator: locate(
                "thadam-simulator",
                "THADAM_SIMULATOR_BIN",
                sibling_dir,
                root,
                "cargo build --release -p thadam-simulator",
            )?,
        })
    }
}

fn locate(
    name: &str,
    env_var: &str,
    sibling_dir: Option<&Path>,
    root: &Path,
    build: &str,
) -> Result<PathBuf> {
    if let Some(path) = std::env::var_os(env_var) {
        let path = PathBuf::from(path);
        return if path.is_file() {
            Ok(path)
        } else {
            Err(BenchError::Missing(format!(
                "{env_var} points at {}, which is not a file",
                path.display()
            )))
        };
    }
    let target =
        std::env::var_os("CARGO_TARGET_DIR").map_or_else(|| root.join("target"), PathBuf::from);
    let candidates = sibling_dir
        .map(|d| d.join(name))
        .into_iter()
        .chain(std::iter::once(target.join("release").join(name)));
    for candidate in candidates {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(BenchError::Missing(format!(
        "{name} binary not found; build it with `{build}` or set {env_var}"
    )))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn workspace_root_is_found_from_a_nested_directory() {
        let root = workspace_root(Path::new(env!("CARGO_MANIFEST_DIR"))).expect("root");
        assert!(root.join("tools/thadam-bench/Cargo.toml").is_file());
        assert!(workspace_root(Path::new("/")).is_err());
    }
}
