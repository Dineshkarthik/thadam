//! Where `${env:}` and `${file:}` values come from.

use std::collections::HashMap;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

/// The largest `${file:}` value read, in bytes.
pub const MAX_SECRET_FILE_BYTES: u64 = 64 * 1024;

/// Supplies environment variables and secret files to interpolation.
///
/// Tests and embedders can substitute their own; [`ProcessEnv`] reads the real
/// process environment and filesystem.
pub trait EnvSource {
    /// The value of an environment variable, or `None` when unset.
    fn var(&self, name: &str) -> Option<String>;

    /// Reads at most [`MAX_SECRET_FILE_BYTES`] plus one byte of a file.
    fn read_file(&self, path: &Path) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        std::fs::File::open(path)?
            .take(MAX_SECRET_FILE_BYTES + 1)
            .read_to_end(&mut buf)?;
        Ok(buf)
    }
}

/// The real process environment and filesystem.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessEnv;

impl EnvSource for ProcessEnv {
    fn var(&self, name: &str) -> Option<String> {
        std::env::var_os(name).and_then(|v| v.into_string().ok())
    }
}

/// An in-memory environment: variables and files are exactly those added.
#[derive(Clone, Debug, Default)]
pub struct MapEnv {
    vars: HashMap<String, String>,
    files: HashMap<PathBuf, Vec<u8>>,
}

impl MapEnv {
    /// An empty environment.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a variable.
    #[must_use]
    pub fn with_var(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.vars.insert(name.into(), value.into());
        self
    }

    /// Adds a file.
    #[must_use]
    pub fn with_file(mut self, path: impl Into<PathBuf>, contents: impl Into<Vec<u8>>) -> Self {
        self.files.insert(path.into(), contents.into());
        self
    }
}

impl EnvSource for MapEnv {
    fn var(&self, name: &str) -> Option<String> {
        self.vars.get(name).cloned()
    }

    fn read_file(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.files.get(path).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "no such file in the test environment",
            )
        })
    }
}
