//! A temporary data directory.

use std::io;
use std::path::{Path, PathBuf};

use tempfile::TempDir;

/// A directory removed when dropped, for tests that need a data dir.
#[derive(Debug)]
pub struct TempDataDir {
    dir: TempDir,
}

impl TempDataDir {
    /// Creates a fresh empty directory under the system temp dir.
    pub fn new() -> io::Result<Self> {
        Ok(TempDataDir {
            dir: tempfile::Builder::new().prefix("thadam-test-").tempdir()?,
        })
    }

    /// The directory path.
    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    /// A path inside the directory.
    pub fn join(&self, name: impl AsRef<Path>) -> PathBuf {
        self.dir.path().join(name)
    }

    /// Creates (and returns the path of) a subdirectory.
    pub fn create_dir(&self, name: impl AsRef<Path>) -> io::Result<PathBuf> {
        let path = self.join(name);
        std::fs::create_dir_all(&path)?;
        Ok(path)
    }

    /// Stops the directory from being deleted on drop and returns its path.
    pub fn keep(self) -> PathBuf {
        self.dir.keep()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directory_is_created_and_removed_on_drop() {
        let dir = TempDataDir::new().unwrap();
        let path = dir.path().to_owned();
        assert!(path.is_dir());
        let sub = dir.create_dir("wal/segments").unwrap();
        assert!(sub.is_dir());
        std::fs::write(dir.join("thadam.json"), b"{}").unwrap();
        drop(dir);
        assert!(!path.exists());
    }

    #[test]
    fn kept_directories_survive_drop() {
        let path = TempDataDir::new().unwrap().keep();
        assert!(path.is_dir());
        std::fs::remove_dir_all(path).unwrap();
    }
}
