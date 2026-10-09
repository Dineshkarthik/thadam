//! What `thadam run` left on disk: Parquet files and bytes under the sink
//! directory, walked after shutdown so the files are final.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Totals of Parquet output under a sink directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Storage {
    /// Number of `*.parquet` files.
    pub parquet_files: u64,
    /// Their combined size in bytes.
    pub parquet_bytes: u64,
}

/// Counts `*.parquet` files under `dir`, recursively. A missing directory is
/// zero files, not an error: a run that wrote nothing should say so.
///
/// # Errors
///
/// Returns [`crate::error::BenchError::Io`] when an existing directory cannot
/// be read.
pub fn walk_parquet(dir: &Path) -> Result<Storage> {
    let mut totals = Storage {
        parquet_files: 0,
        parquet_bytes: 0,
    };
    if !dir.exists() {
        return Ok(totals);
    }
    let mut pending = vec![dir.to_path_buf()];
    while let Some(current) = pending.pop() {
        for entry in std::fs::read_dir(&current)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file()
                && entry.path().extension().is_some_and(|e| e == "parquet")
            {
                totals.parquet_files += 1;
                totals.parquet_bytes += entry.metadata()?.len();
            }
        }
    }
    Ok(totals)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn counts_only_parquet_files_recursively() {
        let dir = std::env::temp_dir().join(format!("thadam-bench-walk-{}", std::process::id()));
        let nested = dir.join("ready/telemetry");
        std::fs::create_dir_all(&nested).expect("mkdir");
        std::fs::write(nested.join("a.parquet"), vec![0_u8; 100]).expect("write");
        std::fs::write(nested.join("b.parquet"), vec![0_u8; 23]).expect("write");
        std::fs::write(nested.join("c.tmp"), vec![0_u8; 999]).expect("write");

        let totals = walk_parquet(&dir).expect("walk");
        assert_eq!(totals.parquet_files, 2);
        assert_eq!(totals.parquet_bytes, 123);
        assert_eq!(
            walk_parquet(&dir.join("absent")).expect("missing dir"),
            Storage {
                parquet_files: 0,
                parquet_bytes: 0
            }
        );
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
