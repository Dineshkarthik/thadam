//! Paths inside a data directory for the commands that read it. They mirror
//! the layout `thadam-runtime` writes; its own helpers are crate-private.

use std::path::{Path, PathBuf};

const WAL_DIR: &str = "wal";

/// The WAL directory under `root`, if `root` holds a WAL.
pub fn wal_dir(root: &Path) -> Result<PathBuf, String> {
    let dir = root.join(WAL_DIR);
    if dir.join("wal.json").is_file() {
        Ok(dir)
    } else {
        Err(format!(
            "{} holds no thadam WAL (expected {WAL_DIR}/wal.json)",
            root.display()
        ))
    }
}

/// The directory holding `sink`'s catalog under `root`, if the sink has
/// written there.
pub fn sink_dir(root: &Path, sink: &str) -> Result<PathBuf, String> {
    if !valid_sink_id(sink) {
        return Err(format!("`{sink}` is not a sink id"));
    }
    let dir = root.join("sinks").join(sink);
    if dir.is_dir() {
        Ok(dir)
    } else {
        Err(format!(
            "no state for sink `{sink}` under {}",
            root.display()
        ))
    }
}

fn valid_sink_id(sink: &str) -> bool {
    !sink.is_empty() && !sink.contains(['/', '\\']) && sink != "." && sink != ".."
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_ids_cannot_leave_the_sinks_directory() {
        assert!(!valid_sink_id("../wal"));
        assert!(!valid_sink_id("a/b"));
        assert!(!valid_sink_id(".."));
        assert!(!valid_sink_id(""));
        assert!(valid_sink_id("pq"));
    }
}
