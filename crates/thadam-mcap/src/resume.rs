//! The MCAP source's resume state: how far each input file was replayed
//! *and durably committed*, so a restart on the same data directory neither
//! re-emits committed messages nor skips uncommitted ones.
//!
//! One JSON file, `resume.json`, in the source's component directory
//! (`<data_dir>/sources/<id>/`). It is replaced atomically
//! ([`thadam_core::write_atomic`]: tmp, fsync, rename, fsync the directory),
//! so a crash leaves the previous state and at worst a torn
//! `.resume.json.tmp-*` that [`ResumeStore::load`] never reads.
//!
//! Per file the state holds the file's identity (path, size, mtime), the
//! position (`next_index`: how many of the file's messages, in file order
//! and before any filtering, are covered by committed batches), and a `done`
//! flag for files read to the end. A state is only trusted when the file's
//! identity and the replay selection (`topics`, `start`, `end`) are
//! unchanged; anything else is read again from the start.

use std::path::Path;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use thadam_core::{ErrorDomain, ThadamError, codes, write_atomic};
use serde::{Deserialize, Serialize};

use crate::error::McapError;

const FILE_NAME: &str = "resume.json";
const VERSION: u32 = 1;

/// One input file: identity plus replay position.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileState {
    pub path: String,
    pub size: u64,
    pub mtime_ns: i64,
    /// Messages of the file covered by committed batches.
    pub next_index: u64,
    /// The file was read to its end and every message of it is covered.
    pub done: bool,
}

impl FileState {
    /// The state of `path` before anything of it was replayed.
    pub(crate) fn fresh(path: &Path) -> Result<Self, McapError> {
        let metadata = std::fs::metadata(path).map_err(|e| McapError::open(path, e))?;
        let mtime_ns = metadata
            .modified()
            .map_err(|e| McapError::open(path, e))?
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
        Ok(FileState {
            path: path.to_string_lossy().into_owned(),
            size: metadata.len(),
            mtime_ns,
            next_index: 0,
            done: false,
        })
    }

    fn same_file(&self, other: &FileState) -> bool {
        (&self.path, self.size, self.mtime_ns) == (&other.path, other.size, other.mtime_ns)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Doc {
    version: u32,
    selection: String,
    files: Vec<FileState>,
}

/// What was loaded from disk: the selection it was written for and its files.
pub(crate) struct Saved {
    selection: String,
    files: Vec<FileState>,
}

/// The resume file of one source.
pub(crate) struct ResumeStore {
    path: PathBuf,
}

impl ResumeStore {
    pub(crate) fn new(dir: &Path) -> Self {
        ResumeStore {
            path: dir.join(FILE_NAME),
        }
    }

    /// The saved state, or `None` when there is none. An unreadable,
    /// malformed, or foreign file is an error, not an empty state: silently
    /// starting over would re-ingest everything. Delete the file to replay
    /// from the start on purpose.
    pub(crate) fn load(&self) -> Result<Option<Saved>, ThadamError> {
        let body = match std::fs::read(&self.path) {
            Ok(body) => body,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(ThadamError::fatal(
                    ErrorDomain::Buffer,
                    codes::BUFFER_IO,
                    format!("read {}: {e}", self.path.display()),
                ));
            }
        };
        let doc: Doc = serde_json::from_slice(&body).map_err(|e| {
            ThadamError::fatal(
                ErrorDomain::Buffer,
                codes::BUFFER_CORRUPT,
                format!(
                    "{} is malformed: {e}; delete it to replay the files from the start",
                    self.path.display()
                ),
            )
        })?;
        if doc.version != VERSION {
            return Err(ThadamError::fatal(
                ErrorDomain::Buffer,
                codes::BUFFER_INCOMPATIBLE,
                format!(
                    "{} has version {}, expected {VERSION}",
                    self.path.display(),
                    doc.version
                ),
            ));
        }
        Ok(Some(Saved {
            selection: doc.selection,
            files: doc.files,
        }))
    }

    pub(crate) fn save(&self, selection: &str, files: &[FileState]) -> Result<(), ThadamError> {
        let doc = Doc {
            version: VERSION,
            selection: selection.to_owned(),
            files: files.to_vec(),
        };
        let body = serde_json::to_vec(&doc).map_err(|e| {
            ThadamError::fatal(
                ErrorDomain::Buffer,
                codes::BUFFER_IO,
                format!("encode resume state: {e}"),
            )
        })?;
        write_atomic(&self.path, &body)
    }
}

/// The starting state of each of `files` (freshly read identities): the
/// saved position where the file and selection are unchanged, the start of
/// the file otherwise. Mismatches are logged, because re-reading re-ingests.
pub(crate) fn plan(saved: Option<Saved>, selection: &str, files: Vec<FileState>) -> Vec<FileState> {
    let Some(saved) = saved else { return files };
    if saved.selection != selection {
        tracing::warn!(
            "the MCAP source's topics or time bounds changed since the last run; replaying every file from the start"
        );
        return files;
    }
    files
        .into_iter()
        .map(|file| match saved.files.iter().find(|s| s.path == file.path) {
            Some(old) if old.same_file(&file) => FileState {
                next_index: old.next_index,
                done: old.done,
                ..file
            },
            Some(_) => {
                tracing::warn!(
                    path = %file.path,
                    "MCAP file changed since the last run (size or modification time); replaying it from the start"
                );
                file
            }
            None => file,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, size: u64, next_index: u64, done: bool) -> FileState {
        FileState {
            path: path.into(),
            size,
            mtime_ns: 1_000,
            next_index,
            done,
        }
    }

    #[test]
    fn save_then_load_round_trips_and_a_missing_file_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = ResumeStore::new(&dir.path().join("sources").join("bag"));
        assert!(store.load().unwrap().is_none());

        let files = vec![file("/a.mcap", 10, 4, false), file("/b.mcap", 20, 9, true)];
        store.save("sel", &files).unwrap();
        let saved = store.load().unwrap().expect("saved");
        assert_eq!((saved.selection.as_str(), saved.files), ("sel", files));
    }

    #[test]
    fn a_torn_tmp_file_is_ignored_and_the_last_complete_state_wins() {
        let dir = tempfile::tempdir().unwrap();
        let store = ResumeStore::new(dir.path());
        store.save("sel", &[file("/a.mcap", 10, 4, false)]).unwrap();
        std::fs::write(
            dir.path().join(".resume.json.tmp-1-1"),
            b"{\"version\":1,\"sel",
        )
        .unwrap();
        let saved = store.load().unwrap().expect("saved");
        assert_eq!(saved.files, vec![file("/a.mcap", 10, 4, false)]);
        store.save("sel", &[file("/a.mcap", 10, 7, false)]).unwrap();
        assert_eq!(store.load().unwrap().unwrap().files[0].next_index, 7);
    }

    #[test]
    fn a_corrupt_or_foreign_state_fails_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let store = ResumeStore::new(dir.path());
        std::fs::write(dir.path().join(FILE_NAME), b"{\"version\":1,\"sel").unwrap();
        assert_eq!(store.load().err().unwrap().code, codes::BUFFER_CORRUPT);
        std::fs::write(
            dir.path().join(FILE_NAME),
            br#"{"version":9,"selection":"","files":[]}"#,
        )
        .unwrap();
        assert_eq!(store.load().err().unwrap().code, codes::BUFFER_INCOMPATIBLE);
    }

    #[test]
    fn plan_trusts_only_unchanged_files_and_selections() {
        let saved = || {
            Some(Saved {
                selection: "sel".into(),
                files: vec![
                    file("/same.mcap", 10, 4, false),
                    file("/grown.mcap", 10, 5, true),
                ],
            })
        };
        let current = vec![
            file("/same.mcap", 10, 0, false),
            file("/grown.mcap", 11, 0, false),
            file("/new.mcap", 1, 0, false),
        ];
        let planned = plan(saved(), "sel", current.clone());
        assert_eq!(
            planned,
            vec![
                file("/same.mcap", 10, 4, false),
                file("/grown.mcap", 11, 0, false),
                file("/new.mcap", 1, 0, false),
            ]
        );

        let mut touched = current.clone();
        touched[0].mtime_ns = 2_000;
        assert_eq!(plan(saved(), "sel", touched.clone()), touched);

        assert_eq!(plan(saved(), "other", current.clone()), current);
        assert_eq!(plan(None, "sel", current.clone()), current);
    }
}
