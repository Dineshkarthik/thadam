//! The data directory: its lock and `thadam.json`.
//!
//! ```text
//! data_dir/
//!   LOCK                 flock held by the runtime for the whole run
//!   thadam.json           runtime metadata (this module)
//!   wal/                 thadam-wal root: its own LOCK, wal.json, segments/, cursors/
//!   sinks/<id>/          state of each segment or batch sink
//!   quarantine/<sink>/   each sink's quarantine instance
//!   sources/<id>/        source state (connectors decide)
//!   schemas/             the schema store (thadam-core): <hash>.schema, content-addressed
//!   services/<id>/       service state
//!   replays/<id>/        replay output, never the production directories
//! ```
//!
//! See the crate documentation for what `thadam.json` holds and why it does not
//! repeat `wal.json`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use thadam_core::{ErrorDomain, ThadamError, WalEpoch, codes};
use serde::{Deserialize, Serialize};

/// The file name of the runtime's metadata in the data directory.
pub const THADAM_JSON: &str = "thadam.json";
/// The `kind` field of `thadam.json`.
pub const THADAM_JSON_KIND: &str = "thadam-data-dir";
/// The version of `thadam.json` and of the data-directory layout above.
pub const RUNTIME_FORMAT_VERSION: u32 = 1;
/// The version of how the runtime uses WAL cursors: one cursor per sink named
/// `sink.<id>`, `committed` meaning delivered (segment sinks: covered by a
/// finalized file), and `inflight` holding a batch sink's unacknowledged
/// batch id.
pub const CURSOR_LAYOUT_VERSION: u32 = 1;
/// The segment catalog format this binary expects `thadam-segments` to write.
pub const CATALOG_FORMAT_VERSION: u32 = 1;
/// The WAL root inside the data directory.
pub const WAL_DIR: &str = "wal";
/// The schema-store root inside the data directory
/// ([`thadam_core::SchemaStore`]): durable, content-addressed message
/// schemas written by sources before messages referencing them reach the
/// WAL, read back by chain decoders (and exports) by hash.
pub const SCHEMAS_DIR: &str = "schemas";

const LOCK_FILE: &str = "LOCK";

/// Contents of `data_dir/thadam.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ThadamJson {
    /// Always [`THADAM_JSON_KIND`].
    pub kind: String,
    /// [`RUNTIME_FORMAT_VERSION`] of the binary that wrote it.
    pub format_version: u32,
    /// [`CURSOR_LAYOUT_VERSION`] of the binary that wrote it.
    pub cursor_layout_version: u32,
    /// [`CATALOG_FORMAT_VERSION`] of the binary that wrote it.
    pub catalog_format_version: u32,
    /// The pipeline name. A different name on restart is refused.
    pub pipeline: String,
    /// The config hash of the last run (`thadam_config` hash, hexadecimal).
    pub config_hash: String,
    /// The version of the config hash algorithm.
    pub config_hash_version: u32,
    /// The config hash of the run before, when the configuration changed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_config_hash: Option<String>,
    /// The WAL whose positions every sink's recovered state refers to.
    pub wal: WalBinding,
    /// When the directory was initialized, Unix seconds.
    pub created_unix: u64,
    /// When this file was last written, Unix seconds.
    pub updated_unix: u64,
}

/// A reference to the WAL's identity, which `wal/wal.json` owns.
///
/// Only the epoch is recorded, as a binding check: sink state (finalized
/// segments, batch-sink progress) stores bare WAL positions, which are only
/// meaningful within one epoch. If the WAL directory were replaced (a new
/// epoch starting again at position 1), resuming sinks from their old
/// positions would skip new data silently, so the runtime refuses that unless
/// `--repair` is given.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct WalBinding {
    /// The WAL root, relative to the data directory.
    pub dir: String,
    /// The epoch of `wal/wal.json`, hexadecimal, once the WAL has been opened.
    pub epoch: Option<String>,
}

/// Holds `data_dir/LOCK` for as long as it lives.
#[derive(Debug)]
pub struct DataDirLock {
    pub(crate) _file: File,
}

fn data_dir_error(message: String) -> ThadamError {
    ThadamError::fatal(ErrorDomain::Buffer, codes::BUFFER_INCOMPATIBLE, message)
}

fn io_error(op: &str, path: &Path, e: &io::Error) -> ThadamError {
    ThadamError::fatal(
        ErrorDomain::Buffer,
        codes::BUFFER_IO,
        format!("{op} {}: {e}", path.display()),
    )
}

/// Creates `root` if needed and takes its lock, failing fast if another
/// process holds it.
///
/// This is the data-directory lock used by `thadam run` and `thadam replay` to
/// prevent concurrent access. An export must also take this lock to refuse
/// running while a pipeline holds the directory.
pub fn lock(root: &Path) -> Result<DataDirLock, ThadamError> {
    fs::create_dir_all(root).map_err(|e| io_error("create data directory", root, &e))?;
    let path = root.join(LOCK_FILE);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|e| io_error("open", &path, &e))?;
    match rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(DataDirLock { _file: file }),
        Err(e) if e == rustix::io::Errno::WOULDBLOCK => Err(data_dir_error(format!(
            "data directory {} is locked by another thadam process",
            root.display()
        ))),
        Err(e) => Err(io_error("lock", &path, &e.into())),
    }
}

/// Reads `thadam.json`, if present.
pub fn read_thadam_json(root: &Path) -> Result<Option<ThadamJson>, ThadamError> {
    let path = root.join(THADAM_JSON);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_error("read", &path, &e)),
    };
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| data_dir_error(format!("{} is not valid JSON: {e}", path.display())))?;
    if value.get("kind").and_then(|k| k.as_str()) != Some(THADAM_JSON_KIND) {
        return Err(data_dir_error(format!(
            "{} is not a thadam data directory file (kind is not `{THADAM_JSON_KIND}`)",
            path.display()
        )));
    }
    let version = value
        .get("format_version")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    if version != u64::from(RUNTIME_FORMAT_VERSION) {
        return Err(data_dir_error(format!(
            "{} has format version {version}; this binary reads version {RUNTIME_FORMAT_VERSION}",
            path.display()
        )));
    }
    serde_json::from_value(value)
        .map(Some)
        .map_err(|e| data_dir_error(format!("{} is malformed: {e}", path.display())))
}

pub(crate) fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn epoch_hex(epoch: WalEpoch) -> String {
    format!("{:016x}", epoch.get())
}

/// Checks what can be checked before the WAL opens: versions and the pipeline
/// name.
pub(crate) fn check_before_wal(
    existing: Option<&ThadamJson>,
    pipeline: &str,
    repair: bool,
) -> Result<Vec<String>, ThadamError> {
    let Some(meta) = existing else {
        return Ok(Vec::new());
    };
    let mut repaired = Vec::new();
    for (what, found, known) in [
        (
            "cursor layout",
            meta.cursor_layout_version,
            CURSOR_LAYOUT_VERSION,
        ),
        (
            "catalog format",
            meta.catalog_format_version,
            CATALOG_FORMAT_VERSION,
        ),
    ] {
        if found != known {
            return Err(data_dir_error(format!(
                "thadam.json records {what} version {found}; this binary uses version {known}"
            )));
        }
    }
    if meta.pipeline != pipeline {
        let msg = format!(
            "the data directory belongs to pipeline `{}`, not `{pipeline}`",
            meta.pipeline
        );
        if !repair {
            return Err(data_dir_error(format!(
                "{msg}; use a different data directory or pass --repair"
            )));
        }
        repaired.push(msg);
    }
    Ok(repaired)
}

/// Checks the WAL binding once the WAL is open.
pub(crate) fn check_wal_binding(
    existing: Option<&ThadamJson>,
    epoch: WalEpoch,
    repair: bool,
) -> Result<Vec<String>, ThadamError> {
    let Some(bound) = existing.and_then(|m| m.wal.epoch.as_deref()) else {
        return Ok(Vec::new());
    };
    let now = epoch_hex(epoch);
    if bound == now {
        return Ok(Vec::new());
    }
    let msg = format!(
        "the WAL epoch is {now} but sink state was written against epoch {bound} \
         (the WAL directory was replaced); resuming would skip data"
    );
    if repair {
        Ok(vec![msg])
    } else {
        Err(data_dir_error(format!("{msg}; pass --repair to accept it")))
    }
}

/// The `thadam.json` to write for this run.
pub(crate) fn next_thadam_json(
    existing: Option<&ThadamJson>,
    pipeline: &str,
    config_hash: &str,
    epoch: WalEpoch,
) -> ThadamJson {
    let now = unix_now();
    let previous_config_hash = match existing {
        Some(m) if m.config_hash != config_hash => Some(m.config_hash.clone()),
        Some(m) => m.previous_config_hash.clone(),
        None => None,
    };
    ThadamJson {
        kind: THADAM_JSON_KIND.to_owned(),
        format_version: RUNTIME_FORMAT_VERSION,
        cursor_layout_version: CURSOR_LAYOUT_VERSION,
        catalog_format_version: CATALOG_FORMAT_VERSION,
        pipeline: pipeline.to_owned(),
        config_hash: config_hash.to_owned(),
        config_hash_version: thadam_config::CONFIG_HASH_VERSION,
        previous_config_hash,
        wal: WalBinding {
            dir: WAL_DIR.to_owned(),
            epoch: Some(epoch_hex(epoch)),
        },
        created_unix: existing.map_or(now, |m| m.created_unix),
        updated_unix: now,
    }
}

/// Writes `thadam.json` atomically: temporary file, flush, rename, directory
/// flush.
pub(crate) fn write_thadam_json(root: &Path, meta: &ThadamJson) -> Result<(), ThadamError> {
    let path = root.join(THADAM_JSON);
    let tmp = root.join(format!("{THADAM_JSON}.tmp"));
    let mut text = serde_json::to_vec_pretty(meta)
        .map_err(|e| ThadamError::fatal(ErrorDomain::Buffer, codes::BUFFER_IO, e.to_string()))?;
    text.push(b'\n');
    let write = || -> io::Result<()> {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&text)?;
        sync_file(&f)?;
        drop(f);
        fs::rename(&tmp, &path)?;
        sync_dir(root)
    };
    write().map_err(|e| io_error("write", &path, &e))
}

pub(crate) fn sync_file(f: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    rustix::fs::fcntl_fullfsync(f)?;
    #[cfg(not(target_vendor = "apple"))]
    rustix::fs::fdatasync(f)?;
    Ok(())
}

pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    let d = File::open(dir)?;
    #[cfg(target_vendor = "apple")]
    rustix::fs::fcntl_fullfsync(&d)?;
    #[cfg(not(target_vendor = "apple"))]
    rustix::fs::fsync(&d)?;
    Ok(())
}

/// Directory layout helpers.
#[derive(Clone, Debug)]
pub(crate) struct Layout {
    pub root: PathBuf,
}

impl Layout {
    pub fn wal(&self) -> PathBuf {
        self.root.join(WAL_DIR)
    }
    pub fn schemas(&self) -> PathBuf {
        self.root.join(SCHEMAS_DIR)
    }
    pub fn sink(&self, id: &str) -> PathBuf {
        self.root.join("sinks").join(id)
    }
    pub fn quarantine(&self, sink: &str) -> PathBuf {
        self.root.join("quarantine").join(sink)
    }
    pub fn source(&self, id: &str) -> PathBuf {
        self.root.join("sources").join(id)
    }
    pub fn service(&self, id: &str) -> PathBuf {
        self.root.join("services").join(id)
    }
    pub fn replay(&self, replay_id: &str, sink: &str) -> PathBuf {
        self.root.join("replays").join(replay_id).join(sink)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_lock_fails_fast_and_release_frees_it() {
        let dir = tempfile::tempdir().unwrap();
        let first = lock(dir.path()).unwrap();
        let err = lock(dir.path()).unwrap_err();
        assert_eq!(err.code, codes::BUFFER_INCOMPATIBLE);
        drop(first);
        lock(dir.path()).unwrap();
    }

    #[test]
    fn thadam_json_round_trips_and_checks_bindings() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_thadam_json(dir.path()).unwrap(), None);
        let meta = next_thadam_json(None, "p", "aa", WalEpoch::new(7));
        write_thadam_json(dir.path(), &meta).unwrap();
        let back = read_thadam_json(dir.path()).unwrap().unwrap();
        assert_eq!(back, meta);
        assert_eq!(back.wal.epoch.as_deref(), Some("0000000000000007"));

        assert!(
            check_before_wal(Some(&back), "p", false)
                .unwrap()
                .is_empty()
        );
        assert!(check_before_wal(Some(&back), "q", false).is_err());
        assert_eq!(check_before_wal(Some(&back), "q", true).unwrap().len(), 1);

        assert!(
            check_wal_binding(Some(&back), WalEpoch::new(7), false)
                .unwrap()
                .is_empty()
        );
        let err = check_wal_binding(Some(&back), WalEpoch::new(8), false).unwrap_err();
        assert_eq!(err.code, codes::BUFFER_INCOMPATIBLE);
        assert_eq!(
            check_wal_binding(Some(&back), WalEpoch::new(8), true)
                .unwrap()
                .len(),
            1
        );

        let next = next_thadam_json(Some(&back), "p", "bb", WalEpoch::new(7));
        assert_eq!(next.previous_config_hash.as_deref(), Some("aa"));
        assert_eq!(next.created_unix, back.created_unix);
    }

    #[test]
    fn unknown_versions_and_kinds_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join(THADAM_JSON),
            br#"{"kind":"thadam-data-dir","format_version":99}"#,
        )
        .unwrap();
        let err = read_thadam_json(dir.path()).unwrap_err();
        assert!(err.message.contains("format version 99"), "{err}");
        fs::write(dir.path().join(THADAM_JSON), br#"{"kind":"other"}"#).unwrap();
        assert!(read_thadam_json(dir.path()).is_err());
    }
}
