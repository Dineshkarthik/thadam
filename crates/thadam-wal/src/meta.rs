//! `wal.json`: the WAL directory's identity and format versions.
//!
//! Written once, atomically, when the WAL is created. Every open checks it, and
//! an older binary refuses a newer format instead of guessing.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use thadam_core::WalEpoch;

use crate::cursor::CURSOR_FORMAT_VERSION;
use crate::error::WalError;
use crate::format::{FRAME_VERSION, SEG_FORMAT_VERSION};
use crate::fsutil::write_atomic;

pub(crate) const META_FILE: &str = "wal.json";
const KIND: &str = "thadam-wal";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Meta {
    pub epoch: WalEpoch,
    pub node_id: Option<String>,
    pub machine_id: Option<String>,
}

fn to_json(m: &Meta) -> Value {
    json!({
        "kind": KIND,
        "wal_format_version": SEG_FORMAT_VERSION,
        "frame_format_version": FRAME_VERSION,
        "cursor_format_version": CURSOR_FORMAT_VERSION,
        "epoch": format!("{:016x}", m.epoch.get()),
        "created_unix": unix_now(),
        "node_id": m.node_id,
        "machine_id": m.machine_id,
    })
}

pub(crate) fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn random_epoch() -> u64 {
    // RandomState is seeded from OS randomness; mix in time and pid so two
    // directories created in the same instant still differ.
    let mut h = RandomState::new().build_hasher();
    h.write_u128(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    h.write_u32(std::process::id());
    h.finish().max(1)
}

/// Creates `wal.json` with a fresh epoch.
pub(crate) fn create(
    dir: &Path,
    node_id: Option<String>,
    machine_id: Option<String>,
) -> Result<Meta, WalError> {
    let meta = Meta {
        epoch: WalEpoch::new(random_epoch()),
        node_id,
        machine_id,
    };
    let path = dir.join(META_FILE);
    let text = serde_json::to_vec_pretty(&to_json(&meta))
        .map_err(|e| WalError::io("encode", &path, io::Error::other(e)))?;
    write_atomic(&path, &text).map_err(|e| WalError::io("create", &path, e))?;
    Ok(meta)
}

/// Reads and checks `wal.json`. `Ok(None)` if it does not exist.
pub(crate) fn load(dir: &Path) -> Result<Option<Meta>, WalError> {
    let path = dir.join(META_FILE);
    let text = match std::fs::read(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(WalError::io("read", &path, e)),
    };
    let corrupt = |why: &str| WalError::Corrupt(format!("{}: {why}", path.display()));
    let v: Value = serde_json::from_slice(&text).map_err(|_| corrupt("not valid JSON"))?;
    if v.get("kind").and_then(Value::as_str) != Some(KIND) {
        return Err(corrupt("not a thadam wal metadata file"));
    }
    for (key, supported) in [
        ("wal_format_version", u64::from(SEG_FORMAT_VERSION)),
        ("frame_format_version", u64::from(FRAME_VERSION)),
        ("cursor_format_version", u64::from(CURSOR_FORMAT_VERSION)),
    ] {
        let found = v
            .get(key)
            .and_then(Value::as_u64)
            .ok_or_else(|| corrupt(&format!("missing {key}")))?;
        if found != supported {
            let relation = if found > supported {
                "newer than"
            } else {
                "different from"
            };
            return Err(WalError::Incompatible(format!(
                "{}: {key} {found} is {relation} the supported version {supported}",
                path.display()
            )));
        }
    }
    let epoch = v
        .get("epoch")
        .and_then(Value::as_str)
        .and_then(|s| u64::from_str_radix(s, 16).ok())
        .filter(|&e| e != 0)
        .ok_or_else(|| corrupt("missing or invalid epoch"))?;
    let opt = |key: &str| v.get(key).and_then(Value::as_str).map(str::to_owned);
    Ok(Some(Meta {
        epoch: WalEpoch::new(epoch),
        node_id: opt("node_id"),
        machine_id: opt("machine_id"),
    }))
}

/// Refuses a directory bound to a different machine or node.
pub(crate) fn check_binding(
    meta: &Meta,
    node_id: Option<&str>,
    machine_id: Option<&str>,
) -> Result<(), WalError> {
    for (what, stored, given) in [
        ("machine_id", meta.machine_id.as_deref(), machine_id),
        ("node_id", meta.node_id.as_deref(), node_id),
    ] {
        if let (Some(stored), Some(given)) = (stored, given)
            && stored != given
        {
            return Err(WalError::Incompatible(format!(
                "wal directory is bound to {what} {stored:?}, this process has {given:?}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_then_load() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(load(dir.path()).unwrap(), None);
        let m = create(dir.path(), Some("n1".into()), Some("m1".into())).unwrap();
        assert_eq!(load(dir.path()).unwrap(), Some(m.clone()));
        assert!(check_binding(&m, Some("n1"), Some("m1")).is_ok());
        assert!(check_binding(&m, None, None).is_ok());
        assert!(matches!(
            check_binding(&m, None, Some("other")),
            Err(WalError::Incompatible(_))
        ));
        assert!(check_binding(&m, Some("n2"), None).is_err());
    }

    #[test]
    fn newer_versions_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        create(dir.path(), None, None).unwrap();
        let path = dir.path().join(META_FILE);
        let mut v: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        v["wal_format_version"] = json!(2);
        std::fs::write(&path, serde_json::to_vec(&v).unwrap()).unwrap();
        let err = load(dir.path()).unwrap_err();
        assert!(matches!(err, WalError::Incompatible(_)), "{err}");
        assert!(err.to_string().contains("newer than"));
    }

    #[test]
    fn garbage_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(META_FILE), b"{nope").unwrap();
        assert!(matches!(load(dir.path()), Err(WalError::Corrupt(_))));
    }

    #[test]
    fn epochs_differ() {
        assert_ne!(random_epoch(), random_epoch());
    }
}
