//! The Sparkplug birth store: the latest BIRTH payload of every Sparkplug
//! edge node and device under the data directory, so a decoder restarted on
//! a WAL range that no longer contains the BIRTH can still resolve
//! alias-only DATA metrics (design: docs/design/spikes/
//! sparkplug-b-and-opcua-pubsub.md §4.2, owner decision O-2).
//!
//! Why this exists. Sparkplug B DATA messages carry metric **aliases**, not
//! names; the alias → (name, datatype) table is published once per session
//! in an NBIRTH/DBIRTH certificate (Sparkplug specification 3.0.0, chapter
//! 6). WAL retention may trim the BIRTH while DATA from the same session is
//! still replayed, so each BIRTH is also written here as a **seed**: the raw
//! protobuf bytes, the session `bdSeq`, the `seq` the BIRTH carried, and a
//! dead flag an NDEATH/DDEATH sets. The WAL remains the truth — this store
//! is a cache and may lag the newest committed BIRTH by one crash window.
//! The stored `bdSeq` and `seq` let a seeded decoder notice that (a DATA seq
//! inconsistent with the seed is flagged as a gap like any other) and the
//! next BIRTH resyncs both sides.
//!
//! # On-disk layout
//!
//! One directory (the runtime places it at `<data_dir>/sparkplug/births/`,
//! but this type takes whatever directory it is given), one file per
//! (group, edge node, device) scope, named by the SHA-256 of the key:
//!
//! ```text
//! <dir>/
//!   <sha256-hex>.birth     the latest record for one scope
//!   .<sha256-hex>.tmp-*    in-flight writes (ignored by reads)
//! ```
//!
//! Each `.birth` file is a length-framed record in the schema-store framing:
//!
//! ```text
//! thadam/sparkplug-birth 1\n
//! group <len>\n<group bytes>
//! node <len>\n<node bytes>
//! device <len>\n<device id bytes, empty for a node-scope entry>
//! bdseq <len>\n<0x00 | 0x01 + u64 little-endian>
//! seq <len>\n<0x00 | 0x01 + u64 little-endian>
//! dead <len>\n<0x00 | 0x01>
//! payload <len>\n<raw BIRTH protobuf bytes>
//! sha256 32\n<32 raw digest bytes over everything above>
//! ```
//!
//! `bdseq`/`seq` are flag-prefixed because a spec-violating BIRTH can lack
//! them. `seq` is the value the BIRTH itself carried (per spec an NBIRTH's
//! is 0) — a seed for the decoder's live cursor, not a live cursor itself.
//! There is deliberately **no per-DATA `record_seq` call**: persisting every
//! seq would tmp+rename+fsync+fsync per DATA message, exactly the
//! write-amplification cliff the WAL's group commit exists to avoid, and it
//! buys nothing the seed needs. The live cursor stays decoder memory.
//!
//! # Writes and reads
//!
//! [`BirthStore::put`] and [`BirthStore::mark_dead`] write through with the
//! `SchemaStore` discipline — tmp file, fsync, rename, fsync the directory —
//! so a crash can only leave a torn `.tmp-*`, which reads ignore. Both are
//! rare (one per BIRTH/DEATH), so write-through is cheap. Reads verify: the
//! embedded key must hash to the file name (a record stored under the wrong
//! name is `buffer.corrupt`, not the wrong scope's table) and the trailing
//! digest must cover the record, so bit rot and truncation fail loudly.
//!
//! Single-writer, same as [`crate::schema_store::SchemaStore`]: the
//! data-directory lock keeps other processes out and one decoder owns the
//! store for the run.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sha2::{Digest, Sha256};

use crate::codes;
use crate::error::{ErrorDomain, ThadamError};
use crate::schema_store::{corrupt, hex_of, io_error, is_hash, read_field, sync_dir, sync_file};

/// The first line of every `.birth` file.
const MAGIC: &str = "thadam/sparkplug-birth 1";
/// Prepended to the hashed key parts so a birth-key hash cannot collide
/// with a hash of the same bytes computed for another purpose.
const KEY_DOMAIN: &[u8] = b"thadam/sparkplug-birth 1 key\0";
/// Upper bound for the group/node/device fields: far beyond any legal MQTT
/// topic segment; keeps a corrupt length field from becoming an allocation.
const MAX_LABEL_LEN: u64 = 1024;
/// Upper bound for one stored BIRTH payload; same posture as the schema
/// store's `MAX_TEXT_LEN`.
const MAX_PAYLOAD_LEN: u64 = 16 * 1024 * 1024;

/// Counter making temporary file names unique between writers in one
/// process (the data-directory lock excludes other processes).
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The scope one BIRTH certificate defines: an edge node of a group, or one
/// device below it (`spBv1.0/<group>/<kind>/<node>[/<device>]`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BirthKey {
    /// The Sparkplug group id.
    pub group: Box<str>,
    /// The edge node id.
    pub node: Box<str>,
    /// The device id; `None` is the edge node's own scope.
    pub device: Option<Box<str>>,
}

impl BirthKey {
    /// Builds a key; `device: None` addresses the node's own scope.
    pub fn new(group: &str, node: &str, device: Option<&str>) -> Self {
        BirthKey {
            group: group.into(),
            node: node.into(),
            device: device.map(Into::into),
        }
    }

    /// The hash naming this key's file.
    fn hash(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(KEY_DOMAIN);
        for part in [
            &*self.group,
            &*self.node,
            self.device.as_deref().unwrap_or(""),
        ] {
            digest.update(part.as_bytes());
            digest.update([0]);
        }
        hex_of(&digest.finalize())
    }
}

/// One stored record: a scope's raw BIRTH bytes plus the small header a
/// seeded decoder needs before it replays a single WAL message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BirthRecord {
    /// The BIRTH certificate's protobuf payload exactly as published —
    /// self-describing, so it is only ever re-decoded, never re-serialized.
    pub birth: Vec<u8>,
    /// The session counter the BIRTH announced (its `bdSeq` metric);
    /// `None` when a spec-violating BIRTH carried none.
    pub bd_seq: Option<u64>,
    /// The `seq` the BIRTH carried: the seed for the decoder's live seq
    /// cursor. The store does not track the cursor after this; the first
    /// seq that does not continue from it (e.g. across a crash window the
    /// snapshot missed) surfaces as an ordinary seq gap.
    pub seq: Option<u64>,
    /// Set once an NDEATH/DDEATH marked the scope stale; cleared by the
    /// next BIRTH for the key (the `put` stores `dead: false`).
    pub dead: bool,
}

/// A keyed store of Sparkplug birth records in one directory.
///
/// Cheap to construct: [`BirthStore::open`] touches nothing on disk. The
/// directory is created on the first write; a store whose directory does
/// not exist reads as empty.
#[derive(Clone, Debug)]
pub struct BirthStore {
    dir: PathBuf,
}

impl BirthStore {
    /// A store rooted at `dir`. Takes the store directory itself, not the
    /// data directory: the caller owns the layout (the runtime uses
    /// `<data_dir>/sparkplug/births/`).
    pub fn open(dir: impl Into<PathBuf>) -> Self {
        BirthStore { dir: dir.into() }
    }

    /// The directory the store was opened on.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Stores `record` as the latest state of `key`, replacing any earlier
    /// one. BIRTHs and rebirths write `dead: false`, clearing staleness.
    ///
    /// Idempotent. A write failure is a classified `buffer.io` error; it is
    /// never swallowed.
    pub fn put(&self, key: &BirthKey, record: &BirthRecord) -> Result<(), ThadamError> {
        // Refuse what `load_all` would refuse to read back: a record that is
        // durable but unloadable would fail every later restart.
        let device = key.device.as_deref().unwrap_or("");
        if [&*key.group, &*key.node, device]
            .iter()
            .any(|label| label.len() as u64 > MAX_LABEL_LEN)
            || record.birth.len() as u64 > MAX_PAYLOAD_LEN
        {
            return Err(ThadamError::permanent(
                ErrorDomain::Decode,
                codes::DECODE_TOO_LARGE,
                format!(
                    "sparkplug birth record for {}/{}/{device} exceeds the store limits",
                    key.group, key.node
                ),
            ));
        }
        let hash = key.hash();
        let body = encode_record(key, record);
        fs::create_dir_all(&self.dir)
            .map_err(|e| io_error("create sparkplug birth store directory", &self.dir, &e))?;
        let unique = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = self
            .dir
            .join(format!(".{hash}.tmp-{}-{unique}", std::process::id()));
        let path = self.dir.join(format!("{hash}.birth"));
        let write = || -> io::Result<()> {
            {
                let mut f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&tmp)?;
                f.write_all(&body)?;
                sync_file(&f)?;
            }
            fs::rename(&tmp, &path)?;
            sync_dir(&self.dir)
        };
        write().map_err(|e| io_error("write sparkplug birth record", &path, &e))
    }

    /// Marks `key`'s stored record dead (NDEATH/DDEATH), rewriting the file
    /// only when the flag changes. A key with no stored record is a no-op:
    /// there is no table to resurrect for a scope no BIRTH ever defined, and
    /// the next BIRTH creates the entry fresh.
    pub fn mark_dead(&self, key: &BirthKey) -> Result<(), ThadamError> {
        let hash = key.hash();
        let Some((_, mut record)) = self.read_record(&hash)? else {
            return Ok(());
        };
        if record.dead {
            return Ok(());
        }
        record.dead = true;
        self.put(key, &record)
    }

    /// Every stored record, `(key, record)` pairs ordered by key (hash), so
    /// seeding replays deterministically. Temporary files and any other
    /// non-entry are skipped; a corrupt entry is an error, never silently
    /// dropped state.
    pub fn load_all(&self) -> Result<Vec<(BirthKey, BirthRecord)>, ThadamError> {
        let read = match fs::read_dir(&self.dir) {
            Ok(r) => r,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                return Err(io_error(
                    "read sparkplug birth store directory",
                    &self.dir,
                    &e,
                ));
            }
        };
        let mut all: BTreeMap<String, (BirthKey, BirthRecord)> = BTreeMap::new();
        for entry in read {
            let entry = entry
                .map_err(|e| io_error("read sparkplug birth store directory", &self.dir, &e))?;
            let name = entry.file_name();
            let Some(hash) = name.to_str().and_then(|n| n.strip_suffix(".birth")) else {
                continue;
            };
            // Content verification re-hashes; skip names that cannot be
            // right so the tmp files of an interrupted write never surface
            // as errors here.
            if !is_hash(hash) {
                continue;
            }
            let Some(record) = self.read_record(hash)? else {
                continue;
            };
            all.insert(hash.to_owned(), record);
        }
        Ok(all.into_values().collect())
    }

    /// Reads and verifies the record named by `hash`; `None` when the store
    /// has no such entry.
    fn read_record(&self, hash: &str) -> Result<Option<(BirthKey, BirthRecord)>, ThadamError> {
        let path = self.dir.join(format!("{hash}.birth"));
        let mut body = Vec::new();
        match File::open(&path).and_then(|mut f| f.read_to_end(&mut body)) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error("read sparkplug birth record", &path, &e)),
        }
        let (key, record) = decode_record(&body, &path)?;
        if key.hash() != hash {
            return Err(corrupt(
                &path,
                "the stored key does not hash to the file name (the record belongs to another scope)"
                    .into(),
            ));
        }
        Ok(Some((key, record)))
    }
}

fn encode_record(key: &BirthKey, record: &BirthRecord) -> Vec<u8> {
    let mut out = Vec::with_capacity(record.birth.len() + 160);
    out.extend_from_slice(MAGIC.as_bytes());
    out.push(b'\n');
    write_field(&mut out, "group", key.group.as_bytes());
    write_field(&mut out, "node", key.node.as_bytes());
    write_field(
        &mut out,
        "device",
        key.device.as_deref().unwrap_or("").as_bytes(),
    );
    write_field(&mut out, "bdseq", &encode_opt_u64(record.bd_seq));
    write_field(&mut out, "seq", &encode_opt_u64(record.seq));
    write_field(&mut out, "dead", &[u8::from(record.dead)]);
    write_field(&mut out, "payload", &record.birth);
    let digest = Sha256::digest(&out);
    write_field(&mut out, "sha256", &digest);
    out
}

fn write_field(out: &mut Vec<u8>, key: &str, value: &[u8]) {
    out.extend_from_slice(key.as_bytes());
    out.push(b' ');
    out.extend_from_slice(value.len().to_string().as_bytes());
    out.push(b'\n');
    out.extend_from_slice(value);
}

fn encode_opt_u64(value: Option<u64>) -> Vec<u8> {
    match value {
        None => vec![0],
        Some(v) => {
            let mut out = Vec::with_capacity(9);
            out.push(1);
            out.extend_from_slice(&v.to_le_bytes());
            out
        }
    }
}

fn decode_opt_u64(field: &[u8], path: &Path) -> Result<Option<u64>, ThadamError> {
    match field {
        [0] => Ok(None),
        [1, a, b, c, d, e, f, g, h] => {
            Ok(Some(u64::from_le_bytes([*a, *b, *c, *d, *e, *f, *g, *h])))
        }
        _ => Err(corrupt(path, "an integer field is malformed".into())),
    }
}

fn decode_record(body: &[u8], path: &Path) -> Result<(BirthKey, BirthRecord), ThadamError> {
    let magic = format!("{MAGIC}\n");
    if !body.starts_with(magic.as_bytes()) {
        return Err(ThadamError::fatal(
            ErrorDomain::Buffer,
            codes::BUFFER_INCOMPATIBLE,
            format!("{} is not a thadam sparkplug-birth file", path.display()),
        ));
    }
    let mut rest = &body[magic.len()..];
    let group = read_field(&mut rest, "group", path, MAX_LABEL_LEN)?;
    let node = read_field(&mut rest, "node", path, MAX_LABEL_LEN)?;
    let device = read_field(&mut rest, "device", path, MAX_LABEL_LEN)?;
    let bd_seq = decode_opt_u64(read_field(&mut rest, "bdseq", path, 9)?, path)?;
    let seq = decode_opt_u64(read_field(&mut rest, "seq", path, 9)?, path)?;
    let dead = match read_field(&mut rest, "dead", path, 1)? {
        [0] => false,
        [1] => true,
        _ => return Err(corrupt(path, "the `dead` field is not 0 or 1".into())),
    };
    let birth = read_field(&mut rest, "payload", path, MAX_PAYLOAD_LEN)?;
    // The digest covers everything above this point, magic line included.
    let covered = &body[..body.len() - rest.len()];
    let digest = read_field(&mut rest, "sha256", path, 32)?;
    if !rest.is_empty() {
        return Err(corrupt(path, "trailing bytes after the digest".into()));
    }
    if Sha256::digest(covered)[..] != *digest {
        return Err(corrupt(
            path,
            "the content digest does not match the record".into(),
        ));
    }
    let key = BirthKey {
        group: utf8(group, "group", path)?.into(),
        node: utf8(node, "node", path)?.into(),
        device: match utf8(device, "device", path)? {
            "" => None,
            d => Some(d.into()),
        },
    };
    if key.group.is_empty() || key.node.is_empty() {
        return Err(corrupt(path, "the group or node field is empty".into()));
    }
    Ok((
        key,
        BirthRecord {
            birth: birth.to_vec(),
            bd_seq,
            seq,
            dead,
        },
    ))
}

fn utf8<'a>(bytes: &'a [u8], field: &str, path: &Path) -> Result<&'a str, ThadamError> {
    std::str::from_utf8(bytes).map_err(|_| corrupt(path, format!("the {field} is not UTF-8")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> BirthKey {
        BirthKey::new("plantA", "edge1", None)
    }

    fn device_key() -> BirthKey {
        BirthKey::new("plantA", "edge1", Some("dev1"))
    }

    fn record(seq: Option<u64>, dead: bool) -> BirthRecord {
        BirthRecord {
            birth: b"\x08\x96\x01\x12\x00".to_vec(),
            bd_seq: Some(7),
            seq,
            dead,
        }
    }

    fn store(dir: &tempfile::TempDir) -> BirthStore {
        BirthStore::open(dir.path().join("births"))
    }

    #[test]
    fn put_and_load_all_roundtrip_and_latest_wins() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        // An unread directory reads as empty.
        assert_eq!(store.load_all().unwrap(), Vec::new());

        store.put(&key(), &record(Some(0), false)).unwrap();
        store.put(&device_key(), &record(Some(3), false)).unwrap();
        let mut loaded = store.load_all().unwrap();
        loaded.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].0, key());
        assert_eq!(loaded[0].1, record(Some(0), false));
        assert_eq!(loaded[1].0, device_key());
        assert_eq!(loaded[1].1, record(Some(3), false));

        // A later BIRTH for the same key replaces the record wholesale.
        store.put(&key(), &record(Some(0), true)).unwrap();
        let loaded = store.load_all().unwrap();
        let ours = loaded.iter().find(|(k, _)| *k == key()).unwrap();
        assert_eq!(ours.1, record(Some(0), true));
        let entries: Vec<String> = fs::read_dir(store.dir())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries.len(), 2, "one file per key, tmp files renamed");
    }

    #[test]
    fn mark_dead_flips_persists_and_absent_key_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        // No record: nothing to resurrect, and no file is created.
        store.mark_dead(&key()).unwrap();
        assert!(!store.dir().exists());

        store.put(&key(), &record(Some(0), false)).unwrap();
        store.mark_dead(&key()).unwrap();
        let loaded = store.load_all().unwrap();
        assert!(
            loaded
                .iter()
                .find(|(k, _)| k.device.is_none())
                .unwrap()
                .1
                .dead,
            "dead flag persisted"
        );
        // Already dead: no rewrite needed, still dead.
        store.mark_dead(&key()).unwrap();
        assert!(store.load_all().unwrap()[0].1.dead);
    }

    #[test]
    fn torn_tmp_files_and_foreign_names_are_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store.put(&key(), &record(Some(0), false)).unwrap();
        let hash = key().hash();
        fs::write(store.dir().join(format!(".{hash}.tmp-1-1")), b"torn").unwrap();
        fs::write(store.dir().join(".036906d2.tmp-9-9"), b"thadam/sparkpl").unwrap();
        fs::write(store.dir().join("not-a-record.birth"), b"junk").unwrap();
        fs::write(store.dir().join(format!("{hash}.schema")), b"other store").unwrap();

        let loaded = store.load_all().unwrap();
        assert_eq!(loaded, vec![(key(), record(Some(0), false))]);
    }

    #[test]
    fn corrupt_records_fail_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store.put(&key(), &record(Some(0), false)).unwrap();
        let path = store.dir().join(format!("{}.birth", key().hash()));
        let good = fs::read(&path).unwrap();

        // Bit rot in the payload: the content digest catches it.
        let mut flipped = good.clone();
        let mid = flipped.len() - 40;
        flipped[mid] ^= 0x01;
        fs::write(&path, &flipped).unwrap();
        assert_eq!(store.load_all().unwrap_err().code, codes::BUFFER_CORRUPT);

        // Truncation: the framing or the digest notices.
        fs::write(&path, &good[..good.len() - 3]).unwrap();
        assert_eq!(store.load_all().unwrap_err().code, codes::BUFFER_CORRUPT);

        // mark_dead goes through the same verified read.
        assert_eq!(
            store.mark_dead(&key()).unwrap_err().code,
            codes::BUFFER_CORRUPT
        );
    }

    #[test]
    fn oversize_records_are_refused_not_stored() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let mut huge = record(Some(0), false);
        huge.birth = vec![0; MAX_PAYLOAD_LEN as usize + 1];
        assert_eq!(
            store.put(&key(), &huge).unwrap_err().code,
            codes::DECODE_TOO_LARGE
        );
        let long = "x".repeat(MAX_LABEL_LEN as usize + 1);
        assert_eq!(
            store
                .put(&BirthKey::new(&long, "n", None), &record(None, false))
                .unwrap_err()
                .code,
            codes::DECODE_TOO_LARGE
        );
        assert!(!store.dir().exists(), "nothing was written");
    }

    #[test]
    fn foreign_file_is_incompatible() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        fs::create_dir_all(store.dir()).unwrap();
        fs::write(
            store.dir().join(format!("{}.birth", key().hash())),
            b"nope\n",
        )
        .unwrap();
        assert_eq!(
            store.load_all().unwrap_err().code,
            codes::BUFFER_INCOMPATIBLE
        );
    }

    #[test]
    fn a_record_under_another_keys_name_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        store.put(&key(), &record(Some(0), false)).unwrap();
        store.put(&device_key(), &record(Some(1), false)).unwrap();
        // fs-level rename (restore, raid repair, human error): the embedded
        // key no longer hashes to the file name.
        let node_path = store.dir().join(format!("{}.birth", key().hash()));
        let dev_path = store.dir().join(format!("{}.birth", device_key().hash()));
        fs::copy(&node_path, store.dir().join("backup.birth")).unwrap();
        fs::rename(&dev_path, &node_path).unwrap();
        fs::remove_file(store.dir().join("backup.birth")).unwrap();
        assert_eq!(store.load_all().unwrap_err().code, codes::BUFFER_CORRUPT);
    }
}
