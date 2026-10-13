//! The schema store: a durable, content-addressed store of message schemas
//! under the data directory.
//!
//! Sources that capture schema-carrying messages (today: the MCAP source
//! and its `ros2msg` channels) write each schema here **before** the first
//! message referencing it can be committed to the WAL, and stamp the
//! message with the store hash (for MCAP, the `mcap.schema_hash`
//! attribute). Decoders downstream — the transform chain, and later the
//! export CLI — resolve the schema text by hash, so the WAL never has to
//! carry schema text on every message.
//!
//! # On-disk layout
//!
//! One directory (the runtime places it at `<data_dir>/schemas/`, but this
//! type takes whatever directory it is given):
//!
//! ```text
//! <dir>/
//!   <sha256-hex>.schema     one file per stored schema
//!   .<sha256-hex>.tmp-*     in-flight writes (ignored by reads and `list`)
//! ```
//!
//! Each `.schema` file is a small self-describing record:
//!
//! ```text
//! thadam/schema-store 1\n
//! name <len>\n<name bytes>
//! encoding <len>\n<encoding bytes>
//! text <len>\n<text bytes>
//! ```
//!
//! `<len>` fields are decimal byte counts, so the schema text may contain
//! any bytes a valid UTF-8 string holds. A file whose first line is not the
//! magic above, or whose body does not match the lengths, is `corrupt` (or
//! `incompatible` when the magic is absent).
//!
//! # Hashing
//!
//! The hash is SHA-256, hex-encoded, computed over the framing
//! `"thadam/schema-store 1\0" | name | "\0" | encoding | "\0" | text`.
//! The name is part of the hash because a `ros2msg` document does not name
//! its main type — the same text under two package names compiles
//! differently, so they are two entries.
//!
//! # Writes
//!
//! [`SchemaStore::put`] writes the record to a temporary file in the same
//! directory, fsyncs it, renames it over `<hash>.schema`, and fsyncs the
//! directory — the same publish discipline as `thadam.json`. A crash can
//! only ever leave a torn `.tmp-*` file, which reads and `list` ignore.
//! Rewriting identical content is a no-op result-wise: same hash, same
//! bytes, one file.
//!
//! Reads verify: [`SchemaStore::get`] recomputes the hash of the stored
//! content and refuses a file that does not match the requested hash
//! (`buffer.corrupt`), so a damaged store fails loudly instead of decoding
//! against the wrong schema.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sha2::{Digest, Sha256};

use crate::codes;
use crate::error::{ErrorDomain, ThadamError};

/// The attribute a message carries to reference its schema in the store.
///
/// Defined here — not in a connector crate — because it is a cross-crate
/// contract: the MCAP source writes it, the transform chain reads it, and
/// connector crates and `thadam-transform` may not depend on each other.
pub const SCHEMA_HASH_ATTR: &str = "mcap.schema_hash";

/// The first line of every `.schema` file.
const MAGIC: &str = "thadam/schema-store 1";
/// Prepended to the hashed content so a schema-store hash cannot collide
/// with a hash of the same bytes computed for another purpose.
const HASH_DOMAIN: &[u8] = b"thadam/schema-store 1\0";
/// Upper bound for the name and encoding fields (schema text is bounded by
/// [`MAX_TEXT_LEN`]).
const MAX_LABEL_LEN: usize = 4096;
/// Upper bound for one stored schema text: far beyond any real `ros2msg`
/// document; keeps a corrupt length field from becoming an allocation.
const MAX_TEXT_LEN: u64 = 16 * 1024 * 1024;

/// Counter making temporary file names unique between writers in one
/// process (the data-directory lock excludes other processes).
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A schema read back from the store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredSchema {
    /// The schema name, for example `sensor_msgs/msg/JointState`.
    pub name: Box<str>,
    /// The schema encoding, for example `ros2msg`.
    pub encoding: Box<str>,
    /// The schema text.
    pub text: String,
}

impl StoredSchema {
    /// The hash this content is stored and looked up under.
    pub fn hash(&self) -> String {
        SchemaStore::hash(&self.name, &self.encoding, &self.text)
    }
}

/// A content-addressed store of message schemas in one directory.
///
/// Cheap to construct: [`SchemaStore::open`] touches nothing on disk. The
/// directory is created on the first write; a store whose directory does
/// not exist reads as empty.
#[derive(Clone, Debug)]
pub struct SchemaStore {
    dir: PathBuf,
}

impl SchemaStore {
    /// A store rooted at `dir`. Takes the store directory itself, not the
    /// data directory: the caller owns the layout (the runtime uses
    /// `<data_dir>/schemas/`).
    pub fn open(dir: impl Into<PathBuf>) -> Self {
        SchemaStore { dir: dir.into() }
    }

    /// The directory the store was opened on.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The SHA-256 hash (lowercase hex) `name`, `encoding`, and `text` are
    /// stored under.
    pub fn hash(name: &str, encoding: &str, text: &str) -> String {
        let mut digest = Sha256::new();
        digest.update(HASH_DOMAIN);
        for part in [name, encoding, text] {
            digest.update(part.as_bytes());
            digest.update([0]);
        }
        hex_of(&digest.finalize())
    }

    /// Stores the schema, returning its hash.
    ///
    /// Idempotent: storing the same content again rewrites the same bytes
    /// to the same file (atomically) and returns the same hash. A store
    /// write failure is a classified `buffer.io` error; it is never
    /// swallowed.
    pub fn put(&self, name: &str, encoding: &str, text: &str) -> Result<String, ThadamError> {
        let hash = Self::hash(name, encoding, text);
        let body = encode_record(name, encoding, text);
        fs::create_dir_all(&self.dir)
            .map_err(|e| io_error("create schema store directory", &self.dir, &e))?;
        let unique = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = self
            .dir
            .join(format!(".{hash}.tmp-{}-{unique}", std::process::id()));
        let path = self.dir.join(format!("{hash}.schema"));
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
        write().map_err(|e| io_error("write schema", &path, &e))?;
        Ok(hash)
    }

    /// The schema stored under `hash`, or `None` when the store has no such
    /// entry (including a malformed hash — only 64 lowercase hex digits can
    /// name a file this store wrote, which also keeps lookups inside the
    /// store directory).
    ///
    /// The stored content is hashed again and compared: a mismatch, a
    /// malformed record, or a bad magic/version is a classified error, not
    /// `None`.
    pub fn get(&self, hash: &str) -> Result<Option<StoredSchema>, ThadamError> {
        if !is_hash(hash) {
            return Ok(None);
        }
        let path = self.dir.join(format!("{hash}.schema"));
        let mut body = Vec::new();
        match File::open(&path).and_then(|mut f| f.read_to_end(&mut body)) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_error("read schema", &path, &e)),
        }
        let schema = decode_record(&body, &path)?;
        if schema.hash() != hash {
            return Err(ThadamError::fatal(
                ErrorDomain::Buffer,
                codes::BUFFER_CORRUPT,
                format!(
                    "{} stores content that hashes differently; refusing to use it",
                    path.display()
                ),
            ));
        }
        Ok(Some(schema))
    }

    /// Every stored schema, `(hash, schema)` pairs ordered by hash.
    ///
    /// Temporary files and any other non-entry are skipped; a malformed
    /// entry is an error, so `list` can be trusted for audits and exports.
    pub fn list(&self) -> Result<Vec<(String, StoredSchema)>, ThadamError> {
        let read = match fs::read_dir(&self.dir) {
            Ok(r) => r,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(io_error("read schema store directory", &self.dir, &e)),
        };
        let mut all: BTreeMap<String, StoredSchema> = BTreeMap::new();
        for entry in read {
            let entry =
                entry.map_err(|e| io_error("read schema store directory", &self.dir, &e))?;
            let name = entry.file_name();
            let Some(hash) = name.to_str().and_then(|n| n.strip_suffix(".schema")) else {
                continue;
            };
            // Content verification re-hashes; skip names that cannot be
            // right so the tmp files of an interrupted write never surface
            // as errors here.
            if !is_hash(hash) {
                continue;
            }
            let Some(schema) = self.get(hash)? else {
                continue;
            };
            all.insert(hash.to_owned(), schema);
        }
        Ok(all.into_iter().collect())
    }
}

/// Whether `s` can name a content-addressed file the store wrote.
pub(crate) fn is_hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

pub(crate) fn hex_of(digest: &[u8]) -> String {
    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        out.push(char::from_digit(u32::from(b >> 4), 16).unwrap_or('0'));
        out.push(char::from_digit(u32::from(b & 0xF), 16).unwrap_or('0'));
    }
    out
}

fn encode_record(name: &str, encoding: &str, text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(name.len() + encoding.len() + text.len() + 64);
    out.extend_from_slice(MAGIC.as_bytes());
    out.push(b'\n');
    for (key, value) in [("name", name), ("encoding", encoding), ("text", text)] {
        out.extend_from_slice(key.as_bytes());
        out.push(b' ');
        out.extend_from_slice(value.len().to_string().as_bytes());
        out.push(b'\n');
        out.extend_from_slice(value.as_bytes());
    }
    out
}

/// Parses one `<len>`-prefixed field: a `key len\n` line then `len` bytes.
pub(crate) fn read_field<'a>(
    body: &mut &'a [u8],
    key: &str,
    path: &Path,
    max: u64,
) -> Result<&'a [u8], ThadamError> {
    let newline = body
        .iter()
        .position(|&b| b == b'\n')
        .ok_or_else(|| corrupt(path, format!("the `{key}` line is missing")))?;
    let line = std::str::from_utf8(&body[..newline])
        .map_err(|_| corrupt(path, format!("the `{key}` line is not text")))?;
    let len = line
        .strip_prefix(key)
        .and_then(|rest| rest.strip_prefix(' '))
        .and_then(|n| n.parse::<u64>().ok())
        .ok_or_else(|| corrupt(path, format!("the `{key}` line is malformed")))?;
    if len > max {
        return Err(corrupt(
            path,
            format!("the `{key}` field claims {len} bytes"),
        ));
    }
    let len = usize::try_from(len).map_err(|_| corrupt(path, "a length field overflows".into()))?;
    *body = &body[newline + 1..];
    if body.len() < len {
        return Err(corrupt(path, format!("the `{key}` field is truncated")));
    }
    let (field, rest) = body.split_at(len);
    *body = rest;
    Ok(field)
}

fn decode_record(body: &[u8], path: &Path) -> Result<StoredSchema, ThadamError> {
    let mut rest = body;
    let magic = format!("{MAGIC}\n");
    if !rest.starts_with(magic.as_bytes()) {
        return Err(ThadamError::fatal(
            ErrorDomain::Buffer,
            codes::BUFFER_INCOMPATIBLE,
            format!("{} is not a thadam schema-store file", path.display()),
        ));
    }
    rest = &rest[magic.len()..];
    let name = read_field(&mut rest, "name", path, MAX_LABEL_LEN as u64)?;
    let encoding = read_field(&mut rest, "encoding", path, MAX_LABEL_LEN as u64)?;
    let text = read_field(&mut rest, "text", path, MAX_TEXT_LEN)?;
    Ok(StoredSchema {
        name: utf8_field(name, "name", path)?.into(),
        encoding: utf8_field(encoding, "encoding", path)?.into(),
        text: utf8_field(text, "schema text", path)?.to_owned(),
    })
}

fn utf8_field<'a>(bytes: &'a [u8], what: &str, path: &Path) -> Result<&'a str, ThadamError> {
    std::str::from_utf8(bytes).map_err(|_| corrupt(path, format!("the {what} is not UTF-8")))
}

pub(crate) fn corrupt(path: &Path, message: String) -> ThadamError {
    ThadamError::fatal(
        ErrorDomain::Buffer,
        codes::BUFFER_CORRUPT,
        format!("{} is malformed: {message}", path.display()),
    )
}

pub(crate) fn io_error(op: &str, path: &Path, e: &io::Error) -> ThadamError {
    ThadamError::fatal(
        ErrorDomain::Buffer,
        codes::BUFFER_IO,
        format!("{op} {}: {e}", path.display()),
    )
}

/// Replaces `path` with `body` atomically and durably: write a sibling
/// temporary file, fsync it, rename it over `path`, fsync the directory
/// (the discipline [`SchemaStore::put`] uses). A crash leaves either the old
/// content or the new, plus at worst a torn `.<name>.tmp-*` that readers
/// must ignore. Creates the parent directory. A failure is a classified
/// `buffer.io` error and removes the temporary file.
pub fn write_atomic(path: &Path, body: &[u8]) -> Result<(), ThadamError> {
    let dir = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    fs::create_dir_all(dir).map_err(|e| io_error("create directory", dir, &e))?;
    let unique = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = dir.join(format!(".{name}.tmp-{}-{unique}", std::process::id()));
    let write = || -> io::Result<()> {
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all(body)?;
            sync_file(&f)?;
        }
        fs::rename(&tmp, path)?;
        sync_dir(dir)
    };
    write().map_err(|e| {
        let _ = fs::remove_file(&tmp);
        io_error("write", path, &e)
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    const NAME: &str = "sensor_msgs/msg/JointState";
    const ENCODING: &str = "ros2msg";
    const TEXT: &str = "std_msgs/Header header\nstring[] name\nfloat64[] position\n";

    fn store(dir: &tempfile::TempDir) -> SchemaStore {
        SchemaStore::open(dir.path().join("schemas"))
    }

    #[test]
    fn hash_is_stable_and_content_addressed() {
        let hash = SchemaStore::hash(NAME, ENCODING, TEXT);
        assert_eq!(hash.len(), 64);
        assert_eq!(hash, SchemaStore::hash(NAME, ENCODING, TEXT));
        assert_ne!(hash, SchemaStore::hash("other/Type", ENCODING, TEXT));
        assert_ne!(hash, SchemaStore::hash(NAME, "jsonschema", TEXT));
        assert_ne!(hash, SchemaStore::hash(NAME, ENCODING, "int32 x\n"));
    }

    #[test]
    fn put_get_roundtrip_and_put_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let hash = store.put(NAME, ENCODING, TEXT).unwrap();
        assert_eq!(hash, SchemaStore::hash(NAME, ENCODING, TEXT));

        let got = store.get(&hash).unwrap().expect("stored");
        assert_eq!(&*got.name, NAME);
        assert_eq!(&*got.encoding, ENCODING);
        assert_eq!(got.text, TEXT);
        assert_eq!(got.hash(), hash);

        // Same content again: same hash, still exactly one entry file.
        assert_eq!(store.put(NAME, ENCODING, TEXT).unwrap(), hash);
        let entries: Vec<String> = fs::read_dir(store.dir())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, vec![format!("{hash}.schema")]);
        assert_eq!(store.list().unwrap(), vec![(hash, got)]);
    }

    #[test]
    fn torn_tmp_files_are_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let hash = store.put(NAME, ENCODING, TEXT).unwrap();
        // Interrupted writes: a partial tmp file and a stale tmp suffix.
        fs::write(store.dir().join(format!(".{hash}.tmp-1-1")), b"torn").unwrap();
        fs::write(store.dir().join(".036906d2.tmp-9-9"), b"thadam/schema").unwrap();
        fs::write(store.dir().join("not-a-schema.schema"), b"junk").unwrap();

        assert_eq!(store.list().unwrap().len(), 1);
        let got = store.get(&hash).unwrap();
        assert_eq!(got.map(|s| s.text), Some(TEXT.to_owned()));
    }

    #[test]
    fn unknown_and_malformed_hashes_read_as_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        // No directory yet at all: reads as empty.
        assert_eq!(store.list().unwrap(), Vec::new());
        assert!(
            store
                .get(&SchemaStore::hash(NAME, ENCODING, TEXT))
                .unwrap()
                .is_none()
        );

        store.put(NAME, ENCODING, TEXT).unwrap();
        // Valid but never stored.
        assert!(
            store
                .get(&SchemaStore::hash("a", "b", "c"))
                .unwrap()
                .is_none()
        );
        // Never filenames the store wrote (also path-traversal attempts).
        for bad in [
            String::new(),
            "xyz".to_owned(),
            "../x".to_owned(),
            "a".repeat(63),
            "A".repeat(64),
        ] {
            assert!(store.get(&bad).unwrap().is_none(), "{bad}");
        }
    }

    #[test]
    fn content_that_hashes_differently_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let hash = store.put(NAME, ENCODING, TEXT).unwrap();
        // Tamper: copy another schema's file over this one's name.
        let other = store.put("other/Type", ENCODING, "int32 x\n").unwrap();
        let from = store.dir().join(format!("{other}.schema"));
        let to = store.dir().join(format!("{hash}.schema"));
        fs::copy(from, to).unwrap();
        let err = store.get(&hash).unwrap_err();
        assert_eq!(err.code, codes::BUFFER_CORRUPT, "{err}");
        // A truncated record is malformed, not silently decoded.
        let path = store.dir().join(format!("{other}.schema"));
        let bytes = fs::read(&path).unwrap();
        fs::write(&path, &bytes[..bytes.len() - 3]).unwrap();
        assert_eq!(store.get(&other).unwrap_err().code, codes::BUFFER_CORRUPT);
    }

    #[test]
    fn write_atomic_replaces_content_and_leaves_no_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("state.json");
        write_atomic(&path, b"one").unwrap();
        write_atomic(&path, b"two").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"two");
        let entries: Vec<_> = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![std::ffi::OsString::from("state.json")]);
    }

    #[test]
    fn a_foreign_file_is_incompatible() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        fs::create_dir_all(store.dir()).unwrap();
        let hash = SchemaStore::hash(NAME, ENCODING, TEXT);
        fs::write(store.dir().join(format!("{hash}.schema")), b"nope\n").unwrap();
        assert_eq!(
            store.get(&hash).unwrap_err().code,
            codes::BUFFER_INCOMPATIBLE
        );
    }
}
