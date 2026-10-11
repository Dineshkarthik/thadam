//! Durable staging for quarantine rejects.
//!
//! A reject must not hold its owning sink's WAL cursor until the quarantine
//! Parquet segment rotates. Instead, the reject is appended to a small
//! per-quarantine-instance staging file and fsynced; only then may the owner's
//! cursor pass it. The quarantine segment is built from the same rows on its
//! own cadence, and the staging file shrinks once that segment is durable.
//!
//! # File format
//!
//! `rejects.staging` in the quarantine instance's data directory:
//!
//! ```text
//! header   8 bytes   b"RVQSTG01" (magic + format version)
//! frame*             len u32 LE | crc32c u32 LE | payload (len bytes)
//! ```
//!
//! The CRC32C covers `len` (4 bytes, little endian) followed by the payload.
//! One frame is one `push` of rejects: the WAL range it covers (`epoch`,
//! `first`, `last`), the clock-sync flag, and the encoded quarantine rows
//! (`StagedRow`). A frame is the unit of atomicity: it is written with one
//! `write_all` followed by one fsync, and the owner's cursor only moves after
//! that fsync returns.
//!
//! # Crash-recovery argument
//!
//! - **Torn tail.** A crash during an append leaves a partial or corrupt last
//!   frame. That push never returned, so the cursor did not move past it and
//!   its rejects regenerate from the WAL. [`Staging::open`] scans frames from
//!   the start, stops at the first incomplete or CRC-failing frame, truncates
//!   the file there, and fsyncs. Like the WAL, it never resynchronizes past a
//!   bad frame. A failed append rolls the file back to the last good length
//!   before the next append, so a good frame never follows a torn one.
//! - **Exactly once.** Every frame is a contiguous slice of WAL positions
//!   (ranges chain: each starts right after the previous `last`). The
//!   quarantine segment sink's durable watermark `q` is the highest position
//!   its finalized segments cover. On recovery, a staged row is replayed into
//!   the next quarantine segment only if its position is above `q`, and later
//!   pushes drop rejects at or below the highest staged position. So a reject
//!   is in exactly one place: a durable segment (`pos <= q`) or staging
//!   (`pos > q`). A crash between a segment becoming durable and the staging
//!   file shrinking leaves harmless dead frames: they are filtered by `q`.
//! - **Shrinking.** After the segment covering a prefix of frames is durable,
//!   the file is truncated (nothing live) or rewritten through a temporary
//!   file and an atomic rename (a live suffix remains). Each step is
//!   crash-safe on its own: before the rename the old file is intact, and
//!   after it the new file is complete.
//! - **Bounded size.** The owner closes the open quarantine segment early
//!   once staged bytes pass a soft limit or the quarantine disk budget has no
//!   room, so the live part stays small. Dead bytes are rewritten away once
//!   they outweigh the live bytes.
//!
//! Error messages here never include row content: only constant text, file
//! paths, and OS error text.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use thadam_core::{WalEpoch, WalPos, WalRange};

use crate::datadir::{sync_dir, sync_file};

const MAGIC: [u8; 8] = *b"RVQSTG01";
const HEADER_LEN: u64 = MAGIC.len() as u64;
const FRAME_HEAD: usize = 8;
/// Dead bytes below this are never worth a rewrite.
const COMPACT_MIN_DEAD: u64 = 64 << 10;

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// One quarantine row, in the fixed quarantine schema's terms.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StagedRow {
    pub event_id: u128,
    pub epoch: u64,
    pub pos: u64,
    pub record_index: Option<u32>,
    pub ingest_nanos: i64,
    pub source: Option<String>,
    pub topic: Option<String>,
    pub sink_id: String,
    pub dataset: Option<String>,
    pub error_domain: String,
    pub error_code: String,
    pub reason: String,
    pub raw: Option<Bytes>,
    pub raw_truncated: bool,
}

/// The rows of one push, over the WAL range they close.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StagedBatch {
    pub range: WalRange,
    pub clock_synced: bool,
    pub rows: Vec<StagedRow>,
}

fn put_len(out: &mut Vec<u8>, len: usize) -> io::Result<()> {
    let len = u32::try_from(len).map_err(|_| invalid("staged field is too large"))?;
    out.extend_from_slice(&len.to_le_bytes());
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) -> io::Result<()> {
    put_len(out, b.len())?;
    out.extend_from_slice(b);
    Ok(())
}

fn put_opt<T>(
    out: &mut Vec<u8>,
    v: Option<T>,
    put: impl FnOnce(&mut Vec<u8>, T) -> io::Result<()>,
) -> io::Result<()> {
    match v {
        Some(v) => {
            out.push(1);
            put(out, v)
        }
        None => {
            out.push(0);
            Ok(())
        }
    }
}

impl StagedRow {
    fn encode(&self, out: &mut Vec<u8>) -> io::Result<()> {
        out.extend_from_slice(&self.event_id.to_le_bytes());
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.pos.to_le_bytes());
        put_opt(out, self.record_index, |o, v| {
            o.extend_from_slice(&v.to_le_bytes());
            Ok(())
        })?;
        out.extend_from_slice(&self.ingest_nanos.to_le_bytes());
        put_opt(out, self.source.as_deref(), |o, v| {
            put_bytes(o, v.as_bytes())
        })?;
        put_opt(out, self.topic.as_deref(), |o, v| {
            put_bytes(o, v.as_bytes())
        })?;
        put_bytes(out, self.sink_id.as_bytes())?;
        put_opt(out, self.dataset.as_deref(), |o, v| {
            put_bytes(o, v.as_bytes())
        })?;
        put_bytes(out, self.error_domain.as_bytes())?;
        put_bytes(out, self.error_code.as_bytes())?;
        put_bytes(out, self.reason.as_bytes())?;
        put_opt(out, self.raw.as_deref(), put_bytes)?;
        out.push(u8::from(self.raw_truncated));
        Ok(())
    }

    fn decode(r: &mut Reader<'_>) -> io::Result<StagedRow> {
        Ok(StagedRow {
            event_id: r.u128()?,
            epoch: r.u64()?,
            pos: r.u64()?,
            record_index: r.opt(Reader::u32)?,
            ingest_nanos: r.i64()?,
            source: r.opt(Reader::string)?,
            topic: r.opt(Reader::string)?,
            sink_id: r.string()?,
            dataset: r.opt(Reader::string)?,
            error_domain: r.string()?,
            error_code: r.string()?,
            reason: r.string()?,
            raw: r.opt(|r| r.slice().map(Bytes::copy_from_slice))?,
            raw_truncated: r.flag()?,
        })
    }
}

struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.buf.len() < n {
            return Err(invalid("staged frame is truncated"));
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let mut a = [0u8; N];
        a.copy_from_slice(self.take(N)?);
        Ok(a)
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn i64(&mut self) -> io::Result<i64> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    fn u128(&mut self) -> io::Result<u128> {
        Ok(u128::from_le_bytes(self.array()?))
    }

    fn flag(&mut self) -> io::Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(invalid("staged frame has an invalid flag")),
        }
    }

    fn slice(&mut self) -> io::Result<&'a [u8]> {
        let n = usize::try_from(self.u32()?).map_err(|_| invalid("staged field is too large"))?;
        self.take(n)
    }

    fn string(&mut self) -> io::Result<String> {
        std::str::from_utf8(self.slice()?)
            .map(str::to_owned)
            .map_err(|_| invalid("staged text is not UTF-8"))
    }

    fn opt<T>(&mut self, read: impl FnOnce(&mut Self) -> io::Result<T>) -> io::Result<Option<T>> {
        if self.flag()? {
            read(self).map(Some)
        } else {
            Ok(None)
        }
    }
}

impl StagedBatch {
    fn encode_payload(&self) -> io::Result<Vec<u8>> {
        let mut out = Vec::with_capacity(64 + self.rows.len() * 160);
        out.extend_from_slice(&self.range.epoch().get().to_le_bytes());
        out.extend_from_slice(&self.range.first().get().to_le_bytes());
        out.extend_from_slice(&self.range.last().get().to_le_bytes());
        out.push(u8::from(self.clock_synced));
        put_len(&mut out, self.rows.len())?;
        for row in &self.rows {
            row.encode(&mut out)?;
        }
        Ok(out)
    }

    fn decode_payload(payload: &[u8]) -> io::Result<StagedBatch> {
        let mut r = Reader { buf: payload };
        let epoch = WalEpoch::new(r.u64()?);
        let first = WalPos::new(r.u64()?);
        let last = WalPos::new(r.u64()?);
        let range = WalRange::new(epoch, first, last)
            .map_err(|_| invalid("staged frame has an inverted range"))?;
        let clock_synced = r.flag()?;
        let n = r.u32()?;
        let mut rows = Vec::new();
        for _ in 0..n {
            rows.push(StagedRow::decode(&mut r)?);
        }
        if !r.buf.is_empty() {
            return Err(invalid("staged frame has trailing bytes"));
        }
        Ok(StagedBatch {
            range,
            clock_synced,
            rows,
        })
    }

    /// The frame as written to the file: length, CRC32C, payload.
    pub fn encode_frame(&self) -> io::Result<Vec<u8>> {
        let payload = self.encode_payload()?;
        let len = u32::try_from(payload.len()).map_err(|_| invalid("staged frame is too large"))?;
        let len_le = len.to_le_bytes();
        let crc = crc32c::crc32c_append(crc32c::crc32c(&len_le), &payload);
        let mut frame = Vec::with_capacity(FRAME_HEAD + payload.len());
        frame.extend_from_slice(&len_le);
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(&payload);
        Ok(frame)
    }
}

/// A frame read back by [`scan`].
struct Scanned {
    batch: StagedBatch,
    end: u64,
}

/// Parses `bytes` (the whole file) into its leading valid frames. Returns the
/// frames and the length of the valid prefix; everything after it is a torn
/// tail. A CRC-valid frame that does not decode is an error, not a torn tail:
/// it is a format bug, and dropping it silently would lose a staged reject.
fn scan(bytes: &[u8]) -> io::Result<(Vec<Scanned>, u64)> {
    if bytes.len() < MAGIC.len() || bytes[..MAGIC.len()] != MAGIC {
        return Err(invalid("not a quarantine staging file"));
    }
    let mut at = MAGIC.len();
    let mut out = Vec::new();
    while let Some(head) = bytes.get(at..at + FRAME_HEAD) {
        let mut len_le = [0u8; 4];
        len_le.copy_from_slice(&head[..4]);
        let mut crc_le = [0u8; 4];
        crc_le.copy_from_slice(&head[4..]);
        let len = u32::from_le_bytes(len_le) as usize;
        let Some(payload) = bytes.get(at + FRAME_HEAD..at + FRAME_HEAD + len) else {
            break;
        };
        let crc = crc32c::crc32c_append(crc32c::crc32c(&len_le), payload);
        if crc != u32::from_le_bytes(crc_le) {
            break;
        }
        let batch = StagedBatch::decode_payload(payload)?;
        at += FRAME_HEAD + len;
        out.push(Scanned {
            batch,
            end: at as u64,
        });
    }
    Ok((out, at as u64))
}

/// What [`Staging::open`] found.
pub(crate) struct Opened {
    pub staging: Staging,
    /// Every frame in the file, oldest first (including frames a durable
    /// quarantine segment already covers; the caller filters by position).
    pub batches: Vec<StagedBatch>,
    /// Bytes of torn tail that were cut off.
    pub torn_bytes: u64,
}

/// The staging file. Blocking I/O: callers run it on the blocking pool.
pub(crate) struct Staging {
    path: PathBuf,
    file: File,
    /// Length of the valid prefix: the header plus whole frames.
    len: u64,
    /// Where the first live frame starts (frames before it are dead).
    live_start: u64,
    /// `(end offset, range.last)` of each live frame, oldest first.
    live: VecDeque<(u64, u64)>,
    /// A failed append may have left bytes past `len`; cut them first.
    tail_dirty: bool,
}

fn open_append(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(path)
}

fn parent_of(path: &Path) -> io::Result<&Path> {
    path.parent()
        .ok_or_else(|| invalid("staging path has no parent"))
}

impl Staging {
    /// Opens (creating if needed) the staging file at `path`, cuts a torn
    /// tail, and returns every intact frame.
    pub fn open(path: &Path) -> io::Result<Opened> {
        let dir = parent_of(path)?;
        fs::create_dir_all(dir)?;
        match fs::remove_file(with_tmp(path)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let file = open_append(path)?;
        let mut bytes = Vec::new();
        (&file).read_to_end(&mut bytes)?;
        if bytes.len() < MAGIC.len() {
            // A crash while creating the file: nothing was ever staged.
            if !MAGIC.starts_with(&bytes) {
                return Err(invalid("not a quarantine staging file"));
            }
            file.set_len(0)?;
            (&file).write_all(&MAGIC)?;
            sync_file(&file)?;
            sync_dir(dir)?;
            return Ok(Opened {
                staging: Staging::fresh(path, file),
                batches: Vec::new(),
                torn_bytes: bytes.len() as u64,
            });
        }
        let (frames, valid) = scan(&bytes)?;
        let torn_bytes = bytes.len() as u64 - valid;
        if torn_bytes > 0 {
            file.set_len(valid)?;
            sync_file(&file)?;
        }
        let live = frames
            .iter()
            .map(|f| (f.end, f.batch.range.last().get()))
            .collect();
        Ok(Opened {
            staging: Staging {
                path: path.to_owned(),
                file,
                len: valid,
                live_start: HEADER_LEN,
                live,
                tail_dirty: false,
            },
            batches: frames.into_iter().map(|f| f.batch).collect(),
            torn_bytes,
        })
    }

    fn fresh(path: &Path, file: File) -> Staging {
        Staging {
            path: path.to_owned(),
            file,
            len: HEADER_LEN,
            live_start: HEADER_LEN,
            live: VecDeque::new(),
            tail_dirty: false,
        }
    }

    /// File size in bytes (the figure charged to the disk budget).
    pub fn bytes(&self) -> u64 {
        self.len
    }

    /// Bytes of frames still needed.
    pub fn live_bytes(&self) -> u64 {
        self.len - self.live_start
    }

    /// Whether any live frame ends at or below `durable`.
    pub fn releasable(&self, durable: u64) -> bool {
        self.live.front().is_some_and(|(_, last)| *last <= durable)
    }

    /// Appends one encoded frame (from [`StagedBatch::encode_frame`]) and
    /// fsyncs. On `Ok` the frame survives a crash.
    pub fn append_frame(&mut self, frame: &[u8], last: u64) -> io::Result<()> {
        if self.tail_dirty {
            self.file.set_len(self.len)?;
            self.tail_dirty = false;
        }
        let result = (&self.file)
            .write_all(frame)
            .and_then(|()| sync_file(&self.file));
        if let Err(e) = result {
            self.tail_dirty = true;
            return Err(e);
        }
        self.len += frame.len() as u64;
        self.live.push_back((self.len, last));
        Ok(())
    }

    /// Forgets the frames a durable quarantine segment covers (`last <=
    /// durable`) and gives their bytes back: truncating when nothing is live,
    /// rewriting when dead bytes outweigh live ones. Returns the bytes freed.
    pub fn release_through(&mut self, durable: u64) -> io::Result<u64> {
        let mut covered = 0;
        let mut new_start = self.live_start;
        for (end, last) in &self.live {
            if *last > durable {
                break;
            }
            covered += 1;
            new_start = *end;
        }
        if covered == 0 {
            return Ok(0);
        }
        let before = self.len;
        if covered == self.live.len() {
            self.file.set_len(HEADER_LEN)?;
            self.len = HEADER_LEN;
            self.live_start = HEADER_LEN;
            self.live.clear();
            self.tail_dirty = false;
            sync_file(&self.file)?;
            return Ok(before - self.len);
        }
        let dead = new_start - HEADER_LEN;
        if dead < COMPACT_MIN_DEAD.max(self.len - new_start) {
            self.live.drain(..covered);
            self.live_start = new_start;
            return Ok(0);
        }
        self.rewrite_from(new_start, covered)?;
        Ok(before - self.len)
    }

    /// Replaces the file with the header plus the bytes from `start` on and
    /// forgets the first `covered` live frames. The new file is written and
    /// fsynced under a temporary name; the in-memory state switches over as
    /// soon as the rename has happened, so a failing directory fsync (the
    /// last step) cannot leave it pointing at the replaced file.
    fn rewrite_from(&mut self, start: u64, covered: usize) -> io::Result<()> {
        let live_len =
            usize::try_from(self.len - start).map_err(|_| invalid("staging file is too large"))?;
        let mut keep = vec![0u8; live_len];
        self.file.read_exact_at(&mut keep, start)?;
        let tmp = with_tmp(&self.path);
        let write = || -> io::Result<File> {
            match fs::remove_file(&tmp) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            let mut f = OpenOptions::new()
                .read(true)
                .append(true)
                .create_new(true)
                .open(&tmp)?;
            f.write_all(&MAGIC)?;
            f.write_all(&keep)?;
            sync_file(&f)?;
            fs::rename(&tmp, &self.path)?;
            Ok(f)
        };
        match write() {
            Ok(f) => self.file = f,
            Err(e) => {
                let _ = fs::remove_file(&tmp);
                return Err(e);
            }
        }
        let shift = start - HEADER_LEN;
        self.live.drain(..covered);
        for (end, _) in &mut self.live {
            *end -= shift;
        }
        self.len = HEADER_LEN + keep.len() as u64;
        self.live_start = HEADER_LEN;
        self.tail_dirty = false;
        sync_dir(parent_of(&self.path)?)
    }
}

fn with_tmp(path: &Path) -> PathBuf {
    let mut name = path.file_name().map(ToOwned::to_owned).unwrap_or_default();
    name.push(".tmp");
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(first: u64, last: u64) -> WalRange {
        WalRange::new(WalEpoch::new(1), WalPos::new(first), WalPos::new(last)).unwrap()
    }

    fn row(pos: u64, raw: Option<&[u8]>) -> StagedRow {
        StagedRow {
            event_id: (u128::from(pos) << 64) | 7,
            epoch: 1,
            pos,
            record_index: raw.map(|_| 3),
            ingest_nanos: -42,
            source: raw.map(|_| "src".to_owned()),
            topic: None,
            sink_id: "pq".to_owned(),
            dataset: Some("d".to_owned()),
            error_domain: "decode".to_owned(),
            error_code: "decode.invalid_payload".to_owned(),
            reason: "bad".to_owned(),
            raw: raw.map(Bytes::copy_from_slice),
            raw_truncated: false,
        }
    }

    fn batch(first: u64, last: u64, pos: &[u64]) -> StagedBatch {
        StagedBatch {
            range: range(first, last),
            clock_synced: true,
            rows: pos
                .iter()
                .map(|p| row(*p, (p % 2 == 0).then_some(b"raw bytes".as_slice())))
                .collect(),
        }
    }

    fn append(s: &mut Staging, b: &StagedBatch) {
        let frame = b.encode_frame().unwrap();
        s.append_frame(&frame, b.range.last().get()).unwrap();
    }

    fn path(dir: &tempfile::TempDir) -> PathBuf {
        dir.path().join("q").join("rejects.staging")
    }

    #[test]
    fn append_is_visible_to_a_fresh_open_with_every_field() {
        let dir = tempfile::tempdir().unwrap();
        let p = path(&dir);
        let mut s = Staging::open(&p).unwrap().staging;
        let (a, b) = (batch(1, 4, &[2, 3]), batch(5, 9, &[9]));
        append(&mut s, &a);
        append(&mut s, &b);
        assert_eq!(s.bytes(), fs::metadata(&p).unwrap().len());
        drop(s);

        let opened = Staging::open(&p).unwrap();
        assert_eq!(opened.torn_bytes, 0);
        assert_eq!(opened.batches, vec![a, b]);
        assert_eq!(
            opened.staging.live_bytes(),
            opened.staging.bytes() - HEADER_LEN
        );
    }

    #[test]
    fn open_creates_the_directory_and_a_header_only_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = path(&dir);
        let opened = Staging::open(&p).unwrap();
        assert!(opened.batches.is_empty());
        assert_eq!(fs::read(&p).unwrap(), MAGIC);
    }

    #[test]
    fn a_torn_tail_is_cut_and_earlier_frames_survive() {
        let dir = tempfile::tempdir().unwrap();
        let p = path(&dir);
        let mut s = Staging::open(&p).unwrap().staging;
        let (a, b) = (batch(1, 4, &[2]), batch(5, 9, &[6, 7]));
        append(&mut s, &a);
        let good = s.bytes();
        append(&mut s, &b);
        let full = s.bytes();
        drop(s);

        // Every possible torn length of the second frame.
        let bytes = fs::read(&p).unwrap();
        for cut in good..full {
            fs::write(&p, &bytes[..usize::try_from(cut).unwrap()]).unwrap();
            let opened = Staging::open(&p).unwrap();
            assert_eq!(opened.batches, vec![a.clone()], "cut at {cut}");
            assert_eq!(opened.torn_bytes, cut - good, "cut at {cut}");
            assert_eq!(fs::metadata(&p).unwrap().len(), good, "cut at {cut}");
        }
    }

    #[test]
    fn a_corrupt_last_frame_is_cut_like_a_torn_one() {
        let dir = tempfile::tempdir().unwrap();
        let p = path(&dir);
        let mut s = Staging::open(&p).unwrap().staging;
        let (a, b) = (batch(1, 4, &[2]), batch(5, 9, &[6]));
        append(&mut s, &a);
        let good = s.bytes();
        append(&mut s, &b);
        drop(s);

        let mut bytes = fs::read(&p).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x55;
        fs::write(&p, &bytes).unwrap();
        let opened = Staging::open(&p).unwrap();
        assert_eq!(opened.batches, vec![a]);
        assert_eq!(opened.staging.bytes(), good);
    }

    #[test]
    fn appends_after_a_cut_are_recovered() {
        let dir = tempfile::tempdir().unwrap();
        let p = path(&dir);
        let mut s = Staging::open(&p).unwrap().staging;
        let a = batch(1, 4, &[2]);
        append(&mut s, &a);
        drop(s);
        let mut f = OpenOptions::new().append(true).open(&p).unwrap();
        f.write_all(&[9, 9, 9]).unwrap();
        drop(f);

        let mut s = Staging::open(&p).unwrap().staging;
        let b = batch(5, 6, &[6]);
        append(&mut s, &b);
        drop(s);
        assert_eq!(Staging::open(&p).unwrap().batches, vec![a, b]);
    }

    #[test]
    fn a_torn_header_is_a_fresh_file_but_a_foreign_file_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let p = path(&dir);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(&p, &MAGIC[..3]).unwrap();
        let opened = Staging::open(&p).unwrap();
        assert!(opened.batches.is_empty());
        assert_eq!(opened.torn_bytes, 3);
        assert_eq!(fs::read(&p).unwrap(), MAGIC);

        fs::write(&p, b"not a staging file at all").unwrap();
        let err = Staging::open(&p).err().unwrap();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        fs::write(&p, b"xyz").unwrap();
        assert!(Staging::open(&p).is_err());
    }

    #[test]
    fn a_crc_valid_frame_that_does_not_decode_is_an_error_not_a_cut() {
        let dir = tempfile::tempdir().unwrap();
        let p = path(&dir);
        let payload = vec![1u8, 2, 3];
        let len_le = (payload.len() as u32).to_le_bytes();
        let crc = crc32c::crc32c_append(crc32c::crc32c(&len_le), &payload);
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&len_le);
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes.extend_from_slice(&payload);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(&p, &bytes).unwrap();
        assert!(Staging::open(&p).is_err());
    }

    #[test]
    fn releasing_everything_truncates_to_the_header() {
        let dir = tempfile::tempdir().unwrap();
        let p = path(&dir);
        let mut s = Staging::open(&p).unwrap().staging;
        append(&mut s, &batch(1, 4, &[2]));
        append(&mut s, &batch(5, 9, &[6]));
        let before = s.bytes();
        assert!(!s.releasable(3));
        assert!(s.releasable(4));
        assert_eq!(s.release_through(3).unwrap(), 0);
        assert_eq!(s.release_through(9).unwrap(), before - HEADER_LEN);
        assert_eq!(s.bytes(), HEADER_LEN);
        assert_eq!(fs::read(&p).unwrap(), MAGIC);

        let c = batch(10, 12, &[11]);
        append(&mut s, &c);
        drop(s);
        assert_eq!(Staging::open(&p).unwrap().batches, vec![c]);
    }

    #[test]
    fn a_released_prefix_is_rewritten_once_dead_bytes_outweigh_live_ones() {
        let dir = tempfile::tempdir().unwrap();
        let p = path(&dir);
        let mut s = Staging::open(&p).unwrap().staging;
        let big = vec![7u8; 80 << 10];
        let mut first = batch(1, 10, &[2]);
        first.rows[0].raw = Some(Bytes::from(big));
        append(&mut s, &first);
        let live = batch(11, 20, &[12]);
        append(&mut s, &live);
        let before = s.bytes();

        let freed = s.release_through(10).unwrap();
        assert!(freed > 80 << 10, "freed {freed}");
        assert_eq!(s.bytes(), before - freed);
        assert_eq!(s.bytes(), fs::metadata(&p).unwrap().len());
        assert!(!with_tmp(&p).exists());

        let more = batch(21, 30, &[22]);
        append(&mut s, &more);
        assert!(s.releasable(20));
        drop(s);
        let opened = Staging::open(&p).unwrap();
        assert_eq!(opened.batches, vec![live, more]);
    }

    #[test]
    fn a_small_dead_prefix_is_remembered_without_a_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let p = path(&dir);
        let mut s = Staging::open(&p).unwrap().staging;
        let (a, b) = (batch(1, 4, &[2]), batch(5, 9, &[6]));
        append(&mut s, &a);
        append(&mut s, &b);
        let before = s.bytes();
        assert_eq!(s.release_through(4).unwrap(), 0);
        assert_eq!(s.bytes(), before);
        assert!(s.live_bytes() < before - HEADER_LEN);
        drop(s);
        // The dead frame is still in the file; recovery filters it by position.
        assert_eq!(Staging::open(&p).unwrap().batches, vec![a, b]);
    }

    #[test]
    fn a_leftover_temporary_file_is_removed_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let p = path(&dir);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(with_tmp(&p), b"half a rewrite").unwrap();
        Staging::open(&p).unwrap();
        assert!(!with_tmp(&p).exists());
    }

    #[test]
    fn decode_errors_name_no_row_content() {
        let mut b = batch(1, 2, &[2]);
        b.rows[0].reason = "SECRET-PAYLOAD-TEXT".to_owned();
        let mut payload = b.encode_payload().unwrap();
        payload.truncate(payload.len() - 3);
        let e = StagedBatch::decode_payload(&payload).err().unwrap();
        assert!(!e.to_string().contains("SECRET"), "{e}");
    }
}
