//! Custom segmented log, following thadam_rust_implementation_plan section 5.3 / 4.7.
//!
//! Segment: 64-byte header then frames. Frame: 24-byte header then payload.
//!   header = magic u16 | version u8 | flags u8 | len u32 | frame_seq u64 | reserved u32 | crc32c u32
//!   crc32c covers header bytes [0..20) followed by the payload.
//! Recovery scans only the last segment; at the first invalid frame it truncates, fsyncs, and
//! appends to a fresh segment so bytes beyond the truncation point can never be re-read.

use crate::engine::{Engine, Opts, Result, SyncKind, sync_dir, sync_fd};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const SEG_MAGIC: &[u8; 8] = b"THADMWAL";
const SEG_VERSION: u16 = 1;
pub const SEG_HDR: u64 = 64;
const FRAME_MAGIC: u16 = 0xA7F1;
const FRAME_VERSION: u8 = 1;
pub const FRAME_HDR: usize = 24;
const FRAME_MAX: usize = 16 << 20;
const CUR_MAGIC: &[u8; 4] = b"RCUR";
const META_MAGIC: &[u8; 8] = b"RWALMETA";
const READ_CHUNK: usize = 1 << 20;

struct Seg {
    base: u64,
    path: PathBuf,
}

#[derive(Default, Debug)]
pub struct RecoveryInfo {
    pub segments: usize,
    pub scanned_bytes: u64,
    pub truncated_bytes: u64,
    /// The tail contained non-zero bytes that did not form a valid frame.
    pub torn: bool,
    pub quarantined: usize,
}

pub struct SegLog {
    wal_dir: PathBuf,
    cur_dir: PathBuf,
    epoch: u64,
    sync_kind: SyncKind,
    segment_bytes: u64,
    prealloc: bool,
    segs: Vec<Seg>,
    active: File,
    active_len: u64,
    next_seq: u64,
    written_next: u64,
    staged: u64,
    buf: Vec<u8>,
    unsynced: bool,
    poisoned: Option<String>,
    rd: Option<ReadPos>,
    pub recovery: RecoveryInfo,
}

pub fn encode_frame(out: &mut Vec<u8>, seq: u64, payload: &[u8]) {
    let start = out.len();
    out.extend_from_slice(&FRAME_MAGIC.to_le_bytes());
    out.push(FRAME_VERSION);
    out.push(0);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    let crc = crc32c::crc32c_append(crc32c::crc32c(&out[start..start + 20]), payload);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
}

enum Hdr {
    Frame { len: usize, seq: u64, crc: u32 },
    Zero,
    Bad,
}

fn parse_hdr(h: &[u8]) -> Hdr {
    if h.iter().all(|&b| b == 0) {
        return Hdr::Zero;
    }
    let magic = u16::from_le_bytes([h[0], h[1]]);
    let len = u32::from_le_bytes(h[4..8].try_into().unwrap()) as usize;
    let reserved = u32::from_le_bytes(h[16..20].try_into().unwrap());
    if magic != FRAME_MAGIC || h[2] != FRAME_VERSION || h[3] != 0 || reserved != 0 || len > FRAME_MAX {
        return Hdr::Bad;
    }
    Hdr::Frame {
        len,
        seq: u64::from_le_bytes(h[8..16].try_into().unwrap()),
        crc: u32::from_le_bytes(h[20..24].try_into().unwrap()),
    }
}

fn frame_crc_ok(h: &[u8], payload: &[u8], crc: u32) -> bool {
    crc32c::crc32c_append(crc32c::crc32c(&h[..20]), payload) == crc
}

fn seg_header(epoch: u64, base: u64) -> [u8; SEG_HDR as usize] {
    let mut h = [0u8; SEG_HDR as usize];
    h[0..8].copy_from_slice(SEG_MAGIC);
    h[8..10].copy_from_slice(&SEG_VERSION.to_le_bytes());
    h[12..20].copy_from_slice(&epoch.to_le_bytes());
    h[20..28].copy_from_slice(&base.to_le_bytes());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    h[28..36].copy_from_slice(&now.to_le_bytes());
    let crc = crc32c::crc32c(&h[..60]);
    h[60..64].copy_from_slice(&crc.to_le_bytes());
    h
}

fn seg_header_ok(h: &[u8], epoch: u64, base: u64) -> bool {
    h.len() == SEG_HDR as usize
        && &h[0..8] == SEG_MAGIC
        && u16::from_le_bytes([h[8], h[9]]) == SEG_VERSION
        && u64::from_le_bytes(h[12..20].try_into().unwrap()) == epoch
        && u64::from_le_bytes(h[20..28].try_into().unwrap()) == base
        && crc32c::crc32c(&h[..60]) == u32::from_le_bytes(h[60..64].try_into().unwrap())
}

fn seg_name(base: u64) -> String {
    format!("{base:020}.seg")
}

fn read_full_at(f: &File, buf: &mut [u8], off: u64) -> io::Result<usize> {
    let mut done = 0;
    while done < buf.len() {
        match f.read_at(&mut buf[done..], off + done as u64) {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(done)
}

fn preallocate(f: &File, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    #[cfg(target_os = "linux")]
    {
        let rc = unsafe { libc::fallocate(f.as_raw_fd(), 0, 0, len as libc::off_t) };
        if rc == 0 {
            return Ok(());
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EOPNOTSUPP) {
            return Err(err);
        }
        f.set_len(len)
    }
    #[cfg(target_os = "macos")]
    {
        let mut st = libc::fstore_t {
            fst_flags: libc::F_ALLOCATEALL,
            fst_posmode: libc::F_PEOFPOSMODE,
            fst_offset: 0,
            fst_length: len as libc::off_t,
            fst_bytesalloc: 0,
        };
        unsafe { libc::fcntl(f.as_raw_fd(), libc::F_PREALLOCATE, &mut st) };
        f.set_len(len)
    }
}

/// Scans a segment from its header. Returns (valid_end, last_seq, file_len, torn).
fn scan_segment(path: &Path, base: u64) -> io::Result<(u64, Option<u64>, u64, bool)> {
    let f = File::open(path)?;
    let file_len = f.metadata()?.len();
    let mut off = SEG_HDR;
    let mut expect = base;
    let mut last = None;
    let mut torn = false;
    let mut chunk = Vec::new();
    let mut chunk_off = 0u64;
    loop {
        if off + FRAME_HDR as u64 > file_len {
            torn = off < file_len;
            break;
        }
        let hdr = ensure(&f, &mut chunk, &mut chunk_off, off, FRAME_HDR, file_len)?.to_vec();
        match parse_hdr(&hdr) {
            Hdr::Zero => break,
            Hdr::Bad => {
                torn = true;
                break;
            }
            Hdr::Frame { len, seq, crc } => {
                if seq != expect || off + (FRAME_HDR + len) as u64 > file_len {
                    torn = true;
                    break;
                }
                let fr = ensure(&f, &mut chunk, &mut chunk_off, off, FRAME_HDR + len, file_len)?;
                if !frame_crc_ok(&fr[..FRAME_HDR], &fr[FRAME_HDR..], crc) {
                    torn = true;
                    break;
                }
                last = Some(seq);
                expect += 1;
                off += (FRAME_HDR + len) as u64;
            }
        }
    }
    Ok((off, last, file_len, torn))
}

/// Returns file bytes [off, off+need) using `chunk` as a read-ahead cache. Read-ahead never goes
/// past `end`: bytes beyond the written end of the active segment (e.g. preallocated zeros) must
/// not be cached, or a tailing reader would later see them instead of newly written frames.
fn ensure<'a>(f: &File, chunk: &'a mut Vec<u8>, chunk_off: &mut u64, off: u64, need: usize, end: u64) -> io::Result<&'a [u8]> {
    if off + need as u64 > end {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "frame extends past written end"));
    }
    let have = off >= *chunk_off && off + need as u64 <= *chunk_off + chunk.len() as u64;
    if !have {
        chunk.resize(need.max(READ_CHUNK.min((end - off) as usize)), 0);
        let n = read_full_at(f, chunk, off)?;
        chunk.truncate(n);
        *chunk_off = off;
        if n < need {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read inside frame"));
        }
    }
    let s = (off - *chunk_off) as usize;
    Ok(&chunk[s..s + need])
}

fn load_or_create_meta(dir: &Path, wal_dir: &Path, kind: SyncKind) -> Result<u64> {
    let path = dir.join("wal.meta");
    match fs::read(&path) {
        Ok(b) => {
            if b.len() != 20 || &b[0..8] != META_MAGIC || crc32c::crc32c(&b[..16]) != u32::from_le_bytes(b[16..20].try_into()?) {
                return Err("wal.meta is corrupt".into());
            }
            return Ok(u64::from_le_bytes(b[8..16].try_into()?));
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let has_segments = fs::read_dir(wal_dir)?
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().ends_with(".seg"));
    if has_segments {
        return Err("wal.meta missing but segments exist".into());
    }
    let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos() as u64;
    let epoch = (seed ^ ((std::process::id() as u64) << 32)) | 1;
    let mut b = Vec::with_capacity(20);
    b.extend_from_slice(META_MAGIC);
    b.extend_from_slice(&epoch.to_le_bytes());
    let crc = crc32c::crc32c(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    let tmp = dir.join("wal.meta.tmp");
    let f = File::create(&tmp)?;
    f.write_all_at(&b, 0)?;
    sync_fd(&f, kind)?;
    fs::rename(&tmp, &path)?;
    sync_dir(dir, kind)?;
    Ok(epoch)
}

fn create_segment(wal_dir: &Path, epoch: u64, base: u64, prealloc: Option<u64>, kind: SyncKind) -> io::Result<File> {
    let path = wal_dir.join(seg_name(base));
    let tmp = wal_dir.join(format!("{}.tmp", seg_name(base)));
    let f = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&tmp)?;
    f.write_all_at(&seg_header(epoch, base), 0)?;
    if let Some(len) = prealloc {
        preallocate(&f, len)?;
    }
    sync_fd(&f, kind)?;
    fs::rename(&tmp, &path)?;
    sync_dir(wal_dir, kind)?;
    Ok(f)
}

struct ReadPos {
    seg_base: u64,
    file: File,
    off: u64,
    next: u64,
    chunk: Vec<u8>,
    chunk_off: u64,
}

impl SegLog {
    pub fn open(dir: &Path, o: &Opts) -> Result<Self> {
        let kind = o.sync_kind;
        let wal_dir = dir.join("wal");
        let cur_dir = dir.join("cursors");
        fs::create_dir_all(&wal_dir)?;
        fs::create_dir_all(&cur_dir)?;
        sync_dir(dir, kind)?;
        let epoch = load_or_create_meta(dir, &wal_dir, kind)?;
        let mut info = RecoveryInfo::default();

        let mut bases = Vec::new();
        for e in fs::read_dir(&wal_dir)? {
            let e = e?;
            let name = e.file_name().into_string().map_err(|_| "non-utf8 file name")?;
            if name.ends_with(".tmp") {
                fs::remove_file(e.path())?;
            } else if let Some(b) = name.strip_suffix(".seg") {
                bases.push(b.parse::<u64>()?);
            }
        }
        bases.sort_unstable();
        let mut segs = Vec::new();
        for base in bases {
            let path = wal_dir.join(seg_name(base));
            let mut h = [0u8; SEG_HDR as usize];
            let n = read_full_at(&File::open(&path)?, &mut h, 0)?;
            if n == h.len() && seg_header_ok(&h, epoch, base) {
                segs.push(Seg { base, path });
            } else {
                fs::rename(&path, path.with_extension("corrupt"))?;
                info.quarantined += 1;
            }
        }

        let mut next_seq = 1;
        let mut reuse_last = false;
        if let Some(last) = segs.last() {
            let (valid_end, last_seq, file_len, torn) = scan_segment(&last.path, last.base)?;
            info.scanned_bytes = valid_end;
            info.torn = torn;
            next_seq = last_seq.map_or(last.base, |s| s + 1);
            reuse_last = last_seq.is_none();
            if valid_end < file_len {
                let f = OpenOptions::new().write(true).open(&last.path)?;
                f.set_len(valid_end)?;
                sync_fd(&f, kind)?;
                info.truncated_bytes = file_len - valid_end;
            }
        }

        let prealloc = o.prealloc.then_some(o.segment_bytes);
        let active = if reuse_last {
            // Truncated to its header and fsynced above, so nothing stale lies past the write point.
            let f = OpenOptions::new().read(true).write(true).open(&segs.last().unwrap().path)?;
            if let Some(len) = prealloc {
                preallocate(&f, len)?;
                sync_fd(&f, kind)?;
            }
            f
        } else {
            let f = create_segment(&wal_dir, epoch, next_seq, prealloc, kind)?;
            segs.push(Seg { base: next_seq, path: wal_dir.join(seg_name(next_seq)) });
            f
        };
        info.segments = segs.len();
        Ok(SegLog {
            wal_dir,
            cur_dir,
            epoch,
            sync_kind: kind,
            segment_bytes: o.segment_bytes,
            prealloc: o.prealloc,
            segs,
            active,
            active_len: SEG_HDR,
            next_seq,
            written_next: next_seq,
            staged: 0,
            buf: Vec::with_capacity(4 << 20),
            unsynced: false,
            poisoned: None,
            rd: None,
            recovery: info,
        })
    }

    fn check(&self) -> Result<()> {
        match &self.poisoned {
            Some(why) => Err(format!("wal poisoned: {why}").into()),
            None => Ok(()),
        }
    }

    fn poison<E: std::fmt::Display>(&mut self, what: &str, e: E) -> Box<dyn std::error::Error + Send + Sync> {
        let msg = format!("{what}: {e}");
        self.poisoned = Some(msg.clone());
        msg.into()
    }

    fn roll(&mut self, base: u64) -> io::Result<()> {
        if self.prealloc {
            self.active.set_len(self.active_len)?;
        }
        if self.unsynced {
            sync_fd(&self.active, self.sync_kind)?;
            self.unsynced = false;
        }
        let prealloc = self.prealloc.then_some(self.segment_bytes);
        let f = create_segment(&self.wal_dir, self.epoch, base, prealloc, self.sync_kind)?;
        self.segs.push(Seg { base, path: self.wal_dir.join(seg_name(base)) });
        self.active = f;
        self.active_len = SEG_HDR;
        Ok(())
    }

    fn seek(&self, target: u64) -> Result<ReadPos> {
        let idx = self.segs.partition_point(|s| s.base <= target);
        if idx == 0 {
            return Err(format!("seq {target} was deleted (first retained {})", self.segs[0].base).into());
        }
        let seg = &self.segs[idx - 1];
        let lim = self.read_limit(seg.base);
        let mut r = ReadPos {
            seg_base: seg.base,
            file: File::open(&seg.path)?,
            off: SEG_HDR,
            next: seg.base,
            chunk: Vec::new(),
            chunk_off: 0,
        };
        while r.next < target {
            let h = ensure(&r.file, &mut r.chunk, &mut r.chunk_off, r.off, FRAME_HDR, lim)?;
            match parse_hdr(h) {
                Hdr::Frame { len, seq, .. } if seq == r.next => {
                    r.off += (FRAME_HDR + len) as u64;
                    r.next += 1;
                }
                _ => return Err(format!("corrupt frame while seeking to {target}").into()),
            }
        }
        Ok(r)
    }

    fn read_limit(&self, seg_base: u64) -> u64 {
        if seg_base == self.segs.last().unwrap().base { self.active_len } else { u64::MAX }
    }
}

impl Engine for SegLog {
    fn append_group(&mut self, frames: &[&[u8]]) -> Result<u64> {
        self.check()?;
        if frames.iter().any(|p| p.len() > FRAME_MAX) {
            return Err("frame exceeds frame_max_bytes".into());
        }
        let first = self.next_seq;
        for p in frames {
            encode_frame(&mut self.buf, self.next_seq, p);
            self.next_seq += 1;
            self.staged += 1;
        }
        Ok(first)
    }

    fn flush(&mut self) -> Result<()> {
        self.check()?;
        if self.buf.is_empty() {
            return Ok(());
        }
        if self.active_len > SEG_HDR && self.active_len + self.buf.len() as u64 > self.segment_bytes {
            let base = self.next_seq - self.staged;
            if let Err(e) = self.roll(base) {
                return Err(self.poison("segment roll failed", e));
            }
        }
        if let Err(e) = self.active.write_all_at(&self.buf, self.active_len) {
            return Err(self.poison("write failed", e));
        }
        self.active_len += self.buf.len() as u64;
        self.buf.clear();
        self.staged = 0;
        self.written_next = self.next_seq;
        self.unsynced = true;
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.flush()?;
        if !self.unsynced {
            return Ok(());
        }
        if let Err(e) = sync_fd(&self.active, self.sync_kind) {
            // fsyncgate: never retry; the writer must be reopened and the log re-verified.
            return Err(self.poison("fsync failed", e));
        }
        self.unsynced = false;
        Ok(())
    }

    fn read(&mut self, after: u64, max_bytes: usize, f: &mut dyn FnMut(u64, &[u8])) -> Result<usize> {
        let target = after + 1;
        if target >= self.written_next {
            return Ok(0);
        }
        if self.rd.as_ref().map(|r| r.next) != Some(target) {
            self.rd = Some(self.seek(target)?);
        }
        let end = self.written_next;
        let mut bytes = 0;
        let mut n = 0;
        while bytes < max_bytes {
            let r = self.rd.as_mut().unwrap();
            if r.next >= end {
                break;
            }
            let idx = self.segs.partition_point(|s| s.base <= r.seg_base);
            if let Some(next_seg) = self.segs.get(idx) {
                if r.next == next_seg.base {
                    r.seg_base = next_seg.base;
                    r.file = File::open(&next_seg.path)?;
                    r.off = SEG_HDR;
                    r.chunk.clear();
                    continue;
                }
            }
            let lim = if r.seg_base == self.segs.last().unwrap().base { self.active_len } else { u64::MAX };
            let h = ensure(&r.file, &mut r.chunk, &mut r.chunk_off, r.off, FRAME_HDR, lim)?;
            let (len, crc) = match parse_hdr(h) {
                Hdr::Frame { len, seq, crc } if seq == r.next => (len, crc),
                _ => return Err(format!("corrupt frame header at seq {} (segment {})", r.next, r.seg_base).into()),
            };
            let fr = ensure(&r.file, &mut r.chunk, &mut r.chunk_off, r.off, FRAME_HDR + len, lim)?;
            if !frame_crc_ok(&fr[..FRAME_HDR], &fr[FRAME_HDR..], crc) {
                return Err(format!("crc mismatch at seq {} (segment {})", r.next, r.seg_base).into());
            }
            f(r.next, &fr[FRAME_HDR..]);
            bytes += len;
            n += 1;
            r.off += (FRAME_HDR + len) as u64;
            r.next += 1;
        }
        Ok(n)
    }

    fn commit_cursor(&mut self, name: &str, seq: u64) -> Result<()> {
        self.check()?;
        let mut b = Vec::with_capacity(28);
        b.extend_from_slice(CUR_MAGIC);
        b.extend_from_slice(&1u32.to_le_bytes());
        b.extend_from_slice(&self.epoch.to_le_bytes());
        b.extend_from_slice(&seq.to_le_bytes());
        let crc = crc32c::crc32c(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        let tmp = self.cur_dir.join(format!("{name}.cur.tmp"));
        let f = File::create(&tmp)?;
        f.write_all_at(&b, 0)?;
        sync_fd(&f, self.sync_kind)?;
        fs::rename(&tmp, self.cur_dir.join(format!("{name}.cur")))?;
        sync_dir(&self.cur_dir, self.sync_kind)?;
        Ok(())
    }

    fn load_cursor(&mut self, name: &str) -> Result<Option<u64>> {
        let b = match fs::read(self.cur_dir.join(format!("{name}.cur"))) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if b.len() != 28 || &b[0..4] != CUR_MAGIC || crc32c::crc32c(&b[..24]) != u32::from_le_bytes(b[24..28].try_into()?) {
            return Err(format!("cursor {name} is corrupt").into());
        }
        if u64::from_le_bytes(b[8..16].try_into()?) != self.epoch {
            return Err(format!("cursor {name} has a different epoch").into());
        }
        Ok(Some(u64::from_le_bytes(b[16..24].try_into()?)))
    }

    fn delete_consumed(&mut self, upto: u64) -> Result<()> {
        self.check()?;
        while self.segs.len() > 1 && self.segs[1].base - 1 <= upto {
            fs::remove_file(&self.segs[0].path)?;
            sync_dir(&self.wal_dir, self.sync_kind)?;
            self.segs.remove(0);
        }
        Ok(())
    }

    fn next_seq(&self) -> u64 {
        self.next_seq
    }

    fn first_seq(&mut self) -> Result<u64> {
        Ok(self.segs[0].base)
    }

    fn info(&self) -> String {
        let r = &self.recovery;
        format!(
            "segments={} scanned_last_segment_bytes={} truncated_bytes={} torn={} quarantined={}",
            r.segments, r.scanned_bytes, r.truncated_bytes, r.torn, r.quarantined
        )
    }
}
