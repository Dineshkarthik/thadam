//! Read-only inspection of a WAL directory.
//!
//! Unlike [`open`](crate::open), inspection never takes `LOCK`, never truncates
//! or removes files, and never creates anything, so it is safe while a writer
//! holds the WAL. A torn tail is reported, not repaired. The positions it
//! reports are a snapshot: frames appended after the scan are not included.

use std::collections::VecDeque;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use thadam_core::{Timestamp, WalEpoch, WalPos};

use crate::cursor::{self, CursorState, validate_name};
use crate::error::WalError;
use crate::format::{self, SEG_HDR, ScanEnd, SegmentHeaderError};
use crate::meta;
use crate::proto;
use crate::wal::{CLEAN_MARKER, CURSORS_DIR, SEGMENTS_DIR};

/// Inspection accepts any frame length the header declares; the writer's
/// `frame_max_bytes` limit is a write-side policy.
const ANY_FRAME: usize = u32::MAX as usize;

/// One segment file that passed its header check.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct SegmentInfo {
    /// The first position the segment holds.
    pub base: WalPos,
    /// The segment file.
    pub path: PathBuf,
    /// The file's size on disk, including any torn tail.
    pub bytes: u64,
}

/// A durable consumer cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CursorInfo {
    /// The cursor name.
    pub name: String,
    /// The cursor's last durable state.
    pub state: CursorState,
}

/// One frame as stored: a source batch.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct FrameInfo {
    /// The frame's position.
    pub seq: WalPos,
    /// The source that produced the batch.
    pub source: String,
    /// Whether the clock was synchronized when the batch was received.
    pub clock_synced: bool,
    /// The messages in receive order.
    pub records: Vec<RecordInfo>,
}

/// One message inside a [`FrameInfo`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RecordInfo {
    /// When the edge received the message.
    pub ingest_time: Timestamp,
    /// The raw payload.
    pub payload: Bytes,
}

/// What a read-only scan of a WAL directory found.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WalInspection {
    /// The WAL directory.
    pub dir: PathBuf,
    /// The WAL's epoch.
    pub epoch: WalEpoch,
    /// The node the WAL is bound to, if recorded.
    pub node_id: Option<String>,
    /// The machine the WAL is bound to, if recorded.
    pub machine_id: Option<String>,
    /// The last writer shut down cleanly.
    pub clean_shutdown: bool,
    /// Retained segments, oldest first.
    pub segments: Vec<SegmentInfo>,
    /// The oldest retained position.
    pub first_seq: WalPos,
    /// The position the next frame will get.
    pub next_seq: WalPos,
    /// The last durable frame, if any.
    pub durable_end: Option<WalPos>,
    /// Bytes held by retained segment files.
    pub retained_bytes: u64,
    /// The last segment ends in bytes that are not a valid frame.
    pub torn_tail: bool,
    /// The last segment ends in a zero-filled tail.
    pub zero_tail: bool,
    /// Durable cursors, sorted by name.
    pub cursors: Vec<CursorInfo>,
}

/// Scans the WAL in `dir` without locking or modifying it.
///
/// Fails if `dir` holds no `wal.json`, or if a header or cursor is unreadable
/// in a way recovery would refuse too.
pub fn inspect(dir: &Path) -> Result<WalInspection, WalError> {
    let meta = meta::load(dir)?.ok_or_else(|| {
        WalError::invalid(format!(
            "{} is not a WAL directory (no {})",
            dir.display(),
            meta::META_FILE
        ))
    })?;
    let epoch = meta.epoch;

    let found = list_segments(&dir.join(SEGMENTS_DIR))?;
    let mut segments = Vec::with_capacity(found.len());
    let mut next_seq = 1;
    let n = found.len();
    for (i, (base, path)) in found.into_iter().enumerate() {
        let file = File::open(&path).map_err(|e| WalError::io("open", &path, e))?;
        let len = file
            .metadata()
            .map_err(|e| WalError::io("stat", &path, e))?
            .len();
        let mut h = [0u8; SEG_HDR as usize];
        let got =
            format::read_full_at(&file, &mut h, 0).map_err(|e| WalError::io("read", &path, e))?;
        match format::decode_segment_header(&h[..got]) {
            Ok(hdr) => {
                if hdr.epoch != epoch.get() {
                    return Err(WalError::Incompatible(format!(
                        "segment {} belongs to epoch {:x}, the wal is epoch {epoch}",
                        path.display(),
                        hdr.epoch
                    )));
                }
                if hdr.base != base {
                    return Err(WalError::Corrupt(format!(
                        "segment {} has base {} in its header",
                        path.display(),
                        hdr.base
                    )));
                }
            }
            Err(SegmentHeaderError::UnsupportedVersion(v)) => {
                return Err(WalError::Incompatible(format!(
                    "segment {}: {}",
                    path.display(),
                    SegmentHeaderError::UnsupportedVersion(v).describe()
                )));
            }
            Err(_) if i + 1 == n && len <= SEG_HDR => {
                // Recovery removes an empty last segment; inspection leaves it
                // alone but still lets its base bound the next position.
                next_seq = next_seq.max(base);
                continue;
            }
            Err(e) => {
                return Err(WalError::Corrupt(format!(
                    "segment {}: {}",
                    path.display(),
                    e.describe()
                )));
            }
        }
        segments.push(SegmentInfo {
            base: WalPos::new(base),
            path,
            bytes: len,
        });
    }

    let mut torn_tail = false;
    let mut zero_tail = false;
    if let Some(last) = segments.last() {
        let file = File::open(&last.path).map_err(|e| WalError::io("open", &last.path, e))?;
        let scan = format::scan_frames(
            &file,
            last.bytes,
            SEG_HDR,
            last.base.get(),
            u64::MAX,
            ANY_FRAME,
        )
        .map_err(|e| WalError::io("scan", &last.path, e))?;
        torn_tail = matches!(scan.stop, ScanEnd::Bad(_));
        zero_tail = scan.stop == ScanEnd::Zero && scan.end < last.bytes;
        next_seq = next_seq.max(scan.next_seq);
    }
    let durable_end = segments
        .last()
        .map(|_| next_seq - 1)
        .filter(|&d| d > 0)
        .map(WalPos::new);

    let cursors = load_cursors(&dir.join(CURSORS_DIR), epoch)?;
    let high = cursors
        .iter()
        .map(|c| c.state.high_water())
        .max()
        .unwrap_or(0);
    if high >= next_seq {
        next_seq = high + 1;
    }

    Ok(WalInspection {
        dir: dir.to_owned(),
        epoch,
        node_id: meta.node_id,
        machine_id: meta.machine_id,
        clean_shutdown: dir.join(CLEAN_MARKER).is_file(),
        first_seq: WalPos::new(segments.first().map_or(next_seq, |s| s.base.get())),
        next_seq: WalPos::new(next_seq),
        durable_end,
        retained_bytes: segments.iter().map(|s| s.bytes).sum(),
        segments,
        torn_tail,
        zero_tail,
        cursors,
    })
}

impl WalInspection {
    /// Visits every retained frame in position order. Stops early when `visit`
    /// returns `false`. Segments are read one at a time; a segment that fails
    /// validation stops at its first bad frame.
    pub fn visit_frames(&self, mut visit: impl FnMut(FrameInfo) -> bool) -> Result<(), WalError> {
        for seg in &self.segments {
            if !visit_segment(seg, &mut visit)? {
                break;
            }
        }
        Ok(())
    }

    /// The last `n` retained frames, in position order. Reads segments from
    /// the newest backwards, so only the tail of the log is touched.
    pub fn newest_frames(&self, n: usize) -> Result<Vec<FrameInfo>, WalError> {
        let mut out: VecDeque<FrameInfo> = VecDeque::new();
        if n == 0 {
            return Ok(Vec::new());
        }
        for seg in self.segments.iter().rev() {
            let mut local = VecDeque::new();
            visit_segment(seg, &mut |f| {
                local.push_back(f);
                if local.len() > n {
                    local.pop_front();
                }
                true
            })?;
            for f in local.into_iter().rev() {
                if out.len() == n {
                    break;
                }
                out.push_front(f);
            }
            if out.len() == n {
                break;
            }
        }
        Ok(out.into())
    }

    /// The first frame, in position order, whose first message was ingested
    /// at or after `at`. Assumes ingest times rise with position, which holds
    /// for one edge's writer.
    pub fn first_seq_ingested_since(&self, at: Timestamp) -> Result<Option<WalPos>, WalError> {
        let mut found = None;
        self.visit_frames(|f| {
            if frame_time(&f).is_some_and(|t| t.as_nanos() >= at.as_nanos()) {
                found = Some(f.seq);
                false
            } else {
                true
            }
        })?;
        Ok(found)
    }

    /// The last frame, in position order, whose first message was ingested at
    /// or before `at`.
    pub fn last_seq_ingested_until(&self, at: Timestamp) -> Result<Option<WalPos>, WalError> {
        let mut found = None;
        self.visit_frames(|f| {
            if frame_time(&f).is_some_and(|t| t.as_nanos() <= at.as_nanos()) {
                found = Some(f.seq);
            }
            true
        })?;
        Ok(found)
    }
}

fn frame_time(f: &FrameInfo) -> Option<Timestamp> {
    f.records.first().map(|r| r.ingest_time)
}

/// Returns `Ok(false)` when `visit` asked to stop.
fn visit_segment(
    seg: &SegmentInfo,
    visit: &mut impl FnMut(FrameInfo) -> bool,
) -> Result<bool, WalError> {
    let file = File::open(&seg.path).map_err(|e| WalError::io("open", &seg.path, e))?;
    let mut decode_err = None;
    let mut keep_going = true;
    format::scan_visit(
        &file,
        seg.bytes,
        SEG_HDR,
        seg.base.get(),
        u64::MAX,
        ANY_FRAME,
        |seq, payload| match decode_frame(seq, payload) {
            Ok(frame) => {
                keep_going = visit(frame);
                keep_going
            }
            Err(e) => {
                decode_err = Some(e);
                false
            }
        },
    )
    .map_err(|e| WalError::io("scan", &seg.path, e))?;
    match decode_err {
        Some(e) => Err(e),
        None => Ok(keep_going),
    }
}

fn decode_frame(seq: u64, payload: &[u8]) -> Result<FrameInfo, WalError> {
    let raw = proto::decode(Bytes::copy_from_slice(payload))
        .map_err(|why| WalError::Corrupt(format!("frame {seq}: {why}")))?;
    let source = raw.dict[raw.source_idx as usize].clone();
    let records = raw
        .records
        .iter()
        .map(|r| RecordInfo {
            ingest_time: Timestamp::from_nanos(r.ingest_time_ns),
            payload: r.payload.clone(),
        })
        .collect();
    Ok(FrameInfo {
        seq: WalPos::new(seq),
        source,
        clock_synced: raw.clock_synced,
        records,
    })
}

fn list_segments(seg_dir: &Path) -> Result<Vec<(u64, PathBuf)>, WalError> {
    let rd = match fs::read_dir(seg_dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(WalError::io("list", seg_dir, e)),
    };
    let mut out = Vec::new();
    for entry in rd {
        let entry = entry.map_err(|e| WalError::io("list", seg_dir, e))?;
        let name = entry.file_name();
        let Some(base) = name.to_str().and_then(format::parse_segment_file_name) else {
            continue;
        };
        out.push((base, entry.path()));
    }
    out.sort_unstable_by_key(|(b, _)| *b);
    Ok(out)
}

fn load_cursors(cur_dir: &Path, epoch: WalEpoch) -> Result<Vec<CursorInfo>, WalError> {
    let rd = match fs::read_dir(cur_dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(WalError::io("list", cur_dir, e)),
    };
    let mut out = Vec::new();
    for entry in rd {
        let entry = entry.map_err(|e| WalError::io("list", cur_dir, e))?;
        let path = entry.path();
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(cursor_name) = name.strip_suffix(".cur") else {
            continue;
        };
        if validate_name(cursor_name).is_err() {
            continue;
        }
        let bytes = fs::read(&path).map_err(|e| WalError::io("read", &path, e))?;
        out.push(CursorInfo {
            name: cursor_name.to_owned(),
            state: cursor::decode(&path, epoch, &bytes)?,
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}
