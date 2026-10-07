//! Readers: own read-only descriptors, bounded by the durable end.

use std::fs::File;
use std::io;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use thadam_core::{RawBatch, SourceId, Timestamp, WalEpoch, WalPos, WalRange};
use tokio::sync::watch;

use crate::error::WalError;
use crate::format::{FRAME_HDR, SEG_HDR, ScanEnd, Step, parse_frame, read_full_at, scan_frames};
use crate::proto::{self, RawFrame, RawRecord};
use crate::shared::{Counters, Shared, Status, WriterState, lock};

/// A range of positions a reader will not receive as frames. Gaps are always
/// delivered as items, never skipped silently.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Gap {
    /// Frames failed validation (checksum, sequence, or payload decoding).
    /// Nothing after a corrupt frame in the same segment is trusted.
    Corrupt {
        /// The positions lost.
        range: WalRange,
    },
    /// Segments were deleted before this reader read them: evicted under
    /// `on_full: drop_oldest`, or removed by retention while this consumer was
    /// not pinning them.
    Evicted {
        /// The positions lost.
        range: WalRange,
        /// The reader's cursor name.
        cursor: String,
    },
    /// Positions that were never durably written, for example after a power
    /// loss under `fsync: never`, or skipped so a position at or below a
    /// durable cursor is never reused.
    Missing {
        /// The positions that do not exist.
        range: WalRange,
    },
}

impl Gap {
    /// The positions the gap covers.
    pub fn range(&self) -> WalRange {
        match self {
            Gap::Corrupt { range } | Gap::Evicted { range, .. } | Gap::Missing { range } => *range,
        }
    }
}

/// One item from [`WalReader::next`].
#[derive(Debug)]
pub enum ReadItem {
    /// Consecutive frames.
    Frames(FrameChunk),
    /// Positions that will not be delivered.
    Gap(Gap),
}

/// Consecutive frames from one read. Record payloads are zero-copy slices of
/// the read buffer, so keeping any of them alive keeps the buffer alive.
#[derive(Debug)]
pub struct FrameChunk {
    epoch: WalEpoch,
    frames: Vec<Frame>,
    bytes: usize,
}

impl FrameChunk {
    /// The positions covered, first to last.
    pub fn range(&self) -> WalRange {
        let first = self.frames.first().map_or(WalPos::ZERO, |f| f.seq);
        let last = self.frames.last().map_or(WalPos::ZERO, |f| f.seq);
        WalRange::new(self.epoch, first, last)
            .unwrap_or_else(|_| WalRange::single(self.epoch, first))
    }

    /// The frames, in position order.
    pub fn frames(&self) -> &[Frame] {
        &self.frames
    }

    /// Takes the frames.
    pub fn into_frames(self) -> Vec<Frame> {
        self.frames
    }

    /// Number of frames.
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Always false: a chunk holds at least one frame.
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// On-disk bytes of the frames, headers included.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

/// One frame: one source batch.
#[derive(Clone, Debug)]
pub struct Frame {
    seq: WalPos,
    source: SourceId,
    raw: RawFrame,
}

impl Frame {
    /// The frame's position.
    pub fn seq(&self) -> WalPos {
        self.seq
    }

    /// The source that produced the batch.
    pub fn source(&self) -> &SourceId {
        &self.source
    }

    /// Whether the clock was synchronized when the batch was received.
    pub fn clock_synced(&self) -> bool {
        self.raw.clock_synced
    }

    /// Number of messages.
    pub fn len(&self) -> usize {
        self.raw.records.len()
    }

    /// Whether the batch had no messages.
    pub fn is_empty(&self) -> bool {
        self.raw.records.is_empty()
    }

    /// The messages in receive order.
    pub fn records(&self) -> impl ExactSizeIterator<Item = RecordView<'_>> {
        self.raw.records.iter().map(|rec| RecordView {
            dict: &self.raw.dict,
            rec,
        })
    }

    /// Rebuilds the submitted batch (allocates attribute strings).
    pub fn to_raw_batch(&self) -> RawBatch {
        proto::to_raw_batch(&self.raw)
    }
}

/// One message inside a [`Frame`].
#[derive(Clone, Copy, Debug)]
pub struct RecordView<'a> {
    dict: &'a [String],
    rec: &'a RawRecord,
}

impl<'a> RecordView<'a> {
    /// When the edge received the message.
    pub fn ingest_time(&self) -> Timestamp {
        Timestamp::from_nanos(self.rec.ingest_time_ns)
    }

    /// The payload, a zero-copy slice of the read buffer.
    pub fn payload(&self) -> &'a Bytes {
        &self.rec.payload
    }

    /// Attribute pairs in their original order.
    pub fn attrs(&self) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
        proto::attr_pairs(self.dict, &self.rec.attr_refs)
    }

    /// The value of one attribute.
    pub fn attr(&self, key: &str) -> Option<&'a str> {
        self.attrs().find(|(k, _)| *k == key).map(|(_, v)| v)
    }
}

type ReadResult = Result<Option<ReadItem>, WalError>;

/// Reads frames in position order, never past the durable end.
///
/// Readers do not commit: the consumer calls
/// [`Wal::commit_cursor`](crate::Wal::commit_cursor) when it has processed
/// a range.
pub struct WalReader {
    sh: Arc<Shared>,
    core: Arc<Mutex<Core>>,
    status: watch::Receiver<Status>,
}

impl std::fmt::Debug for WalReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let c = lock(&self.core);
        f.debug_struct("WalReader")
            .field("cursor", &c.name)
            .field("position", &c.expect)
            .finish_non_exhaustive()
    }
}

impl WalReader {
    pub(crate) fn new(sh: Arc<Shared>, name: String, pos: u64) -> Self {
        let status = sh.status.subscribe();
        WalReader {
            core: Arc::new(Mutex::new(Core {
                name,
                expect: pos,
                cur: None,
                stash: None,
            })),
            sh,
            status,
        }
    }

    /// The position of the next frame this reader will return.
    pub fn position(&self) -> WalPos {
        WalPos::new(lock(&self.core).expect)
    }

    /// Moves to `pos`; the next item starts there (or is a gap covering it).
    pub fn seek(&mut self, pos: WalPos) {
        let mut c = lock(&self.core);
        c.stash = None;
        c.cur = None;
        c.expect = pos.get().max(1);
    }

    /// Returns the next item if one is durable now, without waiting. Blocking
    /// I/O: call from a blocking thread.
    pub fn try_next(&mut self, max_bytes: usize) -> ReadResult {
        let durable = self.status.borrow().durable;
        let mut c = lock(&self.core);
        if let Some(r) = c.stash.take() {
            return r;
        }
        c.read(&self.sh, max_bytes, durable)
    }

    /// Waits for and returns the next item: up to about `max_bytes` of whole
    /// frames (at least one), or a gap. `Ok(None)` once the writer has shut
    /// down and everything durable has been read.
    ///
    /// Reads run on the blocking pool. Cancel-safe: if the future is dropped,
    /// a read already in progress is kept and returned by the next call.
    pub async fn next(&mut self, max_bytes: usize) -> ReadResult {
        loop {
            let status = *self.status.borrow_and_update();
            if let Some(item) = self.read_on_pool(max_bytes, status.durable).await? {
                return Ok(Some(item));
            }
            if matches!(status.state, WriterState::Closed | WriterState::Failed) {
                return Ok(None);
            }
            if self.status.changed().await.is_err() {
                return Ok(None);
            }
        }
    }

    async fn read_on_pool(&self, max_bytes: usize, durable: u64) -> ReadResult {
        if let Some(r) = lock(&self.core).stash.take() {
            return r;
        }
        let core = Arc::clone(&self.core);
        let sh = Arc::clone(&self.sh);
        tokio::task::spawn_blocking(move || {
            let mut c = lock(&core);
            if c.stash.is_none() {
                let r = c.read(&sh, max_bytes, durable);
                c.stash = Some(r);
            }
        })
        .await
        .map_err(|_| WalError::Corrupt("wal read task panicked".into()))?;
        lock(&self.core).stash.take().unwrap_or(Ok(None))
    }
}

struct Core {
    name: String,
    /// Position of the next frame to deliver.
    expect: u64,
    cur: Option<Cur>,
    /// A result produced for a caller that went away (cancel safety).
    stash: Option<ReadResult>,
}

struct Cur {
    file: File,
    base: u64,
    /// Offset of the frame holding `expect`.
    off: u64,
}

#[derive(Clone, Copy, Debug)]
enum End {
    Clean,
    Bad(&'static str),
}

enum Chunk {
    Item(ReadItem),
    End(End),
}

impl Core {
    fn read(&mut self, sh: &Shared, max_bytes: usize, durable: u64) -> ReadResult {
        loop {
            if self.expect > durable {
                return Ok(None);
            }
            if self.cur.is_none() {
                match self.locate(sh)? {
                    Located::Gap(g) => return Ok(Some(ReadItem::Gap(g))),
                    Located::Nothing => return Ok(None),
                    Located::Ready => {}
                }
                if self.cur.is_none() {
                    continue;
                }
            }
            match self.read_chunk(sh, max_bytes, durable)? {
                Chunk::Item(item) => return Ok(Some(item)),
                Chunk::End(end) => {
                    if let Some(g) = self.segment_end(sh, end)? {
                        return Ok(Some(ReadItem::Gap(g)));
                    }
                }
            }
        }
    }

    fn range(&self, sh: &Shared, first: u64, last: u64) -> Result<WalRange, WalError> {
        WalRange::new(sh.epoch, WalPos::new(first), WalPos::new(last))
            .map_err(|e| WalError::Corrupt(format!("reader {}: {e}", self.name)))
    }

    /// Opens the segment holding `expect` and positions at it.
    fn locate(&mut self, sh: &Shared) -> Result<Located, WalError> {
        loop {
            let (base, path) = {
                let idx = lock(&sh.index);
                let Some(first) = idx.segs.front() else {
                    return Ok(Located::Nothing);
                };
                if self.expect < first.base {
                    let range = self.range(sh, self.expect, first.base - 1)?;
                    self.expect = first.base;
                    return Ok(Located::Gap(Gap::Evicted {
                        range,
                        cursor: self.name.clone(),
                    }));
                }
                let Some(i) = idx.segment_for(self.expect) else {
                    return Ok(Located::Nothing);
                };
                (idx.segs[i].base, idx.segs[i].path.clone())
            };
            let file = match File::open(&path) {
                Ok(f) => f,
                // Deleted after the lookup: look again.
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(WalError::io("open", &path, e)),
            };
            let len = file
                .metadata()
                .map_err(|e| WalError::io("stat", &path, e))?
                .len();
            let scan = scan_frames(
                &file,
                len,
                SEG_HDR,
                base,
                self.expect,
                sh.opts.frame_max_bytes,
            )
            .map_err(|e| WalError::io("read", &path, e))?;
            self.cur = Some(Cur {
                file,
                base,
                off: scan.end,
            });
            let end = match scan.stop {
                ScanEnd::Reached => return Ok(Located::Ready),
                ScanEnd::Eof | ScanEnd::Zero => End::Clean,
                ScanEnd::Bad(why) => End::Bad(why),
            };
            return Ok(match self.segment_end(sh, end)? {
                Some(g) => Located::Gap(g),
                None => Located::Ready,
            });
        }
    }

    fn read_chunk(
        &mut self,
        sh: &Shared,
        max_bytes: usize,
        durable: u64,
    ) -> Result<Chunk, WalError> {
        let frame_max = sh.opts.frame_max_bytes;
        let Some(cur) = self.cur.as_mut() else {
            return Ok(Chunk::End(End::Clean));
        };
        let base = cur.base;
        let path_err = |op, e| WalError::io(op, &sh.segment_path(base), e);
        let len = cur.file.metadata().map_err(|e| path_err("stat", e))?.len();
        let remaining = len.saturating_sub(cur.off);
        if remaining == 0 {
            return Ok(Chunk::End(End::Clean));
        }
        let want = (max_bytes.max(FRAME_HDR) as u64).min(remaining);
        let mut buf = read_bytes(&cur.file, cur.off, want).map_err(|e| path_err("read", e))?;
        let mut pos = 0usize;
        let mut expect = self.expect;
        let mut frames = Vec::new();
        let mut reread = false;
        while expect <= durable {
            match parse_frame(&buf[pos..], expect, frame_max) {
                Step::Frame { seq, len } => {
                    let size = FRAME_HDR + len;
                    if !frames.is_empty() && pos + size > max_bytes {
                        break;
                    }
                    let payload = buf.slice(pos + FRAME_HDR..pos + size);
                    match proto::decode(payload) {
                        Ok(raw) => frames.push(Frame {
                            seq: WalPos::new(seq),
                            source: SourceId::new(raw.dict[raw.source_idx as usize].as_str()),
                            raw,
                        }),
                        Err(_) if frames.is_empty() => {
                            // The checksum held, so the frame boundary is
                            // trustworthy: only this one frame is lost.
                            cur.off += size as u64;
                            self.expect = seq + 1;
                            Counters::add(&sh.counters.corrupt_gaps, 1);
                            let range = WalRange::single(sh.epoch, WalPos::new(seq));
                            return Ok(Chunk::Item(ReadItem::Gap(Gap::Corrupt { range })));
                        }
                        Err(_) => break,
                    }
                    pos += size;
                    expect += 1;
                }
                Step::NeedMore(need) => {
                    if !frames.is_empty() {
                        break;
                    }
                    if need as u64 <= remaining && !reread {
                        buf = read_bytes(&cur.file, cur.off, need as u64)
                            .map_err(|e| path_err("read", e))?;
                        reread = true;
                        continue;
                    }
                    let end = if buf.iter().all(|&b| b == 0) {
                        End::Clean
                    } else {
                        End::Bad("truncated frame")
                    };
                    return Ok(Chunk::End(end));
                }
                Step::Zero if frames.is_empty() => return Ok(Chunk::End(End::Clean)),
                Step::Bad(why) if frames.is_empty() => return Ok(Chunk::End(End::Bad(why))),
                Step::Zero | Step::Bad(_) => break,
            }
        }
        if frames.is_empty() {
            return Ok(Chunk::End(End::Clean));
        }
        cur.off += pos as u64;
        self.expect = expect;
        Ok(Chunk::Item(ReadItem::Frames(FrameChunk {
            epoch: sh.epoch,
            frames,
            bytes: pos,
        })))
    }

    /// The current segment has no frame at `expect`: move to the next segment,
    /// reporting the positions in between as a gap.
    fn segment_end(&mut self, sh: &Shared, end: End) -> Result<Option<Gap>, WalError> {
        let Some(cur_base) = self.cur.as_ref().map(|c| c.base) else {
            return Ok(None);
        };
        let (next, cur_retained) = {
            let idx = lock(&sh.index);
            let i = idx.segs.partition_point(|s| s.base <= cur_base);
            (
                idx.segs.get(i).map(|s| s.base),
                idx.segs.iter().any(|s| s.base == cur_base),
            )
        };
        let Some(next) = next else {
            return Err(WalError::Corrupt(format!(
                "reader {}: frame {} is durable but the newest segment ends before it ({})",
                self.name,
                self.expect,
                describe(end)
            )));
        };
        self.cur = None;
        if next == self.expect {
            return Ok(None);
        }
        if next < self.expect {
            return Err(WalError::Corrupt(format!(
                "reader {}: segment {next} starts before position {}",
                self.name, self.expect
            )));
        }
        let range = self.range(sh, self.expect, next - 1)?;
        self.expect = next;
        Ok(Some(match end {
            End::Bad(_) => {
                Counters::add(&sh.counters.corrupt_gaps, 1);
                Gap::Corrupt { range }
            }
            End::Clean if !cur_retained => Gap::Evicted {
                range,
                cursor: self.name.clone(),
            },
            End::Clean => Gap::Missing { range },
        }))
    }
}

enum Located {
    Ready,
    Gap(Gap),
    Nothing,
}

fn describe(end: End) -> &'static str {
    match end {
        End::Clean => "clean end",
        End::Bad(why) => why,
    }
}

fn read_bytes(file: &File, off: u64, len: u64) -> io::Result<Bytes> {
    let mut v = vec![0u8; usize::try_from(len).unwrap_or(usize::MAX)];
    let n = read_full_at(file, &mut v, off)?;
    v.truncate(n);
    Ok(Bytes::from(v))
}
