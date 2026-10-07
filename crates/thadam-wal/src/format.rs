//! Segment and frame layout (ADR-0004, S1 section 7.2). All integers are
//! little-endian.
//!
//! Segment header, 64 bytes:
//!
//! ```text
//! [0, 8)   magic "THADMWAL"
//! [8, 10)  format_version u16
//! [10, 12) reserved u16 (zero)
//! [12, 20) epoch u64
//! [20, 28) base_seq u64
//! [28, 36) created_unix u64 (informational; never compared)
//! [36, 60) zero
//! [60, 64) crc32c of [0, 60)
//! ```
//!
//! Frame header, 24 bytes, followed by `len` payload bytes:
//!
//! ```text
//! magic u16 | version u8 | flags u8 | len u32 | frame_seq u64 | reserved u32 | crc32c u32
//! ```
//!
//! The CRC covers header bytes `[0, 20)` followed by the payload. An all-zero
//! header marks the end of data.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;

pub(crate) const SEG_MAGIC: &[u8; 8] = b"THADMWAL";
pub(crate) const SEG_FORMAT_VERSION: u16 = 1;
pub(crate) const SEG_HDR: u64 = 64;

pub(crate) const FRAME_MAGIC: u16 = 0xA7F1;
pub(crate) const FRAME_VERSION: u8 = 1;
pub(crate) const FRAME_HDR: usize = 24;

/// Read size for sequential scans (recovery and reader positioning).
pub(crate) const SCAN_CHUNK: usize = 1 << 20;

pub(crate) fn segment_file_name(base: u64) -> String {
    format!("{base:020}.seg")
}

pub(crate) fn parse_segment_file_name(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(".seg")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

pub(crate) fn encode_segment_header(
    epoch: u64,
    base: u64,
    created_unix: u64,
) -> [u8; SEG_HDR as usize] {
    let mut h = [0u8; SEG_HDR as usize];
    h[0..8].copy_from_slice(SEG_MAGIC);
    h[8..10].copy_from_slice(&SEG_FORMAT_VERSION.to_le_bytes());
    h[12..20].copy_from_slice(&epoch.to_le_bytes());
    h[20..28].copy_from_slice(&base.to_le_bytes());
    h[28..36].copy_from_slice(&created_unix.to_le_bytes());
    let crc = crc32c::crc32c(&h[..60]);
    h[60..64].copy_from_slice(&crc.to_le_bytes());
    h
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SegmentHeader {
    pub epoch: u64,
    pub base: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SegmentHeaderError {
    Short,
    BadMagic,
    BadCrc,
    NonZeroReserved,
    /// A version this binary does not know. Newer versions are refused, never
    /// guessed at.
    UnsupportedVersion(u16),
}

impl SegmentHeaderError {
    pub(crate) fn describe(self) -> String {
        match self {
            SegmentHeaderError::Short => "header is shorter than 64 bytes".into(),
            SegmentHeaderError::BadMagic => "bad magic".into(),
            SegmentHeaderError::BadCrc => "header checksum mismatch".into(),
            SegmentHeaderError::NonZeroReserved => "reserved header bytes are not zero".into(),
            SegmentHeaderError::UnsupportedVersion(v) => format!(
                "segment format version {v} is not supported (this binary supports {SEG_FORMAT_VERSION})"
            ),
        }
    }
}

pub(crate) fn decode_segment_header(h: &[u8]) -> Result<SegmentHeader, SegmentHeaderError> {
    if h.len() < SEG_HDR as usize {
        return Err(SegmentHeaderError::Short);
    }
    if &h[0..8] != SEG_MAGIC {
        return Err(SegmentHeaderError::BadMagic);
    }
    if crc32c::crc32c(&h[..60]) != le_u32(&h[60..64]) {
        return Err(SegmentHeaderError::BadCrc);
    }
    let version = u16::from_le_bytes([h[8], h[9]]);
    if version != SEG_FORMAT_VERSION {
        return Err(SegmentHeaderError::UnsupportedVersion(version));
    }
    if h[10..12] != [0, 0] || h[36..60].iter().any(|&b| b != 0) {
        return Err(SegmentHeaderError::NonZeroReserved);
    }
    Ok(SegmentHeader {
        epoch: le_u64(&h[12..20]),
        base: le_u64(&h[20..28]),
    })
}

fn le_u32(b: &[u8]) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&b[..4]);
    u32::from_le_bytes(a)
}

fn le_u64(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8]);
    u64::from_le_bytes(a)
}

/// Appends a frame header placeholder to `out` and returns its offset. The
/// caller appends the payload and then calls [`finish_frame`].
pub(crate) fn begin_frame(out: &mut Vec<u8>) -> usize {
    let start = out.len();
    out.resize(start + FRAME_HDR, 0);
    start
}

/// Fills in the header of the frame that starts at `start` and whose payload
/// runs to the end of `out`.
pub(crate) fn finish_frame(out: &mut [u8], start: usize, seq: u64) {
    let len = u32::try_from(out.len() - start - FRAME_HDR).unwrap_or(u32::MAX);
    let h = &mut out[start..start + FRAME_HDR];
    h[0..2].copy_from_slice(&FRAME_MAGIC.to_le_bytes());
    h[2] = FRAME_VERSION;
    h[3] = 0;
    h[4..8].copy_from_slice(&len.to_le_bytes());
    h[8..16].copy_from_slice(&seq.to_le_bytes());
    h[16..20].copy_from_slice(&0u32.to_le_bytes());
    let crc = crc32c::crc32c_append(
        crc32c::crc32c(&out[start..start + 20]),
        &out[start + FRAME_HDR..],
    );
    out[start + 20..start + FRAME_HDR].copy_from_slice(&crc.to_le_bytes());
}

/// Encodes one complete frame. The writer builds frames in place with
/// [`begin_frame`] and [`finish_frame`] instead.
#[cfg(test)]
pub(crate) fn encode_frame(out: &mut Vec<u8>, seq: u64, payload: &[u8]) {
    let start = begin_frame(out);
    out.extend_from_slice(payload);
    finish_frame(out, start, seq);
}

/// The result of parsing the bytes at a frame boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// A valid frame with the expected sequence number and `len` payload bytes.
    Frame { seq: u64, len: usize },
    /// At least this many bytes (from the boundary) are needed to decide.
    NeedMore(usize),
    /// An all-zero header: clean end of data.
    Zero,
    /// Anything else: torn, corrupt, stale, or out of sequence.
    Bad(&'static str),
}

/// Parses the frame starting at `buf[0]`, expecting sequence `expect`.
///
/// Checks, in order: the zero marker, magic, version, flags, reserved word,
/// `len` against `frame_max`, `frame_seq` continuity, then the CRC.
pub(crate) fn parse_frame(buf: &[u8], expect: u64, frame_max: usize) -> Step {
    if buf.len() < FRAME_HDR {
        return Step::NeedMore(FRAME_HDR);
    }
    let h = &buf[..FRAME_HDR];
    if h.iter().all(|&b| b == 0) {
        return Step::Zero;
    }
    if u16::from_le_bytes([h[0], h[1]]) != FRAME_MAGIC {
        return Step::Bad("bad frame magic");
    }
    if h[2] != FRAME_VERSION {
        return Step::Bad("unsupported frame version");
    }
    if h[3] != 0 {
        return Step::Bad("unknown frame flags");
    }
    if le_u32(&h[16..20]) != 0 {
        return Step::Bad("frame reserved word is not zero");
    }
    let len = le_u32(&h[4..8]) as usize;
    if len > frame_max {
        return Step::Bad("frame length exceeds frame_max_bytes");
    }
    let seq = le_u64(&h[8..16]);
    if seq != expect {
        return Step::Bad("frame_seq discontinuity");
    }
    if buf.len() < FRAME_HDR + len {
        return Step::NeedMore(FRAME_HDR + len);
    }
    let crc = crc32c::crc32c_append(crc32c::crc32c(&h[..20]), &buf[FRAME_HDR..FRAME_HDR + len]);
    if crc != le_u32(&h[20..24]) {
        return Step::Bad("frame checksum mismatch");
    }
    Step::Frame { seq, len }
}

/// Why a sequential scan stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanEnd {
    /// End of file exactly at a frame boundary.
    Eof,
    /// An all-zero header (clean end, for example a preallocated tail).
    Zero,
    /// Torn or corrupt bytes at the boundary.
    Bad(&'static str),
    /// The scan reached the requested stop sequence.
    Reached,
}

/// What a scan found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ScanResult {
    /// Offset just past the last valid frame.
    pub end: u64,
    /// Sequence number the next frame would have.
    pub next_seq: u64,
    /// Valid frames seen.
    pub frames: u64,
    pub stop: ScanEnd,
}

/// Walks frames from `off` (a frame boundary holding `expect`) until a frame
/// with sequence `stop_before` would be next, or the data ends. Every frame is
/// fully validated, including its CRC.
pub(crate) fn scan_frames(
    file: &File,
    file_len: u64,
    mut off: u64,
    mut expect: u64,
    stop_before: u64,
    frame_max: usize,
) -> io::Result<ScanResult> {
    let mut buf: Vec<u8> = Vec::new();
    let mut buf_off = off;
    let mut frames = 0;
    loop {
        if expect >= stop_before {
            return Ok(done(off, expect, frames, ScanEnd::Reached));
        }
        let remaining = file_len.saturating_sub(off);
        if remaining == 0 {
            return Ok(done(off, expect, frames, ScanEnd::Eof));
        }
        let start = usize::try_from(off - buf_off).unwrap_or(usize::MAX);
        let have = buf.get(start..).unwrap_or(&[]);
        match parse_frame(have, expect, frame_max) {
            Step::Frame { len, .. } => {
                off += (FRAME_HDR + len) as u64;
                expect += 1;
                frames += 1;
            }
            Step::Zero => return Ok(done(off, expect, frames, ScanEnd::Zero)),
            Step::Bad(why) => return Ok(done(off, expect, frames, ScanEnd::Bad(why))),
            Step::NeedMore(need) => {
                if have.len() as u64 >= remaining {
                    // The frame would run past EOF. A short all-zero tail is a
                    // clean end; anything else is a torn write.
                    let end = if have.iter().all(|&b| b == 0) {
                        ScanEnd::Zero
                    } else {
                        ScanEnd::Bad("truncated frame")
                    };
                    return Ok(done(off, expect, frames, end));
                }
                refill(file, &mut buf, &mut buf_off, off, need as u64, remaining)?;
            }
        }
    }
}

fn done(end: u64, next_seq: u64, frames: u64, stop: ScanEnd) -> ScanResult {
    ScanResult {
        end,
        next_seq,
        frames,
        stop,
    }
}

fn refill(
    file: &File,
    buf: &mut Vec<u8>,
    buf_off: &mut u64,
    off: u64,
    need: u64,
    remaining: u64,
) -> io::Result<()> {
    let want = need.max(SCAN_CHUNK as u64).min(remaining);
    let want = usize::try_from(want).unwrap_or(usize::MAX);
    buf.clear();
    buf.resize(want, 0);
    let n = read_full_at(file, buf, off)?;
    buf.truncate(n);
    *buf_off = off;
    if (n as u64) < need.min(remaining) {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "segment shrank while it was being read",
        ));
    }
    Ok(())
}

/// Reads until `buf` is full or EOF; returns the bytes read.
pub(crate) fn read_full_at(file: &File, buf: &mut [u8], off: u64) -> io::Result<usize> {
    let mut done = 0;
    while done < buf.len() {
        match file.read_at(&mut buf[done..], off + done as u64) {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(done)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 1 << 20;

    #[test]
    fn segment_header_round_trips() {
        let h = encode_segment_header(0xdead_beef, 42, 1_700_000_000);
        assert_eq!(
            decode_segment_header(&h),
            Ok(SegmentHeader {
                epoch: 0xdead_beef,
                base: 42
            })
        );
    }

    #[test]
    fn segment_header_detects_every_bit_flip() {
        let h = encode_segment_header(7, 1, 3);
        for byte in 0..SEG_HDR as usize {
            for bit in 0..8 {
                let mut bad = h;
                bad[byte] ^= 1 << bit;
                assert!(
                    decode_segment_header(&bad).is_err(),
                    "byte {byte} bit {bit}"
                );
            }
        }
        assert_eq!(
            decode_segment_header(&h[..63]),
            Err(SegmentHeaderError::Short)
        );
    }

    #[test]
    fn newer_segment_version_is_refused_not_guessed() {
        let mut h = encode_segment_header(7, 1, 3);
        h[8..10].copy_from_slice(&2u16.to_le_bytes());
        let crc = crc32c::crc32c(&h[..60]);
        h[60..64].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            decode_segment_header(&h),
            Err(SegmentHeaderError::UnsupportedVersion(2))
        );
    }

    #[test]
    fn frame_layout_is_24_bytes_little_endian() {
        let mut out = Vec::new();
        encode_frame(&mut out, 0x0102_0304_0506_0708, b"abc");
        assert_eq!(out.len(), FRAME_HDR + 3);
        assert_eq!(&out[0..2], &FRAME_MAGIC.to_le_bytes());
        assert_eq!(out[2], FRAME_VERSION);
        assert_eq!(out[3], 0);
        assert_eq!(&out[4..8], &3u32.to_le_bytes());
        assert_eq!(&out[8..16], &0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(&out[16..20], &[0, 0, 0, 0]);
        let crc = crc32c::crc32c_append(crc32c::crc32c(&out[..20]), b"abc");
        assert_eq!(&out[20..24], &crc.to_le_bytes());
        assert_eq!(&out[24..], b"abc");
    }

    #[test]
    fn crc32c_matches_the_published_check_value() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn parse_accepts_a_valid_frame() {
        let mut out = Vec::new();
        encode_frame(&mut out, 9, b"payload");
        assert_eq!(parse_frame(&out, 9, MAX), Step::Frame { seq: 9, len: 7 });
        assert_eq!(parse_frame(&out[..10], 9, MAX), Step::NeedMore(FRAME_HDR));
        assert_eq!(parse_frame(&out[..30], 9, MAX), Step::NeedMore(31));
    }

    #[test]
    fn parse_rejects_every_single_bit_flip() {
        let mut out = Vec::new();
        encode_frame(&mut out, 5, b"some payload bytes");
        for byte in 0..out.len() {
            for bit in 0..8 {
                let mut bad = out.clone();
                bad[byte] ^= 1 << bit;
                let step = parse_frame(&bad, 5, MAX);
                assert!(
                    matches!(step, Step::Bad(_) | Step::NeedMore(_)),
                    "byte {byte} bit {bit}: {step:?}"
                );
            }
        }
    }

    #[test]
    fn parse_checks_sequence_length_and_zero_marker() {
        let mut out = Vec::new();
        encode_frame(&mut out, 5, b"x");
        assert_eq!(
            parse_frame(&out, 6, MAX),
            Step::Bad("frame_seq discontinuity")
        );
        assert_eq!(
            parse_frame(&out, 5, 0),
            Step::Bad("frame length exceeds frame_max_bytes")
        );
        assert_eq!(parse_frame(&[0u8; FRAME_HDR], 5, MAX), Step::Zero);
    }

    #[test]
    fn empty_payload_frames_are_valid() {
        let mut out = Vec::new();
        encode_frame(&mut out, 1, b"");
        assert_eq!(parse_frame(&out, 1, MAX), Step::Frame { seq: 1, len: 0 });
    }

    #[test]
    fn segment_file_names_round_trip() {
        assert_eq!(segment_file_name(17), "00000000000000000017.seg");
        assert_eq!(
            parse_segment_file_name("00000000000000000017.seg"),
            Some(17)
        );
        assert_eq!(parse_segment_file_name("17.seg"), None);
        assert_eq!(parse_segment_file_name("0000000000000000001x.seg"), None);
        assert_eq!(
            parse_segment_file_name("00000000000000000017.seg.tmp"),
            None
        );
    }
}
