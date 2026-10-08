//! The append-only, CRC-checked `catalog.log`, plus small fs helpers.
//!
//! Frame layout: `[len: u32 LE][payload: len bytes][crc32c(payload): u32 LE]`.
//! The first frame is a header naming the format and its version. A torn
//! final frame is a normal crash artifact and is truncated (with fsync); a
//! CRC mismatch or a bad header is fatal.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::catalog::record::CatalogRecord;
use crate::error::SegmentError;

const HEADER: &str = r#"{"kind":"thadam-segment-catalog","format_version":1}"#;
const MAX_FRAME: usize = 16 << 20;

pub(crate) fn fsync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

pub(crate) fn fsync_file(file: &File) -> io::Result<()> {
    file.sync_data()
}

/// Creates every missing component of `dir`, fsyncing each newly created
/// directory, assuming the sink root already exists.
pub(crate) fn create_dirs(dir: &Path) -> Result<(), SegmentError> {
    let mut new_dirs = Vec::new();
    let mut probe = Some(dir);
    while let Some(d) = probe {
        if d.exists() {
            break;
        }
        new_dirs.push(d.to_path_buf());
        probe = d.parent();
    }
    if new_dirs.is_empty() {
        return Ok(());
    }
    fs::create_dir_all(dir).map_err(|e| SegmentError::io("create_dir_all", dir, e))?;
    for d in &new_dirs {
        fsync_dir(d).map_err(|e| SegmentError::io("fsync dir", d, e))?;
    }
    if let Some(parent) = new_dirs
        .last()
        .and_then(|d| d.parent().map(Path::to_path_buf))
    {
        fsync_dir(&parent).map_err(|e| SegmentError::io("fsync dir", parent, e))?;
    }
    Ok(())
}

pub(crate) struct LogWriter {
    file: File,
    len: u64,
}

impl LogWriter {
    /// Creates a fresh `catalog.log` with the header frame, and fsyncs. The
    /// parent directory must exist.
    pub(crate) fn create(path: &Path) -> Result<Self, SegmentError> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path)
            .map_err(|e| SegmentError::io("create", path, e))?;
        Self::write_header(&mut file, path)?;
        Ok(LogWriter {
            file,
            len: HEADER.len() as u64 + 8,
        })
    }

    /// Takes over an existing, fully-replayed log positioned at its end.
    pub(crate) fn resume(path: &Path, len: u64, truncated: bool) -> Result<Self, SegmentError> {
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .open(path)
            .map_err(|e| SegmentError::io("open", path, e))?;
        if len == 0 {
            // A crash between create and the header write: write it now; no
            // records could have existed.
            Self::write_header(&mut file, path)?;
            return Ok(LogWriter {
                file,
                len: HEADER.len() as u64 + 8,
            });
        }
        if truncated {
            file.set_len(len)
                .map_err(|e| SegmentError::io("truncate", path, e))?;
            fsync_file(&file).map_err(|e| SegmentError::io("fsync", path, e))?;
        }
        file.seek(SeekFrom::Start(len))
            .map_err(|e| SegmentError::io("seek", path, e))?;
        Ok(LogWriter { file, len })
    }

    fn write_header(file: &mut File, path: &Path) -> Result<(), SegmentError> {
        let payload = HEADER.as_bytes();
        let mut frame = Vec::with_capacity(payload.len() + 8);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(payload);
        frame.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
        file.write_all(&frame)
            .map_err(|e| SegmentError::io("write header", path, e))?;
        fsync_file(file).map_err(|e| SegmentError::io("fsync header", path, e))
    }

    /// Appends records in one write and one fdatasync.
    pub(crate) fn append(&mut self, records: &[CatalogRecord]) -> Result<(), SegmentError> {
        let mut frame = Vec::new();
        for record in records {
            let payload = serde_json::to_vec(record)
                .map_err(|e| SegmentError::catalog(format!("record does not serialize: {e}")))?;
            if payload.len() > MAX_FRAME {
                return Err(SegmentError::catalog(
                    "record exceeds the maximum frame size",
                ));
            }
            frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            frame.extend_from_slice(&payload);
            frame.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        }
        self.file
            .write_all(&frame)
            .map_err(|e| SegmentError::io("append", "catalog.log", e))?;
        fsync_file(&self.file).map_err(|e| SegmentError::io("fsync", "catalog.log", e))?;
        self.len += frame.len() as u64;
        Ok(())
    }
}

/// What a replay produced, beside the records.
pub(crate) struct Replay {
    /// Records read, header excluded.
    pub(crate) records: Vec<CatalogRecord>,
    /// Offset of the first unused byte (torn tails are truncated to this).
    pub(crate) end: u64,
    /// Whether a torn tail was found at `end`.
    pub(crate) truncated: bool,
}

/// Reads the log, tolerating exactly one kind of corruption: a torn final
/// frame. A length prefix whose frame cannot fit in the remaining bytes is
/// the torn tail of a partial write; a fully-present frame whose CRC does
/// not match is real corruption and is fatal.
pub(crate) fn replay(path: &Path) -> Result<Replay, SegmentError> {
    let mut file = File::open(path).map_err(|e| SegmentError::io("open", path, e))?;
    let size = file
        .metadata()
        .map_err(|e| SegmentError::io("metadata", path, e))?
        .len();
    if size == 0 {
        return Ok(Replay {
            records: Vec::new(),
            end: 0,
            truncated: true,
        });
    }
    let mut data = Vec::with_capacity(size as usize);
    file.read_to_end(&mut data)
        .map_err(|e| SegmentError::io("read", path, e))?;

    let mut header_checked = false;
    let mut records = Vec::new();
    let mut pos = 0usize;
    while pos < data.len() {
        let frame = &data[pos..];
        if frame.len() < 4 {
            return torn(records, pos as u64);
        }
        let len = u32::from_le_bytes(frame[..4].try_into().unwrap_or([0; 4])) as usize;
        let frame_end = 4usize.checked_add(len).and_then(|e| e.checked_add(4));
        let complete = match frame_end {
            Some(end) => frame.len() >= end,
            None => false,
        };
        if !complete {
            // Not enough bytes for the declared frame: the writer died
            // mid-frame. Anything beyond is undefined and has no fsync
            // backing, so the log ends here.
            return torn(records, pos as u64);
        }
        let end = frame_end.unwrap_or(4 + len + 4);
        if len > MAX_FRAME {
            return Err(corrupt(format!(
                "frame length {len} exceeds the maximum at offset {pos}"
            )));
        }
        let payload = &frame[4..4 + len];
        let crc = u32::from_le_bytes(frame[4 + len..end].try_into().unwrap_or([0; 4]));
        if crc32c::crc32c(payload) != crc {
            return Err(corrupt(format!("crc mismatch at offset {pos}")));
        }
        if !header_checked {
            header_checked = true;
            if payload != HEADER.as_bytes() {
                return Err(SegmentError::catalog(format!(
                    "unrecognised catalog header in {}",
                    path.display()
                )));
            }
        } else {
            let record: CatalogRecord = serde_json::from_slice(payload).map_err(|e| {
                SegmentError::catalog(format!("record at offset {pos} does not parse: {e}"))
            })?;
            records.push(record);
        }
        pos += end;
    }
    Ok(Replay {
        records,
        end: pos as u64,
        truncated: false,
    })
}

fn torn(records: Vec<CatalogRecord>, end: u64) -> Result<Replay, SegmentError> {
    Ok(Replay {
        records,
        end,
        truncated: true,
    })
}

fn corrupt(message: String) -> SegmentError {
    SegmentError::catalog(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::record::*;

    fn eligible(id: &str, pos: u64) -> CatalogRecord {
        CatalogRecord::Eligible(EligibleRecord {
            id: id.to_owned(),
            pos,
        })
    }

    #[test]
    fn append_replay_round_trip_and_torn_tail_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.log");
        {
            let mut w = LogWriter::create(&path).unwrap();
            w.append(&[eligible("a", 1), eligible("b", 2)]).unwrap();
        }
        let rep = replay(&path).unwrap();
        assert_eq!(rep.records.len(), 2);
        assert!(!rep.truncated);

        // Reopen and append more.
        {
            let mut w = LogWriter::resume(&path, rep.end, false).unwrap();
            w.append(&[eligible("c", 3)]).unwrap();
        }
        let rep = replay(&path).unwrap();
        assert_eq!(rep.records.len(), 3);

        // A torn tail is truncated and the writer keeps appending.
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"\x0a\x00\x00__PARTIAL").unwrap();
        drop(f);
        let rep = replay(&path).unwrap();
        assert_eq!(rep.records.len(), 3);
        assert!(rep.truncated);
        {
            let mut w = LogWriter::resume(&path, rep.end, true).unwrap();
            w.append(&[eligible("d", 4)]).unwrap();
        }
        let rep = replay(&path).unwrap();
        assert_eq!(rep.records.len(), 4);
        assert!(!rep.truncated);
    }

    #[test]
    fn crc_mismatch_and_bad_header_are_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.log");
        {
            let mut w = LogWriter::create(&path).unwrap();
            w.append(&[eligible("a", 1)]).unwrap();
        }
        let size = std::fs::metadata(&path).unwrap().len();
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        f.seek(SeekFrom::Start(size - 8)).unwrap();
        f.write_all(b"X").unwrap();
        drop(f);
        assert!(replay(&path).is_err(), "bit flips must not pass the crc");

        let path2 = dir.path().join("other.log");
        {
            let mut f = File::create(&path2).unwrap();
            let payload = br#"{"kind":"wrong"}"#;
            f.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
            f.write_all(payload).unwrap();
            f.write_all(&crc32c::crc32c(payload).to_le_bytes()).unwrap();
        }
        assert!(replay(&path2).is_err());
    }

    #[test]
    fn a_zero_length_log_is_repaired_as_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog.log");
        File::create(&path).unwrap();
        let rep = replay(&path).unwrap();
        assert_eq!(rep.end, 0);
        assert!(rep.truncated);
        let mut w = LogWriter::resume(&path, rep.end, true).unwrap();
        w.append(&[eligible("a", 1)]).unwrap();
        let rep = replay(&path).unwrap();
        assert_eq!(rep.records.len(), 1);
    }
}
