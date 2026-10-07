//! Durable consumer cursors: `cursors/<name>.cur`.
//!
//! Layout, 44 bytes, little-endian:
//!
//! ```text
//! [0, 4)   magic "RCUR"
//! [4, 6)   cursor format version u16
//! [6, 8)   flags u16 (bit 0: in-flight range present)
//! [8, 16)  epoch u64
//! [16, 24) committed position u64
//! [24, 32) in-flight first u64 (zero when absent)
//! [32, 40) in-flight last u64 (zero when absent)
//! [40, 44) crc32c of [0, 40)
//! ```

use std::path::Path;

use thadam_core::{WalEpoch, WalPos, WalRange};

use crate::error::WalError;

const MAGIC: &[u8; 4] = b"RCUR";
pub(crate) const CURSOR_FORMAT_VERSION: u16 = 1;
const LEN: usize = 44;
const FLAG_INFLIGHT: u16 = 1;

/// A consumer's durable progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CursorState {
    /// Every frame at or below this position has been processed.
    pub committed: WalPos,
    /// A range handed to a sink whose outcome is not yet known; it is re-sent
    /// with the same batch id after a crash and pins retention.
    pub inflight: Option<WalRange>,
}

impl CursorState {
    /// A cursor at `committed` with nothing in flight.
    pub fn at(committed: WalPos) -> Self {
        CursorState {
            committed,
            inflight: None,
        }
    }

    /// The lowest position this cursor still needs: frames at or below the
    /// returned value may be deleted as far as this cursor is concerned.
    pub(crate) fn retention_floor(&self) -> u64 {
        let c = self.committed.get();
        match self.inflight {
            Some(r) => c.min(r.first().get().saturating_sub(1)),
            None => c,
        }
    }

    /// The highest position the cursor refers to.
    pub(crate) fn high_water(&self) -> u64 {
        let c = self.committed.get();
        self.inflight.map_or(c, |r| c.max(r.last().get()))
    }
}

/// Cursor names become file names, so they are restricted.
pub(crate) fn validate_name(name: &str) -> Result<(), WalError> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'));
    if ok {
        Ok(())
    } else {
        Err(WalError::invalid(format!(
            "cursor name {name:?} must be 1 to 128 characters of [A-Za-z0-9_.-] and not start with '.'"
        )))
    }
}

pub(crate) fn encode(epoch: WalEpoch, state: &CursorState) -> [u8; LEN] {
    let mut b = [0u8; LEN];
    b[0..4].copy_from_slice(MAGIC);
    b[4..6].copy_from_slice(&CURSOR_FORMAT_VERSION.to_le_bytes());
    let flags = if state.inflight.is_some() {
        FLAG_INFLIGHT
    } else {
        0
    };
    b[6..8].copy_from_slice(&flags.to_le_bytes());
    b[8..16].copy_from_slice(&epoch.get().to_le_bytes());
    b[16..24].copy_from_slice(&state.committed.get().to_le_bytes());
    if let Some(r) = state.inflight {
        b[24..32].copy_from_slice(&r.first().get().to_le_bytes());
        b[32..40].copy_from_slice(&r.last().get().to_le_bytes());
    }
    let crc = crc32c::crc32c(&b[..40]);
    b[40..44].copy_from_slice(&crc.to_le_bytes());
    b
}

pub(crate) fn decode(path: &Path, epoch: WalEpoch, b: &[u8]) -> Result<CursorState, WalError> {
    let corrupt = |why: &str| WalError::Corrupt(format!("cursor {}: {why}", path.display()));
    if b.len() != LEN || &b[0..4] != MAGIC {
        return Err(corrupt("not a cursor file"));
    }
    if crc32c::crc32c(&b[..40]) != u32_at(b, 40) {
        return Err(corrupt("checksum mismatch"));
    }
    let version = u16::from_le_bytes([b[4], b[5]]);
    if version != CURSOR_FORMAT_VERSION {
        return Err(WalError::Incompatible(format!(
            "cursor {}: format version {version} is not supported (this binary supports {CURSOR_FORMAT_VERSION})",
            path.display()
        )));
    }
    let flags = u16::from_le_bytes([b[6], b[7]]);
    if flags & !FLAG_INFLIGHT != 0 {
        return Err(corrupt("unknown flags"));
    }
    let file_epoch = u64_at(b, 8);
    if file_epoch != epoch.get() {
        return Err(WalError::Incompatible(format!(
            "cursor {} belongs to epoch {:x}, the wal is epoch {epoch}",
            path.display(),
            file_epoch
        )));
    }
    let inflight = if flags & FLAG_INFLIGHT != 0 {
        let r = WalRange::new(
            epoch,
            WalPos::new(u64_at(b, 24)),
            WalPos::new(u64_at(b, 32)),
        )
        .map_err(|_| corrupt("inverted in-flight range"))?;
        Some(r)
    } else {
        None
    };
    Ok(CursorState {
        committed: WalPos::new(u64_at(b, 16)),
        inflight,
    })
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&b[at..at + 4]);
    u32::from_le_bytes(a)
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(a)
}

#[cfg(test)]
mod tests {
    use super::*;

    const E: WalEpoch = WalEpoch::new(0xabc);

    #[test]
    fn round_trip_with_and_without_inflight() {
        let p = Path::new("c.cur");
        let s = CursorState::at(WalPos::new(41));
        assert_eq!(decode(p, E, &encode(E, &s)).unwrap(), s);
        let r = WalRange::new(E, WalPos::new(42), WalPos::new(50)).unwrap();
        let s = CursorState {
            committed: WalPos::new(41),
            inflight: Some(r),
        };
        assert_eq!(decode(p, E, &encode(E, &s)).unwrap(), s);
        assert_eq!(s.retention_floor(), 41);
        assert_eq!(s.high_water(), 50);
    }

    #[test]
    fn corruption_epoch_and_version_are_refused() {
        let p = Path::new("c.cur");
        let b = encode(E, &CursorState::at(WalPos::new(1)));
        for i in 0..b.len() {
            let mut bad = b;
            bad[i] ^= 0x10;
            assert!(decode(p, E, &bad).is_err(), "byte {i}");
        }
        assert!(matches!(
            decode(p, WalEpoch::new(1), &b),
            Err(WalError::Incompatible(_))
        ));
        let mut newer = b;
        newer[4..6].copy_from_slice(&2u16.to_le_bytes());
        let crc = crc32c::crc32c(&newer[..40]);
        newer[40..44].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode(p, E, &newer),
            Err(WalError::Incompatible(_))
        ));
    }

    #[test]
    fn names_are_restricted() {
        for ok in ["local", "s3-sink", "a.b_c-1"] {
            assert!(validate_name(ok).is_ok(), "{ok}");
        }
        for bad in ["", ".hidden", "a/b", "../x", "with space", &"x".repeat(129)] {
            assert!(validate_name(bad).is_err(), "{bad}");
        }
    }
}
