//! Helpers shared by the in-crate tests.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use thadam_core::{BudgetCategory, DiskBudget, RawBatch, RawMessage, SourceId, Timestamp};
use thadam_testkit::FakeClock;

use crate::{
    Frame, Fsync, Gap, OnFull, ReadItem, RecoveryReport, Wal, WalError, WalOptions, WalReader,
};

/// Small segments so tests roll and delete constantly.
pub(crate) fn small_opts(on_full: OnFull) -> WalOptions {
    let mut o = WalOptions::new(on_full);
    o.fsync = Fsync::Always;
    o.segment_bytes = 16 << 10;
    o.frame_max_bytes = 4 << 10;
    o.group_max_bytes = 64 << 10;
    o
}

pub(crate) struct Env {
    pub dir: tempfile::TempDir,
    pub budget: Arc<DiskBudget>,
    pub clock: FakeClock,
}

impl Env {
    pub(crate) fn new() -> Self {
        Env {
            dir: tempfile::tempdir().unwrap(),
            budget: Arc::new(DiskBudget::new()),
            clock: FakeClock::default(),
        }
    }

    pub(crate) fn with_limit(limit: u64) -> Self {
        let env = Env::new();
        env.budget.set_limit(BudgetCategory::Wal, Some(limit));
        env
    }

    pub(crate) fn wal_dir(&self) -> PathBuf {
        self.dir.path().join("wal")
    }

    pub(crate) fn try_open(&self, o: WalOptions) -> Result<(Wal, RecoveryReport), WalError> {
        crate::open(
            &self.wal_dir(),
            o,
            Arc::clone(&self.budget),
            self.clock.shared(),
        )
    }

    pub(crate) fn open(&self, o: WalOptions) -> (Wal, RecoveryReport) {
        self.try_open(o).unwrap()
    }

    pub(crate) fn segment_files(&self) -> Vec<PathBuf> {
        let mut v: Vec<PathBuf> = std::fs::read_dir(self.wal_dir().join("segments"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        v.sort();
        v
    }

    /// Bytes of segment files on disk (file lengths).
    pub(crate) fn segment_bytes_on_disk(&self) -> u64 {
        self.segment_files()
            .iter()
            .map(|p| std::fs::metadata(p).map_or(0, |m| m.len()))
            .sum()
    }
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

/// A batch whose every byte is derived from `(run, k)`, so a stale or
/// resurrected frame from another run is detected byte for byte.
pub(crate) fn batch(run: u32, k: u64, nrec: usize, rec_size: usize) -> RawBatch {
    let messages = (0..nrec)
        .map(|i| {
            let mut p = Vec::with_capacity(rec_size.max(16));
            p.extend_from_slice(&run.to_le_bytes());
            p.extend_from_slice(&k.to_le_bytes());
            p.extend_from_slice(&(i as u32).to_le_bytes());
            let mut s = mix((u64::from(run) << 40) ^ (k << 8) ^ i as u64);
            while p.len() < rec_size.max(16) {
                s = mix(s);
                p.push(s as u8);
            }
            let mut m = RawMessage::new(
                Timestamp::from_nanos(i64::try_from(k).unwrap_or(0) * 1000 + i as i64),
                Bytes::from(p),
            );
            m.attrs.insert("topic", format!("robots/r{}/t", k % 3));
            m
        })
        .collect();
    RawBatch::new(
        SourceId::new(format!("src-{}", k % 2)),
        messages,
        k.is_multiple_of(2),
    )
}

/// `(run, k)` if the frame is byte-exact for them.
pub(crate) fn identify(frame: &Frame) -> Option<(u32, u64)> {
    let b = frame.to_raw_batch();
    let first = b.messages.first()?.payload.clone();
    if first.len() < 16 {
        return None;
    }
    let run = u32::from_le_bytes(first[0..4].try_into().ok()?);
    let k = u64::from_le_bytes(first[4..12].try_into().ok()?);
    let expected = batch(run, k, b.messages.len(), first.len());
    (expected == b).then_some((run, k))
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Item {
    Frame(u64, Option<(u32, u64)>),
    Gap(Gap),
}

/// Everything the reader can return now.
pub(crate) fn drain(reader: &mut WalReader, max_bytes: usize) -> Vec<Item> {
    let mut out = Vec::new();
    while let Some(item) = reader.try_next(max_bytes).unwrap() {
        match item {
            ReadItem::Frames(chunk) => {
                for f in chunk.frames() {
                    out.push(Item::Frame(f.seq().get(), identify(f)));
                }
            }
            ReadItem::Gap(g) => out.push(Item::Gap(g)),
        }
    }
    out
}

/// Submits `batches` and awaits every ticket, returning the positions.
pub(crate) async fn append_all(
    w: &crate::WalWriter,
    batches: impl IntoIterator<Item = RawBatch>,
) -> Vec<Result<u64, WalError>> {
    let mut tickets = Vec::new();
    for b in batches {
        tickets.push(w.submit(b).await);
    }
    let mut out = Vec::new();
    for t in tickets {
        out.push(match t {
            Ok(t) => t.await.map(|r| r.first().get()),
            Err(e) => Err(e),
        });
    }
    out
}

pub(crate) fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        let to = dst.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &to);
        } else {
            std::fs::copy(e.path(), to).unwrap();
        }
    }
}

pub(crate) fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
