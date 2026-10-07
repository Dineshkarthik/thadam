//! Deterministic torn-tail, corruption, and stale-frame cases (S1 `torn`,
//! rerun on the production code).

use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use thadam_core::WalPos;

use super::util::*;
use crate::format::{FRAME_HDR, SEG_HDR, Step, parse_frame};
use crate::{Fsync, OnFull, StartFrom, WalError, WalOptions};

const N: u64 = 24;

fn opts() -> WalOptions {
    let mut o = small_opts(OnFull::Block);
    // Recovery logic is under test, not flushing; keeps the many cases fast.
    o.fsync = Fsync::Never;
    o.segment_bytes = 64 << 10;
    o
}

fn frame_batch(run: u32, k: u64) -> thadam_core::RawBatch {
    batch(run, k, 1 + (k % 3) as usize, 24)
}

fn append(env: &Env, run: u32, count: u64) {
    let (wal, report) = env.open(opts());
    let first = report.next_seq.get();
    let w = wal.start_writer().unwrap();
    let rt = rt();
    for k in first..first + count {
        let r = rt.block_on(append_all(&w, [frame_batch(run, k)]));
        assert_eq!(r[0].as_ref().unwrap(), &k);
    }
    rt.block_on(wal.close()).unwrap();
}

/// (offset, length) of every frame in the first segment.
fn frame_offsets(seg: &Path) -> Vec<(u64, u64)> {
    let bytes = std::fs::read(seg).unwrap();
    let mut off = SEG_HDR as usize;
    let mut expect = 1;
    let mut out = Vec::new();
    while let Step::Frame { len, .. } = parse_frame(&bytes[off..], expect, 1 << 20) {
        out.push((off as u64, (FRAME_HDR + len) as u64));
        off += FRAME_HDR + len;
        expect += 1;
    }
    out
}

struct Base {
    env: Env,
    seg: PathBuf,
    frames: Vec<(u64, u64)>,
}

fn build_base() -> Base {
    let env = Env::new();
    append(&env, 0, N);
    let seg = env.segment_files()[0].clone();
    let frames = frame_offsets(&seg);
    assert_eq!(frames.len() as u64, N);
    Base { env, seg, frames }
}

/// `expect[i]` is the run that must have written position `i + 1`.
fn check(env: &Env, expect: &[u32]) -> Result<(), String> {
    let (wal, report) = env.try_open(opts()).map_err(|e| format!("open: {e}"))?;
    if report.next_seq.get() != expect.len() as u64 + 1 {
        return Err(format!(
            "next_seq {} expected {}",
            report.next_seq,
            expect.len() + 1
        ));
    }
    let mut r = wal
        .reader("v", StartFrom::Earliest)
        .map_err(|e| e.to_string())?;
    let got = drain(&mut r, 1 << 20);
    let want: Vec<Item> = expect
        .iter()
        .enumerate()
        .map(|(i, &run)| {
            let k = i as u64 + 1;
            Item::Frame(k, Some((run, k)))
        })
        .collect();
    if got != want {
        return Err(format!(
            "frames differ: got {} items, first mismatch {:?}",
            got.len(),
            got.iter().zip(&want).find(|(a, b)| a != b)
        ));
    }
    Ok(())
}

/// Damages a copy of the base, then: recovery keeps exactly `survivors`
/// frames; `add` new frames from run 1 follow them; two more reopens agree.
fn case(base: &Base, damage: impl FnOnce(&Path), survivors: u64, add: u64) -> Result<(), String> {
    let env = Env::new();
    copy_dir(&base.env.wal_dir(), &env.wal_dir());
    let seg = env
        .wal_dir()
        .join("segments")
        .join(base.seg.file_name().unwrap());
    damage(&seg);
    let mut expect = vec![0u32; survivors as usize];
    check(&env, &expect)?;
    append(&env, 1, add);
    expect.extend(std::iter::repeat_n(1, add as usize));
    check(&env, &expect)?;
    check(&env, &expect)
}

fn assert_all_pass(name: &str, results: Vec<Result<(), String>>) {
    let failures: Vec<&String> = results.iter().filter_map(|r| r.as_ref().err()).collect();
    assert!(
        failures.is_empty(),
        "{name}: {} of {} cases failed, e.g. {:?}",
        failures.len(),
        results.len(),
        failures.iter().take(3).collect::<Vec<_>>()
    );
}

#[test]
fn truncating_the_last_frame_at_every_byte_offset() {
    let base = build_base();
    let (off, len) = base.frames[N as usize - 1];
    let results = (off..off + len)
        .map(|cut| {
            case(
                &base,
                |seg| {
                    OpenOptions::new()
                        .write(true)
                        .open(seg)
                        .unwrap()
                        .set_len(cut)
                        .unwrap()
                },
                N - 1,
                2,
            )
        })
        .collect();
    assert_all_pass("truncate", results);
}

#[test]
fn flipping_one_bit_at_every_byte_of_the_last_frame() {
    let base = build_base();
    let (off, len) = base.frames[N as usize - 1];
    let results = (off..off + len)
        .map(|b| {
            case(
                &base,
                |seg| {
                    let f = OpenOptions::new().read(true).write(true).open(seg).unwrap();
                    let mut x = [0u8; 1];
                    f.read_exact_at(&mut x, b).unwrap();
                    x[0] ^= 1 << (b % 8);
                    f.write_all_at(&x, b).unwrap();
                },
                N - 1,
                2,
            )
        })
        .collect();
    assert_all_pass("bit flip", results);
}

/// Out-of-order writeback loss: a middle frame is zeroed while later frames
/// survive. Appending exactly one frame is the case where appending in place
/// would make the stale frames valid again (S1's mutant).
#[test]
fn stale_frames_after_a_zeroed_middle_frame_never_resurrect() {
    let base = build_base();
    let mut results = Vec::new();
    for k in [2u64, N / 2, N - 1] {
        let (off, len) = base.frames[k as usize - 1];
        for add in [1, N - k + 3] {
            results.push(case(
                &base,
                |seg| {
                    let f = OpenOptions::new().write(true).open(seg).unwrap();
                    f.write_all_at(&vec![0u8; len as usize], off).unwrap();
                },
                k - 1,
                add,
            ));
        }
    }
    assert_all_pass("zeroed middle frame", results);
}

#[test]
fn garbage_after_the_last_frame_is_truncated() {
    let base = build_base();
    let (off, len) = base.frames[N as usize - 1];
    let r = case(
        &base,
        |seg| {
            let f = OpenOptions::new().write(true).open(seg).unwrap();
            let g: Vec<u8> = (0..1000u32)
                .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8 | 1)
                .collect();
            f.write_all_at(&g, off + len).unwrap();
        },
        N,
        2,
    );
    assert_all_pass("garbage tail", vec![r]);
}

#[test]
fn a_zero_filled_tail_is_a_clean_end() {
    let base = build_base();
    let (off, len) = base.frames[N as usize - 1];
    let env = Env::new();
    copy_dir(&base.env.wal_dir(), &env.wal_dir());
    let seg = env.segment_files()[0].clone();
    OpenOptions::new()
        .write(true)
        .open(&seg)
        .unwrap()
        .set_len(off + len + 4096)
        .unwrap();
    let (_wal, report) = env.open(opts());
    assert!(report.zero_tail && !report.torn_tail, "{report:?}");
    assert_eq!(report.truncated_bytes, 4096);
    assert_eq!(report.next_seq, WalPos::new(N + 1));
}

#[test]
fn torn_tail_is_reported() {
    let base = build_base();
    let (off, _) = base.frames[N as usize - 1];
    let env = Env::new();
    copy_dir(&base.env.wal_dir(), &env.wal_dir());
    let seg = env.segment_files()[0].clone();
    OpenOptions::new()
        .write(true)
        .open(&seg)
        .unwrap()
        .set_len(off + 5)
        .unwrap();
    let (_wal, report) = env.open(opts());
    assert!(report.torn_tail);
    assert_eq!(report.truncated_bytes, 5);
}

fn two_segments() -> Env {
    let env = Env::new();
    append(&env, 0, 3);
    append(&env, 0, 3);
    assert_eq!(env.segment_files().len(), 2);
    env
}

fn corrupt_header(path: &Path) {
    let f = OpenOptions::new().write(true).open(path).unwrap();
    f.write_all_at(b"X", 3).unwrap();
}

#[test]
fn a_bad_header_on_a_sealed_segment_fails_the_open() {
    let env = two_segments();
    corrupt_header(&env.segment_files()[0]);
    assert!(matches!(env.try_open(opts()), Err(WalError::Corrupt(_))));
}

#[test]
fn a_bad_header_on_a_last_segment_with_data_fails_the_open() {
    let env = two_segments();
    corrupt_header(&env.segment_files()[1]);
    assert!(matches!(env.try_open(opts()), Err(WalError::Corrupt(_))));
}

#[test]
fn an_empty_last_segment_with_a_bad_header_is_removed_without_rewinding() {
    let env = two_segments();
    // Fake an empty last segment at base 9 whose header is unreadable. Its
    // base still bounds the next position.
    let path = env
        .wal_dir()
        .join("segments")
        .join(crate::format::segment_file_name(9));
    std::fs::write(&path, [0xffu8; 40]).unwrap();
    let (wal, report) = env.open(opts());
    assert!(report.removed_empty_last_segment);
    assert_eq!(report.next_seq, WalPos::new(9));
    assert!(!path.exists());
    let w = wal.start_writer().unwrap();
    let rt = rt();
    assert_eq!(
        rt.block_on(append_all(&w, [frame_batch(1, 9)]))[0]
            .as_ref()
            .unwrap(),
        &9
    );
    let mut r = wal.reader("v", StartFrom::At(WalPos::new(6))).unwrap();
    let epoch = wal.epoch();
    assert_eq!(
        drain(&mut r, 1 << 20),
        vec![
            Item::Frame(6, Some((0, 6))),
            Item::Gap(crate::Gap::Missing {
                range: thadam_core::WalRange::new(epoch, WalPos::new(7), WalPos::new(8)).unwrap()
            }),
            Item::Frame(9, Some((1, 9))),
        ]
    );
}

#[test]
fn an_empty_last_segment_is_reused_without_allocating() {
    let base = build_base();
    let env = Env::new();
    copy_dir(&base.env.wal_dir(), &env.wal_dir());
    // Tear the first frame: the only segment is left with just its header.
    let seg = env.segment_files()[0].clone();
    OpenOptions::new()
        .write(true)
        .open(&seg)
        .unwrap()
        .set_len(SEG_HDR + 3)
        .unwrap();
    append(&env, 1, 2);
    assert_eq!(env.segment_files(), vec![seg]);
    check(&env, &[1, 1]).unwrap();
}

#[test]
fn a_sealed_segment_with_a_corrupt_frame_yields_a_corrupt_gap() {
    let env = two_segments();
    let first = env.segment_files()[0].clone();
    let frames = frame_offsets(&first);
    let (off, _) = frames[1];
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&first)
        .unwrap();
    f.write_all_at(&[0xEE], off + FRAME_HDR as u64).unwrap();
    let (wal, _) = env.open(opts());
    let mut r = wal.reader("v", StartFrom::Earliest).unwrap();
    let items = drain(&mut r, 1 << 20);
    let epoch = wal.epoch();
    assert_eq!(items[0], Item::Frame(1, Some((0, 1))));
    assert_eq!(
        items[1],
        Item::Gap(crate::Gap::Corrupt {
            range: thadam_core::WalRange::new(epoch, WalPos::new(2), WalPos::new(3)).unwrap()
        })
    );
    assert_eq!(items[2..].len(), 3, "the next segment is read normally");
    assert_eq!(wal.stats().corrupt_gaps, 1);
}
