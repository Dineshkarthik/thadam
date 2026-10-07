//! Exhaustive torn-tail / corruption tests for the custom log (deterministic, no kill needed).
//!
//! Builds a segment with N frames, then for each case damages a copy of it, reopens, checks the
//! surviving frames byte-for-byte, appends new frames from a different "run", reopens again, and
//! checks that nothing from the damaged region ever comes back.

use crate::bench::fresh_dir;
use crate::engine::{self, FsyncMode, Kind, Opts, Result, SyncKind, payload};
use crate::seglog::{FRAME_HDR, SEG_HDR};
use serde_json::{Value, json};
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::Path;

const N: u64 = 24;

fn opts(prealloc: bool) -> Opts {
    Opts {
        kind: Kind::Seglog,
        fsync: FsyncMode::Always,
        sync_kind: SyncKind::Data,
        segment_bytes: 64 << 10,
        prealloc,
        redb_quick_repair: false,
        redb_cache_mib: None,
    }
}

fn frame_payload(run: u32, seq: u64) -> Vec<u8> {
    let mut p = Vec::new();
    payload::build(&mut p, run, seq, 1 + (seq % 5) as u32, 100);
    p
}

fn append(o: &Opts, dir: &Path, run: u32, count: u64) -> Result<()> {
    let mut e = engine::open(dir, o)?;
    for _ in 0..count {
        let p = frame_payload(run, e.next_seq());
        e.append_group(&[&p])?;
        e.flush()?;
        e.sync()?;
    }
    Ok(())
}

/// Returns (offset, len) of each frame in segment 1.
fn build_base(o: &Opts, dir: &Path) -> Result<Vec<(u64, u64)>> {
    fresh_dir(dir)?;
    append(o, dir, 0, N)?;
    let mut off = SEG_HDR;
    Ok((1..=N)
        .map(|seq| {
            let len = (FRAME_HDR + frame_payload(0, seq).len()) as u64;
            let r = (off, len);
            off += len;
            r
        })
        .collect())
}

fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for e in std::fs::read_dir(src)? {
        let e = e?;
        let to = dst.join(e.file_name());
        if e.file_type()?.is_dir() {
            copy_dir(&e.path(), &to)?;
        } else {
            std::fs::copy(e.path(), to)?;
        }
    }
    Ok(())
}

/// `expect[i]` is the run that must have written seq i+1.
fn check(o: &Opts, dir: &Path, expect: &[u32]) -> std::result::Result<(), String> {
    let mut e = engine::open(dir, o).map_err(|e| format!("open: {e}"))?;
    if e.next_seq() != expect.len() as u64 + 1 {
        return Err(format!("next_seq {} expected {}", e.next_seq(), expect.len() + 1));
    }
    let mut got = Vec::new();
    let mut after = 0;
    loop {
        let a = after;
        let n = e
            .read(a, 1 << 20, &mut |seq, p| {
                got.push((seq, payload::verify(p)));
                after = seq;
            })
            .map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            break;
        }
    }
    let want: Vec<_> = expect.iter().enumerate().map(|(i, &r)| (i as u64 + 1, Some((r, i as u64 + 1)))).collect();
    if got != want {
        return Err(format!("frames differ: got {} frames, first mismatch {:?}", got.len(),
            got.iter().zip(&want).find(|(a, b)| a != b)));
    }
    Ok(())
}

fn case(o: &Opts, base: &Path, scratch: &Path, damage: impl FnOnce(&Path) -> Result<()>, survivors: u64, add: u64) -> std::result::Result<(), String> {
    let _ = std::fs::remove_dir_all(scratch);
    copy_dir(base, scratch).map_err(|e| e.to_string())?;
    damage(&scratch.join("wal").join(format!("{:020}.seg", 1))).map_err(|e| e.to_string())?;
    let mut expect = vec![0u32; survivors as usize];
    check(o, scratch, &expect)?;
    append(o, scratch, 1, add).map_err(|e| e.to_string())?;
    expect.extend(std::iter::repeat_n(1, add as usize));
    check(o, scratch, &expect)?;
    check(o, scratch, &expect)
}

pub fn run(dir: &Path) -> Result<Value> {
    let mut out = serde_json::Map::new();
    for prealloc in [false, true] {
        out.insert(format!("prealloc_{prealloc}"), run_one(dir, &opts(prealloc))?);
    }
    let _ = std::fs::remove_dir_all(dir);
    Ok(json!({ "test": "torn", "engine": "seglog", "frames_in_segment": N, "results": out }))
}

fn run_one(dir: &Path, o: &Opts) -> Result<Value> {
    let base = dir.join("base");
    let scratch = dir.join("scratch");
    let frames = build_base(o, &base)?;
    let (last_off, last_len) = frames[N as usize - 1];
    let mut results = serde_json::Map::new();
    let mut tally = |name: &str, outcomes: Vec<std::result::Result<(), String>>| {
        let fails: Vec<String> = outcomes.iter().filter_map(|r| r.clone().err()).take(3).collect();
        let failed = outcomes.iter().filter(|r| r.is_err()).count();
        results.insert(name.into(), json!({ "cases": outcomes.len(), "failed": failed, "examples": fails }));
    };

    let truncations = (last_off..last_off + last_len)
        .map(|cut| case(o, &base, &scratch, |seg| Ok(OpenOptions::new().write(true).open(seg)?.set_len(cut)?), N - 1, 2))
        .collect();
    tally("truncate_last_frame_at_every_byte", truncations);

    let flips = (last_off..last_off + last_len)
        .map(|b| {
            case(o, &base, &scratch, |seg| {
                let f = OpenOptions::new().read(true).write(true).open(seg)?;
                let mut x = [0u8; 1];
                f.read_exact_at(&mut x, b)?;
                x[0] ^= 1 << (b % 8);
                Ok(f.write_all_at(&x, b)?)
            }, N - 1, 2)
        })
        .collect();
    tally("flip_one_bit_at_every_byte_of_last_frame", flips);

    // Simulates out-of-order writeback loss: a middle frame is zeroed, later frames survive intact.
    // Appending exactly one frame (same size as the lost one, since size depends only on seq) is
    // the case where appending in place after recovery would make the old frames k+1.. valid again.
    let mut mid = Vec::new();
    for k in [2u64, N / 2, N - 1] {
        let (off, len) = frames[k as usize - 1];
        for add in [1, N - k + 3] {
            mid.push(case(o, &base, &scratch, |seg| Ok(OpenOptions::new().write(true).open(seg)?.write_all_at(&vec![0u8; len as usize], off)?), k - 1, add));
        }
    }
    tally("zeroed_middle_frame_later_frames_never_resurrect", mid);

    let garbage = vec![case(o, &base, &scratch, |seg| {
        let f = OpenOptions::new().write(true).open(seg)?;
        let g: Vec<u8> = (0..1000u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8 | 1).collect();
        Ok(f.write_all_at(&g, last_off + last_len)?)
    }, N, 2)];
    tally("garbage_after_last_frame", garbage);

    results.insert("last_frame_bytes".into(), json!(last_len));
    Ok(Value::Object(results))
}
