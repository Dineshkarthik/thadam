//! kill -9 loop.
//!
//! The child opens the engine (running recovery), prints `OPEN <next_seq>`, then appends frames
//! whose payload encodes (run, seq). It prints `ACK <seq>` only after the commit covering `seq`
//! returned per the fsync mode, and `CUR <seq>` after a cursor commit returned. Every so often it
//! deletes consumed data. The parent SIGKILLs it at a random time, then reopens the engine in
//! process and checks:
//!   - every frame from first_seq to the end is present, contiguous, and byte-exact for the run
//!     that wrote that seq (so stale or resurrected frames from an earlier run are detected);
//!   - the last frame is >= the highest ACK ever printed (no acknowledged loss);
//!   - the cursor did not regress, is not past the end, and nothing past it was deleted.

use crate::engine::{self, Engine, FsyncMode, Opts, Result, disk_bytes, payload};
use serde_json::{Value, json};
use std::io::{BufRead, Write};
use std::path::Path;
use std::sync::mpsc;
use std::time::{Duration, Instant};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }
}

pub fn child(dir: &Path, o: &Opts, run: u32, seed: u64) -> Result<()> {
    let mut eng = engine::open(dir, o)?;
    let mut out = std::io::stdout().lock();
    writeln!(out, "OPEN {}", eng.next_seq())?;
    out.flush()?;
    let mut rng = Rng(seed | 1);
    let mut bufs: Vec<Vec<u8>> = (0..4).map(|_| Vec::new()).collect();
    let mut last_sync = Instant::now();
    let mut acked = eng.next_seq() - 1;
    let mut cur = eng.load_cursor("c")?.unwrap_or(0);
    let mut groups = 0u64;
    loop {
        let g = rng.range(1, 4) as usize;
        let base = eng.next_seq();
        for (j, b) in bufs.iter_mut().take(g).enumerate() {
            let rec = if rng.range(0, 3) == 0 { 4096 } else { 200 };
            payload::build(b, run, base + j as u64, rng.range(1, 32) as u32, rec);
        }
        let frames: Vec<&[u8]> = bufs[..g].iter().map(|b| b.as_slice()).collect();
        eng.append_group(&frames)?;
        eng.flush()?;
        let head = eng.next_seq() - 1;
        let durable = match o.fsync {
            FsyncMode::Always => {
                eng.sync()?;
                true
            }
            FsyncMode::Interval(d) if last_sync.elapsed() >= d => {
                eng.sync()?;
                last_sync = Instant::now();
                true
            }
            FsyncMode::Interval(_) => false,
            FsyncMode::Never => true,
        };
        if durable {
            acked = head;
            writeln!(out, "ACK {acked}")?;
            out.flush()?;
        }
        groups += 1;
        if groups % 8 == 0 {
            let c = acked.saturating_sub(rng.range(0, 300));
            if c > cur {
                eng.commit_cursor("c", c)?;
                writeln!(out, "CUR {c}")?;
                out.flush()?;
                cur = c;
                eng.delete_consumed(c)?;
            }
        }
    }
}

struct Run {
    id: u32,
    start: u64,
}

fn verify(eng: &mut dyn Engine, runs: &[Run], max_acked: u64, last_cur: u64) -> std::result::Result<(u64, u64), String> {
    let cur = eng.load_cursor("c").map_err(|e| e.to_string())?.unwrap_or(0);
    let next = eng.next_seq();
    let first = eng.first_seq().map_err(|e| e.to_string())?;
    if cur < last_cur {
        return Err(format!("cursor regressed: {cur} < acknowledged {last_cur}"));
    }
    if cur >= next {
        return Err(format!("cursor {cur} beyond end {next}"));
    }
    if first > cur + 1 {
        return Err(format!("deleted past cursor: first_seq {first} > cursor {cur} + 1"));
    }
    let mut after = first - 1;
    let mut err = None;
    let mut frames = 0u64;
    loop {
        let a = after;
        let n = eng
            .read(a, 4 << 20, &mut |seq, p| {
                if err.is_some() {
                    return;
                }
                if seq != after + 1 {
                    err = Some(format!("gap: expected {} got {seq}", after + 1));
                }
                let want = runs.iter().rev().find(|r| r.start <= seq).map(|r| r.id);
                match payload::verify(p) {
                    Some((run, s)) if s == seq && Some(run) == want => {}
                    Some((run, s)) => err = Some(format!("seq {seq}: payload is (run {run}, seq {s}), expected run {want:?}")),
                    None => err = Some(format!("seq {seq}: payload corrupt")),
                }
                after = seq;
                frames += 1;
            })
            .map_err(|e| format!("read error after {after}: {e}"))?;
        if let Some(e) = err.take() {
            return Err(e);
        }
        if n == 0 {
            break;
        }
    }
    if after + 1 != next {
        return Err(format!("read ended at {after} but next_seq is {next}"));
    }
    if after < max_acked {
        return Err(format!("acknowledged loss: last frame {after} < max acked {max_acked}"));
    }
    Ok((frames, after))
}

/// `after_kill` is a shell command run after each SIGKILL and before verification (used with
/// LazyFS to drop every write that was not fsynced, simulating power loss at the kill point).
pub fn parent(dir: &Path, o: &Opts, args: &[String], iters: u32, max_ms: u64, seed: u64, after_kill: Option<&str>) -> Result<Value> {
    crate::bench::fresh_dir(dir)?;
    let mut rng = Rng(seed | 1);
    let mut runs: Vec<Run> = Vec::new();
    let (mut max_acked, mut last_cur) = (0u64, 0u64);
    let mut failures: Vec<String> = Vec::new();
    let mut open_ms: Vec<f64> = Vec::new();
    let (mut total_acks, mut early_kills, mut max_frames, mut max_disk) = (0u64, 0u32, 0u64, 0u64);
    let mut verified_next = 1u64;
    for i in 0..iters {
        let mut child = std::process::Command::new(std::env::current_exe()?)
            .arg("crash-child")
            .args(args)
            .args(["--run", &i.to_string(), "--seed", &rng.next().to_string()])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel::<String>();
        let reader = std::thread::spawn(move || {
            for l in std::io::BufReader::new(stdout).lines() {
                match l {
                    Ok(l) => {
                        if tx.send(l).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        let delay = if rng.range(0, 9) == 0 { rng.range(0, 5) } else { rng.range(5, max_ms) };
        if delay < 5 {
            early_kills += 1;
        }
        std::thread::sleep(Duration::from_millis(delay));
        let _ = child.kill();
        let status = child.wait()?;
        reader.join().ok();
        let mut stderr = String::new();
        std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut stderr)?;
        for l in rx.try_iter() {
            let mut it = l.split_whitespace();
            let (Some(k), Some(v)) = (it.next(), it.next().and_then(|v| v.parse::<u64>().ok())) else { continue };
            match k {
                "OPEN" => {
                    if v != verified_next {
                        failures.push(format!("iter {i}: child opened at next_seq {v}, parent verified {verified_next}"));
                    }
                    runs.push(Run { id: i, start: v });
                }
                "ACK" => {
                    max_acked = max_acked.max(v);
                    total_acks += 1;
                }
                "CUR" => last_cur = last_cur.max(v),
                _ => {}
            }
        }
        if !stderr.trim().is_empty() {
            failures.push(format!("iter {i}: child stderr (status {status}): {}", stderr.trim()));
        }
        if let Some(cmd) = after_kill {
            let st = std::process::Command::new("sh").args(["-c", cmd]).status()?;
            if !st.success() {
                return Err(format!("after-kill command failed: {st}").into());
            }
        }
        let t = Instant::now();
        let mut eng = match engine::open(dir, o) {
            Ok(e) => e,
            Err(e) => {
                failures.push(format!("iter {i}: open failed: {e}"));
                break;
            }
        };
        open_ms.push(t.elapsed().as_secs_f64() * 1e3);
        match verify(eng.as_mut(), &runs, max_acked, last_cur) {
            Ok((frames, last)) => {
                max_frames = max_frames.max(frames);
                verified_next = last + 1;
            }
            Err(e) => {
                failures.push(format!("iter {i}: {e}"));
                if failures.len() >= 10 {
                    break;
                }
            }
        }
        drop(eng);
        max_disk = max_disk.max(disk_bytes(dir));
    }
    open_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |q: f64| open_ms.get(((open_ms.len() as f64 - 1.0) * q).round() as usize).copied().unwrap_or(0.0);
    Ok(json!({
        "test": "crash",
        "engine": format!("{:?}", o.kind).to_lowercase(),
        "fsync": o.fsync.to_string(),
        "sync_kind": o.sync_kind.to_string(),
        "prealloc": o.prealloc,
        "after_kill": after_kill,
        "iterations": open_ms.len(),
        "kills_under_5ms": early_kills,
        "acks_observed": total_acks,
        "max_acked_seq": max_acked,
        "max_retained_frames": max_frames,
        "max_disk_mib": max_disk as f64 / 1048576.0,
        "recovery_open_ms_p50": pct(0.5),
        "recovery_open_ms_p99": pct(0.99),
        "recovery_open_ms_max": pct(1.0),
        "failures": failures,
    }))
}
