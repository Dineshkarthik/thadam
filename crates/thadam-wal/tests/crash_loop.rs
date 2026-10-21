//! kill -9 crash loop (PR6 acceptance: zero acknowledged-ticket loss, no stale
//! frame resurrection).
//!
//! The parent re-executes this test binary as a child running `crash_child`.
//! The child opens the WAL (running recovery), prints `@@OPEN <next_seq>`, and
//! appends frames whose every byte derives from `(run, k)`. It prints
//! `@@ACK <seq>` only after the ticket resolved, and `@@CUR <seq>` after a
//! durable cursor commit (followed by a retention pass that deletes segments).
//! The parent SIGKILLs it after a random delay, reopens the WAL in-process,
//! and checks. Delays are anchored to child readiness, never to spawn time,
//! so load cannot turn every kill into a startup kill:
//!
//! - 10% of iterations kill within 5 ms of spawn (during startup and
//!   recovery) and need no signal;
//! - 80% wait for the child's first `@@ACK`, then sleep 5..250 ms;
//! - 10% wait for `@@OPEN` only, then sleep 5..250 ms (kill may land before
//!   the first ack).
//!
//! Waiting is bounded by `READY_TIMEOUT`. Checks:
//!
//! - every retained frame is contiguous and byte-exact for the run that wrote
//!   that position (stale or resurrected frames are detected);
//! - the last frame is at or above the highest position ever acknowledged;
//! - the cursor did not regress, is below the end, and nothing past it was
//!   deleted;
//! - the child's `OPEN` value equals the `next_seq` the parent verified.
//!
//! Iterations: 50 by default; set `THADAM_WAL_CRASH_ITERS=1000` for the full
//! acceptance run. Fsync modes rotate through `always`, `interval(5ms)`, and
//! `never` (all three must survive a process crash).

use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use thadam_core::{DiskBudget, RawBatch, RawMessage, SourceId, SystemClock, Timestamp, WalPos};
use thadam_testkit::SplitMix64;
use thadam_wal::{
    ConsumerSpec, CursorState, Frame, Fsync, OnFull, ReadItem, StartFrom, WalOptions,
};

const CHILD_DIR: &str = "THADAM_WAL_CRASH_CHILD_DIR";
const CHILD_RUN: &str = "THADAM_WAL_CRASH_CHILD_RUN";
const CHILD_SEED: &str = "THADAM_WAL_CRASH_CHILD_SEED";
const CHILD_FSYNC: &str = "THADAM_WAL_CRASH_CHILD_FSYNC";
const ITERS: &str = "THADAM_WAL_CRASH_ITERS";
const CURSOR: &str = "c";

fn fsync_mode(i: u64) -> Fsync {
    match i % 3 {
        0 => Fsync::Always,
        1 => Fsync::Interval(Duration::from_millis(5)),
        _ => Fsync::Never,
    }
}

fn opts(fsync: Fsync) -> WalOptions {
    let mut o = WalOptions::new(OnFull::Block);
    o.fsync = fsync;
    o.segment_bytes = 256 << 10;
    o.frame_max_bytes = 128 << 10;
    o.consumers = vec![ConsumerSpec::required(CURSOR)];
    o
}

fn open(
    dir: &Path,
    fsync: Fsync,
) -> Result<(thadam_wal::Wal, thadam_wal::RecoveryReport), thadam_wal::WalError> {
    thadam_wal::open(
        dir,
        opts(fsync),
        Arc::new(DiskBudget::new()),
        Arc::new(SystemClock::new()),
    )
}

fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

/// Records carry `(run, k, nrec, size)` in their first 24 bytes; every other
/// byte derives from them.
fn make_batch(run: u32, k: u64, nrec: u32, size: u32) -> RawBatch {
    let messages = (0..nrec)
        .map(|i| {
            let mut p = Vec::with_capacity(size as usize);
            p.extend_from_slice(&run.to_le_bytes());
            p.extend_from_slice(&k.to_le_bytes());
            p.extend_from_slice(&nrec.to_le_bytes());
            p.extend_from_slice(&size.to_le_bytes());
            p.extend_from_slice(&i.to_le_bytes());
            let mut s = mix((u64::from(run) << 40) ^ (k << 8) ^ u64::from(i));
            while p.len() < size as usize {
                s = mix(s);
                p.push(s as u8);
            }
            let mut m = RawMessage::new(Timestamp::from_nanos(k as i64), Bytes::from(p));
            m.attrs.insert("topic", format!("t/{}", k % 7));
            m
        })
        .collect();
    RawBatch::new(SourceId::new("crash"), messages, true)
}

fn identify(frame: &Frame) -> Option<(u32, u64)> {
    let b = frame.to_raw_batch();
    let p = &b.messages.first()?.payload;
    let word = |at: usize| -> Option<u32> {
        Some(u32::from_le_bytes(p.get(at..at + 4)?.try_into().ok()?))
    };
    let run = word(0)?;
    let k = u64::from_le_bytes(p.get(4..12)?.try_into().ok()?);
    let (nrec, size) = (word(12)?, word(16)?);
    (make_batch(run, k, nrec, size) == b).then_some((run, k))
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

/// The child half. A no-op unless the parent set `THADAM_WAL_CRASH_CHILD_DIR`.
#[test]
fn crash_child() {
    let Some(dir) = std::env::var_os(CHILD_DIR) else {
        return;
    };
    let run = u32::try_from(env_u64(CHILD_RUN).unwrap()).unwrap();
    let mut rng = SplitMix64::new(env_u64(CHILD_SEED).unwrap());
    let fsync = fsync_mode(env_u64(CHILD_FSYNC).unwrap());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (wal, report) = open(Path::new(&dir), fsync).unwrap();
        println!("@@OPEN {}", report.next_seq);
        let start = report.next_seq.get();
        let w = wal.start_writer().unwrap();
        let mut cursor = wal.cursor(CURSOR).map_or(0, |c| c.committed.get());
        let mut acked = start - 1;
        let mut k = 0u64;
        for group in 1u64.. {
            let mut tickets = Vec::new();
            for _ in 0..rng.between(1, 4) {
                let size = if rng.chance(25) { 4096 } else { 200 };
                let nrec = u32::try_from(rng.between(1, 16)).unwrap();
                tickets.push(w.submit(make_batch(run, k, nrec, size)).await.unwrap());
                k += 1;
            }
            for t in tickets {
                let seq = t.await.unwrap().first().get();
                assert_eq!(seq, acked + 1);
                acked = seq;
                println!("@@ACK {seq}");
            }
            if group % 8 == 0 {
                let c = acked.saturating_sub(rng.below(300));
                if c > cursor {
                    wal.commit_cursor(CURSOR, CursorState::at(WalPos::new(c)))
                        .await
                        .unwrap();
                    println!("@@CUR {c}");
                    cursor = c;
                    wal.maintain().await.unwrap();
                }
            }
        }
    });
}

/// Generous upper bound for a child to open the WAL and acknowledge a frame.
/// It only bounds a hung child; under load the kill waits for the signal
/// instead of racing a fixed wall-clock delay against process startup.
const READY_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, PartialEq, Eq)]
enum Wait {
    Seen,
    /// The child's stdout closed first (it exited); the stderr check reports why.
    Closed,
    TimedOut,
}

/// Collects child output into `lines` until one contains `needle`.
fn wait_for_line(
    rx: &mpsc::Receiver<String>,
    lines: &mut Vec<String>,
    deadline: Instant,
    needle: &str,
) -> Wait {
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(left) {
            Ok(line) => {
                let hit = line.contains(needle);
                lines.push(line);
                if hit {
                    return Wait::Seen;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => return Wait::TimedOut,
            Err(mpsc::RecvTimeoutError::Disconnected) => return Wait::Closed,
        }
    }
}

#[derive(Default)]
struct Tally {
    acks: u64,
    max_acked: u64,
    last_cursor: u64,
    early_kills: u64,
    max_frames: u64,
    recovery_ms: Vec<f64>,
}

#[test]
fn kill_dash_nine_crash_loop() {
    if std::env::var_os(CHILD_DIR).is_some() {
        return;
    }
    let iters = env_u64(ITERS).unwrap_or(50);
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("wal");
    let exe = std::env::current_exe().unwrap();
    let mut rng = SplitMix64::new(0x00c0_ffee);
    // (run, first position the run wrote)
    let mut runs: Vec<(u32, u64)> = Vec::new();
    let mut verified_next = 1u64;
    let mut t = Tally::default();
    let mut failures: Vec<String> = Vec::new();

    for i in 0..iters {
        let mut child = Command::new(&exe)
            .args([
                "--exact",
                "crash_child",
                "--nocapture",
                "--test-threads",
                "1",
            ])
            .env(CHILD_DIR, &dir)
            .env(CHILD_RUN, i.to_string())
            .env(CHILD_SEED, rng.next_u64().to_string())
            .env(CHILD_FSYNC, i.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel::<String>();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        let early = rng.chance(10);
        let delay = if early {
            t.early_kills += 1;
            rng.below(5)
        } else {
            rng.between(5, 250)
        };
        let gate_on_ack = !early && !rng.chance(20);
        let mut lines: Vec<String> = Vec::new();
        if early {
            std::thread::sleep(Duration::from_millis(delay));
        } else {
            let deadline = Instant::now() + READY_TIMEOUT;
            let mut ready = wait_for_line(&rx, &mut lines, deadline, "@@OPEN");
            if ready == Wait::Seen && gate_on_ack {
                ready = wait_for_line(&rx, &mut lines, deadline, "@@ACK");
            }
            if ready == Wait::TimedOut {
                failures.push(format!(
                    "iter {i}: child not ready within {READY_TIMEOUT:?} \
                     (gate_on_ack: {gate_on_ack})"
                ));
            }
            std::thread::sleep(Duration::from_millis(delay));
        }
        let _ = child.kill();
        let status = child.wait().unwrap();
        reader.join().unwrap();
        lines.extend(rx.try_iter());
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();

        for line in &lines {
            let Some(at) = line.find("@@") else { continue };
            let mut it = line[at + 2..].split_whitespace();
            let (Some(kind), Some(v)) = (it.next(), it.next().and_then(|v| v.parse::<u64>().ok()))
            else {
                continue;
            };
            match kind {
                "OPEN" => {
                    if v != verified_next {
                        failures.push(format!(
                            "iter {i}: child opened at {v}, parent verified {verified_next}"
                        ));
                    }
                    runs.push((u32::try_from(i).unwrap(), v));
                }
                "ACK" => {
                    t.acks += 1;
                    t.max_acked = t.max_acked.max(v);
                }
                "CUR" => t.last_cursor = t.last_cursor.max(v),
                _ => {}
            }
        }
        if stderr.contains("panicked") {
            failures.push(format!(
                "iter {i}: child failed ({status}): {}",
                stderr.trim()
            ));
        }

        let started = Instant::now();
        let (wal, report) = match open(&dir, fsync_mode(i)) {
            Ok(x) => x,
            Err(e) => {
                failures.push(format!("iter {i}: open failed: {e}"));
                break;
            }
        };
        t.recovery_ms.push(started.elapsed().as_secs_f64() * 1e3);
        match verify(&wal, &report, &runs, &t) {
            Ok((frames, next)) => {
                t.max_frames = t.max_frames.max(frames);
                verified_next = next;
            }
            Err(e) => failures.push(format!("iter {i}: {e}")),
        }
        drop(wal);
        if failures.len() >= 10 {
            break;
        }
    }

    t.recovery_ms.sort_by(f64::total_cmp);
    let pct = |q: f64| {
        let n = t.recovery_ms.len();
        t.recovery_ms
            .get(((n.max(1) - 1) as f64 * q).round() as usize)
            .copied()
            .unwrap_or(0.0)
    };
    eprintln!(
        "crash loop: {} iterations, {} kills under 5 ms, {} acks (max acked {}), max retained frames {}, \
         recovery p50 {:.1} ms / max {:.1} ms, {} failures",
        t.recovery_ms.len(),
        t.early_kills,
        t.acks,
        t.max_acked,
        t.max_frames,
        pct(0.5),
        pct(1.0),
        failures.len()
    );
    assert!(failures.is_empty(), "{failures:#?}");
    assert!(t.acks > 0, "no child ever acknowledged a frame");
}

fn verify(
    wal: &thadam_wal::Wal,
    report: &thadam_wal::RecoveryReport,
    runs: &[(u32, u64)],
    t: &Tally,
) -> Result<(u64, u64), String> {
    let cursor = wal.cursor(CURSOR).map_or(0, |c| c.committed.get());
    let next = report.next_seq.get();
    let first = report.first_seq.get();
    if cursor < t.last_cursor {
        return Err(format!(
            "cursor regressed: {cursor} < acknowledged {}",
            t.last_cursor
        ));
    }
    if cursor >= next {
        return Err(format!("cursor {cursor} at or beyond the end {next}"));
    }
    if first > cursor + 1 {
        return Err(format!(
            "deleted past the cursor: first {first} > cursor {cursor} + 1"
        ));
    }
    let mut r = wal
        .reader("verify", StartFrom::At(WalPos::new(first)))
        .map_err(|e| e.to_string())?;
    let mut expect = first;
    let mut frames = 0;
    while let Some(item) = r
        .try_next(4 << 20)
        .map_err(|e| format!("read at {expect}: {e}"))?
    {
        let chunk = match item {
            ReadItem::Frames(c) => c,
            ReadItem::Gap(g) => return Err(format!("gap at {expect}: {g:?}")),
        };
        for f in chunk.frames() {
            let seq = f.seq().get();
            if seq != expect {
                return Err(format!("expected position {expect}, got {seq}"));
            }
            let want = runs
                .iter()
                .rev()
                .find(|(_, start)| *start <= seq)
                .map(|&(run, start)| (run, seq - start));
            let got = identify(f);
            if got != want {
                return Err(format!(
                    "position {seq}: frame is {got:?}, expected {want:?} (stale or corrupt)"
                ));
            }
            expect += 1;
            frames += 1;
        }
    }
    if expect != next {
        return Err(format!(
            "read ended at {} but next_seq is {next}",
            expect - 1
        ));
    }
    if expect - 1 < t.max_acked {
        return Err(format!(
            "acknowledged loss: last frame {} < max acked {}",
            expect - 1,
            t.max_acked
        ));
    }
    Ok((frames, next))
}
