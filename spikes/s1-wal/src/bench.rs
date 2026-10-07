use crate::engine::{self, Engine, FsyncMode, Opts, Result, disk_bytes, payload};
use crate::metrics::{maxrss_mib, os_bytes_written};
use hdrhistogram::Histogram;
use serde_json::{Value, json};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

const MIB: f64 = 1024.0 * 1024.0;

pub fn fresh_dir(dir: &Path) -> Result<()> {
    if dir.exists() {
        std::fs::remove_dir_all(dir)?;
    }
    std::fs::create_dir_all(dir)?;
    Ok(())
}

fn template(rec_size: usize, batch: u32) -> Vec<u8> {
    let mut v = Vec::new();
    payload::build(&mut v, 0, 0, batch, rec_size);
    v
}

fn base_json(o: &Opts, rec_size: usize, batch: u32) -> Value {
    json!({
        "engine": format!("{:?}", o.kind).to_lowercase(),
        "fsync": o.fsync.to_string(),
        "sync_kind": o.sync_kind.to_string(),
        "prealloc": o.prealloc,
        "redb_quick_repair": o.redb_quick_repair,
        "redb_cache_mib": o.redb_cache_mib,
        "rec_size": rec_size,
        "batch": batch,
    })
}

fn merge(mut a: Value, b: Value) -> Value {
    for (k, v) in b.as_object().unwrap() {
        a[k] = v.clone();
    }
    a
}

pub struct BenchCfg {
    pub rec_size: usize,
    pub batch: u32,
    pub producers: usize,
    pub window: usize,
    pub secs: f64,
    pub warmup: f64,
    pub rate_mbps: Option<f64>,
    pub group_max: usize,
}

struct Sub {
    t0: Instant,
    producer: usize,
    data: Arc<[u8]>,
}

/// Producers submit frames (one frame = one source batch of `batch` records) to a single
/// writer thread that group-commits, mirroring the WalWriter design. Each producer keeps at most
/// `window` uncommitted frames (like an MQTT inflight window). With `rate_mbps` producers are
/// paced open-loop and latency is measured from the scheduled send time.
pub fn bench(dir: &Path, o: &Opts, c: &BenchCfg) -> Result<Value> {
    fresh_dir(dir)?;
    let mut eng = engine::open(dir, o)?;
    let frame: Arc<[u8]> = template(c.rec_size, c.batch).into();
    let rec_bytes = (c.batch as usize * c.rec_size) as f64;
    let (tx, rx) = mpsc::sync_channel::<Sub>(8192);
    let stop = Arc::new(AtomicBool::new(false));
    let mut ack_txs = Vec::new();
    let mut handles = Vec::new();
    let interval = c
        .rate_mbps
        .map(|r| Duration::from_secs_f64(rec_bytes * c.producers as f64 / (r * 1e6)));
    for p in 0..c.producers {
        let (atx, arx) = mpsc::channel::<u32>();
        ack_txs.push(atx);
        let (tx, stop, frame, window) = (tx.clone(), stop.clone(), frame.clone(), c.window);
        handles.push(std::thread::spawn(move || {
            let start = Instant::now();
            let mut outstanding = 0usize;
            let mut k = 0u32;
            while !stop.load(Ordering::Relaxed) {
                let t0 = match interval {
                    Some(iv) => {
                        let due = start + iv * k;
                        let now = Instant::now();
                        if due > now {
                            std::thread::sleep(due - now);
                        }
                        due
                    }
                    None => Instant::now(),
                };
                while let Ok(n) = arx.try_recv() {
                    outstanding -= n as usize;
                }
                while outstanding >= window {
                    match arx.recv() {
                        Ok(n) => outstanding -= n as usize,
                        Err(_) => return,
                    }
                }
                let t0 = if interval.is_some() { t0 } else { Instant::now() };
                if tx.send(Sub { t0, producer: p, data: frame.clone() }).is_err() {
                    return;
                }
                outstanding += 1;
                k += 1;
            }
        }));
    }
    drop(tx);

    let t_start = Instant::now();
    let t_warm = t_start + Duration::from_secs_f64(c.warmup);
    let t_end = t_warm + Duration::from_secs_f64(c.secs);
    {
        let stop = stop.clone();
        std::thread::spawn(move || {
            std::thread::sleep(t_end - Instant::now());
            stop.store(true, Ordering::Relaxed);
        });
    }
    let w0 = os_bytes_written();
    let mut hist = Histogram::<u64>::new_with_bounds(1, 120_000_000, 3)?;
    let mut pending: Vec<(Instant, usize)> = Vec::new();
    let mut group: Vec<Sub> = Vec::new();
    let mut last_sync = Instant::now();
    let (mut syncs, mut groups, mut frames_in_groups, mut win_frames, mut total_frames) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut counts = vec![0u32; c.producers];
    let mut finished = false;
    loop {
        let timeout = match o.fsync {
            FsyncMode::Interval(d) if !pending.is_empty() => Some((last_sync + d).saturating_duration_since(Instant::now())),
            _ => None,
        };
        let first = match timeout {
            Some(t) => match rx.recv_timeout(t) {
                Ok(s) => Some(s),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    finished = true;
                    None
                }
            },
            None => match rx.recv() {
                Ok(s) => Some(s),
                Err(_) => {
                    finished = true;
                    None
                }
            },
        };
        if let Some(s) = first {
            let mut bytes = s.data.len();
            group.push(s);
            while bytes < c.group_max {
                match rx.try_recv() {
                    Ok(s) => {
                        bytes += s.data.len();
                        group.push(s);
                    }
                    Err(_) => break,
                }
            }
        }
        if !group.is_empty() {
            let frames: Vec<&[u8]> = group.iter().map(|s| &s.data[..]).collect();
            eng.append_group(&frames)?;
            eng.flush()?;
            groups += 1;
            frames_in_groups += frames.len() as u64;
            total_frames += frames.len() as u64;
            pending.extend(group.drain(..).map(|s| (s.t0, s.producer)));
        }
        let resolve = match o.fsync {
            FsyncMode::Always => {
                if !pending.is_empty() {
                    eng.sync()?;
                    syncs += 1;
                }
                true
            }
            FsyncMode::Never => true,
            FsyncMode::Interval(d) => {
                let now = Instant::now();
                if !pending.is_empty() && (now >= last_sync + d || finished) {
                    eng.sync()?;
                    syncs += 1;
                    last_sync = now;
                    true
                } else {
                    false
                }
            }
        };
        if resolve && !pending.is_empty() {
            let now = Instant::now();
            let in_window = now >= t_warm && now <= t_end;
            for (t0, p) in pending.drain(..) {
                if in_window {
                    hist.record((now - t0).as_micros().max(1) as u64)?;
                    win_frames += 1;
                }
                counts[p] += 1;
            }
            for (p, n) in counts.iter_mut().enumerate() {
                if *n > 0 {
                    let _ = ack_txs[p].send(*n);
                    *n = 0;
                }
            }
        }
        if finished {
            eng.sync()?;
            break;
        }
    }
    for h in handles {
        let _ = h.join();
    }
    let w1 = os_bytes_written();
    let disk = disk_bytes(dir);
    let payload_total = total_frames as f64 * rec_bytes;
    Ok(merge(
        base_json(o, c.rec_size, c.batch),
        json!({
            "test": "append",
            "producers": c.producers,
            "window": c.window,
            "rate_mbps_offered": c.rate_mbps,
            "secs": c.secs,
            "mb_per_s": win_frames as f64 * rec_bytes / c.secs / 1e6,
            "records_per_s": (win_frames as f64 * c.batch as f64 / c.secs).round(),
            "frames_per_s": (win_frames as f64 / c.secs).round(),
            "lat_p50_us": hist.value_at_quantile(0.50),
            "lat_p99_us": hist.value_at_quantile(0.99),
            "lat_p999_us": hist.value_at_quantile(0.999),
            "lat_max_us": hist.max(),
            "syncs_per_s": (syncs as f64 / (c.secs + c.warmup)).round(),
            "frames_per_group": frames_in_groups as f64 / groups.max(1) as f64,
            "disk_mib": disk as f64 / MIB,
            "payload_mib": payload_total / MIB,
            "os_write_amp": match (w0, w1) { (Some(a), Some(b)) if payload_total > 0.0 => json!((b - a) as f64 / payload_total), _ => Value::Null },
            "maxrss_mib": maxrss_mib(),
        }),
    ))
}

fn write_backlog(eng: &mut dyn Engine, frame: &[u8], target_bytes: u64, group: usize) -> Result<u64> {
    let frames: Vec<&[u8]> = vec![frame; group];
    let mut written = 0u64;
    let mut n = 0;
    while written < target_bytes {
        eng.append_group(&frames)?;
        eng.flush()?;
        written += (frame.len() * group) as u64;
        n += group as u64;
    }
    eng.sync()?;
    Ok(n)
}

/// Writes `mb` MiB, reopens (dropping engine caches; the OS page cache stays warm), then reads
/// everything from the start in 1 MiB chunks.
pub fn read_bench(dir: &Path, o: &Opts, rec_size: usize, batch: u32, mb: u64) -> Result<Value> {
    fresh_dir(dir)?;
    let frame = template(rec_size, batch);
    let written = {
        let mut eng = engine::open(dir, o)?;
        write_backlog(eng.as_mut(), &frame, mb << 20, 64)?
    };
    let mut eng = engine::open(dir, o)?;
    let first = eng.first_seq()?;
    let t = Instant::now();
    let mut after = first - 1;
    let (mut frames, mut bytes, mut bad) = (0u64, 0u64, 0u64);
    loop {
        let a = after;
        let n = eng.read(a, 1 << 20, &mut |seq, p| {
            if seq != after + 1 || p.get(4..12).map(|s| u64::from_le_bytes(s.try_into().unwrap())) != Some(0) {
                bad += 1;
            }
            after = seq;
            frames += 1;
            bytes += p.len() as u64;
        })?;
        if n == 0 {
            break;
        }
    }
    let dt = t.elapsed().as_secs_f64();
    if frames != written || bad != 0 {
        return Err(format!("read {frames} of {written} frames, {bad} bad").into());
    }
    let rec_bytes = frames as f64 * batch as f64 * rec_size as f64;
    Ok(merge(
        base_json(o, rec_size, batch),
        json!({
            "test": "read",
            "backlog_mib": bytes as f64 / MIB,
            "secs": dt,
            "mb_per_s": rec_bytes / dt / 1e6,
            "records_per_s": (frames as f64 * batch as f64 / dt).round(),
            "maxrss_mib": maxrss_mib(),
        }),
    ))
}

/// Single-threaded churn: append continuously while a consumer trails `backlog_mb` behind,
/// committing its cursor and deleting consumed data every 16 groups. Reports disk usage over time,
/// then consumes everything and reports reclaim.
pub fn churn(dir: &Path, o: &Opts, rec_size: usize, batch: u32, total_mb: u64, backlog_mb: u64) -> Result<Value> {
    fresh_dir(dir)?;
    let mut eng = engine::open(dir, o)?;
    let frame = template(rec_size, batch);
    let group = 16usize;
    let frames: Vec<&[u8]> = vec![&frame; group];
    let lag = (backlog_mb << 20) / frame.len() as u64;
    let w0 = os_bytes_written();
    let t = Instant::now();
    let (mut written, mut cur, mut gi) = (0u64, 0u64, 0u64);
    let (mut max_disk, mut sum_disk, mut samples) = (0u64, 0f64, 0u64);
    let mut del_time = Duration::ZERO;
    let mut last_sync = Instant::now();
    while written < total_mb << 20 {
        eng.append_group(&frames)?;
        eng.flush()?;
        match o.fsync {
            FsyncMode::Always => eng.sync()?,
            FsyncMode::Interval(d) if last_sync.elapsed() >= d => {
                eng.sync()?;
                last_sync = Instant::now();
            }
            _ => {}
        }
        written += (frame.len() * group) as u64;
        gi += 1;
        let head = eng.next_seq() - 1;
        if gi % 16 == 0 && head > lag && head - lag > cur {
            cur = head - lag;
            eng.sync()?;
            eng.commit_cursor("c", cur)?;
            let td = Instant::now();
            eng.delete_consumed(cur)?;
            del_time += td.elapsed();
        }
        if gi % 64 == 0 && written > 2 * (backlog_mb << 20) {
            let d = disk_bytes(dir);
            max_disk = max_disk.max(d);
            sum_disk += d as f64;
            samples += 1;
        }
    }
    eng.sync()?;
    let dt = t.elapsed().as_secs_f64();
    let w1 = os_bytes_written();
    let before = disk_bytes(dir);
    let head = eng.next_seq() - 1;
    eng.commit_cursor("c", head)?;
    let td = Instant::now();
    eng.delete_consumed(head)?;
    let reclaim_ms = td.elapsed().as_secs_f64() * 1e3;
    let after = disk_bytes(dir);
    let rec_total = (written / frame.len() as u64) as f64 * batch as f64 * rec_size as f64;
    Ok(merge(
        base_json(o, rec_size, batch),
        json!({
            "test": "churn",
            "segment_mib": o.segment_bytes as f64 / MIB,
            "total_mib": total_mb,
            "backlog_target_mib": backlog_mb,
            "mb_per_s": rec_total / dt / 1e6,
            "steady_disk_max_mib": max_disk as f64 / MIB,
            "steady_disk_avg_mib": sum_disk / samples.max(1) as f64 / MIB,
            "delete_ms_total": del_time.as_secs_f64() * 1e3,
            "disk_before_full_consume_mib": before as f64 / MIB,
            "disk_after_full_consume_mib": after as f64 / MIB,
            "full_consume_reclaim_ms": reclaim_ms,
            "os_write_amp": match (w0, w1) { (Some(a), Some(b)) => json!((b - a) as f64 / rec_total), _ => Value::Null },
            "maxrss_mib": maxrss_mib(),
        }),
    ))
}

/// Child for `recovery`: writes `mb` MiB, prints READY, then keeps appending until killed.
pub fn fill(dir: &Path, o: &Opts, rec_size: usize, batch: u32, mb: u64) -> Result<()> {
    use std::io::Write;
    fresh_dir(dir)?;
    let mut eng = engine::open(dir, o)?;
    let frame = template(rec_size, batch);
    write_backlog(eng.as_mut(), &frame, mb << 20, 64)?;
    let mut out = std::io::stdout().lock();
    writeln!(out, "READY")?;
    out.flush()?;
    let frames: Vec<&[u8]> = vec![&frame; 16];
    loop {
        eng.append_group(&frames)?;
        eng.flush()?;
        if o.fsync != FsyncMode::Never {
            eng.sync()?;
        }
    }
}

/// Builds a backlog in a child process, kill -9s it mid-append, then times reopening.
pub fn recovery(dir: &Path, o: &Opts, args: &[String], rec_size: usize, batch: u32, mb: u64, drop_caches: bool) -> Result<Value> {
    use std::io::BufRead;
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .arg("fill")
        .args(args)
        .args(["--rec-size", &rec_size.to_string(), "--batch", &batch.to_string(), "--mb", &mb.to_string()])
        .stdout(std::process::Stdio::piped())
        .spawn()?;
    let mut line = String::new();
    std::io::BufReader::new(child.stdout.take().unwrap()).read_line(&mut line)?;
    if line.trim() != "READY" {
        let _ = child.kill();
        return Err(format!("fill child failed: {line:?}").into());
    }
    std::thread::sleep(Duration::from_millis(100));
    child.kill()?;
    child.wait()?;
    let dropped = drop_caches && std::fs::write("/proc/sys/vm/drop_caches", "3").is_ok();
    let disk = disk_bytes(dir);
    let t = Instant::now();
    let mut eng = engine::open(dir, o)?;
    let first = eng.first_seq()?;
    let open_ms = t.elapsed().as_secs_f64() * 1e3;
    let frames = eng.next_seq() - first;
    let info = eng.info();
    Ok(merge(
        base_json(o, rec_size, batch),
        json!({
            "test": "recovery",
            "backlog_mib": disk as f64 / MIB,
            "frames": frames,
            "open_ms": open_ms,
            "caches_dropped": dropped,
            "info": info,
            "maxrss_mib": maxrss_mib(),
        }),
    ))
}
