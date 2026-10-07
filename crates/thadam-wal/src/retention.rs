//! Retention, disk-budget reclaim, and `on_full: drop_oldest` eviction.
//!
//! Only whole segments are deleted, oldest first, and never the newest one (it
//! carries the sequence high-water mark across restarts). Deletion is an
//! `unlink` plus one directory flush, which needs no free space. Time-based
//! decisions use the monotonic clock only.

use std::collections::HashMap;
use std::time::Instant;

use thadam_core::{BudgetCategory, WalPos};

use crate::cursor::CursorState;
use crate::error::WalError;
use crate::fsutil::{remove_if_exists, sync_dir};
use crate::options::{MaxLag, OnFull};
use crate::shared::{Counters, Index, SegMeta, Shared, lock};

/// What one retention pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RetentionReport {
    /// Segments deleted after every required consumer had passed them.
    pub deleted_segments: usize,
    /// Their bytes.
    pub deleted_bytes: u64,
    /// Segments evicted under `drop_oldest` before every required consumer
    /// had read them (an explicit, counted loss).
    pub evicted_segments: usize,
    /// Their bytes.
    pub evicted_bytes: u64,
    /// Required consumers currently past their `max_lag`, which no longer pin
    /// retention.
    pub lagging_consumers: Vec<String>,
    /// The oldest retained position after the pass.
    pub first_retained: WalPos,
    /// Bytes held by retained segments after the pass.
    pub retained_bytes: u64,
}

#[derive(Clone, Copy)]
enum Mode {
    /// Consumed segments past `retain_for`.
    Normal,
    /// Free at least this many bytes: consumed segments regardless of
    /// `retain_for`, then (under `drop_oldest`) unconsumed ones.
    Pressure(u64),
}

/// A periodic pass: normal retention, then keep one segment of headroom under
/// the budget so appends rarely have to reclaim inline.
pub(crate) fn maintain(sh: &Shared) -> Result<RetentionReport, WalError> {
    let mut report = RetentionReport::default();
    run(sh, Mode::Normal, &mut report)?;
    if let Some(limit) = sh.budget.limit(BudgetCategory::Wal) {
        let target = limit.saturating_sub(sh.opts.segment_bytes);
        let used = sh.budget.used(BudgetCategory::Wal);
        if used > target {
            run(sh, Mode::Pressure(used - target), &mut report)?;
        }
    }
    {
        let idx = lock(&sh.index);
        report.first_retained = WalPos::new(idx.segs.front().map_or(
            sh.next_seq.load(std::sync::atomic::Ordering::Relaxed),
            |s| s.base,
        ));
        report.retained_bytes = idx.segs.iter().map(|s| s.bytes).sum();
    }
    let mut lagging: Vec<String> = lock(&sh.lagging).iter().cloned().collect();
    lagging.sort();
    report.lagging_consumers = lagging;
    Ok(report)
}

/// Inline reclaim by the writer when a reservation of `need` bytes failed.
/// Returns the bytes freed.
pub(crate) fn reclaim_for(sh: &Shared, need: u64) -> Result<u64, WalError> {
    let mut report = RetentionReport::default();
    run(sh, Mode::Pressure(need), &mut report)?;
    Ok(report.deleted_bytes + report.evicted_bytes)
}

fn run(sh: &Shared, mode: Mode, report: &mut RetentionReport) -> Result<(), WalError> {
    let cursors = lock(&sh.cursors).clone();
    let now = sh.now();
    let chosen = {
        let mut idx = lock(&sh.index);
        let floor = pin_floor(sh, &idx, &cursors, now);
        select(sh, &mut idx, floor, mode, now)
    };
    if chosen.is_empty() {
        return Ok(());
    }
    delete(sh, chosen, report)
}

/// The highest position every pinning consumer has passed: segments whose
/// frames all lie at or below it are consumed. `u64::MAX` when nothing pins.
fn pin_floor(
    sh: &Shared,
    idx: &Index,
    cursors: &HashMap<String, CursorState>,
    now: Instant,
) -> u64 {
    let mut floor = u64::MAX;
    let mut lagging = lock(&sh.lagging);
    for c in sh.opts.consumers.iter().filter(|c| c.required) {
        // A required consumer that has never committed pins everything.
        let pos = cursors.get(&c.name).map_or(0, CursorState::retention_floor);
        let over = c.max_lag.is_some_and(|lag| exceeds(idx, pos, lag, now));
        if over {
            if lagging.insert(c.name.clone()) {
                Counters::add(&sh.counters.lag_evictions, 1);
            }
            continue;
        }
        lagging.remove(&c.name);
        floor = floor.min(pos);
    }
    floor
}

fn exceeds(idx: &Index, pos: u64, lag: MaxLag, now: Instant) -> bool {
    let next = pos.saturating_add(1);
    let from = idx.segment_for(next).unwrap_or(0);
    match lag {
        MaxLag::Bytes(max) => idx.segs.iter().skip(from).map(|s| s.bytes).sum::<u64>() > max,
        MaxLag::Age(max) => idx
            .segs
            .get(from)
            .is_some_and(|s| now.saturating_duration_since(s.created_at) > max),
    }
}

/// Removes the chosen segments from the index; the caller unlinks them.
fn select(
    sh: &Shared,
    idx: &mut Index,
    floor: u64,
    mode: Mode,
    now: Instant,
) -> Vec<(SegMeta, bool)> {
    let mut out = Vec::new();
    let mut freed = 0u64;
    while idx.segs.len() > 1 {
        let last_seq = idx.segs[1].base.saturating_sub(1);
        let consumed = last_seq <= floor;
        let seg = &idx.segs[0];
        let take = match mode {
            Mode::Normal => {
                let retain = sh.opts.retain_for;
                consumed
                    && (retain.is_zero()
                        || seg
                            .sealed_at
                            .is_some_and(|t| now.saturating_duration_since(t) >= retain))
            }
            Mode::Pressure(need) => {
                freed < need && (consumed || sh.opts.on_full == OnFull::DropOldest)
            }
        };
        if !take {
            break;
        }
        if let Some(seg) = idx.segs.pop_front() {
            freed += seg.bytes;
            out.push((seg, !consumed));
        }
    }
    out
}

fn delete(
    sh: &Shared,
    chosen: Vec<(SegMeta, bool)>,
    report: &mut RetentionReport,
) -> Result<(), WalError> {
    let mut iter = chosen.into_iter();
    let mut failure = None;
    for (seg, evicted) in iter.by_ref() {
        if let Err(e) = remove_if_exists(&seg.path) {
            failure = Some((WalError::io("delete segment", &seg.path, e), seg, evicted));
            break;
        }
        sh.release(seg.bytes);
        let c = &sh.counters;
        if evicted {
            report.evicted_segments += 1;
            report.evicted_bytes += seg.bytes;
            Counters::add(&c.evicted_segments, 1);
            Counters::add(&c.evicted_bytes, seg.bytes);
        } else {
            report.deleted_segments += 1;
            report.deleted_bytes += seg.bytes;
            Counters::add(&c.deleted_segments, 1);
            Counters::add(&c.deleted_bytes, seg.bytes);
        }
    }
    if let Some((err, seg, evicted)) = failure {
        // Put back what was not deleted, in order, so the index matches disk.
        let mut idx = lock(&sh.index);
        let rest: Vec<SegMeta> = std::iter::once((seg, evicted))
            .chain(iter)
            .map(|(s, _)| s)
            .collect();
        for s in rest.into_iter().rev() {
            idx.segs.push_front(s);
        }
        return Err(err);
    }
    sync_dir(&sh.seg_dir).map_err(|e| WalError::io("fsync", &sh.seg_dir, e))?;
    sh.notify_space();
    Ok(())
}
