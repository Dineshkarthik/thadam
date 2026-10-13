//! `window`: tumbling windows on ingest time, one aggregate event per asset
//! and window.
//!
//! A window `[k*size, (k+1)*size)` of an asset closes when the asset's next
//! event falls in a later window; that event also opens the next window. With
//! `idle_close`, another asset's event whose ingest time is at least
//! `idle_close` past a pane's last ingest time also closes that pane (in
//! `(first_pos, asset)` order when several close at once). There are no
//! wall-clock timers, so without traffic from another asset a silent asset's
//! last window stays open until it speaks again. If ingest time steps
//! backwards an event joins the asset's open window.
//!
//! The step keeps open windows in memory only. [`Step::hold`] reports the first
//! WAL frame of the oldest one, and the runtime never commits a cursor past it,
//! so a restart re-reads those frames and rebuilds the same windows. An
//! aggregate takes the WAL position of the event that closed it and an id
//! derived from content alone, so a re-read aggregate is trimmed or
//! deduplicated exactly like any other event.
//!
//! Output columns: `window_start` and `window_end` (`ts`), then one column per
//! aggregate. `count` is a `u64` count of non-null values; `sum`, `min`, `max`
//! and `mean` are `f64`, null when the window saw no value. A null or `NaN`
//! value is skipped.

use std::collections::{BTreeMap, HashMap, HashSet};

use thadam_core::{
    AssetPath, Event, FieldDef, FieldIdx, FieldType, Payload, Quality, QualityFlags, QualityStatus,
    Record, ThadamError, Shape, ShapeRef, TimeDomain, Times, Timestamp, Value, WalPos,
};

use super::fieldops::step_failed;
use super::{Compiled, Step, StepCtx, StepResult, config_error, intern};
use crate::eventid::IdBuf;
use crate::spec::{AggFn, WindowSpec, WindowTime};

const START: &str = "window_start";
const END: &str = "window_end";

#[derive(Debug)]
struct Agg {
    field: FieldIdx,
    func: AggFn,
}

type OrderKey = (i64, u64, u32);

#[derive(Clone, Copy, Debug)]
struct Acc {
    n: u64,
    sum: f64,
    sum_sq: f64,
    mean: f64,
    m2: f64,
    min: f64,
    max: f64,
    first_key: OrderKey,
    first_val: f64,
    last_key: OrderKey,
    last_val: f64,
}

impl Acc {
    const EMPTY: Acc = Acc {
        n: 0,
        sum: 0.0,
        sum_sq: 0.0,
        mean: 0.0,
        m2: 0.0,
        min: f64::INFINITY,
        max: f64::NEG_INFINITY,
        first_key: (i64::MAX, u64::MAX, u32::MAX),
        first_val: 0.0,
        last_key: (i64::MIN, u64::MIN, u32::MIN),
        last_val: 0.0,
    };

    fn add(&mut self, key: OrderKey, x: f64) {
        self.n += 1;
        self.sum += x;
        self.sum_sq += x * x;
        let delta = x - self.mean;
        self.mean += delta / self.n as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
        self.min = self.min.min(x);
        self.max = self.max.max(x);
        if self.n == 1 || key < self.first_key {
            self.first_key = key;
            self.first_val = x;
        }
        if self.n == 1 || key >= self.last_key {
            self.last_key = key;
            self.last_val = x;
        }
    }
}

#[derive(Debug)]
struct Pane {
    index: i64,
    first_pos: WalPos,
    last_ingest_ns: i64,
    has_gap: bool,
    range_start: bool,
    accs: Vec<Acc>,
}

#[derive(Debug)]
struct AssetState {
    panes: BTreeMap<i64, Pane>,
    last_ingest_ns: i64,
    max_event_ns: Option<i64>,
    watermark_ns: Option<i64>,
    opened_any: bool,
}

// shortcut: tumbling windows only (`idle_close` advances only when another
// asset speaks, and late events are dropped rather than re-emitting); upgrade
// to sliding windows (design note S4) when overlapping windows are needed.
#[derive(Debug)]
struct Window {
    size_ns: i64,
    time: WindowTime,
    lateness_ns: i64,
    idle_ns: Option<i64>,
    aggs: Vec<Agg>,
    out: ShapeRef,
    ids: IdBuf,
    cap: usize,
    range_start: bool,
    assets: HashMap<AssetPath, AssetState>,
    /// First positions of the open panes, with how many panes share each.
    open: BTreeMap<u64, usize>,
}

fn numeric(ty: &FieldType) -> bool {
    matches!(
        ty,
        FieldType::I8
            | FieldType::I16
            | FieldType::I32
            | FieldType::U8
            | FieldType::U16
            | FieldType::U32
            | FieldType::F32
            | FieldType::F64
    )
}

pub(super) fn compile(
    spec: &WindowSpec,
    ctx: &mut StepCtx<'_>,
    input: &ShapeRef,
    path: &str,
) -> Result<Compiled, Vec<ThadamError>> {
    let mut errs = Vec::new();
    let size_ns = match i64::try_from(spec.size.0.as_nanos()) {
        Ok(n) if n > 0 => n,
        _ => {
            errs.push(config_error(
                &format!("{path}.size"),
                "`size` must be positive and fit in 63 bits of nanoseconds",
            ));
            1
        }
    };
    let lateness_ns = match spec.allowed_lateness {
        None => 0,
        Some(_) if spec.time != WindowTime::Event => {
            errs.push(config_error(
                &format!("{path}.allowed_lateness"),
                "`allowed_lateness` applies only to `time: event` windows",
            ));
            0
        }
        Some(d) => match i64::try_from(d.0.as_nanos()) {
            Ok(n) if n >= 0 => n,
            _ => {
                errs.push(config_error(
                    &format!("{path}.allowed_lateness"),
                    "`allowed_lateness` must fit in 63 bits of nanoseconds",
                ));
                0
            }
        },
    };
    let idle_ns = match spec.idle_close {
        None => None,
        Some(d) => match i64::try_from(d.0.as_nanos()) {
            Ok(n) if n > 0 => Some(n),
            _ => {
                errs.push(config_error(
                    &format!("{path}.idle_close"),
                    "`idle_close` must be positive and fit in 63 bits of nanoseconds",
                ));
                None
            }
        },
    };
    if spec.aggregates.is_empty() {
        errs.push(config_error(
            &format!("{path}.aggregates"),
            "`aggregates` needs at least one entry",
        ));
    }
    let mut defs = vec![
        FieldDef::new(START, FieldType::Ts),
        FieldDef::new(END, FieldType::Ts),
    ];
    let mut names: HashSet<&str> = HashSet::from([START, END]);
    let mut aggs = Vec::new();
    for (i, a) in spec.aggregates.iter().enumerate() {
        let apath = format!("{path}.aggregates[{i}]");
        if a.name.is_empty() || !names.insert(&a.name) {
            errs.push(config_error(
                &format!("{apath}.as"),
                format!(
                    "output column `{}` is empty, repeated, or reserved (`{START}`, `{END}`)",
                    a.name
                ),
            ));
        }
        let Some(idx) = input.index_of(&a.field) else {
            errs.push(config_error(
                &format!("{apath}.field"),
                format!("unknown field `{}`", a.field),
            ));
            continue;
        };
        let Some(def) = input.field(idx) else {
            continue;
        };
        let count = a.func == AggFn::Count;
        if !count && !numeric(&def.ty) {
            errs.push(config_error(
                &format!("{apath}.field"),
                format!(
                    "`{}` is {:?}; sum, min, max and mean need an i8, i16, i32, u8, u16, u32, f32 or f64 field (i64 and u64 do not fit f64 exactly)",
                    a.field, def.ty
                ),
            ));
            continue;
        }
        let mut col = FieldDef::new(
            a.name.as_str(),
            if count {
                FieldType::U64
            } else {
                FieldType::F64
            },
        );
        if !count && let Some(unit) = &def.unit {
            col = col.with_unit(unit.clone());
        }
        defs.push(col);
        aggs.push(Agg {
            field: idx,
            func: a.func,
        });
    }
    if !errs.is_empty() {
        return Err(errs);
    }
    let out = intern(ctx, Shape::new(defs), path)?;
    let columns: Vec<&str> = out.fields().iter().map(|f| &*f.name).collect();
    let ids = IdBuf::window(ctx.dataset, size_ns, &columns);
    Ok(Compiled::new(
        Window {
            size_ns,
            time: spec.time,
            lateness_ns,
            idle_ns,
            aggs,
            out: out.clone(),
            ids,
            cap: ctx.limits.max_state_keys.max(1),
            range_start: false,
            assets: HashMap::new(),
            open: BTreeMap::new(),
        },
        out,
    ))
}

/// The bounds of window `index`, or `None` where they overflow a timestamp.
fn bounds(index: i64, size_ns: i64) -> Option<(i64, i64)> {
    let start = index.checked_mul(size_ns)?;
    Some((start, start.checked_add(size_ns)?))
}

fn accumulate(aggs: &[Agg], accs: &mut [Acc], key: OrderKey, record: &Record) {
    for (agg, acc) in aggs.iter().zip(accs) {
        let Some(v) = record.try_get_at(agg.field).filter(|v| !v.is_null()) else {
            continue;
        };
        if agg.func == AggFn::Count {
            acc.n += 1;
        } else if let Some(x) = v.as_f64().or_else(|| v.as_i64().map(|i| i as f64))
            && !x.is_nan()
        {
            acc.add(key, x);
        }
    }
}

fn pane_opened(open: &mut BTreeMap<u64, usize>, pos: WalPos) {
    *open.entry(pos.get()).or_default() += 1;
}

fn pane_closed(open: &mut BTreeMap<u64, usize>, pos: WalPos) {
    if let Some(n) = open.get_mut(&pos.get()) {
        *n -= 1;
        if *n == 0 {
            open.remove(&pos.get());
        }
    }
}

fn new_pane(
    aggs: &[Agg],
    index: i64,
    pos: WalPos,
    ingest_ns: i64,
    key: OrderKey,
    range_start: bool,
    record: &Record,
) -> Pane {
    let mut accs = vec![Acc::EMPTY; aggs.len()];
    accumulate(aggs, &mut accs, key, record);
    Pane {
        index,
        first_pos: pos,
        last_ingest_ns: ingest_ns,
        has_gap: false,
        range_start,
        accs,
    }
}

impl Window {
    fn ensure_room(&mut self) -> bool {
        if self.assets.len() < self.cap {
            return true;
        }
        let victim = self
            .assets
            .iter()
            .filter(|(_, state)| state.panes.is_empty())
            .min_by(|(a_path, a_st), (b_path, b_st)| {
                a_st.last_ingest_ns
                    .cmp(&b_st.last_ingest_ns)
                    .then_with(|| a_path.iter().cmp(b_path.iter()))
            })
            .map(|(asset, _)| asset.clone());
        if let Some(asset) = victim {
            self.assets.remove(&asset);
            true
        } else {
            false
        }
    }

    fn close_idle(
        &mut self,
        active: &AssetPath,
        trigger_ingest_ns: i64,
        trigger: &Event,
        out: &mut Vec<Event>,
    ) -> Result<(), ThadamError> {
        let Some(idle_ns) = self.idle_ns else {
            return Ok(());
        };
        let mut due: Vec<(WalPos, AssetPath)> = self
            .assets
            .iter()
            .filter_map(|(asset, state)| {
                let first_pos = state.panes.values().map(|p| p.first_pos).min()?;
                (asset != active
                    && trigger_ingest_ns.saturating_sub(state.last_ingest_ns) >= idle_ns)
                    .then(|| (first_pos, asset.clone()))
            })
            .collect();
        due.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.iter().cmp(b.1.iter())));
        for (_, asset) in due {
            let Some(state) = self.assets.get_mut(&asset) else {
                continue;
            };
            let panes = std::mem::take(&mut state.panes);
            for idx in panes.keys() {
                if let Some((_, end)) = bounds(*idx, self.size_ns) {
                    state.watermark_ns = Some(state.watermark_ns.map_or(end, |w| w.max(end)));
                }
            }
            for (_, done) in panes {
                pane_closed(&mut self.open, done.first_pos);
                out.push(self.aggregate(&asset, &done, trigger)?);
            }
        }
        Ok(())
    }

    /// The aggregate of `done`, closed by `trigger`.
    fn aggregate(
        &self,
        asset: &AssetPath,
        done: &Pane,
        trigger: &Event,
    ) -> Result<Event, ThadamError> {
        let (start, end) = bounds(done.index, self.size_ns)
            .ok_or_else(|| step_failed("window bounds overflow a timestamp"))?;
        let mut values = vec![
            Value::Ts(Timestamp::from_nanos(start)),
            Value::Ts(Timestamp::from_nanos(end)),
        ];
        for (agg, acc) in self.aggs.iter().zip(&done.accs) {
            let seen = acc.n > 0;
            values.push(match agg.func {
                AggFn::Count => Value::U64(acc.n),
                _ if !seen => Value::Null,
                AggFn::Sum => Value::F64(acc.sum),
                AggFn::Min => Value::F64(acc.min),
                AggFn::Max => Value::F64(acc.max),
                AggFn::Mean => Value::F64(acc.sum / acc.n as f64),
                AggFn::First => Value::F64(acc.first_val),
                AggFn::Last => Value::F64(acc.last_val),
                AggFn::Stddev => Value::F64((acc.m2 / acc.n as f64).max(0.0).sqrt()),
                AggFn::Range => Value::F64(acc.max - acc.min),
                AggFn::Delta => Value::F64(acc.last_val - acc.first_val),
                AggFn::Rms => Value::F64((acc.sum_sq / acc.n as f64).sqrt()),
            });
        }
        let record = Record::new(self.out.clone(), values)
            .map_err(|e| step_failed(&format!("derived record is invalid: {e}")))?;
        let times = Times::new(trigger.times().ingest(), TimeDomain::Utc)
            .with_source(Timestamp::from_nanos(start))
            .map_err(|e| step_failed(&e.to_string()))?;
        let quality = if done.has_gap {
            Quality {
                status: QualityStatus::Uncertain,
                flags: QualityFlags::empty(),
                reason: Some("window_gap".into()),
            }
        } else if done.range_start {
            Quality {
                status: QualityStatus::Uncertain,
                flags: QualityFlags::empty(),
                reason: Some("window_range_start".into()),
            }
        } else {
            Quality::good()
        };
        Event::builder(
            self.ids.window_event(asset, start),
            trigger.wal_pos(),
            times,
        )
        .asset(asset.clone())
        .quality(quality)
        .payload(Payload::Record(record))
        .build()
        .map_err(|e| step_failed(&e.to_string()))
    }
}

impl Step for Window {
    fn apply(&mut self, event: Event) -> StepResult {
        let Some(record) = event.payload().as_record() else {
            return StepResult::Reject(step_failed("event payload is not a record"));
        };
        let ingest_ns = event.times().ingest().as_nanos();
        let t_ns = match self.time {
            WindowTime::Ingest => ingest_ns,
            WindowTime::Event => {
                let Some(ts) = event.times().event_time() else {
                    return StepResult::Filtered;
                };
                ts.as_nanos()
            }
        };
        let index = t_ns.div_euclid(self.size_ns);
        let pos = event.wal_pos();
        let order_key = (t_ns, pos.get(), event.sample_idx().unwrap_or(0));
        let asset = event.asset();

        let mut out = Vec::new();
        if self.time == WindowTime::Ingest {
            let mut closed_pane = None;
            if let Some(state) = self.assets.get_mut(asset) {
                if let Some((_, pane)) = state.panes.iter_mut().next()
                    && index <= pane.index
                {
                    accumulate(&self.aggs, &mut pane.accs, order_key, record);
                    pane.last_ingest_ns = pane.last_ingest_ns.max(ingest_ns);
                    state.last_ingest_ns = state.last_ingest_ns.max(ingest_ns);
                } else {
                    if bounds(index, self.size_ns).is_none() {
                        return StepResult::Reject(step_failed(
                            "window bounds overflow a timestamp",
                        ));
                    }
                    let is_first = !state.opened_any;
                    state.opened_any = true;
                    state.last_ingest_ns = ingest_ns;
                    let next = new_pane(
                        &self.aggs,
                        index,
                        pos,
                        ingest_ns,
                        order_key,
                        is_first && self.range_start,
                        record,
                    );
                    if let Some((_, done)) = state.panes.pop_first() {
                        pane_closed(&mut self.open, done.first_pos);
                        closed_pane = Some(done);
                    }
                    pane_opened(&mut self.open, pos);
                    state.panes.insert(index, next);
                }
            } else {
                if bounds(index, self.size_ns).is_none() {
                    return StepResult::Reject(step_failed("window bounds overflow a timestamp"));
                }
                if let Err(e) = self.close_idle(asset, ingest_ns, &event, &mut out) {
                    return StepResult::Reject(e);
                }
                if !self.ensure_room() {
                    return StepResult::Reject(step_failed(&format!(
                        "window state is full: {} assets have an open window",
                        self.cap
                    )));
                }
                let next = new_pane(
                    &self.aggs,
                    index,
                    pos,
                    ingest_ns,
                    order_key,
                    self.range_start,
                    record,
                );
                pane_opened(&mut self.open, pos);
                let mut panes = BTreeMap::new();
                panes.insert(index, next);
                self.assets.insert(
                    asset.clone(),
                    AssetState {
                        panes,
                        last_ingest_ns: ingest_ns,
                        max_event_ns: None,
                        watermark_ns: None,
                        opened_any: true,
                    },
                );
                return StepResult::Emit(out);
            }
            if let Some(done) = closed_pane {
                match self.aggregate(asset, &done, &event) {
                    Ok(aggregate) => out.push(aggregate),
                    Err(e) => return StepResult::Reject(e),
                }
            }
            if let Err(e) = self.close_idle(asset, ingest_ns, &event, &mut out) {
                return StepResult::Reject(e);
            }
            return StepResult::Emit(out);
        }

        let Some((_, end)) = bounds(index, self.size_ns) else {
            return StepResult::Reject(step_failed("window bounds overflow a timestamp"));
        };
        let mut closed = Vec::new();
        let mut late = false;
        if let Some(state) = self.assets.get_mut(asset) {
            let max_ev = state.max_event_ns.map_or(t_ns, |m| m.max(t_ns));
            state.max_event_ns = Some(max_ev);
            let wm = max_ev.saturating_sub(self.lateness_ns);
            let watermark_ns = state.watermark_ns.map_or(wm, |w| w.max(wm));
            state.watermark_ns = Some(watermark_ns);
            if end <= watermark_ns {
                late = true;
            } else {
                if state.panes.is_empty() {
                    state.last_ingest_ns = ingest_ns;
                } else {
                    state.last_ingest_ns = state.last_ingest_ns.max(ingest_ns);
                }
                if let Some(pane) = state.panes.get_mut(&index) {
                    accumulate(&self.aggs, &mut pane.accs, order_key, record);
                    pane.last_ingest_ns = pane.last_ingest_ns.max(ingest_ns);
                } else {
                    let is_first = !state.opened_any;
                    state.opened_any = true;
                    let next = new_pane(
                        &self.aggs,
                        index,
                        pos,
                        ingest_ns,
                        order_key,
                        is_first && self.range_start,
                        record,
                    );
                    pane_opened(&mut self.open, pos);
                    state.panes.insert(index, next);
                }
                while let Some((&idx, _)) = state.panes.first_key_value() {
                    let Some((_, pane_end)) = bounds(idx, self.size_ns) else {
                        break;
                    };
                    if pane_end > watermark_ns {
                        break;
                    }
                    if let Some((_, done)) = state.panes.pop_first() {
                        pane_closed(&mut self.open, done.first_pos);
                        closed.push(done);
                    }
                }
            }
        } else {
            if let Err(e) = self.close_idle(asset, ingest_ns, &event, &mut out) {
                return StepResult::Reject(e);
            }
            if !self.ensure_room() {
                return StepResult::Reject(step_failed(&format!(
                    "window state is full: {} assets have an open window",
                    self.cap
                )));
            }
            let next = new_pane(
                &self.aggs,
                index,
                pos,
                ingest_ns,
                order_key,
                self.range_start,
                record,
            );
            pane_opened(&mut self.open, pos);
            let mut panes = BTreeMap::new();
            panes.insert(index, next);
            self.assets.insert(
                asset.clone(),
                AssetState {
                    panes,
                    last_ingest_ns: ingest_ns,
                    max_event_ns: Some(t_ns),
                    watermark_ns: Some(t_ns.saturating_sub(self.lateness_ns)),
                    opened_any: true,
                },
            );
            return StepResult::Emit(out);
        }

        if late {
            if let Err(e) = self.close_idle(asset, ingest_ns, &event, &mut out) {
                return StepResult::Reject(e);
            }
            if out.is_empty() {
                return StepResult::Late;
            }
            return StepResult::Emit(out);
        }

        for done in closed {
            match self.aggregate(asset, &done, &event) {
                Ok(aggregate) => out.push(aggregate),
                Err(e) => return StepResult::Reject(e),
            }
        }
        if let Err(e) = self.close_idle(asset, ingest_ns, &event, &mut out) {
            return StepResult::Reject(e);
        }
        StepResult::Emit(out)
    }

    fn hold(&self) -> Option<WalPos> {
        self.open.keys().next().map(|p| WalPos::new(*p))
    }

    fn windowed(&self) -> bool {
        true
    }

    fn on_gap(&mut self) {
        for state in self.assets.values_mut() {
            for pane in state.panes.values_mut() {
                pane.has_gap = true;
            }
        }
    }

    fn mark_range_start(&mut self) {
        self.range_start = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::ChainLimits;
    use crate::testutil::{event_at, shape, with_ctx, with_ctx_limits};
    use serde_json::json;

    fn input() -> ShapeRef {
        shape(&[
            ("temp", FieldType::F64),
            ("level", FieldType::I32),
            ("note", FieldType::Str),
        ])
    }

    fn build(spec: serde_json::Value) -> Result<Box<dyn Step>, Vec<ThadamError>> {
        let spec: WindowSpec = serde_json::from_value(spec).unwrap();
        with_ctx(|ctx| compile(&spec, ctx, &input(), "transforms[0]").map(|c| c.step))
    }

    fn temp_stats() -> serde_json::Value {
        json!({
            "size": "10s",
            "aggregates": [
                { "field": "temp", "fn": "count", "as": "n" },
                { "field": "temp", "fn": "sum", "as": "sum" },
                { "field": "temp", "fn": "min", "as": "lo" },
                { "field": "temp", "fn": "max", "as": "hi" },
                { "field": "temp", "fn": "mean", "as": "avg" },
            ],
        })
    }

    fn feed(
        step: &mut Box<dyn Step>,
        robot: &str,
        secs: i64,
        pos: u64,
        temp: Value,
        level: Value,
    ) -> Vec<Event> {
        let e = event_at(&input(), vec![temp, level, Value::Null], robot, secs, pos);
        match step.apply(e) {
            StepResult::Emit(events) => events,
            _ => panic!("a window step emits"),
        }
    }

    fn temp(step: &mut Box<dyn Step>, robot: &str, secs: i64, pos: u64, t: f64) -> Vec<Event> {
        feed(step, robot, secs, pos, Value::F64(t), Value::Null)
    }

    fn col<'a>(e: &'a Event, name: &str) -> &'a Value {
        e.payload().as_record().unwrap().get(name).unwrap()
    }

    #[test]
    fn a_window_closes_when_the_asset_speaks_in_a_later_one() {
        let mut step = build(temp_stats()).unwrap();
        assert!(temp(&mut step, "a", 0, 1, 1.0).is_empty());
        assert!(temp(&mut step, "a", 5, 2, 2.0).is_empty());
        assert!(
            temp(&mut step, "a", 9, 3, 6.0).is_empty(),
            "9.99s is still in [0, 10)"
        );
        let out = temp(&mut step, "a", 10, 4, 100.0);
        assert_eq!(out.len(), 1);
        let e = &out[0];
        assert_eq!(
            col(e, "window_start"),
            &Value::Ts(Timestamp::from_secs(0).unwrap())
        );
        assert_eq!(
            col(e, "window_end"),
            &Value::Ts(Timestamp::from_secs(10).unwrap())
        );
        assert_eq!(col(e, "n"), &Value::U64(3));
        assert_eq!(col(e, "sum"), &Value::F64(9.0));
        assert_eq!(col(e, "lo"), &Value::F64(1.0));
        assert_eq!(col(e, "hi"), &Value::F64(6.0));
        assert_eq!(col(e, "avg"), &Value::F64(3.0));
        assert_eq!(e.wal_pos().get(), 4, "the trigger's frame");
        assert_eq!(e.times().ingest(), Timestamp::from_secs(10).unwrap());
        assert_eq!(
            e.times().event_time(),
            Some(Timestamp::from_secs(0).unwrap()),
            "event time is the window start"
        );
        assert_eq!(e.asset(), &AssetPath::new([("robot", "a")]).unwrap());

        let out = temp(&mut step, "a", 25, 5, 0.0);
        assert_eq!(out.len(), 1, "a skipped window emits nothing");
        assert_eq!(
            col(&out[0], "window_start"),
            &Value::Ts(Timestamp::from_secs(10).unwrap())
        );
        assert_eq!(
            col(&out[0], "n"),
            &Value::U64(1),
            "the trigger started [10, 20)"
        );
    }

    #[test]
    fn assets_have_their_own_windows() {
        let mut step = build(temp_stats()).unwrap();
        assert!(temp(&mut step, "a", 1, 1, 1.0).is_empty());
        assert!(temp(&mut step, "b", 2, 2, 10.0).is_empty());
        assert!(temp(&mut step, "b", 12, 3, 20.0).len() == 1, "b moved on");
        assert!(
            temp(&mut step, "a", 3, 4, 3.0).is_empty(),
            "a is unaffected"
        );
        let out = temp(&mut step, "a", 11, 5, 0.0);
        assert_eq!(out.len(), 1);
        assert_eq!(col(&out[0], "n"), &Value::U64(2));
        assert_eq!(col(&out[0], "sum"), &Value::F64(4.0));
        assert_ne!(
            out[0].id(),
            temp(&mut step, "b", 25, 6, 0.0)[0].id(),
            "ids differ by asset"
        );
    }

    #[test]
    fn integers_nulls_and_nan_are_handled() {
        let mut step = build(json!({
            "size": "10s",
            "aggregates": [
                { "field": "level", "fn": "count", "as": "n" },
                { "field": "level", "fn": "sum", "as": "sum" },
                { "field": "level", "fn": "mean", "as": "avg" },
                { "field": "temp", "fn": "max", "as": "hi" },
                { "field": "note", "fn": "count", "as": "notes" },
            ],
        }))
        .unwrap();
        feed(&mut step, "a", 0, 1, Value::F64(f64::NAN), Value::I32(4));
        feed(&mut step, "a", 1, 2, Value::Null, Value::Null);
        feed(&mut step, "a", 2, 3, Value::F64(2.5), Value::I32(-1));
        let out = feed(&mut step, "a", 10, 4, Value::Null, Value::Null);
        let e = &out[0];
        assert_eq!(col(e, "n"), &Value::U64(2), "the null is not counted");
        assert_eq!(col(e, "sum"), &Value::F64(3.0));
        assert_eq!(col(e, "avg"), &Value::F64(1.5));
        assert_eq!(col(e, "hi"), &Value::F64(2.5), "NaN is skipped");
        assert_eq!(col(e, "notes"), &Value::U64(0));

        let out = feed(&mut step, "a", 20, 5, Value::Null, Value::Null);
        let e = &out[0];
        assert_eq!(col(e, "n"), &Value::U64(0), "a window with no value");
        assert_eq!(col(e, "sum"), &Value::Null);
        assert_eq!(col(e, "avg"), &Value::Null);
        assert_eq!(col(e, "hi"), &Value::Null);
    }

    #[test]
    fn ingest_time_stepping_backwards_joins_the_open_window() {
        let mut step = build(temp_stats()).unwrap();
        temp(&mut step, "a", 15, 1, 1.0);
        assert!(temp(&mut step, "a", 3, 2, 2.0).is_empty());
        let out = temp(&mut step, "a", 20, 3, 0.0);
        assert_eq!(out.len(), 1);
        assert_eq!(col(&out[0], "n"), &Value::U64(2));
        assert_eq!(
            col(&out[0], "window_start"),
            &Value::Ts(Timestamp::from_secs(10).unwrap())
        );
    }

    #[test]
    fn negative_ingest_times_floor_to_the_window_below() {
        let mut step = build(temp_stats()).unwrap();
        temp(&mut step, "a", -1, 1, 1.0);
        let out = temp(&mut step, "a", 0, 2, 2.0);
        assert_eq!(
            col(&out[0], "window_start"),
            &Value::Ts(Timestamp::from_secs(-10).unwrap())
        );
    }

    #[test]
    fn hold_is_the_first_frame_of_the_oldest_open_window() {
        let mut step = build(temp_stats()).unwrap();
        assert_eq!(step.hold(), None);
        temp(&mut step, "a", 1, 5, 1.0);
        temp(&mut step, "b", 2, 6, 1.0);
        temp(&mut step, "a", 3, 7, 1.0);
        assert_eq!(step.hold().map(WalPos::get), Some(5));
        temp(&mut step, "a", 11, 8, 1.0);
        assert_eq!(
            step.hold().map(WalPos::get),
            Some(6),
            "a's window moved on, b's is still open"
        );
        temp(&mut step, "b", 12, 9, 1.0);
        assert_eq!(step.hold().map(WalPos::get), Some(8));
        assert!(step.windowed());
    }

    #[test]
    fn two_assets_opening_in_one_frame_share_a_hold_position() {
        let mut step = build(temp_stats()).unwrap();
        temp(&mut step, "a", 1, 5, 1.0);
        temp(&mut step, "b", 1, 5, 1.0);
        temp(&mut step, "a", 11, 6, 1.0);
        assert_eq!(
            step.hold().map(WalPos::get),
            Some(5),
            "b still holds frame 5"
        );
        temp(&mut step, "b", 11, 6, 1.0);
        assert_eq!(step.hold().map(WalPos::get), Some(6));
    }

    #[test]
    fn replaying_the_same_events_gives_the_same_aggregates() {
        let run = || {
            let mut step = build(temp_stats()).unwrap();
            let mut out = Vec::new();
            for (i, secs) in [0, 4, 11, 12, 31, 45].into_iter().enumerate() {
                out.extend(temp(&mut step, "a", secs, i as u64 + 1, secs as f64));
            }
            out.iter()
                .map(|e| (e.id(), e.wal_pos(), format!("{:?}", e.payload())))
                .collect::<Vec<_>>()
        };
        let first = run();
        assert_eq!(first.len(), 3);
        assert_eq!(first, run());
    }

    #[test]
    fn a_full_state_rejects_new_assets_and_keeps_serving_known_ones() {
        let limits = ChainLimits::default().with_max_state_keys(1);
        let spec: WindowSpec = serde_json::from_value(temp_stats()).unwrap();
        let mut step = with_ctx_limits(&limits, |ctx| {
            compile(&spec, ctx, &input(), "t").map(|c| c.step)
        })
        .unwrap();
        temp(&mut step, "a", 1, 1, 1.0);
        let e = event_at(
            &input(),
            vec![Value::F64(1.0), Value::Null, Value::Null],
            "b",
            2,
            2,
        );
        let StepResult::Reject(err) = step.apply(e) else {
            panic!("the second asset has no room");
        };
        assert!(
            err.message.contains("window state is full"),
            "{}",
            err.message
        );
        assert_eq!(
            step.hold().map(WalPos::get),
            Some(1),
            "the reject opened nothing"
        );
        assert_eq!(temp(&mut step, "a", 11, 3, 1.0).len(), 1);
    }

    #[test]
    fn an_aggregate_of_a_unit_field_keeps_the_unit() {
        let with_unit = ShapeRef::new(
            Shape::new(vec![FieldDef::new("temp", FieldType::F32).with_unit("Cel")]).unwrap(),
        );
        let spec: WindowSpec = serde_json::from_value(json!({
            "size": "1m",
            "aggregates": [
                { "field": "temp", "fn": "mean", "as": "avg" },
                { "field": "temp", "fn": "count", "as": "n" },
            ],
        }))
        .unwrap();
        let c = with_ctx(|ctx| compile(&spec, ctx, &with_unit, "t")).unwrap();
        let unit = |name: &str| {
            let i = c.out_shape.index_of(name).unwrap();
            c.out_shape.field(i).unwrap().unit.clone()
        };
        assert_eq!(unit("avg").as_deref(), Some("Cel"));
        assert_eq!(unit("n"), None);
        assert_eq!(
            c.out_shape
                .field(c.out_shape.index_of("avg").unwrap())
                .unwrap()
                .ty,
            FieldType::F64
        );
    }

    #[test]
    fn idle_close_releases_silent_assets_in_deterministic_order() {
        let mut step = build(json!({
            "size": "10s",
            "idle_close": "20s",
            "aggregates": [
                { "field": "temp", "fn": "count", "as": "n" },
                { "field": "temp", "fn": "sum", "as": "sum" },
            ],
        }))
        .unwrap();
        assert!(temp(&mut step, "z", 1, 1, 10.0).is_empty());
        assert!(temp(&mut step, "b", 2, 2, 20.0).is_empty());
        assert!(temp(&mut step, "a", 2, 2, 30.0).is_empty());
        assert!(
            temp(&mut step, "z", 4, 3, 5.0).is_empty(),
            "accumulating into z's window advances z's last_ingest_ns to 4s"
        );
        assert_eq!(step.hold().map(WalPos::get), Some(1));

        assert!(
            temp(&mut step, "d", 21, 4, 1.0).is_empty(),
            "21s is 19s after a/b (2s) and 17s after z (4s): none is >= 20s idle yet"
        );
        let out = temp(&mut step, "d", 22, 5, 2.0);
        let robots: Vec<&str> = out
            .iter()
            .map(|e| e.asset().get("robot").unwrap())
            .collect();
        assert_eq!(
            robots,
            ["a", "b"],
            "equal first_pos (2) ties break by asset"
        );
        assert_eq!(out[0].wal_pos().get(), 5);
        assert_eq!(out[0].times().ingest(), Timestamp::from_secs(22).unwrap());
        assert_eq!(col(&out[0], "sum"), &Value::F64(30.0));
        assert_eq!(col(&out[1], "sum"), &Value::F64(20.0));
        assert_eq!(
            step.hold().map(WalPos::get),
            Some(1),
            "z (last_ingest 4s) is still open at frame 1"
        );

        assert!(temp(&mut step, "c", 3, 6, 40.0).is_empty());
        let out = temp(&mut step, "d", 24, 7, 3.0);
        let robots: Vec<&str> = out
            .iter()
            .map(|e| e.asset().get("robot").unwrap())
            .collect();
        assert_eq!(
            robots,
            ["z", "c"],
            "ordered by first_pos (1 then 6) before asset"
        );
        assert_eq!(col(&out[0], "n"), &Value::U64(2));
        assert_eq!(col(&out[0], "sum"), &Value::F64(15.0));
        assert_eq!(col(&out[1], "sum"), &Value::F64(40.0));
        assert_eq!(
            step.hold().map(WalPos::get),
            Some(4),
            "z and c released; d's window at frame 4 is now the oldest"
        );
    }

    #[test]
    fn event_time_windows_allow_lateness_and_drop_late_and_timeless_events() {
        let mut step = build(json!({
            "size": "10s",
            "time": "event",
            "allowed_lateness": "5s",
            "aggregates": [
                { "field": "temp", "fn": "count", "as": "n" },
                { "field": "temp", "fn": "sum", "as": "sum" },
            ],
        }))
        .unwrap();

        let mk = |robot: &str, ingest_s: i64, event_s: Option<i64>, pos: u64, v: f64| {
            let mut times = Times::new(
                Timestamp::from_secs(ingest_s).unwrap(),
                if event_s.is_some() {
                    TimeDomain::Utc
                } else {
                    TimeDomain::Monotonic
                },
            );
            if let Some(es) = event_s {
                times = times
                    .with_source(Timestamp::from_secs(es).unwrap())
                    .unwrap();
            }
            Event::builder(
                thadam_core::EventId::from_u128(pos as u128),
                WalPos::new(pos),
                times,
            )
            .asset(AssetPath::new([("robot", robot)]).unwrap())
            .payload(Record::new(input(), vec![Value::F64(v), Value::Null, Value::Null]).unwrap())
            .build()
            .unwrap()
        };

        assert!(matches!(
            step.apply(mk("a", 1, None, 1, 99.0)),
            StepResult::Filtered
        ));
        assert_eq!(step.hold(), None);

        // Event at t=8s opens [0, 10), watermark = 3s.
        let StepResult::Emit(out) = step.apply(mk("a", 2, Some(8), 2, 10.0)) else {
            panic!()
        };
        assert!(out.is_empty());
        assert_eq!(step.hold().map(WalPos::get), Some(2));

        // Event at t=12s opens [10, 20), watermark = 7s (< 10s, so [0, 10) stays open).
        let StepResult::Emit(out) = step.apply(mk("a", 3, Some(12), 3, 20.0)) else {
            panic!()
        };
        assert!(out.is_empty());
        assert_eq!(step.hold().map(WalPos::get), Some(2));

        // Out-of-order event at t=6s (within 5s lateness) joins [0, 10).
        let StepResult::Emit(out) = step.apply(mk("a", 4, Some(6), 4, 5.0)) else {
            panic!()
        };
        assert!(out.is_empty());

        // Event at t=15s advances watermark to 10s, closing [0, 10).
        let StepResult::Emit(out) = step.apply(mk("a", 5, Some(15), 5, 30.0)) else {
            panic!()
        };
        assert_eq!(out.len(), 1);
        assert_eq!(
            col(&out[0], "window_start"),
            &Value::Ts(Timestamp::from_secs(0).unwrap())
        );
        assert_eq!(col(&out[0], "n"), &Value::U64(2));
        assert_eq!(col(&out[0], "sum"), &Value::F64(15.0));
        assert_eq!(step.hold().map(WalPos::get), Some(3));

        // Late event at t=9s (window [0, 10) has end 10s <= watermark 10s) is dropped as Late.
        assert!(matches!(
            step.apply(mk("a", 6, Some(9), 6, 100.0)),
            StepResult::Late
        ));
        assert_eq!(step.hold().map(WalPos::get), Some(3));
    }

    #[test]
    fn on_gap_and_mark_range_start_flag_affected_windows_uncertain() {
        let mut step = build(temp_stats()).unwrap();
        step.mark_range_start();

        assert!(temp(&mut step, "a", 1, 1, 1.0).is_empty());
        assert!(temp(&mut step, "b", 2, 2, 2.0).is_empty());
        let first_a = &temp(&mut step, "a", 11, 3, 3.0)[0];
        assert_eq!(first_a.quality().status, QualityStatus::Uncertain);
        assert_eq!(
            first_a.quality().reason.as_deref(),
            Some("window_range_start")
        );

        let second_a = &temp(&mut step, "a", 21, 4, 4.0)[0];
        assert_eq!(second_a.quality().status, QualityStatus::Good);
        assert_eq!(second_a.quality().reason, None);

        // A gap while a's [20, 30) and b's first [0, 10) are open marks both window_gap.
        step.on_gap();
        assert!(temp(&mut step, "c", 22, 5, 5.0).is_empty());
        let gapped_a = &temp(&mut step, "a", 31, 6, 6.0)[0];
        assert_eq!(gapped_a.quality().status, QualityStatus::Uncertain);
        assert_eq!(gapped_a.quality().reason.as_deref(), Some("window_gap"));

        let gapped_b = &temp(&mut step, "b", 12, 7, 7.0)[0];
        assert_eq!(gapped_b.quality().status, QualityStatus::Uncertain);
        assert_eq!(
            gapped_b.quality().reason.as_deref(),
            Some("window_gap"),
            "window_gap takes precedence over window_range_start"
        );

        let post_gap_a = &temp(&mut step, "a", 41, 8, 8.0)[0];
        assert_eq!(post_gap_a.quality().status, QualityStatus::Good);
    }

    #[test]
    fn idle_close_on_event_time_advances_watermark_and_frees_capacity() {
        let limits = ChainLimits::default().with_max_state_keys(1);
        let spec: WindowSpec = serde_json::from_value(json!({
            "size": "10s",
            "time": "event",
            "allowed_lateness": "5s",
            "idle_close": "20s",
            "aggregates": [{ "field": "temp", "fn": "sum", "as": "sum" }],
        }))
        .unwrap();
        let mut step = with_ctx_limits(&limits, |ctx| {
            compile(&spec, ctx, &input(), "t").map(|c| c.step)
        })
        .unwrap();

        let mk = |robot: &str, ingest_s: i64, event_s: i64, pos: u64, v: f64| {
            let times = Times::new(Timestamp::from_secs(ingest_s).unwrap(), TimeDomain::Utc)
                .with_source(Timestamp::from_secs(event_s).unwrap())
                .unwrap();
            Event::builder(
                thadam_core::EventId::from_u128(pos as u128),
                WalPos::new(pos),
                times,
            )
            .asset(AssetPath::new([("robot", robot)]).unwrap())
            .payload(Record::new(input(), vec![Value::F64(v), Value::Null, Value::Null]).unwrap())
            .build()
            .unwrap()
        };

        assert!(matches!(step.apply(mk("a", 1, 2, 1, 10.0)), StepResult::Emit(v) if v.is_empty()));
        // Another asset triggers idle_close on `a` (at cap=1) and opens its own window.
        let StepResult::Emit(out) = step.apply(mk("b", 25, 25, 2, 20.0)) else {
            panic!("idle-closed slot should be reused at cap=1")
        };
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].asset().get("robot"), Some("a"));
        assert_eq!(step.hold().map(WalPos::get), Some(2));
    }

    #[test]
    fn first_last_stddev_range_delta_and_rms_cover_empty_single_and_large_offset_windows() {
        let mut step = build(json!({
            "size": "10s",
            "aggregates": [
                { "field": "temp", "fn": "count", "as": "n" },
                { "field": "temp", "fn": "first", "as": "first" },
                { "field": "temp", "fn": "last", "as": "last" },
                { "field": "temp", "fn": "stddev", "as": "stddev" },
                { "field": "temp", "fn": "range", "as": "range" },
                { "field": "temp", "fn": "delta", "as": "delta" },
                { "field": "temp", "fn": "rms", "as": "rms" },
            ],
        }))
        .unwrap();

        // Window [0, 10): numeric accs have n == 0 (only Null / NaN).
        feed(&mut step, "a", 1, 1, Value::Null, Value::Null);
        feed(&mut step, "a", 2, 2, Value::F64(f64::NAN), Value::Null);

        // Window [10, 20): n == 1 (value -5.0).
        let out = temp(&mut step, "a", 12, 3, -5.0);
        let e = &out[0];
        for name in ["first", "last", "stddev", "range", "delta", "rms"] {
            assert_eq!(col(e, name), &Value::Null, "{name} on empty window");
        }

        // Window [20, 30): n == 3 with large offset 1e9 + {1.0, 3.0, 2.0}.
        let base = 1e9;
        let out = temp(&mut step, "a", 21, 4, base + 1.0);
        let e = &out[0];
        assert_eq!(col(e, "n"), &Value::U64(1));
        assert_eq!(col(e, "first"), &Value::F64(-5.0));
        assert_eq!(col(e, "last"), &Value::F64(-5.0));
        assert_eq!(col(e, "stddev"), &Value::F64(0.0));
        assert_eq!(col(e, "range"), &Value::F64(0.0));
        assert_eq!(col(e, "delta"), &Value::F64(0.0));
        assert_eq!(col(e, "rms"), &Value::F64(5.0));

        temp(&mut step, "a", 22, 5, base + 3.0);
        temp(&mut step, "a", 25, 6, base + 2.0);
        let out = temp(&mut step, "a", 31, 7, 0.0);
        let e = &out[0];
        assert_eq!(col(e, "n"), &Value::U64(3));
        assert_eq!(col(e, "first"), &Value::F64(base + 1.0));
        assert_eq!(col(e, "last"), &Value::F64(base + 2.0));
        assert_eq!(col(e, "range"), &Value::F64(2.0));
        assert_eq!(col(e, "delta"), &Value::F64(1.0));
        let Value::F64(sd) = *col(e, "stddev") else {
            panic!()
        };
        let want_sd = (2.0_f64 / 3.0).sqrt();
        assert!((sd - want_sd).abs() < 1e-9, "sd={sd} want={want_sd}");

        // Window [30, 40): known RMS on 3.0 and 4.0 -> sqrt((9 + 16) / 2) = sqrt(12.5).
        let mut rms_step = build(json!({
            "size": "10s",
            "aggregates": [{ "field": "temp", "fn": "rms", "as": "rms" }],
        }))
        .unwrap();
        temp(&mut rms_step, "a", 1, 1, 3.0);
        temp(&mut rms_step, "a", 2, 2, -4.0);
        let out = temp(&mut rms_step, "a", 11, 3, 0.0);
        assert_eq!(col(&out[0], "rms"), &Value::F64(12.5_f64.sqrt()));
    }

    #[test]
    fn event_time_first_last_and_delta_follow_event_time_across_out_of_order_arrivals() {
        let mut step = build(json!({
            "size": "10s",
            "time": "event",
            "allowed_lateness": "5s",
            "aggregates": [
                { "field": "temp", "fn": "first", "as": "first" },
                { "field": "temp", "fn": "last", "as": "last" },
                { "field": "temp", "fn": "delta", "as": "delta" },
                { "field": "temp", "fn": "range", "as": "range" },
            ],
        }))
        .unwrap();

        let mk = |ingest_s: i64, event_s: i64, pos: u64, v: f64| {
            let times = Times::new(Timestamp::from_secs(ingest_s).unwrap(), TimeDomain::Utc)
                .with_source(Timestamp::from_secs(event_s).unwrap())
                .unwrap();
            Event::builder(
                thadam_core::EventId::from_u128(pos as u128),
                WalPos::new(pos),
                times,
            )
            .asset(AssetPath::new([("robot", "a")]).unwrap())
            .payload(Record::new(input(), vec![Value::F64(v), Value::Null, Value::Null]).unwrap())
            .build()
            .unwrap()
        };

        // Arrive at t=5s (val 20), then t=8s (val 5), then out-of-order t=2s (val 12).
        for e in [mk(1, 5, 1, 20.0), mk(2, 8, 2, 5.0), mk(3, 2, 3, 12.0)] {
            let StepResult::Emit(out) = step.apply(e) else {
                panic!()
            };
            assert!(out.is_empty());
        }
        let StepResult::Emit(out) = step.apply(mk(4, 15, 4, 0.0)) else {
            panic!()
        };
        assert_eq!(out.len(), 1);
        let e = &out[0];
        assert_eq!(col(e, "first"), &Value::F64(12.0));
        assert_eq!(col(e, "last"), &Value::F64(5.0));
        assert_eq!(col(e, "delta"), &Value::F64(-7.0));
        assert_eq!(col(e, "range"), &Value::F64(15.0));
    }

    #[test]
    fn config_errors() {
        let bad = [
            json!({ "size": "0s", "aggregates": [{ "field": "temp", "fn": "sum", "as": "s" }] }),
            json!({ "size": "10s", "idle_close": "0s", "aggregates": [{ "field": "temp", "fn": "sum", "as": "s" }] }),
            json!({ "size": "10s", "allowed_lateness": "5s", "aggregates": [{ "field": "temp", "fn": "sum", "as": "s" }] }),
            json!({ "size": "10s", "aggregates": [] }),
            json!({ "size": "10s", "aggregates": [{ "field": "nope", "fn": "sum", "as": "s" }] }),
            json!({ "size": "10s", "aggregates": [{ "field": "note", "fn": "sum", "as": "s" }] }),
            json!({ "size": "10s", "aggregates": [{ "field": "temp", "fn": "sum", "as": "" }] }),
            json!({ "size": "10s", "aggregates": [
                { "field": "temp", "fn": "sum", "as": "s" },
                { "field": "temp", "fn": "max", "as": "s" },
            ] }),
            json!({ "size": "10s", "aggregates": [{ "field": "temp", "fn": "sum", "as": "window_start" }] }),
        ];
        for spec in bad {
            assert!(build(spec.clone()).is_err(), "{spec}");
        }
        let wide = shape(&[("big", FieldType::I64)]);
        let spec: WindowSpec = serde_json::from_value(json!({
            "size": "10s",
            "aggregates": [{ "field": "big", "fn": "max", "as": "m" }],
        }))
        .unwrap();
        let errs = with_ctx(|ctx| compile(&spec, ctx, &wide, "t"))
            .err()
            .unwrap();
        assert!(
            errs[0].message.contains("do not fit f64"),
            "{}",
            errs[0].message
        );
        let counted: WindowSpec = serde_json::from_value(json!({
            "size": "10s",
            "aggregates": [{ "field": "big", "fn": "count", "as": "n" }],
        }))
        .unwrap();
        assert!(with_ctx(|ctx| compile(&counted, ctx, &wide, "t")).is_ok());

        assert!(
            serde_json::from_value::<WindowSpec>(json!({
                "size": "10s",
                "aggregates": [{ "field": "temp", "fn": "median", "as": "m" }],
            }))
            .is_err()
        );
    }
}
