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
    AssetPath, Event, FieldDef, FieldIdx, FieldType, Payload, Quality, Record, ThadamError, Shape,
    ShapeRef, TimeDomain, Times, Timestamp, Value, WalPos,
};

use super::fieldops::step_failed;
use super::{Compiled, Step, StepCtx, StepResult, config_error, intern};
use crate::eventid::IdBuf;
use crate::spec::{AggFn, WindowSpec};

const START: &str = "window_start";
const END: &str = "window_end";

#[derive(Debug)]
struct Agg {
    field: FieldIdx,
    func: AggFn,
}

#[derive(Clone, Copy, Debug)]
struct Acc {
    n: u64,
    sum: f64,
    min: f64,
    max: f64,
}

impl Acc {
    const EMPTY: Acc = Acc {
        n: 0,
        sum: 0.0,
        min: f64::INFINITY,
        max: f64::NEG_INFINITY,
    };

    fn add(&mut self, x: f64) {
        self.n += 1;
        self.sum += x;
        self.min = self.min.min(x);
        self.max = self.max.max(x);
    }
}

#[derive(Debug)]
struct Pane {
    index: i64,
    first_pos: WalPos,
    last_ingest_ns: i64,
    accs: Vec<Acc>,
}

// shortcut: ingest-time tumbling windows only (one open pane per asset, and
// `idle_close` advances only when another asset speaks); upgrade to event-time
// watermarks and `allowed_lateness` (design note S2) when out-of-order sources
// need them.
#[derive(Debug)]
struct Window {
    size_ns: i64,
    idle_ns: Option<i64>,
    aggs: Vec<Agg>,
    out: ShapeRef,
    ids: IdBuf,
    cap: usize,
    panes: HashMap<AssetPath, Pane>,
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
            idle_ns,
            aggs,
            out: out.clone(),
            ids,
            cap: ctx.limits.max_state_keys.max(1),
            panes: HashMap::new(),
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

fn accumulate(aggs: &[Agg], accs: &mut [Acc], record: &Record) {
    for (agg, acc) in aggs.iter().zip(accs) {
        let Some(v) = record.try_get_at(agg.field).filter(|v| !v.is_null()) else {
            continue;
        };
        if agg.func == AggFn::Count {
            acc.n += 1;
        } else if let Some(x) = v.as_f64().or_else(|| v.as_i64().map(|i| i as f64))
            && !x.is_nan()
        {
            acc.add(x);
        }
    }
}

impl Window {
    fn pane_opened(&mut self, pos: WalPos) {
        *self.open.entry(pos.get()).or_default() += 1;
    }

    fn pane_closed(&mut self, pos: WalPos) {
        if let Some(n) = self.open.get_mut(&pos.get()) {
            *n -= 1;
            if *n == 0 {
                self.open.remove(&pos.get());
            }
        }
    }

    fn pane(&self, index: i64, pos: WalPos, ingest_ns: i64, record: &Record) -> Pane {
        let mut accs = vec![Acc::EMPTY; self.aggs.len()];
        accumulate(&self.aggs, &mut accs, record);
        Pane {
            index,
            first_pos: pos,
            last_ingest_ns: ingest_ns,
            accs,
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
            .panes
            .iter()
            .filter(|&(asset, pane)| {
                asset != active && trigger_ingest_ns.saturating_sub(pane.last_ingest_ns) >= idle_ns
            })
            .map(|(asset, pane)| (pane.first_pos, asset.clone()))
            .collect();
        due.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.iter().cmp(b.1.iter())));
        for (_, asset) in due {
            if let Some(done) = self.panes.remove(&asset) {
                self.pane_closed(done.first_pos);
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
            });
        }
        let record = Record::new(self.out.clone(), values)
            .map_err(|e| step_failed(&format!("derived record is invalid: {e}")))?;
        let times = Times::new(trigger.times().ingest(), TimeDomain::Utc)
            .with_source(Timestamp::from_nanos(start))
            .map_err(|e| step_failed(&e.to_string()))?;
        Event::builder(
            self.ids.window_event(asset, start),
            trigger.wal_pos(),
            times,
        )
        .asset(asset.clone())
        .quality(Quality::good())
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
        let index = ingest_ns.div_euclid(self.size_ns);
        let pos = event.wal_pos();
        let asset = event.asset();

        let mut out = Vec::new();
        if let Some(pane) = self.panes.get_mut(asset)
            && index <= pane.index
        {
            accumulate(&self.aggs, &mut pane.accs, record);
            pane.last_ingest_ns = pane.last_ingest_ns.max(ingest_ns);
        } else {
            if bounds(index, self.size_ns).is_none() {
                return StepResult::Reject(step_failed("window bounds overflow a timestamp"));
            }
            let next = self.pane(index, pos, ingest_ns, record);
            if let Some(slot) = self.panes.get_mut(asset) {
                let done = std::mem::replace(slot, next);
                self.pane_closed(done.first_pos);
                self.pane_opened(pos);
                match self.aggregate(asset, &done, &event) {
                    Ok(aggregate) => out.push(aggregate),
                    Err(e) => return StepResult::Reject(e),
                }
            } else {
                if let Err(e) = self.close_idle(asset, ingest_ns, &event, &mut out) {
                    return StepResult::Reject(e);
                }
                if self.panes.len() >= self.cap {
                    return StepResult::Reject(step_failed(&format!(
                        "window state is full: {} assets have an open window",
                        self.cap
                    )));
                }
                self.pane_opened(pos);
                self.panes.insert(asset.clone(), next);
                return StepResult::Emit(out);
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
    fn config_errors() {
        let bad = [
            json!({ "size": "0s", "aggregates": [{ "field": "temp", "fn": "sum", "as": "s" }] }),
            json!({ "size": "10s", "idle_close": "0s", "aggregates": [{ "field": "temp", "fn": "sum", "as": "s" }] }),
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
