//! `on_change`: report by exception, with a heartbeat.
//!
//! Per asset, an event is kept when it is the asset's first, when a watched
//! field moved by more than its deadband from the **last kept** value, or when
//! the heartbeat interval passed since the last kept event. Values are compared
//! in arrival order and never by source time, so out-of-order input cannot make
//! the step panic or depend on the wall clock; the heartbeat is measured in
//! ingest time. If ingest time steps backwards the heartbeat baseline restarts.
//!
//! This is value-based and lossy by design. It is not deduplication.

use thadam_core::{Event, FieldIdx, Payload, ShapeRef, ThadamError, Value};

use super::state::AssetMap;
use super::{Compiled, Step, StepCtx, StepResult, config_error};
use crate::spec::OnChangeSpec;
use thadam_core::{ErrorDomain, codes};

#[derive(Debug)]
struct Watch {
    idx: FieldIdx,
    deadband: f64,
}

#[derive(Debug)]
struct State {
    last: Vec<Value>,
    last_kept_ns: i64,
}

#[derive(Debug)]
struct OnChange {
    watches: Vec<Watch>,
    heartbeat_ns: Option<i128>,
    states: AssetMap<State>,
}

fn is_numeric_type(ty: &thadam_core::FieldType) -> bool {
    use thadam_core::FieldType as T;
    match ty {
        T::List(inner) => is_numeric_type(inner),
        T::I8 | T::I16 | T::I32 | T::I64 | T::U8 | T::U16 | T::U32 | T::U64 | T::F32 | T::F64 => {
            true
        }
        _ => false,
    }
}

pub(super) fn compile(
    spec: &OnChangeSpec,
    ctx: &mut StepCtx<'_>,
    input: &ShapeRef,
    path: &str,
) -> Result<Compiled, Vec<ThadamError>> {
    let mut errs = Vec::new();
    let mut watches: Vec<Watch> = Vec::new();
    if spec.fields.is_empty() {
        errs.push(config_error(
            &format!("{path}.fields"),
            "`fields` needs at least one field",
        ));
    }
    for (i, w) in spec.fields.iter().enumerate() {
        let fpath = format!("{path}.fields[{i}]");
        let Some(idx) = input.index_of(&w.field) else {
            errs.push(config_error(&fpath, format!("unknown field `{}`", w.field)));
            continue;
        };
        if watches.iter().any(|x| x.idx == idx) {
            errs.push(config_error(
                &fpath,
                format!("field `{}` is listed twice", w.field),
            ));
            continue;
        }
        if !w.deadband.is_finite() || w.deadband < 0.0 {
            errs.push(config_error(
                &fpath,
                "`deadband` must be a finite number, zero or more",
            ));
            continue;
        }
        if w.deadband > 0.0 && input.field(idx).is_some_and(|d| !is_numeric_type(&d.ty)) {
            errs.push(config_error(
                &fpath,
                format!("`deadband` needs a numeric field; `{}` is not", w.field),
            ));
            continue;
        }
        watches.push(Watch {
            idx,
            deadband: w.deadband,
        });
    }
    let heartbeat_ns = match spec.heartbeat {
        Some(h) if h.0.is_zero() => {
            errs.push(config_error(
                &format!("{path}.heartbeat"),
                "`heartbeat` must be positive",
            ));
            None
        }
        Some(h) => Some(h.0.as_nanos() as i128),
        None => None,
    };
    if !errs.is_empty() {
        return Err(errs);
    }
    Ok(Compiled::new(
        OnChange {
            watches,
            heartbeat_ns,
            states: AssetMap::new(ctx.limits.max_state_keys),
        },
        input.clone(),
    ))
}

fn num_changed(a: f64, b: f64, deadband: f64) -> bool {
    if a.is_nan() || b.is_nan() {
        a.is_nan() != b.is_nan()
    } else {
        (a - b).abs() > deadband
    }
}

fn int_of(v: &Value) -> Option<i128> {
    Some(match v {
        Value::I8(x) => i128::from(*x),
        Value::I16(x) => i128::from(*x),
        Value::I32(x) => i128::from(*x),
        Value::I64(x) => i128::from(*x),
        Value::U8(x) => i128::from(*x),
        Value::U16(x) => i128::from(*x),
        Value::U32(x) => i128::from(*x),
        Value::U64(x) => i128::from(*x),
        _ => return None,
    })
}

fn int_slice_changed<T: Copy + Into<i128>>(a: &[T], b: &[T], deadband: f64) -> bool {
    a.len() != b.len()
        || a.iter()
            .zip(b)
            .any(|(x, y)| ((*x).into() - (*y).into()).unsigned_abs() as f64 > deadband)
}

/// Whether `cur` differs from the last kept `prev` by more than `deadband`.
/// `NaN` equals `NaN`; any other change of kind counts as a change.
fn value_changed(prev: &Value, cur: &Value, deadband: f64) -> bool {
    match (prev, cur) {
        (Value::F32s(a), Value::F32s(b)) => {
            a.len() != b.len()
                || a.iter()
                    .zip(b.iter())
                    .any(|(x, y)| num_changed(f64::from(*x), f64::from(*y), deadband))
        }
        (Value::F64s(a), Value::F64s(b)) => {
            a.len() != b.len()
                || a.iter()
                    .zip(b.iter())
                    .any(|(x, y)| num_changed(*x, *y, deadband))
        }
        (Value::I32s(a), Value::I32s(b)) => int_slice_changed(a, b, deadband),
        (Value::I64s(a), Value::I64s(b)) => int_slice_changed(a, b, deadband),
        _ => match (int_of(prev), int_of(cur)) {
            (Some(a), Some(b)) => (a - b).unsigned_abs() as f64 > deadband,
            _ => match (prev.as_f64(), cur.as_f64()) {
                (Some(a), Some(b)) => num_changed(a, b, deadband),
                _ => prev != cur,
            },
        },
    }
}

impl Step for OnChange {
    fn apply(&mut self, event: Event) -> StepResult {
        let Payload::Record(record) = event.payload() else {
            return StepResult::Reject(ThadamError::permanent(
                ErrorDomain::Transform,
                codes::TRANSFORM_FAILED,
                "event payload is not a record",
            ));
        };
        let now = event.times().ingest().as_nanos();
        let (state, is_new) = self.states.get_or_insert_with(event.asset(), || State {
            last: Vec::new(),
            last_kept_ns: now,
        });

        let mut keep = is_new;
        if !keep {
            for (watch, prev) in self.watches.iter().zip(&state.last) {
                match record.try_get_at(watch.idx) {
                    Some(cur) => {
                        if value_changed(prev, cur, watch.deadband) {
                            keep = true;
                            break;
                        }
                    }
                    None => {
                        return StepResult::Reject(ThadamError::permanent(
                            ErrorDomain::Transform,
                            codes::TRANSFORM_FAILED,
                            "event record does not match the step's shape",
                        ));
                    }
                }
            }
        }
        if !keep && let Some(beat) = self.heartbeat_ns {
            keep = i128::from(now) - i128::from(state.last_kept_ns) >= beat;
        }

        if keep {
            state.last.clear();
            for watch in &self.watches {
                state
                    .last
                    .push(record.try_get_at(watch.idx).cloned().unwrap_or(Value::Null));
            }
            state.last_kept_ns = now;
            StepResult::Keep(event)
        } else {
            if now < state.last_kept_ns {
                state.last_kept_ns = now;
            }
            StepResult::Filtered
        }
    }

    fn evicted(&self) -> u64 {
        self.states.evicted()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{event, shape, with_ctx, with_ctx_limits};
    use serde_json::json;
    use thadam_core::FieldType;

    fn s() -> ShapeRef {
        shape(&[
            ("mode", FieldType::Str),
            ("temp", FieldType::F64),
            ("arr", FieldType::List(Box::new(FieldType::F64))),
        ])
    }

    fn build(spec: serde_json::Value) -> Result<Box<dyn Step>, Vec<ThadamError>> {
        let spec: OnChangeSpec = serde_json::from_value(spec).unwrap();
        with_ctx(|ctx| compile(&spec, ctx, &s(), "transforms[0]").map(|c| c.step))
    }

    fn ev(mode: &str, temp: f64, robot: &str, secs: i64) -> Event {
        event(
            &s(),
            vec![
                Value::str(mode),
                Value::F64(temp),
                Value::F64s(vec![temp].into()),
            ],
            robot,
            secs,
        )
    }

    fn kept(step: &mut Box<dyn Step>, e: Event) -> bool {
        matches!(step.apply(e), StepResult::Keep(_))
    }

    #[test]
    fn keeps_first_and_changes_only() {
        let mut step = build(json!({ "fields": ["mode"] })).unwrap();
        assert!(kept(&mut step, ev("IDLE", 1.0, "r1", 0)));
        assert!(!kept(&mut step, ev("IDLE", 2.0, "r1", 1)));
        assert!(kept(&mut step, ev("RUNNING", 2.0, "r1", 2)));
        assert!(!kept(&mut step, ev("RUNNING", 3.0, "r1", 3)));
        assert!(
            kept(&mut step, ev("IDLE", 3.0, "r2", 3)),
            "state is per asset"
        );
    }

    #[test]
    fn deadband_compares_against_the_last_kept_value() {
        let mut step = build(json!({ "fields": [{ "field": "temp", "deadband": 1.0 }] })).unwrap();
        assert!(kept(&mut step, ev("a", 10.0, "r", 0)));
        assert!(!kept(&mut step, ev("a", 10.6, "r", 1)));
        assert!(!kept(&mut step, ev("a", 9.4, "r", 2)));
        assert!(
            kept(&mut step, ev("a", 11.1, "r", 3)),
            "moved 1.1 from the last kept 10.0"
        );
        assert!(
            !kept(&mut step, ev("a", 11.9, "r", 4)),
            "0.8 from the new baseline"
        );
    }

    #[test]
    fn deadband_applies_element_wise_to_arrays() {
        let mut step = build(json!({ "fields": [{ "field": "arr", "deadband": 0.5 }] })).unwrap();
        assert!(kept(&mut step, ev("a", 1.0, "r", 0)));
        assert!(!kept(&mut step, ev("a", 1.4, "r", 1)));
        assert!(kept(&mut step, ev("a", 1.6, "r", 2)));
    }

    #[test]
    fn heartbeat_keeps_an_unchanged_event_after_the_interval() {
        let mut step = build(json!({ "fields": ["mode"], "heartbeat": "5m" })).unwrap();
        assert!(kept(&mut step, ev("a", 0.0, "r", 0)));
        assert!(!kept(&mut step, ev("a", 0.0, "r", 299)));
        assert!(kept(&mut step, ev("a", 0.0, "r", 300)));
        assert!(!kept(&mut step, ev("a", 0.0, "r", 301)));
    }

    #[test]
    fn ingest_time_stepping_backwards_restarts_the_heartbeat() {
        let mut step = build(json!({ "fields": ["mode"], "heartbeat": "10s" })).unwrap();
        assert!(kept(&mut step, ev("a", 0.0, "r", 1_000)));
        assert!(
            !kept(&mut step, ev("a", 0.0, "r", 10)),
            "clock stepped back"
        );
        assert!(!kept(&mut step, ev("a", 0.0, "r", 19)));
        assert!(
            kept(&mut step, ev("a", 0.0, "r", 20)),
            "10 s after the new baseline"
        );
    }

    #[test]
    fn nan_equals_nan_and_nan_to_number_is_a_change() {
        let mut step = build(json!({ "fields": [{ "field": "temp", "deadband": 1.0 }] })).unwrap();
        assert!(kept(&mut step, ev("a", f64::NAN, "r", 0)));
        assert!(!kept(&mut step, ev("a", f64::NAN, "r", 1)));
        assert!(kept(&mut step, ev("a", 1.0, "r", 2)));
    }

    #[test]
    fn per_asset_state_is_bounded() {
        let spec: OnChangeSpec = serde_json::from_value(json!({ "fields": ["mode"] })).unwrap();
        let limits = crate::limits::ChainLimits {
            max_state_keys: 2,
            ..Default::default()
        };
        let mut compiled = with_ctx_limits(&limits, |ctx| {
            compile(&spec, ctx, &s(), "t").map(|c| c.step)
        })
        .unwrap();
        for i in 0..10 {
            assert!(kept(&mut compiled, ev("a", 0.0, &format!("r{i}"), 0)));
        }
        assert_eq!(compiled.evicted(), 8);
        assert!(
            kept(&mut compiled, ev("a", 0.0, "r0", 1)),
            "evicted, so kept again"
        );
    }

    #[test]
    fn config_errors() {
        assert!(build(json!({ "fields": [] })).is_err());
        assert!(build(json!({ "fields": ["nope"] })).is_err());
        assert!(build(json!({ "fields": ["mode", "mode"] })).is_err());
        assert!(build(json!({ "fields": [{ "field": "mode", "deadband": 1.0 }] })).is_err());
        assert!(build(json!({ "fields": [{ "field": "temp", "deadband": -1.0 }] })).is_err());
        assert!(build(json!({ "fields": ["mode"], "heartbeat": "0s" })).is_err());
    }
}
