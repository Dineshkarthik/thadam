//! `validate`: range rules that raise a quality flag and a status.
//!
//! `{field: f, min: a, max: b}` is `f >= a && f <= b`, and for a list field
//! every element must satisfy it. A null field is not checked, so a missing
//! optional value is never flagged. A `NaN` is out of range.

use thadam_core::{
    ErrorDomain, Event, FieldIdx, Payload, QualityFlags, QualityStatus, ShapeRef, Sym, ThadamError,
    codes,
};

use super::{Compiled, Step, StepCtx, StepResult, config_error};
use crate::predicate::{Range, compile_range};
use crate::spec::ValidateSpec;

#[derive(Debug)]
struct Rule {
    idx: FieldIdx,
    range: Range,
    flag: QualityFlags,
    status: QualityStatus,
    reason: Sym,
}

#[derive(Debug)]
struct Validate {
    rules: Vec<Rule>,
}

pub(super) fn parse_status(s: &str) -> Option<QualityStatus> {
    match s {
        "good" => Some(QualityStatus::Good),
        "uncertain" => Some(QualityStatus::Uncertain),
        "bad" => Some(QualityStatus::Bad),
        _ => None,
    }
}

pub(super) fn parse_flag(s: &str) -> Option<QualityFlags> {
    QualityFlags::from_name(s).filter(|f| f.bits().count_ones() == 1)
}

pub(super) fn compile(
    spec: &ValidateSpec,
    _ctx: &mut StepCtx<'_>,
    input: &ShapeRef,
    path: &str,
) -> Result<Compiled, Vec<ThadamError>> {
    let mut errs = Vec::new();
    let mut rules = Vec::new();
    if spec.rules.is_empty() {
        errs.push(config_error(
            &format!("{path}.rules"),
            "`rules` needs at least one rule",
        ));
    }
    for (i, r) in spec.rules.iter().enumerate() {
        let rpath = format!("{path}.rules[{i}]");
        let Some(idx) = input.index_of(&r.field) else {
            errs.push(config_error(
                &format!("{rpath}.field"),
                format!("unknown field `{}`", r.field),
            ));
            continue;
        };
        let Some(def) = input.field(idx) else {
            continue;
        };
        let range = match compile_range(&def.ty, r.min.as_ref(), r.max.as_ref()) {
            Ok(range) => range,
            Err(e) => {
                errs.push(config_error(&rpath, format!("field `{}`: {e}", r.field)));
                continue;
            }
        };
        let flag = match r.flag.as_deref() {
            None => QualityFlags::OUT_OF_RANGE,
            Some(name) => match parse_flag(name) {
                Some(f) => f,
                None => {
                    errs.push(config_error(
                        &format!("{rpath}.flag"),
                        format!("unknown quality flag `{name}`; use e.g. OUT_OF_RANGE"),
                    ));
                    continue;
                }
            },
        };
        let status = match r.status.as_deref() {
            None => QualityStatus::Bad,
            Some(name) => match parse_status(name) {
                Some(s) => s,
                None => {
                    errs.push(config_error(
                        &format!("{rpath}.status"),
                        format!("unknown status `{name}`; use good, uncertain, or bad"),
                    ));
                    continue;
                }
            },
        };
        rules.push(Rule {
            idx,
            range,
            flag,
            status,
            reason: Sym::from(format!("{} out of range", r.field)),
        });
    }
    if errs.is_empty() {
        Ok(Compiled::new(Validate { rules }, input.clone()))
    } else {
        Err(errs)
    }
}

impl Step for Validate {
    fn apply(&mut self, mut event: Event) -> StepResult {
        let mut flags = QualityFlags::empty();
        let mut status = QualityStatus::Good;
        let mut reason: Option<Sym> = None;
        {
            let Payload::Record(record) = event.payload() else {
                return StepResult::Reject(mismatch("event payload is not a record"));
            };
            for rule in &self.rules {
                let Some(value) = record.try_get_at(rule.idx) else {
                    return StepResult::Reject(mismatch(
                        "event record does not match the rule's shape",
                    ));
                };
                if rule.range.check(value) == Some(false) {
                    flags |= rule.flag;
                    status = status.max(rule.status);
                    reason.get_or_insert_with(|| rule.reason.clone());
                }
            }
        }
        if !flags.is_empty() {
            event.add_quality_flags(flags);
            if status > event.quality().status {
                event.set_quality_status(status);
            }
            if event.quality().reason.is_none() {
                event.set_quality_reason(reason);
            }
        }
        StepResult::Keep(event)
    }
}

fn mismatch(message: &str) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Transform, codes::TRANSFORM_FAILED, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{event, shape, with_ctx};
    use serde_json::json;
    use thadam_core::{FieldType, Quality, Value};

    fn build(spec: serde_json::Value, shape: &ShapeRef) -> Result<Box<dyn Step>, Vec<ThadamError>> {
        let spec: ValidateSpec = serde_json::from_value(spec).unwrap();
        with_ctx(|ctx| compile(&spec, ctx, shape, "transforms[0]").map(|c| c.step))
    }

    fn demo_shape() -> ShapeRef {
        shape(&[
            ("motor_temp_c", FieldType::List(Box::new(FieldType::F32))),
            ("soc", FieldType::F32),
        ])
    }

    fn demo_rule() -> serde_json::Value {
        json!({ "rules": [{ "field": "motor_temp_c", "min": -40, "max": 150,
                            "flag": "OUT_OF_RANGE", "status": "bad" }] })
    }

    fn kept(r: StepResult) -> Event {
        match r {
            StepResult::Keep(e) => e,
            _ => panic!("expected the event to be kept"),
        }
    }

    #[test]
    fn in_range_events_pass_untouched() {
        let s = demo_shape();
        let mut step = build(demo_rule(), &s).unwrap();
        let e = event(
            &s,
            vec![Value::F32s(vec![20.0f32, 149.0].into()), Value::F32(1.0)],
            "r",
            0,
        );
        let out = kept(step.apply(e.clone()));
        assert_eq!(out, e);
    }

    #[test]
    fn one_element_out_of_range_flags_the_event() {
        let s = demo_shape();
        let mut step = build(demo_rule(), &s).unwrap();
        let e = event(
            &s,
            vec![Value::F32s(vec![20.0f32, 151.0].into()), Value::F32(1.0)],
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(out.quality().flags, QualityFlags::OUT_OF_RANGE);
        assert_eq!(out.quality().status, QualityStatus::Bad);
        assert_eq!(
            out.quality().reason.as_deref(),
            Some("motor_temp_c out of range")
        );
    }

    #[test]
    fn nulls_are_not_checked_and_nan_is_out_of_range() {
        let s = demo_shape();
        let mut step = build(demo_rule(), &s).unwrap();
        let null = event(&s, vec![Value::Null, Value::F32(1.0)], "r", 0);
        assert!(kept(step.apply(null)).quality().flags.is_empty());
        let nan = event(
            &s,
            vec![Value::F32s(vec![f32::NAN].into()), Value::F32(1.0)],
            "r",
            0,
        );
        assert!(
            kept(step.apply(nan))
                .quality()
                .flags
                .contains(QualityFlags::OUT_OF_RANGE)
        );
    }

    #[test]
    fn status_only_escalates_and_flags_accumulate() {
        let s = demo_shape();
        let mut step = build(
            json!({ "rules": [
                { "field": "soc", "min": 0, "flag": "STALE", "status": "uncertain" },
                { "field": "motor_temp_c", "max": 10, "flag": "OUT_OF_RANGE", "status": "bad" } ] }),
            &s,
        )
        .unwrap();
        let e = event(
            &s,
            vec![Value::F32s(vec![50.0f32].into()), Value::F32(-1.0)],
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(
            out.quality().flags,
            QualityFlags::STALE | QualityFlags::OUT_OF_RANGE
        );
        assert_eq!(out.quality().status, QualityStatus::Bad);

        let mut mild = build(
            json!({ "rules": [{ "field": "soc", "min": 0, "flag": "STALE", "status": "uncertain" }] }),
            &s,
        )
        .unwrap();
        let already_bad = Quality {
            status: QualityStatus::Bad,
            ..Quality::good()
        };
        let e = crate::testutil::event_with_quality(
            &s,
            vec![Value::Null, Value::F32(-1.0)],
            "r",
            0,
            already_bad,
        );
        assert_eq!(kept(mild.apply(e)).quality().status, QualityStatus::Bad);
    }

    #[test]
    fn compile_errors_name_the_rule() {
        let s = demo_shape();
        let err = |v| build(v, &s).unwrap_err();
        let e = err(json!({ "rules": [{ "field": "nope", "min": 1 }] }));
        assert_eq!(
            e[0].context.get("path"),
            Some("transforms[0].rules[0].field")
        );
        assert!(
            err(json!({ "rules": [] }))[0]
                .message
                .contains("at least one")
        );
        assert!(
            err(json!({ "rules": [{ "field": "soc" }] }))[0]
                .message
                .contains("min")
        );
        assert!(
            err(json!({ "rules": [{ "field": "soc", "min": 1, "flag": "NOPE" }] }))[0]
                .message
                .contains("unknown quality flag")
        );
        assert!(
            err(
                json!({ "rules": [{ "field": "soc", "min": 1, "flag": "STALE", "status": "meh" }] })
            )[0]
            .message
            .contains("unknown status")
        );
    }
}
