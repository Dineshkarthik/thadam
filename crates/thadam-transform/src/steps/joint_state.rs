//! `joint_state`: flags malformed joint-state arrays.
//!
//! Follows ROS `sensor_msgs/JointState` semantics: the position, velocity,
//! and effort arrays must each be empty or as long as the joint-name list,
//! names must not repeat, and every value must be finite; a null element in
//! a generic list counts as not finite. A null array field reads as an empty
//! array. A violation raises `INVALID` with reason `joint_state_length_mismatch`,
//! `duplicate_joint_name`, or `joint_state_not_finite` (first violation wins
//! the reason). Typed float arrays (`F64s`, `F32s`) and generic lists of
//! floats are accepted. Events are flagged, not dropped: quality is data.

use std::collections::HashSet;

use thadam_core::{
    ErrorDomain, Event, FieldIdx, FieldType, Payload, QualityFlags, QualityStatus, ShapeRef, Sym,
    ThadamError, Value, codes,
};

use super::{Compiled, Step, StepCtx, StepResult, config_error};
use crate::spec::JointStateSpec;

#[derive(Debug)]
struct JointState {
    name: FieldIdx,
    position: FieldIdx,
    velocity: FieldIdx,
    effort: FieldIdx,
}

fn raise(flags: &mut QualityFlags, reason: &mut Option<&'static str>, why: &'static str) {
    *flags |= QualityFlags::INVALID;
    reason.get_or_insert(why);
}

/// The names as strings, or `None` for a variant the declared `list<str>`
/// rules out (`Null` reads as an empty list).
fn name_list(value: &Value) -> Option<Vec<Option<&str>>> {
    match value {
        Value::Null => Some(Vec::new()),
        Value::List(items) => Some(items.iter().map(Value::as_str).collect()),
        _ => None,
    }
}

/// The array as floats, or `None` for a variant the declared list-of-float
/// rules out (`Null` reads as an empty array; a null list item stays `None`).
fn float_array(value: &Value) -> Option<Vec<Option<f64>>> {
    match value {
        Value::Null => Some(Vec::new()),
        Value::F32s(a) => Some(a.iter().map(|v| Some(f64::from(*v))).collect()),
        Value::F64s(a) => Some(a.iter().map(|v| Some(*v)).collect()),
        Value::List(items) => Some(items.iter().map(Value::as_f64).collect()),
        _ => None,
    }
}

fn resolve(
    input: &ShapeRef,
    key: &str,
    name: &str,
    floats: bool,
    path: &str,
    errs: &mut Vec<ThadamError>,
) -> Option<FieldIdx> {
    let fpath = format!("{path}.{key}");
    if name.is_empty() {
        errs.push(config_error(&fpath, format!("`{key}` must not be empty")));
        return None;
    }
    let Some(idx) = input.index_of(name) else {
        errs.push(config_error(&fpath, format!("unknown field `{name}`")));
        return None;
    };
    let ok = match (floats, input.field(idx).map(|d| &d.ty)) {
        (true, Some(FieldType::List(inner))) => matches!(**inner, FieldType::F32 | FieldType::F64),
        (false, Some(FieldType::List(inner))) => matches!(**inner, FieldType::Str),
        _ => false,
    };
    if !ok {
        let want = if floats {
            "list<f32> or list<f64>"
        } else {
            "list<str>"
        };
        errs.push(config_error(
            &fpath,
            format!("field `{name}` must be {want}"),
        ));
        return None;
    }
    Some(idx)
}

pub(super) fn compile(
    spec: &JointStateSpec,
    _ctx: &mut StepCtx<'_>,
    input: &ShapeRef,
    path: &str,
) -> Result<Compiled, Vec<ThadamError>> {
    let mut errs = Vec::new();
    let name = resolve(
        input,
        "name_field",
        &spec.name_field,
        false,
        path,
        &mut errs,
    );
    let position = resolve(
        input,
        "position_field",
        &spec.position_field,
        true,
        path,
        &mut errs,
    );
    let velocity = resolve(
        input,
        "velocity_field",
        &spec.velocity_field,
        true,
        path,
        &mut errs,
    );
    let effort = resolve(
        input,
        "effort_field",
        &spec.effort_field,
        true,
        path,
        &mut errs,
    );
    // Every error above pairs with a `None`, so a complete tuple means the
    // error list is empty.
    let (Some(name), Some(position), Some(velocity), Some(effort)) =
        (name, position, velocity, effort)
    else {
        return Err(errs);
    };
    Ok(Compiled::new(
        JointState {
            name,
            position,
            velocity,
            effort,
        },
        input.clone(),
    ))
}

impl Step for JointState {
    fn apply(&mut self, mut event: Event) -> StepResult {
        let mut flags = QualityFlags::empty();
        let mut reason: Option<&'static str> = None;
        {
            let Payload::Record(record) = event.payload() else {
                return StepResult::Reject(mismatch("event payload is not a record"));
            };
            let array = |idx: FieldIdx| record.try_get_at(idx).and_then(float_array);
            let (Some(names), Some(position), Some(velocity), Some(effort)) = (
                record.try_get_at(self.name).and_then(name_list),
                array(self.position),
                array(self.velocity),
                array(self.effort),
            ) else {
                return StepResult::Reject(mismatch(
                    "event record does not match the step's shape",
                ));
            };

            let mismatch = [&position, &velocity, &effort]
                .iter()
                .any(|a| !a.is_empty() && a.len() != names.len());
            if mismatch {
                raise(&mut flags, &mut reason, "joint_state_length_mismatch");
            }

            let mut seen = HashSet::new();
            if names.iter().flatten().any(|n| !seen.insert(n)) {
                raise(&mut flags, &mut reason, "duplicate_joint_name");
            }

            let not_finite = [&position, &velocity, &effort]
                .iter()
                .flat_map(|a| a.iter())
                .any(|v| v.is_none_or(|f| !f.is_finite()));
            if not_finite {
                raise(&mut flags, &mut reason, "joint_state_not_finite");
            }
        }
        if !flags.is_empty() {
            event.add_quality_flags(flags);
            event.set_quality_status(QualityStatus::Bad);
            if event.quality().reason.is_none() {
                event.set_quality_reason(reason.map(Sym::from));
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
    use thadam_core::{FieldType, Quality, Timestamp, Value};

    fn build(spec: serde_json::Value, shape: &ShapeRef) -> Result<Box<dyn Step>, Vec<ThadamError>> {
        let spec: JointStateSpec = serde_json::from_value(spec).unwrap();
        with_ctx(|ctx| compile(&spec, ctx, shape, "transforms[0]").map(|c| c.step))
    }

    /// The shape of `sensor_msgs/JointState` flattened by the ROS 2 decoder.
    fn joint_state_shape() -> ShapeRef {
        shape(&[
            ("header.stamp", FieldType::Ts),
            ("header.frame_id", FieldType::Str),
            ("name", FieldType::List(Box::new(FieldType::Str))),
            ("position", FieldType::List(Box::new(FieldType::F64))),
            ("velocity", FieldType::List(Box::new(FieldType::F64))),
            ("effort", FieldType::List(Box::new(FieldType::F64))),
        ])
    }

    fn joint_values(names: Value, position: Value, velocity: Value, effort: Value) -> Vec<Value> {
        vec![
            Value::Ts(Timestamp::from_nanos(1_700_000_000_000_000_000)),
            Value::from("base_link"),
            names,
            position,
            velocity,
            effort,
        ]
    }

    fn names(items: &[&str]) -> Value {
        Value::List(items.iter().map(|n| Value::from(*n)).collect())
    }

    fn kept(r: StepResult) -> Event {
        match r {
            StepResult::Keep(e) => e,
            _ => panic!("expected the event to be kept"),
        }
    }

    #[test]
    fn a_ros_joint_state_record_passes_untouched() {
        let s = joint_state_shape();
        let mut step = build(json!({}), &s).unwrap();
        let e = event(
            &s,
            joint_values(
                names(&["shoulder", "elbow"]),
                Value::F64s(vec![0.1, -0.2].into()),
                Value::F64s(vec![].into()),
                Value::F64s(vec![1.0, 2.0].into()),
            ),
            "r",
            0,
        );
        let out = kept(step.apply(e.clone()));
        assert_eq!(out, e);
    }

    #[test]
    fn typed_f32_arrays_and_generic_lists_are_accepted() {
        let s = shape(&[
            ("name", FieldType::List(Box::new(FieldType::Str))),
            ("position", FieldType::List(Box::new(FieldType::F32))),
            ("velocity", FieldType::List(Box::new(FieldType::F64))),
            ("effort", FieldType::List(Box::new(FieldType::F64))),
        ]);
        let mut step = build(json!({}), &s).unwrap();
        let e = event(
            &s,
            vec![
                names(&["j1"]),
                Value::F32s(vec![1.5f32].into()),
                Value::List(vec![Value::F64(0.25)]),
                Value::Null,
            ],
            "r",
            0,
        );
        assert!(kept(step.apply(e)).quality().flags.is_empty());
    }

    #[test]
    fn a_wrong_sized_array_is_a_length_mismatch() {
        let s = joint_state_shape();
        let mut step = build(json!({}), &s).unwrap();
        let e = event(
            &s,
            joint_values(
                names(&["a", "b"]),
                Value::F64s(vec![0.1, 0.2, 0.3].into()),
                Value::Null,
                Value::Null,
            ),
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(out.quality().flags, QualityFlags::INVALID);
        assert_eq!(out.quality().status, QualityStatus::Bad);
        assert_eq!(
            out.quality().reason.as_deref(),
            Some("joint_state_length_mismatch")
        );

        // No names: any non-empty array mismatches.
        let e = event(
            &s,
            joint_values(
                Value::Null,
                Value::Null,
                Value::F64s(vec![1.0].into()),
                Value::Null,
            ),
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(
            out.quality().reason.as_deref(),
            Some("joint_state_length_mismatch")
        );
    }

    #[test]
    fn duplicate_names_are_flagged() {
        let s = joint_state_shape();
        let mut step = build(json!({}), &s).unwrap();
        let e = event(
            &s,
            joint_values(
                Value::List(vec![Value::from("a"), Value::from("b"), Value::from("a")]),
                Value::F64s(vec![0.1, 0.2, 0.3].into()),
                Value::Null,
                Value::Null,
            ),
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(out.quality().flags, QualityFlags::INVALID);
        assert_eq!(
            out.quality().reason.as_deref(),
            Some("duplicate_joint_name")
        );
    }

    #[test]
    fn non_finite_values_are_flagged() {
        let s = joint_state_shape();
        let mut step = build(json!({}), &s).unwrap();
        for bad in [f64::NAN, f64::INFINITY] {
            let e = event(
                &s,
                joint_values(
                    names(&["a", "b"]),
                    Value::F64s(vec![0.1, bad].into()),
                    Value::Null,
                    Value::Null,
                ),
                "r",
                0,
            );
            let out = kept(step.apply(e));
            assert_eq!(out.quality().flags, QualityFlags::INVALID);
            assert_eq!(
                out.quality().reason.as_deref(),
                Some("joint_state_not_finite")
            );
        }

        // A null element in a generic list counts as not finite.
        let e = event(
            &s,
            joint_values(
                names(&["a", "b"]),
                Value::List(vec![Value::F64(0.1), Value::Null]),
                Value::Null,
                Value::Null,
            ),
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(
            out.quality().reason.as_deref(),
            Some("joint_state_not_finite")
        );
    }

    #[test]
    fn the_reason_is_the_first_violation_and_an_existing_one_is_kept() {
        let s = joint_state_shape();
        let mut step = build(json!({}), &s).unwrap();
        let e = event(
            &s,
            joint_values(
                Value::List(vec![Value::from("a"), Value::from("a")]),
                Value::F64s(vec![0.1, f64::NAN, 0.3].into()),
                Value::Null,
                Value::Null,
            ),
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(out.quality().flags, QualityFlags::INVALID);
        assert_eq!(
            out.quality().reason.as_deref(),
            Some("joint_state_length_mismatch")
        );

        let already = Quality {
            status: QualityStatus::Bad,
            flags: QualityFlags::STALE,
            reason: Some(Sym::from("upstream")),
        };
        let e = crate::testutil::event_with_quality(
            &s,
            joint_values(
                Value::List(vec![Value::from("a"), Value::from("a")]),
                Value::Null,
                Value::Null,
                Value::Null,
            ),
            "r",
            0,
            already,
        );
        let out = kept(step.apply(e));
        assert_eq!(
            out.quality().flags,
            QualityFlags::STALE | QualityFlags::INVALID
        );
        assert_eq!(out.quality().reason.as_deref(), Some("upstream"));
    }

    #[test]
    fn custom_field_names_are_read() {
        let s = shape(&[
            ("joints", FieldType::List(Box::new(FieldType::Str))),
            ("q", FieldType::List(Box::new(FieldType::F64))),
            ("qd", FieldType::List(Box::new(FieldType::F64))),
            ("tau", FieldType::List(Box::new(FieldType::F64))),
        ]);
        let mut step = build(
            json!({ "name_field": "joints", "position_field": "q",
                    "velocity_field": "qd", "effort_field": "tau" }),
            &s,
        )
        .unwrap();
        let e = event(
            &s,
            vec![
                Value::List(vec![Value::from("a")]),
                Value::F64s(vec![0.1, 0.2].into()),
                Value::Null,
                Value::Null,
            ],
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(
            out.quality().reason.as_deref(),
            Some("joint_state_length_mismatch")
        );
    }

    #[test]
    fn compile_errors_name_the_offending_key() {
        let s = joint_state_shape();
        let err = |v| build(v, &s).unwrap_err();

        let e = err(json!({ "name_field": "position" }));
        assert!(e[0].message.contains("list<str>"));
        assert_eq!(e[0].context.get("path"), Some("transforms[0].name_field"));

        let e = err(json!({ "position_field": "name" }));
        assert!(e[0].message.contains("list<f32> or list<f64>"));

        let i32s = shape(&[
            ("name", FieldType::List(Box::new(FieldType::Str))),
            ("position", FieldType::List(Box::new(FieldType::I32))),
            ("velocity", FieldType::List(Box::new(FieldType::F64))),
            ("effort", FieldType::List(Box::new(FieldType::F64))),
        ]);
        let e = build(json!({}), &i32s).unwrap_err();
        assert!(e[0].message.contains("list<f32> or list<f64>"));

        let e = err(json!({ "velocity_field": "spd" }));
        assert!(e[0].message.contains("unknown field `spd`"));
        assert_eq!(
            e[0].context.get("path"),
            Some("transforms[0].velocity_field")
        );

        assert!(
            err(json!({ "name_field": "" }))[0]
                .message
                .contains("must not be empty")
        );
        assert!(
            err(json!({ "effort_field": "" }))[0]
                .message
                .contains("must not be empty")
        );
    }
}
