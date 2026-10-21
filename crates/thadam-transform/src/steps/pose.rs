//! `pose`: flags malformed flattened poses.
//!
//! Reads `<prefix>.position.{x,y,z}` and `<prefix>.orientation.{x,y,z,w}`,
//! each an f32 or f64 field. A null or non-numeric component raises `INVALID`
//! with reason `pose_missing_field`; a non-finite one raises `INVALID` with
//! `pose_not_finite`. A quaternion whose norm is farther than the tolerance
//! from 1 raises `OUT_OF_RANGE` with `quaternion_not_unit`; the norm is only
//! computed when all four components are finite numbers. When a frame field
//! is configured, a null, non-string, or empty frame id raises `INVALID`
//! with `missing_frame_id`. Events are flagged, not dropped: quality is data.

use thadam_core::{
    ErrorDomain, Event, FieldIdx, FieldType, Payload, QualityFlags, QualityStatus, ShapeRef, Sym,
    ThadamError, codes,
};

use super::{Compiled, Step, StepCtx, StepResult, config_error};
use crate::spec::PoseSpec;

#[derive(Debug)]
struct Pose {
    position: [FieldIdx; 3],
    orientation: [FieldIdx; 4],
    frame: Option<FieldIdx>,
    tolerance: f64,
}

fn raise(
    flags: &mut QualityFlags,
    reason: &mut Option<&'static str>,
    flag: QualityFlags,
    why: &'static str,
) {
    *flags |= flag;
    reason.get_or_insert(why);
}

fn numeric_component(
    input: &ShapeRef,
    name: &str,
    path: &str,
    errs: &mut Vec<ThadamError>,
) -> Option<FieldIdx> {
    let Some(idx) = input.index_of(name) else {
        errs.push(config_error(path, format!("unknown field `{name}`")));
        return None;
    };
    if !matches!(
        input.field(idx).map(|d| &d.ty),
        Some(FieldType::F32 | FieldType::F64)
    ) {
        errs.push(config_error(
            path,
            format!("field `{name}` must be f32 or f64"),
        ));
        return None;
    }
    Some(idx)
}

pub(super) fn compile(
    spec: &PoseSpec,
    _ctx: &mut StepCtx<'_>,
    input: &ShapeRef,
    path: &str,
) -> Result<Compiled, Vec<ThadamError>> {
    let mut errs = Vec::new();
    if spec.prefix.is_empty() {
        errs.push(config_error(
            &format!("{path}.prefix"),
            "`prefix` must not be empty",
        ));
    }
    if !(spec.quaternion_tolerance > 0.0 && spec.quaternion_tolerance < 1.0) {
        errs.push(config_error(
            &format!("{path}.quaternion_tolerance"),
            "`quaternion_tolerance` must be greater than 0 and less than 1",
        ));
    }
    let ppath = format!("{path}.prefix");
    let mut position: [Option<FieldIdx>; 3] = [None; 3];
    let mut orientation: [Option<FieldIdx>; 4] = [None; 4];
    for (slot, axis) in position.iter_mut().zip(["x", "y", "z"]) {
        *slot = numeric_component(
            input,
            &format!("{}.position.{axis}", spec.prefix),
            &ppath,
            &mut errs,
        );
    }
    for (slot, axis) in orientation.iter_mut().zip(["x", "y", "z", "w"]) {
        *slot = numeric_component(
            input,
            &format!("{}.orientation.{axis}", spec.prefix),
            &ppath,
            &mut errs,
        );
    }
    let frame = match spec.frame_field.as_deref() {
        None => None,
        Some(name) => {
            let fpath = format!("{path}.frame_field");
            if name.is_empty() {
                errs.push(config_error(&fpath, "`frame_field` must not be empty"));
                None
            } else {
                match input.index_of(name) {
                    None => {
                        errs.push(config_error(&fpath, format!("unknown field `{name}`")));
                        None
                    }
                    Some(idx) => {
                        if !matches!(input.field(idx).map(|d| &d.ty), Some(FieldType::Str)) {
                            errs.push(config_error(
                                &fpath,
                                format!("field `{name}` must be a string"),
                            ));
                            None
                        } else {
                            Some(idx)
                        }
                    }
                }
            }
        }
    };
    // Every error above pairs with a `None` slot, so complete slots mean the
    // error list is empty.
    let (Some(px), Some(py), Some(pz)) = (position[0], position[1], position[2]) else {
        return Err(errs);
    };
    let (Some(ox), Some(oy), Some(oz), Some(ow)) = (
        orientation[0],
        orientation[1],
        orientation[2],
        orientation[3],
    ) else {
        return Err(errs);
    };
    if !errs.is_empty() {
        return Err(errs);
    }
    Ok(Compiled::new(
        Pose {
            position: [px, py, pz],
            orientation: [ox, oy, oz, ow],
            frame,
            tolerance: spec.quaternion_tolerance,
        },
        input.clone(),
    ))
}

impl Step for Pose {
    fn apply(&mut self, mut event: Event) -> StepResult {
        let mut flags = QualityFlags::empty();
        let mut reason: Option<&'static str> = None;
        {
            let Payload::Record(record) = event.payload() else {
                return StepResult::Reject(mismatch("event payload is not a record"));
            };
            let mut missing = false;
            let mut not_finite = false;
            for &idx in &self.position {
                let Some(value) = record.try_get_at(idx) else {
                    return StepResult::Reject(mismatch(
                        "event record does not match the step's shape",
                    ));
                };
                match value.as_f64() {
                    None => missing = true,
                    Some(v) if !v.is_finite() => not_finite = true,
                    Some(_) => {}
                }
            }
            let mut orientation = [0.0f64; 4];
            let mut orientation_ok = true;
            for (slot, &idx) in orientation.iter_mut().zip(&self.orientation) {
                let Some(value) = record.try_get_at(idx) else {
                    return StepResult::Reject(mismatch(
                        "event record does not match the step's shape",
                    ));
                };
                match value.as_f64() {
                    None => {
                        missing = true;
                        orientation_ok = false;
                    }
                    Some(v) if !v.is_finite() => {
                        not_finite = true;
                        orientation_ok = false;
                    }
                    Some(v) => *slot = v,
                }
            }
            if missing {
                raise(
                    &mut flags,
                    &mut reason,
                    QualityFlags::INVALID,
                    "pose_missing_field",
                );
            }
            if not_finite {
                raise(
                    &mut flags,
                    &mut reason,
                    QualityFlags::INVALID,
                    "pose_not_finite",
                );
            }
            if orientation_ok {
                let norm = orientation.iter().map(|v| v * v).sum::<f64>().sqrt();
                if (norm - 1.0).abs() > self.tolerance {
                    raise(
                        &mut flags,
                        &mut reason,
                        QualityFlags::OUT_OF_RANGE,
                        "quaternion_not_unit",
                    );
                }
            }
            if let Some(idx) = self.frame {
                let Some(value) = record.try_get_at(idx) else {
                    return StepResult::Reject(mismatch(
                        "event record does not match the step's shape",
                    ));
                };
                if value.as_str().is_none_or(str::is_empty) {
                    raise(
                        &mut flags,
                        &mut reason,
                        QualityFlags::INVALID,
                        "missing_frame_id",
                    );
                }
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
        let spec: PoseSpec = serde_json::from_value(spec).unwrap();
        with_ctx(|ctx| compile(&spec, ctx, shape, "transforms[0]").map(|c| c.step))
    }

    /// The shape of `geometry_msgs/PoseStamped` flattened by the ROS 2 decoder.
    fn pose_stamped_shape() -> ShapeRef {
        shape(&[
            ("header.stamp", FieldType::Ts),
            ("header.frame_id", FieldType::Str),
            ("pose.position.x", FieldType::F64),
            ("pose.position.y", FieldType::F64),
            ("pose.position.z", FieldType::F64),
            ("pose.orientation.x", FieldType::F64),
            ("pose.orientation.y", FieldType::F64),
            ("pose.orientation.z", FieldType::F64),
            ("pose.orientation.w", FieldType::F64),
        ])
    }

    fn pose_values(position: [Value; 3], orientation: [f64; 4], frame_id: Value) -> Vec<Value> {
        let mut values = vec![
            Value::Ts(Timestamp::from_nanos(1_700_000_000_000_000_000)),
            frame_id,
        ];
        values.extend(position);
        values.extend(orientation.map(Value::F64));
        values
    }

    fn identity_orientation() -> [f64; 4] {
        [0.0, 0.0, 0.0, 1.0]
    }

    fn positions() -> [Value; 3] {
        [Value::F64(1.0), Value::F64(2.0), Value::F64(3.0)]
    }

    /// A position whose x component is `v` and whose y and z are 0.0.
    fn at_x(v: Value) -> [Value; 3] {
        [v, Value::F64(0.0), Value::F64(0.0)]
    }

    fn kept(r: StepResult) -> Event {
        match r {
            StepResult::Keep(e) => e,
            _ => panic!("expected the event to be kept"),
        }
    }

    #[test]
    fn a_ros_pose_stamped_record_passes_untouched() {
        let s = pose_stamped_shape();
        let mut step = build(json!({}), &s).unwrap();
        let e = event(
            &s,
            pose_values(
                positions(),
                identity_orientation(),
                Value::from("base_link"),
            ),
            "r",
            0,
        );
        let out = kept(step.apply(e.clone()));
        assert_eq!(out, e);
    }

    #[test]
    fn f32_components_are_accepted() {
        let s = shape(&[
            ("pose.position.x", FieldType::F32),
            ("pose.position.y", FieldType::F32),
            ("pose.position.z", FieldType::F32),
            ("pose.orientation.x", FieldType::F32),
            ("pose.orientation.y", FieldType::F32),
            ("pose.orientation.z", FieldType::F32),
            ("pose.orientation.w", FieldType::F64),
        ]);
        let mut step = build(json!({ "frame_field": null }), &s).unwrap();
        let e = event(
            &s,
            vec![
                Value::F32(0.5),
                Value::F32(-1.5),
                Value::F32(3.25),
                Value::F32(0.0),
                Value::F32(0.0),
                Value::F32(0.0),
                Value::F64(1.0),
            ],
            "r",
            0,
        );
        assert!(kept(step.apply(e)).quality().flags.is_empty());
    }

    #[test]
    fn a_null_component_is_a_missing_field() {
        let s = pose_stamped_shape();
        let mut step = build(json!({}), &s).unwrap();
        let e = event(
            &s,
            pose_values(
                at_x(Value::Null),
                identity_orientation(),
                Value::from("odom"),
            ),
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(out.quality().flags, QualityFlags::INVALID);
        assert_eq!(out.quality().status, QualityStatus::Bad);
        assert_eq!(out.quality().reason.as_deref(), Some("pose_missing_field"));
    }

    #[test]
    fn a_non_finite_component_is_flagged() {
        let s = pose_stamped_shape();
        let mut step = build(json!({}), &s).unwrap();
        let e = event(
            &s,
            pose_values(
                [
                    Value::F64(f64::NEG_INFINITY),
                    Value::F64(0.0),
                    Value::F64(0.0),
                ],
                identity_orientation(),
                Value::from("odom"),
            ),
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(out.quality().flags, QualityFlags::INVALID);
        assert_eq!(out.quality().reason.as_deref(), Some("pose_not_finite"));

        let nan_orientation = event(
            &s,
            pose_values(positions(), [0.0, f64::NAN, 0.0, 1.0], Value::from("odom")),
            "r",
            0,
        );
        let out = kept(step.apply(nan_orientation));
        assert_eq!(out.quality().flags, QualityFlags::INVALID);
        assert_eq!(out.quality().reason.as_deref(), Some("pose_not_finite"));
    }

    #[test]
    fn a_non_unit_quaternion_is_out_of_range() {
        let s = pose_stamped_shape();
        let mut step = build(json!({}), &s).unwrap();
        let e = event(
            &s,
            pose_values(positions(), [0.0, 0.0, 0.0, 2.0], Value::from("odom")),
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(out.quality().flags, QualityFlags::OUT_OF_RANGE);
        assert_eq!(out.quality().status, QualityStatus::Bad);
        assert_eq!(out.quality().reason.as_deref(), Some("quaternion_not_unit"));

        let within_tolerance = event(
            &s,
            pose_values(positions(), [0.0, 0.0, 0.0, 1.0005], Value::from("odom")),
            "r",
            0,
        );
        assert!(
            kept(step.apply(within_tolerance))
                .quality()
                .flags
                .is_empty()
        );
    }

    #[test]
    fn the_norm_is_not_computed_when_a_component_cannot_be_read() {
        let s = pose_stamped_shape();
        let mut step = build(json!({}), &s).unwrap();
        let e = event(
            &s,
            pose_values(
                positions(),
                [f64::INFINITY, 9.0, 9.0, 9.0],
                Value::from("odom"),
            ),
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(out.quality().flags, QualityFlags::INVALID);
        assert_eq!(out.quality().reason.as_deref(), Some("pose_not_finite"));
    }

    #[test]
    fn an_empty_or_missing_frame_id_is_invalid() {
        let s = pose_stamped_shape();
        let mut step = build(json!({}), &s).unwrap();
        for frame_id in [Value::from(""), Value::Null] {
            let e = event(
                &s,
                pose_values(positions(), identity_orientation(), frame_id),
                "r",
                0,
            );
            let out = kept(step.apply(e));
            assert_eq!(out.quality().flags, QualityFlags::INVALID);
            assert_eq!(out.quality().reason.as_deref(), Some("missing_frame_id"));
        }
    }

    #[test]
    fn a_null_frame_field_config_skips_the_frame_check() {
        let s = pose_stamped_shape();
        let mut step = build(json!({ "frame_field": null }), &s).unwrap();
        let e = event(
            &s,
            pose_values(positions(), identity_orientation(), Value::Null),
            "r",
            0,
        );
        assert!(kept(step.apply(e)).quality().flags.is_empty());
    }

    #[test]
    fn flags_accumulate_and_an_existing_reason_is_kept() {
        let s = pose_stamped_shape();
        let mut step = build(json!({}), &s).unwrap();
        let e = event(
            &s,
            pose_values(at_x(Value::Null), [0.0, 0.0, 0.0, 2.0], Value::from("odom")),
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(
            out.quality().flags,
            QualityFlags::INVALID | QualityFlags::OUT_OF_RANGE
        );
        assert_eq!(out.quality().reason.as_deref(), Some("pose_missing_field"));

        let already = Quality {
            status: QualityStatus::Bad,
            flags: QualityFlags::STALE,
            reason: Some(Sym::from("upstream")),
        };
        let e = crate::testutil::event_with_quality(
            &s,
            pose_values(
                at_x(Value::Null),
                identity_orientation(),
                Value::from("odom"),
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
    fn a_custom_prefix_and_frame_field_are_read() {
        let s = shape(&[
            ("tf.parent", FieldType::Str),
            ("base_pose.position.x", FieldType::F32),
            ("base_pose.position.y", FieldType::F32),
            ("base_pose.position.z", FieldType::F32),
            ("base_pose.orientation.x", FieldType::F64),
            ("base_pose.orientation.y", FieldType::F64),
            ("base_pose.orientation.z", FieldType::F64),
            ("base_pose.orientation.w", FieldType::F64),
        ]);
        let mut step = build(
            json!({ "prefix": "base_pose", "frame_field": "tf.parent" }),
            &s,
        )
        .unwrap();
        let e = event(
            &s,
            vec![
                Value::from(""),
                Value::F32(0.0),
                Value::F32(0.0),
                Value::F32(0.0),
                Value::F64(0.0),
                Value::F64(0.0),
                Value::F64(0.0),
                Value::F64(1.0),
            ],
            "r",
            0,
        );
        let out = kept(step.apply(e));
        assert_eq!(out.quality().reason.as_deref(), Some("missing_frame_id"));
    }

    #[test]
    fn compile_errors_name_the_offending_key() {
        let s = pose_stamped_shape();
        let err = |v| build(v, &s).unwrap_err();
        let e = err(json!({ "prefix": "twist" }));
        assert_eq!(e.len(), 7);
        assert!(
            e.iter()
                .all(|e| e.context.get("path") == Some("transforms[0].prefix"))
        );
        assert!(e[0].message.contains("unknown field `twist.position.x`"));

        let e = err(json!({ "frame_field": "pose.position.x" }));
        assert!(e[0].message.contains("must be a string"));
        assert_eq!(e[0].context.get("path"), Some("transforms[0].frame_field"));

        let e = err(json!({ "frame_field": "gps.frame_id" }));
        assert!(e[0].message.contains("unknown field `gps.frame_id`"));

        for tol in [0.0, 1.0, -0.5, 2.0] {
            let e = err(json!({ "quaternion_tolerance": tol }));
            assert_eq!(
                e[0].context.get("path"),
                Some("transforms[0].quaternion_tolerance")
            );
            assert!(e[0].message.contains("greater than 0 and less than 1"));
        }

        let e = err(json!({ "prefix": "" }));
        assert!(e[0].message.contains("must not be empty"));
        let e = err(json!({ "frame_field": "" }));
        assert!(e[0].message.contains("must not be empty"));
    }
}
