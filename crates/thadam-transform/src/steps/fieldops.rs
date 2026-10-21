//! `rename`, `project`, and `cast`: field operations that derive a new shape
//! once, at compile time, and remap each record in O(fields).

use std::collections::HashSet;

use thadam_core::{
    ErrorDomain, Event, FieldDef, FieldIdx, FieldType, Payload, Record, Shape, ShapeRef,
    ThadamError, Value, ValueType, codes,
};

use super::{Compiled, Step, StepCtx, StepResult, config_error, intern};
use crate::spec::{CastSpec, ProjectSpec, RenameSpec, parse_field_type};

pub(super) fn step_failed(message: &str) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Transform, codes::TRANSFORM_FAILED, message)
}

/// Rebuilds the event with a record of `out` whose values come from `f`. The
/// event's other fields carry over unchanged.
pub(super) fn rebuild(
    event: Event,
    out: &ShapeRef,
    f: impl FnOnce(Vec<Value>) -> Result<Vec<Value>, ThadamError>,
) -> Result<Event, ThadamError> {
    let builder = event.to_builder();
    let Payload::Record(record) = event.into_payload() else {
        return Err(step_failed("event payload is not a record"));
    };
    let values = f(record.into_values())?;
    let record = Record::new(out.clone(), values)
        .map_err(|e| step_failed(&format!("derived record is invalid: {e}")))?;
    builder
        .payload(record)
        .build()
        .map_err(|e| step_failed(&e.to_string()))
}

#[derive(Debug)]
struct Rename {
    out: ShapeRef,
}

pub(super) fn compile_rename(
    spec: &RenameSpec,
    ctx: &mut StepCtx<'_>,
    input: &ShapeRef,
    path: &str,
) -> Result<Compiled, Vec<ThadamError>> {
    let mut errs = Vec::new();
    if spec.fields.is_empty() {
        errs.push(config_error(
            &format!("{path}.fields"),
            "`fields` needs at least one entry",
        ));
    }
    let mut defs: Vec<FieldDef> = input.fields().to_vec();
    for (old, new) in &spec.fields {
        let fpath = format!("{path}.fields.{old}");
        match input.index_of(old) {
            Some(i) => {
                if let Some(def) = defs.get_mut(i.index()) {
                    def.name = new.as_str().into();
                }
            }
            None => errs.push(config_error(&fpath, format!("unknown field `{old}`"))),
        }
    }
    if !errs.is_empty() {
        return Err(errs);
    }
    let shape = intern(ctx, Shape::new(defs), path)?;
    Ok(Compiled::new(Rename { out: shape.clone() }, shape))
}

impl Step for Rename {
    fn apply(&mut self, event: Event) -> StepResult {
        match rebuild(event, &self.out, Ok) {
            Ok(event) => StepResult::Keep(event),
            Err(e) => StepResult::Reject(e),
        }
    }
}

#[derive(Debug)]
struct Project {
    picks: Vec<FieldIdx>,
    out: ShapeRef,
}

pub(super) fn compile_project(
    spec: &ProjectSpec,
    ctx: &mut StepCtx<'_>,
    input: &ShapeRef,
    path: &str,
) -> Result<Compiled, Vec<ThadamError>> {
    let mut errs = Vec::new();
    let mut picks = Vec::new();
    let mut defs = Vec::new();
    let mut seen = HashSet::new();
    if spec.fields.is_empty() {
        errs.push(config_error(
            &format!("{path}.fields"),
            "`fields` needs at least one field",
        ));
    }
    for (i, name) in spec.fields.iter().enumerate() {
        let fpath = format!("{path}.fields[{i}]");
        match input.index_of(name) {
            Some(idx) if seen.insert(idx) => {
                if let Some(def) = input.field(idx) {
                    picks.push(idx);
                    defs.push(def.clone());
                }
            }
            Some(_) => errs.push(config_error(
                &fpath,
                format!("field `{name}` is listed twice"),
            )),
            None => errs.push(config_error(&fpath, format!("unknown field `{name}`"))),
        }
    }
    if !errs.is_empty() {
        return Err(errs);
    }
    let shape = intern(ctx, Shape::new(defs), path)?;
    Ok(Compiled::new(
        Project {
            picks,
            out: shape.clone(),
        },
        shape,
    ))
}

impl Step for Project {
    fn apply(&mut self, event: Event) -> StepResult {
        let picks = &self.picks;
        let result = rebuild(event, &self.out, |mut values| {
            picks
                .iter()
                .map(|i| {
                    values
                        .get_mut(i.index())
                        .map(|v| std::mem::replace(v, Value::Null))
                        .ok_or_else(|| step_failed("event record does not match the step's shape"))
                })
                .collect()
        });
        match result {
            Ok(event) => StepResult::Keep(event),
            Err(e) => StepResult::Reject(e),
        }
    }
}

fn value_type(ty: &FieldType) -> Option<ValueType> {
    Some(match ty {
        FieldType::Bool => ValueType::Bool,
        FieldType::I8 => ValueType::I8,
        FieldType::I16 => ValueType::I16,
        FieldType::I32 => ValueType::I32,
        FieldType::I64 => ValueType::I64,
        FieldType::U8 => ValueType::U8,
        FieldType::U16 => ValueType::U16,
        FieldType::U32 => ValueType::U32,
        FieldType::U64 => ValueType::U64,
        FieldType::F32 => ValueType::F32,
        FieldType::F64 => ValueType::F64,
        FieldType::Str => ValueType::Str,
        FieldType::Bytes => ValueType::Bytes,
        FieldType::Ts => ValueType::Ts,
        FieldType::List(inner) => match **inner {
            FieldType::F32 => ValueType::F32s,
            FieldType::F64 => ValueType::F64s,
            FieldType::I32 => ValueType::I32s,
            FieldType::I64 => ValueType::I64s,
            _ => ValueType::List,
        },
        _ => return None,
    })
}

#[derive(Debug)]
struct Cast {
    casts: Vec<(FieldIdx, ValueType)>,
    out: ShapeRef,
}

pub(super) fn compile_cast(
    spec: &CastSpec,
    ctx: &mut StepCtx<'_>,
    input: &ShapeRef,
    path: &str,
) -> Result<Compiled, Vec<ThadamError>> {
    let mut errs = Vec::new();
    let mut defs: Vec<FieldDef> = input.fields().to_vec();
    let mut casts = Vec::new();
    if spec.fields.is_empty() {
        errs.push(config_error(
            &format!("{path}.fields"),
            "`fields` needs at least one entry",
        ));
    }
    for (name, ty_text) in &spec.fields {
        let fpath = format!("{path}.fields.{name}");
        let Some(idx) = input.index_of(name) else {
            errs.push(config_error(&fpath, format!("unknown field `{name}`")));
            continue;
        };
        let target = match parse_field_type(ty_text) {
            Ok(t) => t,
            Err(e) => {
                errs.push(
                    ThadamError::permanent(ErrorDomain::Schema, codes::SCHEMA_UNSUPPORTED_TYPE, e)
                        .with_context("path", fpath),
                );
                continue;
            }
        };
        let Some(def) = defs.get_mut(idx.index()) else {
            continue;
        };
        if def.ty == target {
            continue;
        }
        let widens = match (value_type(&def.ty), value_type(&target)) {
            (Some(from), Some(to)) => from.widens_to(to),
            _ => false,
        };
        if !widens {
            errs.push(
                ThadamError::permanent(
                    ErrorDomain::Schema,
                    codes::SCHEMA_NARROWING,
                    format!(
                        "cast of `{name}` from {:?} to {target:?} would narrow; casts only widen",
                        def.ty
                    ),
                )
                .with_context("path", fpath),
            );
            continue;
        }
        if let Some(to) = value_type(&target) {
            casts.push((idx, to));
        }
        def.ty = target;
    }
    if !errs.is_empty() {
        return Err(errs);
    }
    let shape = intern(ctx, Shape::new(defs), path)?;
    Ok(Compiled::new(
        Cast {
            casts,
            out: shape.clone(),
        },
        shape,
    ))
}

impl Step for Cast {
    fn apply(&mut self, event: Event) -> StepResult {
        let casts = &self.casts;
        let result = rebuild(event, &self.out, |mut values| {
            for (idx, to) in casts {
                let slot = values
                    .get_mut(idx.index())
                    .ok_or_else(|| step_failed("event record does not match the step's shape"))?;
                *slot = slot
                    .widen(*to)
                    .ok_or_else(|| step_failed("a widening cast failed on a value"))?;
            }
            Ok(values)
        });
        match result {
            Ok(event) => StepResult::Keep(event),
            Err(e) => StepResult::Reject(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{event, shape, with_ctx};
    use serde_json::json;

    fn input() -> ShapeRef {
        shape(&[
            ("a", FieldType::I32),
            ("b", FieldType::F32),
            ("c", FieldType::Str),
            ("d", FieldType::List(Box::new(FieldType::F32))),
        ])
    }

    fn row() -> Vec<Value> {
        vec![
            Value::I32(7),
            Value::F32(1.5),
            Value::str("x"),
            Value::F32s(vec![1.0f32, 2.0].into()),
        ]
    }

    fn run(c: Compiled, vals: Vec<Value>) -> Event {
        let mut step = c.step;
        match step.apply(event(&input(), vals, "r", 0)) {
            StepResult::Keep(e) => e,
            _ => panic!("expected keep"),
        }
    }

    #[test]
    fn rename_keeps_values_and_changes_names() {
        let spec: RenameSpec =
            serde_json::from_value(json!({ "fields": { "a": "count" } })).unwrap();
        let c = with_ctx(|ctx| compile_rename(&spec, ctx, &input(), "t")).unwrap();
        let out_shape = c.out_shape.clone();
        assert_eq!(out_shape.index_of("count"), Some(FieldIdx::new(0)));
        assert_eq!(out_shape.index_of("a"), None);
        let e = run(c, row());
        let r = e.payload().as_record().unwrap();
        assert_eq!(r.get("count"), Some(&Value::I32(7)));
        assert_eq!(r.len(), 4);
    }

    #[test]
    fn rename_errors() {
        let bad: RenameSpec = serde_json::from_value(json!({ "fields": { "zzz": "q" } })).unwrap();
        assert!(with_ctx(|ctx| compile_rename(&bad, ctx, &input(), "t")).is_err());
        let clash: RenameSpec = serde_json::from_value(json!({ "fields": { "a": "b" } })).unwrap();
        let e = with_ctx(|ctx| compile_rename(&clash, ctx, &input(), "t"))
            .err()
            .unwrap();
        assert!(e[0].message.contains("duplicate field name"));
    }

    #[test]
    fn project_keeps_listed_fields_in_listed_order() {
        let spec: ProjectSpec = serde_json::from_value(json!({ "fields": ["d", "a"] })).unwrap();
        let c = with_ctx(|ctx| compile_project(&spec, ctx, &input(), "t")).unwrap();
        assert_eq!(c.out_shape.len(), 2);
        let e = run(c, row());
        let r = e.payload().as_record().unwrap();
        let names: Vec<&str> = r.iter().map(|(n, _)| n).collect();
        assert_eq!(names, ["d", "a"]);
        assert_eq!(r.get("a"), Some(&Value::I32(7)));
        assert!(r.get("d").unwrap().as_f32s().is_some());
    }

    #[test]
    fn project_errors() {
        for bad in [
            json!({ "fields": [] }),
            json!({ "fields": ["nope"] }),
            json!({ "fields": ["a", "a"] }),
        ] {
            let spec: ProjectSpec = serde_json::from_value(bad).unwrap();
            assert!(with_ctx(|ctx| compile_project(&spec, ctx, &input(), "t")).is_err());
        }
    }

    #[test]
    fn cast_widens_values_and_types() {
        let spec: CastSpec = serde_json::from_value(
            json!({ "fields": { "a": "i64", "b": "f64", "d": "list<f64>" } }),
        )
        .unwrap();
        let c = with_ctx(|ctx| compile_cast(&spec, ctx, &input(), "t")).unwrap();
        assert_eq!(
            c.out_shape.field(FieldIdx::new(0)).unwrap().ty,
            FieldType::I64
        );
        let e = run(c, row());
        let r = e.payload().as_record().unwrap();
        assert_eq!(r.get("a"), Some(&Value::I64(7)));
        assert_eq!(r.get("b"), Some(&Value::F64(1.5)));
        assert_eq!(r.get("d").unwrap().as_f64s(), Some(&[1.0f64, 2.0][..]));
        assert!(r.validate().is_ok());
    }

    #[test]
    fn cast_refuses_narrowing_and_kind_changes() {
        for (field, ty) in [
            ("a", "i16"),
            ("b", "i32"),
            ("a", "f64"),
            ("c", "i64"),
            ("a", "u64"),
            ("d", "list<f32>x"),
        ] {
            let spec: CastSpec =
                serde_json::from_value(json!({ "fields": { field: ty } })).unwrap();
            let errs = with_ctx(|ctx| compile_cast(&spec, ctx, &input(), "t"))
                .err()
                .unwrap();
            assert!(
                errs[0].code == codes::SCHEMA_NARROWING
                    || errs[0].code == codes::SCHEMA_UNSUPPORTED_TYPE,
                "{field}: {ty}"
            );
        }
    }

    #[test]
    fn cast_to_the_same_type_is_allowed() {
        let spec: CastSpec = serde_json::from_value(json!({ "fields": { "a": "i32" } })).unwrap();
        assert!(with_ctx(|ctx| compile_cast(&spec, ctx, &input(), "t")).is_ok());
    }

    #[test]
    fn derived_shapes_count_against_max_shapes() {
        let limits = crate::limits::ChainLimits {
            max_shapes: 1,
            ..Default::default()
        };
        let spec: RenameSpec = serde_json::from_value(json!({ "fields": { "a": "z" } })).unwrap();
        let spec2: RenameSpec = serde_json::from_value(json!({ "fields": { "b": "y" } })).unwrap();
        crate::testutil::with_ctx_limits(&limits, |ctx| {
            assert!(compile_rename(&spec, ctx, &input(), "t").is_ok());
            let e = compile_rename(&spec2, ctx, &input(), "t").err().unwrap();
            assert_eq!(e[0].code, codes::SCHEMA_SHAPE_LIMIT);
        });
    }
}
