//! `filter`: drops events that do not satisfy a structured predicate.

use thadam_core::{ErrorDomain, Event, ShapeRef, ThadamError, codes};

use super::{Compiled, Step, StepCtx, StepResult, config_error};
use crate::predicate::{Env, Predicate, compile as compile_predicate};
use crate::spec::FilterSpec;

#[derive(Debug)]
struct Filter {
    predicate: Predicate,
}

pub(super) fn compile(
    spec: &FilterSpec,
    ctx: &mut StepCtx<'_>,
    input: &ShapeRef,
    path: &str,
) -> Result<Compiled, Vec<ThadamError>> {
    if spec.expr.is_some() {
        return Err(vec![config_error(
            &format!("{path}.expr"),
            "`expr` is reserved for a future expression language; write the condition under `where`",
        )]);
    }
    let Some(pred) = &spec.where_ else {
        return Err(vec![config_error(
            path,
            "a filter needs a `where` condition",
        )]);
    };
    let env = Env {
        shape: input,
        levels: ctx.levels,
    };
    let predicate =
        compile_predicate(pred, &env, ctx.limits, &format!("{path}.where")).map_err(|errs| {
            errs.into_iter()
                .map(|(p, m)| config_error(&p, m))
                .collect::<Vec<_>>()
        })?;
    Ok(Compiled::new(Filter { predicate }, input.clone()))
}

impl Step for Filter {
    fn apply(&mut self, event: Event) -> StepResult {
        match self.predicate.eval(&event) {
            Ok(true) => StepResult::Keep(event),
            Ok(false) => StepResult::Filtered,
            Err(e) => StepResult::Reject(ThadamError::permanent(
                ErrorDomain::Transform,
                codes::TRANSFORM_EVAL_FAILED,
                e.0,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{event, shape, with_ctx};
    use serde_json::json;
    use thadam_core::{FieldType, Value};

    fn build(spec: serde_json::Value, s: &ShapeRef) -> Result<Box<dyn Step>, Vec<ThadamError>> {
        let spec: FilterSpec = serde_json::from_value(spec).unwrap();
        with_ctx(|ctx| compile(&spec, ctx, s, "transforms[2]").map(|c| c.step))
    }

    #[test]
    fn keeps_matching_and_filters_the_rest() {
        let s = shape(&[("level", FieldType::Str)]);
        let mut step = build(json!({ "where": { "field": "level", "ne": "debug" } }), &s).unwrap();
        let info = event(&s, vec![Value::str("info")], "r", 0);
        let debug = event(&s, vec![Value::str("debug")], "r", 0);
        assert!(matches!(step.apply(info), StepResult::Keep(_)));
        assert!(matches!(step.apply(debug), StepResult::Filtered));
    }

    #[test]
    fn expr_is_reserved() {
        let s = shape(&[("level", FieldType::Str)]);
        let e = build(json!({ "expr": "level != 'debug'" }), &s).unwrap_err();
        assert!(e[0].message.contains("reserved"));
        assert_eq!(e[0].context.get("path"), Some("transforms[2].expr"));
    }

    #[test]
    fn where_is_required_and_errors_carry_paths() {
        let s = shape(&[("level", FieldType::Str)]);
        assert!(build(json!({}), &s).is_err());
        let e = build(
            json!({ "where": { "all": [{ "field": "zzz", "eq": 1 }] } }),
            &s,
        )
        .unwrap_err();
        assert_eq!(
            e[0].context.get("path"),
            Some("transforms[2].where.all[0].field")
        );
    }

    #[test]
    fn a_foreign_event_is_a_reject_not_a_panic() {
        let s = shape(&[("level", FieldType::Str)]);
        let other = shape(&[]);
        let mut step = build(json!({ "where": { "field": "level", "eq": "a" } }), &s).unwrap();
        match step.apply(event(&other, vec![], "r", 0)) {
            StepResult::Reject(e) => assert_eq!(e.code, codes::TRANSFORM_EVAL_FAILED),
            _ => panic!("expected a reject"),
        }
    }
}
