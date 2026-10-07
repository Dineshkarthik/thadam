//! `sample`: every-N and minimum-interval sampling, per asset.
//!
//! Both rules read arrival order and ingest time, never the wall clock. With
//! `every_n`, an asset's first event and every Nth after it pass the count
//! rule (every event counts, whether or not it is kept). With `min_interval`,
//! an event is kept only if enough ingest time passed since the asset's last
//! kept event; if ingest time steps backwards the baseline restarts and the
//! event is kept. With both, an event must pass the count rule first.

use thadam_core::{Event, ThadamError, ShapeRef};

use super::state::AssetMap;
use super::{Compiled, Step, StepCtx, StepResult, config_error};
use crate::spec::SampleSpec;

#[derive(Debug, Default)]
struct State {
    seen: u64,
    last_kept_ns: Option<i64>,
}

#[derive(Debug)]
struct Sample {
    every_n: Option<u64>,
    min_interval_ns: Option<i128>,
    states: AssetMap<State>,
}

pub(super) fn compile(
    spec: &SampleSpec,
    ctx: &mut StepCtx<'_>,
    input: &ShapeRef,
    path: &str,
) -> Result<Compiled, Vec<ThadamError>> {
    let mut errs = Vec::new();
    if spec.every_n.is_none() && spec.min_interval.is_none() {
        errs.push(config_error(
            path,
            "give `every_n`, `min_interval`, or both",
        ));
    }
    if spec.every_n == Some(0) {
        errs.push(config_error(
            &format!("{path}.every_n"),
            "`every_n` must be at least 1",
        ));
    }
    if spec.min_interval.is_some_and(|d| d.0.is_zero()) {
        errs.push(config_error(
            &format!("{path}.min_interval"),
            "`min_interval` must be positive",
        ));
    }
    if !errs.is_empty() {
        return Err(errs);
    }
    Ok(Compiled::new(
        Sample {
            every_n: spec.every_n,
            min_interval_ns: spec.min_interval.map(|d| d.0.as_nanos() as i128),
            states: AssetMap::new(ctx.limits.max_state_keys),
        },
        input.clone(),
    ))
}

impl Step for Sample {
    fn apply(&mut self, event: Event) -> StepResult {
        let now = event.times().ingest().as_nanos();
        let (state, _) = self
            .states
            .get_or_insert_with(event.asset(), State::default);
        let index = state.seen;
        state.seen = state.seen.wrapping_add(1);

        if self.every_n.is_some_and(|n| index % n != 0) {
            return StepResult::Filtered;
        }
        if let Some(gap) = self.min_interval_ns
            && let Some(last) = state.last_kept_ns
            && now >= last
            && i128::from(now) - i128::from(last) < gap
        {
            return StepResult::Filtered;
        }
        state.last_kept_ns = Some(now);
        StepResult::Keep(event)
    }

    fn evicted(&self) -> u64 {
        self.states.evicted()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{event, shape, with_ctx};
    use thadam_core::{FieldType, Value};
    use serde_json::json;

    fn s() -> ShapeRef {
        shape(&[("v", FieldType::I64)])
    }

    fn build(spec: serde_json::Value) -> Result<Box<dyn Step>, Vec<ThadamError>> {
        let spec: SampleSpec = serde_json::from_value(spec).unwrap();
        with_ctx(|ctx| compile(&spec, ctx, &s(), "transforms[1]").map(|c| c.step))
    }

    fn keep(step: &mut Box<dyn Step>, robot: &str, secs: i64) -> bool {
        matches!(
            step.apply(event(&s(), vec![Value::I64(0)], robot, secs)),
            StepResult::Keep(_)
        )
    }

    #[test]
    fn every_n_keeps_the_first_and_every_nth_per_asset() {
        let mut step = build(json!({ "every_n": 3 })).unwrap();
        let kept: Vec<bool> = (0..7).map(|i| keep(&mut step, "a", i)).collect();
        assert_eq!(kept, [true, false, false, true, false, false, true]);
        assert!(keep(&mut step, "b", 0), "another asset has its own counter");
    }

    #[test]
    fn min_interval_uses_ingest_time() {
        let mut step = build(json!({ "min_interval": "1s" })).unwrap();
        assert!(keep(&mut step, "a", 0));
        assert!(!keep(&mut step, "a", 0));
        assert!(keep(&mut step, "a", 1));
        assert!(!keep(&mut step, "a", 1));
    }

    #[test]
    fn min_interval_survives_time_going_backwards() {
        let mut step = build(json!({ "min_interval": "10s" })).unwrap();
        assert!(keep(&mut step, "a", 1_000));
        assert!(keep(&mut step, "a", 5), "stepped back: baseline restarts");
        assert!(!keep(&mut step, "a", 14));
        assert!(keep(&mut step, "a", 15));
    }

    #[test]
    fn both_rules_must_pass() {
        let mut step = build(json!({ "every_n": 2, "min_interval": "10s" })).unwrap();
        assert!(keep(&mut step, "a", 0));
        assert!(!keep(&mut step, "a", 100), "fails the count rule");
        assert!(keep(&mut step, "a", 200));
        assert!(!keep(&mut step, "a", 201));
        assert!(
            !keep(&mut step, "a", 202),
            "passes the count rule, too soon"
        );
    }

    #[test]
    fn config_errors() {
        assert!(build(json!({})).is_err());
        assert!(build(json!({ "every_n": 0 })).is_err());
        assert!(build(json!({ "min_interval": "0s" })).is_err());
    }
}
