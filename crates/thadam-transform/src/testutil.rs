//! Shared helpers for unit tests.

use std::path::Path;

use thadam_core::{
    AssetPath, Event, EventId, FieldDef, FieldType, Quality, Record, Shape, ShapeRef,
    ShapeRegistry, Sym, TimeDomain, Times, Timestamp, Value, WalPos,
};

use crate::limits::ChainLimits;
use crate::steps::StepCtx;

pub(crate) fn shape(fields: &[(&str, FieldType)]) -> ShapeRef {
    ShapeRef::new(
        Shape::new(
            fields
                .iter()
                .map(|(n, t)| FieldDef::new(*n, t.clone()))
                .collect(),
        )
        .unwrap(),
    )
}

pub(crate) fn event_with_quality(
    shape: &ShapeRef,
    values: Vec<Value>,
    robot: &str,
    ingest_secs: i64,
    quality: Quality,
) -> Event {
    Event::builder(
        EventId::from_u128(1),
        WalPos::new(0),
        Times::new(Timestamp::from_secs(ingest_secs).unwrap(), TimeDomain::Utc),
    )
    .asset(AssetPath::new([("robot", robot)]).unwrap())
    .quality(quality)
    .payload(Record::new(shape.clone(), values).unwrap())
    .build()
    .unwrap()
}

pub(crate) fn event(shape: &ShapeRef, values: Vec<Value>, robot: &str, ingest_secs: i64) -> Event {
    event_with_quality(shape, values, robot, ingest_secs, Quality::good())
}

/// Like [`event`], at WAL position `pos`.
pub(crate) fn event_at(
    shape: &ShapeRef,
    values: Vec<Value>,
    robot: &str,
    ingest_secs: i64,
    pos: u64,
) -> Event {
    Event::builder(
        EventId::from_u128(1),
        WalPos::new(pos),
        Times::new(Timestamp::from_secs(ingest_secs).unwrap(), TimeDomain::Utc),
    )
    .asset(AssetPath::new([("robot", robot)]).unwrap())
    .payload(Record::new(shape.clone(), values).unwrap())
    .build()
    .unwrap()
}

pub(crate) fn with_ctx_full<R>(
    limits: &ChainLimits,
    base_dir: Option<&Path>,
    f: impl FnOnce(&mut StepCtx<'_>) -> R,
) -> R {
    let dataset = Sym::from("ds");
    let levels = [Sym::from("robot")];
    let mut registry = ShapeRegistry::new(limits.max_shapes);
    let mut ctx = StepCtx {
        dataset: &dataset,
        levels: &levels,
        limits,
        registry: &mut registry,
        base_dir,
    };
    f(&mut ctx)
}

pub(crate) fn with_ctx_limits<R>(limits: &ChainLimits, f: impl FnOnce(&mut StepCtx<'_>) -> R) -> R {
    with_ctx_full(limits, None, f)
}

pub(crate) fn with_ctx<R>(f: impl FnOnce(&mut StepCtx<'_>) -> R) -> R {
    with_ctx_limits(&ChainLimits::default(), f)
}
