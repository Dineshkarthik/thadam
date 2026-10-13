//! Transform steps. Steps are crate-private and compiled from [`TransformSpec`];
//! there is deliberately no user-defined step trait (ADR-0017 is reserved).

use std::fmt;
use std::path::Path;

use thadam_core::{ErrorDomain, Event, ThadamError, ShapeRef, ShapeRegistry, Sym, WalPos, codes};

use crate::limits::ChainLimits;
use crate::spec::TransformSpec;

mod enrich;
mod fieldops;
mod filter;
mod joint_state;
mod on_change;
mod pose;
mod sample;
mod state;
mod validate;
mod window;

pub use enrich::EnrichmentInfo;
pub(crate) use enrich::lineage_hash;

/// What a step did with one event.
pub(crate) enum StepResult {
    Keep(Event),
    Filtered,
    /// Dropped because its event time belongs to a window that already closed.
    Late,
    Reject(ThadamError),
    /// Zero or more events in place of the input; each continues through the
    /// later steps. An empty list means the step absorbed the input.
    Emit(Vec<Event>),
}

/// One stage of a dataset's transform list. A step owns its state and runs on
/// one chain task, so it needs no locking.
pub(crate) trait Step: Send + fmt::Debug {
    fn apply(&mut self, event: Event) -> StepResult;

    /// Ids and counts the step forgot because a bound was hit.
    fn evicted(&self) -> u64 {
        0
    }

    /// The oldest WAL position this step still needs to see again to rebuild
    /// its state after a restart (the first frame of an open window), if any.
    fn hold(&self) -> Option<WalPos> {
        None
    }

    /// Whether the step's state is rebuilt by re-reading from [`Step::hold`]
    /// rather than by warm-up, which must then not feed it.
    fn windowed(&self) -> bool {
        false
    }

    /// Marks any open window as having spanned a WAL gap.
    fn on_gap(&mut self) {}

    /// Marks the chain as starting mid-WAL (a range replay from `from > 1`),
    /// so the first window opened per asset is flagged `uncertain`.
    fn mark_range_start(&mut self) {}
}

/// What a step may use while compiling.
pub(crate) struct StepCtx<'a> {
    pub(crate) dataset: &'a Sym,
    pub(crate) levels: &'a [Sym],
    pub(crate) limits: &'a ChainLimits,
    pub(crate) registry: &'a mut ShapeRegistry,
    pub(crate) base_dir: Option<&'a Path>,
}

pub(crate) struct Compiled {
    pub(crate) step: Box<dyn Step>,
    pub(crate) out_shape: ShapeRef,
    pub(crate) enrichment: Option<EnrichmentInfo>,
}

impl Compiled {
    fn new(step: impl Step + 'static, out_shape: ShapeRef) -> Self {
        Compiled {
            step: Box::new(step),
            out_shape,
            enrichment: None,
        }
    }
}

/// A configuration error at `path`.
pub(crate) fn config_error(path: &str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
        .with_context("path", path)
}

pub(crate) fn compile(
    spec: &TransformSpec,
    ctx: &mut StepCtx<'_>,
    input: &ShapeRef,
    path: &str,
) -> Result<Compiled, Vec<ThadamError>> {
    match spec {
        TransformSpec::Validate(s) => validate::compile(s, ctx, input, path),
        TransformSpec::OnChange(s) => on_change::compile(s, ctx, input, path),
        TransformSpec::Filter(s) => filter::compile(s, ctx, input, path),
        TransformSpec::Sample(s) => sample::compile(s, ctx, input, path),
        TransformSpec::Enrich(s) => enrich::compile(s, ctx, input, path),
        TransformSpec::Rename(s) => fieldops::compile_rename(s, ctx, input, path),
        TransformSpec::Project(s) => fieldops::compile_project(s, ctx, input, path),
        TransformSpec::Cast(s) => fieldops::compile_cast(s, ctx, input, path),
        TransformSpec::Pose(s) => pose::compile(s, ctx, input, path),
        TransformSpec::JointState(s) => joint_state::compile(s, ctx, input, path),
        TransformSpec::Window(s) => window::compile(s, ctx, input, path),
    }
}

/// Interns a derived shape, or reports that the chain exceeds `max_shapes`.
pub(crate) fn intern(
    ctx: &mut StepCtx<'_>,
    shape: Result<thadam_core::Shape, thadam_core::ShapeError>,
    path: &str,
) -> Result<ShapeRef, Vec<ThadamError>> {
    let shape = shape.map_err(|e| vec![config_error(path, e.to_string())])?;
    ctx.registry.intern(shape).map_err(|e| {
        vec![
            ThadamError::permanent(
                ErrorDomain::Schema,
                codes::SCHEMA_SHAPE_LIMIT,
                e.to_string(),
            )
            .with_context("path", path),
        ]
    })
}
