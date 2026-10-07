pub mod alloc;
pub mod arrow_conv;
pub mod repr;
pub mod value;
pub mod workload;

use repr::Repr;
use serde_json::Value as Json;
use std::sync::Arc;
use workload::Spec;

/// Build `n` records by cycling through the pre-parsed JSON events.
pub fn build_n<R: Repr>(ctx: &mut R::Ctx, events: &[Json], n: usize) -> Vec<R::Rec> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push(R::build(ctx, &events[i % events.len()]));
    }
    out
}

/// Dispatch a generic function over every (value set x representation) pair.
/// `$f` must be a macro-callable generic: `$f::<R>(args...)`.
#[macro_export]
macro_rules! for_each_repr {
    ($mac:ident) => {{
        use $crate::repr::{ById, Keyed, Shared};
        use $crate::value::{ext, min};
        $mac!("min", Keyed<min::Value, false>);
        $mac!("min", Keyed<min::Value, true>);
        $mac!("min", Shared<min::Value>);
        $mac!("min", ById<min::Value>);
        $mac!("ext", Keyed<ext::Value, false>);
        $mac!("ext", Keyed<ext::Value, true>);
        $mac!("ext", Shared<ext::Value>);
        $mac!("ext", ById<ext::Value>);
    }};
}

pub fn spec_arc(w: &workload::Workload) -> Arc<Spec> {
    w.spec.clone()
}
