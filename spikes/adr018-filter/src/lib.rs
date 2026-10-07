//! ADR-0018 spike: per-event filter evaluation, three ways. See docs/design/spikes/ADR-018-filter.md.

pub mod cases;
pub mod cel_engine;
#[cfg(feature = "evalexpr")]
pub mod ee;
pub mod model;
pub mod native;
