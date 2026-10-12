//! Sparkplug B decode evaluation for Thadam (report:
//! `docs/design/spikes/sparkplug-b-and-opcua-pubsub.md`).
//!
//! Two pieces that a `decode: sparkplug_b` step would need:
//!
//! - [`topic`]: the `spBv1.0/<group>/<kind>/<edge_node>[/<device>]` namespace.
//! - [`decode`]: the stateful consumer half — BIRTH alias tables, `bdSeq`
//!   sessions, `seq` gap detection (0..=255 walk), DEATH staleness.
//!
//! The protobuf bindings are compiled from the vendored Eclipse Tahu
//! `sparkplug_b.proto` (EPL-2.0, `proto/sparkplug_b.proto`).

pub mod decode;
pub mod topic;

#[allow(missing_docs)]
#[allow(clippy::all)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/org.eclipse.tahu.protobuf.rs"));
}
