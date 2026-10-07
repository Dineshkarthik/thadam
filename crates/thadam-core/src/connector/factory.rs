//! Factories: how a configured `kind` becomes a connector.
//!
//! Configuration crosses this boundary as a `serde_json::Value` (D22), so no
//! YAML type reaches a connector.

use async_trait::async_trait;

use super::context::{BuildContext, ServiceContext};
use super::sink::{BatchSink, SegmentSink};
use super::source::Source;
use crate::error::ThadamError;

/// A long-running component that is neither a source nor a sink, such as the
/// shipper. It consumes finalized segments through the catalog, not the WAL.
#[async_trait]
pub trait Service: Send + 'static {
    /// Runs until `ctx.shutdown` triggers, then returns.
    async fn run(&mut self, ctx: &ServiceContext) -> Result<(), ThadamError>;
}

macro_rules! factory_trait {
    ($(#[$meta:meta])* $name:ident, $built:path) => {
        $(#[$meta])*
        pub trait $name: Send + Sync {
            /// The `kind` string this factory is registered under.
            fn kind(&self) -> &'static str;

            /// A JSON Schema for the connector's `config:` block.
            fn config_schema(&self) -> serde_json::Value;

            /// Checks `cfg` without building, returning every problem found.
            fn validate(&self, cfg: &serde_json::Value) -> Result<(), Vec<ThadamError>>;

            /// Builds the connector from validated configuration.
            fn build(
                &self,
                cfg: serde_json::Value,
                ctx: &BuildContext,
            ) -> Result<Box<dyn $built>, ThadamError>;
        }
    };
}

factory_trait!(
    /// Builds [`Source`]s of one kind.
    SourceFactory,
    Source
);
factory_trait!(
    /// Builds [`BatchSink`]s of one kind.
    BatchSinkFactory,
    BatchSink
);
factory_trait!(
    /// Builds [`SegmentSink`]s of one kind.
    SegmentSinkFactory,
    SegmentSink
);
factory_trait!(
    /// Builds [`Service`]s of one kind.
    ServiceFactory,
    Service
);
