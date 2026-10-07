//! The contexts the runtime hands to connectors and factories.
//!
//! These are placeholders the runtime extends (metric handles, tracing spans);
//! they are `#[non_exhaustive]` so adding a field is not a breaking change.

use std::path::PathBuf;
use std::sync::Arc;

use crate::budget::DiskBudget;
use crate::clock::Clock;
use crate::identity::{SinkId, SourceId};
use crate::shutdown::Shutdown;
use crate::text::Sym;

/// Passed to [`Source::open`](crate::Source::open).
#[derive(Clone)]
#[non_exhaustive]
pub struct SourceContext {
    /// The configured source id.
    pub source_id: SourceId,
    /// Cooperative cancellation for connect, receive, and ack.
    pub shutdown: Shutdown,
    /// Time source for ingest stamps and the clock-sync flag.
    pub clock: Arc<dyn Clock>,
}

impl SourceContext {
    /// A context for `source_id`.
    pub fn new(source_id: SourceId, shutdown: Shutdown, clock: Arc<dyn Clock>) -> Self {
        SourceContext {
            source_id,
            shutdown,
            clock,
        }
    }
}

/// Passed to sink `open` and `recover`.
#[derive(Clone)]
#[non_exhaustive]
pub struct SinkContext {
    /// The configured sink id.
    pub sink_id: SinkId,
    /// Cooperative cancellation.
    pub shutdown: Shutdown,
    /// Time source.
    pub clock: Arc<dyn Clock>,
    /// The sink's own directory for staging and recovery state.
    pub data_dir: PathBuf,
    /// The shared disk budget.
    pub budget: Arc<DiskBudget>,
}

impl SinkContext {
    /// A context for `sink_id` rooted at `data_dir`.
    pub fn new(
        sink_id: SinkId,
        shutdown: Shutdown,
        clock: Arc<dyn Clock>,
        data_dir: PathBuf,
        budget: Arc<DiskBudget>,
    ) -> Self {
        SinkContext {
            sink_id,
            shutdown,
            clock,
            data_dir,
            budget,
        }
    }
}

/// Passed to [`Service::run`](crate::Service::run).
#[derive(Clone)]
#[non_exhaustive]
pub struct ServiceContext {
    /// The configured service id.
    pub service_id: Sym,
    /// Cooperative cancellation; a service runs until it triggers.
    pub shutdown: Shutdown,
    /// Time source.
    pub clock: Arc<dyn Clock>,
    /// The service's directory.
    pub data_dir: PathBuf,
    /// The shared disk budget.
    pub budget: Arc<DiskBudget>,
}

impl ServiceContext {
    /// A context for `service_id` rooted at `data_dir`.
    pub fn new(
        service_id: impl Into<Sym>,
        shutdown: Shutdown,
        clock: Arc<dyn Clock>,
        data_dir: PathBuf,
        budget: Arc<DiskBudget>,
    ) -> Self {
        ServiceContext {
            service_id: service_id.into(),
            shutdown,
            clock,
            data_dir,
            budget,
        }
    }
}

/// Passed to factory `build`: what a connector needs at construction.
#[derive(Clone)]
#[non_exhaustive]
pub struct BuildContext {
    /// The configured id of the component being built.
    pub component_id: Sym,
    /// Time source.
    pub clock: Arc<dyn Clock>,
    /// The component's directory.
    pub data_dir: PathBuf,
}

impl BuildContext {
    /// A context for building `component_id`.
    pub fn new(component_id: impl Into<Sym>, clock: Arc<dyn Clock>, data_dir: PathBuf) -> Self {
        BuildContext {
            component_id: component_id.into(),
            clock,
            data_dir,
        }
    }
}
