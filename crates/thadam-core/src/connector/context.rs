//! The contexts the runtime hands to connectors and factories.
//!
//! These are placeholders the runtime extends (metric handles, tracing spans);
//! they are `#[non_exhaustive]` so adding a field is not a breaking change.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::budget::DiskBudget;
use crate::clock::Clock;
use crate::dataset::DatasetDecl;
use crate::identity::{SinkId, SourceId};
use crate::metrics::{MetricsRecorder, NoopMetricsRecorder};
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
    /// Reports [`crate::ConnectorMetric`]s to the runtime; a no-op sink
    /// unless the runtime threaded one through.
    pub metrics: Arc<dyn MetricsRecorder>,
}

impl SourceContext {
    /// A context for `source_id`.
    pub fn new(source_id: SourceId, shutdown: Shutdown, clock: Arc<dyn Clock>) -> Self {
        SourceContext {
            source_id,
            shutdown,
            clock,
            metrics: Arc::new(NoopMetricsRecorder),
        }
    }

    /// Sets the metrics recorder connector metrics flow through.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
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
    /// Objects other components in this pipeline published (the shared set;
    /// empty unless the runtime threaded one through).
    pub shared: SharedObjects,
    /// Reports [`crate::ConnectorMetric`]s to the runtime; a no-op sink
    /// unless the runtime threaded one through.
    pub metrics: Arc<dyn MetricsRecorder>,
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
            shared: SharedObjects::default(),
            metrics: Arc::new(NoopMetricsRecorder),
        }
    }

    /// Sets the pipeline's shared objects.
    #[must_use]
    pub fn with_shared(mut self, shared: SharedObjects) -> Self {
        self.shared = shared;
        self
    }

    /// Sets the metrics recorder connector metrics flow through.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
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
    /// Objects other components in this pipeline published (the shared set;
    /// empty unless the runtime threaded one through).
    pub shared: SharedObjects,
    /// Reports [`crate::ConnectorMetric`]s to the runtime; a no-op sink
    /// unless the runtime threaded one through.
    pub metrics: Arc<dyn MetricsRecorder>,
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
            shared: SharedObjects::default(),
            metrics: Arc::new(NoopMetricsRecorder),
        }
    }

    /// Sets the pipeline's shared objects.
    #[must_use]
    pub fn with_shared(mut self, shared: SharedObjects) -> Self {
        self.shared = shared;
        self
    }

    /// Sets the metrics recorder connector metrics flow through.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
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
    /// The pipeline's shared schema-store directory (the runtime places it
    /// at `<data_dir>/schemas`), set when the runtime plans against a data
    /// directory. A source whose messages reference durably stored schema
    /// text — the MCAP source's `ros2msg` channels today — writes the
    /// schema here (see [`crate::schema_store`]) before the first message
    /// referencing it can reach the WAL, and stamps messages with
    /// [`crate::schema_store::SCHEMA_HASH_ATTR`]. `None` for compositions
    /// that do not manage a data directory.
    pub schema_dir: Option<PathBuf>,
    /// The pipeline-declared datasets this component consumes, in
    /// configuration order. The runtime fills this for sinks built with a
    /// `datasets:` selector (restricted to that selector) and for each
    /// sink's quarantine instance; it is empty for sources and services.
    /// Declaring schemas locally duplicates this, so segment sinks must take
    /// their dataset schemas from here.
    pub datasets: Arc<[DatasetDecl]>,
    /// The pipeline's config hash (lowercase hex), which Parquet segments
    /// record as `thadam.config_hash`. Empty unless the runtime built the
    /// component for a pipeline; see [`BuildContext::with_config_hashes`].
    pub config_hash: String,
    /// The enrichment hash of the transform chain the component's segments
    /// come from (`thadam.enrichment_hash`); empty when the chain loaded no
    /// enrichment table.
    pub enrichment_hash: String,
    /// The node id Parquet segment files are named under. The runtime sets
    /// it to `pipeline.node_id` (see [`BuildContext::with_node_id`]); outside
    /// a pipeline it is the component id.
    pub node_id: String,
}

impl BuildContext {
    /// A context for building `component_id`, consuming no datasets.
    pub fn new(component_id: impl Into<Sym>, clock: Arc<dyn Clock>, data_dir: PathBuf) -> Self {
        let component_id = component_id.into();
        BuildContext {
            node_id: component_id.to_string(),
            component_id,
            clock,
            data_dir,
            schema_dir: None,
            datasets: Arc::from([]),
            config_hash: String::new(),
            enrichment_hash: String::new(),
        }
    }

    /// Sets the shared schema-store directory.
    #[must_use]
    pub fn with_schema_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.schema_dir = Some(dir.into());
        self
    }

    /// Sets the declared datasets the component consumes.
    #[must_use]
    pub fn with_datasets(mut self, datasets: Arc<[DatasetDecl]>) -> Self {
        self.datasets = datasets;
        self
    }

    /// Sets the pipeline's config hash and the enrichment hash of the chain
    /// feeding the component.
    #[must_use]
    pub fn with_config_hashes(
        mut self,
        config_hash: impl Into<String>,
        enrichment_hash: impl Into<String>,
    ) -> Self {
        self.config_hash = config_hash.into();
        self.enrichment_hash = enrichment_hash.into();
        self
    }

    /// Sets the pipeline's node id, which segment file names carry.
    #[must_use]
    pub fn with_node_id(mut self, node_id: impl Into<String>) -> Self {
        self.node_id = node_id.into();
        self
    }
}

/// Objects a component publishes for other components in the same pipeline,
/// looked up by component id.
///
/// The motivating use is a shipper service finding a named segment sink's
/// catalog: `thadam-core` cannot name the catalog type (dependency rules), so
/// values are type-erased and [`SharedObjects::get`] downcasts. Publishing is
/// last-write-wins, which is what a re-recovered sink wants. Cloning shares
/// the map; poisoning is recovered like in the rest of the runtime.
#[derive(Clone, Default)]
pub struct SharedObjects {
    map: Arc<RwLock<BTreeMap<String, Arc<dyn Any + Send + Sync>>>>,
}

impl SharedObjects {
    /// Publishes `object` under `component`, replacing any earlier object.
    pub fn publish<T: Send + Sync + 'static>(&self, component: &str, object: Arc<T>) {
        let mut map = self.map.write().unwrap_or_else(|e| e.into_inner());
        map.insert(component.to_owned(), object);
    }

    /// The object published under `component`, downcast to `T`; `None` when
    /// the component published nothing or published a different type.
    pub fn get<T: Send + Sync + 'static>(&self, component: &str) -> Option<Arc<T>> {
        let map = self.map.read().unwrap_or_else(|e| e.into_inner());
        map.get(component)?.clone().downcast().ok()
    }
}

/// The publishing components only; published objects stay opaque.
impl fmt::Debug for SharedObjects {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let map = self.map.read().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("SharedObjects")
            .field("components", &map.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_objects_publish_get_replace() {
        let shared = SharedObjects::default();
        shared.publish("local", Arc::new(String::from("catalog-a")));
        assert_eq!(
            shared.get::<String>("local").map(|s| s.as_str().to_owned()),
            Some(String::from("catalog-a"))
        );
        // Clones share the map.
        assert_eq!(
            shared
                .clone()
                .get::<String>("local")
                .map(|s| s.as_str().to_owned()),
            Some(String::from("catalog-a"))
        );
        // A wrong type downcast misses; an unknown component misses.
        assert!(shared.get::<u64>("local").is_none());
        assert!(shared.get::<String>("other").is_none());
        // Last write wins.
        shared.publish("local", Arc::new(String::from("catalog-b")));
        assert_eq!(
            shared.get::<String>("local").map(|s| s.as_str().to_owned()),
            Some(String::from("catalog-b"))
        );
        // Debug lists components, not values.
        assert!(format!("{shared:?}").contains("local"));
    }

    #[test]
    fn contexts_default_to_isolated_shared_maps() {
        let a = SinkContext::new(
            SinkId::new("a"),
            Shutdown::new(),
            Arc::new(crate::SystemClock::new()),
            PathBuf::from("/tmp/a"),
            Arc::new(DiskBudget::new()),
        );
        let b = SinkContext::new(
            SinkId::new("b"),
            Shutdown::new(),
            Arc::new(crate::SystemClock::new()),
            PathBuf::from("/tmp/b"),
            Arc::new(DiskBudget::new()),
        );
        a.shared.publish("a", Arc::new(1u8));
        assert!(b.shared.get::<u8>("a").is_none());
        let shared = SharedObjects::default();
        let ctx = ServiceContext::new(
            "svc",
            Shutdown::new(),
            Arc::new(crate::SystemClock::new()),
            PathBuf::from("/tmp/s"),
            Arc::new(DiskBudget::new()),
        )
        .with_shared(shared.clone());
        shared.publish("local", Arc::new(7u8));
        assert_eq!(ctx.shared.get::<u8>("local"), Some(Arc::new(7u8)));
    }
}
