//! The connector registry: configured `kind` to factory.

use std::collections::BTreeMap;

use thadam_config::KindPolicy;
use thadam_core::{BatchSinkFactory, SegmentSinkFactory, ServiceFactory, SourceFactory};

/// How a `sinks:` entry's kind resolved.
pub(crate) enum SinkFactory<'a> {
    Segment(&'a dyn SegmentSinkFactory),
    Batch(&'a dyn BatchSinkFactory),
    Service(&'a dyn ServiceFactory),
}

/// Factories by kind. The binary (`thadam`) is the only place that fills
/// one; connector crates never see the runtime.
///
/// Entries under `sinks:` in the configuration resolve against segment sinks,
/// batch sinks, and services (the shipper is configured as a sink entry but
/// runs as a service). A kind registered in more than one of those three is
/// refused by [`Pipeline::build`](crate::Pipeline::build). Registering a kind
/// again replaces the earlier factory.
#[derive(Default)]
pub struct Registry {
    sources: BTreeMap<&'static str, Box<dyn SourceFactory>>,
    segment_sinks: BTreeMap<&'static str, Box<dyn SegmentSinkFactory>>,
    batch_sinks: BTreeMap<&'static str, Box<dyn BatchSinkFactory>>,
    services: BTreeMap<&'static str, Box<dyn ServiceFactory>>,
    quarantine: Option<Box<dyn SegmentSinkFactory>>,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("sources", &self.sources.keys().collect::<Vec<_>>())
            .field(
                "segment_sinks",
                &self.segment_sinks.keys().collect::<Vec<_>>(),
            )
            .field("batch_sinks", &self.batch_sinks.keys().collect::<Vec<_>>())
            .field("services", &self.services.keys().collect::<Vec<_>>())
            .field("quarantine", &self.quarantine.as_ref().map(|q| q.kind()))
            .finish()
    }
}

impl Registry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a source factory under its kind.
    pub fn source(&mut self, f: Box<dyn SourceFactory>) -> &mut Self {
        self.sources.insert(f.kind(), f);
        self
    }

    /// Registers a segment-sink factory under its kind.
    pub fn segment_sink(&mut self, f: Box<dyn SegmentSinkFactory>) -> &mut Self {
        self.segment_sinks.insert(f.kind(), f);
        self
    }

    /// Registers a batch-sink factory under its kind.
    pub fn batch_sink(&mut self, f: Box<dyn BatchSinkFactory>) -> &mut Self {
        self.batch_sinks.insert(f.kind(), f);
        self
    }

    /// Registers a service factory under its kind.
    pub fn service(&mut self, f: Box<dyn ServiceFactory>) -> &mut Self {
        self.services.insert(f.kind(), f);
        self
    }

    /// Registers the segment sink that receives rejects. Each data sink gets
    /// its own instance (component id `quarantine.<sink>`, directory
    /// `data_dir/quarantine/<sink>`), built from the `quarantine:` block as
    /// `{"format": ..., "max_disk": <bytes>}`. Without one, rejects are counted
    /// and dropped.
    pub fn quarantine_sink(&mut self, f: Box<dyn SegmentSinkFactory>) -> &mut Self {
        self.quarantine = Some(f);
        self
    }

    /// The registered kinds, for [`thadam_config::CheckOptions::kinds`] so
    /// `thadam validate` refuses unknown kinds.
    pub fn kind_policy(&self) -> KindPolicy {
        let sinks: Vec<&str> = self
            .segment_sinks
            .keys()
            .chain(self.batch_sinks.keys())
            .chain(self.services.keys())
            .copied()
            .collect();
        KindPolicy::new()
            .sources(self.sources.keys().copied())
            .sinks(sinks)
    }

    pub(crate) fn source_factory(&self, kind: &str) -> Option<&dyn SourceFactory> {
        self.sources.get(kind).map(|f| f.as_ref())
    }

    pub(crate) fn quarantine_factory(&self) -> Option<&dyn SegmentSinkFactory> {
        self.quarantine.as_deref()
    }

    /// Every registry table that knows `kind`.
    pub(crate) fn sink_factories(&self, kind: &str) -> Vec<SinkFactory<'_>> {
        let mut found = Vec::new();
        if let Some(f) = self.segment_sinks.get(kind) {
            found.push(SinkFactory::Segment(f.as_ref()));
        }
        if let Some(f) = self.batch_sinks.get(kind) {
            found.push(SinkFactory::Batch(f.as_ref()));
        }
        if let Some(f) = self.services.get(kind) {
            found.push(SinkFactory::Service(f.as_ref()));
        }
        found
    }
}
