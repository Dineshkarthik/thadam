//! The connector registry builder.
//!
//! Copied from `apps/thadam-cli/src/registry.rs` (the CLI is evolving in
//! parallel and must not be edited from here). Keep in sync: the SDK builds
//! the same registry so `validate` and `Pipeline` accept the same connector
//! kinds as the CLI.

use thadam_runtime::Registry;

/// Build a [`Registry`] with all source and sink factories registered.
///
/// Registers:
/// - MQTT source factory ([`thadam_mqtt::MqttSourceFactory`])
/// - HTTP source factory ([`thadam_http::HttpSourceFactory`])
/// - gRPC source factory ([`thadam_grpc::GrpcSourceFactory`])
/// - Parquet segment sink factory ([`thadam_segments::ParquetSegmentSinkFactory`])
/// - Quarantine segment sink factory ([`thadam_segments::QuarantineSinkFactory`])
/// - Object-store shipper service factory
///   ([`thadam_object_store::ObjectStoreShipperFactory`])
pub fn build_registry() -> Registry {
    let mut registry = Registry::new();

    // Register source factories
    registry.source(Box::new(thadam_mqtt::MqttSourceFactory::new()));
    registry.source(Box::new(thadam_http::HttpSourceFactory::new()));
    registry.source(Box::new(thadam_grpc::GrpcSourceFactory::new()));

    registry.segment_sink(Box::new(thadam_segments::ParquetSegmentSinkFactory::new()));
    registry.quarantine_sink(Box::new(thadam_segments::QuarantineSinkFactory::new()));

    // The shipper is configured as a sinks: entry (`kind: object_store`) and
    // runs as a service consuming the named segment sink's catalog.
    registry.service(Box::new(
        thadam_object_store::ObjectStoreShipperFactory::new(),
    ));

    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_all_kinds() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        let has = |list: &Option<Vec<String>>, kind: &str| {
            list.as_ref().is_some_and(|k| k.iter().any(|s| s == kind))
        };
        let sources = policy.source_kinds.clone();
        let sinks = policy.sink_kinds.clone();
        assert!(has(&sources, "mqtt"));
        assert!(has(&sources, "http"));
        assert!(has(&sources, "grpc"));
        assert!(has(&sinks, "parquet"));
        assert!(has(&sinks, "object_store"));
    }
}
