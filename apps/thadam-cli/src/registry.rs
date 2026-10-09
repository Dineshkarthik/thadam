//! The connector registry builder.
//!
//! This module provides a helper to construct the [`Registry`] with all
//! source and sink factories registered.

use thadam_runtime::Registry;

/// Build a [`Registry`] with all source and sink factories registered.
///
/// This function creates a new registry and registers:
/// - MQTT source factory ([`thadam_mqtt::MqttSourceFactory`])
/// - HTTP source factory ([`thadam_http::HttpSourceFactory`])
/// - Parquet segment sink factory ([`thadam_segments::ParquetSegmentSinkFactory`])
/// - Quarantine segment sink factory ([`thadam_segments::QuarantineSinkFactory`])
/// - Object-store shipper service factory
///   ([`thadam_object_store::ObjectStoreShipperFactory`])
///
/// The registry can be reused by multiple commands (e.g., `validate`, `run`).
pub fn build_registry() -> Registry {
    let mut registry = Registry::new();

    // Register source factories
    registry.source(Box::new(thadam_mqtt::MqttSourceFactory::new()));
    registry.source(Box::new(thadam_http::HttpSourceFactory::new()));

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
    fn build_registry_registers_mqtt_source() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        // Verify mqtt source is registered
        assert!(
            policy
                .source_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "mqtt"))
        );
    }

    #[test]
    fn build_registry_registers_http_source() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        assert!(
            policy
                .source_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "http"))
        );
    }

    #[test]
    fn build_registry_registers_parquet_and_object_store_sinks() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        // Verify sinks are registered
        assert!(
            policy
                .sink_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "parquet"))
        );
        assert!(
            policy
                .sink_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "object_store"))
        );
    }

    #[test]
    fn build_registry_returns_registry() {
        let registry = build_registry();
        // Just check we can build a registry - the internal methods are private
        drop(registry);
    }
}
