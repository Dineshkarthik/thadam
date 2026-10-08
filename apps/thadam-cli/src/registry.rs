//! The connector registry builder.
//!
//! This module provides a helper to construct the [`Registry`] with all
//! source and sink factories registered.

use thadam_runtime::Registry;

/// Build a [`Registry`] with all source and sink factories registered.
///
/// This function creates a new registry and registers:
/// - MQTT source factory ([`thadam_mqtt::MqttSourceFactory`])
/// - Parquet segment sink factory
/// - Object store batch sink factory
///
/// The registry can be reused by multiple commands (e.g., `validate`, `run`).
pub fn build_registry() -> Registry {
    let mut registry = Registry::new();

    // Register MQTT source factory
    registry.source(Box::new(thadam_mqtt::MqttSourceFactory::new()));

    // Register sink factories (stubs for now - these crates are placeholders)
    // TODO: Add actual factory implementations when thadam-segments and
    // thadam-object-store are implemented
    registry.segment_sink(Box::new(ParquetSinkFactory));
    registry.batch_sink(Box::new(ObjectStoreSinkFactory));

    registry
}

/// Stub Parquet segment sink factory.
///
/// This is a placeholder until thadam-segments is implemented.
struct ParquetSinkFactory;

impl thadam_core::SegmentSinkFactory for ParquetSinkFactory {
    fn kind(&self) -> &'static str {
        "parquet"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "title": "Parquet segment sink",
            "description": "Placeholder - not yet implemented"
        })
    }

    fn validate(&self, _cfg: &serde_json::Value) -> Result<(), Vec<thadam_core::ThadamError>> {
        Ok(())
    }

    fn build(
        &self,
        _cfg: serde_json::Value,
        _ctx: &thadam_core::BuildContext,
    ) -> Result<Box<dyn thadam_core::SegmentSink>, thadam_core::ThadamError> {
        Err(thadam_core::ThadamError::permanent(
            thadam_core::ErrorDomain::Connector,
            thadam_core::codes::CONNECTOR_CONNECT_FAILED,
            "Parquet sink not yet implemented",
        ))
    }
}

/// Stub Object Store batch sink factory.
///
/// This is a placeholder until thadam-object-store is implemented.
struct ObjectStoreSinkFactory;

impl thadam_core::BatchSinkFactory for ObjectStoreSinkFactory {
    fn kind(&self) -> &'static str {
        "object_store"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "title": "Object store batch sink",
            "description": "Placeholder - not yet implemented"
        })
    }

    fn validate(&self, _cfg: &serde_json::Value) -> Result<(), Vec<thadam_core::ThadamError>> {
        Ok(())
    }

    fn build(
        &self,
        _cfg: serde_json::Value,
        _ctx: &thadam_core::BuildContext,
    ) -> Result<Box<dyn thadam_core::BatchSink>, thadam_core::ThadamError> {
        Err(thadam_core::ThadamError::permanent(
            thadam_core::ErrorDomain::Connector,
            thadam_core::codes::CONNECTOR_CONNECT_FAILED,
            "Object store sink not yet implemented",
        ))
    }
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
