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
/// - gRPC source factory ([`thadam_grpc::GrpcSourceFactory`])
/// - MCAP file source factory ([`thadam_mcap::McapSourceFactory`])
/// - Modbus TCP source factory ([`thadam_modbus::ModbusSourceFactory`])
/// - SocketCAN source factory ([`thadam_can::CanSourceFactory`])
/// - OPC UA subscription source factory (`thadam_opcua::OpcUaSourceFactory`,
///   behind the default-off `opcua` feature)
/// - Live ROS 2 source factory (`thadam_ros2_live::Ros2LiveSourceFactory`,
///   behind the default-off `ros2-live` feature)
/// - Parquet segment sink factory ([`thadam_segments::ParquetSegmentSinkFactory`])
/// - Quarantine segment sink factory ([`thadam_segments::QuarantineSinkFactory`])
/// - NATS JetStream batch sink factory ([`thadam_nats::NatsSinkFactory`])
/// - Apache Kafka batch sink factory ([`thadam_kafka::KafkaSinkFactory`])
/// - MQTT publish batch sink factory ([`thadam_mqtt::sink::MqttPublishSinkFactory`])
/// - ClickHouse batch sink factory (`thadam_clickhouse::ClickHouseSinkFactory`,
///   behind the default-on `clickhouse` feature)
/// - Object-store shipper service factory
///   ([`thadam_object_store::ObjectStoreShipperFactory`])
///
/// The registry can be reused by multiple commands (e.g., `validate`, `run`).
pub fn build_registry() -> Registry {
    let mut registry = Registry::new();

    // Register source factories
    registry.source(Box::new(thadam_mqtt::MqttSourceFactory::new()));
    registry.source(Box::new(thadam_http::HttpSourceFactory::new()));
    registry.source(Box::new(thadam_grpc::GrpcSourceFactory::new()));
    registry.source(Box::new(thadam_mcap::McapSourceFactory::new()));
    registry.source(Box::new(thadam_modbus::ModbusSourceFactory::new()));
    registry.source(Box::new(thadam_can::CanSourceFactory::new()));
    #[cfg(feature = "opcua")]
    registry.source(Box::new(thadam_opcua::OpcUaSourceFactory::new()));
    #[cfg(feature = "ros2-live")]
    registry.source(Box::new(thadam_ros2_live::Ros2LiveSourceFactory::new()));

    registry.segment_sink(Box::new(thadam_segments::ParquetSegmentSinkFactory::new()));
    registry.quarantine_sink(Box::new(thadam_segments::QuarantineSinkFactory::new()));

    // Batch sinks: JetStream publish with per-event Nats-Msg-Id dedupe.
    registry.batch_sink(Box::new(thadam_nats::NatsSinkFactory::new()));
    // Batch sink: one Kafka record per event, acks=all, at-least-once.
    registry.batch_sink(Box::new(thadam_kafka::KafkaSinkFactory::new()));
    // Batch sink: one MQTT message per event, at-least-once at QoS 1.
    registry.batch_sink(Box::new(thadam_mqtt::sink::MqttPublishSinkFactory::new()));
    // Batch sink: Native inserts into an existing ClickHouse table, deduped by
    // the batch id where the table can.
    #[cfg(feature = "clickhouse")]
    registry.batch_sink(Box::new(thadam_clickhouse::ClickHouseSinkFactory::new()));

    // The shipper is configured as a sinks: entry (`kind: object_store`) and
    // runs as a service consuming the named segment sink's catalog.
    registry.service(Box::new(
        thadam_object_store::ObjectStoreShipperFactory::new(),
    ));
    // The Iceberg registrar (`kind: iceberg`, P2 experimental): same
    // sinks:-entry service shape, registering uploaded segments in tables.
    #[cfg(feature = "iceberg")]
    registry.service(Box::new(thadam_iceberg::IcebergRegistrarFactory::new()));

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
    fn build_registry_registers_grpc_source() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        assert!(
            policy
                .source_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "grpc"))
        );
    }

    #[test]
    fn build_registry_registers_mcap_source() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        assert!(
            policy
                .source_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "mcap"))
        );
    }

    #[test]
    fn build_registry_registers_modbus_source() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        assert!(
            policy
                .source_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "modbus"))
        );
    }

    #[test]
    fn build_registry_registers_can_source() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        assert!(
            policy
                .source_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "can"))
        );
    }

    #[cfg(feature = "opcua")]
    #[test]
    fn build_registry_registers_opcua_source() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        assert!(
            policy
                .source_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "opcua"))
        );
    }

    #[cfg(feature = "ros2-live")]
    #[test]
    fn build_registry_registers_ros2_live_source() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        assert!(
            policy
                .source_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "ros2_live"))
        );
    }

    #[test]
    fn build_registry_registers_nats_batch_sink() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        assert!(
            policy
                .sink_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "nats"))
        );
    }

    #[test]
    fn build_registry_registers_kafka_batch_sink() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        assert!(
            policy
                .sink_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "kafka"))
        );
    }

    #[test]
    fn build_registry_registers_mqtt_publish_batch_sink() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        assert!(
            policy
                .sink_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "mqtt_publish"))
        );
    }

    #[cfg(feature = "clickhouse")]
    #[test]
    fn build_registry_registers_clickhouse_batch_sink() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        assert!(
            policy
                .sink_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "clickhouse"))
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

    #[cfg(feature = "iceberg")]
    #[test]
    fn build_registry_registers_iceberg_service() {
        let registry = build_registry();
        let policy = registry.kind_policy();
        assert!(
            policy
                .sink_kinds
                .as_ref()
                .is_some_and(|k| k.iter().any(|s| s == "iceberg"))
        );
    }

    #[test]
    fn build_registry_returns_registry() {
        let registry = build_registry();
        // Just check we can build a registry - the internal methods are private
        drop(registry);
    }
}
