//! JSON Schema output for editors.

use serde_json::Value;

use crate::spec::PipelineSpec;

/// The JSON Schema (draft 2020-12) of a configuration file.
///
/// Connector `config` blocks are opaque here; their schemas come from each
/// connector factory's `config_schema`. Durations, sizes, counts, and field
/// types are described as strings (sizes and counts also as integers).
pub fn json_schema() -> Value {
    let schema = schemars::schema_for!(PipelineSpec);
    // A schema is plain data and always converts; the fallback keeps this infallible.
    let mut value = serde_json::to_value(&schema).unwrap_or(Value::Null);
    if let Value::Object(map) = &mut value {
        map.insert(
            "title".to_owned(),
            Value::String("Thadam pipeline configuration".to_owned()),
        );
    }
    value
}
