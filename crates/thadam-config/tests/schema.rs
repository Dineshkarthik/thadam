//! JSON Schema snapshot and structural checks.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use thadam_config::json_schema;
use serde_json::Value;

#[test]
fn schema_snapshot() {
    insta::assert_snapshot!(serde_json::to_string_pretty(&json_schema()).unwrap());
}

/// Every schema object with named properties rejects unknown keys, like
/// `deny_unknown_fields` does (the opaque connector `config` has none).
#[test]
fn every_object_with_properties_rejects_unknown_keys() {
    fn walk(v: &Value, path: &str, checked: &mut usize) {
        match v {
            Value::Object(map) => {
                if map.contains_key("properties") && map.get("type").is_some_and(|t| t == "object")
                {
                    *checked += 1;
                    assert_eq!(
                        map.get("additionalProperties"),
                        Some(&Value::Bool(false)),
                        "{path} accepts unknown keys"
                    );
                }
                for (k, child) in map {
                    walk(child, &format!("{path}/{k}"), checked);
                }
            }
            Value::Array(items) => {
                for (i, child) in items.iter().enumerate() {
                    walk(child, &format!("{path}/{i}"), checked);
                }
            }
            _ => {}
        }
    }
    let mut checked = 0;
    walk(&json_schema(), "", &mut checked);
    assert!(checked >= 15, "only {checked} object schemas found");
}

#[test]
fn required_keys_match_the_documented_rules() {
    let schema = json_schema();
    let required = |def: &str| -> Vec<String> {
        schema["$defs"][def]["required"]
            .as_array()
            .unwrap_or_else(|| panic!("{def} has no required list"))
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect()
    };
    assert!(
        required("WalSpec").contains(&"on_full".to_owned()),
        "on_full has no default"
    );
    assert!(required("WalSpec").contains(&"max_disk".to_owned()));
    assert!(!required("WalSpec").contains(&"fsync".to_owned()));
    let top: Vec<_> = schema["required"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    for k in ["version", "pipeline", "sources", "wal", "datasets", "sinks"] {
        assert!(top.contains(&k), "{k}");
    }
    assert!(!top.contains(&"transforms"));
    assert_eq!(schema["properties"]["version"]["minimum"], 1);
    assert_eq!(schema["properties"]["version"]["maximum"], 1);
}

#[test]
fn transform_kinds_are_listed() {
    let text = json_schema()["$defs"]["TransformSpec"].to_string();
    for kind in [
        "validate",
        "on_change",
        "filter",
        "sample",
        "enrich",
        "rename",
        "project",
        "cast",
        "pose",
        "joint_state",
        "window",
    ] {
        assert!(text.contains(kind), "{kind}");
    }
    assert!(text.contains("\"where\""));
    assert!(
        !text.contains("\"expr\""),
        "`expr` is reserved, not accepted"
    );
}
