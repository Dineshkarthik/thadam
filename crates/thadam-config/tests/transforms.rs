//! The eleven transform kinds, the `null` test, `on_change` deadbands, and RFC 3339
//! time: how they parse, what they check, and the form they serialize to (which
//! is what `thadam-transform` reads).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::Path;

use thadam_config::{
    CheckOptions, Checked, EnrichFormat, MapEnv, OnMissSpec, Predicate, Test, TimeUnit,
    TransformSpec, WatchField, check_str,
};
use serde_json::{Value, json};

fn minimal() -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/valid/minimal.yaml"),
    )
    .unwrap()
}

fn check(yaml: &str) -> Checked {
    check_str(yaml, &MapEnv::new(), &CheckOptions::new())
}

/// `minimal.yaml` plus extra fields, a second sink, and the given
/// `transforms:` lines (one step per entry). The `name`, `position`,
/// `velocity`, and `effort` fields give `joint_state` its defaults.
fn with_steps(steps: &[&str]) -> String {
    let base = minimal()
        .replacen(
            "      - { id: 1001, name: level, type: str, from: /level }\n",
            "      - { id: 1001, name: level, type: str, from: /level }\n      - { id: 1002, name: count, type: i64, from: /count }\n      - { id: 1003, name: wave, type: list<f32>, from: /wave }\n      - { id: 1004, name: narrow, type: i32, from: /narrow }\n      - { id: 1005, name: name, type: list<str>, from: /name }\n      - { id: 1006, name: position, type: list<f64>, from: /position }\n      - { id: 1007, name: velocity, type: list<f64>, from: /velocity }\n      - { id: 1008, name: effort, type: list<f64>, from: /effort }\n",
            1,
        )
        .replacen(
            "sinks:\n",
            &format!(
                "transforms:\n{}sinks:\n",
                steps
                    .iter()
                    .map(|s| format!("  - {s}\n"))
                    .collect::<String>()
            ),
            1,
        );
    format!("{base}  - id: other\n    kind: parquet\n    config: {{ datasets: [readings] }}\n")
}

/// `with_steps` plus the flattened pose fields on `readings`: `header.stamp`
/// (`ts`), `header.frame_id` (`str`), and `pose.*` / `tool_pose.*`
/// position/orientation (`f64`), declared under their dotted names without
/// `from` (the default pointer reads the flattened key), so `pose` steps
/// validate.
fn with_pose_fields(steps: &[&str]) -> String {
    let mut lines = vec![
        "      - { id: 1020, name: header.stamp, type: ts, unit: ns }".to_owned(),
        "      - { id: 1021, name: header.frame_id, type: str }".to_owned(),
    ];
    let parts = [
        ("position", "x"),
        ("position", "y"),
        ("position", "z"),
        ("orientation", "x"),
        ("orientation", "y"),
        ("orientation", "z"),
        ("orientation", "w"),
    ];
    for (i, prefix) in ["pose", "tool_pose"].into_iter().enumerate() {
        for (j, (group, axis)) in parts.iter().enumerate() {
            lines.push(format!(
                "      - {{ id: {}, name: {prefix}.{group}.{axis}, type: f64 }}",
                1022 + i * 7 + j
            ));
        }
    }
    let extra = lines.join("\n") + "\n";
    with_steps(steps).replacen(
        "      - { id: 1008, name: effort, type: list<f64>, from: /effort }\n",
        &format!("      - {{ id: 1008, name: effort, type: list<f64>, from: /effort }}\n{extra}"),
        1,
    )
}

fn errors(steps: &[&str]) -> Vec<String> {
    check(&with_steps(steps))
        .report
        .errors
        .iter()
        .map(|d| format!("{}: {}", d.path, d.message))
        .collect()
}

fn assert_ok(steps: &[&str]) {
    let c = check(&with_steps(steps));
    assert!(c.report.errors.is_empty(), "{}", c.report);
    assert!(c.report.warnings.is_empty(), "{}", c.report);
}

fn assert_one_error(steps: &[&str], path: &str, contains: &str) {
    let errs = errors(steps);
    assert_eq!(errs.len(), 1, "{errs:?}");
    assert!(
        errs[0].starts_with(&format!("{path}: ")) && errs[0].contains(contains),
        "{errs:?}"
    );
}

fn steps_json(steps: &[&str]) -> Vec<Value> {
    let c = check(&with_pose_fields(steps));
    assert!(c.report.errors.is_empty(), "{}", c.report);
    let spec = c.spec.unwrap();
    spec.transforms
        .iter()
        .map(|t| serde_json::to_value(t).unwrap())
        .collect()
}

mod serialization {
    use super::*;

    #[test]
    fn every_kind_serializes_in_the_engine_form() {
        let out = steps_json(&[
            "{ kind: sample, dataset: readings, every_n: 10k, min_interval: 100ms }",
            "{ kind: enrich, dataset: readings, file: t.csv, format: csv, key: { field: device, column: id }, add: [{ name: model, type: str, column: m, unit: x }], on_miss: drop }",
            "{ kind: rename, dataset: readings, fields: { temp: temp_c } }",
            "{ kind: cast, dataset: readings, fields: { temp_c: f64, level: str } }",
            "{ kind: pose, dataset: readings, prefix: tool_pose, frame_field: null, quaternion_tolerance: 0.01 }",
            "{ kind: joint_state, dataset: readings, velocity_field: wave }",
            "{ kind: project, dataset: readings, fields: [temp_c, level] }",
        ]);
        assert_eq!(
            out,
            [
                json!({ "kind": "sample", "dataset": "readings", "every_n": 10_000, "min_interval": "100ms" }),
                json!({
                    "kind": "enrich", "dataset": "readings", "file": "t.csv", "format": "csv",
                    "key": { "field": "device", "column": "id" },
                    "add": [{ "name": "model", "type": "str", "column": "m", "unit": "x" }],
                    "on_miss": "drop"
                }),
                json!({ "kind": "rename", "dataset": "readings", "fields": { "temp": "temp_c" } }),
                json!({ "kind": "cast", "dataset": "readings", "fields": { "temp_c": "f64", "level": "str" } }),
                json!({ "kind": "pose", "dataset": "readings", "prefix": "tool_pose", "frame_field": null, "quaternion_tolerance": 0.01 }),
                json!({ "kind": "joint_state", "dataset": "readings", "velocity_field": "wave" }),
                json!({ "kind": "project", "dataset": "readings", "fields": ["temp_c", "level"] }),
            ]
        );
    }

    #[test]
    fn pose_and_joint_state_defaults_are_omitted_from_the_engine_form() {
        let out = steps_json(&[
            "{ kind: pose, dataset: readings, prefix: pose, quaternion_tolerance: 0.001 }",
            "{ kind: joint_state, dataset: readings, name_field: name, position_field: position, velocity_field: velocity, effort_field: effort }",
        ]);
        assert_eq!(
            out,
            [
                json!({ "kind": "pose", "dataset": "readings" }),
                json!({ "kind": "joint_state", "dataset": "readings" }),
            ]
        );
    }

    #[test]
    fn defaults_are_omitted_so_they_do_not_move_hashes() {
        let out = steps_json(&[
            "{ kind: enrich, dataset: readings, file: t.csv, key: { field: device, column: id }, add: [{ name: model, type: str }], on_miss: 'null' }",
        ]);
        assert_eq!(
            out,
            [json!({
                "kind": "enrich", "dataset": "readings", "file": "t.csv",
                "key": { "field": "device", "column": "id" },
                "add": [{ "name": "model", "type": "str" }]
            })]
        );
    }

    #[test]
    fn the_typed_spec_exposes_what_was_written() {
        let c = check(&with_steps(&[
            "{ kind: enrich, dataset: readings, file: t.yaml, format: yaml, key: { field: device, column: id }, add: [{ name: model, type: str }], on_miss: reject }",
        ]));
        let spec = c.spec.unwrap();
        let TransformSpec::Enrich {
            format, on_miss, ..
        } = &spec.transforms[0]
        else {
            panic!("{:?}", spec.transforms[0])
        };
        assert_eq!(*format, Some(EnrichFormat::Yaml));
        assert_eq!(*on_miss, OnMissSpec::Reject);
        assert_eq!(spec.transforms[0].kind(), "enrich");
        assert_eq!(spec.transforms[0].dataset(), "readings");
    }
}

mod on_change {
    use super::*;

    #[test]
    fn plain_names_and_deadband_entries_parse() {
        let c = check(&with_steps(&[
            "{ kind: on_change, dataset: readings, fields: [level, { field: temp, deadband: 0.5 }, { field: count }] }",
        ]));
        let spec = c.spec.unwrap();
        let TransformSpec::OnChange { fields, .. } = &spec.transforms[0] else {
            panic!()
        };
        assert_eq!(
            fields,
            &[
                WatchField::new("level", 0.0),
                WatchField::new("temp", 0.5),
                WatchField::new("count", 0.0)
            ]
        );
    }

    #[test]
    fn a_zero_deadband_serializes_as_the_plain_name() {
        let out = steps_json(&[
            "{ kind: on_change, dataset: readings, fields: [level, { field: temp, deadband: 0 }, { field: count, deadband: 2 }] }",
        ]);
        assert_eq!(
            out[0]["fields"],
            json!(["level", "temp", { "field": "count", "deadband": 2.0 }])
        );
    }

    #[test]
    fn unknown_keys_in_the_mapping_form_are_refused() {
        assert_one_error(
            &["{ kind: on_change, dataset: readings, fields: [{ field: temp, deadbnd: 1 }] }"],
            "transforms[0].fields[0].deadbnd",
            "unknown key `deadbnd`",
        );
        assert_one_error(
            &["{ kind: on_change, dataset: readings, fields: [{ deadband: 1 }] }"],
            "transforms[0].fields[0].field",
            "missing required field `field`",
        );
    }

    #[test]
    fn deadband_rules_mirror_the_engine() {
        assert_ok(&[
            "{ kind: on_change, dataset: readings, fields: [{ field: wave, deadband: 0.1 }, { field: count, deadband: 3 }] }",
        ]);
        assert_one_error(
            &["{ kind: on_change, dataset: readings, fields: [{ field: level, deadband: 0.5 }] }"],
            "transforms[0].fields[0].deadband",
            "needs a numeric field",
        );
        assert_one_error(
            &["{ kind: on_change, dataset: readings, fields: [{ field: temp, deadband: -1 }] }"],
            "transforms[0].fields[0].deadband",
            "zero or more",
        );
        assert_one_error(
            &[
                "{ kind: on_change, dataset: readings, fields: [temp, { field: temp, deadband: 1 }] }",
            ],
            "transforms[0].fields[1]",
            "listed twice",
        );
        assert_one_error(
            &["{ kind: on_change, dataset: readings, fields: [device] }"],
            "transforms[0].fields[0]",
            "not a declared field",
        );
    }
}

mod null_test {
    use super::*;

    fn filter(where_: &str) -> Checked {
        check(&with_steps(&[&format!(
            "{{ kind: filter, dataset: readings, where: {where_} }}"
        )]))
    }

    fn test_of(c: &Checked) -> Test {
        let spec = c.spec.as_ref().unwrap();
        let TransformSpec::Filter {
            where_: Predicate::Condition(cond),
            ..
        } = &spec.transforms[0]
        else {
            panic!()
        };
        cond.test.clone()
    }

    #[test]
    fn null_takes_a_boolean_on_any_resolvable_name() {
        for field in [
            "temp",
            "device",
            "quality.status",
            "quality.flags",
            "quality.reason",
        ] {
            for (text, value) in [("true", true), ("false", false)] {
                let c = filter(&format!("{{ field: {field}, null: {text} }}"));
                assert!(c.report.errors.is_empty(), "{field}: {}", c.report);
                assert_eq!(test_of(&c), Test::IsNull(value));
            }
        }
    }

    #[test]
    fn null_serializes_as_a_boolean() {
        let c = filter("{ field: temp, null: true }");
        let spec = c.spec.unwrap();
        assert_eq!(
            serde_json::to_value(&spec.transforms[0]).unwrap()["where"],
            json!({ "field": "temp", "null": true })
        );
    }

    #[test]
    fn anything_but_a_boolean_is_refused() {
        for value in ["1", "\"true\"", "'null'", "[true]", "{ a: 1 }"] {
            let c = filter(&format!("{{ field: temp, null: {value} }}"));
            let d = &c.report.errors[0];
            assert_eq!(c.report.errors.len(), 1, "{value}: {}", c.report);
            assert_eq!(d.path, "transforms[0].where.null");
            assert!(
                d.message.contains("`null` takes a boolean"),
                "{}",
                d.message
            );
        }
    }

    #[test]
    fn null_is_one_of_the_exclusive_tests() {
        let c = filter("{ field: temp, null: false, gt: 1 }");
        assert!(c.report.errors[0].message.contains("takes one test"));
        let c = filter("{ field: nope, null: true }");
        assert!(c.report.errors[0].message.contains("not a field"));
        let c = filter("{ field: temp, nul: true }");
        let d = &c.report.errors[0];
        assert!(d.message.contains("in, null, min, max"), "{}", d.message);
        assert_eq!(d.hint.as_deref(), Some("did you mean `null`?"));
    }

    #[test]
    fn a_null_literal_points_at_the_null_test() {
        let c = filter("{ field: temp, eq: null }");
        let d = &c.report.errors[0];
        assert!(d.message.contains("cannot be `null`"));
        assert!(d.hint.as_deref().unwrap().contains("null: true"));
    }
}

mod time {
    use super::*;

    fn with_time(time: &str) -> Checked {
        check(&minimal().replace("{ field: /ts, unit: ms, domain: utc }", time))
    }

    #[test]
    fn rfc3339_and_default_offset_parse_and_round_trip() {
        let c =
            with_time(r#"{ field: /ts, unit: rfc3339, domain: utc, default_offset: "+02:00" }"#);
        assert!(c.report.errors.is_empty(), "{}", c.report);
        let spec = c.spec.unwrap();
        let t = spec.datasets[0].time.as_ref().unwrap();
        assert_eq!(t.unit, TimeUnit::Rfc3339);
        assert_eq!(t.unit.as_str(), "rfc3339");
        assert_eq!(t.default_offset.as_deref(), Some("+02:00"));
        let value = serde_json::to_value(&spec.datasets[0]).unwrap();
        assert_eq!(value["time"]["unit"], "rfc3339");
        assert_eq!(value["time"]["default_offset"], "+02:00");
    }

    #[test]
    fn units_spell_themselves_as_the_engine_does() {
        let spelled: Vec<_> = [
            TimeUnit::S,
            TimeUnit::Ms,
            TimeUnit::Us,
            TimeUnit::Ns,
            TimeUnit::Rfc3339,
        ]
        .iter()
        .map(TimeUnit::as_str)
        .collect();
        assert_eq!(spelled, ["s", "ms", "us", "ns", "rfc3339"]);
    }

    #[test]
    fn default_offset_is_omitted_when_absent() {
        let c = with_time("{ field: /ts, unit: rfc3339, domain: utc }");
        assert!(c.report.errors.is_empty(), "{}", c.report);
        let value = serde_json::to_value(&c.spec.unwrap().datasets[0]).unwrap();
        assert!(value["time"].get("default_offset").is_none());
    }

    #[test]
    fn default_offset_needs_rfc3339() {
        let c = with_time(r#"{ field: /ts, unit: s, domain: utc, default_offset: "+02:00" }"#);
        assert_eq!(c.report.errors.len(), 1, "{}", c.report);
        let d = &c.report.errors[0];
        assert_eq!(d.path, "datasets[0].time.default_offset");
        assert!(d.hint.as_deref().unwrap().contains("unit: rfc3339"));
    }

    #[test]
    fn offsets_the_engine_refuses_are_refused() {
        for bad in [
            "+25:00",
            "+24:00",
            "+02:60",
            "02:00",
            "+2:00",
            "+02:0",
            "+0200:",
            "+02:",
            "+02:00:00",
            "",
            " +02:00",
            "+02:00 ",
            "UTC+2",
            "+ab",
        ] {
            let c = with_time(&format!(
                r#"{{ field: /ts, unit: rfc3339, domain: utc, default_offset: "{bad}" }}"#
            ));
            assert_eq!(c.report.errors.len(), 1, "{bad:?}: {}", c.report);
            assert_eq!(c.report.errors[0].path, "datasets[0].time.default_offset");
        }
    }

    #[test]
    fn offsets_the_engine_accepts_are_accepted() {
        for good in [
            "+02:00", "-05:30", "+0200", "-0530", "+02", "-11", "+23:59", "+00:00", "Z", "z", "UTC",
        ] {
            let c = with_time(&format!(
                r#"{{ field: /ts, unit: rfc3339, domain: utc, default_offset: "{good}" }}"#
            ));
            assert!(c.report.errors.is_empty(), "{good}: {}", c.report);
        }
    }

    #[test]
    fn field_form_serializes_without_attr() {
        let c = with_time("{ field: /header.stamp, unit: ns, domain: utc }");
        assert!(c.report.errors.is_empty(), "{}", c.report);
        let spec = c.spec.unwrap();
        let value = serde_json::to_value(&spec.datasets[0]).unwrap();
        assert_eq!(value["time"]["field"], "/header.stamp");
        assert!(value["time"].get("attr").is_none());
    }

    #[test]
    fn attr_form_parses_and_round_trips() {
        let c = with_time("{ attr: mcap.publish_time, unit: ns, domain: utc, max_skew: 5s }");
        assert!(c.report.errors.is_empty(), "{}", c.report);
        let spec = c.spec.unwrap();
        let t = spec.datasets[0].time.as_ref().unwrap();
        assert_eq!(t.attr.as_deref(), Some("mcap.publish_time"));
        assert_eq!(t.field, None);
        let value = serde_json::to_value(&spec.datasets[0]).unwrap();
        assert_eq!(value["time"]["attr"], "mcap.publish_time");
        assert!(value["time"].get("field").is_none());
    }

    #[test]
    fn field_and_attr_are_mutually_exclusive() {
        for time in [
            "{ field: /ts, attr: mcap.log_time, unit: ms, domain: utc }",
            "{ unit: ms, domain: utc }",
        ] {
            let c = with_time(time);
            assert_eq!(c.report.errors.len(), 1, "{time}: {}", c.report);
            assert_eq!(c.report.errors[0].path, "datasets[0].time");
            assert!(
                c.report.errors[0]
                    .message
                    .contains("exactly one of `field` or `attr`"),
                "{}",
                c.report
            );
        }
    }

    #[test]
    fn empty_attr_is_refused() {
        let c = with_time(r#"{ attr: "", unit: ms, domain: utc }"#);
        assert_eq!(c.report.errors.len(), 1, "{}", c.report);
        assert_eq!(c.report.errors[0].path, "datasets[0].time.attr");
    }

    #[test]
    fn unknown_time_unit_is_refused_for_attr_form() {
        let c = with_time("{ attr: mcap.log_time, unit: fortnights, domain: utc }");
        assert!(c.spec.is_none());
        assert!(
            c.report
                .errors
                .iter()
                .any(|d| d.message.contains("fortnights")),
            "{}",
            c.report
        );
    }
}

mod dedup {
    use super::*;

    const DEDUP: &str = "    dedup: { window: 10m, max_keys: 100k }\n";

    fn with_dedup(event_id: &str) -> Checked {
        check(&minimal().replacen("    time:", &format!("{DEDUP}{event_id}    time:"), 1))
    }

    #[test]
    fn dedup_without_an_event_id_is_an_error() {
        let c = with_dedup("");
        assert!(c.spec.is_none());
        assert!(c.report.warnings.is_empty());
        let d = &c.report.errors[0];
        assert_eq!(c.report.errors.len(), 1);
        assert_eq!(d.path, "datasets[0].dedup");
        assert_eq!(d.code, "config.invalid");
        assert!(d.message.contains("`dedup` requires `event_id.key`"));
        assert!(d.message.contains("ADR-0011 D17"));
    }

    #[test]
    fn dedup_with_an_event_id_loads_cleanly() {
        let c = with_dedup("    event_id: { key: [device, temp] }\n");
        assert!(c.report.errors.is_empty(), "{}", c.report);
        assert!(c.report.warnings.is_empty(), "{}", c.report);
    }
}

mod sample {
    use super::*;

    #[test]
    fn needs_a_rule_and_positive_values() {
        assert_ok(&["{ kind: sample, dataset: readings, every_n: 1 }"]);
        assert_ok(&["{ kind: sample, dataset: readings, min_interval: 1ms }"]);
        assert_ok(&["{ kind: sample, dataset: readings, every_n: 5, min_interval: 1s }"]);
        assert_one_error(
            &["{ kind: sample, dataset: readings }"],
            "transforms[0]",
            "needs `every_n`, `min_interval`, or both",
        );
        assert_one_error(
            &["{ kind: sample, dataset: readings, every_n: 0 }"],
            "transforms[0].every_n",
            "at least 1",
        );
        assert_one_error(
            &["{ kind: sample, dataset: readings, min_interval: 0s }"],
            "transforms[0].min_interval",
            "greater than zero",
        );
    }

    #[test]
    fn unknown_keys_are_refused() {
        assert_one_error(
            &["{ kind: sample, dataset: readings, every: 3 }"],
            "transforms[0].every",
            "unknown key `every`",
        );
    }
}

mod enrich {
    use super::*;

    const KEY: &str = "key: { field: device, column: id }";

    fn enrich(rest: &str) -> String {
        format!("{{ kind: enrich, dataset: readings, {rest} }}")
    }

    #[test]
    fn a_valid_step_loads_without_the_file_existing() {
        assert_ok(&[&enrich(&format!(
            "file: /does/not/exist/devices.csv, {KEY}, add: [{{ name: model, type: str }}, {{ name: installed, type: ts, unit: rfc3339, column: at }}, {{ name: serial, type: i32 }}]"
        ))]);
        assert_ok(&[&enrich(&format!(
            "file: devices.yaml, {KEY}, add: [{{ name: tags, type: 'list<f64>' }}]"
        ))]);
    }

    #[test]
    fn the_key_may_be_an_identity_level_or_a_string_or_integer_field() {
        for field in ["device", "level", "count"] {
            assert_ok(&[&enrich(&format!(
                "file: t.csv, key: {{ field: {field}, column: id }}, add: [{{ name: model, type: str }}]"
            ))]);
        }
        assert_one_error(
            &[&enrich(
                "file: t.csv, key: { field: temp, column: id }, add: [{ name: model, type: str }]",
            )],
            "transforms[0].key.field",
            "must be a string or integer field",
        );
        assert_one_error(
            &[&enrich(
                "file: t.csv, key: { field: quality.reason, column: id }, add: [{ name: model, type: str }]",
            )],
            "transforms[0].key.field",
            "neither a field nor an identity level",
        );
    }

    #[test]
    fn added_fields_are_checked() {
        let one = |add: &str| errors(&[&enrich(&format!("file: t.csv, {KEY}, add: [{add}]"))]);
        assert!(one("{ name: temp, type: f32 }")[0].contains("`temp` is already a field"));
        assert!(one("{ name: device, type: str }")[0].contains("an identity level"));
        assert!(one("{ name: _x, type: str }")[0].contains("not a valid field name"));
        assert!(one("{ name: a, type: str }, { name: a, type: str }")[0].contains("added twice"));
        assert!(
            one("{ name: a, type: str, column: '' }")[0].contains("`column` must not be empty")
        );
        assert!(one("{ name: a, type: str, unit: ' ' }")[0].contains("`unit` must not be empty"));
        assert!(one("{ name: a, type: bytes }")[0].contains("cannot add a field of type `bytes`"));
        assert!(one("{ name: a, type: 'list<ts>' }")[0].contains("cannot add"));
        assert!(one("{ name: a, type: 'list<list<i32>>' }")[0].contains("cannot add"));
        assert!(one("{ name: a, type: ts }")[0].contains("needs `unit`"));
        assert!(one("{ name: a, type: ts, unit: minutes }")[0].contains("unknown time unit"));
        assert!(one("{ name: a, type: 'list<f32>' }")[0].contains("CSV column cannot hold a list"));
        assert_one_error(
            &[&enrich(&format!("file: t.csv, {KEY}, add: []"))],
            "transforms[0].add",
            "at least one field",
        );
    }

    #[test]
    fn file_and_format_are_checked() {
        assert_one_error(
            &[&enrich(&format!(
                "file: '', {KEY}, add: [{{ name: m, type: str }}]"
            ))],
            "transforms[0].file",
            "must not be empty",
        );
        assert_one_error(
            &[&enrich(&format!(
                "file: devices.txt, {KEY}, add: [{{ name: m, type: str }}]"
            ))],
            "transforms[0].file",
            "cannot tell the table format",
        );
        assert_ok(&[&enrich(&format!(
            "file: devices.txt, format: csv, {KEY}, add: [{{ name: m, type: str }}]"
        ))]);
        assert_ok(&[&enrich(&format!(
            "file: d.yml, {KEY}, add: [{{ name: m, type: str }}]"
        ))]);
        assert_one_error(
            &[&enrich(&format!(
                "file: d.csv, format: json, {KEY}, add: [{{ name: m, type: str }}]"
            ))],
            "transforms[0].format",
            "unknown variant `json`",
        );
    }

    #[test]
    fn on_miss_has_three_values() {
        for v in ["'null'", "null", "~", "reject", "drop"] {
            assert_ok(&[&enrich(&format!(
                "file: t.csv, {KEY}, add: [{{ name: m, type: str }}], on_miss: {v}"
            ))]);
        }
        assert_one_error(
            &[&enrich(&format!(
                "file: t.csv, {KEY}, add: [{{ name: m, type: str }}], on_miss: skip"
            ))],
            "transforms[0].on_miss",
            "unknown variant `skip`",
        );
    }
}

mod field_operations {
    use super::*;

    #[test]
    fn rename_checks_old_and_new_names() {
        assert_ok(&[
            "{ kind: rename, dataset: readings, fields: { temp: temp_c, level: severity } }",
        ]);
        assert_ok(&["{ kind: rename, dataset: readings, fields: { temp: temp } }"]);
        assert_one_error(
            &["{ kind: rename, dataset: readings, fields: { tmp: x } }"],
            "transforms[0].fields.tmp",
            "not a declared field",
        );
        assert_one_error(
            &["{ kind: rename, dataset: readings, fields: { device: site } }"],
            "transforms[0].fields.device",
            "not a declared field",
        );
        assert_one_error(
            &["{ kind: rename, dataset: readings, fields: { temp: level } }"],
            "transforms[0].fields.temp",
            "already a field",
        );
        assert_one_error(
            &["{ kind: rename, dataset: readings, fields: { temp: device } }"],
            "transforms[0].fields.temp",
            "an identity level",
        );
        assert_one_error(
            &["{ kind: rename, dataset: readings, fields: { temp: x, level: x } }"],
            "transforms[0].fields.temp",
            "new name of another",
        );
        assert_one_error(
            &["{ kind: rename, dataset: readings, fields: { temp: _x } }"],
            "transforms[0].fields.temp",
            "not a valid field name",
        );
        assert_one_error(
            &["{ kind: rename, dataset: readings, fields: {} }"],
            "transforms[0].fields",
            "at least one entry",
        );
    }

    #[test]
    fn renames_apply_together_so_a_swap_is_allowed() {
        assert_ok(&["{ kind: rename, dataset: readings, fields: { temp: level, level: temp } }"]);
        assert_ok(&["{ kind: rename, dataset: readings, fields: { temp: count, count: temp_c } }"]);
    }

    #[test]
    fn project_checks_names_and_duplicates() {
        assert_ok(&["{ kind: project, dataset: readings, fields: [level, temp] }"]);
        assert_one_error(
            &["{ kind: project, dataset: readings, fields: [temp, levl] }"],
            "transforms[0].fields[1]",
            "not a declared field",
        );
        assert_one_error(
            &["{ kind: project, dataset: readings, fields: [temp, temp] }"],
            "transforms[0].fields[1]",
            "listed twice",
        );
        assert_one_error(
            &["{ kind: project, dataset: readings, fields: [] }"],
            "transforms[0].fields",
            "at least one field",
        );
    }

    #[test]
    fn cast_only_widens() {
        assert_ok(&[
            "{ kind: cast, dataset: readings, fields: { temp: f64, narrow: i64, wave: 'list<f64>', count: i64, level: str } }",
        ]);
        for (field, ty) in [
            ("count", "i32"),
            ("temp", "i32"),
            ("narrow", "f64"),
            ("narrow", "u64"),
            ("level", "i64"),
            ("wave", "'list<f32>x'"),
            ("wave", "'list<i32>'"),
            ("temp", "str"),
        ] {
            let errs = errors(&[&format!(
                "{{ kind: cast, dataset: readings, fields: {{ {field}: {ty} }} }}"
            )]);
            assert_eq!(errs.len(), 1, "{field}: {ty}: {errs:?}");
        }
        assert_one_error(
            &["{ kind: cast, dataset: readings, fields: { count: i32 } }"],
            "transforms[0].fields.count",
            "casts only widen",
        );
        assert_one_error(
            &["{ kind: cast, dataset: readings, fields: { nope: i64 } }"],
            "transforms[0].fields.nope",
            "not a declared field",
        );
        assert_one_error(
            &["{ kind: cast, dataset: readings, fields: {} }"],
            "transforms[0].fields",
            "at least one entry",
        );
    }

    #[test]
    fn the_cast_widening_lattice_matches_the_engine() {
        let widens = |from: &str, to: &str| {
            let yaml = with_steps(&[&format!(
                "{{ kind: cast, dataset: readings, fields: {{ x: '{to}' }} }}"
            )])
            .replace(
                "name: narrow, type: i32",
                &format!("name: x, type: '{from}'"),
            );
            check(&yaml).report.errors.is_empty()
        };
        for (from, to) in [
            ("i8", "i16"),
            ("i8", "i64"),
            ("u8", "u16"),
            ("u8", "i16"),
            ("u32", "i64"),
            ("f32", "f64"),
            ("list<f32>", "list<f64>"),
            ("list<i32>", "list<i64>"),
            ("i32", "i32"),
            ("list<str>", "list<str>"),
        ] {
            assert!(widens(from, to), "{from} -> {to}");
        }
        for (from, to) in [
            ("i16", "i8"),
            ("i8", "u16"),
            ("u64", "i64"),
            ("u8", "i8"),
            ("i32", "f64"),
            ("f64", "f32"),
            ("bool", "u8"),
            ("list<u8>", "list<u16>"),
            ("list<str>", "list<bool>"),
            ("list<f32>", "f64"),
        ] {
            assert!(!widens(from, to), "{from} -> {to}");
        }
    }
}

mod chains {
    use super::*;

    #[test]
    fn later_steps_see_the_fields_earlier_steps_left() {
        assert_ok(&[
            "{ kind: rename, dataset: readings, fields: { temp: temp_c } }",
            "{ kind: filter, dataset: readings, where: { field: temp_c, gt: 0 } }",
            "{ kind: cast, dataset: readings, fields: { temp_c: f64 } }",
            "{ kind: on_change, dataset: readings, fields: [{ field: temp_c, deadband: 0.5 }] }",
            "{ kind: project, dataset: readings, fields: [temp_c, level] }",
        ]);
        assert_one_error(
            &[
                "{ kind: rename, dataset: readings, fields: { temp: temp_c } }",
                "{ kind: filter, dataset: readings, where: { field: temp, gt: 0 } }",
            ],
            "transforms[1].where.field",
            "`temp` is not a field",
        );
        assert_one_error(
            &[
                "{ kind: project, dataset: readings, fields: [temp] }",
                "{ kind: on_change, dataset: readings, fields: [level] }",
            ],
            "transforms[1].fields[0]",
            "not a declared field",
        );
        assert_one_error(
            &[
                "{ kind: cast, dataset: readings, fields: { narrow: i64 } }",
                "{ kind: cast, dataset: readings, fields: { narrow: i32 } }",
            ],
            "transforms[1].fields.narrow",
            "from `i64` to `i32`",
        );
    }

    #[test]
    fn enrich_adds_fields_for_later_steps() {
        assert_ok(&[
            "{ kind: enrich, dataset: readings, file: t.csv, key: { field: device, column: id }, add: [{ name: model, type: str }] }",
            "{ kind: filter, dataset: readings, where: { field: model, ne: x } }",
            "{ kind: rename, dataset: readings, fields: { model: robot_model } }",
        ]);
        assert_one_error(
            &[
                "{ kind: enrich, dataset: readings, file: t.csv, key: { field: device, column: id }, add: [{ name: model, type: str }] }",
                "{ kind: enrich, dataset: readings, file: t.csv, key: { field: device, column: id }, add: [{ name: model, type: str }] }",
            ],
            "transforms[1].add[0].name",
            "already a field",
        );
    }

    #[test]
    fn sinks_have_their_own_chains() {
        assert_ok(&[
            "{ kind: rename, dataset: readings, sinks: [local], fields: { temp: temp_c } }",
            "{ kind: filter, dataset: readings, sinks: [local], where: { field: temp_c, gt: 0 } }",
            "{ kind: filter, dataset: readings, sinks: [other], where: { field: temp, gt: 0 } }",
        ]);
        let errs = errors(&[
            "{ kind: rename, dataset: readings, sinks: [local], fields: { temp: temp_c } }",
            "{ kind: filter, dataset: readings, where: { field: temp, gt: 0 } }",
        ]);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(
            errs[0].starts_with("transforms[1].where.field: "),
            "{errs:?}"
        );
    }
}

#[test]
fn the_valid_fixture_round_trips_through_its_serialized_form() {
    let yaml = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/valid/transform_kinds.yaml"),
    )
    .unwrap();
    let first = check(&yaml);
    assert!(first.report.errors.is_empty(), "{}", first.report);
    let spec = first.spec.unwrap();
    assert_eq!(spec.transforms.len(), 9);
    let again = check(&serde_json::to_string(&spec).unwrap());
    assert!(again.report.errors.is_empty(), "{}", again.report);
    let again = again.spec.unwrap();
    assert_eq!(again, spec);
    assert_eq!(again.config_hash(), spec.config_hash());
}

mod pose {
    use super::*;

    fn errs(steps: &[&str]) -> Vec<String> {
        check(&with_pose_fields(steps))
            .report
            .errors
            .iter()
            .map(|d| format!("{}: {}", d.path, d.message))
            .collect()
    }

    fn assert_ok_pose(steps: &[&str]) {
        let c = check(&with_pose_fields(steps));
        assert!(c.report.errors.is_empty(), "{}", c.report);
        assert!(c.report.warnings.is_empty(), "{}", c.report);
    }

    fn assert_one_pose_error(steps: &[&str], path: &str, contains: &str) {
        let errs = errs(steps);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(
            errs[0].starts_with(&format!("{path}: ")) && errs[0].contains(contains),
            "{errs:?}"
        );
    }

    #[test]
    fn a_default_step_loads_and_an_explicit_null_skips_the_frame_check() {
        assert_ok_pose(&["{ kind: pose, dataset: readings }"]);
        assert_ok_pose(&["{ kind: pose, dataset: readings, frame_field: null }"]);
        assert_ok_pose(&[
            "{ kind: pose, dataset: readings, sinks: [local], prefix: tool_pose, frame_field: header.frame_id, quaternion_tolerance: 0.01 }",
        ]);
    }

    #[test]
    fn the_typed_spec_keeps_the_three_states_of_frame_field() {
        let c = check(&with_pose_fields(&[
            "{ kind: pose, dataset: readings, frame_field: null }",
        ]));
        let spec = c.spec.unwrap();
        let TransformSpec::Pose { frame_field, .. } = &spec.transforms[0] else {
            panic!("{:?}", spec.transforms[0])
        };
        assert_eq!(*frame_field, Some(None));

        let c = check(&with_pose_fields(&["{ kind: pose, dataset: readings }"]));
        let spec = c.spec.unwrap();
        let TransformSpec::Pose {
            prefix,
            frame_field,
            quaternion_tolerance,
            ..
        } = &spec.transforms[0]
        else {
            panic!("{:?}", spec.transforms[0])
        };
        assert_eq!(prefix, "pose");
        assert_eq!(*frame_field, None);
        assert_eq!(*quaternion_tolerance, 1e-3);
        assert_eq!(spec.transforms[0].kind(), "pose");
        assert_eq!(spec.transforms[0].dataset(), "readings");
    }

    #[test]
    fn scalar_options_mirror_the_engine_config_errors() {
        for tol in ["0", "1", "-0.5", "2"] {
            assert_one_pose_error(
                &[&format!(
                    "{{ kind: pose, dataset: readings, quaternion_tolerance: {tol} }}"
                )],
                "transforms[0].quaternion_tolerance",
                "greater than 0 and less than 1",
            );
        }
        assert_one_pose_error(
            &["{ kind: pose, dataset: readings, frame_field: '' }"],
            "transforms[0].frame_field",
            "`frame_field` must not be empty",
        );
    }

    #[test]
    fn an_empty_prefix_reports_the_prefix_and_each_unknown_component() {
        let errs = errs(&["{ kind: pose, dataset: readings, prefix: '' }"]);
        assert_eq!(errs.len(), 8, "{errs:?}");
        assert!(
            errs[0].starts_with("transforms[0].prefix: ")
                && errs[0].contains("`prefix` must not be empty"),
            "{errs:?}"
        );
        assert!(
            errs[1].contains("`.position.x` is not a declared field"),
            "{errs:?}"
        );
    }

    #[test]
    fn unknown_keys_are_refused() {
        assert_one_error(
            &["{ kind: pose, dataset: readings, toleranc: 1 }"],
            "transforms[0].toleranc",
            "unknown key `toleranc`",
        );
    }

    #[test]
    fn every_missing_component_is_reported_at_the_prefix() {
        let errs = errs(&["{ kind: pose, dataset: readings, prefix: no_such }"]);
        assert_eq!(errs.len(), 7, "{errs:?}");
        for (group, axes) in [
            ("position", &["x", "y", "z"][..]),
            ("orientation", &["x", "y", "z", "w"][..]),
        ] {
            for axis in axes {
                let want = format!(
                    "transforms[0].prefix: `no_such.{group}.{axis}` is not a declared field of dataset `readings`"
                );
                assert!(errs.contains(&want), "{want} in {errs:?}");
            }
        }
    }

    #[test]
    fn a_missing_frame_field_is_reported_at_the_frame_field_key() {
        assert_one_pose_error(
            &["{ kind: pose, dataset: readings, frame_field: nope.frame }"],
            "transforms[0].frame_field",
            "`nope.frame` is not a declared field of dataset `readings`",
        );
    }

    #[test]
    fn component_and_frame_types_are_checked() {
        let bad_axis = with_pose_fields(&["{ kind: pose, dataset: readings, prefix: tool_pose }"])
            .replacen(
                "name: tool_pose.position.x, type: f64",
                "name: tool_pose.position.x, type: i64",
                1,
            );
        let errs: Vec<String> = check(&bad_axis)
            .report
            .errors
            .iter()
            .map(|d| format!("{}: {}", d.path, d.message))
            .collect();
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(
            errs[0].starts_with("transforms[0].prefix: ")
                && errs[0].contains("`tool_pose.position.x` must be f32 or f64")
                && errs[0].contains("`i64`"),
            "{errs:?}"
        );

        let bad_frame = with_pose_fields(&["{ kind: pose, dataset: readings }"]).replacen(
            "name: header.frame_id, type: str",
            "name: header.frame_id, type: f64",
            1,
        );
        let errs: Vec<String> = check(&bad_frame)
            .report
            .errors
            .iter()
            .map(|d| format!("{}: {}", d.path, d.message))
            .collect();
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(
            errs[0].starts_with("transforms[0].frame_field: ")
                && errs[0].contains("`frame_field` must name a str field")
                && errs[0].contains("`header.frame_id` is `f64`"),
            "{errs:?}"
        );
    }
}

mod joint_state {
    use super::*;

    #[test]
    fn defaults_and_custom_field_names_load() {
        assert_ok(&["{ kind: joint_state, dataset: readings }"]);
        assert_ok(&[
            "{ kind: joint_state, dataset: readings, sinks: [other], name_field: name, position_field: position, velocity_field: wave, effort_field: effort }",
        ]);
    }

    #[test]
    fn the_typed_spec_exposes_what_was_written() {
        let c = check(&with_steps(&[
            "{ kind: joint_state, dataset: readings, velocity_field: wave }",
        ]));
        let spec = c.spec.unwrap();
        let TransformSpec::JointState {
            name_field,
            velocity_field,
            ..
        } = &spec.transforms[0]
        else {
            panic!("{:?}", spec.transforms[0])
        };
        assert_eq!(name_field, "name");
        assert_eq!(velocity_field, "wave");
        assert_eq!(spec.transforms[0].kind(), "joint_state");
        assert_eq!(spec.transforms[0].dataset(), "readings");
    }

    #[test]
    fn fields_must_be_declared_and_of_the_right_shape() {
        assert_one_error(
            &["{ kind: joint_state, dataset: readings, position_field: boring }"],
            "transforms[0].position_field",
            "not a declared field",
        );
        assert_one_error(
            &["{ kind: joint_state, dataset: readings, name_field: position }"],
            "transforms[0].name_field",
            "must name a list<str> field",
        );
        assert_one_error(
            &["{ kind: joint_state, dataset: readings, velocity_field: level }"],
            "transforms[0].velocity_field",
            "must name a list<f32> or list<f64> field",
        );
        assert_one_error(
            &["{ kind: joint_state, dataset: readings, effort_field: count }"],
            "transforms[0].effort_field",
            "must name a list<f32> or list<f64> field",
        );
    }

    #[test]
    fn field_names_must_not_be_empty_and_unknown_keys_are_refused() {
        assert_one_error(
            &["{ kind: joint_state, dataset: readings, name_field: '' }"],
            "transforms[0].name_field",
            "`name_field` must not be empty",
        );
        assert_one_error(
            &["{ kind: joint_state, dataset: readings, effort_field: '' }"],
            "transforms[0].effort_field",
            "`effort_field` must not be empty",
        );
        assert_one_error(
            &["{ kind: joint_state, dataset: readings, names: name }"],
            "transforms[0].names",
            "unknown key `names`",
        );
    }

    #[test]
    fn later_steps_see_the_fields_joint_state_kept() {
        assert_ok(&[
            "{ kind: joint_state, dataset: readings, velocity_field: wave }",
            "{ kind: filter, dataset: readings, where: { field: position, null: false } }",
        ]);
    }
}

mod window {
    use super::*;

    const OK: &str = "{ kind: window, dataset: readings, size: 1m, aggregates: [{ field: temp, fn: mean, as: temp_mean }, { field: level, fn: count, as: n }] }";

    #[test]
    fn a_valid_step_checks_and_serializes_in_the_engine_form() {
        assert_ok(&[OK]);
        assert_eq!(
            steps_json(&[OK]),
            [json!({
                "kind": "window", "dataset": "readings", "size": "1m",
                "aggregates": [
                    { "field": "temp", "fn": "mean", "as": "temp_mean" },
                    { "field": "level", "fn": "count", "as": "n" },
                ],
            })]
        );
        let spec = check(&with_steps(&[OK])).spec.unwrap();
        assert_eq!(spec.transforms[0].kind(), "window");
        assert_eq!(spec.transforms[0].dataset(), "readings");

        let with_idle = "{ kind: window, dataset: readings, size: 1m, idle_close: 30s, aggregates: [{ field: temp, fn: mean, as: temp_mean }] }";
        assert_ok(&[with_idle]);
        assert_eq!(
            steps_json(&[with_idle]),
            [json!({
                "kind": "window", "dataset": "readings", "size": "1m", "idle_close": "30s",
                "aggregates": [{ "field": "temp", "fn": "mean", "as": "temp_mean" }],
            })]
        );

        let with_ingest = "{ kind: window, dataset: readings, size: 1m, time: ingest, aggregates: [{ field: temp, fn: mean, as: temp_mean }] }";
        assert_ok(&[with_ingest]);
        assert_eq!(
            steps_json(&[with_ingest]),
            [json!({
                "kind": "window", "dataset": "readings", "size": "1m",
                "aggregates": [{ "field": "temp", "fn": "mean", "as": "temp_mean" }],
            })],
            "default `time: ingest` is omitted so existing configs keep their hash"
        );

        let with_event = "{ kind: window, dataset: readings, size: 1m, time: event, allowed_lateness: 5s, aggregates: [{ field: temp, fn: mean, as: temp_mean }] }";
        assert_ok(&[with_event]);
        assert_eq!(
            steps_json(&[with_event]),
            [json!({
                "kind": "window", "dataset": "readings", "size": "1m",
                "time": "event", "allowed_lateness": "5s",
                "aggregates": [{ "field": "temp", "fn": "mean", "as": "temp_mean" }],
            })]
        );
        assert_ok(&[
            "{ kind: window, dataset: readings, size: 1m, time: event, allowed_lateness: 0s, aggregates: [{ field: temp, fn: sum, as: s }] }",
        ]);
    }

    #[test]
    fn size_and_aggregates_are_checked() {
        assert_one_error(
            &[
                "{ kind: window, dataset: readings, size: 0s, aggregates: [{ field: temp, fn: sum, as: s }] }",
            ],
            "transforms[0].size",
            "greater than zero",
        );
        assert_one_error(
            &[
                "{ kind: window, dataset: readings, size: 1m, idle_close: 0s, aggregates: [{ field: temp, fn: sum, as: s }] }",
            ],
            "transforms[0].idle_close",
            "greater than zero",
        );
        assert_one_error(
            &["{ kind: window, dataset: readings, size: 1m, aggregates: [] }"],
            "transforms[0].aggregates",
            "at least one",
        );
        assert_one_error(
            &[
                "{ kind: window, dataset: readings, size: 1m, aggregates: [{ field: nope, fn: sum, as: s }] }",
            ],
            "transforms[0].aggregates[0].field",
            "not a declared field",
        );
        assert_one_error(
            &[
                "{ kind: window, dataset: readings, size: 1m, aggregates: [{ field: level, fn: max, as: s }] }",
            ],
            "transforms[0].aggregates[0].field",
            "needs a numeric field",
        );
        assert_one_error(
            &[
                "{ kind: window, dataset: readings, size: 1m, aggregates: [{ field: count, fn: sum, as: s }] }",
            ],
            "transforms[0].aggregates[0].field",
            "other than i64 and u64",
        );
        assert_ok(&[
            "{ kind: window, dataset: readings, size: 1m, aggregates: [{ field: count, fn: count, as: n }] }",
        ]);
        assert_one_error(
            &[
                "{ kind: window, dataset: readings, size: 1m, aggregates: [{ field: temp, fn: sum, as: window_start }] }",
            ],
            "transforms[0].aggregates[0].as",
            "repeated, or reserved",
        );
        assert_one_error(
            &[
                "{ kind: window, dataset: readings, size: 1m, aggregates: [{ field: temp, fn: sum, as: s }, { field: temp, fn: max, as: s }] }",
            ],
            "transforms[0].aggregates[1].as",
            "repeated, or reserved",
        );
        assert_one_error(
            &[
                "{ kind: window, dataset: readings, size: 1m, aggregates: [{ field: temp, fn: median, as: s }] }",
            ],
            "transforms[0].aggregates[0].fn",
            "median",
        );
        assert_one_error(
            &[
                "{ kind: window, dataset: readings, size: 1m, allowed_lateness: 5s, aggregates: [{ field: temp, fn: sum, as: s }] }",
            ],
            "transforms[0].allowed_lateness",
            "applies only to `time: event` windows",
        );
        assert_one_error(
            &[
                "{ kind: window, dataset: readings, size: 1m, time: ingest, allowed_lateness: 5s, aggregates: [{ field: temp, fn: sum, as: s }] }",
            ],
            "transforms[0].allowed_lateness",
            "applies only to `time: event` windows",
        );
        assert_one_error(
            &[
                "{ kind: window, dataset: readings, size: 1m, time: wall, aggregates: [{ field: temp, fn: sum, as: s }] }",
            ],
            "transforms[0].time",
            "unknown variant `wall`",
        );
    }

    #[test]
    fn later_steps_see_the_window_columns_only() {
        assert_ok(&[
            OK,
            "{ kind: filter, dataset: readings, where: { field: temp_mean, gt: 90 } }",
            "{ kind: filter, dataset: readings, where: { field: window_start, null: false } }",
            "{ kind: rename, dataset: readings, fields: { n: samples } }",
        ]);
        assert_one_error(
            &[
                OK,
                "{ kind: filter, dataset: readings, where: { field: temp, gt: 0 } }",
            ],
            "transforms[1].where.field",
            "`temp` is not a field",
        );
    }
}

#[test]
fn unknown_kinds_list_all_eleven() {
    let c = check(&with_steps(&["{ kind: sampel, dataset: readings }"]));
    let d = &c.report.errors[0];
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
        assert!(d.message.contains(&format!("`{kind}`")), "{}", d.message);
    }
    assert_eq!(d.hint.as_deref(), Some("did you mean `sample`?"));
}
