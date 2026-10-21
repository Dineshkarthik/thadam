//! Parquet field IDs (ADR-0013 "PR13 implementation decisions"): the `id` key on
//! declared fields and identity levels, its diagnostics, and the hand-off to
//! `thadam-arrow`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::Path;

use thadam_arrow::columns::{FIRST_DECLARED_ID, LAST_DECLARED_ID, LEVEL_ID_RANGE};
use thadam_arrow::{DatasetSchema, SchemaError, field_id};
use thadam_config::{
    CheckOptions, Checked, DECLARED_FIELD_ID_RANGE, DatasetSpec, Diagnostic,
    IDENTITY_LEVEL_ID_RANGE, LineCol, MapEnv, PipelineSpec, check_str, json_schema,
};

fn minimal() -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/valid/minimal.yaml"),
    )
    .unwrap()
}

fn example() -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/edge-capture/thadam/pipeline.yaml"),
    )
    .unwrap()
}

fn example_env() -> MapEnv {
    MapEnv::new()
        .with_file("/run/secrets/minio_uploader_key", "AKIA-demo\n")
        .with_file("/run/secrets/minio_uploader_secret", "s3cr3t-demo\n")
}

fn check(yaml: &str) -> Checked {
    check_str(yaml, &MapEnv::new(), &CheckOptions::new())
}

fn load(yaml: &str) -> PipelineSpec {
    let c = check(yaml);
    assert!(c.report.errors.is_empty(), "{}", c.report);
    c.spec.unwrap()
}

fn edit(base: &str, from: &str, to: &str) -> String {
    assert!(base.contains(from), "base lacks `{from}`");
    base.replacen(from, to, 1)
}

/// The 1-based line and column of `needle` within the first line containing `line`.
fn position(yaml: &str, line: &str, needle: &str) -> LineCol {
    let (index, text) = yaml
        .lines()
        .enumerate()
        .find(|(_, l)| l.contains(line))
        .unwrap_or_else(|| panic!("no line containing `{line}`"));
    let column = text
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` is not in `{text}`"));
    LineCol {
        line: u32::try_from(index + 1).unwrap(),
        column: u32::try_from(column + 1).unwrap(),
    }
}

/// Where the value after the first `id: ` on the first line containing `line` starts.
fn id_value(yaml: &str, line: &str) -> LineCol {
    let key = position(yaml, line, "id: ");
    LineCol {
        column: key.column + 4,
        ..key
    }
}

fn only_error(yaml: &str) -> Diagnostic {
    let c = check(yaml);
    assert!(c.spec.is_none());
    assert_eq!(c.report.errors.len(), 1, "{}", c.report);
    c.report.errors.into_iter().next().unwrap()
}

/// Builds the Arrow schema of a dataset from nothing but what the config exposes.
fn schema_of(ds: &DatasetSpec) -> Result<DatasetSchema, SchemaError> {
    let mut builder = DatasetSchema::builder(ds.id.as_str())
        .exploded(ds.explode.is_some())
        .fields(ds.field_defs());
    for (name, id) in ds.level_ids() {
        builder = builder.level(name, id);
    }
    builder.build()
}

mod key_spelling_and_defaults {
    use super::*;

    #[test]
    fn the_id_ranges_match_thadam_arrow() {
        assert_eq!(*DECLARED_FIELD_ID_RANGE.start(), FIRST_DECLARED_ID);
        assert_eq!(*DECLARED_FIELD_ID_RANGE.end(), LAST_DECLARED_ID);
        assert_eq!(IDENTITY_LEVEL_ID_RANGE, LEVEL_ID_RANGE);
    }

    #[test]
    fn ids_are_read_from_the_id_key_on_fields_and_levels() {
        let spec = load(&minimal());
        let ds = &spec.datasets[0];
        let ids: Vec<_> = ds.fields.iter().map(|f| (f.name.as_str(), f.id)).collect();
        assert_eq!(ids, [("temp", 1000), ("level", 1001)]);
        assert_eq!(ds.level_ids(), [("device", 100)]);
        assert_eq!(ds.identity[0].id, 100);
    }

    #[test]
    fn field_defs_carry_the_declared_id() {
        let spec = load(&minimal());
        let defs = spec.datasets[0].field_defs();
        assert_eq!(defs[0].field_id, Some(1000));
        assert_eq!(defs[1].field_id, Some(1001));
        assert_eq!(
            spec.datasets[0].fields[1].to_field_def().field_id,
            Some(1001)
        );
    }

    #[test]
    fn ids_do_not_depend_on_declaration_order() {
        let swapped = edit(
            &minimal(),
            "{ id: 1000, name: temp, type: f32, from: /temp }\n      - { id: 1001, name: level, type: str, from: /level }",
            "{ id: 1001, name: level, type: str, from: /level }\n      - { id: 1000, name: temp, type: f32, from: /temp }",
        );
        let defs = load(&swapped).datasets[0].field_defs();
        let by_name: Vec<_> = defs.iter().map(|d| (&*d.name, d.field_id)).collect();
        assert_eq!(by_name, [("level", Some(1001)), ("temp", Some(1000))]);
    }

    #[test]
    fn identity_level_ids_follow_the_written_order_not_the_id_order() {
        let yaml = edit(
            &minimal(),
            "identity: { device: { id: 100, template: \"{topic.1}\" } }",
            "identity: { zone: { id: 150, template: \"{topic.0}\" }, device: { id: 100, template: \"{topic.1}\" } }",
        );
        let spec = load(&yaml);
        assert_eq!(
            spec.datasets[0].level_ids(),
            [("zone", 150), ("device", 100)]
        );
    }

    #[test]
    fn the_boundary_ids_are_accepted() {
        let yaml = edit(&minimal(), "id: 1000,", "id: 4194303,");
        let yaml = edit(&yaml, "id: 100,", "id: 199,");
        let spec = load(&yaml);
        assert_eq!(spec.datasets[0].fields[0].id, 4_194_303);
        assert_eq!(spec.datasets[0].identity[0].id, 199);
        let spec = load(&minimal());
        assert_eq!(spec.datasets[0].identity[0].id, 100);
        assert_eq!(spec.datasets[0].fields[0].id, 1000);
    }
}

mod diagnostics {
    use super::*;

    #[test]
    fn a_missing_field_id_is_reported_at_the_field_with_a_hint() {
        let yaml = edit(&minimal(), "{ id: 1000, name: temp", "{ name: temp");
        let d = only_error(&yaml);
        assert_eq!(d.code, "config.missing");
        assert_eq!(d.path, "datasets[0].fields[0].id");
        assert_eq!(d.message, "missing required field `id`");
        assert_eq!(d.span, Some(position(&yaml, "name: temp", "{ name")));
        assert!(d.hint.unwrap().contains("id: 1000"));
    }

    #[test]
    fn a_missing_level_id_is_reported_at_the_level_with_a_hint() {
        let yaml = edit(&minimal(), "{ id: 100, template", "{ template");
        let d = only_error(&yaml);
        assert_eq!(d.code, "config.missing");
        assert_eq!(d.path, "datasets[0].identity.device.id");
        assert_eq!(d.span, Some(position(&yaml, "identity:", "device")));
        assert!(d.hint.unwrap().contains("100 to 199"));
    }

    #[test]
    fn a_missing_dataset_id_keeps_its_own_message() {
        let yaml = edit(&minimal(), "  - id: readings\n    match", "  - match");
        let c = check(&yaml);
        let d = c
            .report
            .errors
            .iter()
            .find(|d| d.path == "datasets[0].id")
            .expect("a missing dataset id is reported");
        assert!(d.hint.is_none(), "{d:?}");
    }

    #[test]
    fn the_bare_template_string_is_refused_with_the_new_form() {
        let yaml = edit(
            &minimal(),
            "device: { id: 100, template: \"{topic.1}\" }",
            "device: \"{topic.1}\"",
        );
        let d = only_error(&yaml);
        assert_eq!(d.path, "datasets[0].identity.device");
        assert!(
            d.message.contains("expected a mapping `{ id:"),
            "{}",
            d.message
        );
        assert!(d.hint.unwrap().contains("id: 100, template:"));
        assert_eq!(d.span, Some(position(&yaml, "identity:", "\"{topic.1}\"")));
    }

    #[test]
    fn an_out_of_range_field_id_points_at_the_value() {
        for bad in [999u32, 4_194_304, 100, 0] {
            let yaml = edit(&minimal(), "id: 1001,", &format!("id: {bad},"));
            let d = only_error(&yaml);
            assert_eq!(d.path, "datasets[0].fields[1].id", "{bad}");
            assert_eq!(
                d.message,
                format!("field id {bad} of `level` is outside 1000..=4194303")
            );
            assert_eq!(d.span, Some(id_value(&yaml, "name: level")), "{bad}");
            assert!(d.hint.unwrap().contains("100 to 199"));
        }
    }

    #[test]
    fn an_out_of_range_level_id_points_at_the_value() {
        for bad in [99u32, 200, 1000] {
            let yaml = edit(&minimal(), "id: 100,", &format!("id: {bad},"));
            let d = only_error(&yaml);
            assert_eq!(d.path, "datasets[0].identity.device.id", "{bad}");
            assert_eq!(
                d.message,
                format!("identity level id {bad} of `device` is outside 100..=199")
            );
            assert_eq!(d.span, Some(id_value(&yaml, "identity:")), "{bad}");
        }
    }

    #[test]
    fn a_duplicate_field_id_names_both_fields_and_points_at_the_second() {
        let yaml = edit(&minimal(), "id: 1001,", "id: 1000,");
        let d = only_error(&yaml);
        assert_eq!(d.path, "datasets[0].fields[1].id");
        assert_eq!(
            d.message,
            "duplicate field id 1000: `level` reuses the id of `temp`"
        );
        assert!(d.hint.unwrap().contains("never reused"));
        assert_eq!(d.span, Some(id_value(&yaml, "name: level")));
    }

    #[test]
    fn a_duplicate_level_id_is_reported() {
        let yaml = edit(
            &minimal(),
            "identity: { device: { id: 100, template: \"{topic.1}\" } }",
            "identity: { device: { id: 100, template: \"{topic.1}\" }, zone: { id: 100, template: \"{topic.0}\" } }",
        );
        let d = only_error(&yaml);
        assert_eq!(d.path, "datasets[0].identity.zone.id");
        assert_eq!(
            d.message,
            "duplicate identity level id 100: `zone` reuses the id of `device`"
        );
    }

    #[test]
    fn ids_are_unique_per_dataset_not_across_datasets() {
        let ds = "  - id: readings\n";
        let start = minimal().find(ds).unwrap();
        let end = minimal().find("sinks:").unwrap();
        let block = minimal()[start..end].to_owned();
        let second = block.replacen("id: readings", "id: other", 1);
        let yaml = edit(&minimal(), "sinks:", &format!("{second}sinks:"));
        let spec = load(&yaml);
        assert_eq!(spec.datasets.len(), 2);
        assert_eq!(spec.datasets[1].fields[0].id, spec.datasets[0].fields[0].id);
    }

    #[test]
    fn a_non_integer_id_gets_a_hint() {
        for (written, expect) in [
            ("\"1000\"", "invalid type: string"),
            ("-5", "invalid value: integer `-5`"),
            ("4294967296", "invalid value: integer `4294967296`"),
            ("1000.5", "invalid type: floating point"),
        ] {
            let yaml = edit(&minimal(), "id: 1000,", &format!("id: {written},"));
            let d = only_error(&yaml);
            assert_eq!(d.path, "datasets[0].fields[0].id", "{written}");
            assert!(d.message.contains(expect), "{written}: {}", d.message);
            assert!(d.message.contains("expected a field ID"), "{}", d.message);
            assert!(d.hint.unwrap().contains("id: 1000"), "{written}");
        }
    }

    #[test]
    fn id_errors_do_not_hide_other_errors_in_the_dataset() {
        let yaml = edit(&minimal(), "id: 1000,", "id: 5,");
        let yaml = edit(&yaml, "from: /temp", "from: temp");
        let c = check(&yaml);
        let paths: Vec<_> = c.report.errors.iter().map(|d| d.path.as_str()).collect();
        assert!(paths.contains(&"datasets[0].fields[0].id"), "{paths:?}");
        assert!(paths.contains(&"datasets[0].fields[0].from"), "{paths:?}");
    }

    #[test]
    fn unknown_keys_in_a_level_are_still_reported() {
        let yaml = edit(
            &minimal(),
            "template: \"{topic.1}\" }",
            "template: \"{topic.1}\", colour: red }",
        );
        let d = only_error(&yaml);
        assert_eq!(d.path, "datasets[0].identity.device.colour");
        assert!(d.message.contains("unknown key `colour`"), "{}", d.message);
    }
}

mod hash_and_serialization {
    use super::*;

    #[test]
    fn a_field_id_is_part_of_the_config_hash() {
        let base = load(&minimal()).config_hash();
        let changed = load(&edit(&minimal(), "id: 1001,", "id: 1002,")).config_hash();
        assert_ne!(base, changed);
    }

    #[test]
    fn a_level_id_is_part_of_the_config_hash() {
        let base = load(&minimal()).config_hash();
        let changed = load(&edit(&minimal(), "id: 100,", "id: 101,")).config_hash();
        assert_ne!(base, changed);
    }

    #[test]
    fn the_canonical_form_contains_the_ids() {
        let canonical = load(&minimal()).canonical_json();
        assert!(canonical.contains("\"id\":1000"), "{canonical}");
        assert!(
            canonical.contains("\"device\":{\"id\":100,\"template\":\"{topic.1}\"}"),
            "{canonical}"
        );
    }

    #[test]
    fn the_spec_round_trips_with_ids() {
        let spec = load(&minimal());
        let again = load(&serde_json::to_string(&spec).unwrap());
        assert_eq!(again.datasets, spec.datasets);
        assert_eq!(again.config_hash(), spec.config_hash());
    }

    #[test]
    fn the_schema_requires_ids_and_documents_their_ranges() {
        let schema = json_schema();
        let field = &schema["$defs"]["FieldSpec"];
        assert!(field["required"].as_array().unwrap().contains(&"id".into()));
        assert_eq!(field["properties"]["id"]["minimum"], 1000);
        assert_eq!(field["properties"]["id"]["maximum"], 4_194_303);
        let level = &schema["$defs"]["IdentityLevel"];
        assert!(level["required"].as_array().unwrap().contains(&"id".into()));
        assert_eq!(level["properties"]["id"]["minimum"], 100);
        assert_eq!(level["properties"]["id"]["maximum"], 199);
    }
}

mod thadam_arrow_integration {
    use super::*;

    #[test]
    fn the_example_config_builds_a_dataset_schema_for_every_dataset() {
        let c = check_str(&example(), &example_env(), &CheckOptions::new());
        assert!(c.report.errors.is_empty(), "{}", c.report);
        let spec = c.spec.unwrap();
        assert_eq!(spec.datasets.len(), 6);
        for ds in &spec.datasets {
            let schema = schema_of(ds).unwrap_or_else(|e| panic!("{}: {e}", ds.id));
            assert_eq!(schema.name(), ds.id);
            assert_eq!(schema.is_exploded(), ds.explode.is_some());

            let arrow = schema.arrow_schema();
            for f in &ds.fields {
                let column = arrow.field_with_name(&f.name).unwrap();
                assert_eq!(
                    field_id(column),
                    Some(i32::try_from(f.id).unwrap()),
                    "{}",
                    f.name
                );
            }
            for level in &ds.identity {
                let column = arrow.field_with_name(&level.name).unwrap();
                assert_eq!(
                    field_id(column),
                    Some(i32::try_from(level.id).unwrap()),
                    "{}",
                    level.name
                );
            }
            let levels: Vec<_> = schema
                .levels()
                .iter()
                .map(|l| (&*l.name, l.field_id))
                .collect();
            assert_eq!(levels, ds.level_ids(), "{}", ds.id);
        }
    }

    #[test]
    fn the_frozen_demo_fixture_builds_schemas_too() {
        let yaml = fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/demo/pipeline.yaml"),
        )
        .unwrap();
        let c = check_str(&yaml, &example_env(), &CheckOptions::new());
        assert!(c.report.errors.is_empty(), "{}", c.report);
        for ds in &c.spec.unwrap().datasets {
            schema_of(ds).unwrap_or_else(|e| panic!("{}: {e}", ds.id));
        }
    }

    #[test]
    fn the_schema_fingerprint_follows_the_ids() {
        let a = load(&minimal());
        let b = load(&edit(&minimal(), "id: 1001,", "id: 1002,"));
        assert_ne!(
            schema_of(&a.datasets[0]).unwrap().fingerprint(),
            schema_of(&b.datasets[0]).unwrap().fingerprint()
        );
    }

    /// The loader accepts exactly the IDs `thadam-arrow` accepts.
    #[test]
    fn config_validation_agrees_with_the_arrow_builder_on_field_ids() {
        for id in [
            0u32,
            1,
            99,
            100,
            199,
            200,
            999,
            1000,
            1001,
            4_194_303,
            4_194_304,
            8_388_608,
            u32::MAX,
        ] {
            let yaml = edit(&minimal(), "id: 1000,", &format!("id: {id},"));
            let c = check(&yaml);
            let config_ok = c.report.errors.is_empty();
            if id == 1001 {
                assert!(!config_ok, "1001 duplicates the second field");
                continue;
            }
            let arrow_ok = DatasetSchema::builder("x")
                .field(
                    thadam_core::FieldDef::new("a", thadam_core::FieldType::F32).with_field_id(id),
                )
                .build()
                .is_ok();
            assert_eq!(config_ok, arrow_ok, "field id {id}");
            if let Some(spec) = c.spec {
                assert!(schema_of(&spec.datasets[0]).is_ok(), "field id {id}");
            }
        }
    }

    #[test]
    fn config_validation_agrees_with_the_arrow_builder_on_level_ids() {
        for id in [0u32, 1, 99, 100, 150, 199, 200, 1000, u32::MAX] {
            let yaml = edit(&minimal(), "id: 100,", &format!("id: {id},"));
            let c = check(&yaml);
            let config_ok = c.report.errors.is_empty();
            let arrow_ok = DatasetSchema::builder("x")
                .level("device", id)
                .build()
                .is_ok();
            assert_eq!(config_ok, arrow_ok, "level id {id}");
            if let Some(spec) = c.spec {
                assert!(schema_of(&spec.datasets[0]).is_ok(), "level id {id}");
            }
        }
    }
}
