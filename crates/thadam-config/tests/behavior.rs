//! Behavior tests: interpolation, scalar hazards, predicates, core integration,
//! the memory estimate, and file loading.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::Path;

use thadam_config::{
    CheckOptions, Checked, FsyncSpec, Literal, MapEnv, OnFull, Predicate, ProcessEnv, Test,
    TransformSpec, UnresolvedPolicy, check_str, load_file, load_str,
};
use thadam_core::{FieldType, Shape};

fn minimal() -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/valid/minimal.yaml"),
    )
    .unwrap()
}

fn demo() -> String {
    fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/edge-capture/thadam/pipeline.yaml"),
    )
    .unwrap()
}

fn demo_env() -> MapEnv {
    MapEnv::new()
        .with_file("/run/secrets/minio_uploader_key", "AKIA-demo\n")
        .with_file("/run/secrets/minio_uploader_secret", "s3cr3t-demo\r\n")
}

fn edit(base: &str, from: &str, to: &str) -> String {
    assert!(base.contains(from), "base lacks `{from}`");
    base.replacen(from, to, 1)
}

fn check(yaml: &str) -> Checked {
    check_str(yaml, &MapEnv::new(), &CheckOptions::new())
}

fn errors(yaml: &str) -> Vec<String> {
    check(yaml)
        .report
        .errors
        .iter()
        .map(|d| format!("{}: {}", d.path, d.message))
        .collect()
}

mod interpolation {
    use super::*;

    fn sink_url(yaml_value: &str, env: &MapEnv) -> Checked {
        let yaml = edit(
            &minimal(),
            "config: { datasets: [readings] }",
            &format!("config: {{ datasets: [readings], url: {yaml_value} }}"),
        );
        check_str(&yaml, env, &CheckOptions::new())
    }

    fn url_of(checked: &Checked) -> String {
        checked.spec.as_ref().unwrap().sinks[0].config["url"]
            .as_str()
            .unwrap()
            .to_owned()
    }

    #[test]
    fn env_and_file_values_are_substituted() {
        let env = MapEnv::new()
            .with_var("HOST", "broker.local")
            .with_file("/run/p", "pw\n");
        let c = sink_url("\"http://${env:HOST}:9000/${file:/run/p}\"", &env);
        assert_eq!(url_of(&c), "http://broker.local:9000/pw");
    }

    #[test]
    fn a_file_loses_one_trailing_newline_only() {
        let env = MapEnv::new()
            .with_file("/a", "x\n\n")
            .with_file("/b", "y\r\n");
        assert_eq!(url_of(&sink_url("\"${file:/a}\"", &env)), "x\n");
        assert_eq!(url_of(&sink_url("\"${file:/b}\"", &env)), "y");
    }

    #[test]
    fn dollar_dollar_brace_is_a_literal() {
        let c = sink_url("\"a$${env:X}b\"", &MapEnv::new());
        assert_eq!(url_of(&c), "a${env:X}b");
        assert!(c.report.errors.is_empty());
    }

    #[test]
    fn a_lone_dollar_is_left_alone() {
        let c = sink_url("\"cost $5 and $\"", &MapEnv::new());
        assert_eq!(url_of(&c), "cost $5 and $");
    }

    #[test]
    fn values_cannot_change_document_structure() {
        let nasty =
            "x\n  - injected: true\nwal: {max_disk: 1B}\n}, [ \"quoted\" ] : &anchor ${env:HOST}";
        let env = MapEnv::new().with_var("NASTY", nasty).with_var("HOST", "h");
        let c = sink_url("\"${env:NASTY}\"", &env);
        assert!(c.report.errors.is_empty(), "{}", c.report);
        assert_eq!(
            url_of(&c),
            nasty,
            "the value is data and is not expanded again"
        );
        assert_eq!(c.spec.unwrap().sources.len(), 1);
    }

    #[test]
    fn keys_are_not_interpolated() {
        let yaml = edit(
            &minimal(),
            "config: { datasets: [readings] }",
            "config: { datasets: [readings], \"${env:K}\": 1 }",
        );
        let c = check_str(&yaml, &MapEnv::new(), &CheckOptions::new());
        assert!(c.report.errors.is_empty(), "{}", c.report);
        assert!(c.spec.unwrap().sinks[0].config.get("${env:K}").is_some());
    }

    #[test]
    fn typed_fields_accept_interpolated_text() {
        let yaml = edit(&minimal(), "max_disk: 1GiB", "max_disk: \"${env:DISK}\"");
        let env = MapEnv::new().with_var("DISK", "3GiB");
        let spec = check_str(&yaml, &env, &CheckOptions::new()).spec.unwrap();
        assert_eq!(spec.wal.max_disk.get(), 3 << 30);
    }

    #[test]
    fn a_bad_interpolated_value_does_not_leak_into_the_message() {
        let yaml = edit(&minimal(), "max_disk: 1GiB", "max_disk: \"${env:DISK}\"");
        let env = MapEnv::new().with_var("DISK", "hunter2-hunter2");
        let report = check_str(&yaml, &env, &CheckOptions::new()).report;
        assert!(report.has_errors());
        assert!(!report.render("x.yaml").contains("hunter2"), "{report}");
    }

    #[test]
    fn unresolved_references_can_be_downgraded_to_warnings() {
        let yaml = edit(
            &minimal(),
            "config: { datasets: [readings] }",
            "config: { datasets: [readings], url: \"${env:NOPE}\", k: \"${file:/no/such}\" }",
        );
        let c = check_str(
            &yaml,
            &MapEnv::new(),
            &CheckOptions::new().unresolved(UnresolvedPolicy::Placeholder),
        );
        assert!(c.report.errors.is_empty(), "{}", c.report);
        assert_eq!(c.report.warnings.len(), 2);
        let spec = c.spec.unwrap();
        assert_eq!(spec.sinks[0].config["url"], "<unresolved:env:NOPE>");
        assert_eq!(spec.sinks[0].secret_paths.len(), 2);
    }

    #[test]
    fn invalid_environment_names_are_rejected() {
        let c = sink_url("\"${env:1BAD}\"", &MapEnv::new());
        assert!(
            c.report.errors[0]
                .message
                .contains("not a valid environment variable name")
        );
    }

    #[test]
    fn oversized_and_non_utf8_files_are_rejected() {
        let big = vec![b'a'; 70_000];
        let env = MapEnv::new()
            .with_file("/big", big)
            .with_file("/bin", vec![0xff, 0xfe]);
        assert!(
            sink_url("\"${file:/big}\"", &env).report.errors[0]
                .message
                .contains("larger than")
        );
        assert!(
            sink_url("\"${file:/bin}\"", &env).report.errors[0]
                .message
                .contains("UTF-8")
        );
    }

    #[test]
    fn real_files_are_read_through_the_process_environment() {
        let dir = tempfile::tempdir().unwrap();
        let secret = dir.path().join("key");
        fs::write(&secret, "from-disk\n").unwrap();
        let yaml = edit(
            &minimal(),
            "config: { datasets: [readings] }",
            &format!(
                "config: {{ datasets: [readings], token: \"${{file:{}}}\" }}",
                secret.display()
            ),
        );
        let spec = check_str(&yaml, &ProcessEnv, &CheckOptions::new())
            .spec
            .unwrap();
        assert_eq!(spec.sinks[0].config["token"], "from-disk");
    }
}

mod redaction {
    use super::*;

    const KEY: &str = "AKIA-DEMO-KEY-1234";
    const SECRET: &str = "wJalrXUtnFEMI-SECRET-5678";

    fn spec() -> thadam_config::PipelineSpec {
        let env = MapEnv::new()
            .with_file("/run/secrets/minio_uploader_key", KEY)
            .with_file("/run/secrets/minio_uploader_secret", SECRET);
        check_str(&demo(), &env, &CheckOptions::new()).spec.unwrap()
    }

    #[test]
    fn the_resolved_values_are_present_for_connectors() {
        let spec = spec();
        assert_eq!(spec.sinks[1].config["access_key_id"], KEY);
        let secrets = spec.sinks[1].secrets();
        assert_eq!(secrets.len(), 2);
        assert!(
            secrets
                .iter()
                .any(|(p, s)| p == "/secret_access_key" && s.expose() == SECRET)
        );
    }

    #[test]
    fn debug_never_prints_a_secret() {
        let spec = spec();
        for text in [
            format!("{spec:?}"),
            format!("{spec:#?}"),
            format!("{:?}", spec.sinks[1]),
        ] {
            assert!(!text.contains(KEY), "key leaked");
            assert!(!text.contains(SECRET), "secret leaked");
        }
        assert!(format!("{:?}", spec.sinks[1]).contains("<redacted>"));
        assert!(!format!("{:?}", spec.sinks[1].secrets()).contains(SECRET));
    }

    #[test]
    fn serialization_hash_and_canonical_form_never_contain_a_secret() {
        let spec = spec();
        for text in [
            serde_json::to_string(&spec).unwrap(),
            spec.canonical_json(),
            spec.config_hash_hex(),
            serde_json::to_string(&spec.sinks[1].redacted_config()).unwrap(),
        ] {
            assert!(!text.contains(KEY) && !text.contains(SECRET));
        }
        assert!(
            spec.canonical_json()
                .contains("${file:/run/secrets/minio_uploader_key}")
        );
    }

    #[test]
    fn reports_never_contain_a_secret_even_from_files() {
        // The secret is read from a file and lands in a field that fails validation.
        let env = MapEnv::new().with_file("/run/token", "not-a-duration-SECRETVALUE");
        let yaml = edit(
            &minimal(),
            "config: { datasets: [readings] }",
            "config: { datasets: [readings] }",
        );
        let yaml = edit(
            &yaml,
            "on_full: block",
            "on_full: block, retain_for: \"${file:/run/token}\"",
        );
        let c = check_str(&yaml, &env, &CheckOptions::new());
        assert!(c.report.has_errors());
        let text = format!("{}\n{:?}", c.report.render("cfg.yaml"), c.report);
        assert!(!text.contains("SECRETVALUE"), "{text}");
    }

    #[test]
    fn a_secret_in_a_typed_string_field_is_scrubbed_from_errors() {
        let env = MapEnv::new().with_var("NODE", "bad node id with spaces SECRETNODE");
        let yaml = edit(&minimal(), "node_id: n1", "node_id: \"${env:NODE}\"");
        let c = check_str(&yaml, &env, &CheckOptions::new());
        assert!(c.report.has_errors());
        assert!(!format!("{c:?}").contains("SECRETNODE"));
    }
}

mod scalar_hazards {
    use super::*;

    #[test]
    fn yaml_11_boolean_words_stay_text() {
        let yaml = fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/valid/boolean_words.yaml"),
        )
        .unwrap();
        let spec = check(&yaml).spec.unwrap();
        assert_eq!(spec.pipeline.name, "no");
        assert_eq!(
            spec.sinks[0].config["flag"], "yes",
            "a bare `yes` is text, not a boolean"
        );
    }

    #[test]
    fn only_true_and_false_are_booleans() {
        let yaml = edit(
            &minimal(),
            "config: { datasets: [readings] }",
            "config: { datasets: [readings], a: true, b: False, c: off, d: n }",
        );
        let cfg = &check(&yaml).spec.unwrap().sinks[0].config;
        assert_eq!(cfg["a"], true);
        assert_eq!(cfg["b"], false);
        assert_eq!(cfg["c"], "off");
        assert_eq!(cfg["d"], "n");
    }

    #[test]
    fn leading_zero_and_underscores_become_numbers_so_identifiers_must_be_quoted() {
        let yaml = edit(
            &minimal(),
            "config: { datasets: [readings] }",
            "config: { datasets: [readings], zip: 014, big: 1_000, quoted: \"014\" }",
        );
        let cfg = &check(&yaml).spec.unwrap().sinks[0].config;
        assert!(cfg["zip"].is_number());
        assert!(cfg["big"].is_number());
        assert_eq!(cfg["quoted"], "014");
        let e = errors(&edit(&minimal(), "node_id: n1", "node_id: 014"));
        assert_eq!(e.len(), 1);
        assert!(e[0].starts_with("pipeline.node_id: invalid type"), "{e:?}");
    }

    #[test]
    fn a_quoted_version_is_refused() {
        assert!(
            errors(&edit(&minimal(), "version: 1", "version: \"1\""))[0]
                .contains("must be the integer 1")
        );
    }
}

mod versions {
    use super::*;
    use thadam_core::{Classify, codes};

    #[test]
    fn version_1_loads() {
        assert_eq!(load_str(&minimal(), &MapEnv::new()).unwrap().version, 1);
    }

    #[test]
    fn newer_and_older_versions_are_refused_with_the_incompatible_code() {
        for v in ["2", "0", "99"] {
            let report = load_str(
                &edit(&minimal(), "version: 1", &format!("version: {v}")),
                &MapEnv::new(),
            )
            .unwrap_err();
            assert_eq!(report.errors.len(), 1, "{report}");
            assert_eq!(report.errors[0].code, codes::CONFIG_INCOMPATIBLE);
            assert_eq!(report.code(), codes::CONFIG_INCOMPATIBLE);
        }
    }

    #[test]
    fn nothing_else_is_checked_when_the_version_is_unsupported() {
        let yaml = format!(
            "{}\nbogus: 1\n",
            edit(&minimal(), "version: 1", "version: 2")
        );
        assert_eq!(errors(&yaml).len(), 1);
    }
}

mod structure {
    use super::*;

    #[test]
    fn parse_budget_rejects_deep_and_huge_documents_quickly() {
        let deep = format!("version: 1\nx: {}1{}\n", "[".repeat(100), "]".repeat(100));
        assert!(!errors(&deep).is_empty());
        let wide = format!("version: 1\nx: [{}]\n", vec!["1"; 60_000].join(","));
        assert!(!errors(&wide).is_empty());
        let huge = format!("version: 1\n# {}\n", "x".repeat(2 << 20));
        assert!(errors(&huge)[0].contains("larger than"));
    }

    #[test]
    fn a_large_realistic_configuration_fits_the_budget() {
        let mut yaml = minimal();
        for i in 0..40 {
            let fields: String = (0..40)
                .map(|f| format!("      - {{ id: {}, name: f{f}, type: f32, from: /f{f}, unit: mV, convert_to: V }}\n", 1000 + f))
                .collect();
            yaml = edit(
                &yaml,
                "sinks:\n",
                &format!(
                    "  - id: d{i}\n    match: {{ source: src, topic: \"t/+/data\" }}\n    decode: json\n    fields:\n{fields}sinks:\n"
                ),
            );
        }
        let c = check(&yaml);
        assert!(c.report.errors.is_empty(), "{}", c.report);
        assert_eq!(c.spec.unwrap().datasets.len(), 41);
    }

    #[test]
    fn identity_levels_keep_the_written_order() {
        let spec = load_str(&demo(), &demo_env()).unwrap();
        let names: Vec<_> = spec.datasets[0]
            .identity
            .iter()
            .map(|l| l.name.as_str())
            .collect();
        assert_eq!(
            names,
            ["site", "robot"],
            "alphabetical order would be robot, site"
        );
    }

    #[test]
    fn fsync_forms() {
        let with = |v: &str| {
            load_str(
                &edit(
                    &minimal(),
                    "on_full: block",
                    &format!("on_full: block, fsync: {v}"),
                ),
                &MapEnv::new(),
            )
            .unwrap()
            .wal
            .fsync
        };
        assert!(matches!(with("always"), FsyncSpec::Always));
        assert!(matches!(with("never"), FsyncSpec::Never));
        assert!(
            matches!(with("{ interval: 5ms }"), FsyncSpec::Interval(d) if d.get().as_millis() == 5)
        );
        assert!(
            matches!(load_str(&minimal(), &MapEnv::new()).unwrap().wal.fsync, FsyncSpec::Interval(d) if d.get().as_millis() == 10)
        );
        assert!(
            !errors(&edit(
                &minimal(),
                "on_full: block",
                "on_full: block, fsync: sometimes"
            ))
            .is_empty()
        );
        assert!(
            !errors(&edit(
                &minimal(),
                "on_full: block",
                "on_full: block, fsync: { interval: 0s }"
            ))
            .is_empty()
        );
    }

    #[test]
    fn on_full_is_parsed_and_required() {
        assert_eq!(
            load_str(&minimal(), &MapEnv::new()).unwrap().wal.on_full,
            OnFull::Block
        );
        let drop = edit(&minimal(), "on_full: block", "on_full: drop_oldest");
        assert_eq!(
            load_str(&drop, &MapEnv::new()).unwrap().wal.on_full,
            OnFull::DropOldest
        );
        let missing = edit(&minimal(), ", on_full: block", "");
        let report = load_str(&missing, &MapEnv::new()).unwrap_err();
        assert_eq!(report.errors[0].path, "wal.on_full");
        assert_eq!(report.errors[0].code, thadam_core::codes::CONFIG_MISSING);
    }

    #[test]
    fn transforms_can_be_bound_to_sinks() {
        let yaml = edit(
            &minimal(),
            "sinks:\n",
            "transforms:\n  - { kind: filter, dataset: readings, sinks: [local], where: { field: level, ne: debug } }\nsinks:\n",
        );
        let spec = load_str(&yaml, &MapEnv::new()).unwrap();
        assert_eq!(spec.transforms[0].sinks(), Some(&["local".to_owned()][..]));
        assert_eq!(spec.transforms[0].kind(), "filter");
    }

    #[test]
    fn exploded_datasets_may_read_parent_fields() {
        let demo = load_str(&demo(), &demo_env()).unwrap();
        assert!(demo.datasets[1].explode.is_some());
        assert_eq!(demo.datasets[1].fields[0].from, "^/seq");
    }

    #[test]
    fn dedup_and_estimate_use_counts_with_suffixes() {
        let spec = load_str(&demo(), &demo_env()).unwrap();
        assert_eq!(
            spec.datasets[0].dedup.as_ref().unwrap().max_keys.get(),
            500_000
        );
    }
}

mod predicates {
    use super::*;

    fn filter(where_: &str) -> Result<Predicate, Vec<String>> {
        let yaml = edit(
            &minimal(),
            "sinks:\n",
            &format!(
                "transforms:\n  - kind: filter\n    dataset: readings\n    where: {where_}\nsinks:\n"
            ),
        );
        let c = check(&yaml);
        match c.spec {
            Some(spec) => match &spec.transforms[0] {
                TransformSpec::Filter { where_, .. } => Ok(where_.clone()),
                other => panic!("{other:?}"),
            },
            None => Err(c
                .report
                .errors
                .iter()
                .map(|d| format!("{}: {}", d.path, d.message))
                .collect()),
        }
    }

    #[test]
    fn the_adr_example_parses() {
        let p = filter("{ all: [{ field: level, ne: debug }, { any: [{ field: device, ne: robot-03 }, { field: quality.status, eq: good }] }, { field: temp, ge: 10.0 }] }").unwrap();
        let Predicate::All(items) = p else { panic!() };
        assert_eq!(items.len(), 3);
        assert!(
            matches!(&items[2], Predicate::Condition(c) if c.test == Test::Ge(Literal::F64(10.0)))
        );
    }

    #[test]
    fn integers_above_i64_max_stay_u64() {
        let yaml = edit(
            &minimal(),
            "- { id: 1001, name: level, type: str, from: /level }",
            "- { id: 1001, name: level, type: str, from: /level }\n      - { id: 1002, name: big, type: u64, from: /big }",
        );
        let yaml = edit(
            &yaml,
            "sinks:\n",
            "transforms:\n  - { kind: filter, dataset: readings, where: { field: big, ge: 18446744073709551615 } }\nsinks:\n",
        );
        let spec = load_str(&yaml, &MapEnv::new()).unwrap();
        let TransformSpec::Filter {
            where_: Predicate::Condition(c),
            ..
        } = &spec.transforms[0]
        else {
            panic!()
        };
        assert_eq!(c.test, Test::Ge(Literal::U64(u64::MAX)));
        let signed = edit(&yaml, "type: u64", "type: i64");
        assert!(load_str(&signed, &MapEnv::new()).is_err());
    }

    #[test]
    fn min_max_range_and_in_lists() {
        assert!(
            matches!(filter("{ field: temp, min: -40, max: 150 }").unwrap(), Predicate::Condition(c) if matches!(c.test, Test::Between { min: Some(Literal::I64(-40)), max: Some(Literal::I64(150)) }))
        );
        assert!(
            matches!(filter("{ field: level, in: [info, warn] }").unwrap(), Predicate::Condition(c) if matches!(&c.test, Test::In(v) if v.len() == 2))
        );
        assert!(filter("{ field: temp, in: [1, two] }").is_err());
        assert!(matches!(
            filter("{ not: { field: level, eq: debug } }").unwrap(),
            Predicate::Not(_)
        ));
    }

    #[test]
    fn quality_names_are_checked() {
        assert!(filter("{ field: quality.status, in: [good, uncertain] }").is_ok());
        assert!(filter("{ field: quality.status, eq: fine }").is_err());
        assert!(filter("{ field: quality.flags, eq: OUT_OF_RANGE }").is_ok());
        assert!(filter("{ field: quality.flags, eq: NOPE }").is_err());
        assert!(filter("{ field: quality.status, lt: 1 }").is_err());
    }

    #[test]
    fn limits_bound_the_size_of_a_predicate() {
        let mut deep = "{ field: level, eq: x }".to_owned();
        for _ in 0..10 {
            deep = format!("{{ not: {deep} }}");
        }
        assert!(filter(&deep).unwrap_err()[0].contains("nest at most"));
        let many = format!(
            "{{ all: [{}] }}",
            vec!["{ field: level, eq: x }"; 70].join(", ")
        );
        assert!(filter(&many).unwrap_err()[0].contains("at most 64"));
    }

    #[test]
    fn null_and_structured_literals_are_refused() {
        assert!(filter("{ field: level, eq: null }").unwrap_err()[0].contains("null"));
        assert!(filter("{ field: level, eq: [a] }").is_err());
    }

    #[test]
    fn predicates_serialize_in_the_shape_they_are_written() {
        let p = filter("{ all: [{ field: level, ne: debug }, { field: temp, min: 1, max: 2 }] }")
            .unwrap();
        assert_eq!(
            serde_json::to_value(&p).unwrap(),
            serde_json::json!({ "all": [{ "field": "level", "ne": "debug" }, { "field": "temp", "min": 1, "max": 2 }] })
        );
    }
}

mod core_integration {
    use super::*;

    #[test]
    fn declared_fields_become_core_shapes() {
        let spec = load_str(&demo(), &demo_env()).unwrap();
        for ds in &spec.datasets {
            Shape::new(ds.field_defs()).unwrap_or_else(|e| panic!("{}: {e}", ds.id));
        }
        let telemetry = spec.datasets[0].field_defs();
        let seq = &telemetry[0];
        assert_eq!(seq.ty, FieldType::U64);
        assert!(
            !seq.nullable,
            "`required: true` makes the field non-nullable"
        );
        let voltage = telemetry.iter().find(|f| &*f.name == "voltage_v").unwrap();
        assert_eq!(
            voltage.unit.as_deref(),
            Some("V"),
            "the stored unit is `convert_to`"
        );
        let motor = telemetry
            .iter()
            .find(|f| &*f.name == "motor_temp_c")
            .unwrap();
        assert_eq!(motor.ty, FieldType::List(Box::new(FieldType::F32)));
        assert!(motor.nullable);
    }

    #[test]
    fn time_domains_come_from_core() {
        let spec = load_str(&demo(), &demo_env()).unwrap();
        assert_eq!(
            spec.datasets[0].time.as_ref().unwrap().domain.domain(),
            thadam_core::TimeDomain::Utc
        );
        assert!(!errors(&edit(&minimal(), "domain: utc", "domain: local")).is_empty());
    }
}

mod estimate {
    use super::*;

    #[test]
    fn the_demo_estimate_is_below_its_target() {
        let spec = load_str(&demo(), &demo_env()).unwrap();
        let est = spec.memory_estimate();
        assert!(!est.exceeds_target(), "{est}");
        let mib = |n: u64| n << 20;
        let by_label = |needle: &str| {
            est.components
                .iter()
                .find(|c| c.label.contains(needle))
                .unwrap()
                .bytes
        };
        assert_eq!(by_label("broker window"), mib(20));
        assert_eq!(by_label("wal.max_pending"), mib(8));
        assert_eq!(by_label("local` open files"), 6 * 2 * mib(8));
        assert_eq!(by_label("s3` uploads"), 2 * mib(8));
        // PR15: `state` and `events` carry 50k dedup windows as well
        // (chaos duplicates apply fleet-wide), so 4 x 500k + 2 x 50k keys.
        assert_eq!(by_label("dedup"), (4 * 500_000 + 2 * 50_000) * 48);
    }

    #[test]
    fn defaults_are_marked_and_the_target_can_be_exceeded() {
        let yaml = edit(
            &minimal(),
            "sinks:\n",
            "runtime: { memory_target: 64MiB }\nsinks:\n",
        );
        let c = check(&yaml);
        let spec = c.spec.as_ref().unwrap();
        let est = spec.memory_estimate();
        assert!(
            est.components
                .iter()
                .any(|c| c.detail.contains("(default)"))
        );
        assert!(
            est.components
                .iter()
                .any(|c| c.label.contains("open files") && c.bytes == 4 * 2 * (8 << 20))
        );
        assert!(est.exceeds_target(), "{est}");
        assert_eq!(c.report.warnings.len(), 1);
        assert_eq!(c.report.warnings[0].path, "runtime.memory_target");
        assert!(c.report.errors.is_empty());
    }

    #[test]
    fn opaque_settings_that_cannot_be_read_fall_back_to_defaults() {
        let yaml = edit(
            &minimal(),
            "config: { datasets: [readings] }",
            "config: { datasets: [readings], max_open_files: [1], row_group_bytes: \"${env:X}\" }",
        );
        let env = MapEnv::new().with_var("X", "not a size");
        let spec = check_str(&yaml, &env, &CheckOptions::new()).spec.unwrap();
        assert!(
            spec.memory_estimate()
                .components
                .iter()
                .any(|c| c.bytes == 4 * 2 * (8 << 20))
        );
    }
}

mod files {
    use super::*;

    #[test]
    fn a_missing_file_is_a_config_error() {
        let report =
            load_file(Path::new("/no/such/thadam/config.yaml"), &MapEnv::new()).unwrap_err();
        assert!(report.errors[0].message.contains("cannot read"));
        assert!(report.errors[0].span.is_none());
    }

    #[test]
    fn a_file_loads_like_a_string() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.yaml");
        fs::write(&path, minimal()).unwrap();
        assert_eq!(
            load_file(&path, &MapEnv::new()).unwrap(),
            load_str(&minimal(), &MapEnv::new()).unwrap()
        );
    }

    #[test]
    fn non_utf8_files_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.yaml");
        fs::write(&path, [0xff, 0xfe, 0x00]).unwrap();
        assert!(
            load_file(&path, &MapEnv::new()).unwrap_err().errors[0]
                .message
                .contains("UTF-8")
        );
    }

    #[test]
    fn a_byte_order_mark_is_ignored() {
        let with_bom = format!("\u{feff}{}", minimal());
        assert!(load_str(&with_bom, &MapEnv::new()).is_ok());
    }
}
