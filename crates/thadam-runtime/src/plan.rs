//! Translation from the parsed configuration to WAL options and per-sink
//! chain specs.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use thadam_config::{
    DatasetSpec, DecodeFormat, FsyncSpec, MaxLagSpec, OnFull as ConfigOnFull, PipelineSpec,
    SinkSpec, StartFromSpec, TransformSpec,
};
use thadam_core::{
    BudgetCategory, DatasetDecl, DiskBudget, ErrorDomain, LevelDecl, ThadamError, codes,
};
use thadam_transform::ChainSpec;
use thadam_wal::{ConsumerSpec, Fsync, MaxLag, OnFull, StartFrom, WalOptions};
use serde_json::{Map, Value, json};

/// The WAL cursor of a sink's chain.
pub(crate) fn cursor_name(sink_id: &str) -> String {
    format!("sink.{sink_id}")
}

pub(crate) fn config_error(path: &str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, message)
        .with_context("path", path)
}

/// `thadam-transform` parses `<digits><unit>`; humantime text such as `1h 30m`
/// is not accepted, so durations always travel as nanoseconds.
fn duration_text(d: Duration) -> String {
    format!("{}ns", d.as_nanos())
}

pub(crate) fn max_lag(index: usize, sink: &SinkSpec) -> Result<Option<MaxLag>, ThadamError> {
    Ok(match sink.max_lag {
        None => None,
        Some(MaxLagSpec::Bytes(b)) => Some(MaxLag::Bytes(b.get())),
        Some(MaxLagSpec::Age(a)) => Some(MaxLag::Age(a.get())),
        Some(_) => {
            return Err(config_error(
                &format!("sinks[{index}].max_lag"),
                "unsupported max_lag form",
            ));
        }
    })
}

/// Where a chain sink starts when its WAL cursor has never been committed.
pub(crate) fn start_from(index: usize, sink: &SinkSpec) -> Result<StartFrom, ThadamError> {
    match sink.start_from {
        StartFromSpec::Earliest => Ok(StartFrom::Earliest),
        StartFromSpec::Latest => Ok(StartFrom::Latest),
        _ => Err(config_error(
            &format!("sinks[{index}].start_from"),
            "unsupported start_from",
        )),
    }
}

/// The WAL consumer a chain sink registers: required unless the sink says
/// otherwise, with its optional lag limit.
fn consumer(index: usize, sink: &SinkSpec) -> Result<ConsumerSpec, ThadamError> {
    let name = cursor_name(&sink.id);
    if !sink.required {
        return Ok(ConsumerSpec::optional(name));
    }
    let c = ConsumerSpec::required(name);
    Ok(match max_lag(index, sink)? {
        Some(lag) => c.with_max_lag(lag),
        None => c,
    })
}

/// The WAL options for `spec`. `chain_sinks` are the sinks that read the WAL
/// (services do not), as `(index in `spec.sinks`, spec)`.
pub(crate) fn wal_options(
    spec: &PipelineSpec,
    chain_sinks: &[(usize, &SinkSpec)],
    machine_id: Option<String>,
) -> Result<WalOptions, ThadamError> {
    let on_full = match spec.wal.on_full {
        ConfigOnFull::Block => OnFull::Block,
        ConfigOnFull::DropOldest => OnFull::DropOldest,
        _ => return Err(config_error("wal.on_full", "unsupported on_full policy")),
    };
    let mut o = WalOptions::new(on_full);
    o.fsync = match spec.wal.fsync {
        FsyncSpec::Always => Fsync::Always,
        FsyncSpec::Never => Fsync::Never,
        FsyncSpec::Interval(d) => Fsync::Interval(d.get()),
        _ => return Err(config_error("wal.fsync", "unsupported fsync policy")),
    };
    o.segment_bytes = spec.wal.segment_bytes.get();
    o.max_pending_bytes = usize::try_from(spec.wal.max_pending_bytes.get()).unwrap_or(usize::MAX);
    o.retain_for = spec.wal.retain_for.map_or(Duration::ZERO, |d| d.get());
    o.frame_max_bytes = o
        .frame_max_bytes
        .min(usize::try_from(o.segment_bytes / 2).unwrap_or(usize::MAX));
    o.group_max_bytes = o.group_max_bytes.min(o.frame_max_bytes.max(1));
    o.consumers = chain_sinks
        .iter()
        .map(|(i, s)| consumer(*i, s))
        .collect::<Result<_, _>>()?;
    o.node_id = Some(spec.pipeline.node_id.clone());
    o.machine_id = machine_id;
    Ok(o)
}

pub(crate) fn budget(spec: &PipelineSpec) -> DiskBudget {
    DiskBudget::new()
        .with_limit(BudgetCategory::Wal, spec.wal.max_disk.get())
        .with_limit(BudgetCategory::Quarantine, spec.quarantine.max_disk.get())
}

pub(crate) fn quarantine_config(spec: &PipelineSpec) -> Value {
    serde_json::to_value(&spec.quarantine).unwrap_or_else(|_| json!({}))
}

/// The datasets a sink's chain processes: the optional `datasets` list in the
/// sink's `config` (the demo's Parquet sink has one), or every dataset.
pub(crate) fn sink_datasets(
    spec: &PipelineSpec,
    index: usize,
    cfg: &Value,
) -> Result<Option<Vec<String>>, ThadamError> {
    let path = format!("sinks[{index}].config.datasets");
    let Some(v) = cfg.get("datasets") else {
        return Ok(None);
    };
    let Some(items) = v.as_array() else {
        return Err(config_error(
            &path,
            "`datasets` must be a list of dataset ids",
        ));
    };
    let mut out = Vec::new();
    for (i, item) in items.iter().enumerate() {
        let Some(id) = item.as_str() else {
            return Err(config_error(
                &format!("{path}[{i}]"),
                "a dataset id must be a string",
            ));
        };
        if !spec.datasets.iter().any(|d| d.id == id) {
            return Err(config_error(
                &format!("{path}[{i}]"),
                format!("unknown dataset `{id}`"),
            ));
        }
        out.push(id.to_owned());
    }
    Ok(Some(out))
}

fn selected<'a>(spec: &'a PipelineSpec, only: Option<&[String]>) -> Vec<&'a DatasetSpec> {
    spec.datasets
        .iter()
        .filter(|d| only.is_none_or(|ids| ids.contains(&d.id)))
        .collect()
}

/// The declared datasets a sink consumes, as `thadam-core` types for the
/// sink's [`thadam_core::BuildContext`]: `spec.datasets` restricted to the
/// `datasets:` selector, each carrying its field IDs, identity level IDs,
/// and exploded flag. Infallible because callers validate `only` with
/// [`sink_datasets`] first.
pub(crate) fn dataset_decls(spec: &PipelineSpec, only: Option<&[String]>) -> Arc<[DatasetDecl]> {
    selected(spec, only)
        .into_iter()
        .map(|d| {
            DatasetDecl::new(
                d.id.as_str(),
                d.field_defs(),
                d.level_ids()
                    .into_iter()
                    .map(|(name, id)| LevelDecl::new(name, id))
                    .collect(),
                d.explode.is_some(),
            )
        })
        .collect()
}

fn transforms_for<'a>(
    spec: &'a PipelineSpec,
    sink_id: &str,
    datasets: &[&DatasetSpec],
) -> Vec<&'a TransformSpec> {
    spec.transforms
        .iter()
        .filter(|t| t.sinks().is_none_or(|s| s.iter().any(|x| x == sink_id)))
        .filter(|t| datasets.iter().any(|d| d.id == t.dataset()))
        .collect()
}

/// Whether the sink's chain has steps whose state a warm start rebuilds:
/// dedup, `on_change`, and `sample` (its per-asset counters and last-kept
/// ingest times).
pub(crate) fn chain_has_state(spec: &PipelineSpec, sink_id: &str, only: Option<&[String]>) -> bool {
    let ds = selected(spec, only);
    ds.iter().any(|d| d.dedup.is_some())
        || transforms_for(spec, sink_id, &ds).iter().any(|t| {
            matches!(
                t,
                TransformSpec::OnChange { .. } | TransformSpec::Sample { .. }
            )
        })
}

fn dataset_value(d: &DatasetSpec, path: &str) -> Result<Value, ThadamError> {
    let mut m = Map::new();
    m.insert("id".into(), json!(d.id));
    let mut matcher = Map::new();
    matcher.insert("source".into(), json!(d.match_.source));
    if let Some(t) = &d.match_.topic {
        matcher.insert("topic".into(), json!(t));
    }
    m.insert("match".into(), Value::Object(matcher));
    let decode = match d.decode {
        DecodeFormat::Json => "json",
        DecodeFormat::Ros2 => "ros2",
        DecodeFormat::SparkplugB => "sparkplug_b",
        _ => {
            return Err(config_error(
                &format!("{path}.decode"),
                "unsupported decoder",
            ));
        }
    };
    m.insert("decode".into(), json!(decode));
    let identity: Vec<Value> = d
        .identity
        .iter()
        .map(|l| {
            let mut one = Map::new();
            one.insert(l.name.clone(), json!(l.template));
            Value::Object(one)
        })
        .collect();
    m.insert("identity".into(), Value::Array(identity));
    if let Some(e) = &d.event_id {
        m.insert("event_id".into(), json!({ "key": e.key }));
    }
    if let Some(dd) = &d.dedup {
        m.insert(
            "dedup".into(),
            json!({ "window": duration_text(dd.window.get()), "max_keys": dd.max_keys.get() }),
        );
    }
    if let Some(x) = &d.explode {
        m.insert(
            "explode".into(),
            json!({
                "path": x.path,
                "index_field": x.index_field.clone().unwrap_or_else(|| "sample_idx".to_owned()),
            }),
        );
    }
    if let Some(t) = &d.time {
        let mut tm = Map::new();
        if let Some(field) = &t.field {
            tm.insert("field".into(), json!(field));
        }
        if let Some(attr) = &t.attr {
            tm.insert("attr".into(), json!(attr));
        }
        tm.insert("unit".into(), json!(t.unit.as_str()));
        tm.insert("domain".into(), json!(t.domain.domain().as_str()));
        if let Some(s) = t.max_skew {
            tm.insert("max_skew".into(), json!(duration_text(s.get())));
        }
        if let Some(offset) = &t.default_offset {
            tm.insert("default_offset".into(), json!(offset));
        }
        m.insert("time".into(), Value::Object(tm));
    }
    let fields: Vec<Value> = d
        .fields
        .iter()
        .map(|f| {
            let mut fm = Map::new();
            fm.insert("name".into(), json!(f.name));
            fm.insert("type".into(), json!(f.ty.canonical()));
            // Absent `from` stays absent: the engine defaults it to `/<name>`,
            // one pointer token, which for a dotted name reads the decoder's
            // flattened key.
            if let Some(from) = &f.from {
                fm.insert("from".into(), json!(from));
            }
            fm.insert("required".into(), json!(f.required));
            if let Some(u) = &f.unit {
                fm.insert("unit".into(), json!(u));
            }
            if let Some(c) = &f.convert_to {
                fm.insert("convert_to".into(), json!(c));
            }
            Value::Object(fm)
        })
        .collect();
    m.insert("fields".into(), Value::Array(fields));
    Ok(Value::Object(m))
}

fn transform_value(t: &TransformSpec, path: &str) -> Result<Value, ThadamError> {
    let mut v = serde_json::to_value(t).map_err(|e| config_error(path, e.to_string()))?;
    // The derived form of every kind matches the engine's spec except for
    // durations: they humantime-serialize compositely (`1m 30s`), which its
    // `HumanDuration` refuses, so they travel as nanoseconds.
    if let Some(obj) = v.as_object_mut() {
        let duration = match t {
            TransformSpec::OnChange {
                heartbeat: Some(d), ..
            } => Some(("heartbeat", d.get())),
            TransformSpec::Sample {
                min_interval: Some(d),
                ..
            } => Some(("min_interval", d.get())),
            TransformSpec::Window { size, .. } => Some(("size", size.get())),
            _ => None,
        };
        if let Some((key, d)) = duration {
            obj.insert(key.into(), json!(duration_text(d)));
        }
    }
    Ok(v)
}

/// The `ChainSpec` value for one sink: its datasets and the transforms that
/// apply to it, with identity levels as an ordered list.
pub(crate) fn chain_value(
    spec: &PipelineSpec,
    sink_id: &str,
    only: Option<&[String]>,
) -> Result<Value, ThadamError> {
    let ds = selected(spec, only);
    let mut datasets = Vec::new();
    for d in &ds {
        let i = spec.datasets.iter().position(|x| x.id == d.id).unwrap_or(0);
        datasets.push(dataset_value(d, &format!("datasets[{i}]"))?);
    }
    let mut transforms = Vec::new();
    for t in transforms_for(spec, sink_id, &ds) {
        let i = spec
            .transforms
            .iter()
            .position(|x| std::ptr::eq(x, t))
            .unwrap_or(0);
        transforms.push(transform_value(t, &format!("transforms[{i}]"))?);
    }
    Ok(json!({ "datasets": datasets, "transforms": transforms }))
}

pub(crate) fn chain_spec(
    spec: &PipelineSpec,
    sink_id: &str,
    only: Option<&[String]>,
    base_dir: Option<&Path>,
    schema_dir: Option<&Path>,
) -> Result<ChainSpec, ThadamError> {
    let cs = ChainSpec::from_value(chain_value(spec, sink_id, only)?)?;
    let cs = match base_dir {
        Some(dir) => cs.with_base_dir(dir),
        None => cs,
    };
    Ok(match schema_dir {
        Some(dir) => cs.with_schema_dir(dir),
        None => cs,
    })
}

/// Points a sink's chain at its own birth-store directory under `births_root`.
/// One store per sink chain: chains run at different WAL positions, and a
/// shared store would let a lagging chain overwrite a newer BIRTH. Replay
/// passes `read_only` so history never overwrites production seeds.
pub(crate) fn with_births(
    cs: ChainSpec,
    births_root: &Path,
    sink_id: &str,
    read_only: bool,
) -> ChainSpec {
    let cs = cs.with_births_dir(births_root.join(sink_id));
    if read_only {
        cs.with_births_read_only()
    } else {
        cs
    }
}

#[cfg(test)]
mod tests {
    use thadam_config::MapEnv;
    use thadam_transform as rt;

    use super::*;

    /// A pipeline with one dataset `d` (fields `n: u64` and `temp: f32`) and
    /// the given sinks. `dataset_options` is spliced into the dataset mapping
    /// (indented, with a trailing newline when non-empty) and `extra` between
    /// the datasets and sinks sections, for `transforms:`.
    fn spec_with_parts(sinks: &str, dataset_options: &str, extra: &str) -> PipelineSpec {
        let yaml = format!(
            r#"version: 1
pipeline: {{ name: test, node_id: node-1, data_dir: /tmp/thadam-plan-test }}
sources:
  - {{ id: src, kind: memory }}
wal: {{ max_disk: 64MiB, on_full: block }}
datasets:
  - id: d
    match: {{ source: src }}
    decode: json
{dataset_options}    fields:
      - {{ id: 1000, name: n, type: u64, from: /n, required: true }}
      - {{ id: 1001, name: temp, type: f32, from: /temp }}
{extra}sinks:
{sinks}
"#
        );
        thadam_config::load_str(&yaml, &MapEnv::new())
            .unwrap_or_else(|r| panic!("{}", r.render(&yaml)))
    }

    fn spec_with_sinks(sinks: &str) -> PipelineSpec {
        spec_with_parts(sinks, "", "")
    }

    fn consumers(spec: &PipelineSpec) -> Vec<ConsumerSpec> {
        let sinks: Vec<_> = spec.sinks.iter().enumerate().collect();
        wal_options(spec, &sinks, None).unwrap().consumers
    }

    #[test]
    fn sinks_default_to_required_consumers_without_a_lag_limit() {
        let spec = spec_with_sinks("  - { id: a, kind: recseg }");
        assert_eq!(consumers(&spec), [ConsumerSpec::required("sink.a")]);
    }

    #[test]
    fn required_and_max_lag_map_to_consumer_specs() {
        let spec = spec_with_sinks(
            "  - { id: a, kind: recseg, required: true, max_lag: { bytes: 2GiB } }\n\
             \x20 - { id: b, kind: recseg, max_lag: { age: 30m } }\n\
             \x20 - { id: c, kind: recseg, required: false }\n\
             \x20 - { id: d, kind: recseg, required: false, start_from: latest }",
        );
        assert_eq!(
            consumers(&spec),
            [
                ConsumerSpec::required("sink.a").with_max_lag(MaxLag::Bytes(2 << 30)),
                ConsumerSpec::required("sink.b")
                    .with_max_lag(MaxLag::Age(Duration::from_secs(1800))),
                ConsumerSpec::optional("sink.c"),
                ConsumerSpec::optional("sink.d"),
            ]
        );
    }

    #[test]
    fn only_the_given_chain_sinks_become_consumers() {
        let spec = spec_with_sinks(
            "  - { id: a, kind: recseg }\n  - { id: svc, kind: recservice }\n  - { id: b, kind: recseg, required: false }",
        );
        let chain: Vec<_> = spec
            .sinks
            .iter()
            .enumerate()
            .filter(|(_, s)| s.kind != "recservice")
            .collect();
        let o = wal_options(&spec, &chain, None).unwrap();
        let names: Vec<_> = o.consumers.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["sink.a", "sink.b"]);
    }

    #[test]
    fn start_from_maps_to_the_wal_start() {
        let spec = spec_with_sinks(
            "  - { id: a, kind: recseg }\n  - { id: b, kind: recseg, start_from: latest }",
        );
        assert_eq!(start_from(0, &spec.sinks[0]).unwrap(), StartFrom::Earliest);
        assert_eq!(start_from(1, &spec.sinks[1]).unwrap(), StartFrom::Latest);
    }

    /// One transform of every kind, written as a config file would write
    /// them; `enrich`'s file is read when a chain is compiled, not by
    /// `ChainSpec::from_value`.
    const EACH_KIND: &str = r#"transforms:
  - { kind: validate, dataset: d, rules: [{ field: temp, min: -40, max: 150, flag: OUT_OF_RANGE, status: bad }] }
  - { kind: on_change, dataset: d, fields: [n, { field: temp, deadband: 0.5 }], heartbeat: 5m }
  - { kind: filter, dataset: d, where: { field: temp, null: false } }
  - { kind: sample, dataset: d, every_n: 2, min_interval: 90s }
  - kind: enrich
    dataset: d
    file: devices.csv
    key: { field: n, column: id }
    add:
      - { name: model, type: str }
    on_miss: reject
  - { kind: cast, dataset: d, fields: { temp: f64 } }
  - { kind: rename, dataset: d, fields: { temp: temp_c } }
  - { kind: project, dataset: d, fields: [n, temp_c, model] }
"#;

    #[test]
    fn every_transform_kind_deserializes_into_a_chain_spec() {
        let spec = spec_with_parts("  - { id: a, kind: recseg }", "", EACH_KIND);
        let chain = chain_spec(&spec, "a", None, None, None).unwrap();
        let kinds: Vec<&str> = chain
            .transforms
            .iter()
            .map(rt::TransformSpec::kind)
            .collect();
        assert_eq!(
            kinds,
            [
                "validate",
                "on_change",
                "filter",
                "sample",
                "enrich",
                "cast",
                "rename",
                "project"
            ]
        );

        let rt::TransformSpec::Enrich(e) = &chain.transforms[4] else {
            panic!("transforms: {kinds:?}")
        };
        assert_eq!(e.file, "devices.csv");
        assert_eq!(e.on_miss, rt::OnMiss::Reject);
        assert_eq!(e.key.field, "n");
        assert_eq!(e.key.column, "id");
        assert_eq!(e.add.len(), 1);
        assert_eq!(e.add[0].name, "model");
        assert_eq!(e.add[0].ty, "str");

        let rt::TransformSpec::Cast(c) = &chain.transforms[5] else {
            panic!("transforms: {kinds:?}")
        };
        assert_eq!(c.fields.get("temp").map(String::as_str), Some("f64"));

        let rt::TransformSpec::Rename(r) = &chain.transforms[6] else {
            panic!("transforms: {kinds:?}")
        };
        assert_eq!(r.fields.get("temp").map(String::as_str), Some("temp_c"));

        let rt::TransformSpec::Project(p) = &chain.transforms[7] else {
            panic!("transforms: {kinds:?}")
        };
        assert_eq!(p.fields, ["n", "temp_c", "model"]);
    }

    #[test]
    fn each_sink_chain_gets_its_own_birth_store_and_replay_is_read_only() {
        let spec = spec_with_parts("  - { id: a, kind: recseg }", "", "");
        let root = Path::new("/data/sparkplug/births");
        let live = with_births(
            chain_spec(&spec, "a", None, None, None).unwrap(),
            root,
            "a",
            false,
        );
        assert_eq!(live.births_dir.as_deref(), Some(root.join("a").as_path()));
        assert!(!live.births_read_only);
        let replay = with_births(
            chain_spec(&spec, "a", None, None, None).unwrap(),
            root,
            "a",
            true,
        );
        assert!(replay.births_read_only);
    }

    #[test]
    fn sample_min_interval_reaches_the_engine_as_nanoseconds() {
        let spec = spec_with_parts(
            "  - { id: a, kind: recseg }",
            "",
            "transforms:\n  - { kind: sample, dataset: d, every_n: 2, min_interval: 90s }\n",
        );
        let v = chain_value(&spec, "a", None).unwrap();
        assert_eq!(
            v["transforms"][0]["min_interval"],
            json!("90000000000ns"),
            "humantime text (`1m 30s`) does not parse in thadam-transform"
        );
        assert_eq!(v["transforms"][0]["every_n"], json!(2));
        let chain = chain_spec(&spec, "a", None, None, None).unwrap();
        let rt::TransformSpec::Sample(s) = &chain.transforms[0] else {
            panic!("{:?}", chain.transforms)
        };
        assert_eq!(s.every_n, Some(2));
        assert_eq!(s.min_interval.map(|d| d.0), Some(Duration::from_secs(90)));
    }

    #[test]
    fn rfc3339_time_unit_and_default_offset_pass_through() {
        let spec = spec_with_parts(
            "  - { id: a, kind: recseg }",
            "    time: { field: /ts, unit: rfc3339, domain: utc, default_offset: \"+02:00\" }\n",
            "",
        );
        let v = chain_value(&spec, "a", None).unwrap();
        assert_eq!(v["datasets"][0]["time"]["unit"], json!("rfc3339"));
        assert_eq!(v["datasets"][0]["time"]["default_offset"], json!("+02:00"));
        let chain = chain_spec(&spec, "a", None, None, None).unwrap();
        let time = chain.datasets[0].time.as_ref().unwrap();
        assert_eq!(time.unit, "rfc3339");
        assert_eq!(time.default_offset.as_deref(), Some("+02:00"));

        let epoch = spec_with_parts(
            "  - { id: a, kind: recseg }",
            "    time: { field: /ts, unit: us, domain: utc }\n",
            "",
        );
        let chain = chain_spec(&epoch, "a", None, None, None).unwrap();
        assert_eq!(chain.datasets[0].time.as_ref().unwrap().unit, "us");
    }

    #[test]
    fn on_change_deadband_watches_reach_the_engine() {
        let spec = spec_with_parts(
            "  - { id: a, kind: recseg }",
            "",
            "transforms:\n  - { kind: on_change, dataset: d, fields: [n, { field: temp, deadband: 0.5 }], heartbeat: 90s }\n",
        );
        let chain = chain_spec(&spec, "a", None, None, None).unwrap();
        let rt::TransformSpec::OnChange(oc) = &chain.transforms[0] else {
            panic!("{:?}", chain.transforms)
        };
        assert_eq!(oc.fields.len(), 2);
        assert_eq!(oc.fields[0].field, "n");
        assert_eq!(oc.fields[0].deadband, 0.0);
        assert_eq!(oc.fields[1].field, "temp");
        assert_eq!(oc.fields[1].deadband, 0.5);
        assert_eq!(oc.heartbeat.map(|h| h.0), Some(Duration::from_secs(90)));
    }

    #[test]
    fn filter_null_test_reaches_the_engine() {
        let spec = spec_with_parts(
            "  - { id: a, kind: recseg }",
            "",
            "transforms:\n  - { kind: filter, dataset: d, where: { field: temp, null: true } }\n",
        );
        let chain = chain_spec(&spec, "a", None, None, None).unwrap();
        let rt::TransformSpec::Filter(f) = &chain.transforms[0] else {
            panic!("{:?}", chain.transforms)
        };
        let p = f.where_.as_ref().unwrap();
        assert_eq!(p.field.as_deref(), Some("temp"));
        assert_eq!(p.null, Some(true));
    }

    #[test]
    fn sample_steps_make_a_chain_stateful() {
        let sampled = spec_with_parts(
            "  - { id: a, kind: recseg }",
            "",
            "transforms:\n  - { kind: sample, dataset: d, every_n: 2 }\n",
        );
        assert!(chain_has_state(&sampled, "a", None));
        let filtered = spec_with_parts(
            "  - { id: a, kind: recseg }",
            "",
            "transforms:\n  - { kind: filter, dataset: d, where: { field: n, gt: 0 } }\n",
        );
        assert!(!chain_has_state(&filtered, "a", None));
    }
}
