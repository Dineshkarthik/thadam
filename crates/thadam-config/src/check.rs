//! The multi-error structural pass: each section, and each item of a list
//! section, is deserialized on its own from the parsed `serde_json::Value`, so
//! one bad item does not hide the others (ADR-0012, "collecting all errors").

use std::collections::BTreeMap;

use thadam_core::codes;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::ctx::Ctx;
use crate::path::{self, PathExt, PathVec, Seg};
use crate::predicate::{Condition, Literal, Predicate, Test};
use crate::scalars::{CountSpec, DurationSpec, FieldTypeSpec};
use crate::spans::Anchor;
use crate::spec::{
    AggregateSpec, CONFIG_VERSION, ConnectorSpec, DatasetSpec, EnrichFieldSpec, EnrichFormat,
    EnrichKeySpec, IdentityLevel, ObservabilitySpec, OnMissSpec, PipelineMeta, QuarantineSpec,
    RuntimeSpec, SinkSpec, TransformSpec, ValidateRule, WalSpec, WatchField,
};

/// The top-level keys, in document order.
pub(crate) const SECTIONS: [&str; 10] = [
    "version",
    "pipeline",
    "sources",
    "wal",
    "quarantine",
    "datasets",
    "transforms",
    "sinks",
    "runtime",
    "observability",
];

const TRANSFORM_KINDS: [&str; 11] = [
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
];
const PREDICATE_KEYS: [&str; 14] = [
    "all", "any", "not", "field", "eq", "ne", "lt", "le", "gt", "ge", "in", "min", "max", "null",
];
const MAX_PREDICATE_DEPTH: usize = 8;
const MAX_PREDICATE_NODES: usize = 64;
const MAX_IN_LIST: usize = 64;
const MAX_UNKNOWN_RETRIES: usize = 16;

/// The hint for a malformed `max_lag`, shared with the semantic checks.
pub(crate) const MAX_LAG_HINT: &str = "write `max_lag: { bytes: 2GiB }` or `max_lag: { age: 30m }`";

/// Every section after structural validation. `None` means that section or
/// item had an error (already reported).
pub(crate) struct Parsed {
    pub pipeline: Option<PipelineMeta>,
    pub sources: Section<ConnectorSpec>,
    pub wal: Option<WalSpec>,
    pub quarantine: Option<QuarantineSpec>,
    pub datasets: Section<DatasetSpec>,
    pub transforms: Section<TransformSpec>,
    pub sinks: Section<SinkSpec>,
    pub runtime: Option<RuntimeSpec>,
    pub observability: Option<ObservabilitySpec>,
}

/// A list section: the typed items and the `id` of each raw item.
pub(crate) struct Section<T> {
    /// `None` when the section itself was missing or not a list.
    pub items: Option<Vec<Option<T>>>,
    pub ids: Vec<Option<String>>,
}

impl<T> Section<T> {
    fn empty() -> Self {
        Section {
            items: Some(Vec::new()),
            ids: Vec::new(),
        }
    }

    fn broken() -> Self {
        Section {
            items: None,
            ids: Vec::new(),
        }
    }

    pub(crate) fn present(&self) -> impl Iterator<Item = (usize, &T)> {
        self.items
            .iter()
            .flatten()
            .enumerate()
            .filter_map(|(i, t)| t.as_ref().map(|t| (i, t)))
    }
}

pub(crate) fn levenshtein(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut cur = vec![i + 1];
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != *cb);
            cur.push((prev[j] + cost).min(prev[j + 1] + 1).min(cur[j] + 1));
        }
        prev = cur;
    }
    prev[b.len()]
}

/// The candidate closest to `name`, if it is close enough to be a typo.
pub(crate) fn closest<'a>(
    name: &str,
    candidates: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    let limit = (name.chars().count() / 3).clamp(1, 3);
    candidates
        .into_iter()
        .map(|c| (levenshtein(name, c), c))
        .filter(|(d, _)| *d <= limit)
        .min_by_key(|(d, _)| *d)
        .map(|(_, c)| c)
}

fn backticked(msg: &str) -> Vec<&str> {
    msg.split('`').skip(1).step_by(2).collect()
}

fn missing_hint(last_key: &str) -> Option<&'static str> {
    Some(match last_key {
        "on_full" => {
            "`on_full` has no default because it decides whether data may be lost: write `block` (apply backpressure) or `drop_oldest` (evict the oldest data; the loss is counted)"
        }
        "kind" => "name a registered connector kind, for example `kind: mqtt`",
        "decode" => "write `decode: json`",
        "version" => "write `version: 1` at the top of the file",
        "max_disk" => "give a size such as `2GiB`",
        _ => return None,
    })
}

/// The hint for a missing `id`, which means a field ID inside a dataset's
/// `fields` or `identity` and a plain identifier elsewhere.
fn missing_id_hint(parent: &[Seg]) -> Option<&'static str> {
    match parent {
        [.., Seg::Key(k), Seg::Idx(_)] if k == "fields" => Some(
            "every declared field needs a stable Parquet field ID: write `id: 1000` (1000 to 4194303, unique in the dataset) and never reuse the ID of a removed field",
        ),
        [.., Seg::Key(k), Seg::Key(_)] if k == "identity" => Some(
            "every identity level needs a stable Parquet field ID: write `{ id: 100, template: \"{topic.2}\" }` (100 to 199, unique in the dataset)",
        ),
        _ => None,
    }
}

/// Reports one serde error as a diagnostic with a path, position, and hint.
///
/// `typos` records `(parent path, suggested name)` for each misspelled key, so
/// the "missing field" that the typo causes is not reported a second time.
fn report_serde(
    base: &[Seg],
    err: &serde_path_to_error::Error<serde_json::Error>,
    typos: &mut Vec<(PathVec, String)>,
    ctx: &mut Ctx<'_>,
) {
    let mut full = base.to_vec();
    full.extend(path::from_serde(err.path()));
    let msg = err.inner().to_string();
    let names = backticked(&msg);

    if let Some(rest) = msg.strip_prefix("missing field `") {
        let name = rest.split('`').next().unwrap_or_default();
        if typos.iter().any(|(parent, n)| *parent == full && n == name) {
            return;
        }
        let child = full.key(name);
        let hint = if name == "id" {
            missing_id_hint(&full)
        } else {
            missing_hint(name)
        };
        ctx.error(
            codes::CONFIG_MISSING,
            &child,
            Anchor::Key,
            format!("missing required field `{name}`"),
            hint.map(str::to_owned),
        );
        return;
    }

    let hint = if msg.starts_with("unknown field `") || msg.starts_with("unknown variant `") {
        names.split_first().and_then(|(name, rest)| {
            closest(name, rest.iter().copied()).map(|c| {
                if msg.starts_with("unknown field `") {
                    let parent = full[..full.len().saturating_sub(1)].to_vec();
                    typos.push((parent, c.to_owned()));
                }
                format!("did you mean `{c}`?")
            })
        })
    } else if msg.contains("expected a string")
        && (msg.contains("integer") || msg.contains("floating point") || msg.contains("boolean"))
    {
        Some("quote the value to make it a string, for example `\"014\"`".to_owned())
    } else if msg.contains("expected a field ID") {
        Some("write the field ID as a plain integer, for example `id: 1000`".to_owned())
    } else if msg.contains("expected a mapping `{ id:") {
        Some(
            "an identity level now needs a field ID: `site: { id: 100, template: \"{topic.2}\" }`"
                .to_owned(),
        )
    } else if msg.contains("`bytes` or `age`") || msg.contains("`{ bytes: <size> }`") {
        Some(MAX_LAG_HINT.to_owned())
    } else if msg.contains("expected a boolean") {
        Some("write `true` or `false` without quotes".to_owned())
    } else {
        None
    };
    let hint = hint.or_else(|| {
        let is_start_from = matches!(full.last(), Some(Seg::Key(k)) if k == "start_from");
        (is_start_from && msg.starts_with("unknown variant `")).then(|| {
            "`earliest` starts at the oldest data the WAL holds, `latest` at its current end"
                .to_owned()
        })
    });
    let anchor = if msg.starts_with("unknown field `") {
        Anchor::Key
    } else {
        Anchor::Value
    };
    let (msg, hint) = tidy_message(&msg, hint);
    ctx.error(codes::CONFIG_INVALID, &full, anchor, msg, hint);
}

/// Rewrites a few serde messages into friendlier text.
fn tidy_message(msg: &str, hint: Option<String>) -> (String, Option<String>) {
    if msg.starts_with("unknown field `") {
        let names = backticked(msg);
        if let Some((name, rest)) = names.split_first() {
            let expected = if rest.is_empty() {
                "this section takes no keys".to_owned()
            } else {
                format!(
                    "expected one of {}",
                    rest.iter()
                        .map(|n| format!("`{n}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            return (format!("unknown key `{name}`, {expected}"), hint);
        }
    }
    (msg.to_owned(), hint)
}

fn remove_at(root: &mut Value, rel: &[Seg]) -> bool {
    let Some((last, parents)) = rel.split_last() else {
        return false;
    };
    let mut cur = root;
    for seg in parents {
        let next = match (seg, cur) {
            (Seg::Key(k), Value::Object(m)) => m.get_mut(k),
            (Seg::Idx(i), Value::Array(a)) => a.get_mut(*i),
            _ => None,
        };
        let Some(next) = next else { return false };
        cur = next;
    }
    match (last, cur) {
        (Seg::Key(k), Value::Object(m)) => m.remove(k).is_some(),
        _ => false,
    }
}

/// Deserializes `value` as `T`. Unknown keys are removed and the attempt
/// repeated, so every unknown key in the value is reported; any other error
/// stops the value.
fn typed<T: DeserializeOwned>(value: &Value, base: &[Seg], ctx: &mut Ctx<'_>) -> Option<T> {
    let mut working = value.clone();
    let mut failed = false;
    let mut typos = Vec::new();
    for _ in 0..=MAX_UNKNOWN_RETRIES {
        match serde_path_to_error::deserialize::<_, T>(&working) {
            Ok(t) => return (!failed).then_some(t),
            Err(err) => {
                failed = true;
                report_serde(base, &err, &mut typos, ctx);
                let rel = path::from_serde(err.path());
                let unknown = err.inner().to_string().starts_with("unknown field `");
                if !(unknown && remove_at(&mut working, &rel)) {
                    return None;
                }
            }
        }
    }
    None
}

fn typed_list<T: DeserializeOwned>(
    items: &[Value],
    base: &[Seg],
    ctx: &mut Ctx<'_>,
    mut finish: impl FnMut(usize, T, &PathVec, &mut Ctx<'_>) -> T,
) -> Section<T> {
    let mut out = Vec::with_capacity(items.len());
    let mut ids = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        let p = base.idx(i);
        ids.push(item.get("id").and_then(Value::as_str).map(str::to_owned));
        out.push(typed::<T>(item, &p, ctx).map(|t| finish(i, t, &p, ctx)));
    }
    Section {
        items: Some(out),
        ids,
    }
}

fn list_of<'a>(
    map: &'a Map<String, Value>,
    name: &str,
    required: bool,
    ctx: &mut Ctx<'_>,
) -> Option<&'a [Value]> {
    let base = Vec::new().key(name);
    match map.get(name) {
        None => {
            if required {
                ctx.error(
                    codes::CONFIG_MISSING,
                    &base,
                    Anchor::Key,
                    format!("missing required section `{name}`"),
                    Some(format!("add a `{name}:` list")),
                );
            }
            None
        }
        Some(Value::Array(a)) => Some(a),
        Some(_) => {
            ctx.invalid_hint(
                &base,
                Anchor::Value,
                format!("`{name}` must be a list"),
                "write each entry on its own line, starting with `-`",
            );
            None
        }
    }
}

fn optional_section<T: DeserializeOwned + Default>(
    map: &Map<String, Value>,
    name: &str,
    ctx: &mut Ctx<'_>,
) -> Option<T> {
    match map.get(name) {
        None => Some(T::default()),
        Some(v) => typed::<T>(v, &Vec::new().key(name), ctx),
    }
}

fn single_required<T: DeserializeOwned>(
    map: &Map<String, Value>,
    name: &str,
    ctx: &mut Ctx<'_>,
) -> Option<T> {
    let base = Vec::new().key(name);
    match map.get(name) {
        None => {
            ctx.error(
                codes::CONFIG_MISSING,
                &base,
                Anchor::Key,
                format!("missing required section `{name}`"),
                Some(format!("add a `{name}:` mapping")),
            );
            None
        }
        Some(v) => typed::<T>(v, &base, ctx),
    }
}

/// Validates `version`, then every section. Returns `None` only when the
/// version is missing, malformed, or unsupported: the schema is then unknown,
/// so nothing else is checked.
pub(crate) fn parse_document(root: &Value, ctx: &mut Ctx<'_>) -> Option<Parsed> {
    let Some(map) = root.as_object() else {
        ctx.invalid_hint(
            &[],
            Anchor::Key,
            "the document must be a mapping of sections",
            "start with `version: 1`, then `pipeline:`, `sources:`, and the other sections",
        );
        return None;
    };

    let version_path = Vec::new().key("version");
    match map.get("version") {
        None => {
            ctx.error(
                codes::CONFIG_MISSING,
                &version_path,
                Anchor::Key,
                "missing required field `version`",
                missing_hint("version").map(str::to_owned),
            );
            return None;
        }
        Some(v) => {
            match v.as_u64() {
                Some(n) if n == u64::from(CONFIG_VERSION) => {}
                Some(n) => {
                    ctx.error(
                    codes::CONFIG_INCOMPATIBLE,
                    &version_path,
                    Anchor::Value,
                    format!("unsupported config version {n}: this thadam reads version {CONFIG_VERSION}"),
                    Some(if n > u64::from(CONFIG_VERSION) {
                        "the file is newer than this binary; upgrade thadam".to_owned()
                    } else {
                        "version numbers start at 1".to_owned()
                    }),
                );
                    return None;
                }
                None => {
                    let hint = if v.is_string() {
                        "write the number without quotes: `version: 1`"
                    } else {
                        "`version` is the integer 1"
                    };
                    ctx.error(
                        codes::CONFIG_INCOMPATIBLE,
                        &version_path,
                        Anchor::Value,
                        "`version` must be the integer 1",
                        Some(hint.to_owned()),
                    );
                    return None;
                }
            }
        }
    }

    for key in map.keys() {
        if !SECTIONS.contains(&key.as_str()) {
            let hint = closest(key, SECTIONS).map(|c| format!("did you mean `{c}`?"));
            ctx.error(
                codes::CONFIG_INVALID,
                &Vec::new().key(key),
                Anchor::Key,
                format!(
                    "unknown top-level key `{key}`, expected one of {}",
                    SECTIONS
                        .iter()
                        .map(|s| format!("`{s}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                hint,
            );
        }
    }

    let pipeline = single_required::<PipelineMeta>(map, "pipeline", ctx);
    let wal = single_required::<WalSpec>(map, "wal", ctx);
    let quarantine = optional_section::<QuarantineSpec>(map, "quarantine", ctx);
    let runtime = optional_section::<RuntimeSpec>(map, "runtime", ctx);
    let observability = optional_section::<ObservabilitySpec>(map, "observability", ctx);
    let text_spelling = map
        .get("observability")
        .and_then(|o| o.get("logs"))
        .and_then(|l| l.get("format"))
        .is_some_and(|f| f.as_str() == Some("text"));
    if observability.is_some() && text_spelling {
        ctx.warn(
            codes::CONFIG_INVALID,
            &Vec::new().key("observability").key("logs").key("format"),
            Anchor::Value,
            "`text` is a deprecated spelling of the log format `pretty`",
            Some("write `format: pretty`; `text` will be rejected in a future version".to_owned()),
        );
    }

    let sources = match list_of(map, "sources", true, ctx) {
        Some(items) => {
            typed_list::<ConnectorSpec>(items, &Vec::new().key("sources"), ctx, |_, t, _, _| t)
        }
        None => Section::broken(),
    };
    let sinks = match list_of(map, "sinks", true, ctx) {
        Some(items) => typed_list::<SinkSpec>(items, &Vec::new().key("sinks"), ctx, |_, t, _, _| t),
        None => Section::broken(),
    };
    let datasets = match list_of(map, "datasets", true, ctx) {
        Some(items) => {
            typed_list::<DatasetSpec>(items, &Vec::new().key("datasets"), ctx, order_identity)
        }
        None => Section::broken(),
    };
    let transforms = match list_of(map, "transforms", false, ctx) {
        Some(items) => parse_transforms(items, ctx),
        None if map.contains_key("transforms") => Section::broken(),
        None => Section::empty(),
    };

    Some(Parsed {
        pipeline,
        sources,
        wal,
        quarantine,
        datasets,
        transforms,
        sinks,
        runtime,
        observability,
    })
}

/// Restores the written order of identity levels; the parsed value has them sorted by name.
fn order_identity(_: usize, mut ds: DatasetSpec, base: &PathVec, ctx: &mut Ctx<'_>) -> DatasetSpec {
    if let Some(order) = ctx.spans().key_order(&base.key("identity")) {
        let rank = |l: &IdentityLevel| {
            order
                .iter()
                .position(|k| *k == l.name)
                .unwrap_or(usize::MAX)
        };
        ds.identity.sort_by_key(rank);
    }
    ds
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidateRaw {
    dataset: String,
    #[serde(default)]
    sinks: Option<Vec<String>>,
    rules: Vec<ValidateRule>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OnChangeRaw {
    dataset: String,
    #[serde(default)]
    sinks: Option<Vec<String>>,
    fields: Vec<WatchField>,
    #[serde(default)]
    heartbeat: Option<DurationSpec>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FilterRaw {
    dataset: String,
    #[serde(default)]
    sinks: Option<Vec<String>>,
    #[serde(rename = "where")]
    where_: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SampleRaw {
    dataset: String,
    #[serde(default)]
    sinks: Option<Vec<String>>,
    #[serde(default)]
    every_n: Option<CountSpec>,
    #[serde(default)]
    min_interval: Option<DurationSpec>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnrichRaw {
    dataset: String,
    #[serde(default)]
    sinks: Option<Vec<String>>,
    file: String,
    #[serde(default)]
    format: Option<EnrichFormat>,
    key: EnrichKeySpec,
    add: Vec<EnrichFieldSpec>,
    /// An unquoted `on_miss: null` is YAML null; it means the `null` policy.
    #[serde(default)]
    on_miss: Option<OnMissSpec>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RenameRaw {
    dataset: String,
    #[serde(default)]
    sinks: Option<Vec<String>>,
    fields: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectRaw {
    dataset: String,
    #[serde(default)]
    sinks: Option<Vec<String>>,
    fields: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CastRaw {
    dataset: String,
    #[serde(default)]
    sinks: Option<Vec<String>>,
    fields: BTreeMap<String, FieldTypeSpec>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PoseRaw {
    dataset: String,
    #[serde(default)]
    sinks: Option<Vec<String>>,
    #[serde(default)]
    prefix: Option<String>,
    /// An explicit `frame_field: null` reads as `None`, indistinguishable
    /// from an absent key; the parse arm tells the two apart by the key.
    #[serde(default)]
    frame_field: Option<String>,
    #[serde(default)]
    quaternion_tolerance: Option<f64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JointStateRaw {
    dataset: String,
    #[serde(default)]
    sinks: Option<Vec<String>>,
    #[serde(default)]
    name_field: Option<String>,
    #[serde(default)]
    position_field: Option<String>,
    #[serde(default)]
    velocity_field: Option<String>,
    #[serde(default)]
    effort_field: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WindowRaw {
    dataset: String,
    #[serde(default)]
    sinks: Option<Vec<String>>,
    size: DurationSpec,
    #[serde(default)]
    idle_close: Option<DurationSpec>,
    aggregates: Vec<AggregateSpec>,
}

fn parse_transforms(items: &[Value], ctx: &mut Ctx<'_>) -> Section<TransformSpec> {
    let base = Vec::new().key("transforms");
    let mut out = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        out.push(parse_transform(item, &base.idx(i), ctx));
    }
    Section {
        items: Some(out),
        ids: Vec::new(),
    }
}

fn parse_transform(item: &Value, base: &PathVec, ctx: &mut Ctx<'_>) -> Option<TransformSpec> {
    let Some(obj) = item.as_object() else {
        ctx.invalid_hint(
            base,
            Anchor::Key,
            "a transform must be a mapping with a `kind`",
            "for example `- kind: filter`",
        );
        return None;
    };
    let kind_path = base.key("kind");
    let kind = match obj.get("kind") {
        None => {
            ctx.error(
                codes::CONFIG_MISSING,
                &kind_path,
                Anchor::Key,
                "missing required field `kind`",
                Some(format!("one of {}", TRANSFORM_KINDS.join(", "))),
            );
            return None;
        }
        Some(Value::String(s)) => s.as_str(),
        Some(_) => {
            ctx.invalid(&kind_path, Anchor::Value, "`kind` must be a string");
            return None;
        }
    };
    let mut rest = obj.clone();
    rest.remove("kind");
    let rest = Value::Object(rest);

    match kind {
        "validate" => {
            let raw = typed::<ValidateRaw>(&rest, base, ctx)?;
            Some(TransformSpec::Validate {
                dataset: raw.dataset,
                sinks: raw.sinks,
                rules: raw.rules,
            })
        }
        "on_change" => {
            let raw = typed::<OnChangeRaw>(&rest, base, ctx)?;
            Some(TransformSpec::OnChange {
                dataset: raw.dataset,
                sinks: raw.sinks,
                fields: raw.fields,
                heartbeat: raw.heartbeat,
            })
        }
        "filter" => {
            if obj.contains_key("expr") {
                ctx.invalid_hint(
                    &base.key("expr"),
                    Anchor::Key,
                    "`expr:` is reserved for a future expression language and is not accepted in config version 1",
                    "write a structured predicate instead, for example `where: { field: level, ne: debug }`",
                );
                return None;
            }
            let raw = typed::<FilterRaw>(&rest, base, ctx)?;
            let where_path = base.key("where");
            let mut nodes = 0;
            let predicate = parse_predicate(&raw.where_, &where_path, 0, &mut nodes, ctx)?;
            Some(TransformSpec::Filter {
                dataset: raw.dataset,
                sinks: raw.sinks,
                where_: predicate,
            })
        }
        "sample" => {
            let raw = typed::<SampleRaw>(&rest, base, ctx)?;
            Some(TransformSpec::Sample {
                dataset: raw.dataset,
                sinks: raw.sinks,
                every_n: raw.every_n,
                min_interval: raw.min_interval,
            })
        }
        "enrich" => {
            let raw = typed::<EnrichRaw>(&rest, base, ctx)?;
            Some(TransformSpec::Enrich {
                dataset: raw.dataset,
                sinks: raw.sinks,
                file: raw.file,
                format: raw.format,
                key: raw.key,
                add: raw.add,
                on_miss: raw.on_miss.unwrap_or_default(),
            })
        }
        "rename" => {
            let raw = typed::<RenameRaw>(&rest, base, ctx)?;
            Some(TransformSpec::Rename {
                dataset: raw.dataset,
                sinks: raw.sinks,
                fields: raw.fields,
            })
        }
        "project" => {
            let raw = typed::<ProjectRaw>(&rest, base, ctx)?;
            Some(TransformSpec::Project {
                dataset: raw.dataset,
                sinks: raw.sinks,
                fields: raw.fields,
            })
        }
        "cast" => {
            let raw = typed::<CastRaw>(&rest, base, ctx)?;
            Some(TransformSpec::Cast {
                dataset: raw.dataset,
                sinks: raw.sinks,
                fields: raw.fields,
            })
        }
        "pose" => {
            let raw = typed::<PoseRaw>(&rest, base, ctx)?;
            Some(TransformSpec::Pose {
                dataset: raw.dataset,
                sinks: raw.sinks,
                prefix: raw.prefix.unwrap_or_else(crate::spec::default_pose_prefix),
                // An absent key keeps the engine default (`header.frame_id`);
                // an explicit `null` skips the frame check.
                frame_field: obj.contains_key("frame_field").then_some(raw.frame_field),
                quaternion_tolerance: raw
                    .quaternion_tolerance
                    .unwrap_or(crate::spec::DEFAULT_QUATERNION_TOLERANCE),
            })
        }
        "joint_state" => {
            let raw = typed::<JointStateRaw>(&rest, base, ctx)?;
            Some(TransformSpec::JointState {
                dataset: raw.dataset,
                sinks: raw.sinks,
                name_field: raw
                    .name_field
                    .unwrap_or_else(crate::spec::default_joint_name_field),
                position_field: raw
                    .position_field
                    .unwrap_or_else(crate::spec::default_joint_position_field),
                velocity_field: raw
                    .velocity_field
                    .unwrap_or_else(crate::spec::default_joint_velocity_field),
                effort_field: raw
                    .effort_field
                    .unwrap_or_else(crate::spec::default_joint_effort_field),
            })
        }
        "window" => {
            let raw = typed::<WindowRaw>(&rest, base, ctx)?;
            Some(TransformSpec::Window {
                dataset: raw.dataset,
                sinks: raw.sinks,
                size: raw.size,
                idle_close: raw.idle_close,
                aggregates: raw.aggregates,
            })
        }
        other => {
            let hint = closest(other, TRANSFORM_KINDS).map(|c| format!("did you mean `{c}`?"));
            ctx.error(
                codes::CONFIG_INVALID,
                &kind_path,
                Anchor::Value,
                format!(
                    "unknown transform kind `{other}`, expected one of {}",
                    TRANSFORM_KINDS
                        .iter()
                        .map(|k| format!("`{k}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                hint,
            );
            None
        }
    }
}

fn parse_predicate(
    v: &Value,
    path: &PathVec,
    depth: usize,
    nodes: &mut usize,
    ctx: &mut Ctx<'_>,
) -> Option<Predicate> {
    *nodes += 1;
    if *nodes == MAX_PREDICATE_NODES + 1 {
        ctx.invalid(
            path,
            Anchor::Key,
            format!(
                "a predicate may have at most {MAX_PREDICATE_NODES} conditions and combinators"
            ),
        );
    }
    if *nodes > MAX_PREDICATE_NODES {
        return None;
    }
    if depth > MAX_PREDICATE_DEPTH {
        ctx.invalid(
            path,
            Anchor::Key,
            format!("predicates nest at most {MAX_PREDICATE_DEPTH} deep"),
        );
        return None;
    }
    let Some(obj) = v.as_object() else {
        ctx.invalid_hint(
            path,
            Anchor::Value,
            "a predicate must be a mapping",
            "for example `{ field: level, ne: debug }` or `{ all: [ ... ] }`",
        );
        return None;
    };

    for combinator in ["all", "any", "not"] {
        let Some(inner) = obj.get(combinator) else {
            continue;
        };
        if obj.len() > 1 {
            ctx.invalid_hint(
                path,
                Anchor::Key,
                format!("`{combinator}` cannot be combined with other keys"),
                "put each condition in its own mapping inside the list",
            );
            return None;
        }
        let inner_path = path.key(combinator);
        return match combinator {
            "not" => parse_predicate(inner, &inner_path, depth + 1, nodes, ctx)
                .map(|p| Predicate::Not(Box::new(p))),
            _ => {
                let Some(items) = inner.as_array().filter(|a| !a.is_empty()) else {
                    ctx.invalid(
                        &inner_path,
                        Anchor::Value,
                        format!("`{combinator}` needs a non-empty list of predicates"),
                    );
                    return None;
                };
                let parsed: Vec<_> = items
                    .iter()
                    .enumerate()
                    .map(|(i, item)| {
                        parse_predicate(item, &inner_path.idx(i), depth + 1, nodes, ctx)
                    })
                    .collect();
                let all: Option<Vec<_>> = parsed.into_iter().collect();
                all.map(|ps| {
                    if combinator == "all" {
                        Predicate::All(ps)
                    } else {
                        Predicate::Any(ps)
                    }
                })
            }
        };
    }
    parse_condition(obj, path, ctx)
}

fn literal(v: &Value, path: &PathVec, ctx: &mut Ctx<'_>) -> Option<Literal> {
    match v {
        Value::Bool(b) => Some(Literal::Bool(*b)),
        Value::String(s) => Some(Literal::Str(s.clone())),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Literal::I64(i))
            } else if let Some(u) = n.as_u64() {
                Some(Literal::U64(u))
            } else {
                n.as_f64().filter(|f| f.is_finite()).map(Literal::F64)
            }
        }
        Value::Null => {
            ctx.invalid_hint(
                path,
                Anchor::Value,
                "a predicate value cannot be `null`",
                "to test for a null or missing value, write `{ field: <name>, null: true }`",
            );
            None
        }
        _ => {
            ctx.invalid(
                path,
                Anchor::Value,
                "a predicate value must be a boolean, number, or string",
            );
            None
        }
    }
}

fn numeric_literal(v: &Value, op: &str, path: &PathVec, ctx: &mut Ctx<'_>) -> Option<Literal> {
    let lit = literal(v, path, ctx)?;
    if lit.is_numeric() {
        Some(lit)
    } else {
        ctx.invalid(
            path,
            Anchor::Value,
            format!("`{op}` compares numbers, found {}", lit.kind()),
        );
        None
    }
}

fn parse_condition(
    obj: &Map<String, Value>,
    path: &PathVec,
    ctx: &mut Ctx<'_>,
) -> Option<Predicate> {
    let mut ok = true;
    for key in obj.keys() {
        if !PREDICATE_KEYS.contains(&key.as_str()) {
            let hint = closest(key, PREDICATE_KEYS).map(|c| format!("did you mean `{c}`?"));
            ctx.error(
                codes::CONFIG_INVALID,
                &path.key(key),
                Anchor::Key,
                format!(
                    "unknown predicate key `{key}`, expected `field` with one of eq, ne, lt, le, gt, ge, in, null, min, max, or all, any, not"
                ),
                hint,
            );
            ok = false;
        }
    }

    let field = match obj.get("field") {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        Some(_) => {
            ctx.invalid(
                &path.key("field"),
                Anchor::Value,
                "`field` must be a non-empty string",
            );
            ok = false;
            None
        }
        None => {
            ctx.error(
                codes::CONFIG_MISSING,
                &path.key("field"),
                Anchor::Key,
                "missing required field `field`",
                Some("a condition names the field it tests, for example `{ field: level, ne: debug }`".into()),
            );
            ok = false;
            None
        }
    };

    let simple: Vec<&str> = ["eq", "ne", "lt", "le", "gt", "ge", "in", "null"]
        .into_iter()
        .filter(|k| obj.contains_key(*k))
        .collect();
    let has_range = obj.contains_key("min") || obj.contains_key("max");

    if simple.is_empty() && !has_range {
        if ok {
            ctx.invalid_hint(
                path,
                Anchor::Key,
                "a condition needs a test",
                "add one of eq, ne, lt, le, gt, ge, in, null, or min/max",
            );
        }
        return None;
    }
    if simple.len() + usize::from(has_range) > 1 {
        ctx.invalid_hint(
            path,
            Anchor::Key,
            "a condition takes one test",
            "combine several tests with `all` or `any`; `min` and `max` may be used together",
        );
        return None;
    }

    let test = if has_range {
        let min = obj
            .get("min")
            .and_then(|v| numeric_literal(v, "min", &path.key("min"), ctx));
        let max = obj
            .get("max")
            .and_then(|v| numeric_literal(v, "max", &path.key("max"), ctx));
        if (obj.contains_key("min") && min.is_none()) || (obj.contains_key("max") && max.is_none())
        {
            return None;
        }
        Test::Between { min, max }
    } else {
        let op = simple[0];
        let value = obj.get(op)?;
        let op_path = path.key(op);
        match op {
            "eq" => Test::Eq(literal(value, &op_path, ctx)?),
            "ne" => Test::Ne(literal(value, &op_path, ctx)?),
            "lt" => Test::Lt(numeric_literal(value, op, &op_path, ctx)?),
            "le" => Test::Le(numeric_literal(value, op, &op_path, ctx)?),
            "gt" => Test::Gt(numeric_literal(value, op, &op_path, ctx)?),
            "ge" => Test::Ge(numeric_literal(value, op, &op_path, ctx)?),
            "null" => {
                let Value::Bool(b) = value else {
                    ctx.invalid_hint(
                        &op_path,
                        Anchor::Value,
                        "`null` takes a boolean (`true` or `false`)",
                        "`null: true` matches a null or missing value, `null: false` a present one",
                    );
                    return None;
                };
                Test::IsNull(*b)
            }
            _ => {
                let Some(items) = value
                    .as_array()
                    .filter(|a| !a.is_empty() && a.len() <= MAX_IN_LIST)
                else {
                    ctx.invalid(
                        &op_path,
                        Anchor::Value,
                        format!("`in` needs a list of 1 to {MAX_IN_LIST} values"),
                    );
                    return None;
                };
                let lits: Vec<_> = items
                    .iter()
                    .enumerate()
                    .map(|(i, v)| literal(v, &op_path.idx(i), ctx))
                    .collect();
                let lits: Option<Vec<_>> = lits.into_iter().collect();
                let lits = lits?;
                let numeric = lits.iter().filter(|l| l.is_numeric()).count();
                if numeric != 0 && numeric != lits.len() {
                    ctx.invalid(&op_path, Anchor::Value, "the values of `in` must all be numbers or all be the same non-numeric type");
                    return None;
                }
                Test::In(lits)
            }
        }
    };
    if !ok {
        return None;
    }
    Some(Predicate::Condition(Condition {
        field: field?,
        test,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closest_finds_typos() {
        assert_eq!(closest("wall", SECTIONS), Some("wal"));
        assert_eq!(closest("fromm", ["from", "name"]), Some("from"));
        assert_eq!(closest("zzzzzz", SECTIONS), None);
    }
}
