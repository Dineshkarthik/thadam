//! One compiled dataset: route, decode, identify, deduplicate, explode, shape,
//! stamp times, and run the dataset's transform steps.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use thadam_core::{
    AssetPath, ErrorDomain, Event, EventId, FieldDef, FieldType, Quality, QualityFlags, Record,
    ThadamError, Shape, ShapeRef, ShapeRegistry, SourceId, Sym, TimeDomain, Times, Value, WalPos,
    codes,
};
use serde_json::Value as Json;

use crate::decode::{Coerce, FieldPlan};
use crate::dedup::DedupWindow;
use crate::eventid::{IdBuf, positional_id, sample_id};
use crate::input::{RawRecordView, RejectedRaw};
use crate::limits::ChainLimits;
use crate::paths::{FieldPath, PathError, Template, TemplateContext, TemplatePart, TopicFilter};
use crate::ros2::record_to_json;
use crate::spec::{DatasetSpec, DecodeSpec, FieldSpec, parse_field_type};
use crate::steps::{EnrichmentInfo, Step, StepResult};
use crate::timex::{TimeParser, TimePlan};
use crate::units::{Unit, UnitConversion};

/// Counters for one dataset. `written` counts events handed to the sink;
/// `exploded` counts the samples `explode` produced, before the steps.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct DatasetStats {
    /// Messages routed to the dataset.
    pub messages: u64,
    /// Events emitted.
    pub written: u64,
    /// Events dropped by a step (`filter`, `on_change`, `sample`, `enrich`),
    /// or absorbed into an open `window`.
    pub filtered: u64,
    /// Messages dropped as duplicates.
    pub deduped: u64,
    /// Messages or samples rejected to quarantine.
    pub rejected: u64,
    /// Samples produced by `explode`.
    pub exploded: u64,
    /// Dedup ids forgotten because the window was full.
    pub dedup_evictions: u64,
    /// Per-asset step states forgotten because a step was full.
    pub state_evictions: u64,
}

struct Matcher {
    source: Option<SourceId>,
    topic: Option<TopicFilter>,
    attributes: Vec<(String, String)>,
}

impl Matcher {
    fn matches(&self, view: &RawRecordView<'_>) -> bool {
        self.source.as_ref().is_none_or(|s| s == view.source)
            && self
                .topic
                .as_ref()
                .is_none_or(|t| view.topic().is_some_and(|topic| t.matches(topic)))
            && self
                .attributes
                .iter()
                .all(|(k, v)| view.attrs.get(k) == Some(v.as_str()))
    }
}

struct Ctx<'a> {
    view: &'a RawRecordView<'a>,
    root: &'a Json,
}

impl TemplateContext for Ctx<'_> {
    fn topic(&self) -> Option<&str> {
        self.view.topic()
    }
    fn source(&self) -> &str {
        self.view.source.as_str()
    }
    fn attr(&self, key: &str) -> Option<&str> {
        self.view.attrs.get(key)
    }
    fn payload_text(&self, path: &FieldPath) -> Option<String> {
        match path.resolve(self.root, self.root)? {
            Json::String(s) => Some(s.clone()),
            Json::Number(n) => Some(n.to_string()),
            Json::Bool(b) => Some(b.to_string()),
            _ => None,
        }
    }
}

/// The template context of a decoded ROS 2 record: payload placeholders
/// resolve a single flattened token to one record field, rendered the way
/// the JSON bridge spelled it (integers and booleans as-is, floats through
/// `serde_json`'s number formatting, a timestamp as its nanoseconds). Lists
/// and records render as `None`, like the bridge's arrays and objects.
struct RecordCtx<'a> {
    view: &'a RawRecordView<'a>,
    record: &'a Record,
}

impl TemplateContext for RecordCtx<'_> {
    fn topic(&self) -> Option<&str> {
        self.view.topic()
    }
    fn source(&self) -> &str {
        self.view.source.as_str()
    }
    fn attr(&self, key: &str) -> Option<&str> {
        self.view.attrs.get(key)
    }
    fn payload_text(&self, path: &FieldPath) -> Option<String> {
        let token = path.sole_token()?;
        let value = self.record.get(token)?;
        match value {
            Value::Str(s) => Some(s.as_str().to_owned()),
            Value::Bool(b) => Some(b.to_string()),
            Value::I8(v) => Some(v.to_string()),
            Value::I16(v) => Some(v.to_string()),
            Value::I32(v) => Some(v.to_string()),
            Value::I64(v) => Some(v.to_string()),
            Value::U8(v) => Some(v.to_string()),
            Value::U16(v) => Some(v.to_string()),
            Value::U32(v) => Some(v.to_string()),
            Value::U64(v) => Some(v.to_string()),
            Value::F32(v) => serde_json::Number::from_f64(f64::from(*v)).map(|n| n.to_string()),
            Value::F64(v) => serde_json::Number::from_f64(*v).map(|n| n.to_string()),
            Value::Ts(t) => Some(t.as_nanos().to_string()),
            _ => None,
        }
    }
}

enum KeyPart {
    Level(usize),
    Field(FieldPlan),
}

/// Asset paths of a dataset whose identity depends on the topic alone, keyed by
/// topic so a device's events share one path and skip rendering. Bounded: once
/// full, new topics are rendered each time rather than remembered.
struct AssetCache {
    paths: HashMap<Box<str>, AssetPath>,
    cap: usize,
}

struct Explode {
    path: FieldPath,
    index_field: Sym,
}

pub(crate) struct Dataset {
    pub(crate) id: Sym,
    /// How the payload becomes the JSON root `process` reads.
    pub(crate) decode: DecodeSpec,
    matcher: Matcher,
    identity: Vec<(Sym, Template)>,
    fixed_asset: Option<AssetPath>,
    asset_cache: Option<AssetCache>,
    pub(crate) levels: Vec<Sym>,
    key: Option<Vec<KeyPart>>,
    dedup: Option<DedupWindow>,
    explode: Option<Explode>,
    fields: Vec<FieldPlan>,
    pub(crate) declared: ShapeRef,
    time: Option<TimePlan>,
    steps: Vec<Box<dyn Step>>,
    /// During warm-up, the index of the step records stop at; `None` runs
    /// every step.
    warm_to: Option<usize>,
    pub(crate) out_shape: ShapeRef,
    pub(crate) enrichment: Vec<EnrichmentInfo>,
    pub(crate) stats: DatasetStats,
    pub(crate) warm_evictions: (u64, u64),
    explode_limit: usize,
    /// `decode: ros2` with every payload pointer (fields, event-id fields,
    /// `time.field`, identity placeholders, `explode.path`) a single
    /// flattened token, so records are read directly without the JSON
    /// bridge.
    pub(crate) ros2_direct: bool,
    /// The quality the message being processed arrived with (a Sparkplug
    /// decoder's flags, status and reason); merged into every event it
    /// produces. `None` outside [`Dataset::process_sparkplug`].
    msg_quality: Option<Quality>,
}

fn schema_error(path: &str, code: &'static str, message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(ErrorDomain::Schema, code, message).with_context("path", path)
}

fn config_error(path: &str, message: impl Into<String>) -> ThadamError {
    crate::steps::config_error(path, message)
}

fn path_error(path: &str, e: &PathError) -> ThadamError {
    config_error(path, e.to_string())
}

struct CompiledField {
    plan: FieldPlan,
    def: FieldDef,
}

fn compile_field(
    spec: &FieldSpec,
    path: &str,
    exploded: bool,
    errs: &mut Vec<ThadamError>,
) -> Option<CompiledField> {
    let before = errs.len();
    let ty = match parse_field_type(&spec.ty) {
        Ok(t) => Some(t),
        Err(e) => {
            errs.push(schema_error(
                &format!("{path}.type"),
                codes::SCHEMA_UNSUPPORTED_TYPE,
                e,
            ));
            None
        }
    };
    let from = spec
        .from
        .clone()
        .unwrap_or_else(|| format!("/{}", spec.name));
    let field_path = match FieldPath::parse(&from) {
        Ok(p) => {
            if p.is_parent() && !exploded {
                errs.push(config_error(
                    &format!("{path}.from"),
                    "`^/` reads the parent message, which only exists in an exploded dataset",
                ));
            }
            Some(p)
        }
        Err(e) => {
            errs.push(path_error(&format!("{path}.from"), &e));
            None
        }
    };
    let ty = ty?;
    let is_ts = ty == FieldType::Ts;
    let is_float = matches!(&ty, FieldType::F32 | FieldType::F64)
        || matches!(&ty, FieldType::List(inner) if matches!(**inner, FieldType::F32 | FieldType::F64));

    let mut ts_parser = None;
    let mut conv = None;
    let mut declared_unit: Option<&str> = None;
    if is_ts {
        if spec.convert_to.is_some() {
            errs.push(config_error(
                &format!("{path}.convert_to"),
                "a `ts` field has no unit conversion",
            ));
        }
        match spec.unit.as_deref().map(|u| TimeParser::compile(u, None)) {
            Some(Ok(p)) => ts_parser = Some(p),
            Some(Err(e)) => errs.push(config_error(&format!("{path}.unit"), e)),
            None => errs.push(config_error(
                path,
                "a `ts` field needs `unit`: s, ms, us, ns, or rfc3339",
            )),
        }
    } else {
        declared_unit = spec.convert_to.as_deref().or(spec.unit.as_deref());
        match (spec.unit.as_deref(), spec.convert_to.as_deref()) {
            (Some(from_code), Some(to_code)) => {
                match UnitConversion::from_codes(from_code, to_code) {
                    Ok(c) if c.is_identity() => {}
                    Ok(c) if is_float => conv = Some(c),
                    Ok(_) => errs.push(schema_error(
                        path,
                        codes::SCHEMA_UNSUPPORTED_TYPE,
                        "a unit conversion needs a float field (f32, f64, or a list of them)",
                    )),
                    Err(e) => errs.push(
                        ThadamError::permanent(
                            ErrorDomain::Transform,
                            codes::TRANSFORM_UNKNOWN_UNIT,
                            e.to_string(),
                        )
                        .with_context("path", path),
                    ),
                }
            }
            (None, Some(_)) => errs.push(config_error(
                &format!("{path}.convert_to"),
                "`convert_to` needs the source `unit`",
            )),
            (Some(code), None) => {
                if let Err(e) = Unit::parse(code) {
                    errs.push(
                        ThadamError::permanent(
                            ErrorDomain::Transform,
                            codes::TRANSFORM_UNKNOWN_UNIT,
                            e.to_string(),
                        )
                        .with_context("path", format!("{path}.unit")),
                    );
                }
            }
            (None, None) => {}
        }
    }

    let Some(coerce) = Coerce::for_type(&ty, ts_parser) else {
        if errs.len() == before {
            errs.push(schema_error(
                &format!("{path}.type"),
                codes::SCHEMA_UNSUPPORTED_TYPE,
                format!("type {ty:?} is not supported in a declared field"),
            ));
        }
        return None;
    };
    if errs.len() != before {
        return None;
    }
    let mut def = FieldDef::new(spec.name.as_str(), ty);
    if let Some(unit) = declared_unit {
        def = def.with_unit(unit);
    }
    if spec.required {
        def = def.required();
    }
    Some(CompiledField {
        plan: FieldPlan {
            name: Arc::from(spec.name.as_str()),
            path: field_path?,
            coerce,
            conv,
            required: spec.required,
        },
        def,
    })
}

impl Dataset {
    pub(crate) fn compile(
        spec: &DatasetSpec,
        path: &str,
        limits: &ChainLimits,
        registry: &mut ShapeRegistry,
    ) -> Result<Dataset, Vec<ThadamError>> {
        let mut errs = Vec::new();
        if spec.id.is_empty() {
            errs.push(config_error(
                &format!("{path}.id"),
                "a dataset id must not be empty",
            ));
        }

        let topic = match spec.matcher.topic.as_deref().map(TopicFilter::parse) {
            Some(Ok(t)) => Some(t),
            Some(Err(e)) => {
                errs.push(path_error(&format!("{path}.match.topic"), &e));
                None
            }
            None => None,
        };
        let matcher = Matcher {
            source: spec.matcher.source.as_deref().map(SourceId::new),
            topic,
            attributes: spec
                .matcher
                .attributes
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };

        let exploded = spec.explode.is_some();
        let mut identity = Vec::new();
        let mut level_names: HashSet<&str> = HashSet::new();
        for (name, template) in &spec.identity.0 {
            let ipath = format!("{path}.identity.{name}");
            if name.is_empty() || !level_names.insert(name) {
                errs.push(config_error(
                    &ipath,
                    "identity level names must be unique and not empty",
                ));
                continue;
            }
            match Template::parse(template) {
                Ok(t) => identity.push((Sym::from(name.as_str()), t)),
                Err(e) => errs.push(path_error(&ipath, &e)),
            }
        }
        if identity.len() > thadam_core::MAX_ASSET_LEVELS {
            errs.push(config_error(
                &format!("{path}.identity"),
                format!(
                    "an asset path has at most {} levels",
                    thadam_core::MAX_ASSET_LEVELS
                ),
            ));
        }
        let levels: Vec<Sym> = identity.iter().map(|(n, _)| n.clone()).collect();
        let fixed_asset = if !identity.is_empty() && identity.iter().all(|(_, t)| t.is_literal()) {
            let lits = identity.iter().map(|(n, t)| {
                let text = t
                    .parts()
                    .iter()
                    .map(|p| match p {
                        crate::paths::TemplatePart::Literal(s) => s.as_str(),
                        _ => "",
                    })
                    .collect::<String>();
                (n.clone(), text)
            });
            AssetPath::new(lits).ok()
        } else {
            None
        };

        let topic_only = !identity.is_empty()
            && fixed_asset.is_none()
            && identity.iter().all(|(_, t)| {
                t.parts().iter().all(|p| {
                    matches!(
                        p,
                        crate::paths::TemplatePart::Literal(_)
                            | crate::paths::TemplatePart::Topic
                            | crate::paths::TemplatePart::TopicLevel(_)
                    )
                })
            });
        let asset_cache = topic_only.then(|| AssetCache {
            paths: HashMap::new(),
            cap: limits.max_state_keys,
        });

        let explode = match &spec.explode {
            None => None,
            Some(e) => {
                let epath = format!("{path}.explode");
                match FieldPath::parse(&e.path) {
                    Ok(p) if p.is_parent() => {
                        errs.push(config_error(
                            &format!("{epath}.path"),
                            "the explode path cannot be a `^/` parent path",
                        ));
                        None
                    }
                    Ok(p) => {
                        if e.index_field.is_empty() {
                            errs.push(config_error(
                                &format!("{epath}.index_field"),
                                "`index_field` must not be empty",
                            ));
                            None
                        } else {
                            Some(Explode {
                                path: p,
                                index_field: Sym::from(e.index_field.as_str()),
                            })
                        }
                    }
                    Err(err) => {
                        errs.push(path_error(&format!("{epath}.path"), &err));
                        None
                    }
                }
            }
        };

        let mut fields = Vec::new();
        let mut defs = Vec::new();
        let mut names: HashSet<&str> = HashSet::new();
        for (i, f) in spec.fields.iter().enumerate() {
            let fpath = format!("{path}.fields[{i}]");
            if !names.insert(&f.name) {
                errs.push(config_error(
                    &format!("{fpath}.name"),
                    format!("duplicate field `{}`", f.name),
                ));
                continue;
            }
            if level_names.contains(f.name.as_str()) {
                errs.push(config_error(
                    &format!("{fpath}.name"),
                    format!("field `{}` has the same name as an identity level", f.name),
                ));
                continue;
            }
            if let Some(c) = compile_field(f, &fpath, exploded, &mut errs) {
                fields.push(c.plan);
                defs.push(c.def);
            }
        }
        if let Some(e) = &spec.explode
            && (names.contains(e.index_field.as_str())
                || level_names.contains(e.index_field.as_str()))
        {
            errs.push(config_error(
                &format!("{path}.explode.index_field"),
                format!(
                    "`{}` collides with a field or identity level",
                    e.index_field
                ),
            ));
        }

        let time = match &spec.time {
            None => None,
            Some(t) => {
                let tpath = format!("{path}.time");
                if t.field.as_deref().is_some_and(|f| f.starts_with('^')) && !exploded {
                    errs.push(config_error(
                        &format!("{tpath}.field"),
                        "`^/` reads the parent message, which only exists in an exploded dataset",
                    ));
                }
                match TimePlan::compile(t) {
                    Ok(p) => Some(p),
                    Err(list) => {
                        errs.extend(
                            list.into_iter()
                                .map(|(k, m)| config_error(&format!("{tpath}.{k}"), m)),
                        );
                        None
                    }
                }
            }
        };

        let key = match &spec.event_id {
            None => {
                if spec.dedup.is_some() {
                    errs.push(config_error(
                        &format!("{path}.dedup"),
                        "`dedup` needs a content-derived `event_id.key`: without one, a redelivered message gets a new id and is never seen as a duplicate",
                    ));
                }
                None
            }
            Some(e) => {
                let kpath = format!("{path}.event_id.key");
                let mut parts = Vec::new();
                if e.key.is_empty() {
                    errs.push(config_error(
                        &kpath,
                        "`key` needs at least one field or identity level",
                    ));
                }
                for (i, name) in e.key.iter().enumerate() {
                    let ipath = format!("{kpath}[{i}]");
                    if let Some(li) = identity.iter().position(|(n, _)| **n == **name) {
                        parts.push(KeyPart::Level(li));
                        continue;
                    }
                    let Some(fi) = spec.fields.iter().position(|f| f.name == *name) else {
                        errs.push(config_error(
                            &ipath,
                            format!("`{name}` is neither an identity level nor a declared field"),
                        ));
                        continue;
                    };
                    let fspec = &spec.fields[fi];
                    let mut key_spec = fspec.clone();
                    key_spec.required = true;
                    let before = errs.len();
                    let Some(c) = compile_field(&key_spec, &ipath, exploded, &mut errs) else {
                        if errs.len() == before {
                            errs.push(config_error(&ipath, "the key field could not be compiled"));
                        }
                        continue;
                    };
                    if matches!(c.def.ty, FieldType::List(_)) {
                        errs.push(config_error(
                            &ipath,
                            format!("`{name}` is a list and cannot be part of an event id"),
                        ));
                        continue;
                    }
                    if exploded != c.plan.path.is_parent() {
                        errs.push(config_error(
                            &ipath,
                            if exploded {
                                format!("`{name}` is read per sample; a key field of an exploded dataset must read the parent message with `^/`")
                            } else {
                                format!("`{name}` cannot be a key field")
                            },
                        ));
                        continue;
                    }
                    parts.push(KeyPart::Field(c.plan));
                }
                Some(parts)
            }
        };

        let dedup = match (&spec.dedup, &key) {
            (Some(d), Some(_)) => {
                let dpath = format!("{path}.dedup");
                if d.window.0.is_zero() {
                    errs.push(config_error(
                        &format!("{dpath}.window"),
                        "`window` must be positive",
                    ));
                }
                if d.max_keys.0 == 0 {
                    errs.push(config_error(
                        &format!("{dpath}.max_keys"),
                        "`max_keys` must be at least 1",
                    ));
                }
                Some(DedupWindow::new(
                    d.window.0,
                    d.max_keys.0.min(limits.max_dedup_keys),
                ))
            }
            _ => None,
        };

        if !errs.is_empty() {
            return Err(errs);
        }
        let declared = match Shape::new(defs) {
            Ok(s) => s,
            Err(e) => return Err(vec![config_error(&format!("{path}.fields"), e.to_string())]),
        };
        let declared = registry
            .intern(declared)
            .map_err(|e| vec![schema_error(path, codes::SCHEMA_SHAPE_LIMIT, e.to_string())])?;

        let key_field = |part: &KeyPart| match part {
            KeyPart::Level(_) => true,
            KeyPart::Field(plan) => plan.path.sole_token().is_some(),
        };
        let ros2_direct = spec.decode == DecodeSpec::Ros2
            && fields.iter().all(|p| p.path.sole_token().is_some())
            && key.as_ref().is_none_or(|parts| parts.iter().all(key_field))
            && time.as_ref().is_none_or(TimePlan::uses_flat_pointer)
            && identity.iter().all(|(_, t)| {
                t.parts().iter().all(|part| match part {
                    TemplatePart::Payload(path) => path.sole_token().is_some(),
                    _ => true,
                })
            })
            && explode
                .as_ref()
                .is_none_or(|e| e.path.sole_token().is_some());

        Ok(Dataset {
            id: Sym::from(spec.id.as_str()),
            decode: spec.decode,
            matcher,
            identity,
            fixed_asset,
            asset_cache,
            levels,
            key,
            dedup,
            explode,
            fields,
            out_shape: declared.clone(),
            declared,
            time,
            steps: Vec::new(),
            warm_to: None,
            enrichment: Vec::new(),
            stats: DatasetStats::default(),
            warm_evictions: (0, 0),
            explode_limit: limits.max_explode_samples,
            ros2_direct,
            msg_quality: None,
        })
    }

    pub(crate) fn push_step(&mut self, step: Box<dyn Step>, out_shape: ShapeRef) {
        self.steps.push(step);
        self.out_shape = out_shape;
    }

    /// Whether the dataset explodes an array into events.
    pub(crate) fn explodes(&self) -> bool {
        self.explode.is_some()
    }

    /// Counts a routed message that produces no event by design (a Sparkplug
    /// STATE or CMD).
    pub(crate) fn skip_message(&mut self) {
        self.stats.messages += 1;
        self.stats.filtered += 1;
    }

    pub(crate) fn index_field(&self) -> Option<&Sym> {
        self.explode.as_ref().map(|e| &e.index_field)
    }

    pub(crate) fn time_domain(&self) -> TimeDomain {
        self.time
            .as_ref()
            .map_or(TimeDomain::Unknown, TimePlan::domain)
    }

    pub(crate) fn matches(&self, view: &RawRecordView<'_>) -> bool {
        self.matcher.matches(view)
    }

    /// Dedup and state evictions so far, including those during warm-up.
    pub(crate) fn evictions(&self) -> (u64, u64) {
        (
            self.dedup.as_ref().map_or(0, DedupWindow::evicted),
            self.steps.iter().map(|s| s.evicted()).sum(),
        )
    }

    pub(crate) fn snapshot_stats(&self) -> DatasetStats {
        let (dedup, state) = self.evictions();
        DatasetStats {
            dedup_evictions: dedup.saturating_sub(self.warm_evictions.0),
            state_evictions: state.saturating_sub(self.warm_evictions.1),
            ..self.stats.clone()
        }
    }

    fn build_asset_ctx(
        &mut self,
        view: &RawRecordView<'_>,
        ctx: &dyn TemplateContext,
    ) -> Result<AssetPath, ThadamError> {
        if let Some(fixed) = &self.fixed_asset {
            return Ok(fixed.clone());
        }
        if let Some(cache) = &self.asset_cache
            && let Some(topic) = view.topic()
            && let Some(hit) = cache.paths.get(topic)
        {
            return Ok(hit.clone());
        }
        let mut levels = Vec::with_capacity(self.identity.len());
        for (name, template) in &self.identity {
            let text = template.render(ctx).map_err(|e| {
                ThadamError::permanent(
                    ErrorDomain::Schema,
                    codes::SCHEMA_MISSING_FIELD,
                    format!("identity level `{name}`: {e}"),
                )
                .with_context("level", &**name)
            })?;
            levels.push((name.clone(), text));
        }
        let path = AssetPath::new(levels).map_err(|e| {
            ThadamError::permanent(
                ErrorDomain::Schema,
                codes::SCHEMA_INVALID_SHAPE,
                e.to_string(),
            )
        })?;
        if let Some(cache) = &mut self.asset_cache
            && let Some(topic) = view.topic()
            && cache.paths.len() < cache.cap
        {
            cache.paths.insert(topic.into(), path.clone());
        }
        Ok(path)
    }

    fn build_asset(
        &mut self,
        view: &RawRecordView<'_>,
        root: &Json,
    ) -> Result<AssetPath, ThadamError> {
        self.build_asset_ctx(view, &Ctx { view, root })
    }

    fn build_asset_record(
        &mut self,
        view: &RawRecordView<'_>,
        record: &Record,
    ) -> Result<AssetPath, ThadamError> {
        self.build_asset_ctx(view, &RecordCtx { view, record })
    }

    fn content_id(&self, root: &Json, asset: &AssetPath) -> Result<Option<EventId>, ThadamError> {
        let Some(parts) = &self.key else {
            return Ok(None);
        };
        let mut buf = IdBuf::content(&self.id);
        for part in parts {
            match part {
                KeyPart::Level(i) => {
                    let value = asset
                        .levels()
                        .get(*i)
                        .map(|(_, v)| v.as_str())
                        .unwrap_or_default();
                    buf.level(value);
                }
                KeyPart::Field(plan) => {
                    let value = plan.extract(root, root)?;
                    if !buf.value(&value) {
                        return Err(Self::key_type_error(plan));
                    }
                }
            }
        }
        Ok(Some(buf.finish()))
    }

    fn content_id_record(
        &self,
        root: &Record,
        asset: &AssetPath,
    ) -> Result<Option<EventId>, ThadamError> {
        let Some(parts) = &self.key else {
            return Ok(None);
        };
        let mut buf = IdBuf::content(&self.id);
        for part in parts {
            match part {
                KeyPart::Level(i) => {
                    let value = asset
                        .levels()
                        .get(*i)
                        .map(|(_, v)| v.as_str())
                        .unwrap_or_default();
                    buf.level(value);
                }
                KeyPart::Field(plan) => {
                    let value = plan.extract_record(Some(root), root)?;
                    if !buf.value(&value) {
                        return Err(Self::key_type_error(plan));
                    }
                }
            }
        }
        Ok(Some(buf.finish()))
    }

    fn key_type_error(plan: &FieldPlan) -> ThadamError {
        ThadamError::permanent(
            ErrorDomain::Schema,
            codes::SCHEMA_TYPE_MISMATCH,
            format!("key field `{}` cannot form an event id", plan.name),
        )
    }

    fn reject(
        &mut self,
        view: &RawRecordView<'_>,
        sample: Option<u32>,
        error: ThadamError,
        rejects: &mut Vec<RejectedRaw>,
    ) {
        self.stats.rejected += 1;
        rejects.push(RejectedRaw::new(view, Some(&self.id), sample, error));
    }

    /// Runs one routed message through the dataset. Events land in `out`;
    /// anything that cannot become an event lands in `rejects` with its raw
    /// bytes.
    pub(crate) fn process(
        &mut self,
        view: &RawRecordView<'_>,
        root: &Json,
        out: &mut Vec<Event>,
        rejects: &mut Vec<RejectedRaw>,
    ) {
        self.stats.messages += 1;
        self.process_json(view, root, out, rejects);
    }

    /// Like [`Dataset::process`], for a Sparkplug message: every event the
    /// message yields carries `quality` (flags are merged with the dataset's
    /// own, such as `CLOCK_SKEW`).
    pub(crate) fn process_sparkplug(
        &mut self,
        view: &RawRecordView<'_>,
        root: &Json,
        quality: &Quality,
        out: &mut Vec<Event>,
        rejects: &mut Vec<RejectedRaw>,
    ) {
        self.msg_quality = Some(quality.clone());
        self.process(view, root, out, rejects);
        self.msg_quality = None;
    }

    fn process_json(
        &mut self,
        view: &RawRecordView<'_>,
        root: &Json,
        out: &mut Vec<Event>,
        rejects: &mut Vec<RejectedRaw>,
    ) {
        let asset = match self.build_asset(view, root) {
            Ok(a) => a,
            Err(e) => return self.reject(view, None, e, rejects),
        };
        let id = match self.content_id(root, &asset) {
            Ok(id) => id,
            Err(e) => return self.reject(view, None, e, rejects),
        };
        if let (Some(window), Some(id)) = (&mut self.dedup, id)
            && window.is_duplicate(id, view.ingest_time)
        {
            self.stats.deduped += 1;
            return;
        }
        let msg = Msg {
            view,
            root,
            id,
            asset: &asset,
        };

        let Some(explode) = &self.explode else {
            self.emit(&msg, root, None, out, rejects);
            return;
        };
        let samples = match explode.path.resolve(root, root) {
            Some(Json::Array(items)) => items,
            Some(Json::Null) | None => {
                let e = ThadamError::permanent(
                    ErrorDomain::Schema,
                    codes::SCHEMA_MISSING_FIELD,
                    format!("explode path `{}` is missing", explode.path.as_str()),
                );
                return self.reject(view, None, e, rejects);
            }
            Some(_) => {
                let e = ThadamError::permanent(
                    ErrorDomain::Schema,
                    codes::SCHEMA_TYPE_MISMATCH,
                    format!("explode path `{}` is not an array", explode.path.as_str()),
                );
                return self.reject(view, None, e, rejects);
            }
        };
        if samples.len() > self.explode_limit {
            let e = ThadamError::permanent(
                ErrorDomain::Decode,
                codes::DECODE_TOO_LARGE,
                "message has more samples than the explode limit",
            )
            .with_context("limit", self.explode_limit.to_string());
            return self.reject(view, None, e, rejects);
        }
        for (i, sample) in samples.iter().enumerate() {
            self.stats.exploded += 1;
            let idx = u32::try_from(i).unwrap_or(u32::MAX);
            self.emit(&msg, sample, Some(idx), out, rejects);
        }
    }

    /// Runs one routed ROS 2 message held as a decoded record — the
    /// bridge-free path for [`Dataset::ros2_direct`] datasets. Every payload
    /// pointer resolves to one field lookup. The single runtime case that
    /// needs the JSON spelling is an `explode` over a typed array or raw
    /// bytes (which the bridge spelled as a JSON array): only then is
    /// `bridge` materialized, and this message runs through
    /// [`Dataset::process_json`].
    pub(crate) fn process_record(
        &mut self,
        view: &RawRecordView<'_>,
        record: &Record,
        bridge: &mut Option<Json>,
        out: &mut Vec<Event>,
        rejects: &mut Vec<RejectedRaw>,
    ) {
        self.stats.messages += 1;
        let asset = match self.build_asset_record(view, record) {
            Ok(a) => a,
            Err(e) => return self.reject(view, None, e, rejects),
        };
        let id = match self.content_id_record(record, &asset) {
            Ok(id) => id,
            Err(e) => return self.reject(view, None, e, rejects),
        };
        if let (Some(window), Some(id)) = (&mut self.dedup, id)
            && window.is_duplicate(id, view.ingest_time)
        {
            self.stats.deduped += 1;
            return;
        }
        let msg = MsgRecord {
            view,
            id,
            asset: &asset,
        };

        let Some(explode) = &self.explode else {
            self.emit_record(&msg, record, Some(record), None, out, rejects);
            return;
        };
        let Some(token) = explode.path.sole_token() else {
            let e = ThadamError::fatal(
                ErrorDomain::Config,
                codes::CONFIG_MISSING,
                "a direct-read explode path is one pointer token (the compile gate missed it)",
            );
            return self.reject(view, None, e, rejects);
        };
        match record.get(token) {
            None | Some(Value::Null) => {
                let e = ThadamError::permanent(
                    ErrorDomain::Schema,
                    codes::SCHEMA_MISSING_FIELD,
                    format!("explode path `{}` is missing", explode.path.as_str()),
                );
                self.reject(view, None, e, rejects);
            }
            Some(Value::List(samples)) => {
                if samples.len() > self.explode_limit {
                    let e = ThadamError::permanent(
                        ErrorDomain::Decode,
                        codes::DECODE_TOO_LARGE,
                        "message has more samples than the explode limit",
                    )
                    .with_context("limit", self.explode_limit.to_string());
                    return self.reject(view, None, e, rejects);
                }
                for (i, sample) in samples.iter().enumerate() {
                    self.stats.exploded += 1;
                    let idx = u32::try_from(i).unwrap_or(u32::MAX);
                    self.emit_record(&msg, record, sample.as_record(), Some(idx), out, rejects);
                }
            }
            Some(
                Value::F32s(_) | Value::F64s(_) | Value::I32s(_) | Value::I64s(_) | Value::Bytes(_),
            ) => {
                let root = bridge.get_or_insert_with(|| record_to_json(record));
                self.process_json(view, root, out, rejects);
            }
            Some(_) => {
                let e = ThadamError::permanent(
                    ErrorDomain::Schema,
                    codes::SCHEMA_TYPE_MISMATCH,
                    format!("explode path `{}` is not an array", explode.path.as_str()),
                );
                self.reject(view, None, e, rejects);
            }
        }
    }

    fn emit(
        &mut self,
        msg: &Msg<'_>,
        scope: &Json,
        sample: Option<u32>,
        out: &mut Vec<Event>,
        rejects: &mut Vec<RejectedRaw>,
    ) {
        let view = msg.view;
        let mut values = Vec::with_capacity(self.fields.len());
        let mut failure = None;
        for field in &self.fields {
            match field.extract(scope, msg.root) {
                Ok(v) => values.push(v),
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            }
        }
        if let Some(e) = failure {
            return self.reject(view, sample, e, rejects);
        }
        let record = match Record::new(self.declared.clone(), values) {
            Ok(r) => r,
            Err(e) => {
                let e = ThadamError::permanent(
                    ErrorDomain::Schema,
                    codes::SCHEMA_INVALID_SHAPE,
                    e.to_string(),
                );
                return self.reject(view, sample, e, rejects);
            }
        };

        let (times, flags) = match &self.time {
            Some(plan) => match plan.extract(scope, msg.root, view.attrs) {
                Ok(source) => plan.times(view.ingest_time, source),
                Err(e) => return self.reject(view, sample, e, rejects),
            },
            None => (
                Times::new(view.ingest_time, TimeDomain::Unknown),
                QualityFlags::empty(),
            ),
        };

        self.finish_event(
            view, msg.id, msg.asset, record, times, flags, sample, out, rejects,
        );
    }

    /// The record-read counterpart of [`Dataset::emit`]: `scope` is the
    /// nested record of an exploded sample, or `None` for a sample that is
    /// not a record (which resolves like a JSON scalar scope).
    fn emit_record(
        &mut self,
        msg: &MsgRecord<'_>,
        root: &Record,
        scope: Option<&Record>,
        sample: Option<u32>,
        out: &mut Vec<Event>,
        rejects: &mut Vec<RejectedRaw>,
    ) {
        let view = msg.view;
        let mut values = Vec::with_capacity(self.fields.len());
        let mut failure = None;
        for field in &self.fields {
            match field.extract_record(scope, root) {
                Ok(v) => values.push(v),
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            }
        }
        if let Some(e) = failure {
            return self.reject(view, sample, e, rejects);
        }
        let record = match Record::new(self.declared.clone(), values) {
            Ok(r) => r,
            Err(e) => {
                let e = ThadamError::permanent(
                    ErrorDomain::Schema,
                    codes::SCHEMA_INVALID_SHAPE,
                    e.to_string(),
                );
                return self.reject(view, sample, e, rejects);
            }
        };

        let (times, flags) = match &self.time {
            Some(plan) => match plan.extract_record(scope, root, view.attrs) {
                Ok(source) => plan.times(view.ingest_time, source),
                Err(e) => return self.reject(view, sample, e, rejects),
            },
            None => (
                Times::new(view.ingest_time, TimeDomain::Unknown),
                QualityFlags::empty(),
            ),
        };
        self.finish_event(
            view, msg.id, msg.asset, record, times, flags, sample, out, rejects,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_event(
        &mut self,
        view: &RawRecordView<'_>,
        id: Option<EventId>,
        asset: &AssetPath,
        record: Record,
        times: Times,
        flags: QualityFlags,
        sample: Option<u32>,
        out: &mut Vec<Event>,
        rejects: &mut Vec<RejectedRaw>,
    ) {
        let id = match (id, sample) {
            (Some(m), Some(i)) => sample_id(m, i),
            (Some(m), None) => m,
            (None, s) => positional_id(view.epoch, view.pos, view.record_index, s),
        };
        let mut quality = Quality {
            flags,
            ..Quality::good()
        };
        if let Some(msg) = &self.msg_quality {
            quality.flags |= msg.flags;
            quality.status = msg.status;
            quality.reason = msg.reason.clone();
        }
        let mut builder = Event::builder(id, view.pos, times)
            .asset(asset.clone())
            .quality(quality)
            .payload(record);
        if let Some(i) = sample {
            builder = builder.sample_idx(i);
        }
        let event = match builder.build() {
            Ok(e) => e,
            Err(e) => {
                let e = ThadamError::permanent(
                    ErrorDomain::Transform,
                    codes::TRANSFORM_FAILED,
                    e.to_string(),
                );
                return self.reject(view, sample, e, rejects);
            }
        };

        self.run_steps(0, event, view, sample, out, rejects);
    }

    /// Runs `event` through the steps from `first` on. A step that emits
    /// several events continues each of them through the steps after it.
    fn run_steps(
        &mut self,
        first: usize,
        mut event: Event,
        view: &RawRecordView<'_>,
        sample: Option<u32>,
        out: &mut Vec<Event>,
        rejects: &mut Vec<RejectedRaw>,
    ) {
        let end = self.warm_to.unwrap_or(self.steps.len());
        for i in first..end {
            let Some(step) = self.steps.get_mut(i) else {
                break;
            };
            match step.apply(event) {
                StepResult::Keep(next) => event = next,
                StepResult::Filtered => {
                    self.stats.filtered += 1;
                    return;
                }
                StepResult::Reject(e) => {
                    self.stats.rejected += 1;
                    rejects.push(RejectedRaw::new(view, Some(&self.id), sample, e));
                    return;
                }
                StepResult::Emit(events) => {
                    if events.is_empty() {
                        self.stats.filtered += 1;
                    }
                    for next in events {
                        self.run_steps(i + 1, next, view, sample, out, rejects);
                    }
                    return;
                }
            }
        }
        if end < self.steps.len() {
            return;
        }
        self.stats.written += 1;
        out.push(event);
    }

    /// The oldest WAL position an open window still needs; see
    /// [`Step::hold`].
    pub(crate) fn hold(&self) -> Option<WalPos> {
        self.steps.iter().filter_map(|s| s.hold()).min()
    }

    /// Whether a step's state is rebuilt by re-reading from [`Dataset::hold`].
    pub(crate) fn holds_back(&self) -> bool {
        self.steps.iter().any(|s| s.windowed())
    }

    /// With `stop`, records go no further than the first window step. Warm-up
    /// covers frames before the cursor, which the hold-back guarantees no
    /// pending window needs; feeding them would only open windows whose
    /// aggregates the sink already holds, and pin the cursor on them.
    pub(crate) fn stop_at_windows(&mut self, stop: bool) {
        self.warm_to = if stop {
            self.steps.iter().position(|s| s.windowed())
        } else {
            None
        };
    }
}

struct Msg<'a> {
    view: &'a RawRecordView<'a>,
    root: &'a Json,
    id: Option<EventId>,
    asset: &'a AssetPath,
}

struct MsgRecord<'a> {
    view: &'a RawRecordView<'a>,
    id: Option<EventId>,
    asset: &'a AssetPath,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn an_omitted_from_reads_the_flattened_dotted_key() {
        // The default pointer is `/<name>`: one JSON Pointer token, so a
        // dotted name reads the decoder's flattened key as the ros2 bridge
        // writes it, never a nested path.
        let spec = FieldSpec {
            name: "pose.position.x".to_owned(),
            ty: "f64".to_owned(),
            from: None,
            required: false,
            unit: None,
            convert_to: None,
        };
        let mut errs = Vec::new();
        let compiled = compile_field(&spec, "datasets[0].fields[0]", false, &mut errs)
            .expect("the field compiles");
        assert!(errs.is_empty(), "{errs:?}");
        assert_eq!(compiled.plan.path.as_str(), "/pose.position.x");

        let flat = json!({ "pose.position.x": 1.5 });
        assert_eq!(compiled.plan.path.resolve(&flat, &flat), Some(&json!(1.5)));
        let nested = json!({ "pose": { "position": { "x": 1.5 } } });
        assert_eq!(compiled.plan.path.resolve(&nested, &nested), None);
    }
}
