//! Cross-field and cross-section checks that serde cannot express: unique ids,
//! references between sections, identifier and pointer syntax, and predicate
//! names and literal types resolved against the declared schema.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::path::Path;

use thadam_core::codes;
use thadam_core::{
    FieldType, MAX_ASSET_LEVELS, MAX_SHAPE_FIELDS, QualityFlags, QualityStatus, ValueType,
};

use crate::check::{Parsed, Section, closest};
use crate::ctx::Ctx;
use crate::path::{PathExt, PathVec};
use crate::predicate::{Condition, Literal, Predicate, Test};
use crate::scalars::FieldTypeSpec;
use crate::spans::Anchor;
use crate::spec::{
    ConnectorSpec, DECLARED_FIELD_ID_RANGE, DatasetSpec, EnrichFieldSpec, EnrichFormat,
    EnrichKeySpec, FieldSpec, IDENTITY_LEVEL_ID_RANGE, MaxLagSpec, SinkSpec, TimeUnit,
    TransformSpec, WalSpec, WatchField,
};

/// What the connector checks read from a source or a sink.
trait Connector {
    fn kind(&self) -> &str;
    fn config(&self) -> &serde_json::Value;
}

impl Connector for ConnectorSpec {
    fn kind(&self) -> &str {
        &self.kind
    }

    fn config(&self) -> &serde_json::Value {
        &self.config
    }
}

impl Connector for SinkSpec {
    fn kind(&self) -> &str {
        &self.kind
    }

    fn config(&self) -> &serde_json::Value {
        &self.config
    }
}

/// Which kind of column a Parquet field ID belongs to.
#[derive(Clone, Copy)]
enum IdKind {
    Field,
    Level,
}

impl IdKind {
    fn range(self) -> RangeInclusive<u32> {
        match self {
            IdKind::Field => DECLARED_FIELD_ID_RANGE,
            IdKind::Level => IDENTITY_LEVEL_ID_RANGE,
        }
    }

    fn noun(self) -> &'static str {
        match self {
            IdKind::Field => "field",
            IdKind::Level => "identity level",
        }
    }
}

/// Constraints the loader enforces that depend on what the binary has registered.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct KindPolicy {
    /// The registered source kinds; `None` accepts any well-formed kind.
    pub source_kinds: Option<Vec<String>>,
    /// The registered sink kinds; `None` accepts any well-formed kind.
    pub sink_kinds: Option<Vec<String>>,
}

impl KindPolicy {
    /// A policy that accepts any well-formed kind.
    pub fn new() -> Self {
        Self::default()
    }

    /// Accepts only these source kinds.
    #[must_use]
    pub fn sources<S: Into<String>>(mut self, kinds: impl IntoIterator<Item = S>) -> Self {
        self.source_kinds = Some(kinds.into_iter().map(Into::into).collect());
        self
    }

    /// Accepts only these sink kinds.
    #[must_use]
    pub fn sinks<S: Into<String>>(mut self, kinds: impl IntoIterator<Item = S>) -> Self {
        self.sink_kinds = Some(kinds.into_iter().map(Into::into).collect());
        self
    }
}

pub(crate) fn id_ok(s: &str) -> bool {
    let mut chars = s.chars();
    s.len() <= 64
        && matches!(chars.next(), Some(c) if c.is_ascii_alphanumeric())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn name_ok(s: &str) -> bool {
    let mut chars = s.chars();
    s.len() <= 63
        && matches!(chars.next(), Some(c) if c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn label_ok(s: &str) -> bool {
    let mut chars = s.chars();
    s.len() <= 64
        && matches!(chars.next(), Some(c) if c.is_ascii_alphanumeric())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

fn kind_ok(s: &str) -> bool {
    let mut chars = s.chars();
    s.len() <= 32
        && matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Validates an RFC 6901 JSON Pointer, with the `^/` parent prefix when allowed.
pub(crate) fn check_pointer(s: &str, parent_allowed: bool) -> Result<bool, String> {
    let (body, parent) = match s.strip_prefix("^/") {
        Some(rest) => (rest, true),
        None => match s.strip_prefix('/') {
            Some(rest) => (rest, false),
            None => {
                return Err(format!(
                    "`{s}` is not a JSON Pointer: it must start with `/` (or `^/` for the parent message)"
                ));
            }
        },
    };
    if parent && !parent_allowed {
        return Err(
            "`^/` reads from the parent message and is only valid in a dataset with `explode`"
                .into(),
        );
    }
    for token in body.split('/') {
        if token.is_empty() {
            return Err(format!("`{s}` has an empty path segment"));
        }
        let mut chars = token.chars();
        while let Some(c) = chars.next() {
            if c == '~' && !matches!(chars.next(), Some('0' | '1')) {
                return Err(format!(
                    "`{s}` has an invalid escape: `~` must be `~0` or `~1`"
                ));
            }
        }
    }
    Ok(parent)
}

fn mqtt_filter_problem(filter: &str) -> Option<String> {
    if filter.is_empty() {
        return Some("a topic filter must not be empty".into());
    }
    if filter.contains('\0') {
        return Some("a topic filter must not contain NUL".into());
    }
    let levels: Vec<&str> = filter.split('/').collect();
    for (i, level) in levels.iter().enumerate() {
        if level.contains('#') && (*level != "#" || i + 1 != levels.len()) {
            return Some("`#` must be a whole level and the last one".into());
        }
        if level.contains('+') && *level != "+" {
            return Some("`+` must be a whole level".into());
        }
    }
    None
}

/// The number of topic levels a filter always has, or `None` when `#` makes it open-ended.
fn topic_levels(filter: &str) -> Option<usize> {
    (!filter.contains('#')).then(|| filter.split('/').count())
}

fn check_template(template: &str, levels: Option<usize>) -> Result<(), String> {
    let mut rest = template;
    while let Some(open) = rest.find(['{', '}']) {
        if rest.as_bytes()[open] == b'}' {
            return Err("unmatched `}`".into());
        }
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            return Err("unterminated `{`".into());
        };
        let inner = &after[..close];
        let Some(n) = inner
            .strip_prefix("topic.")
            .and_then(|n| n.parse::<usize>().ok())
        else {
            return Err(format!(
                "unknown placeholder `{{{inner}}}`: only `{{topic.N}}` (the Nth topic level, from 0) is supported"
            ));
        };
        if let Some(levels) = levels
            && n >= levels
        {
            return Err(format!(
                "`{{topic.{n}}}` is out of range: the topic filter has {levels} levels (0 to {})",
                levels - 1
            ));
        }
        rest = &after[close + 1..];
    }
    Ok(())
}

fn int_range(ty: &FieldType) -> Option<(i128, i128)> {
    Some(match ty {
        FieldType::I8 => (i8::MIN.into(), i8::MAX.into()),
        FieldType::I16 => (i16::MIN.into(), i16::MAX.into()),
        FieldType::I32 => (i32::MIN.into(), i32::MAX.into()),
        FieldType::I64 => (i64::MIN.into(), i64::MAX.into()),
        FieldType::U8 => (0, u8::MAX.into()),
        FieldType::U16 => (0, u16::MAX.into()),
        FieldType::U32 => (0, u32::MAX.into()),
        FieldType::U64 => (0, u64::MAX.into()),
        _ => return None,
    })
}

fn is_numeric(ty: &FieldType) -> bool {
    int_range(ty).is_some() || matches!(ty, FieldType::F32 | FieldType::F64)
}

fn type_label(ty: &FieldType) -> String {
    crate::scalars::FieldTypeSpec(ty.clone()).canonical()
}

/// What a name in a predicate or rule resolves to.
enum Target {
    Typed(FieldType),
    Status,
    Flags,
}

/// The fields a step sees: the dataset's declared fields, changed by the
/// `rename`, `project`, `cast`, and `enrich` steps before it.
type FieldMap = BTreeMap<String, FieldType>;

struct Names {
    dataset: String,
    fields: FieldMap,
    identity: HashSet<String>,
    index_field: Option<String>,
}

impl Names {
    fn with_fields(ds: &DatasetSpec, fields: FieldMap) -> Self {
        Names {
            dataset: ds.id.clone(),
            fields,
            identity: ds.identity.iter().map(|l| l.name.clone()).collect(),
            index_field: ds.explode.as_ref().and_then(|e| e.index_field.clone()),
        }
    }

    /// Whether a new field may not take `name`: it is a field, an identity
    /// level, or the `explode` index field already.
    fn taken(&self, name: &str) -> Option<&'static str> {
        if self.fields.contains_key(name) {
            Some("a field")
        } else if self.identity.contains(name) {
            Some("an identity level")
        } else if self.index_field.as_deref() == Some(name) {
            Some("the `explode.index_field`")
        } else {
            None
        }
    }

    fn resolve(&self, name: &str) -> Option<Target> {
        match name {
            "quality.status" => return Some(Target::Status),
            "quality.flags" => return Some(Target::Flags),
            "quality.reason" => return Some(Target::Typed(FieldType::Str)),
            _ => {}
        }
        if let Some(ty) = self.fields.get(name) {
            return Some(Target::Typed(ty.clone()));
        }
        if self.identity.contains(name) {
            return Some(Target::Typed(FieldType::Str));
        }
        if self.index_field.as_deref() == Some(name) {
            return Some(Target::Typed(FieldType::U32));
        }
        None
    }

    fn all(&self) -> Vec<&str> {
        self.fields
            .keys()
            .map(String::as_str)
            .chain(self.identity.iter().map(String::as_str))
            .chain(self.index_field.as_deref())
            .chain(["quality.status", "quality.flags", "quality.reason"])
            .collect()
    }
}

fn declared_fields(ds: &DatasetSpec) -> FieldMap {
    ds.fields
        .iter()
        .map(|f| (f.name.clone(), f.ty.field_type().clone()))
        .collect()
}

struct Checker<'p, 'c, 'a> {
    ctx: &'c mut Ctx<'a>,
    parsed: &'p Parsed,
    source_ids: HashSet<String>,
    sink_ids: HashSet<String>,
    dataset_ids: HashSet<String>,
}

pub(crate) fn check(parsed: &Parsed, kinds: &KindPolicy, ctx: &mut Ctx<'_>) {
    let ids = |s: &[Option<String>]| s.iter().flatten().cloned().collect::<HashSet<_>>();
    let mut c = Checker {
        source_ids: ids(&parsed.sources.ids),
        sink_ids: ids(&parsed.sinks.ids),
        dataset_ids: ids(&parsed.datasets.ids),
        ctx,
        parsed,
    };
    c.pipeline();
    c.wal();
    c.quarantine();
    c.connectors("sources", &parsed.sources, kinds.source_kinds.as_deref());
    c.connectors("sinks", &parsed.sinks, kinds.sink_kinds.as_deref());
    c.sink_policies();
    c.unique_ids("datasets", &parsed.datasets.ids);
    c.datasets();
    c.transforms();
    c.observability();
    c.runtime();
}

impl<'p> Checker<'p, '_, '_> {
    fn pipeline(&mut self) {
        let Some(p) = &self.parsed.pipeline else {
            return;
        };
        let base = Vec::new().key("pipeline");
        for (key, value) in [("name", &p.name), ("node_id", &p.node_id)] {
            if !label_ok(value) {
                self.ctx.invalid_hint(
                    &base.key(key),
                    Anchor::Value,
                    format!("`{key}` must be 1 to 64 characters: letters, digits, `.`, `_`, `-`, starting with a letter or digit"),
                    "it is used in file names and object keys",
                );
            }
        }
        if p.data_dir.as_os_str().is_empty() {
            self.ctx.invalid(
                &base.key("data_dir"),
                Anchor::Value,
                "`data_dir` must not be empty",
            );
        }
    }

    fn wal(&mut self) {
        let Some(w) = &self.parsed.wal else { return };
        let base = Vec::new().key("wal");
        let WalSpec {
            max_disk,
            segment_bytes,
            fsync,
            retain_for,
            max_pending_bytes,
            ..
        } = w;
        if segment_bytes.get() < 1 << 20 {
            self.ctx.invalid(
                &base.key("segment_bytes"),
                Anchor::Value,
                "`segment_bytes` must be at least 1MiB",
            );
        } else if max_disk.get() < segment_bytes.get() {
            self.ctx.invalid_hint(
                &base.key("max_disk"),
                Anchor::Value,
                "`max_disk` is smaller than one segment",
                "raise `max_disk` or lower `segment_bytes`",
            );
        } else if max_disk.get() < segment_bytes.get().saturating_mul(4) {
            self.ctx.warn(
                codes::CONFIG_INVALID,
                &base.key("max_disk"),
                Anchor::Value,
                "`max_disk` holds fewer than four segments, so retention and `on_full` act on very coarse units",
                Some("raise `max_disk` or lower `segment_bytes`".into()),
            );
        }
        if max_pending_bytes.get() == 0 {
            self.ctx.invalid(
                &base.key("max_pending_bytes"),
                Anchor::Value,
                "`max_pending_bytes` must be greater than zero",
            );
        }
        if let crate::spec::FsyncSpec::Interval(d) = fsync
            && d.get().is_zero()
        {
            self.ctx.invalid_hint(
                &base.key("fsync"),
                Anchor::Value,
                "the fsync interval must be greater than zero",
                "use `fsync: always` to sync after every commit",
            );
        }
        if retain_for.is_some_and(|d| d.get().is_zero()) {
            self.ctx.invalid(&base.key("retain_for"), Anchor::Value, "`retain_for` must be greater than zero; omit it to keep nothing beyond consumption");
        }
    }

    fn quarantine(&mut self) {
        if let Some(q) = &self.parsed.quarantine
            && q.max_disk.get() == 0
        {
            self.ctx.invalid(
                &Vec::new().key("quarantine").key("max_disk"),
                Anchor::Value,
                "`max_disk` must be greater than zero",
            );
        }
    }

    fn unique_ids(&mut self, section: &str, ids: &[Option<String>]) {
        let mut seen = HashSet::new();
        for (i, id) in ids.iter().enumerate() {
            let Some(id) = id else { continue };
            let path = Vec::new().key(section).idx(i).key("id");
            if !id_ok(id) {
                self.ctx.invalid_hint(
                    &path,
                    Anchor::Value,
                    format!("`{id}` is not a valid id"),
                    "ids are 1 to 64 characters: letters, digits, `_`, `-`, starting with a letter or digit",
                );
            }
            if !seen.insert(id.as_str()) {
                self.ctx.invalid(
                    &path,
                    Anchor::Value,
                    format!("duplicate id `{id}` in `{section}`"),
                );
            }
        }
    }

    fn connectors<T: Connector>(
        &mut self,
        section: &str,
        items: &Section<T>,
        registered: Option<&[String]>,
    ) {
        self.unique_ids(section, &items.ids);
        let Some(list) = &items.items else { return };
        if list.is_empty() {
            self.ctx.invalid_hint(
                &Vec::new().key(section),
                Anchor::Key,
                format!("`{section}` needs at least one entry"),
                "add an entry with `id`, `kind`, and `config`",
            );
        }
        for (i, spec) in items.present() {
            let path = Vec::new().key(section).idx(i).key("kind");
            let kind = spec.kind();
            if !kind_ok(kind) {
                self.ctx.invalid(
                    &path,
                    Anchor::Value,
                    format!("`{kind}` is not a valid kind: use lowercase letters, digits, and `_`"),
                );
            } else if let Some(registered) = registered
                && !registered.iter().any(|r| r == kind)
            {
                let hint = closest(kind, registered.iter().map(String::as_str))
                    .map(|c| format!("did you mean `{c}`?"))
                    .unwrap_or_else(|| format!("registered kinds: {}", registered.join(", ")));
                self.ctx.error(
                    codes::CONFIG_UNKNOWN_KIND,
                    &path,
                    Anchor::Value,
                    format!("no connector is registered for kind `{kind}`"),
                    Some(hint),
                );
            }
            if !spec.config().is_object() && !spec.config().is_null() {
                self.ctx.invalid(
                    &Vec::new().key(section).idx(i).key("config"),
                    Anchor::Value,
                    "`config` must be a mapping",
                );
            }
        }
    }

    fn sink_policies(&mut self) {
        for (i, sink) in self.parsed.sinks.present() {
            let lag_path = Vec::new().key("sinks").idx(i).key("max_lag");
            let Some(lag) = sink.max_lag else { continue };
            let (key, zero) = match lag {
                MaxLagSpec::Bytes(b) => ("bytes", b.get() == 0),
                MaxLagSpec::Age(a) => ("age", a.get().is_zero()),
            };
            if zero {
                self.ctx.invalid_hint(
                    &lag_path.key(key),
                    Anchor::Value,
                    format!("`max_lag.{key}` must be greater than zero"),
                    "omit `max_lag` to set no limit",
                );
            }
            if !sink.required {
                self.ctx.invalid_hint(
                    &lag_path,
                    Anchor::Key,
                    "`max_lag` has no effect on a sink with `required: false`: it never holds WAL data back, so there is nothing to limit",
                    "remove `max_lag` or set `required: true`",
                );
            }
        }
    }

    fn source_kind(&self, id: &str) -> Option<&str> {
        let idx = self
            .parsed
            .sources
            .ids
            .iter()
            .position(|s| s.as_deref() == Some(id))?;
        self.parsed
            .sources
            .items
            .as_ref()?
            .get(idx)?
            .as_ref()
            .map(|s| s.kind.as_str())
    }

    fn datasets(&mut self) {
        let Some(list) = &self.parsed.datasets.items else {
            return;
        };
        if list.is_empty() {
            self.ctx.invalid_hint(
                &Vec::new().key("datasets"),
                Anchor::Key,
                "`datasets` needs at least one entry",
                "a dataset is an output table: it names a source, a decoder, and its fields",
            );
        }
        for (i, ds) in self.parsed.datasets.present() {
            self.dataset(i, ds);
        }
    }

    fn dataset(&mut self, i: usize, ds: &DatasetSpec) {
        let base = Vec::new().key("datasets").idx(i);

        let src = &ds.match_.source;
        let mut levels = None;
        if !self.source_ids.contains(src) {
            let hint = closest(src, self.source_ids.iter().map(String::as_str))
                .map(|c| format!("did you mean `{c}`?"));
            self.ctx.error(
                codes::CONFIG_INVALID,
                &base.key("match").key("source"),
                Anchor::Value,
                format!("no source with id `{src}`"),
                hint,
            );
        } else if let Some(topic) = &ds.match_.topic
            && self.source_kind(src) == Some("mqtt")
        {
            if let Some(problem) = mqtt_filter_problem(topic) {
                self.ctx
                    .invalid(&base.key("match").key("topic"), Anchor::Value, problem);
            } else {
                levels = topic_levels(topic);
            }
        }

        if ds.identity.len() > MAX_ASSET_LEVELS {
            self.ctx.invalid(
                &base.key("identity"),
                Anchor::Key,
                format!("an asset path has at most {MAX_ASSET_LEVELS} levels"),
            );
        }
        let mut level_ids: HashMap<u32, &str> = HashMap::new();
        for level in &ds.identity {
            let p = base.key("identity").key(&level.name);
            if !name_ok(&level.name) {
                self.ctx.invalid_hint(
                    &p,
                    Anchor::Key,
                    format!("`{}` is not a valid level name", level.name),
                    "level names start with a letter and use letters, digits, and `_`; a leading `_` is reserved",
                );
            }
            if let Err(msg) = check_template(&level.template, levels) {
                self.ctx.invalid(&p.key("template"), Anchor::Value, msg);
            }
            self.field_id(
                &p.key("id"),
                IdKind::Level,
                &level.name,
                level.id,
                &mut level_ids,
            );
        }

        self.dataset_fields(&base, ds);
        self.dataset_options(&base, ds);
    }

    fn dataset_fields(&mut self, base: &PathVec, ds: &DatasetSpec) {
        let fields_path = base.key("fields");
        if ds.fields.is_empty() {
            self.ctx.invalid_hint(&fields_path, Anchor::Key, "a dataset needs at least one field", "declare the columns to write, for example `{ name: temp, type: f32, from: /temp }`");
        }
        if ds.fields.len() > MAX_SHAPE_FIELDS {
            self.ctx.invalid(
                &fields_path,
                Anchor::Key,
                format!("a dataset has at most {MAX_SHAPE_FIELDS} fields"),
            );
        }
        let identity: HashSet<&str> = ds.identity.iter().map(|l| l.name.as_str()).collect();
        let index_field = ds.explode.as_ref().and_then(|e| e.index_field.as_deref());
        let exploded = ds.explode.is_some();
        let mut seen: HashSet<&str> = HashSet::new();
        let mut ids: HashMap<u32, &str> = HashMap::new();
        for (j, f) in ds.fields.iter().enumerate() {
            let p = fields_path.idx(j);
            self.field(&p, f, exploded);
            self.field_id(&p.key("id"), IdKind::Field, &f.name, f.id, &mut ids);
            if !seen.insert(f.name.as_str()) {
                self.ctx.invalid(
                    &p.key("name"),
                    Anchor::Value,
                    format!("duplicate field name `{}`", f.name),
                );
            }
            if identity.contains(f.name.as_str()) {
                self.ctx.invalid(
                    &p.key("name"),
                    Anchor::Value,
                    format!("`{}` is already an identity level", f.name),
                );
            }
            if index_field == Some(f.name.as_str()) {
                self.ctx.invalid(
                    &p.key("name"),
                    Anchor::Value,
                    format!("`{}` is already the `explode.index_field`", f.name),
                );
            }
        }
    }

    /// Checks one Parquet field ID against the range for its kind and against
    /// the IDs already used in the dataset (`seen` maps an ID to its name).
    fn field_id<'a>(
        &mut self,
        id_path: &PathVec,
        kind: IdKind,
        name: &'a str,
        id: u32,
        seen: &mut HashMap<u32, &'a str>,
    ) {
        let range = kind.range();
        if !range.contains(&id) {
            self.ctx.invalid_hint(
                id_path,
                Anchor::Value,
                format!(
                    "{} id {id} of `{name}` is outside {}..={}",
                    kind.noun(),
                    range.start(),
                    range.end()
                ),
                "declared fields use 1000 to 4194303 and identity levels use 100 to 199; 1 to 99 are Thadam's envelope columns and higher ids are derived for nested fields",
            );
            return;
        }
        if let Some(first) = seen.insert(id, name) {
            self.ctx.invalid_hint(
                id_path,
                Anchor::Value,
                format!(
                    "duplicate {} id {id}: `{name}` reuses the id of `{first}`",
                    kind.noun()
                ),
                "ids are unique within a dataset, and an id is never reused for a different column, even after the column is removed",
            );
        }
    }

    fn field(&mut self, p: &PathVec, f: &FieldSpec, exploded: bool) {
        if !name_ok(&f.name) {
            self.ctx.invalid_hint(
                &p.key("name"),
                Anchor::Value,
                format!("`{}` is not a valid field name", f.name),
                "names start with a letter and use letters, digits, and `_`; a leading `_` is reserved for Thadam columns",
            );
        }
        if let Err(msg) = check_pointer(&f.from, exploded) {
            self.ctx.invalid(&p.key("from"), Anchor::Value, msg);
        }
        if f.convert_to.is_some() && f.unit.is_none() {
            self.ctx.invalid_hint(
                &p.key("convert_to"),
                Anchor::Key,
                "`convert_to` needs a source `unit`",
                "add `unit:` with the unit of the incoming value, for example `unit: mV`",
            );
        }
        for (key, value) in [("unit", &f.unit), ("convert_to", &f.convert_to)] {
            if value.as_deref().is_some_and(|u| u.trim().is_empty()) {
                self.ctx.invalid(
                    &p.key(key),
                    Anchor::Value,
                    format!("`{key}` must not be empty"),
                );
            }
        }
        if f.convert_to.is_some() && !is_numeric_or_numeric_list(f.ty.field_type()) {
            self.ctx.invalid(
                &p.key("convert_to"),
                Anchor::Key,
                format!(
                    "`convert_to` needs a numeric type, not `{}`",
                    f.ty.canonical()
                ),
            );
        }
    }

    fn dataset_options(&mut self, base: &PathVec, ds: &DatasetSpec) {
        let exploded = ds.explode.is_some();
        if let Some(t) = &ds.time {
            match (&t.field, &t.attr) {
                (Some(field), None) => {
                    if let Err(msg) = check_pointer(field, exploded) {
                        self.ctx
                            .invalid(&base.key("time").key("field"), Anchor::Value, msg);
                    }
                }
                (None, Some(attr)) => {
                    if attr.is_empty() {
                        self.ctx.invalid(
                            &base.key("time").key("attr"),
                            Anchor::Value,
                            "`attr` must name a message attribute",
                        );
                    }
                }
                _ => self.ctx.invalid_hint(
                    &base.key("time"),
                    Anchor::Key,
                    "`time` needs exactly one of `field` or `attr`",
                    "use `field: /pointer` to read the payload, or `attr: mcap.publish_time` to read a message attribute",
                ),
            }
            if t.max_skew.is_some_and(|d| d.get().is_zero()) {
                self.ctx.invalid(
                    &base.key("time").key("max_skew"),
                    Anchor::Value,
                    "`max_skew` must be greater than zero",
                );
            }
            if let Some(offset) = &t.default_offset {
                let p = base.key("time").key("default_offset");
                if t.unit != TimeUnit::Rfc3339 {
                    self.ctx.invalid_hint(
                        &p,
                        Anchor::Key,
                        "`default_offset` only applies to `unit: rfc3339`",
                        format!(
                            "`unit: {}` reads a number, which carries no offset: remove `default_offset`, or set `unit: rfc3339` to read text",
                            t.unit.as_str()
                        ),
                    );
                } else if let Err(msg) = check_utc_offset(offset) {
                    self.ctx.invalid_hint(
                        &p,
                        Anchor::Value,
                        msg,
                        "write an offset such as `+02:00`, `-0530`, `+01`, or `Z`",
                    );
                }
            }
        }
        if let Some(e) = &ds.explode {
            if let Err(msg) = check_pointer(&e.path, false) {
                self.ctx
                    .invalid(&base.key("explode").key("path"), Anchor::Value, msg);
            }
            if let Some(name) = &e.index_field
                && !name_ok(name)
            {
                self.ctx.invalid(
                    &base.key("explode").key("index_field"),
                    Anchor::Value,
                    format!("`{name}` is not a valid field name"),
                );
            }
        }
        if let Some(id) = &ds.event_id {
            let known: HashSet<&str> = ds
                .identity
                .iter()
                .map(|l| l.name.as_str())
                .chain(ds.fields.iter().map(|f| f.name.as_str()))
                .collect();
            if id.key.is_empty() {
                self.ctx.invalid(
                    &base.key("event_id").key("key"),
                    Anchor::Value,
                    "`key` needs at least one name",
                );
            }
            let mut seen = HashSet::new();
            for (k, name) in id.key.iter().enumerate() {
                let p = base.key("event_id").key("key").idx(k);
                if !known.contains(name.as_str()) {
                    let hint = closest(name, known.iter().copied())
                        .map(|c| format!("did you mean `{c}`?"));
                    self.ctx.error(
                        codes::CONFIG_INVALID,
                        &p,
                        Anchor::Key,
                        format!(
                            "`{name}` is neither an identity level nor a declared field of `{}`",
                            ds.id
                        ),
                        hint,
                    );
                } else if !seen.insert(name.as_str()) {
                    self.ctx
                        .invalid(&p, Anchor::Key, format!("`{name}` is listed twice"));
                }
            }
        }
        if let Some(d) = &ds.dedup {
            if d.window.get().is_zero() {
                self.ctx.invalid(
                    &base.key("dedup").key("window"),
                    Anchor::Value,
                    "`window` must be greater than zero",
                );
            }
            if d.max_keys.get() == 0 {
                self.ctx.invalid(
                    &base.key("dedup").key("max_keys"),
                    Anchor::Value,
                    "`max_keys` must be greater than zero",
                );
            }
            if ds.event_id.is_none() {
                self.ctx.error(
                    codes::CONFIG_INVALID,
                    &base.key("dedup"),
                    Anchor::Key,
                    "`dedup` requires `event_id.key`: without a content-derived event ID, the default ID changes when a message is redelivered (ADR-0011 D17)",
                    Some("add `event_id: { key: [...] }` with the identity levels and fields that identify a message".into()),
                );
            }
        }
    }

    fn dataset_named(&self, id: &str) -> Option<&'p DatasetSpec> {
        let parsed: &'p Parsed = self.parsed;
        let idx = parsed
            .datasets
            .ids
            .iter()
            .position(|d| d.as_deref() == Some(id))?;
        parsed.datasets.items.as_ref()?.get(idx)?.as_ref()
    }

    fn transforms(&mut self) {
        let Some(list) = &self.parsed.transforms.items else {
            return;
        };
        let mut views: Views = HashMap::new();
        for (i, t) in list.iter().enumerate() {
            let Some(t) = t else { continue };
            let base = Vec::new().key("transforms").idx(i);
            self.transform(&base, t, &mut views);
        }
    }

    fn transform(&mut self, base: &PathVec, t: &TransformSpec, views: &mut Views) {
        let dataset = t.dataset();
        let known = self.dataset_ids.contains(dataset);
        if !known {
            let hint = closest(dataset, self.dataset_ids.iter().map(String::as_str))
                .map(|c| format!("did you mean `{c}`?"));
            self.ctx.error(
                codes::CONFIG_INVALID,
                &base.key("dataset"),
                Anchor::Value,
                format!("no dataset with id `{dataset}`"),
                hint,
            );
        }
        if let Some(sinks) = t.sinks() {
            for (k, s) in sinks.iter().enumerate() {
                if !self.sink_ids.contains(s) {
                    let hint = closest(s, self.sink_ids.iter().map(String::as_str))
                        .map(|c| format!("did you mean `{c}`?"));
                    self.ctx.error(
                        codes::CONFIG_INVALID,
                        &base.key("sinks").idx(k),
                        Anchor::Value,
                        format!("no sink with id `{s}`"),
                        hint,
                    );
                }
            }
            if sinks.is_empty() {
                self.ctx.invalid(
                    &base.key("sinks"),
                    Anchor::Value,
                    "`sinks` must list at least one sink; omit it to apply to every sink",
                );
            }
        }
        let Some(ds) = self.dataset_named(dataset) else {
            return;
        };

        // Each sink has its own chain, so the fields a step sees depend on
        // which earlier steps apply to that sink. Check the step once per
        // distinct set of fields among the sinks it applies to.
        let mut all: Vec<String> = self.sink_ids.iter().cloned().collect();
        all.sort();
        if all.is_empty() {
            all.push(String::new());
        }
        let view = views.entry(dataset.to_owned()).or_insert_with(|| {
            all.iter()
                .map(|s| (s.clone(), declared_fields(ds)))
                .collect()
        });
        let mut applies: Vec<&String> = match t.sinks() {
            Some(list) => all.iter().filter(|s| list.contains(s)).collect(),
            None => all.iter().collect(),
        };
        if applies.is_empty() {
            applies = all.iter().collect();
        }
        let mut groups: Vec<(FieldMap, Vec<&String>)> = Vec::new();
        for sink in applies {
            let fields = view.get(sink).cloned().unwrap_or_default();
            match groups.iter_mut().find(|(f, _)| *f == fields) {
                Some((_, sinks)) => sinks.push(sink),
                None => groups.push((fields, vec![sink])),
            }
        }
        for (fields, sinks) in groups {
            let names = Names::with_fields(ds, fields);
            self.step(base, t, &names);
            let next = evolve(t, &names);
            for sink in sinks {
                view.insert(sink.clone(), next.clone());
            }
        }
    }

    fn step(&mut self, base: &PathVec, t: &TransformSpec, names: &Names) {
        match t {
            TransformSpec::Validate { rules, .. } => {
                if rules.is_empty() {
                    self.ctx.invalid(
                        &base.key("rules"),
                        Anchor::Value,
                        "`rules` needs at least one rule",
                    );
                }
                for (r, rule) in rules.iter().enumerate() {
                    self.rule(&base.key("rules").idx(r), rule, names);
                }
            }
            TransformSpec::OnChange {
                fields, heartbeat, ..
            } => {
                self.on_change_fields(base, fields, names);
                if heartbeat.is_some_and(|d| d.get().is_zero()) {
                    self.ctx.invalid(
                        &base.key("heartbeat"),
                        Anchor::Value,
                        "`heartbeat` must be greater than zero",
                    );
                }
            }
            TransformSpec::Filter { where_, .. } => {
                self.predicate(&base.key("where"), where_, names);
            }
            TransformSpec::Sample {
                every_n,
                min_interval,
                ..
            } => {
                if every_n.is_none() && min_interval.is_none() {
                    self.ctx.invalid_hint(
                        base,
                        Anchor::Key,
                        "`sample` needs `every_n`, `min_interval`, or both",
                        "for example `every_n: 10` keeps every 10th event, `min_interval: 1s` at most one a second",
                    );
                }
                if every_n.is_some_and(|n| n.get() == 0) {
                    self.ctx.invalid(
                        &base.key("every_n"),
                        Anchor::Value,
                        "`every_n` must be at least 1",
                    );
                }
                if min_interval.is_some_and(|d| d.get().is_zero()) {
                    self.ctx.invalid(
                        &base.key("min_interval"),
                        Anchor::Value,
                        "`min_interval` must be greater than zero",
                    );
                }
            }
            TransformSpec::Enrich {
                file,
                format,
                key,
                add,
                ..
            } => self.enrich(base, file, *format, key, add, names),
            TransformSpec::Rename { fields, .. } => self.rename(base, fields, names),
            TransformSpec::Project { fields, .. } => self.project(base, fields, names),
            TransformSpec::Cast { fields, .. } => self.cast(base, fields, names),
            TransformSpec::Pose {
                prefix,
                frame_field,
                quaternion_tolerance,
                ..
            } => {
                // The flattened fields a pose step reads (`<prefix>.position.x`,
                // `<prefix>.orientation.w`, the frame field) come from the ROS 2
                // decoder's dotted shape and so cannot be declared fields today;
                // like an `enrich` table, they are resolved when the transform
                // chain compiles, not here.
                if prefix.is_empty() {
                    self.ctx.invalid(
                        &base.key("prefix"),
                        Anchor::Value,
                        "`prefix` must not be empty",
                    );
                }
                if !(*quaternion_tolerance > 0.0 && *quaternion_tolerance < 1.0) {
                    self.ctx.invalid(
                        &base.key("quaternion_tolerance"),
                        Anchor::Value,
                        "`quaternion_tolerance` must be greater than 0 and less than 1",
                    );
                }
                if let Some(Some(field)) = frame_field
                    && field.is_empty()
                {
                    self.ctx.invalid(
                        &base.key("frame_field"),
                        Anchor::Value,
                        "`frame_field` must not be empty",
                    );
                }
            }
            TransformSpec::JointState {
                name_field,
                position_field,
                velocity_field,
                effort_field,
                ..
            } => {
                for (key, name, floats) in [
                    ("name_field", name_field, false),
                    ("position_field", position_field, true),
                    ("velocity_field", velocity_field, true),
                    ("effort_field", effort_field, true),
                ] {
                    self.joint_state_field(base, key, name, floats, names);
                }
            }
        }
    }

    /// One `joint_state` array field: non-empty, a declared field, and a list
    /// of the right element type — the engine's checks, mirrored.
    fn joint_state_field(
        &mut self,
        base: &PathVec,
        key: &str,
        name: &str,
        floats: bool,
        names: &Names,
    ) {
        let p = base.key(key);
        if name.is_empty() {
            self.ctx
                .invalid(&p, Anchor::Value, format!("`{key}` must not be empty"));
            return;
        }
        let Some(ty) = names.fields.get(name) else {
            self.unknown_step_field(&p, Anchor::Value, name, names);
            return;
        };
        let ok = match ty {
            FieldType::List(inner) if floats => matches!(**inner, FieldType::F32 | FieldType::F64),
            FieldType::List(inner) => matches!(**inner, FieldType::Str),
            _ => false,
        };
        if !ok {
            let want = if floats {
                "a list<f32> or list<f64>"
            } else {
                "a list<str>"
            };
            self.ctx.invalid(
                &p,
                Anchor::Value,
                format!(
                    "`{key}` must name {want} field, but `{name}` is `{}`",
                    type_label(ty)
                ),
            );
        }
    }

    /// Reports `name` as not a field a step can use, with a typo suggestion.
    fn unknown_step_field(&mut self, path: &PathVec, anchor: Anchor, name: &str, names: &Names) {
        let hint = if names.identity.contains(name) {
            Some(
                "identity levels are part of the asset path, not record fields, so this step cannot use them"
                    .to_owned(),
            )
        } else {
            closest(name, names.fields.keys().map(String::as_str))
                .map(|c| format!("did you mean `{c}`?"))
        };
        self.ctx.error(
            codes::CONFIG_INVALID,
            path,
            anchor,
            format!(
                "`{name}` is not a declared field of dataset `{}`",
                names.dataset
            ),
            hint,
        );
    }

    fn on_change_fields(&mut self, base: &PathVec, fields: &[WatchField], names: &Names) {
        if fields.is_empty() {
            self.ctx.invalid(
                &base.key("fields"),
                Anchor::Value,
                "`fields` needs at least one field",
            );
        }
        let mut seen = HashSet::new();
        for (k, w) in fields.iter().enumerate() {
            let p = base.key("fields").idx(k);
            let Some(ty) = names.fields.get(&w.field) else {
                self.unknown_step_field(&p, Anchor::Key, &w.field, names);
                continue;
            };
            if !seen.insert(w.field.as_str()) {
                self.ctx.invalid(
                    &p,
                    Anchor::Key,
                    format!("field `{}` is listed twice", w.field),
                );
                continue;
            }
            if !w.deadband.is_finite() || w.deadband < 0.0 {
                self.ctx.invalid(
                    &p.key("deadband"),
                    Anchor::Value,
                    "`deadband` must be a finite number, zero or more",
                );
            } else if w.deadband > 0.0 && !is_numeric_or_numeric_list(ty) {
                self.ctx.invalid_hint(
                    &p.key("deadband"),
                    Anchor::Value,
                    format!(
                        "`deadband` needs a numeric field or a list of numbers, but `{}` is `{}`",
                        w.field,
                        type_label(ty)
                    ),
                    "remove `deadband`, or watch a numeric field",
                );
            }
        }
    }

    fn enrich(
        &mut self,
        base: &PathVec,
        file: &str,
        format: Option<EnrichFormat>,
        key: &EnrichKeySpec,
        add: &[EnrichFieldSpec],
        names: &Names,
    ) {
        let extension = Path::new(file)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or_default();
        if file.trim().is_empty() {
            self.ctx
                .invalid(&base.key("file"), Anchor::Value, "`file` must not be empty");
        } else if format.is_none() && !matches!(extension, "csv" | "yaml" | "yml") {
            self.ctx.invalid_hint(
                &base.key("file"),
                Anchor::Value,
                format!("cannot tell the table format from `{file}`"),
                "name the file `*.csv`, `*.yaml` or `*.yml`, or set `format: csv` or `format: yaml`",
            );
        }
        let csv = match format {
            Some(f) => f == EnrichFormat::Csv,
            None => extension == "csv",
        };

        let key_path = base.key("key");
        match names.fields.get(&key.field) {
            Some(FieldType::Str) => {}
            Some(ty) if int_range(ty).is_some() => {}
            Some(ty) => self.ctx.invalid(
                &key_path.key("field"),
                Anchor::Value,
                format!(
                    "an enrichment key must be a string or integer field, but `{}` is `{}`",
                    key.field,
                    type_label(ty)
                ),
            ),
            None if names.identity.contains(&key.field) => {}
            None => {
                let candidates = names
                    .fields
                    .keys()
                    .map(String::as_str)
                    .chain(names.identity.iter().map(String::as_str));
                let hint = closest(&key.field, candidates)
                    .map(|c| format!("did you mean `{c}`?"))
                    .unwrap_or_else(|| {
                        "the key is a string or integer field of the dataset, or an identity level"
                            .to_owned()
                    });
                self.ctx.error(
                    codes::CONFIG_INVALID,
                    &key_path.key("field"),
                    Anchor::Value,
                    format!(
                        "`{}` is neither a field nor an identity level of dataset `{}`",
                        key.field, names.dataset
                    ),
                    Some(hint),
                );
            }
        }
        if key.column.trim().is_empty() {
            self.ctx.invalid(
                &key_path.key("column"),
                Anchor::Value,
                "`column` must not be empty",
            );
        }

        if add.is_empty() {
            self.ctx.invalid_hint(
                &base.key("add"),
                Anchor::Value,
                "`add` needs at least one field",
                "for example `add: [{ name: model, type: str }]`",
            );
        }
        let mut seen = HashSet::new();
        for (i, a) in add.iter().enumerate() {
            let p = base.key("add").idx(i);
            if !name_ok(&a.name) {
                self.ctx.invalid_hint(
                    &p.key("name"),
                    Anchor::Value,
                    format!("`{}` is not a valid field name", a.name),
                    "names start with a letter and use letters, digits, and `_`; a leading `_` is reserved for Thadam columns",
                );
            } else if let Some(what) = names.taken(&a.name) {
                self.ctx.invalid_hint(
                    &p.key("name"),
                    Anchor::Value,
                    format!(
                        "`{}` is already {what} of dataset `{}`",
                        a.name, names.dataset
                    ),
                    "an added field needs a new name; use `rename` first to free the old one",
                );
            } else if !seen.insert(a.name.as_str()) {
                self.ctx.invalid(
                    &p.key("name"),
                    Anchor::Value,
                    format!("`{}` is added twice", a.name),
                );
            }
            for (k, value) in [("column", &a.column), ("unit", &a.unit)] {
                if value.as_deref().is_some_and(|v| v.trim().is_empty()) {
                    self.ctx
                        .invalid(&p.key(k), Anchor::Value, format!("`{k}` must not be empty"));
                }
            }
            self.enrich_type(&p, a, csv);
        }
    }

    fn enrich_type(&mut self, p: &PathVec, a: &EnrichFieldSpec, csv: bool) {
        let ty = a.ty.field_type();
        let label = type_label(ty);
        match ty {
            FieldType::Bytes | FieldType::List(_) if list_unsupported(ty) => self.ctx.invalid(
                &p.key("type"),
                Anchor::Value,
                format!("`enrich` cannot add a field of type `{label}`"),
            ),
            FieldType::List(_) if csv => self.ctx.invalid_hint(
                &p.key("type"),
                Anchor::Value,
                format!("a CSV column cannot hold a list, but `{}` is `{label}`", a.name),
                "use a YAML table, or add a scalar field",
            ),
            FieldType::Ts => match a.unit.as_deref() {
                None => self.ctx.invalid_hint(
                    p,
                    Anchor::Key,
                    "a `ts` field needs `unit`",
                    "write `unit: s`, `ms`, `us`, or `ns` for epoch numbers, or `unit: rfc3339` for text",
                ),
                Some(u) if !u.trim().is_empty() && !is_time_unit(u) => self.ctx.invalid(
                    &p.key("unit"),
                    Anchor::Value,
                    format!("unknown time unit `{u}`; use s, ms, us, ns, or rfc3339"),
                ),
                Some(_) => {}
            },
            _ => {}
        }
    }

    fn rename(&mut self, base: &PathVec, fields: &BTreeMap<String, String>, names: &Names) {
        let fields_path = base.key("fields");
        if fields.is_empty() {
            self.ctx.invalid_hint(
                &fields_path,
                Anchor::Value,
                "`fields` needs at least one entry",
                "for example `fields: { old_name: new_name }`",
            );
        }
        // Renames apply together, so a name freed by one entry may be taken by
        // another (a swap is fine); only the names that stay are in the way.
        let mut taken: HashSet<&str> = names
            .fields
            .keys()
            .map(String::as_str)
            .filter(|f| fields.get(*f).is_none_or(|new| new == f))
            .collect();
        for (old, new) in fields {
            let p = fields_path.key(old);
            if !names.fields.contains_key(old) {
                self.unknown_step_field(&p, Anchor::Key, old, names);
                continue;
            }
            if !name_ok(new) {
                self.ctx.invalid_hint(
                    &p,
                    Anchor::Value,
                    format!("`{new}` is not a valid field name"),
                    "names start with a letter and use letters, digits, and `_`; a leading `_` is reserved for Thadam columns",
                );
            } else if new == old {
                // Renaming a field to its own name is a no-op.
            } else if names.identity.contains(new) || names.index_field.as_deref() == Some(new) {
                let what = if names.identity.contains(new) {
                    "an identity level"
                } else {
                    "the `explode.index_field`"
                };
                self.ctx.invalid(
                    &p,
                    Anchor::Value,
                    format!("`{new}` is already {what} of dataset `{}`", names.dataset),
                );
            } else if !taken.insert(new.as_str()) {
                self.ctx.invalid_hint(
                    &p,
                    Anchor::Value,
                    format!("`{new}` is already a field of dataset `{}`, or the new name of another renamed field", names.dataset),
                    "choose a name no other field has or is renamed to",
                );
            }
        }
    }

    fn project(&mut self, base: &PathVec, fields: &[String], names: &Names) {
        if fields.is_empty() {
            self.ctx.invalid_hint(
                &base.key("fields"),
                Anchor::Value,
                "`fields` needs at least one field",
                "list the fields to keep",
            );
        }
        let mut seen = HashSet::new();
        for (k, f) in fields.iter().enumerate() {
            let p = base.key("fields").idx(k);
            if !names.fields.contains_key(f) {
                self.unknown_step_field(&p, Anchor::Key, f, names);
            } else if !seen.insert(f.as_str()) {
                self.ctx
                    .invalid(&p, Anchor::Key, format!("field `{f}` is listed twice"));
            }
        }
    }

    fn cast(&mut self, base: &PathVec, fields: &BTreeMap<String, FieldTypeSpec>, names: &Names) {
        let fields_path = base.key("fields");
        if fields.is_empty() {
            self.ctx.invalid_hint(
                &fields_path,
                Anchor::Value,
                "`fields` needs at least one entry",
                "for example `fields: { temp: f64 }`",
            );
        }
        for (name, target) in fields {
            let p = fields_path.key(name);
            let Some(from) = names.fields.get(name) else {
                self.unknown_step_field(&p, Anchor::Key, name, names);
                continue;
            };
            if !widens(from, target.field_type()) {
                self.ctx.invalid_hint(
                    &p,
                    Anchor::Value,
                    format!(
                        "cannot cast `{name}` from `{}` to `{}`: casts only widen",
                        type_label(from),
                        target.canonical()
                    ),
                    "widening casts: i8 < i16 < i32 < i64, u8 < u16 < u32 < u64, an unsigned integer to a larger signed one, f32 to f64, list<f32> to list<f64>, list<i32> to list<i64>",
                );
            }
        }
    }

    fn unknown_name(&mut self, path: &PathVec, anchor: Anchor, name: &str, names: &Names) {
        let hint = closest(name, names.all()).map(|c| format!("did you mean `{c}`?")).unwrap_or_else(|| {
            "names are the dataset's fields, its identity levels, the `explode` index field, and `quality.status`, `quality.flags`, `quality.reason`".to_owned()
        });
        self.ctx.error(
            codes::CONFIG_INVALID,
            path,
            anchor,
            format!("`{name}` is not a field of dataset `{}`", names.dataset),
            Some(hint),
        );
    }

    fn rule(&mut self, p: &PathVec, rule: &crate::spec::ValidateRule, names: &Names) {
        match names.resolve(&rule.field) {
            None => self.unknown_name(&p.key("field"), Anchor::Value, &rule.field, names),
            Some(Target::Typed(ty)) if names.fields.contains_key(&rule.field) => {
                let element = match &ty {
                    FieldType::List(inner) => inner.as_ref(),
                    other => other,
                };
                if !is_numeric(element) {
                    self.ctx.invalid(
                        &p.key("field"),
                        Anchor::Value,
                        format!("`min` and `max` need a numeric field or a list of numbers, but `{}` is `{}`", rule.field, type_label(&ty)),
                    );
                }
            }
            Some(_) => self.ctx.invalid(
                &p.key("field"),
                Anchor::Value,
                "range rules apply to declared numeric fields",
            ),
        }
        match (rule.min, rule.max) {
            (None, None) => self
                .ctx
                .invalid(p, Anchor::Key, "a rule needs `min`, `max`, or both"),
            (Some(a), Some(b)) if a > b => self.ctx.invalid(
                &p.key("min"),
                Anchor::Value,
                format!("`min` ({a}) is greater than `max` ({b})"),
            ),
            _ => {}
        }
        if QualityFlags::from_name(&rule.flag).is_none() {
            let names: Vec<&str> = QualityFlags::all().iter_names().map(|(n, _)| n).collect();
            let hint = closest(&rule.flag, names.iter().copied())
                .map(|c| format!("did you mean `{c}`?"))
                .unwrap_or_else(|| format!("flags: {}", names.join(", ")));
            self.ctx.error(
                codes::CONFIG_INVALID,
                &p.key("flag"),
                Anchor::Value,
                format!("unknown quality flag `{}`", rule.flag),
                Some(hint),
            );
        }
    }

    fn predicate(&mut self, p: &PathVec, pred: &Predicate, names: &Names) {
        match pred {
            Predicate::All(items) => {
                for (i, item) in items.iter().enumerate() {
                    self.predicate(&p.key("all").idx(i), item, names);
                }
            }
            Predicate::Any(items) => {
                for (i, item) in items.iter().enumerate() {
                    self.predicate(&p.key("any").idx(i), item, names);
                }
            }
            Predicate::Not(inner) => self.predicate(&p.key("not"), inner, names),
            Predicate::Condition(c) => self.condition(p, c, names),
        }
    }

    fn condition(&mut self, p: &PathVec, c: &Condition, names: &Names) {
        let Some(target) = names.resolve(&c.field) else {
            self.unknown_name(&p.key("field"), Anchor::Value, &c.field, names);
            return;
        };
        if matches!(c.test, Test::IsNull(_)) {
            return;
        }
        let (op, literals): (&str, Vec<&Literal>) = match &c.test {
            Test::Eq(l) => ("eq", vec![l]),
            Test::Ne(l) => ("ne", vec![l]),
            Test::Lt(l) => ("lt", vec![l]),
            Test::Le(l) => ("le", vec![l]),
            Test::Gt(l) => ("gt", vec![l]),
            Test::Ge(l) => ("ge", vec![l]),
            Test::In(ls) => ("in", ls.iter().collect()),
            Test::Between { min, max } => ("min", min.iter().chain(max.iter()).collect()),
            Test::IsNull(_) => ("null", Vec::new()),
        };
        let ordered = !matches!(
            c.test,
            Test::Eq(_) | Test::Ne(_) | Test::In(_) | Test::IsNull(_)
        );
        let op_path = |c: &Condition, idx: usize| -> PathVec {
            match &c.test {
                Test::Between { min, .. } => p.key(if idx == 0 && min.is_some() {
                    "min"
                } else {
                    "max"
                }),
                Test::In(_) => p.key("in").idx(idx),
                _ => p.key(op),
            }
        };
        match target {
            Target::Status => {
                if ordered {
                    self.ctx.invalid(
                        &p.key(op),
                        Anchor::Key,
                        "`quality.status` supports `eq`, `ne`, and `in`",
                    );
                    return;
                }
                for (i, l) in literals.iter().enumerate() {
                    let ok = matches!(l, Literal::Str(s) if [QualityStatus::Good, QualityStatus::Uncertain, QualityStatus::Bad].iter().any(|q| q.as_str() == s));
                    if !ok {
                        self.ctx.invalid_hint(
                            &op_path(c, i),
                            Anchor::Value,
                            "`quality.status` is `good`, `uncertain`, or `bad`",
                            "write the status as a string",
                        );
                    }
                }
            }
            Target::Flags => {
                if ordered {
                    self.ctx.invalid(
                        &p.key(op),
                        Anchor::Key,
                        "`quality.flags` supports `eq`, `ne`, and `in`",
                    );
                    return;
                }
                for (i, l) in literals.iter().enumerate() {
                    let ok = matches!(l, Literal::Str(s) if QualityFlags::from_name(s).is_some());
                    if !ok {
                        let names: Vec<&str> =
                            QualityFlags::all().iter_names().map(|(n, _)| n).collect();
                        self.ctx.invalid_hint(
                            &op_path(c, i),
                            Anchor::Value,
                            "`quality.flags` compares with a flag name",
                            format!("flags: {}", names.join(", ")),
                        );
                    }
                }
            }
            Target::Typed(ty) => {
                for (i, l) in literals.iter().enumerate() {
                    if let Some(msg) = literal_problem(&c.field, &ty, l, ordered) {
                        self.ctx.invalid(&op_path(c, i), Anchor::Value, msg);
                    }
                }
            }
        }
    }

    fn observability(&mut self) {
        let Some(o) = &self.parsed.observability else {
            return;
        };
        let base = Vec::new().key("observability").key("admin");
        match o.admin.bind.parse::<SocketAddr>() {
            Err(_) => self.ctx.invalid_hint(
                &base.key("bind"),
                Anchor::Value,
                format!("`{}` is not a socket address", o.admin.bind),
                "write `host:port` with an IP address, for example `127.0.0.1:9464`",
            ),
            Ok(addr) => {
                if !addr.ip().is_loopback() && o.admin.token_file.is_none() {
                    self.ctx.invalid_hint(
                        &base.key("token_file"),
                        Anchor::Key,
                        "a non-loopback admin address requires `token_file`",
                        "set `token_file` to a file holding the bearer token, or bind to 127.0.0.1",
                    );
                }
            }
        }
    }

    fn runtime(&mut self) {
        if let Some(r) = &self.parsed.runtime {
            if r.worker_threads.is_some_and(|n| n.get() == 0) {
                self.ctx.invalid(
                    &Vec::new().key("runtime").key("worker_threads"),
                    Anchor::Value,
                    "`worker_threads` must be at least 1",
                );
            }
            if r.memory_target.is_some_and(|n| n.get() == 0) {
                self.ctx.invalid(
                    &Vec::new().key("runtime").key("memory_target"),
                    Anchor::Value,
                    "`memory_target` must be greater than zero",
                );
            }
        }
    }
}

fn is_numeric_or_numeric_list(ty: &FieldType) -> bool {
    match ty {
        FieldType::List(inner) => is_numeric_or_numeric_list(inner),
        other => is_numeric(other),
    }
}

/// The fields each step sees, per dataset and per sink (the empty sink name
/// stands in when the configuration has none).
type Views = HashMap<String, BTreeMap<String, FieldMap>>;

/// The fields after `t`, as far as they can be told when the step has errors.
fn evolve(t: &TransformSpec, names: &Names) -> FieldMap {
    let mut out = names.fields.clone();
    match t {
        TransformSpec::Rename { fields, .. } => {
            out = names
                .fields
                .iter()
                .map(|(name, ty)| (fields.get(name).unwrap_or(name).clone(), ty.clone()))
                .collect();
        }
        TransformSpec::Project { fields, .. } => {
            out.retain(|name, _| fields.contains(name));
        }
        TransformSpec::Cast { fields, .. } => {
            for (name, target) in fields {
                if let Some(slot) = out.get_mut(name)
                    && widens(slot, target.field_type())
                {
                    *slot = target.field_type().clone();
                }
            }
        }
        TransformSpec::Enrich { add, .. } => {
            for a in add {
                if names.taken(&a.name).is_none() {
                    out.entry(a.name.clone())
                        .or_insert_with(|| a.ty.field_type().clone());
                }
            }
        }
        _ => {}
    }
    out
}

/// `thadam-core`'s runtime type for a declared type; every list the engine has
/// no packed form for is plain `List`.
fn value_type(ty: &FieldType) -> Option<ValueType> {
    Some(match ty {
        FieldType::Bool => ValueType::Bool,
        FieldType::I8 => ValueType::I8,
        FieldType::I16 => ValueType::I16,
        FieldType::I32 => ValueType::I32,
        FieldType::I64 => ValueType::I64,
        FieldType::U8 => ValueType::U8,
        FieldType::U16 => ValueType::U16,
        FieldType::U32 => ValueType::U32,
        FieldType::U64 => ValueType::U64,
        FieldType::F32 => ValueType::F32,
        FieldType::F64 => ValueType::F64,
        FieldType::Str => ValueType::Str,
        FieldType::Bytes => ValueType::Bytes,
        FieldType::Ts => ValueType::Ts,
        FieldType::List(inner) => match **inner {
            FieldType::F32 => ValueType::F32s,
            FieldType::F64 => ValueType::F64s,
            FieldType::I32 => ValueType::I32s,
            FieldType::I64 => ValueType::I64s,
            _ => ValueType::List,
        },
        _ => return None,
    })
}

/// Whether `cast` may turn `from` into `to`: the same type, or a widening in
/// `thadam-core`'s lattice. Lists with no packed form only cast to themselves.
fn widens(from: &FieldType, to: &FieldType) -> bool {
    if from == to {
        return true;
    }
    match (value_type(from), value_type(to)) {
        (Some(ValueType::List), _) | (_, Some(ValueType::List)) => false,
        (Some(a), Some(b)) => a.widens_to(b),
        _ => false,
    }
}

/// Types `enrich` has no column decoder for: `bytes`, and lists of `bytes`,
/// `ts`, or lists.
fn list_unsupported(ty: &FieldType) -> bool {
    match ty {
        FieldType::Bytes => true,
        FieldType::List(inner) => matches!(
            **inner,
            FieldType::Bytes | FieldType::Ts | FieldType::List(_)
        ),
        _ => false,
    }
}

fn is_time_unit(u: &str) -> bool {
    matches!(u, "s" | "ms" | "us" | "ns" | "rfc3339")
}

/// Checks a UTC offset the way `thadam-transform` parses `default_offset`:
/// `Z`, `z`, `UTC`, or a sign and two-digit hours with optional minutes, as
/// `+HH:MM`, `+HHMM`, or `+HH`; hours at most 23, minutes at most 59.
pub(crate) fn check_utc_offset(s: &str) -> Result<(), String> {
    const FORMS: &str = "write `+HH:MM`, `-HH:MM`, `+HHMM`, `+HH`, or `Z`";
    if s.len() > 64 {
        return Err("a UTC offset is at most 64 characters".to_owned());
    }
    if matches!(s, "Z" | "z" | "UTC") {
        return Ok(());
    }
    let bad = || format!("`{s}` is not a UTC offset: {FORMS}");
    let rest = s
        .strip_prefix('+')
        .or_else(|| s.strip_prefix('-'))
        .ok_or_else(bad)?
        .as_bytes();
    let (hh, mm) = match *rest {
        [h1, h2] => ([h1, h2], None),
        [h1, h2, b':', m1, m2] | [h1, h2, m1, m2] => ([h1, h2], Some([m1, m2])),
        _ => return Err(bad()),
    };
    let two_digits = |[a, b]: [u8; 2]| -> Option<u32> {
        (a.is_ascii_digit() && b.is_ascii_digit())
            .then(|| u32::from(a - b'0') * 10 + u32::from(b - b'0'))
    };
    let hours = two_digits(hh).ok_or_else(bad)?;
    let minutes = match mm {
        Some(pair) => two_digits(pair).ok_or_else(bad)?,
        None => 0,
    };
    if hours > 23 {
        return Err(format!(
            "the offset hours in `{s}` are above 23: offsets run from -23:59 to +23:59"
        ));
    }
    if minutes > 59 {
        return Err(format!("the offset minutes in `{s}` are above 59"));
    }
    Ok(())
}

/// Why `lit` cannot be compared with a field of type `ty`, if it cannot.
fn literal_problem(field: &str, ty: &FieldType, lit: &Literal, ordered: bool) -> Option<String> {
    match ty {
        FieldType::Str if ordered => Some(format!(
            "ordering comparisons are not supported on the string field `{field}`"
        )),
        FieldType::Str => match lit {
            Literal::Str(_) => None,
            other => Some(format!(
                "`{field}` is a string, but the value is {}",
                other.kind()
            )),
        },
        FieldType::Bool => match lit {
            Literal::Bool(_) => None,
            other => Some(format!(
                "`{field}` is a boolean, but the value is {}",
                other.kind()
            )),
        },
        t if is_numeric(t) => {
            let value: i128 = match lit {
                Literal::I64(i) => i128::from(*i),
                Literal::U64(u) => i128::from(*u),
                Literal::F64(_) if int_range(t).is_some() => {
                    return Some(format!(
                        "`{field}` is `{}`: compare it with an integer, not a float",
                        type_label(t)
                    ));
                }
                Literal::F64(_) => return None,
                other => {
                    return Some(format!(
                        "`{field}` is `{}`, but the value is {}",
                        type_label(t),
                        other.kind()
                    ));
                }
            };
            let (lo, hi) = int_range(t)?;
            (value < lo || value > hi).then(|| {
                format!(
                    "{value} is out of range for `{field}` (`{}`: {lo} to {hi})",
                    type_label(t)
                )
            })
        }
        other => Some(format!(
            "`{field}` is `{}`, which predicates cannot compare",
            type_label(other)
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_offsets_follow_the_engine_grammar() {
        for good in [
            "+02:00", "-05:30", "+0200", "-0530", "+02", "-23", "+23:59", "Z", "z", "UTC",
        ] {
            assert_eq!(check_utc_offset(good), Ok(()), "{good}");
        }
        for bad in [
            "",
            "+",
            "02:00",
            "+2",
            "+002",
            "+02:0",
            "+02:",
            "+0:00",
            "+02:00:00",
            "+24:00",
            "+02:60",
            " +02:00",
            "+02:00 ",
            "utc",
            "+0２:00",
            "+a€",
            "+€€",
            "é",
        ] {
            assert!(check_utc_offset(bad).is_err(), "{bad:?}");
        }
        assert!(check_utc_offset(&format!("+{}", "0".repeat(64))).is_err());
    }

    #[test]
    fn widening_follows_the_core_lattice() {
        use FieldType as T;
        let list = |t: T| T::List(Box::new(t));
        assert!(widens(&T::U8, &T::I16));
        assert!(widens(&T::F32, &T::F64));
        assert!(widens(&list(T::I32), &list(T::I64)));
        assert!(widens(&list(T::Str), &list(T::Str)));
        assert!(!widens(&T::I64, &T::U64));
        assert!(!widens(&T::U64, &T::I64));
        assert!(!widens(&list(T::Str), &list(T::Bool)));
        assert!(!widens(&T::I32, &T::F64));
    }
}
