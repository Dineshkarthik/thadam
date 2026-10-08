//! Cross-field and cross-section checks that serde cannot express: unique ids,
//! references between sections, identifier and pointer syntax, and predicate
//! names and literal types resolved against the declared schema.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::ops::RangeInclusive;

use thadam_core::codes;
use thadam_core::{FieldType, MAX_ASSET_LEVELS, MAX_SHAPE_FIELDS, QualityFlags, QualityStatus};

use crate::check::{Parsed, Section, closest};
use crate::ctx::Ctx;
use crate::path::{PathExt, PathVec};
use crate::predicate::{Condition, Literal, Predicate, Test};
use crate::spans::Anchor;
use crate::spec::{
    ConnectorSpec, DECLARED_FIELD_ID_RANGE, DatasetSpec, FieldSpec, IDENTITY_LEVEL_ID_RANGE,
    MaxLagSpec, SinkSpec, TransformSpec, WalSpec,
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

struct Names {
    dataset: String,
    fields: HashMap<String, FieldType>,
    identity: HashSet<String>,
    index_field: Option<String>,
}

impl Names {
    fn new(ds: &DatasetSpec) -> Self {
        Names {
            dataset: ds.id.clone(),
            fields: ds
                .fields
                .iter()
                .map(|f| (f.name.clone(), f.ty.field_type().clone()))
                .collect(),
            identity: ds.identity.iter().map(|l| l.name.clone()).collect(),
            index_field: ds.explode.as_ref().and_then(|e| e.index_field.clone()),
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

impl Checker<'_, '_, '_> {
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
            if let Err(msg) = check_pointer(&t.field, exploded) {
                self.ctx
                    .invalid(&base.key("time").key("field"), Anchor::Value, msg);
            }
            if t.max_skew.is_some_and(|d| d.get().is_zero()) {
                self.ctx.invalid(
                    &base.key("time").key("max_skew"),
                    Anchor::Value,
                    "`max_skew` must be greater than zero",
                );
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
                self.ctx.warn(
                codes::CONFIG_INVALID,
                    &base.key("dedup"),
                    Anchor::Key,
                    "`dedup` has little effect without `event_id.key`: the default event ID changes when a message is redelivered",
                    Some("add `event_id: { key: [...] }` with names that identify a message".into()),
                );
            }
        }
    }

    fn dataset_named(&self, id: &str) -> Option<&DatasetSpec> {
        let idx = self
            .parsed
            .datasets
            .ids
            .iter()
            .position(|d| d.as_deref() == Some(id))?;
        self.parsed.datasets.items.as_ref()?.get(idx)?.as_ref()
    }

    fn transforms(&mut self) {
        let Some(list) = &self.parsed.transforms.items else {
            return;
        };
        for (i, t) in list.iter().enumerate() {
            let Some(t) = t else { continue };
            let base = Vec::new().key("transforms").idx(i);
            self.transform(&base, t);
        }
    }

    fn transform(&mut self, base: &PathVec, t: &TransformSpec) {
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
        let names = Names::new(ds);
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
                    self.rule(&base.key("rules").idx(r), rule, &names);
                }
            }
            TransformSpec::OnChange {
                fields, heartbeat, ..
            } => {
                if fields.is_empty() {
                    self.ctx.invalid(
                        &base.key("fields"),
                        Anchor::Value,
                        "`fields` needs at least one field",
                    );
                }
                for (k, f) in fields.iter().enumerate() {
                    if !names.fields.contains_key(f) {
                        let hint = closest(f, names.fields.keys().map(String::as_str))
                            .map(|c| format!("did you mean `{c}`?"));
                        self.ctx.error(
                            codes::CONFIG_INVALID,
                            &base.key("fields").idx(k),
                            Anchor::Key,
                            format!(
                                "`{f}` is not a declared field of dataset `{}`",
                                names.dataset
                            ),
                            hint,
                        );
                    }
                }
                if heartbeat.is_some_and(|d| d.get().is_zero()) {
                    self.ctx.invalid(
                        &base.key("heartbeat"),
                        Anchor::Value,
                        "`heartbeat` must be greater than zero",
                    );
                }
            }
            TransformSpec::Filter { where_, .. } => {
                self.predicate(&base.key("where"), where_, &names);
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
        let (op, literals): (&str, Vec<&Literal>) = match &c.test {
            Test::Eq(l) => ("eq", vec![l]),
            Test::Ne(l) => ("ne", vec![l]),
            Test::Lt(l) => ("lt", vec![l]),
            Test::Le(l) => ("le", vec![l]),
            Test::Gt(l) => ("gt", vec![l]),
            Test::Ge(l) => ("ge", vec![l]),
            Test::In(ls) => ("in", ls.iter().collect()),
            Test::Between { min, max } => ("min", min.iter().chain(max.iter()).collect()),
        };
        let ordered = !matches!(c.test, Test::Eq(_) | Test::Ne(_) | Test::In(_));
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
