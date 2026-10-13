//! Pipeline configuration loading and validation.
//!
//! # Responsibility
//!
//! - Load and interpolate YAML into a typed, validated [`PipelineSpec`].
//! - Diagnostics with paths and line numbers ([`ConfigReport`]).
//! - JSON Schema output for editors ([`json_schema`]).
//! - A stable config hash ([`PipelineSpec::config_hash`]) and the memory
//!   estimate ([`PipelineSpec::memory_estimate`]) that `thadam validate` prints.
//!
//! # Deliberately not exposed
//!
//! - The YAML library and its AST.
//! - Connector-specific config types; connector factories validate those.
//! - Hot reload.
//! - Multiple pipelines per file.
//!
//! # Lifecycle
//!
//! 1. Index the text with the low-level YAML parser: syntax errors, positions,
//!    duplicate keys, anchors and aliases, merge keys, and tags.
//! 2. Parse to a `serde_json::Value` with `serde-saphyr` (`strict_booleans`,
//!    unsupported tags rejected, a tight node and depth budget).
//! 3. Interpolate `${env:NAME}` and `${file:/path}` on string scalars.
//! 4. Check `version`, then deserialize each section and each list item on its
//!    own with `deny_unknown_fields`, so one mistake does not hide the rest.
//! 5. Run cross-field checks (ids, references, pointers, predicates, limits).
//! 6. Estimate memory and warn above `runtime.memory_target`.
//!
//! Connector `config` blocks stay `serde_json::Value` (D22). Connector-specific
//! validation through the registry happens after this crate, in the binary.
//!
//! # Syntax notes for users
//!
//! - **Quote identifiers.** YAML reads `014` as a number and `no` as text or a
//!   boolean depending on context. Write `client_id: "014"`.
//!   Booleans are only `true` and `false`.
//! - **Anchors, aliases, merge keys, and tags are rejected.** Repeat values.
//! - **Durations need units** (`50ms`, `5m`, `1h`, `1d`); sizes use `MiB`/`GiB`;
//!   counts may use `k` and `m`.
//! - **`on_full` has no default.** Choose `block` or `drop_oldest`.
//! - **Filters are structured** (`where: { field: level, ne: debug }`). The
//!   `expr:` key is reserved for a future expression language.
//! - **`${env:X}` and `${file:/abs/path}` always produce strings**, so use them
//!   for text, durations, and sizes; write `$${` for a literal `${`.
//!
//! # Decisions closed in PR5 (ADR-0012 "PR5 implementation details")
//!
//! 1. **Key spelling** is frozen as `kind`, `where`, `wal`, `match`, `from`,
//!    `convert_to`, `on_full`; `expr` is reserved and rejected with a hint.
//! 2. **O2:** `transforms[].sinks: [ids]` restricts a step to those sinks; absent
//!    means every sink.
//! 3. **Hash:** see [`CONFIG_HASH_VERSION`] and [`PipelineSpec::config_hash`]. It is
//!    taken after typed normalization but before interpolation: interpolated
//!    values are hashed as written, so a rotated secret does not move it. Every key
//!    added after the first release is omitted from the serialized form at its
//!    default, so [`CONFIG_HASH_VERSION`] stays 1: `on_miss: null` is omitted, and
//!    an `on_change` entry with `deadband: 0` serializes as the bare field name, so
//!    `fields: [mode]` and `fields: [{ field: mode, deadband: 0 }]` hash alike.
//! 4. **Interpolation:** parsed string scalars only; `$${` escapes; no nesting, no
//!    defaults; results are strings; errors name the variable or file, never its
//!    value. The `serde-saphyr` `properties` feature is not used.
//! 5. **Parse budget:** 50,000 YAML nodes and depth 32 (the demo uses a few
//!    thousand and about 7 levels), plus a 1 MiB file cap.
//! 6. **Anchors, aliases, merge keys, and custom tags are rejected** with a
//!    diagnostic at their position, so every source line means one thing.
//! 7. **Path-to-span index:** a second pass walks the `granit-parser` events
//!    (re-exported by `serde-saphyr`) for positions and duplicate keys, and each
//!    section and list item is validated separately for multi-error output.
//! 8. **Internally tagged enums are not used on input:** `kind` is dispatched by
//!    hand so errors point at the key (`transforms[1].heartbeat`).
//! 9. **Scalar hazards** (`yes`/`no` are text, `014` and `1_000` are numbers at the
//!    `Value` boundary, quoted numbers are strings) are pinned by tests.
//! 10. Predicates use `all`/`any`/`not` and `eq ne lt le gt ge in null min max`; names
//!     resolve against the dataset (fields, identity levels, the `explode` index
//!     field, `quality.status|flags|reason`) and literals are type-checked, with
//!     integers above `i64::MAX` kept as `u64`.
//! 11. **Transforms:** all eight kinds of `thadam-transform` are accepted, with its
//!     option names. A step is checked against the dataset's fields as left by the
//!     earlier steps that apply to the same sink, so `rename`, `project`, `cast`,
//!     and `enrich` change what later steps may name. An `enrich` table is read
//!     when the chain is compiled, never while validating.
//!
//! # Parquet field IDs (ADR-0013, "PR13 implementation decisions")
//!
//! `thadam-arrow` writes a stable `PARQUET:field_id` on every column and takes
//! the IDs of declared fields and identity levels from configuration. They are
//! spelled **`id`** (the same key the other lists use for their identifier) and
//! are required:
//!
//! ```yaml
//! identity:
//!   site:  { id: 100, template: "{topic.2}" }
//!   robot: { id: 101, template: "{topic.3}" }
//! fields:
//!   - { id: 1000, name: seq,     type: u64, from: /seq, required: true }
//!   - { id: 1001, name: soc_pct, type: f32, from: /soc_pct }
//! ```
//!
//! - A declared field's `id` is in [`DECLARED_FIELD_ID_RANGE`] (1000 to
//!   4,194,303); an identity level's `id` is in [`IDENTITY_LEVEL_ID_RANGE`]
//!   (100 to 199). IDs 1 to 99 belong to the envelope columns and are fixed by
//!   `thadam-arrow`; nested IDs are derived from the parent's.
//! - IDs are unique within a dataset, not across datasets. Violations are
//!   diagnostics at the `id` value (`datasets[0].fields[1].id`).
//! - An identity level is a mapping now; the earlier `site: "{topic.2}"` string
//!   form is refused with a hint showing the new form.
//! - **Never reuse the ID of a removed field.** The loader cannot see history,
//!   so retiring an ID is the configuration owner's job.
//! - IDs are part of the config hash, so changing one changes the hash.
//! - [`FieldSpec::to_field_def`] and [`DatasetSpec::field_defs`] set
//!   `FieldDef::field_id`; [`DatasetSpec::level_ids`] gives the `(name, id)`
//!   pairs for `thadam_arrow::DatasetSchema::builder(..).level(name, id)`.
//!
//! # Per-sink WAL policy
//!
//! A sink entry takes three keys next to `id`, `kind`, and `config` (see
//! [`SinkSpec`]):
//!
//! ```yaml
//! required: false              # default true
//! max_lag: { bytes: 2GiB }     # or { age: 30m }; optional, needs required: true
//! start_from: latest           # earliest (default) | latest
//! ```
//!
//! - `required: false` makes the sink an optional WAL consumer: it never holds
//!   retention back. `max_lag` bounds how far a required sink may fall behind
//!   before it stops holding it back, so the two together are refused.
//! - `start_from` only applies to a sink with no committed WAL cursor.
//! - A sink serializes (and hashes) without a key that holds its default, so
//!   existing configurations keep their [`PipelineSpec::config_hash`].
//!
//! # Versioning
//!
//! `version: 1` is mandatory. A newer or unknown version is refused with
//! `config.incompatible` and exit code 2; the loader never guesses.

mod check;
mod ctx;
mod diagnostics;
mod doc;
mod estimate;
mod hash;
mod interpolate;
mod path;
mod predicate;
mod scalars;
mod schema;
mod secret;
mod semantic;
mod source;
mod spans;
mod spec;

use std::io::Read;
use std::path::Path;

use thadam_core::codes;
use serde_json::Value;

pub use diagnostics::{ConfigReport, Diagnostic, LineCol};
pub use estimate::{MemoryComponent, MemoryEstimate, format_bytes};
pub use hash::CONFIG_HASH_VERSION;
pub use interpolate::UnresolvedPolicy;
pub use predicate::{Condition, Literal, Predicate, Test};
pub use scalars::{CountSpec, DurationSpec, FieldTypeSpec, SizeSpec, TimeDomainSpec};
pub use schema::json_schema;
pub use secret::{REDACTED, Secret};
pub use semantic::KindPolicy;
pub use source::{EnvSource, MAX_SECRET_FILE_BYTES, MapEnv, ProcessEnv};
pub use spec::{
    AdminSpec, AggregateFn, AggregateSpec, CONFIG_VERSION, ConnectorSpec, DECLARED_FIELD_ID_RANGE,
    DatasetSpec, DecodeFormat, DedupSpec, EnrichFieldSpec, EnrichFormat, EnrichKeySpec,
    EventIdSpec, ExplodeSpec, FieldSpec, FsyncSpec, IDENTITY_LEVEL_ID_RANGE, IdentityLevel,
    LogFormat, LogLevel, LogsSpec, MatchSpec, MaxLagSpec, ObservabilitySpec, OnFull, OnMissSpec,
    PipelineMeta, PipelineSpec, QuarantineFormat, QuarantineSpec, RuleStatus, RuntimeSpec,
    SinkSpec, StartFromSpec, TimeSpec, TimeUnit, TransformSpec, ValidateRule, WalSpec, WatchField,
    WindowTime,
};

use crate::ctx::Ctx;
use crate::path::PathExt;
use crate::spans::{Scan, StructuralKind};

/// The largest configuration file read, in bytes.
pub const MAX_CONFIG_BYTES: usize = 1 << 20;

/// The most YAML nodes (scalars, lists, and mappings) a configuration may have.
const MAX_YAML_NODES: usize = 50_000;
/// The deepest YAML nesting a configuration may have.
const MAX_YAML_DEPTH: usize = 32;

/// How [`check_str`] and [`check_file`] behave.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct CheckOptions {
    /// What to do with an unresolvable `${env:}` or `${file:}`.
    pub unresolved: UnresolvedPolicy,
    /// The connector kinds the binary has registered.
    pub kinds: KindPolicy,
}

impl CheckOptions {
    /// Default options: unresolved references are errors, any well-formed kind is accepted.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the policy for unresolvable references.
    #[must_use]
    pub fn unresolved(mut self, policy: UnresolvedPolicy) -> Self {
        self.unresolved = policy;
        self
    }

    /// Restricts connector kinds to the registered ones.
    #[must_use]
    pub fn kinds(mut self, kinds: KindPolicy) -> Self {
        self.kinds = kinds;
        self
    }
}

/// The result of checking a configuration: the spec when there are no errors,
/// and every diagnostic either way (warnings included).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Checked {
    /// The validated spec; `Some` exactly when `report` has no errors.
    pub spec: Option<PipelineSpec>,
    /// All errors and warnings.
    pub report: ConfigReport,
}

/// Loads and validates a configuration file. Warnings are discarded; use
/// [`check_file`] to see them.
pub fn load_file(path: &Path, env: &dyn EnvSource) -> Result<PipelineSpec, ConfigReport> {
    into_result(check_file(path, env, &CheckOptions::default()))
}

/// Loads and validates configuration text. Warnings are discarded; use
/// [`check_str`] to see them.
pub fn load_str(yaml: &str, env: &dyn EnvSource) -> Result<PipelineSpec, ConfigReport> {
    into_result(check_str(yaml, env, &CheckOptions::default()))
}

fn into_result(checked: Checked) -> Result<PipelineSpec, ConfigReport> {
    match checked.spec {
        Some(spec) if !checked.report.has_errors() => Ok(spec),
        _ => Err(checked.report),
    }
}

fn failure(code: &'static str, message: String, span: Option<LineCol>) -> Checked {
    Checked {
        spec: None,
        report: ConfigReport {
            errors: vec![Diagnostic {
                code,
                path: "$".to_owned(),
                span,
                message,
                hint: None,
            }],
            warnings: Vec::new(),
        },
    }
}

/// Reads, validates, and reports on a configuration file.
pub fn check_file(path: &Path, env: &dyn EnvSource, opts: &CheckOptions) -> Checked {
    let mut bytes = Vec::new();
    let read = std::fs::File::open(path)
        .and_then(|f| f.take(MAX_CONFIG_BYTES as u64 + 1).read_to_end(&mut bytes));
    if let Err(e) = read {
        return failure(
            codes::CONFIG_INVALID,
            format!("cannot read `{}`: {e}", path.display()),
            None,
        );
    }
    match String::from_utf8(bytes) {
        Ok(text) => check_str(&text, env, opts),
        Err(_) => failure(
            codes::CONFIG_INVALID,
            format!("`{}` is not valid UTF-8", path.display()),
            None,
        ),
    }
}

fn report_structural(scan: &Scan, ctx: &mut Ctx<'_>) {
    for s in &scan.structural {
        let (msg, hint) = match &s.kind {
            StructuralKind::DuplicateKey(k) => (
                format!("duplicate key `{k}`"),
                Some("each key may appear once per mapping".to_owned()),
            ),
            StructuralKind::Anchor | StructuralKind::Alias => (
                "anchors and aliases are not supported in config version 1".to_owned(),
                Some("repeat the value instead of sharing it by reference".to_owned()),
            ),
            StructuralKind::MergeKey => (
                "merge keys (`<<`) are not supported in config version 1".to_owned(),
                Some("repeat the keys instead of merging a mapping".to_owned()),
            ),
            StructuralKind::Tag(t) => (
                format!("YAML tag `{t}` is not supported"),
                Some("remove the tag; Thadam configuration does not use custom tags".to_owned()),
            ),
            StructuralKind::MultipleDocuments => (
                "a configuration is one YAML document, but a second one starts here".to_owned(),
                Some("remove the extra `---` separator".to_owned()),
            ),
        };
        ctx.error_at(codes::CONFIG_INVALID, &s.path, s.pos, msg, hint);
    }
}

fn parse_value(
    src: &str,
    policy: serde_saphyr::options::DuplicateKeyPolicy,
) -> Result<Value, serde_saphyr::Error> {
    let options = serde_saphyr::options! {
        strict_booleans: true,
        reject_unsupported_tags: true,
        with_snippet: false,
        emit_comments: false,
        duplicate_keys: policy,
        merge_keys: serde_saphyr::options::MergeKeyPolicy::AsOrdinary,
        budget: serde_saphyr::budget! {
            max_nodes: MAX_YAML_NODES,
            max_depth: MAX_YAML_DEPTH,
        },
    };
    serde_saphyr::from_str_with_options::<doc::Doc>(src, options).map(|doc| doc.0)
}

/// Validates configuration text and reports every problem found.
pub fn check_str(yaml: &str, env: &dyn EnvSource, opts: &CheckOptions) -> Checked {
    use serde_saphyr::options::DuplicateKeyPolicy;

    let yaml = yaml.strip_prefix('\u{feff}').unwrap_or(yaml);
    if yaml.len() > MAX_CONFIG_BYTES {
        return failure(
            codes::CONFIG_INVALID,
            format!("the configuration is larger than {MAX_CONFIG_BYTES} bytes"),
            None,
        );
    }
    let scan = match spans::scan(yaml) {
        Ok(scan) => scan,
        Err(e) => {
            return failure(
                codes::CONFIG_INVALID,
                format!("YAML syntax error: {}", e.message),
                Some(e.pos),
            );
        }
    };
    let mut ctx = Ctx::new(&scan.index);
    if scan.empty {
        ctx.error(
            codes::CONFIG_INVALID,
            &[],
            spans::Anchor::Key,
            "the configuration file is empty",
            Some("start with `version: 1`".to_owned()),
        );
        return finish(ctx, None);
    }
    report_structural(&scan, &mut ctx);

    let duplicates = scan
        .structural
        .iter()
        .any(|s| matches!(s.kind, StructuralKind::DuplicateKey(_)));
    let policy = if duplicates {
        DuplicateKeyPolicy::LastWins
    } else {
        DuplicateKeyPolicy::Error
    };
    let mut root = match parse_value(yaml, policy) {
        Ok(v) => v,
        Err(e) => {
            if scan.structural.is_empty() {
                let span = e.location().map(|l| LineCol {
                    line: u32::try_from(l.line()).unwrap_or(u32::MAX),
                    column: u32::try_from(l.column()).unwrap_or(u32::MAX),
                });
                let mut d = ctx.into_report();
                d.errors.push(Diagnostic {
                    code: codes::CONFIG_INVALID,
                    path: "$".to_owned(),
                    span,
                    message: e.to_string(),
                    hint: None,
                });
                return Checked {
                    spec: None,
                    report: d,
                };
            }
            return finish(ctx, None);
        }
    };

    let sites = interpolate::interpolate(&mut root, env, opts.unresolved, &mut ctx);
    let Some(parsed) = check::parse_document(&root, &mut ctx) else {
        return finish(ctx, None);
    };
    semantic::check(&parsed, &opts.kinds, &mut ctx);

    let spec = if ctx.error_count() == 0 {
        assemble(parsed, sites)
    } else {
        None
    };
    if let Some(spec) = &spec {
        let estimate = spec.memory_estimate();
        if estimate.exceeds_target() {
            ctx.warn(
                codes::CONFIG_INVALID,
                &Vec::new().key("runtime").key("memory_target"),
                spans::Anchor::Value,
                format!(
                    "estimated Thadam-owned memory {} exceeds `runtime.memory_target` {}",
                    format_bytes(estimate.total_bytes()),
                    format_bytes(estimate.target_bytes.unwrap_or(0))
                ),
                Some("lower `dedup.max_keys`, `max_open_files`, or `row_group_bytes`, or raise `memory_target`; run `thadam validate` for the breakdown".to_owned()),
            );
        }
    }
    finish(ctx, spec)
}

fn finish(ctx: Ctx<'_>, spec: Option<PipelineSpec>) -> Checked {
    let report = ctx.into_report();
    let spec = if report.has_errors() { None } else { spec };
    Checked { spec, report }
}

fn assemble(parsed: check::Parsed, sites: Vec<spec::InterpolationSite>) -> Option<PipelineSpec> {
    let secret_paths = |section: &str, i: usize| -> Vec<String> {
        let prefix = Vec::new().key(section).idx(i).key("config");
        sites
            .iter()
            .filter(|s| s.is_under(&prefix))
            .map(|s| path::pointer(&s.path[prefix.len()..]))
            .collect()
    };
    let sources: Vec<ConnectorSpec> = parsed.sources.items?.into_iter().collect::<Option<_>>()?;
    let sources = sources
        .into_iter()
        .enumerate()
        .map(|(i, mut c)| {
            if c.config.is_null() {
                c.config = Value::Object(serde_json::Map::new());
            }
            c.secret_paths = secret_paths("sources", i);
            c
        })
        .collect();
    let sinks: Vec<SinkSpec> = parsed.sinks.items?.into_iter().collect::<Option<_>>()?;
    let sinks = sinks
        .into_iter()
        .enumerate()
        .map(|(i, mut c)| {
            if c.config.is_null() {
                c.config = Value::Object(serde_json::Map::new());
            }
            c.secret_paths = secret_paths("sinks", i);
            c
        })
        .collect();
    Some(PipelineSpec {
        version: CONFIG_VERSION,
        pipeline: parsed.pipeline?,
        sources,
        wal: parsed.wal?,
        quarantine: parsed.quarantine?,
        datasets: parsed
            .datasets
            .items?
            .into_iter()
            .collect::<Option<Vec<_>>>()?,
        transforms: parsed
            .transforms
            .items?
            .into_iter()
            .collect::<Option<Vec<_>>>()?,
        sinks,
        runtime: parsed.runtime?,
        observability: parsed.observability?,
        interpolations: sites.clone(),
    })
}
