//! `enrich`: adds fields from a static CSV or YAML table.
//!
//! The file is read once, at compile time; that is the only I/O in the crate.
//! Its SHA-256 is reported through [`EnrichmentInfo`] so lineage can record it:
//! row-set equivalence on replay holds for the same WAL range, configuration,
//! and enrichment-file hash.
//!
//! A CSV table has a header row. A YAML table is either a list of mappings
//! (each holding the key column) or a mapping from key to a mapping of columns.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use thadam_core::{
    ErrorDomain, Event, FieldDef, FieldIdx, FieldType, Payload, ThadamError, Shape, ShapeRef, Sym,
    Value, codes,
};
use serde_json::Value as Json;
use sha2::{Digest, Sha256};

use super::fieldops::{rebuild, step_failed};
use super::{Compiled, Step, StepCtx, StepResult, config_error, intern};
use crate::decode::Coerce;
use crate::spec::{EnrichSpec, OnMiss, parse_field_type};
use crate::timex::TimeParser;

/// The recorded identity of one loaded enrichment file.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct EnrichmentInfo {
    /// The dataset the table enriches.
    pub dataset: Sym,
    /// The file path as configured.
    pub file: String,
    /// Lowercase hex SHA-256 of the file's bytes.
    pub sha256: String,
    /// Rows loaded.
    pub rows: usize,
}

#[derive(Clone, Copy, Debug)]
enum KeySource {
    Field(FieldIdx),
    Level(usize),
}

#[derive(Debug)]
struct Enrich {
    key: KeySource,
    table: HashMap<String, Box<[Value]>>,
    width: usize,
    on_miss: OnMiss,
    out: ShapeRef,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Format {
    Csv,
    Yaml,
}

struct Column {
    name: String,
    ty: FieldType,
    coerce: Coerce,
    source: String,
}

pub(super) fn compile(
    spec: &EnrichSpec,
    ctx: &mut StepCtx<'_>,
    input: &ShapeRef,
    path: &str,
) -> Result<Compiled, Vec<ThadamError>> {
    let mut errs = Vec::new();

    let key_path = format!("{path}.key.field");
    let key = if let Some(i) = input.index_of(&spec.key.field) {
        match input.field(i).map(|d| &d.ty) {
            Some(FieldType::Str) => Some(KeySource::Field(i)),
            Some(t) if is_int(t) => Some(KeySource::Field(i)),
            _ => {
                errs.push(config_error(
                    &key_path,
                    "an enrichment key must be a string or integer field",
                ));
                None
            }
        }
    } else if let Some(i) = ctx.levels.iter().position(|l| **l == *spec.key.field) {
        Some(KeySource::Level(i))
    } else {
        errs.push(config_error(
            &key_path,
            format!("unknown field `{}`", spec.key.field),
        ));
        None
    };

    let mut columns = Vec::new();
    let mut defs: Vec<FieldDef> = input.fields().to_vec();
    for (i, add) in spec.add.iter().enumerate() {
        let apath = format!("{path}.add[{i}]");
        if defs.iter().any(|d| *d.name == *add.name) {
            errs.push(config_error(
                &apath,
                format!("field `{}` already exists", add.name),
            ));
            continue;
        }
        let ty = match parse_field_type(&add.ty) {
            Ok(t) => t,
            Err(e) => {
                errs.push(
                    ThadamError::permanent(ErrorDomain::Schema, codes::SCHEMA_UNSUPPORTED_TYPE, e)
                        .with_context("path", format!("{apath}.type")),
                );
                continue;
            }
        };
        let ts = if ty == FieldType::Ts {
            match add.unit.as_deref().map(|u| TimeParser::compile(u, None)) {
                Some(Ok(p)) => Some(p),
                Some(Err(e)) => {
                    errs.push(config_error(&format!("{apath}.unit"), e));
                    continue;
                }
                None => {
                    errs.push(config_error(&apath, "a `ts` field needs `unit`"));
                    continue;
                }
            }
        } else {
            None
        };
        let Some(coerce) = Coerce::for_type(&ty, ts) else {
            errs.push(config_error(
                &apath,
                format!("type {ty:?} is not supported here"),
            ));
            continue;
        };
        let mut def = FieldDef::new(add.name.as_str(), ty.clone());
        if ty != FieldType::Ts
            && let Some(unit) = &add.unit
        {
            def = def.with_unit(unit.as_str());
        }
        defs.push(def);
        columns.push(Column {
            name: add.name.clone(),
            ty,
            coerce,
            source: add.column.clone().unwrap_or_else(|| add.name.clone()),
        });
    }
    if spec.add.is_empty() {
        errs.push(config_error(
            &format!("{path}.add"),
            "`add` needs at least one field",
        ));
    }
    let Some(key) = key else {
        return Err(errs);
    };
    if !errs.is_empty() {
        return Err(errs);
    }

    let file_path = resolve(&spec.file, ctx.base_dir);
    let format = match format_of(spec.format.as_deref(), &file_path) {
        Ok(f) => f,
        Err(e) => return Err(vec![config_error(&format!("{path}.format"), e)]),
    };
    let bytes = read_limited(&file_path, ctx.limits.max_enrichment_bytes)
        .map_err(|e| vec![config_error(&format!("{path}.file"), e)])?;
    let sha256 = hex(&Sha256::digest(&bytes));

    let rows = match format {
        Format::Csv => parse_csv(
            &bytes,
            &spec.key.column,
            &columns,
            ctx.limits.max_enrichment_rows,
        ),
        Format::Yaml => parse_yaml(&bytes, &spec.key.column, ctx.limits.max_enrichment_rows),
    }
    .map_err(|e| vec![config_error(&format!("{path}.file"), e)])?;

    let mut table: HashMap<String, Box<[Value]>> = HashMap::with_capacity(rows.len());
    for (n, (key_text, cells)) in rows.iter().enumerate() {
        let mut values = Vec::with_capacity(columns.len());
        for col in &columns {
            let cell = cells.get(col.source.as_str()).unwrap_or(&Json::Null);
            let value = if cell.is_null() {
                Value::Null
            } else {
                col.coerce.extract(cell, None).map_err(|e| {
                    vec![config_error(
                        &format!("{path}.file"),
                        format!("row {}: {}", n + 1, e.into_error(&col.name).message),
                    )]
                })?
            };
            values.push(value);
        }
        if table
            .insert(key_text.clone(), values.into_boxed_slice())
            .is_some()
        {
            return Err(vec![config_error(
                &format!("{path}.file"),
                format!("row {}: duplicate key", n + 1),
            )]);
        }
    }

    let out = intern(ctx, Shape::new(defs), path)?;
    let mut compiled = Compiled::new(
        Enrich {
            key,
            width: columns.len(),
            table,
            on_miss: spec.on_miss,
            out: out.clone(),
        },
        out,
    );
    compiled.enrichment = Some(EnrichmentInfo {
        dataset: ctx.dataset.clone(),
        file: spec.file.clone(),
        sha256,
        rows: rows.len(),
    });
    Ok(compiled)
}

fn is_int(t: &FieldType) -> bool {
    matches!(
        t,
        FieldType::I8
            | FieldType::I16
            | FieldType::I32
            | FieldType::I64
            | FieldType::U8
            | FieldType::U16
            | FieldType::U32
            | FieldType::U64
    )
}

fn resolve(file: &str, base: Option<&Path>) -> PathBuf {
    let p = Path::new(file);
    match base {
        Some(base) if p.is_relative() => base.join(p),
        _ => p.to_path_buf(),
    }
}

fn format_of(explicit: Option<&str>, path: &Path) -> Result<Format, String> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default();
    match explicit.unwrap_or(ext) {
        "csv" => Ok(Format::Csv),
        "yaml" | "yml" => Ok(Format::Yaml),
        other => Err(format!(
            "cannot tell the table format from `{other}`; set `format` to csv or yaml"
        )),
    }
}

fn read_limited(path: &Path, max: u64) -> Result<Vec<u8>, String> {
    use std::io::Read as _;
    let file =
        std::fs::File::open(path).map_err(|e| format!("cannot read enrichment file: {e}"))?;
    let mut bytes = Vec::new();
    // `take` bounds the read even for files whose metadata reports no size,
    // such as device files.
    file.take(max.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|e| format!("cannot read enrichment file: {e}"))?;
    if bytes.len() as u64 > max {
        return Err(format!(
            "enrichment file is larger than the limit of {max} bytes"
        ));
    }
    Ok(bytes)
}

fn hex(digest: &[u8]) -> String {
    digest.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

const LINEAGE_DOMAIN: &[u8] = b"thadam-enrichment-hash-v1\0";

/// The lineage hash of the enrichment tables a chain loaded: lowercase hex
/// SHA-256 over the sorted, distinct `dataset sha256` lines, or empty when
/// there are none. Each file's own digest is [`EnrichmentInfo::sha256`]; this
/// one covers the set a sink's footer names as `thadam.enrichment_hash`.
pub(crate) fn lineage_hash<'a>(loaded: impl Iterator<Item = (&'a str, &'a str)>) -> String {
    let mut lines: Vec<String> = loaded
        .map(|(dataset, sha256)| format!("{dataset} {sha256}\n"))
        .collect();
    if lines.is_empty() {
        return String::new();
    }
    lines.sort_unstable();
    lines.dedup();
    let mut digest = Sha256::new();
    digest.update(LINEAGE_DOMAIN);
    for line in &lines {
        digest.update(line.as_bytes());
    }
    hex(&digest.finalize())
}

type Cells = HashMap<String, Json>;

fn cell_json(text: &str, ty: &FieldType) -> Result<Json, String> {
    if text.is_empty() {
        return Ok(Json::Null);
    }
    match ty {
        FieldType::Str => Ok(Json::String(text.to_owned())),
        FieldType::Bool => match text.to_ascii_lowercase().as_str() {
            "true" => Ok(Json::Bool(true)),
            "false" => Ok(Json::Bool(false)),
            _ => Err(format!("`{text}` is not true or false")),
        },
        FieldType::List(_) => Err("a CSV column cannot hold a list; use a YAML table".to_owned()),
        _ => match serde_json::from_str::<Json>(text) {
            Ok(j @ Json::Number(_)) => Ok(j),
            _ if *ty == FieldType::Ts => Ok(Json::String(text.to_owned())),
            _ => Err(format!("`{text}` is not a number")),
        },
    }
}

fn parse_csv(
    bytes: &[u8],
    key_column: &str,
    columns: &[Column],
    max_rows: usize,
) -> Result<Vec<(String, Cells)>, String> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(bytes);
    let headers = reader
        .headers()
        .map_err(|e| format!("bad CSV header: {e}"))?
        .clone();
    let position = |name: &str| headers.iter().position(|h| h == name);
    let key_at =
        position(key_column).ok_or_else(|| format!("the CSV has no key column `{key_column}`"))?;
    let mut at = Vec::with_capacity(columns.len());
    for col in columns {
        at.push(
            position(&col.source)
                .ok_or_else(|| format!("the CSV has no column `{}`", col.source))?,
        );
    }
    let mut rows = Vec::new();
    for (n, record) in reader.records().enumerate() {
        if n >= max_rows {
            return Err(format!("the table has more than {max_rows} rows"));
        }
        let record = record.map_err(|e| format!("row {}: {e}", n + 1))?;
        let key = record.get(key_at).unwrap_or_default().to_owned();
        if key.is_empty() {
            return Err(format!("row {}: empty key", n + 1));
        }
        let mut cells = Cells::new();
        for (col, &i) in columns.iter().zip(&at) {
            let text = record.get(i).unwrap_or_default();
            let json = cell_json(text, &col.ty)
                .map_err(|e| format!("row {}, column `{}`: {e}", n + 1, col.source))?;
            cells.insert(col.source.clone(), json);
        }
        rows.push((key, cells));
    }
    Ok(rows)
}

fn key_text(j: &Json) -> Option<String> {
    match j {
        Json::String(s) if !s.is_empty() => Some(s.clone()),
        Json::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn parse_yaml(
    bytes: &[u8],
    key_column: &str,
    max_rows: usize,
) -> Result<Vec<(String, Cells)>, String> {
    let text =
        std::str::from_utf8(bytes).map_err(|_| "the YAML table is not valid UTF-8".to_owned())?;
    let doc: Json = serde_saphyr::from_str(text).map_err(|e| format!("bad YAML table: {e}"))?;
    let to_cells = |obj: &serde_json::Map<String, Json>| -> Cells {
        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    };
    let mut rows = Vec::new();
    match &doc {
        Json::Array(items) => {
            for (n, item) in items.iter().enumerate() {
                let Json::Object(obj) = item else {
                    return Err(format!("row {}: expected a mapping", n + 1));
                };
                let key = obj
                    .get(key_column)
                    .and_then(key_text)
                    .ok_or_else(|| format!("row {}: missing key column `{key_column}`", n + 1))?;
                rows.push((key, to_cells(obj)));
            }
        }
        Json::Object(map) => {
            for (n, (key, item)) in map.iter().enumerate() {
                let Json::Object(obj) = item else {
                    return Err(format!("row {}: expected a mapping", n + 1));
                };
                rows.push((key.clone(), to_cells(obj)));
            }
        }
        _ => return Err("a YAML table is a list of mappings or a mapping of mappings".to_owned()),
    }
    if rows.len() > max_rows {
        return Err(format!("the table has more than {max_rows} rows"));
    }
    Ok(rows)
}

impl Enrich {
    fn lookup_key<'e>(&self, event: &'e Event) -> Result<Option<Cow<'e, str>>, ThadamError> {
        Ok(match self.key {
            KeySource::Level(i) => event
                .asset()
                .levels()
                .get(i)
                .map(|(_, v)| Cow::Borrowed(v.as_str())),
            KeySource::Field(i) => {
                let Payload::Record(record) = event.payload() else {
                    return Err(step_failed("event payload is not a record"));
                };
                let value = record
                    .try_get_at(i)
                    .ok_or_else(|| step_failed("event record does not match the step's shape"))?;
                match value {
                    Value::Str(s) => Some(Cow::Borrowed(s.as_str())),
                    Value::Null => None,
                    other => other
                        .as_i64()
                        .map(|v| Cow::Owned(v.to_string()))
                        .or_else(|| other.as_u64().map(|v| Cow::Owned(v.to_string()))),
                }
            }
        })
    }
}

impl Step for Enrich {
    fn apply(&mut self, event: Event) -> StepResult {
        let found = match self.lookup_key(&event) {
            Ok(Some(key)) => self.table.get(key.as_ref()).cloned(),
            Ok(None) => None,
            Err(e) => return StepResult::Reject(e),
        };
        let row: Box<[Value]> = match found {
            Some(row) => row,
            None => match self.on_miss {
                OnMiss::Null => vec![Value::Null; self.width].into_boxed_slice(),
                OnMiss::Drop => return StepResult::Filtered,
                OnMiss::Reject => {
                    return StepResult::Reject(ThadamError::permanent(
                        ErrorDomain::Transform,
                        codes::TRANSFORM_FAILED,
                        "no enrichment row for the event's key",
                    ));
                }
            },
        };
        match rebuild(event, &self.out, |mut values| {
            values.extend(row.iter().cloned());
            Ok(values)
        }) {
            Ok(event) => StepResult::Keep(event),
            Err(e) => StepResult::Reject(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::limits::ChainLimits;
    use crate::testutil::{event, shape};
    use serde_json::json;
    use std::io::Write as _;

    fn input() -> ShapeRef {
        shape(&[("model_key", FieldType::Str), ("n", FieldType::U32)])
    }

    fn write(dir: &tempfile::TempDir, name: &str, text: &str) -> String {
        let p = dir.path().join(name);
        std::fs::File::create(&p)
            .unwrap()
            .write_all(text.as_bytes())
            .unwrap();
        name.to_owned()
    }

    fn compile_spec(
        dir: &tempfile::TempDir,
        spec: serde_json::Value,
    ) -> Result<Compiled, Vec<ThadamError>> {
        let spec: EnrichSpec = serde_json::from_value(spec).unwrap();
        crate::testutil::with_ctx_full(&ChainLimits::default(), Some(dir.path()), |ctx| {
            compile(&spec, ctx, &input(), "transforms[0]")
        })
    }

    fn csv_spec(file: &str, extra: serde_json::Value) -> serde_json::Value {
        let mut v = json!({
            "file": file,
            "key": { "field": "robot", "column": "robot_id" },
            "add": [
                { "name": "model", "type": "str" },
                { "name": "payload_kg", "type": "f32", "column": "payload", "unit": "kg" },
            ]
        });
        if let (Some(o), Some(e)) = (v.as_object_mut(), extra.as_object()) {
            o.extend(e.clone());
        }
        v
    }

    const CSV: &str = "robot_id,model,payload\nrobot-01,A1,12.5\nrobot-02,B2,\n";

    fn apply(c: &mut Compiled, robot: &str) -> StepResult {
        c.step.apply(event(
            &input(),
            vec![Value::str("k"), Value::U32(1)],
            robot,
            0,
        ))
    }

    #[test]
    fn csv_join_on_an_identity_level_with_a_recorded_hash() {
        let dir = tempfile::tempdir().unwrap();
        let f = write(&dir, "robots.csv", CSV);
        let mut c = compile_spec(&dir, csv_spec(&f, json!({}))).unwrap();
        let info = c.enrichment.clone().unwrap();
        assert_eq!(info.rows, 2);
        assert_eq!(info.sha256.len(), 64);
        assert_eq!(info.sha256, hex(&Sha256::digest(CSV.as_bytes())));
        assert_eq!(c.out_shape.len(), 4);
        assert_eq!(
            c.out_shape.field(FieldIdx::new(3)).unwrap().unit.as_deref(),
            Some("kg")
        );

        let StepResult::Keep(e) = apply(&mut c, "robot-01") else {
            panic!()
        };
        let r = e.payload().as_record().unwrap();
        assert_eq!(r.get("model").and_then(Value::as_str), Some("A1"));
        assert_eq!(r.get("payload_kg"), Some(&Value::F32(12.5)));
        assert!(r.validate().is_ok());

        let StepResult::Keep(e) = apply(&mut c, "robot-02") else {
            panic!()
        };
        assert_eq!(
            e.payload().as_record().unwrap().get("payload_kg"),
            Some(&Value::Null)
        );

        let StepResult::Keep(e) = apply(&mut c, "robot-99") else {
            panic!()
        };
        assert_eq!(
            e.payload().as_record().unwrap().get("model"),
            Some(&Value::Null)
        );
    }

    #[test]
    fn hash_changes_with_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let a = write(&dir, "a.csv", CSV);
        let b = write(&dir, "b.csv", &format!("{CSV}robot-03,C3,1\n"));
        let ha = compile_spec(&dir, csv_spec(&a, json!({})))
            .unwrap()
            .enrichment
            .unwrap()
            .sha256;
        let hb = compile_spec(&dir, csv_spec(&b, json!({})))
            .unwrap()
            .enrichment
            .unwrap()
            .sha256;
        assert_ne!(ha, hb);
    }

    #[test]
    fn on_miss_reject_and_drop() {
        let dir = tempfile::tempdir().unwrap();
        let f = write(&dir, "robots.csv", CSV);
        let mut c = compile_spec(&dir, csv_spec(&f, json!({ "on_miss": "reject" }))).unwrap();
        assert!(matches!(apply(&mut c, "nope"), StepResult::Reject(_)));
        let mut c = compile_spec(&dir, csv_spec(&f, json!({ "on_miss": "drop" }))).unwrap();
        assert!(matches!(apply(&mut c, "nope"), StepResult::Filtered));
        assert!(matches!(apply(&mut c, "robot-01"), StepResult::Keep(_)));
    }

    #[test]
    fn yaml_list_and_map_forms() {
        let dir = tempfile::tempdir().unwrap();
        let list = write(
            &dir,
            "r.yaml",
            "- {robot_id: robot-01, model: A1, payload: 12.5}\n- {robot_id: robot-02, model: B2}\n",
        );
        let mut c = compile_spec(&dir, csv_spec(&list, json!({}))).unwrap();
        let StepResult::Keep(e) = apply(&mut c, "robot-01") else {
            panic!()
        };
        assert_eq!(
            e.payload().as_record().unwrap().get("payload_kg"),
            Some(&Value::F32(12.5))
        );
        let StepResult::Keep(e) = apply(&mut c, "robot-02") else {
            panic!()
        };
        assert_eq!(
            e.payload().as_record().unwrap().get("payload_kg"),
            Some(&Value::Null)
        );

        let map = write(&dir, "m.yml", "robot-01: {model: A1, payload: 3}\n");
        let mut c = compile_spec(&dir, csv_spec(&map, json!({}))).unwrap();
        let StepResult::Keep(e) = apply(&mut c, "robot-01") else {
            panic!()
        };
        assert_eq!(
            e.payload()
                .as_record()
                .unwrap()
                .get("model")
                .and_then(Value::as_str),
            Some("A1")
        );
    }

    #[test]
    fn integer_fields_can_be_keys() {
        let dir = tempfile::tempdir().unwrap();
        let f = write(&dir, "n.csv", "n,label\n1,one\n2,two\n");
        let mut c = compile_spec(
            &dir,
            json!({ "file": f, "key": { "field": "n", "column": "n" },
                    "add": [{ "name": "label", "type": "str" }] }),
        )
        .unwrap();
        let StepResult::Keep(e) = apply(&mut c, "r") else {
            panic!()
        };
        assert_eq!(
            e.payload()
                .as_record()
                .unwrap()
                .get("label")
                .and_then(Value::as_str),
            Some("one")
        );
    }

    #[test]
    fn load_errors_are_compile_errors() {
        let dir = tempfile::tempdir().unwrap();
        let good = write(&dir, "g.csv", CSV);
        let msg = |r: Result<Compiled, Vec<ThadamError>>| r.err().unwrap()[0].message.clone();

        assert!(
            msg(compile_spec(&dir, csv_spec("missing.csv", json!({})))).contains("cannot read")
        );
        let dup = write(&dir, "d.csv", "robot_id,model,payload\na,x,1\na,y,2\n");
        assert!(msg(compile_spec(&dir, csv_spec(&dup, json!({})))).contains("duplicate key"));
        let nocol = write(&dir, "c.csv", "robot_id,model\na,x\n");
        assert!(
            msg(compile_spec(&dir, csv_spec(&nocol, json!({})))).contains("no column `payload`")
        );
        let badnum = write(&dir, "n.csv", "robot_id,model,payload\na,x,heavy\n");
        assert!(msg(compile_spec(&dir, csv_spec(&badnum, json!({})))).contains("not a number"));
        let txt = write(&dir, "t.txt", "x");
        assert!(msg(compile_spec(&dir, csv_spec(&txt, json!({})))).contains("format"));

        let limits = crate::limits::ChainLimits {
            max_enrichment_bytes: 10,
            ..Default::default()
        };
        let spec: EnrichSpec = serde_json::from_value(csv_spec(&good, json!({}))).unwrap();
        let e = crate::testutil::with_ctx_full(&limits, Some(dir.path()), |ctx| {
            compile(&spec, ctx, &input(), "t")
        })
        .err()
        .unwrap();
        assert!(e[0].message.contains("limit"));
    }

    #[cfg(unix)]
    #[test]
    fn device_files_are_read_with_a_bound() {
        let dir = tempfile::tempdir().unwrap();
        let spec = csv_spec("/dev/zero", json!({ "format": "csv" }));
        let e = compile_spec(&dir, spec).err().unwrap();
        assert!(e[0].message.contains("larger than the limit"));
    }

    #[test]
    fn yaml_alias_bombs_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let bomb = "a: &a [x,x,x,x,x,x,x,x,x]\nb: &b [*a,*a,*a,*a,*a,*a,*a,*a,*a]\nc: &c [*b,*b,*b,*b,*b,*b,*b,*b,*b]\nd: &d [*c,*c,*c,*c,*c,*c,*c,*c,*c]\ne: &e [*d,*d,*d,*d,*d,*d,*d,*d,*d]\nf: &f [*e,*e,*e,*e,*e,*e,*e,*e,*e]\ng: &g [*f,*f,*f,*f,*f,*f,*f,*f,*f]\nh: &h [*g,*g,*g,*g,*g,*g,*g,*g,*g]\ni: [*h,*h,*h,*h,*h,*h,*h,*h,*h]\n";
        let f = write(&dir, "bomb.yaml", bomb);
        assert!(compile_spec(&dir, csv_spec(&f, json!({}))).is_err());
    }

    #[test]
    fn spec_errors() {
        let dir = tempfile::tempdir().unwrap();
        let f = write(&dir, "robots.csv", CSV);
        let clash = json!({ "file": f, "key": { "field": "robot", "column": "robot_id" },
                            "add": [{ "name": "n", "type": "str", "column": "model" }] });
        assert!(compile_spec(&dir, clash).is_err());
        let bad_key = json!({ "file": f, "key": { "field": "n_missing", "column": "robot_id" },
                              "add": [{ "name": "m", "type": "str", "column": "model" }] });
        assert!(compile_spec(&dir, bad_key).is_err());
        let list_key = shape(&[("l", FieldType::List(Box::new(FieldType::F32)))]);
        let spec: EnrichSpec = serde_json::from_value(json!({
            "file": f, "key": { "field": "l", "column": "robot_id" },
            "add": [{ "name": "m", "type": "str", "column": "model" }] }))
        .unwrap();
        let r = crate::testutil::with_ctx_full(&ChainLimits::default(), Some(dir.path()), |ctx| {
            compile(&spec, ctx, &list_key, "t")
        });
        assert!(r.is_err());
    }
}
