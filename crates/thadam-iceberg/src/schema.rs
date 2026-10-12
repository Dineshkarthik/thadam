//! Segment footer → Iceberg schema, and the table-assignability check.
//!
//! The Parquet footer is the single source of truth: `thadam-arrow` writes
//! the Arrow schema with `PARQUET:field_id` metadata into it, so a ranged
//! tail read plus the conversions inside the `iceberg` crate's Arrow-58
//! tree (`parquet` 58 footer → Arrow schema →
//! [`iceberg::arrow::arrow_schema_to_schema`]) rebuild the Iceberg schema
//! without any hand-maintained copy of the envelope/declared mapping. No
//! Arrow types cross the crate boundary: the workspace builds Arrow 60 while
//! `iceberg` builds Arrow 58, so the conversion can only live here.
//!
//! shortcut: the schema is checked once per dataset per process (on first
//! contact), not per file; step 2 (schema evolution) re-derives and
//! compares per file. Data files store every field the table was created
//! from, so per-file checks add nothing until evolution exists.

use std::collections::HashSet;

use iceberg::spec::{ListType, MapType, NestedField, StructType, Type};

use crate::error::IcebergError;
use crate::storage::S3Io;

/// Reads one segment's Parquet footer through the table's storage (two
/// ranged GETs: the 8-byte tail, then the footer) and converts the embedded
/// Arrow schema to an Iceberg schema. Conversion failures (for example
/// `UInt64`, which the `unsigned: native` envelope writes for `_wal_epoch`
/// and `_wal_pos`) are [`IcebergError::UnsupportedType`] with the remedy in
/// the message.
pub(crate) async fn iceberg_schema_of(
    io: &S3Io,
    uri: &str,
) -> Result<iceberg::spec::Schema, IcebergError> {
    let size = io.size_of(uri).await?;
    if size < 8 {
        return Err(IcebergError::Internal(format!(
            "{uri}: too small to be a Parquet file ({size} bytes)"
        )));
    }
    let mut tail = [0u8; 8];
    tail.copy_from_slice(&io.read_range(uri, (size - 8)..size).await?);
    // Parquet tail: 4-byte little-endian footer length, then the "PAR1" magic.
    if &tail[4..] != b"PAR1" {
        return Err(footer_error(uri, "bad Parquet magic in the 8-byte tail"));
    }
    let footer_len = u32::from_le_bytes(tail[..4].try_into().unwrap_or([0; 4])) as u64;

    let start = size.saturating_sub(8 + footer_len);
    let body = io.read_range(uri, start..(size - 8)).await?;
    let body = if body.len() as u64 > footer_len {
        &body[(body.len() - footer_len as usize)..]
    } else {
        &body[..]
    };
    let meta = parquet58::file::metadata::ParquetMetaDataReader::decode_metadata(body)
        .map_err(|e| footer_error(uri, e))?;
    let arrow_schema = parquet58::arrow::parquet_to_arrow_schema(
        meta.file_metadata().schema_descr(),
        meta.file_metadata().key_value_metadata(),
    )
    .map_err(|e| footer_error(uri, e))?;
    iceberg::arrow::arrow_schema_to_schema(&arrow_schema)
        .map_err(|e| IcebergError::UnsupportedType(e.to_string()))
}

fn footer_error(uri: &str, e: impl std::fmt::Display) -> IcebergError {
    // A footer that does not decode contradicts the shipper's checksum
    // verification of the same bytes: stop loudly, never guess a schema.
    IcebergError::Internal(format!("{uri}: Parquet footer does not decode: {e}"))
}

/// Every column of the file schema must be readable through the table
/// schema: present by field ID with a type equal to or promotable from the
/// file type (`int` → `long`, `float` → `double`), recursively through
/// structs, list elements, and map key/values (their child IDs compared
/// too). Required table fields must be present (and required) in the file —
/// a nullable file column must not null into them. Returns every mismatch.
pub(crate) fn schema_mismatches(
    table: &iceberg::spec::Schema,
    file: &iceberg::spec::Schema,
) -> Vec<String> {
    let mut out = Vec::new();
    let table_ids: HashSet<i32> = table.as_struct().fields().iter().map(|f| f.id).collect();
    for field in file.as_struct().fields() {
        let Some(existing) = table.as_struct().field_by_id(field.id) else {
            out.push(format!(
                "table is missing the file column `{}` (id {})",
                field.name, field.id
            ));
            continue;
        };
        type_mismatches(&mut out, &field.name, existing, field);
    }
    for field in table.as_struct().fields() {
        if !table_ids.contains(&field.id) {
            continue;
        }
        if field.required
            && file
                .as_struct()
                .field_by_id(field.id)
                .is_none_or(|f| !f.required)
        {
            out.push(format!(
                "table column `{}` (id {}) is required but the segment file has it optional or missing",
                field.name, field.id
            ));
        }
    }
    out
}

fn type_mismatches(out: &mut Vec<String>, path: &str, table: &NestedField, file: &NestedField) {
    if table.field_type.as_ref() == file.field_type.as_ref() {
        return;
    }
    match (table.field_type.as_ref(), file.field_type.as_ref()) {
        (Type::Primitive(t), Type::Primitive(f)) => {
            use iceberg::spec::PrimitiveType as P;
            let promotable = matches!((f, t), (P::Int, P::Long) | (P::Float, P::Double));
            if !promotable {
                out.push(format!(
                    "column `{path}` (id {}) has file type `{:?}` but the table has `{:?}`",
                    table.id, f, t
                ));
            }
        }
        (Type::Struct(t), Type::Struct(f)) => struct_mismatches(out, path, t, f),
        (Type::List(t), Type::List(f)) => list_mismatches(out, path, t, f),
        (Type::Map(t), Type::Map(f)) => map_mismatches(out, path, t, f),
        (t, f) => out.push(format!(
            "column `{path}` (id {}) has file type `{:?}` but the table has `{:?}`",
            table.id, f, t
        )),
    }
}

fn struct_mismatches(out: &mut Vec<String>, path: &str, table: &StructType, file: &StructType) {
    for field in file.fields() {
        let path = format!("{path}.{}", field.name);
        let Some(existing) = table.field_by_id(field.id) else {
            out.push(format!(
                "table is missing the file column `{path}` (id {})",
                field.id
            ));
            continue;
        };
        type_mismatches(out, &path, existing, field);
    }
}

fn list_mismatches(out: &mut Vec<String>, path: &str, table: &ListType, file: &ListType) {
    let (t, f) = (&table.element_field, &file.element_field);
    if t.id != f.id {
        out.push(format!(
            "list `{path}` has element id {} in the table but {} in the file",
            t.id, f.id
        ));
        return;
    }
    type_mismatches(out, &format!("{path}.element"), t, f);
}

fn map_mismatches(out: &mut Vec<String>, path: &str, table: &MapType, file: &MapType) {
    for (kind, t, f) in [
        ("key", &table.key_field, &file.key_field),
        ("value", &table.value_field, &file.value_field),
    ] {
        if t.id != f.id {
            out.push(format!(
                "map `{path}` has {kind} id {} in the table but {} in the file",
                t.id, f.id
            ));
            continue;
        }
        type_mismatches(out, &format!("{path}.{kind}"), t, f);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, NestedFieldRef, PrimitiveType, Schema, Type};

    use super::*;

    fn schema(fields: Vec<NestedField>) -> Schema {
        let fields: Vec<NestedFieldRef> = fields.into_iter().map(Arc::new).collect();
        Schema::builder()
            .with_fields(fields)
            .build()
            .expect("schema")
    }

    fn st(fields: Vec<NestedField>) -> Type {
        let fields: Vec<NestedFieldRef> = fields.into_iter().map(Arc::new).collect();
        Type::Struct(StructType::new(fields))
    }

    fn list(element_id: i32) -> Type {
        Type::List(ListType::new(Arc::new(NestedField::optional(
            element_id,
            "element",
            Type::Primitive(PrimitiveType::Int),
        ))))
    }

    #[test]
    fn identical_schemas_pass() {
        let s = schema(vec![
            NestedField::required(2, "ts", Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::required(10, "wal_epoch", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(1000, "temp", Type::Primitive(PrimitiveType::Float)),
        ]);
        assert!(schema_mismatches(&s, &s).is_empty());
    }

    #[test]
    fn int_long_and_float_double_promotions_pass() {
        let table = schema(vec![NestedField::required(
            1,
            "n",
            Type::Primitive(PrimitiveType::Long),
        )]);
        let file = schema(vec![NestedField::required(
            1,
            "n",
            Type::Primitive(PrimitiveType::Int),
        )]);
        assert!(schema_mismatches(&table, &file).is_empty(), "int→long");
        let table = schema(vec![NestedField::optional(
            1,
            "n",
            Type::Primitive(PrimitiveType::Double),
        )]);
        let file = schema(vec![NestedField::optional(
            1,
            "n",
            Type::Primitive(PrimitiveType::Float),
        )]);
        assert!(schema_mismatches(&table, &file).is_empty(), "float→double");
    }

    #[test]
    fn reports_missing_narrowing_and_required() {
        let table = schema(vec![
            NestedField::required(1, "a", Type::Primitive(PrimitiveType::Int)),
            NestedField::required(2, "b", Type::Primitive(PrimitiveType::Int)),
        ]);
        let file = schema(vec![
            NestedField::optional(1, "a", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(3, "c", Type::Primitive(PrimitiveType::String)),
        ]);
        let mismatches = schema_mismatches(&table, &file);
        assert!(
            mismatches
                .iter()
                .any(|m| m.contains("missing the file column `c`")),
            "{mismatches:?}"
        );
        assert!(
            mismatches.iter().any(|m| m.contains("file type `Long`")),
            "long never narrows into int: {mismatches:?}"
        );
        assert!(
            mismatches.iter().any(|m| m.contains("is required")),
            "nullable file column must not null into a required table column: {mismatches:?}"
        );
    }

    #[test]
    fn struct_children_are_compared_by_id() {
        let table = schema(vec![NestedField::optional(
            1000,
            "rec",
            st(vec![NestedField::optional(
                8_388_608,
                "x",
                Type::Primitive(PrimitiveType::Int),
            )]),
        )]);
        let file = schema(vec![NestedField::optional(
            1000,
            "rec",
            st(vec![NestedField::optional(
                8_388_608,
                "x",
                Type::Primitive(PrimitiveType::Long),
            )]),
        )]);
        let mismatches = schema_mismatches(&table, &file);
        assert_eq!(mismatches.len(), 1, "{mismatches:?}");
        assert!(mismatches[0].contains("rec.x"), "{mismatches:?}");
    }

    #[test]
    fn list_element_ids_must_match() {
        let table = schema(vec![NestedField::optional(1000, "vals", list(8_388_608))]);
        let file = schema(vec![NestedField::optional(1000, "vals", list(8_388_609))]);
        let mismatches = schema_mismatches(&table, &file);
        assert_eq!(mismatches.len(), 1, "{mismatches:?}");
    }
}
