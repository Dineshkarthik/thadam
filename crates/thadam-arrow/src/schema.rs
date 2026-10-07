//! The dataset schema: declared fields plus envelope options, and the Arrow
//! schema derived from them (ADR-0013).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use thadam_core::{
    FieldDef, FieldType, MAX_ASSET_LEVELS, MAX_SHAPE_FIELDS, ShapeRef, Sym, ValueType,
};

use crate::columns::{
    self, Column, DATASET_META_KEY, FIRST_DECLARED_ID, LAST_DECLARED_ID, LEVEL_ID_RANGE,
    MAX_NESTED_FIELDS, NESTED_ID_BASE, PARQUET_FIELD_ID_META_KEY, SCHEMA_FP_META_KEY,
    UNIT_META_KEY,
};
use crate::error::SchemaError;
use crate::fingerprint;
use crate::options::{ArrowOptions, TsUnit, UnsignedPolicy};

/// Deepest list and record nesting a declared field may have.
const MAX_NESTING_DEPTH: usize = 6;

/// The Arrow storage type of a scalar after the unsigned policy and the
/// timestamp unit are applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Storage {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Utf8,
    Binary,
    TsMicros,
    TsNanos,
}

impl Storage {
    pub(crate) fn data_type(self) -> DataType {
        match self {
            Storage::Bool => DataType::Boolean,
            Storage::I8 => DataType::Int8,
            Storage::I16 => DataType::Int16,
            Storage::I32 => DataType::Int32,
            Storage::I64 => DataType::Int64,
            Storage::U8 => DataType::UInt8,
            Storage::U16 => DataType::UInt16,
            Storage::U32 => DataType::UInt32,
            Storage::U64 => DataType::UInt64,
            Storage::F32 => DataType::Float32,
            Storage::F64 => DataType::Float64,
            Storage::Utf8 => DataType::Utf8,
            Storage::Binary => DataType::Binary,
            Storage::TsMicros => timestamp(TimeUnit::Microsecond),
            Storage::TsNanos => timestamp(TimeUnit::Nanosecond),
        }
    }

    /// The inclusive value range of an integer storage type.
    pub(crate) fn int_range(self) -> Option<(i128, i128)> {
        Some(match self {
            Storage::I8 => (i128::from(i8::MIN), i128::from(i8::MAX)),
            Storage::I16 => (i128::from(i16::MIN), i128::from(i16::MAX)),
            Storage::I32 => (i128::from(i32::MIN), i128::from(i32::MAX)),
            Storage::I64 => (i128::from(i64::MIN), i128::from(i64::MAX)),
            Storage::U8 => (0, i128::from(u8::MAX)),
            Storage::U16 => (0, i128::from(u16::MAX)),
            Storage::U32 => (0, i128::from(u32::MAX)),
            Storage::U64 => (0, i128::from(u64::MAX)),
            _ => return None,
        })
    }
}

pub(crate) fn timestamp(unit: TimeUnit) -> DataType {
    DataType::Timestamp(unit, Some("UTC".into()))
}

pub(crate) fn timestamp_for(unit: TsUnit) -> DataType {
    match unit {
        TsUnit::Micros => timestamp(TimeUnit::Microsecond),
        TsUnit::Nanos => timestamp(TimeUnit::Nanosecond),
    }
}

/// A scalar declared type and where it is stored.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ScalarNode {
    pub(crate) decl: ValueType,
    pub(crate) storage: Storage,
}

/// The type tree of a declared field, with every nested Arrow field (and its
/// ID) already built so builders and the schema share them.
#[derive(Clone, Debug)]
pub(crate) enum Node {
    Scalar(ScalarNode),
    List {
        child: Box<Child>,
        /// The typed-array variant that converts into this list without
        /// per-element dispatch, when the element is `f32`, `f64`, `i32`, or
        /// `i64`.
        typed: Option<ValueType>,
    },
    Struct {
        shape: ShapeRef,
        children: Vec<Child>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct Child {
    pub(crate) field: FieldRef,
    pub(crate) node: Node,
}

impl Node {
    fn data_type(&self) -> DataType {
        match self {
            Node::Scalar(s) => s.storage.data_type(),
            Node::List { child, .. } => DataType::List(Arc::clone(&child.field)),
            Node::Struct { children, .. } => {
                DataType::Struct(children.iter().map(|c| Arc::clone(&c.field)).collect())
            }
        }
    }
}

/// A declared top-level column.
#[derive(Clone, Debug)]
pub(crate) struct DeclaredColumn {
    pub(crate) name: Sym,
    pub(crate) child: Child,
}

/// An identity level column: a name and its explicit field ID.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IdentityLevel {
    /// The level name, such as `site` or `robot`. Also the column name.
    pub name: Sym,
    /// The column's field ID, in [`LEVEL_ID_RANGE`].
    pub field_id: u32,
}

impl IdentityLevel {
    /// A level column with an explicit ID.
    pub fn new(name: impl Into<Sym>, field_id: u32) -> Self {
        IdentityLevel {
            name: name.into(),
            field_id,
        }
    }
}

/// The schema of one dataset's Parquet files and Arrow batches.
///
/// Built from the declared fields (types come from `thadam-core`, so there is
/// no dependency on `thadam-config`), the identity levels, and
/// [`ArrowOptions`]. Immutable once built and cheap to share behind an `Arc`.
///
/// # Columns, in order
///
/// 1. `_event_id`, `FixedSizeBinary(16)` (lineage columns only)
/// 2. `_ingest_time`, `Timestamp(unit, UTC)`, never null
/// 3. `_source_time`, nullable timestamp
/// 4. `_event_time`, nullable timestamp
/// 5. `_time_domain`, `Dictionary(Int8, Utf8)`
/// 6. `_quality_status`, `Dictionary(Int8, Utf8)`
/// 7. `_quality_flags`, `UInt32`
/// 8. `_quality_reason`, nullable `Utf8`
/// 9. `_clock_synced`, `Boolean`
/// 10. `_wal_epoch`, `UInt64` (lineage columns only)
/// 11. `_wal_pos`, `UInt64` (lineage columns only)
/// 12. one nullable `Dictionary(Int32, Utf8)` column per identity level
/// 13. `sample_idx`, nullable `UInt16` (exploded datasets only)
/// 14. the declared fields, in declaration order
///
/// Every column carries a `PARQUET:field_id` (see [`columns`]).
///
/// [`columns`]: crate::columns
#[derive(Debug)]
pub struct DatasetSchema {
    name: Sym,
    options: ArrowOptions,
    exploded: bool,
    levels: Vec<IdentityLevel>,
    declared: Vec<DeclaredColumn>,
    arrow: SchemaRef,
    fingerprint: [u8; 16],
}

impl DatasetSchema {
    /// Starts building the schema of dataset `name`.
    pub fn builder(name: impl Into<Sym>) -> DatasetSchemaBuilder {
        DatasetSchemaBuilder {
            name: name.into(),
            options: ArrowOptions::default(),
            exploded: false,
            levels: Vec::new(),
            fields: Vec::new(),
        }
    }

    /// The dataset name (`thadam.dataset` in the footer).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The options the schema was built with.
    pub fn options(&self) -> &ArrowOptions {
        &self.options
    }

    /// Whether the dataset carries `sample_idx`.
    pub fn is_exploded(&self) -> bool {
        self.exploded
    }

    /// The identity level columns, in order.
    pub fn levels(&self) -> &[IdentityLevel] {
        &self.levels
    }

    /// The Arrow schema, with field IDs on every field and `thadam.dataset` and
    /// `thadam.schema_fp` in the schema metadata. The handle is shared.
    pub fn arrow_schema(&self) -> SchemaRef {
        Arc::clone(&self.arrow)
    }

    /// A stable 128-bit fingerprint of the output columns: names, types,
    /// nullability, field IDs, units, and unit and policy effects.
    ///
    /// The encoding is versioned and independent of Arrow's own formatting, so
    /// it is stable across processes and Arrow upgrades. Column order is part
    /// of it; the dataset name is not. This is `thadam.schema_fp`, as
    /// [`DatasetSchema::fingerprint_hex`].
    pub fn fingerprint(&self) -> [u8; 16] {
        self.fingerprint
    }

    /// [`DatasetSchema::fingerprint`] as 32 lowercase hex digits.
    pub fn fingerprint_hex(&self) -> String {
        fingerprint::to_hex(&self.fingerprint)
    }

    pub(crate) fn declared(&self) -> &[DeclaredColumn] {
        &self.declared
    }
}

/// Builds a [`DatasetSchema`], validating names, IDs, and types.
#[derive(Debug)]
pub struct DatasetSchemaBuilder {
    name: Sym,
    options: ArrowOptions,
    exploded: bool,
    levels: Vec<IdentityLevel>,
    fields: Vec<FieldDef>,
}

impl DatasetSchemaBuilder {
    /// Sets the Arrow options.
    #[must_use]
    pub fn options(mut self, options: ArrowOptions) -> Self {
        self.options = options;
        self
    }

    /// Marks the dataset as exploded, adding the `sample_idx` column.
    #[must_use]
    pub fn exploded(mut self, exploded: bool) -> Self {
        self.exploded = exploded;
        self
    }

    /// Adds an identity level column with an explicit field ID.
    #[must_use]
    pub fn level(mut self, name: impl Into<Sym>, field_id: u32) -> Self {
        self.levels.push(IdentityLevel::new(name, field_id));
        self
    }

    /// Adds a declared field. It needs an explicit `field_id`.
    #[must_use]
    pub fn field(mut self, field: FieldDef) -> Self {
        self.fields.push(field);
        self
    }

    /// Adds declared fields in order.
    #[must_use]
    pub fn fields(mut self, fields: impl IntoIterator<Item = FieldDef>) -> Self {
        self.fields.extend(fields);
        self
    }

    /// Validates everything and builds the schema.
    pub fn build(self) -> Result<DatasetSchema, SchemaError> {
        let DatasetSchemaBuilder {
            name,
            options,
            exploded,
            levels,
            fields,
        } = self;
        if name.is_empty() {
            return Err(SchemaError::EmptyDatasetName);
        }
        if levels.len() > MAX_ASSET_LEVELS {
            return Err(SchemaError::TooManyLevels(levels.len()));
        }
        if fields.len() > MAX_SHAPE_FIELDS {
            return Err(SchemaError::TooManyFields(fields.len()));
        }

        let mut names: HashSet<&str> = envelope_names(&options, exploded).collect();
        let mut ids: HashMap<u32, &str> = HashMap::new();
        for level in &levels {
            check_name(&level.name, &mut names)?;
            if !LEVEL_ID_RANGE.contains(&level.field_id) {
                return Err(SchemaError::level_id_out_of_range(
                    &level.name,
                    level.field_id,
                ));
            }
            check_id(level.field_id, &level.name, &mut ids)?;
        }
        for def in &fields {
            check_name(&def.name, &mut names)?;
            let id = def
                .field_id
                .ok_or_else(|| SchemaError::MissingFieldId(def.name.to_string()))?;
            if !(FIRST_DECLARED_ID..=LAST_DECLARED_ID).contains(&id) {
                return Err(SchemaError::FieldIdOutOfRange {
                    field: def.name.to_string(),
                    id,
                    min: FIRST_DECLARED_ID,
                    max: LAST_DECLARED_ID,
                });
            }
            check_id(id, &def.name, &mut ids)?;
        }

        let declared = fields
            .iter()
            .map(|def| build_declared(def, &options))
            .collect::<Result<Vec<_>, _>>()?;

        let mut arrow_fields: Vec<FieldRef> = envelope_fields(&options, &levels, exploded);
        arrow_fields.extend(declared.iter().map(|d| Arc::clone(&d.child.field)));

        let fingerprint = fingerprint::of_fields(&arrow_fields);
        let metadata = HashMap::from([
            (DATASET_META_KEY.to_owned(), name.to_string()),
            (
                SCHEMA_FP_META_KEY.to_owned(),
                fingerprint::to_hex(&fingerprint),
            ),
        ]);
        let arrow = Arc::new(Schema::new_with_metadata(arrow_fields, metadata));

        Ok(DatasetSchema {
            name,
            options,
            exploded,
            levels,
            declared,
            arrow,
            fingerprint,
        })
    }
}

fn envelope_names(options: &ArrowOptions, exploded: bool) -> impl Iterator<Item = &'static str> {
    let lineage = options.lineage_columns;
    [
        (columns::EVENT_ID, lineage),
        (columns::INGEST_TIME, true),
        (columns::SOURCE_TIME, true),
        (columns::EVENT_TIME, true),
        (columns::TIME_DOMAIN, true),
        (columns::QUALITY_STATUS, true),
        (columns::QUALITY_FLAGS, true),
        (columns::QUALITY_REASON, true),
        (columns::CLOCK_SYNCED, true),
        (columns::WAL_EPOCH, lineage),
        (columns::WAL_POS, lineage),
        (columns::SAMPLE_IDX, exploded),
    ]
    .into_iter()
    .filter_map(|(c, present)| present.then_some(c.name))
}

fn check_name<'a>(name: &'a str, names: &mut HashSet<&'a str>) -> Result<(), SchemaError> {
    if name.is_empty() {
        return Err(SchemaError::EmptyName);
    }
    if name.starts_with('_') {
        return Err(SchemaError::ReservedName(name.to_owned()));
    }
    if !names.insert(name) {
        return Err(SchemaError::DuplicateName(name.to_owned()));
    }
    Ok(())
}

fn check_id<'a>(
    id: u32,
    name: &'a str,
    ids: &mut HashMap<u32, &'a str>,
) -> Result<(), SchemaError> {
    match ids.insert(id, name) {
        Some(first) => Err(SchemaError::DuplicateFieldId {
            id,
            first: first.to_owned(),
            second: name.to_owned(),
        }),
        None => Ok(()),
    }
}

fn metadata(id: u32, unit: Option<&str>) -> HashMap<String, String> {
    let mut m = HashMap::with_capacity(2);
    m.insert(PARQUET_FIELD_ID_META_KEY.to_owned(), id.to_string());
    if let Some(unit) = unit {
        m.insert(UNIT_META_KEY.to_owned(), unit.to_owned());
    }
    m
}

fn envelope_field(column: Column, data_type: DataType, nullable: bool) -> FieldRef {
    Arc::new(
        Field::new(column.name, data_type, nullable).with_metadata(metadata(column.field_id, None)),
    )
}

fn unsigned_type(policy: UnsignedPolicy, native: Storage, signed: Storage) -> DataType {
    match policy {
        UnsignedPolicy::Native => native,
        UnsignedPolicy::Signed => signed,
    }
    .data_type()
}

/// The storage type of an envelope unsigned column of `bits` logical width.
pub(crate) fn unsigned_storage(policy: UnsignedPolicy, bits: u8) -> Storage {
    match (policy, bits) {
        (UnsignedPolicy::Native, 16) => Storage::U16,
        (UnsignedPolicy::Native, 32) => Storage::U32,
        (UnsignedPolicy::Native, _) => Storage::U64,
        (UnsignedPolicy::Signed, 16) => Storage::I32,
        (UnsignedPolicy::Signed, _) => Storage::I64,
    }
}

fn dictionary(key: DataType) -> DataType {
    DataType::Dictionary(Box::new(key), Box::new(DataType::Utf8))
}

fn envelope_fields(
    options: &ArrowOptions,
    levels: &[IdentityLevel],
    exploded: bool,
) -> Vec<FieldRef> {
    let ts = timestamp_for(options.ts_unit);
    let policy = options.unsigned;
    let mut fields = Vec::with_capacity(16 + levels.len());
    if options.lineage_columns {
        fields.push(envelope_field(
            columns::EVENT_ID,
            DataType::FixedSizeBinary(16),
            false,
        ));
    }
    fields.push(envelope_field(columns::INGEST_TIME, ts.clone(), false));
    fields.push(envelope_field(columns::SOURCE_TIME, ts.clone(), true));
    fields.push(envelope_field(columns::EVENT_TIME, ts, true));
    fields.push(envelope_field(
        columns::TIME_DOMAIN,
        dictionary(DataType::Int8),
        false,
    ));
    fields.push(envelope_field(
        columns::QUALITY_STATUS,
        dictionary(DataType::Int8),
        false,
    ));
    fields.push(envelope_field(
        columns::QUALITY_FLAGS,
        unsigned_type(policy, Storage::U32, Storage::I64),
        false,
    ));
    fields.push(envelope_field(
        columns::QUALITY_REASON,
        DataType::Utf8,
        true,
    ));
    fields.push(envelope_field(
        columns::CLOCK_SYNCED,
        DataType::Boolean,
        false,
    ));
    if options.lineage_columns {
        let epoch_pos = unsigned_type(policy, Storage::U64, Storage::I64);
        fields.push(envelope_field(columns::WAL_EPOCH, epoch_pos.clone(), false));
        fields.push(envelope_field(columns::WAL_POS, epoch_pos, false));
    }
    for level in levels {
        fields.push(Arc::new(
            Field::new(&*level.name, dictionary(DataType::Int32), true)
                .with_metadata(metadata(level.field_id, None)),
        ));
    }
    if exploded {
        fields.push(envelope_field(
            columns::SAMPLE_IDX,
            unsigned_type(policy, Storage::U16, Storage::I32),
            true,
        ));
    }
    fields
}

/// Hands out derived nested field IDs for one top-level field.
struct NestedIds<'a> {
    field: &'a str,
    top_id: u32,
    next_slot: u32,
}

impl NestedIds<'_> {
    fn alloc(&mut self) -> Result<u32, SchemaError> {
        if self.next_slot >= MAX_NESTED_FIELDS {
            return Err(SchemaError::TooManyNestedFields(self.field.to_owned()));
        }
        let id =
            NESTED_ID_BASE + (self.top_id - FIRST_DECLARED_ID) * MAX_NESTED_FIELDS + self.next_slot;
        self.next_slot += 1;
        Ok(id)
    }
}

fn build_declared(def: &FieldDef, options: &ArrowOptions) -> Result<DeclaredColumn, SchemaError> {
    // `build` checked the ID is present and in range.
    let top_id = def.field_id.unwrap_or(FIRST_DECLARED_ID);
    let mut ids = NestedIds {
        field: &def.name,
        top_id,
        next_slot: 0,
    };
    let node = build_node(&def.ty, options, &mut ids, 0)?;
    let field = Arc::new(
        Field::new(&*def.name, node.data_type(), def.nullable)
            .with_metadata(metadata(top_id, def.unit.as_deref())),
    );
    Ok(DeclaredColumn {
        name: Sym::clone(&def.name),
        child: Child { field, node },
    })
}

fn typed_array_of(element: ValueType) -> Option<ValueType> {
    match element {
        ValueType::F32 => Some(ValueType::F32s),
        ValueType::F64 => Some(ValueType::F64s),
        ValueType::I32 => Some(ValueType::I32s),
        ValueType::I64 => Some(ValueType::I64s),
        _ => None,
    }
}

fn scalar(ty: &FieldType, options: &ArrowOptions) -> Option<ScalarNode> {
    let signed = options.unsigned == UnsignedPolicy::Signed;
    let (decl, storage) = match ty {
        FieldType::Bool => (ValueType::Bool, Storage::Bool),
        FieldType::I8 => (ValueType::I8, Storage::I8),
        FieldType::I16 => (ValueType::I16, Storage::I16),
        FieldType::I32 => (ValueType::I32, Storage::I32),
        FieldType::I64 => (ValueType::I64, Storage::I64),
        FieldType::U8 => (
            ValueType::U8,
            if signed { Storage::I16 } else { Storage::U8 },
        ),
        FieldType::U16 => (
            ValueType::U16,
            if signed { Storage::I32 } else { Storage::U16 },
        ),
        FieldType::U32 => (
            ValueType::U32,
            if signed { Storage::I64 } else { Storage::U32 },
        ),
        FieldType::U64 => (
            ValueType::U64,
            if signed { Storage::I64 } else { Storage::U64 },
        ),
        FieldType::F32 => (ValueType::F32, Storage::F32),
        FieldType::F64 => (ValueType::F64, Storage::F64),
        FieldType::Str => (ValueType::Str, Storage::Utf8),
        FieldType::Bytes => (ValueType::Bytes, Storage::Binary),
        FieldType::Ts => (
            ValueType::Ts,
            match options.ts_unit {
                TsUnit::Micros => Storage::TsMicros,
                TsUnit::Nanos => Storage::TsNanos,
            },
        ),
        _ => return None,
    };
    Some(ScalarNode { decl, storage })
}

fn build_node(
    ty: &FieldType,
    options: &ArrowOptions,
    ids: &mut NestedIds<'_>,
    depth: usize,
) -> Result<Node, SchemaError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(SchemaError::TooDeep {
            field: ids.field.to_owned(),
            max: MAX_NESTING_DEPTH,
        });
    }
    match ty {
        FieldType::List(inner) => {
            let id = ids.alloc()?;
            let node = build_node(inner, options, ids, depth + 1)?;
            let typed = match &node {
                Node::Scalar(s) => typed_array_of(s.decl),
                _ => None,
            };
            Ok(Node::List {
                child: Box::new(child("element", node, true, None, id)),
                typed,
            })
        }
        FieldType::Record(shape) => {
            if shape.is_empty() {
                return Err(SchemaError::EmptyRecord(ids.field.to_owned()));
            }
            let mut children = Vec::with_capacity(shape.len());
            for def in shape.fields() {
                let id = ids.alloc()?;
                let node = build_node(&def.ty, options, ids, depth + 1)?;
                children.push(child(
                    &def.name,
                    node,
                    def.nullable,
                    def.unit.as_deref(),
                    id,
                ));
            }
            Ok(Node::Struct {
                shape: shape.clone(),
                children,
            })
        }
        other => scalar(other, options)
            .map(Node::Scalar)
            .ok_or_else(|| SchemaError::UnsupportedType(ids.field.to_owned())),
    }
}

fn child(name: &str, node: Node, nullable: bool, unit: Option<&str>, id: u32) -> Child {
    let field =
        Arc::new(Field::new(name, node.data_type(), nullable).with_metadata(metadata(id, unit)));
    Child { field, node }
}

/// The Parquet field ID stored on an Arrow field, if any.
pub fn field_id(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|id| id.parse().ok())
}
