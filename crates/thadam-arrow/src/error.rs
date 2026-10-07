//! Schema construction errors.

use thadam_core::{Classify, ErrorDomain, Retry, codes};

use crate::columns::{LEVEL_ID_RANGE, MAX_NESTED_FIELDS};

/// Why a [`DatasetSchema`](crate::DatasetSchema) could not be built.
///
/// These are configuration errors: they classify as
/// [`ErrorDomain::Config`], `config.invalid`, permanent.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SchemaError {
    /// The dataset name was empty.
    #[error("dataset name must not be empty")]
    EmptyDatasetName,
    /// A column name was empty.
    #[error("column name must not be empty")]
    EmptyName,
    /// A column name starts with `_`, which is reserved for Thadam.
    #[error("column name `{0}` is reserved: names starting with `_` belong to Thadam")]
    ReservedName(String),
    /// Two columns share a name.
    #[error("duplicate column name `{0}`")]
    DuplicateName(String),
    /// A declared field or identity level has no field ID.
    #[error("field `{0}` has no field id")]
    MissingFieldId(String),
    /// A field ID is outside the range its origin allows.
    #[error("field `{field}` has id {id}, outside the allowed range {min}..={max}")]
    FieldIdOutOfRange {
        /// The field name.
        field: String,
        /// The ID given.
        id: u32,
        /// Lowest allowed ID.
        min: u32,
        /// Highest allowed ID.
        max: u32,
    },
    /// Two columns share a field ID.
    #[error("fields `{first}` and `{second}` share field id {id}")]
    DuplicateFieldId {
        /// The ID both use.
        id: u32,
        /// The first field.
        first: String,
        /// The second field.
        second: String,
    },
    /// More identity levels than an asset path may have.
    #[error("{0} identity levels, the maximum is {max}", max = thadam_core::MAX_ASSET_LEVELS)]
    TooManyLevels(usize),
    /// More declared fields than a shape may have.
    #[error("{0} declared fields, the maximum is {max}", max = thadam_core::MAX_SHAPE_FIELDS)]
    TooManyFields(usize),
    /// A declared type has no Arrow mapping.
    #[error("field `{0}` has a type with no Arrow mapping")]
    UnsupportedType(String),
    /// A record field has no children.
    #[error("field `{0}` is a record with no fields, which Parquet cannot store")]
    EmptyRecord(String),
    /// Lists and records nest deeper than the limit.
    #[error("field `{field}` nests more than {max} levels")]
    TooDeep {
        /// The field name.
        field: String,
        /// The depth limit.
        max: usize,
    },
    /// A field's type tree has too many nested fields to derive IDs for.
    #[error("field `{0}` has more than {MAX_NESTED_FIELDS} nested fields")]
    TooManyNestedFields(String),
}

impl SchemaError {
    pub(crate) fn level_id_out_of_range(field: &str, id: u32) -> Self {
        SchemaError::FieldIdOutOfRange {
            field: field.to_owned(),
            id,
            min: *LEVEL_ID_RANGE.start(),
            max: *LEVEL_ID_RANGE.end(),
        }
    }
}

impl Classify for SchemaError {
    fn domain(&self) -> ErrorDomain {
        ErrorDomain::Config
    }
    fn code(&self) -> &'static str {
        codes::CONFIG_INVALID
    }
    fn retry(&self) -> Retry {
        Retry::Permanent
    }
}
