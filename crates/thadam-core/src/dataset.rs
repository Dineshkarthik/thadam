//! Declared datasets, handed to connectors at build time.
//!
//! A pipeline declares each dataset once in its configuration; the runtime
//! compiles the declaration into these connector-facing types and attaches
//! them to the [`BuildContext`](crate::BuildContext) of the sinks that
//! consume the dataset, so a segment sink never re-declares schemas in its
//! own configuration. Only `thadam-core` types appear here: no configuration
//! format, no Arrow.

use crate::shape::FieldDef;
use crate::text::Sym;

/// An identity level of a declared dataset.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct LevelDecl {
    /// The level name; becomes a column of the same name.
    pub name: Sym,
    /// The stable Parquet field ID of the level's column (the identity-level
    /// range, 100 to 199).
    pub id: u32,
}

impl LevelDecl {
    /// A level `name` with field ID `id`.
    pub fn new(name: impl Into<Sym>, id: u32) -> Self {
        LevelDecl {
            name: name.into(),
            id,
        }
    }
}

/// One dataset declared at pipeline level, restricted to what a sink needs:
/// the schema fields with their stable IDs, the identity levels with theirs,
/// and whether messages explode into one row per sample.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DatasetDecl {
    /// The dataset id; also the output table name.
    pub id: Sym,
    /// The declared fields in declaration order, each carrying its explicit
    /// `field_id` (the declared top-level range, 1000 to 4,194,303).
    pub fields: Vec<FieldDef>,
    /// The identity levels in the order they are written.
    pub levels: Vec<LevelDecl>,
    /// Whether the dataset explodes messages into one event per sample
    /// (carrying `sample_idx`).
    pub exploded: bool,
}

impl DatasetDecl {
    /// A declaration for dataset `id`.
    pub fn new(
        id: impl Into<Sym>,
        fields: Vec<FieldDef>,
        levels: Vec<LevelDecl>,
        exploded: bool,
    ) -> Self {
        DatasetDecl {
            id: id.into(),
            fields,
            levels,
            exploded,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shape::FieldType;

    #[test]
    fn decls_construct_and_clone() {
        let d = DatasetDecl::new(
            "telemetry",
            vec![FieldDef::new("soc", FieldType::F32).with_field_id(1000)],
            vec![LevelDecl::new("site", 100)],
            true,
        );
        assert_eq!(&*d.id, "telemetry");
        assert_eq!(d.fields[0].field_id, Some(1000));
        assert_eq!(d.levels[0].id, 100);
        assert!(d.exploded);
        let cloned = d.clone();
        assert_eq!(cloned, d);
    }
}
