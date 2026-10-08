//! Structured filter predicates (ADR-0018).
//!
//! ```yaml
//! where: { field: quality.status, eq: good }
//! where:
//!   all:
//!     - { field: level, ne: debug }
//!     - any:
//!         - { field: robot, ne: robot-03 }
//!         - { field: soc_pct, min: 10 }
//! ```
//!
//! `all`, `any`, and `not` combine conditions. A condition names a `field` and
//! one test: `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `null`, or a `min`/`max`
//! range (inclusive; either or both). `null: true` is true when the value is
//! null or missing, `null: false` when it is present and not null. Integer
//! literals above `i64::MAX` are kept as `u64`, never widened to a float.
//!
//! This module only represents predicates. Evaluation lives in
//! `thadam-transform`; the key spellings here are what PR9 compiles.

use std::borrow::Cow;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

/// A literal in a predicate.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Literal {
    /// `true` or `false`.
    Bool(bool),
    /// An integer that fits `i64`.
    I64(i64),
    /// An integer above `i64::MAX`.
    U64(u64),
    /// A finite floating-point number.
    F64(f64),
    /// A string.
    Str(String),
}

impl Literal {
    /// Whether the literal is a number.
    pub fn is_numeric(&self) -> bool {
        matches!(self, Literal::I64(_) | Literal::U64(_) | Literal::F64(_))
    }

    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Literal::Bool(_) => "a boolean",
            Literal::I64(_) | Literal::U64(_) => "an integer",
            Literal::F64(_) => "a float",
            Literal::Str(_) => "a string",
        }
    }
}

impl Serialize for Literal {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Literal::Bool(b) => s.serialize_bool(*b),
            Literal::I64(n) => s.serialize_i64(*n),
            Literal::U64(n) => s.serialize_u64(*n),
            Literal::F64(n) => s.serialize_f64(*n),
            Literal::Str(t) => s.serialize_str(t),
        }
    }
}

/// The comparison in a [`Condition`].
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Test {
    /// `eq: v`
    Eq(Literal),
    /// `ne: v`
    Ne(Literal),
    /// `lt: v`
    Lt(Literal),
    /// `le: v`
    Le(Literal),
    /// `gt: v`
    Gt(Literal),
    /// `ge: v`
    Ge(Literal),
    /// `in: [v, ...]`
    In(Vec<Literal>),
    /// `min: a` and/or `max: b`, inclusive.
    Between {
        /// The inclusive lower bound.
        min: Option<Literal>,
        /// The inclusive upper bound.
        max: Option<Literal>,
    },
    /// `null: true` tests that the value is null or missing; `null: false`
    /// that it is present and not null.
    IsNull(bool),
}

/// A test on one field.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Condition {
    /// A dataset field, an identity level, or `quality.status` / `quality.flags` / `quality.reason`.
    pub field: String,
    /// What is checked.
    pub test: Test,
}

/// A structured filter predicate.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Predicate {
    /// True when every member is true.
    All(Vec<Predicate>),
    /// True when any member is true.
    Any(Vec<Predicate>),
    /// True when the member is false.
    Not(Box<Predicate>),
    /// A single condition.
    Condition(Condition),
}

impl Serialize for Predicate {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Predicate::All(v) => {
                let mut m = s.serialize_map(Some(1))?;
                m.serialize_entry("all", v)?;
                m.end()
            }
            Predicate::Any(v) => {
                let mut m = s.serialize_map(Some(1))?;
                m.serialize_entry("any", v)?;
                m.end()
            }
            Predicate::Not(p) => {
                let mut m = s.serialize_map(Some(1))?;
                m.serialize_entry("not", p)?;
                m.end()
            }
            Predicate::Condition(c) => {
                let mut m = s.serialize_map(None)?;
                m.serialize_entry("field", &c.field)?;
                match &c.test {
                    Test::Eq(l) => m.serialize_entry("eq", l)?,
                    Test::Ne(l) => m.serialize_entry("ne", l)?,
                    Test::Lt(l) => m.serialize_entry("lt", l)?,
                    Test::Le(l) => m.serialize_entry("le", l)?,
                    Test::Gt(l) => m.serialize_entry("gt", l)?,
                    Test::Ge(l) => m.serialize_entry("ge", l)?,
                    Test::In(ls) => m.serialize_entry("in", ls)?,
                    Test::IsNull(b) => m.serialize_entry("null", b)?,
                    Test::Between { min, max } => {
                        if let Some(l) = min {
                            m.serialize_entry("min", l)?;
                        }
                        if let Some(l) = max {
                            m.serialize_entry("max", l)?;
                        }
                    }
                }
                m.end()
            }
        }
    }
}

impl JsonSchema for Predicate {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("Predicate")
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let me = generator.subschema_for::<Predicate>();
        let literal = json_schema!({ "type": ["boolean", "integer", "number", "string"] });
        let number = json_schema!({ "type": "number" });
        json_schema!({
            "description": "A structured predicate: `all`, `any`, `not`, or a condition on one field.",
            "oneOf": [
                {
                    "type": "object",
                    "properties": { "all": { "type": "array", "items": me, "minItems": 1 } },
                    "required": ["all"],
                    "additionalProperties": false
                },
                {
                    "type": "object",
                    "properties": { "any": { "type": "array", "items": me, "minItems": 1 } },
                    "required": ["any"],
                    "additionalProperties": false
                },
                {
                    "type": "object",
                    "properties": { "not": me },
                    "required": ["not"],
                    "additionalProperties": false
                },
                {
                    "type": "object",
                    "properties": {
                        "field": { "type": "string" },
                        "eq": literal,
                        "ne": literal,
                        "lt": number,
                        "le": number,
                        "gt": number,
                        "ge": number,
                        "in": { "type": "array", "items": literal, "minItems": 1 },
                        "min": number,
                        "max": number,
                        "null": { "type": "boolean" }
                    },
                    "required": ["field"],
                    "additionalProperties": false
                }
            ]
        })
    }
}
