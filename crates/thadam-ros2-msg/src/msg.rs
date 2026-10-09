//! ROS 2 `.msg` interface definition parsing.
//!
//! Covers the ROS 2 interface-definition grammar: the fifteen primitive
//! types, bounded strings (`string<=N`, `wstring<=N`), fixed arrays `T[N]`,
//! bounded sequences `T[<=N]`, unbounded sequences `T[]`, nested type
//! references (`pkg/Type`, the IDL-style `pkg/msg/Type`, and bare `Type`
//! resolved within the same package), constants, default values, and `#`
//! comments.
//!
//! [`SchemaDocument`] additionally parses the concatenated multi-definition
//! format carried by MCAP/rosbag2 `ros2msg` schemas: the main definition,
//! then one `================================================================================`
//! separator plus `MSG: pkg/Type` header per transitively referenced type.

use std::fmt;

use thadam_core::{ErrorDomain, ThadamError, codes};

/// A fully qualified message type name (`package`, `name`).
///
/// Parsed from `pkg/Type` or the IDL-style `pkg/msg/Type`; the display form
/// is always `pkg/Type`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TypeName {
    package: String,
    name: String,
}

impl TypeName {
    /// A type name from validated parts.
    pub fn new(package: &str, name: &str) -> Result<Self, ThadamError> {
        check_ident(package, "package")?;
        check_ident(name, "message")?;
        Ok(TypeName {
            package: package.to_owned(),
            name: name.to_owned(),
        })
    }

    /// Parses `pkg/Type` or `pkg/msg/Type`.
    pub fn parse(text: &str) -> Result<Self, ThadamError> {
        let text = text.trim();
        let (package, name) = split_type_path(text)
            .ok_or_else(|| invalid_definition(format!("invalid type name `{text}`")))?;
        let package =
            package.ok_or_else(|| invalid_definition(format!("`{text}` has no package")))?;
        Self::new(package, name)
    }

    /// The ROS 2 package.
    pub fn package(&self) -> &str {
        &self.package
    }

    /// The message name within the package.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for TypeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.package, self.name)
    }
}

/// A `.msg` primitive type.
///
/// `byte` and `char` are the ROS aliases: `byte` is signed (`int8` semantics)
/// and `char` is unsigned (`uint8` semantics).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Primitive {
    /// `bool`.
    Bool,
    /// `byte` (ROS alias of `int8`).
    Byte,
    /// `char` (ROS alias of `uint8`).
    Char,
    /// `int8`.
    I8,
    /// `uint8`.
    U8,
    /// `int16`.
    I16,
    /// `uint16`.
    U16,
    /// `int32`.
    I32,
    /// `uint32`.
    U32,
    /// `int64`.
    I64,
    /// `uint64`.
    U64,
    /// `float32`.
    F32,
    /// `float64`.
    F64,
    /// `string` (wired as UTF-8).
    String,
    /// `wstring` (wired as UTF-16, decoded to UTF-8).
    WString,
}

impl Primitive {
    fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "bool" => Primitive::Bool,
            "byte" => Primitive::Byte,
            "char" => Primitive::Char,
            "int8" => Primitive::I8,
            "uint8" => Primitive::U8,
            "int16" => Primitive::I16,
            "uint16" => Primitive::U16,
            "int32" => Primitive::I32,
            "uint32" => Primitive::U32,
            "int64" => Primitive::I64,
            "uint64" => Primitive::U64,
            "float32" => Primitive::F32,
            "float64" => Primitive::F64,
            "string" => Primitive::String,
            "wstring" => Primitive::WString,
            _ => return None,
        })
    }

    /// Whether this is `string` or `wstring`, the types a `<=N` bound
    /// applies to.
    pub fn is_text(self) -> bool {
        matches!(self, Primitive::String | Primitive::WString)
    }

    /// The wire size and alignment in bytes, `None` for the variable-length
    /// text types.
    pub(crate) fn wire_size(self) -> Option<usize> {
        Some(match self {
            Primitive::Bool | Primitive::Byte | Primitive::Char | Primitive::I8 | Primitive::U8 => {
                1
            }
            Primitive::I16 | Primitive::U16 => 2,
            Primitive::I32
            | Primitive::U32
            | Primitive::F32
            | Primitive::String
            | Primitive::WString => 4,
            Primitive::I64 | Primitive::U64 | Primitive::F64 => 8,
        })
    }

    /// The `.msg` spelling.
    pub const fn ros_name(self) -> &'static str {
        match self {
            Primitive::Bool => "bool",
            Primitive::Byte => "byte",
            Primitive::Char => "char",
            Primitive::I8 => "int8",
            Primitive::U8 => "uint8",
            Primitive::I16 => "int16",
            Primitive::U16 => "uint16",
            Primitive::I32 => "int32",
            Primitive::U32 => "uint32",
            Primitive::I64 => "int64",
            Primitive::U64 => "uint64",
            Primitive::F32 => "float32",
            Primitive::F64 => "float64",
            Primitive::String => "string",
            Primitive::WString => "wstring",
        }
    }
}

/// A reference to another message type.
///
/// `package` is `None` for the bare `Type` form, which resolves within the
/// referencing message's own package.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamedType {
    /// The package, if written.
    pub package: Option<String>,
    /// The message name.
    pub name: String,
}

/// A scalar (non-array) type: a primitive, optionally a bounded string, or a
/// nested message reference.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScalarType {
    /// A primitive; `string_bound` is the `string<=N` / `wstring<=N` bound.
    Primitive {
        /// The primitive kind.
        prim: Primitive,
        /// The `<=N` character bound for `string` / `wstring`.
        string_bound: Option<u32>,
    },
    /// A nested message type reference.
    Named(NamedType),
}

/// The bound of an array field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ArrayBound {
    /// `T[N]`: exactly `N` elements, no length prefix on the wire.
    Fixed(u32),
    /// `T[<=N]`: at most `N` elements.
    Bounded(u32),
    /// `T[]`: any number of elements (bounded by the payload itself).
    Unbounded,
}

/// A field type: scalar or array.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TypeKind {
    /// A scalar field.
    Scalar(ScalarType),
    /// An array field.
    Array {
        /// The element type.
        elem: ScalarType,
        /// The array bound.
        bound: ArrayBound,
    },
}

/// A message field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    /// The field name, unique within the message.
    pub name: String,
    /// The field type.
    pub ty: TypeKind,
    /// The raw default-value text (`type name default`), if present. Defaults
    /// are ignored for decoding; CDR always carries every field.
    pub default: Option<String>,
}

/// A parsed constant value. Constants live on the definition and are never
/// part of the decoded record.
#[derive(Clone, Debug, PartialEq)]
pub enum ConstantValue {
    /// A `bool` constant.
    Bool(bool),
    /// A signed-integer constant (`byte`, `int8` .. `int64`).
    I64(i64),
    /// An unsigned-integer constant (`char`, `uint8` .. `uint64`).
    U64(u64),
    /// A floating-point constant (`float32`, `float64`; `float32` values are
    /// parsed exactly and kept as `f64`).
    F64(f64),
    /// A `string` / `wstring` constant, quotes stripped.
    Str(String),
}

/// A `TYPE NAME = value` constant (spaces around `=` are allowed, matching
/// the ROS 2 grammar and real-world definitions such as
/// `sensor_msgs/NavSatStatus`).
#[derive(Clone, Debug, PartialEq)]
pub struct Constant {
    /// The constant name (conventionally `UPPER_CASE`).
    pub name: String,
    /// The declared primitive type.
    pub prim: Primitive,
    /// The parsed value, range-checked against the declared type.
    pub value: ConstantValue,
}

/// A parsed `.msg` definition: ordered fields and constants. Comments are
/// stripped and do not appear here.
#[derive(Clone, Debug, PartialEq)]
pub struct MessageSpec {
    type_name: TypeName,
    fields: Vec<Field>,
    constants: Vec<Constant>,
}

impl MessageSpec {
    /// Parses one `.msg` definition for `type_name`.
    pub fn parse(type_name: &TypeName, text: &str) -> Result<Self, ThadamError> {
        let mut fields = Vec::new();
        let mut constants = Vec::new();
        for (index, raw_line) in text.lines().enumerate() {
            let line_no = index + 1;
            let line = raw_line.split_once('#').map_or(raw_line, |(head, _)| head);
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match parse_line(line).map_err(|e| e.with_context("line", line_no.to_string()))? {
                ParsedLine::Field(field) => {
                    if fields.iter().any(|f: &Field| f.name == field.name)
                        || constants.iter().any(|c: &Constant| c.name == field.name)
                    {
                        return Err(invalid_definition(format!(
                            "duplicate name `{}` in {type_name}",
                            field.name
                        ))
                        .with_context("line", line_no.to_string()));
                    }
                    fields.push(field);
                }
                ParsedLine::Constant(constant) => {
                    if fields.iter().any(|f| f.name == constant.name)
                        || constants.iter().any(|c| c.name == constant.name)
                    {
                        return Err(invalid_definition(format!(
                            "duplicate name `{}` in {type_name}",
                            constant.name
                        ))
                        .with_context("line", line_no.to_string()));
                    }
                    constants.push(constant);
                }
            }
        }
        Ok(MessageSpec {
            type_name: type_name.clone(),
            fields,
            constants,
        })
    }

    /// The message type name.
    pub fn type_name(&self) -> &TypeName {
        &self.type_name
    }

    /// Fields in declaration order.
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Constants in declaration order. Kept for reference; never decoded.
    pub fn constants(&self) -> &[Constant] {
        &self.constants
    }
}

/// A parsed `ros2msg` schema document: the main definition followed by every
/// referenced nested definition, as concatenated by MCAP and rosbag2.
#[derive(Clone, Debug, PartialEq)]
pub struct SchemaDocument {
    /// Sections in document order; the first is the main type.
    sections: Vec<(TypeName, MessageSpec)>,
}

impl SchemaDocument {
    /// Parses `text` for a schema whose main type is `main_type`.
    ///
    /// Both a bare single definition and the concatenated
    /// separator/`MSG:`-section form are accepted. Section files that
    /// duplicate a type, carry a dangling separator, or name a section
    /// header that fails to parse are rejected.
    pub fn parse(main_type: &TypeName, text: &str) -> Result<Self, ThadamError> {
        let mut raw_sections: Vec<(Option<TypeName>, String)> = vec![(None, String::new())];
        let mut expect_header = false;
        for (index, line) in text.lines().enumerate() {
            let line_no = index + 1;
            if is_separator(line) {
                if expect_header {
                    return Err(invalid_definition("dangling definition separator")
                        .with_context("line", line_no.to_string()));
                }
                expect_header = true;
                continue;
            }
            if expect_header {
                if line.trim().is_empty() {
                    continue;
                }
                let declared = line
                    .trim()
                    .strip_prefix("MSG: ")
                    .ok_or_else(|| {
                        invalid_definition("expected `MSG: pkg/Type` after the separator")
                            .with_context("line", line_no.to_string())
                    })?
                    .trim();
                let type_name = TypeName::parse(declared)
                    .map_err(|e| e.with_context("line", line_no.to_string()))?;
                raw_sections.push((Some(type_name), String::new()));
                expect_header = false;
                continue;
            }
            let (_, body) = raw_sections
                .last_mut()
                .ok_or_else(|| invalid_definition("empty schema document"))?;
            body.push_str(line);
            body.push('\n');
        }
        if expect_header {
            return Err(invalid_definition(
                "the document ends with a dangling definition separator",
            ));
        }

        let mut sections = Vec::with_capacity(raw_sections.len());
        for (declared, body) in raw_sections {
            let type_name = declared.unwrap_or_else(|| main_type.clone());
            if sections
                .iter()
                .any(|(seen, _): &(TypeName, MessageSpec)| *seen == type_name)
            {
                return Err(invalid_definition(format!(
                    "the document defines {type_name} twice"
                )));
            }
            let spec = MessageSpec::parse(&type_name, &body)?;
            if type_name == *main_type && spec.fields.is_empty() && !body.trim().is_empty() {
                // Unreachable in practice: a non-empty body parses to at
                // least one field or constant, or fails. Guarded so an
                // all-constants main definition is still accepted below.
            }
            sections.push((type_name, spec));
        }
        let (first, _) = &sections[0];
        if first != main_type {
            return Err(invalid_definition(format!(
                "the first section is {first}, not the main type {main_type}"
            )));
        }
        Ok(SchemaDocument { sections })
    }

    /// Sections in document order; the first is the main type.
    pub fn sections(&self) -> &[(TypeName, MessageSpec)] {
        &self.sections
    }

    /// Consumes the document into its sections.
    pub fn into_sections(self) -> Vec<(TypeName, MessageSpec)> {
        self.sections
    }

    /// The main type of the document.
    pub fn main_type(&self) -> &TypeName {
        &self.sections[0].0
    }
}

enum ParsedLine {
    Field(Field),
    Constant(Constant),
}

fn parse_line(line: &str) -> Result<ParsedLine, ThadamError> {
    if let Some(equal) = line.find('=') {
        let lhs = &line[..equal];
        let tokens: Vec<&str> = lhs.split_whitespace().collect();
        if let [type_text, name] = tokens.as_slice()
            && let TypeKind::Scalar(ScalarType::Primitive { prim, string_bound }) =
                parse_type(type_text)?
        {
            check_ident(name, "constant")?;
            let raw = line[equal + 1..].trim();
            let value = parse_constant_value(prim, string_bound, raw)?;
            return Ok(ParsedLine::Constant(Constant {
                name: (*name).to_owned(),
                prim,
                value,
            }));
        }
        // Otherwise this is a field whose default text contains `=`.
    }
    let (type_text, rest) = split_once_whitespace(line);
    let (name, default) = split_once_whitespace(rest);
    if name.is_empty() {
        return Err(invalid_definition(format!(
            "a field line needs a type and a name: `{line}`"
        )));
    }
    check_ident(name, "field")?;
    let ty = parse_type(type_text)?;
    let default = if default.is_empty() {
        None
    } else {
        Some(default.to_owned())
    };
    Ok(ParsedLine::Field(Field {
        name: name.to_owned(),
        ty,
        default,
    }))
}

fn parse_type(text: &str) -> Result<TypeKind, ThadamError> {
    if text.ends_with(']') {
        let open = text
            .rfind('[')
            .ok_or_else(|| invalid_definition(format!("unbalanced brackets in type `{text}`")))?;
        let base = &text[..open];
        let sig = text[open + 1..text.len() - 1].trim();
        let bound = if sig.is_empty() {
            ArrayBound::Unbounded
        } else if let Some(rest) = sig.strip_prefix("<=") {
            ArrayBound::Bounded(parse_bound(rest.trim(), text)?)
        } else {
            ArrayBound::Fixed(parse_bound(sig, text)?)
        };
        Ok(TypeKind::Array {
            elem: parse_scalar(base)?,
            bound,
        })
    } else {
        Ok(TypeKind::Scalar(parse_scalar(text)?))
    }
}

fn parse_bound(text: &str, whole: &str) -> Result<u32, ThadamError> {
    let bound: u32 = text
        .parse()
        .map_err(|_| invalid_definition(format!("invalid array bound in type `{whole}`")))?;
    if bound == 0 {
        return Err(invalid_definition(format!(
            "array bound must be at least 1 in type `{whole}`"
        )));
    }
    Ok(bound)
}

fn parse_scalar(text: &str) -> Result<ScalarType, ThadamError> {
    let (base, string_bound) = match text.split_once("<=") {
        Some((base, bound)) => {
            let bound: u32 = bound.trim().parse().map_err(|_| {
                invalid_definition(format!("invalid string bound in type `{text}`"))
            })?;
            if bound == 0 {
                return Err(invalid_definition(format!(
                    "string bound must be at least 1 in type `{text}`"
                )));
            }
            (base, Some(bound))
        }
        None => (text, None),
    };
    if let Some(prim) = Primitive::parse(base) {
        if string_bound.is_some() && !prim.is_text() {
            return Err(invalid_definition(format!(
                "a `<=` bound applies to string types only: `{text}`"
            )));
        }
        return Ok(ScalarType::Primitive { prim, string_bound });
    }
    if string_bound.is_some() {
        return Err(invalid_definition(format!(
            "a `<=` bound applies to string types only: `{text}`"
        )));
    }
    let (package, name) = split_type_path(text)
        .ok_or_else(|| invalid_definition(format!("unknown or invalid type `{text}`")))?;
    if let Some(package) = package {
        check_ident(package, "package")?;
    }
    check_ident(name, "message")?;
    Ok(ScalarType::Named(NamedType {
        package: package.map(str::to_owned),
        name: name.to_owned(),
    }))
}

/// Splits `pkg/Type`, `pkg/msg/Type`, or bare `Type` into `(package, name)`.
fn split_type_path(text: &str) -> Option<(Option<&str>, &str)> {
    let parts: Vec<&str> = text.split('/').collect();
    match parts.as_slice() {
        [name] => Some((None, *name)),
        [package, name] => Some((Some(*package), *name)),
        [package, "msg", name] => Some((Some(*package), *name)),
        _ => None,
    }
}

fn parse_constant_value(
    prim: Primitive,
    string_bound: Option<u32>,
    raw: &str,
) -> Result<ConstantValue, ThadamError> {
    let type_name = prim.ros_name();
    let err = || invalid_definition(format!("invalid {type_name} constant value `{raw}`"));
    if prim.is_text() {
        if let Some(bound) = string_bound
            && raw.len() > bound as usize
        {
            return Err(err());
        }
        let unquoted = strip_quotes(raw);
        return Ok(ConstantValue::Str(unquoted.to_owned()));
    }
    if string_bound.is_some() {
        return Err(err());
    }
    match prim {
        Primitive::Bool => match raw {
            "true" | "1" => Ok(ConstantValue::Bool(true)),
            "false" | "0" => Ok(ConstantValue::Bool(false)),
            _ => Err(err()),
        },
        Primitive::F32 | Primitive::F64 => raw
            .parse::<f64>()
            .map(ConstantValue::F64)
            .map_err(|_| err()),
        Primitive::String | Primitive::WString => unreachable!("handled above"),
        prim => {
            let value = parse_i128(raw).ok_or_else(err)?;
            let (lo, hi) = int_range(prim);
            if value < lo || value > hi {
                return Err(invalid_definition(format!(
                    "{type_name} constant `{raw}` is out of range"
                )));
            }
            if lo < 0 {
                Ok(ConstantValue::I64(value as i64))
            } else {
                Ok(ConstantValue::U64(value as u64))
            }
        }
    }
}

/// Inclusive `[lo, hi]` of a `.msg` integer type as `i128`.
fn int_range(prim: Primitive) -> (i128, i128) {
    match prim {
        Primitive::Byte | Primitive::I8 => (i128::from(i8::MIN), i128::from(i8::MAX)),
        Primitive::Char | Primitive::U8 => (i128::from(u8::MIN), i128::from(u8::MAX)),
        Primitive::I16 => (i128::from(i16::MIN), i128::from(i16::MAX)),
        Primitive::U16 => (i128::from(u16::MIN), i128::from(u16::MAX)),
        Primitive::I32 => (i128::from(i32::MIN), i128::from(i32::MAX)),
        Primitive::U32 => (i128::from(u32::MIN), i128::from(u32::MAX)),
        Primitive::I64 => (i128::from(i64::MIN), i128::from(i64::MAX)),
        Primitive::U64 => (i128::from(u64::MIN), i128::from(u64::MAX)),
        _ => unreachable!("an integer primitive"),
    }
}

/// Parses decimal, `0x` hex, `0o` octal, and `0b` binary integer literals
/// with an optional sign.
fn parse_i128(text: &str) -> Option<i128> {
    let (sign, digits) = match text.as_bytes().first() {
        Some(b'-') => (-1i128, &text[1..]),
        Some(b'+') => (1i128, &text[1..]),
        _ => (1i128, text),
    };
    let (radix, digits) = if let Some(rest) = digits.strip_prefix("0x") {
        (16, rest)
    } else if let Some(rest) = digits.strip_prefix("0X") {
        (16, rest)
    } else if let Some(rest) = digits.strip_prefix("0o") {
        (8, rest)
    } else if let Some(rest) = digits.strip_prefix("0b") {
        (2, rest)
    } else {
        (10, digits)
    };
    if digits.is_empty() {
        return None;
    }
    i128::from_str_radix(digits, radix).ok().map(|v| sign * v)
}

/// Strips one layer of matching single or double quotes.
fn strip_quotes(text: &str) -> &str {
    let bytes = text.as_bytes();
    if bytes.len() >= 2
        && ((bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\''))
    {
        &text[1..text.len() - 1]
    } else {
        text
    }
}

fn split_once_whitespace(text: &str) -> (&str, &str) {
    match text.char_indices().find(|(_, c)| c.is_whitespace()) {
        Some((index, c)) => (&text[..index], text[index + c.len_utf8()..].trim_start()),
        None => (text, ""),
    }
}

fn is_separator(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.len() >= 10 && trimmed.bytes().all(|b| b == b'=')
}

fn check_ident(text: &str, what: &str) -> Result<(), ThadamError> {
    let mut bytes = text.bytes();
    let valid = match bytes.next() {
        Some(first) if first.is_ascii_alphabetic() => {
            bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(invalid_definition(format!("invalid {what} name `{text}`")))
    }
}

fn invalid_definition(message: impl Into<String>) -> ThadamError {
    ThadamError::permanent(
        ErrorDomain::Schema,
        codes::SCHEMA_INVALID_SHAPE,
        message.into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_integer_literals() {
        assert_eq!(parse_i128("42"), Some(42));
        assert_eq!(parse_i128("-7"), Some(-7));
        assert_eq!(parse_i128("+7"), Some(7));
        assert_eq!(parse_i128("0xff"), Some(255));
        assert_eq!(parse_i128("0XFF"), Some(255));
        assert_eq!(parse_i128("-0x10"), Some(-16));
        assert_eq!(parse_i128("0b101"), Some(5));
        assert_eq!(parse_i128("0o17"), Some(15));
        assert_eq!(parse_i128(""), None);
        assert_eq!(parse_i128("-"), None);
        assert_eq!(parse_i128("0x"), None);
        assert_eq!(parse_i128("1.5"), None);
        assert_eq!(parse_i128("12nope"), None);
    }

    #[test]
    fn identifiers_start_with_a_letter() {
        assert!(check_ident("frame_id", "field").is_ok());
        assert!(check_ident("X", "constant").is_ok());
        assert!(check_ident("_x", "field").is_err());
        assert!(check_ident("2d", "field").is_err());
        assert!(check_ident("a-b", "field").is_err());
        assert!(check_ident("", "field").is_err());
    }

    #[test]
    fn separators_are_equal_sign_runs() {
        let standard = "=".repeat(80);
        assert!(is_separator(&standard));
        assert!(is_separator(" ========== "));
        assert!(!is_separator("====="));
        assert!(!is_separator("int32 x"));
    }
}
