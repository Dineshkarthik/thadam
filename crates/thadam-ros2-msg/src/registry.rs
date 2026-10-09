//! The schema registry: resolves nested type references and compiles each
//! message type into a Thadam [`Shape`] plus a decode plan used by the CDR
//! decoder.
//!
//! Compilation flattens nested scalar-message fields into dotted names and
//! handles [`builtin_interfaces/Time`](https://docs.ros2.org/latest/api/builtin_interfaces/msg/Time.html)
//! specially: a scalar `Time` field `t` additionally produces a `t`:
//! [`FieldType::Ts`] field holding `sec * 10^9 + nanosec`.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use thadam_core::{
    Classify, ErrorDomain, FieldDef, FieldType, Record, ThadamError, Shape, ShapeRef, codes,
};

use crate::cdr;
use crate::msg::{
    ArrayBound, MessageSpec, NamedType, Primitive, ScalarType, SchemaDocument, TypeKind, TypeName,
};

/// The deepest nested-message nesting a definition may use.
pub const MAX_MESSAGE_DEPTH: usize = 32;

/// A registry of parsed `.msg` definitions that compiles them to Thadam
/// shapes on demand.
///
/// ```text
/// Registry::register / register_ros2msg   (schema text in)
///         |
///         v
/// Registry::compile("sensor_msgs/JointState")
///         |
///         v
/// CompiledMessage { Shape, decode plan }  ->  decode(&[u8]) -> Record
/// ```
///
/// Re-registering a type replaces its definition and drops any compiled
/// entry for it. Compilation results are cached, so `compile` is cheap after
/// the first call.
#[derive(Debug, Default)]
pub struct Registry {
    specs: HashMap<TypeName, MessageSpec>,
    compiled: HashMap<TypeName, Arc<CompiledMessage>>,
}

impl Registry {
    /// An empty registry.
    pub fn new() -> Self {
        Registry::default()
    }

    /// Registers one `.msg` definition, replacing any previous definition of
    /// the same type.
    pub fn register(
        &mut self,
        package: &str,
        name: &str,
        definition: &str,
    ) -> Result<TypeName, ThadamError> {
        let type_name = TypeName::new(package, name)?;
        let spec = MessageSpec::parse(&type_name, definition)?;
        self.compiled.remove(&type_name);
        self.specs.insert(type_name.clone(), spec);
        Ok(type_name)
    }

    /// Registers a `ros2msg` schema document (the MCAP/rosbag2 concatenated
    /// format): the main definition plus every `MSG: pkg/Type` section.
    /// Returns the main type name.
    pub fn register_ros2msg(
        &mut self,
        main_type: &str,
        text: &str,
    ) -> Result<TypeName, ThadamError> {
        let main = TypeName::parse(main_type)?;
        let document = SchemaDocument::parse(&main, text)?;
        for (type_name, spec) in document.into_sections() {
            self.compiled.remove(&type_name);
            self.specs.insert(type_name, spec);
        }
        Ok(main)
    }

    /// The parsed definition of a registered type, including its constants,
    /// or `None` if the type (or the name spelling) is unknown.
    pub fn spec(&self, type_name: &str) -> Option<&MessageSpec> {
        let type_name = TypeName::parse(type_name).ok()?;
        self.specs.get(&type_name)
    }

    /// Compiles `type_name` to a cached [`CompiledMessage`]: a Thadam
    /// [`Shape`] plus a decode plan, with all nested references resolved.
    ///
    /// Fails if the type or any transitively referenced type is unregistered
    /// (`schema.unsupported_type`), or if the definitions nest too deep or
    /// cycle (`schema.invalid_shape`).
    pub fn compile(&mut self, shape_name: &str) -> Result<Arc<CompiledMessage>, ThadamError> {
        let type_name = TypeName::parse(shape_name)?;
        if let Some(compiled) = self.compiled.get(&type_name) {
            return Ok(Arc::clone(compiled));
        }
        let mut stack = Vec::new();
        let (shape, plan, _) = compile_message(&self.specs, &type_name, 0, &mut stack)?;
        let compiled = Arc::new(CompiledMessage {
            type_name: type_name.clone(),
            shape,
            plan,
        });
        self.compiled.insert(type_name, Arc::clone(&compiled));
        Ok(compiled)
    }
}

/// A message type compiled to a Thadam shape with a ready decode plan.
///
/// Decoding walks the plan against the payload in a single pass; nested
/// record lists recurse only as deep as the (compile-time-bounded) plan.
pub struct CompiledMessage {
    type_name: TypeName,
    shape: ShapeRef,
    plan: Arc<[Node]>,
}

impl CompiledMessage {
    /// The fully qualified message type name.
    pub fn type_name(&self) -> &TypeName {
        &self.type_name
    }

    /// The Thadam shape records are decoded into.
    pub fn shape(&self) -> &ShapeRef {
        &self.shape
    }

    /// Decodes one XCDR1 CDR payload into a record of [`Self::shape`].
    ///
    /// Accepts anything that views as bytes, including `&[u8]` and
    /// `&bytes::Bytes`. Trailing bytes are tolerated. Errors are in the
    /// Decode domain: truncated or malformed payload
    /// (`decode.invalid_payload`), unsupported encapsulation
    /// (`decode.unsupported_encoding`), invalid text (`decode.invalid_utf8`),
    /// or a sequence-cap breach (`decode.too_large`).
    pub fn decode(&self, payload: impl AsRef<[u8]>) -> Result<Record, ThadamError> {
        cdr::decode_message(self, payload.as_ref())
    }

    pub(crate) fn plan(&self) -> &[Node] {
        &self.plan
    }
}

impl fmt::Debug for CompiledMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledMessage")
            .field("type_name", &self.type_name)
            .field("shape", &self.shape)
            .finish_non_exhaustive()
    }
}

/// Which typed-array variant a primitive array decodes into.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TypedArrayKind {
    /// `Value::F32s`.
    F32,
    /// `Value::F64s`.
    F64,
    /// `Value::I32s` (elements widen to i32).
    I32,
    /// `Value::I64s` (elements widen to i64).
    I64,
}

/// A primitive element kind for `List` arrays that have no typed-array map.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ListElem {
    /// `bool[]`, one byte per element.
    Bool,
    /// `uint64[]`.
    U64,
    /// `string[]` / `wstring[]`.
    Text {
        /// UTF-16 wire encoding (`wstring`).
        wide: bool,
        /// The `string<=N` bound, if any.
        bound: Option<u32>,
    },
}

/// One step of the decode plan. Values are appended in plan order, so the
/// decoded value vector matches the flattened field list exactly
/// (`Node::Time` appends three: sec, nanosec, then the timestamp).
#[derive(Clone, Debug)]
pub(crate) enum Node {
    /// A fixed-size scalar.
    Prim {
        /// The wire type.
        prim: Primitive,
    },
    /// A `string` / `wstring` scalar.
    Str {
        /// UTF-16 wire encoding (`wstring`).
        wide: bool,
        /// The `string<=N` bound, if any.
        bound: Option<u32>,
    },
    /// A `byte` / `char` / `uint8` array, decoded to `Bytes`.
    Bytes {
        /// The array bound form.
        bound: ArrayBound,
    },
    /// A primitive numeric array decoded to a typed array.
    TypedArray {
        /// The wire element type.
        elem: Primitive,
        /// The target typed-array kind.
        kind: TypedArrayKind,
        /// The array bound form.
        bound: ArrayBound,
    },
    /// A primitive array with no typed-array mapping, decoded to `List`.
    ValueList {
        /// The element kind.
        elem: ListElem,
        /// The array bound form.
        bound: ArrayBound,
        /// Minimum wire bytes per element (1, 4, or 8).
        elem_min_bytes: usize,
    },
    /// An array of nested messages, decoded to a `List` of `Record`.
    RecordList {
        /// The element record shape.
        shape: ShapeRef,
        /// The element decode plan.
        plan: Arc<[Node]>,
        /// The array bound form.
        bound: ArrayBound,
        /// Minimum wire bytes per element (0 for empty messages).
        item_min_bytes: usize,
    },
    /// A `builtin_interfaces/Time` scalar: appends sec (`I32`), nanosec
    /// (`U32`), and the combined `Ts`.
    Time,
}

impl Node {
    /// The minimum bytes this node consumes on the wire.
    fn min_wire_bytes(&self) -> usize {
        match self {
            Node::Prim { prim } => prim.wire_size().unwrap_or(4),
            Node::Str { .. } => 4,
            Node::Bytes { bound }
            | Node::TypedArray { bound, .. }
            | Node::ValueList { bound, .. } => match bound {
                ArrayBound::Fixed(n) => (*n as usize).saturating_mul(self.fixed_elem_size()),
                _ => 4,
            },
            Node::RecordList {
                bound,
                item_min_bytes,
                ..
            } => match bound {
                ArrayBound::Fixed(n) => (*n as usize).saturating_mul(*item_min_bytes),
                _ => 4,
            },
            Node::Time => 8,
        }
    }

    fn fixed_elem_size(&self) -> usize {
        match self {
            Node::Bytes { .. } => 1,
            Node::TypedArray { elem, .. } => elem.wire_size().unwrap_or(0),
            Node::ValueList { elem_min_bytes, .. } => *elem_min_bytes,
            _ => 0,
        }
    }
}

fn compile_message(
    specs: &HashMap<TypeName, MessageSpec>,
    type_name: &TypeName,
    depth: usize,
    stack: &mut Vec<TypeName>,
) -> Result<(ShapeRef, Arc<[Node]>, usize), ThadamError> {
    let spec = registered(specs, type_name)?;
    let mut fields = Vec::with_capacity(spec.fields().len());
    let mut nodes = Vec::with_capacity(spec.fields().len());
    flatten_fields(specs, spec, "", depth, stack, &mut fields, &mut nodes)?;
    let shape = Shape::new(fields)
        .map(ShapeRef::new)
        .map_err(|err| err.to_thadam_error())?;
    let min = nodes.iter().map(Node::min_wire_bytes).sum();
    Ok((shape, Arc::from(nodes.as_slice()), min))
}

/// Flattens `spec`'s fields into `fields`/`nodes` under `prefix` ("" or
/// "a.b."). Nested scalar messages recurse; message arrays become
/// record-list nodes with independently compiled element shapes.
fn flatten_fields(
    specs: &HashMap<TypeName, MessageSpec>,
    spec: &MessageSpec,
    prefix: &str,
    depth: usize,
    stack: &mut Vec<TypeName>,
    fields: &mut Vec<FieldDef>,
    nodes: &mut Vec<Node>,
) -> Result<(), ThadamError> {
    for field in spec.fields() {
        let name = format!("{prefix}{}", field.name);
        match &field.ty {
            TypeKind::Scalar(ScalarType::Primitive { prim, string_bound }) => match prim {
                Primitive::String | Primitive::WString => {
                    fields.push(FieldDef::new(name, FieldType::Str));
                    nodes.push(Node::Str {
                        wide: *prim == Primitive::WString,
                        bound: *string_bound,
                    });
                }
                _ => {
                    fields.push(FieldDef::new(name, scalar_field_type(*prim)));
                    nodes.push(Node::Prim { prim: *prim });
                }
            },
            TypeKind::Scalar(ScalarType::Named(named)) => {
                let nested = resolve(spec, named)?;
                if is_time(&nested) {
                    fields.push(FieldDef::new(format!("{name}.sec"), FieldType::I32));
                    fields.push(FieldDef::new(format!("{name}.nanosec"), FieldType::U32));
                    fields.push(FieldDef::new(name, FieldType::Ts));
                    nodes.push(Node::Time);
                } else {
                    enter(&nested, depth, stack)?;
                    let nested_spec = registered(specs, &nested)?;
                    flatten_fields(
                        specs,
                        nested_spec,
                        &format!("{name}."),
                        depth + 1,
                        stack,
                        fields,
                        nodes,
                    )?;
                    stack.pop();
                }
            }
            TypeKind::Array { elem, bound } => match elem {
                ScalarType::Primitive { prim, string_bound } => {
                    push_array(fields, nodes, name, *prim, *string_bound, *bound);
                }
                ScalarType::Named(named) => {
                    let nested = resolve(spec, named)?;
                    enter(&nested, depth, stack)?;
                    let (item_shape, item_plan, item_min) =
                        compile_message(specs, &nested, depth + 1, stack)?;
                    stack.pop();
                    fields.push(FieldDef::new(
                        name,
                        FieldType::List(Box::new(FieldType::Record(item_shape.clone()))),
                    ));
                    nodes.push(Node::RecordList {
                        shape: item_shape,
                        plan: item_plan,
                        bound: *bound,
                        item_min_bytes: item_min,
                    });
                }
            },
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn push_array(
    fields: &mut Vec<FieldDef>,
    nodes: &mut Vec<Node>,
    name: String,
    prim: Primitive,
    string_bound: Option<u32>,
    bound: ArrayBound,
) {
    match prim {
        Primitive::Byte | Primitive::Char | Primitive::U8 => {
            fields.push(FieldDef::new(name, FieldType::Bytes));
            nodes.push(Node::Bytes { bound });
        }
        Primitive::F32
        | Primitive::F64
        | Primitive::I8
        | Primitive::I16
        | Primitive::U16
        | Primitive::I32
        | Primitive::I64
        | Primitive::U32 => {
            let (list_elem, kind) = match prim {
                Primitive::F32 => (FieldType::F32, TypedArrayKind::F32),
                Primitive::F64 => (FieldType::F64, TypedArrayKind::F64),
                Primitive::I64 | Primitive::U32 => (FieldType::I64, TypedArrayKind::I64),
                _ => (FieldType::I32, TypedArrayKind::I32),
            };
            fields.push(FieldDef::new(name, FieldType::List(Box::new(list_elem))));
            nodes.push(Node::TypedArray {
                elem: prim,
                kind,
                bound,
            });
        }
        Primitive::U64 => {
            fields.push(FieldDef::new(
                name,
                FieldType::List(Box::new(FieldType::U64)),
            ));
            nodes.push(Node::ValueList {
                elem: ListElem::U64,
                bound,
                elem_min_bytes: 8,
            });
        }
        Primitive::Bool => {
            fields.push(FieldDef::new(
                name,
                FieldType::List(Box::new(FieldType::Bool)),
            ));
            nodes.push(Node::ValueList {
                elem: ListElem::Bool,
                bound,
                elem_min_bytes: 1,
            });
        }
        Primitive::String | Primitive::WString => {
            fields.push(FieldDef::new(
                name,
                FieldType::List(Box::new(FieldType::Str)),
            ));
            nodes.push(Node::ValueList {
                elem: ListElem::Text {
                    wide: prim == Primitive::WString,
                    bound: string_bound,
                },
                bound,
                elem_min_bytes: 4,
            });
        }
    }
}

fn scalar_field_type(prim: Primitive) -> FieldType {
    match prim {
        Primitive::Bool => FieldType::Bool,
        Primitive::Byte => FieldType::I8,
        Primitive::Char => FieldType::U8,
        Primitive::I8 => FieldType::I8,
        Primitive::U8 => FieldType::U8,
        Primitive::I16 => FieldType::I16,
        Primitive::U16 => FieldType::U16,
        Primitive::I32 => FieldType::I32,
        Primitive::U32 => FieldType::U32,
        Primitive::I64 => FieldType::I64,
        Primitive::U64 => FieldType::U64,
        Primitive::F32 => FieldType::F32,
        Primitive::F64 => FieldType::F64,
        Primitive::String | Primitive::WString => FieldType::Str,
    }
}

/// Resolves a nested reference against the referring message's package.
fn resolve(spec: &MessageSpec, named: &NamedType) -> Result<TypeName, ThadamError> {
    let package = named
        .package
        .as_deref()
        .unwrap_or_else(|| spec.type_name().package());
    TypeName::new(package, &named.name)
}

fn registered<'a>(
    specs: &'a HashMap<TypeName, MessageSpec>,
    type_name: &TypeName,
) -> Result<&'a MessageSpec, ThadamError> {
    specs.get(type_name).ok_or_else(|| {
        ThadamError::permanent(
            ErrorDomain::Schema,
            codes::SCHEMA_UNSUPPORTED_TYPE,
            format!("message type {type_name} is not registered"),
        )
    })
}

fn is_time(type_name: &TypeName) -> bool {
    type_name.package() == "builtin_interfaces" && type_name.name() == "Time"
}

/// Pushes a nesting frame, rejecting cycles and excessive depth.
fn enter(type_name: &TypeName, depth: usize, stack: &mut Vec<TypeName>) -> Result<(), ThadamError> {
    if stack.contains(type_name) {
        let mut chain: Vec<String> = stack.iter().map(ToString::to_string).collect();
        chain.push(type_name.to_string());
        return Err(ThadamError::permanent(
            ErrorDomain::Schema,
            codes::SCHEMA_INVALID_SHAPE,
            format!("message definitions cycle: {}", chain.join(" -> ")),
        ));
    }
    if depth >= MAX_MESSAGE_DEPTH {
        return Err(ThadamError::permanent(
            ErrorDomain::Schema,
            codes::SCHEMA_INVALID_SHAPE,
            format!("messages nest deeper than {MAX_MESSAGE_DEPTH} levels"),
        )
        .with_context("type", type_name.to_string()));
    }
    stack.push(type_name.clone());
    Ok(())
}
