//! Structured predicates (ADR-0018): compile against a dataset's fields, then
//! evaluate per event without allocating.
//!
//! # Namespace
//!
//! A leaf names a dataset field (`soc_pct`), an identity level (`robot`), or an
//! envelope field (`quality.status`, `quality.flags`). Dot access into nested
//! records and `^/` parent paths do not exist: filters run on flat typed
//! records after the parent message is gone.
//!
//! # Literal typing
//!
//! A literal is checked against the field's declared type at compile time.
//! Integer fields take integer literals that fit the type (so a `u64` above
//! `i64::MAX` stays exact); float fields take any number and compare in the
//! field's own domain, so `soc_pct == 0.1` holds for the `f32` nearest 0.1;
//! string fields take text; `quality.status` takes `good`, `uncertain`, or
//! `bad`.
//!
//! # Null and missing values
//!
//! A null field (absent from the message, or present as null) makes `eq`, the
//! ordered comparisons, `in`, `min`, and `max` false and `ne` true. `null: true`
//! is the explicit test. A `NaN` is unordered: `eq` and the ordered comparisons
//! are false and `ne` is true. `min`/`max` on a list hold for every element, so
//! an empty list satisfies them and a list with a null or `NaN` element does not.
//!
//! Each construct has an exact CEL equivalent under these rules, so a later
//! move to CEL is mechanical.

use std::cmp::Ordering;

use thadam_core::{Event, FieldIdx, FieldType, Payload, QualityStatus, Shape, Sym, Value};

use crate::limits::ChainLimits;
use crate::spec::{Lit, PredicateSpec};

pub(crate) type CompileErrors = Vec<(String, String)>;

/// The names a predicate may use.
pub(crate) struct Env<'a> {
    pub(crate) shape: &'a Shape,
    pub(crate) levels: &'a [Sym],
}

#[derive(Clone, Copy, Debug)]
enum Sel {
    Field(FieldIdx),
    Level(usize),
    QStatus,
    QFlags,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum NumLit {
    Int(i128),
    F32(f32),
    F64(f64),
}

#[derive(Clone, Copy, Debug)]
enum Cmp {
    Lt,
    Le,
    Gt,
    Ge,
}

/// An inclusive numeric range; either end may be open.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Range {
    min: Option<NumLit>,
    max: Option<NumLit>,
}

#[derive(Clone, Debug)]
enum Test {
    StrEq(Box<str>),
    StrNe(Box<str>),
    StrIn(Vec<Box<str>>),
    BoolEq(bool),
    BoolNe(bool),
    Cmp(Cmp, NumLit),
    NumEq(NumLit),
    NumNe(NumLit),
    NumIn(Vec<NumLit>),
    Range(Range),
    IsNull(bool),
}

#[derive(Clone, Debug)]
enum Node {
    All(Vec<Node>),
    Any(Vec<Node>),
    Not(Box<Node>),
    Leaf(Sel, Test),
}

/// A compiled predicate.
#[derive(Clone, Debug)]
pub(crate) struct Predicate {
    root: Node,
}

/// Why an evaluation could not produce an answer. A chain turns it into a
/// reject; it cannot happen for events built by the same chain.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct EvalError(pub(crate) &'static str);

fn int_of(v: &Value) -> Option<i128> {
    Some(match v {
        Value::I8(x) => i128::from(*x),
        Value::I16(x) => i128::from(*x),
        Value::I32(x) => i128::from(*x),
        Value::I64(x) => i128::from(*x),
        Value::U8(x) => i128::from(*x),
        Value::U16(x) => i128::from(*x),
        Value::U32(x) => i128::from(*x),
        Value::U64(x) => i128::from(*x),
        _ => return None,
    })
}

/// Orders `v` against `lit` in the literal's domain; `None` if either is
/// null, of another kind, or `NaN`.
fn order(v: &Value, lit: NumLit) -> Option<Ordering> {
    match (v, lit) {
        (Value::F32(a), NumLit::F32(b)) => a.partial_cmp(&b),
        (Value::F64(a), NumLit::F64(b)) => a.partial_cmp(&b),
        (_, NumLit::Int(b)) => int_of(v).map(|a| a.cmp(&b)),
        _ => None,
    }
}

fn int_range(ty: &FieldType) -> Option<(i128, i128)> {
    Some(match ty {
        FieldType::I8 => (i128::from(i8::MIN), i128::from(i8::MAX)),
        FieldType::I16 => (i128::from(i16::MIN), i128::from(i16::MAX)),
        FieldType::I32 => (i128::from(i32::MIN), i128::from(i32::MAX)),
        FieldType::I64 => (i128::from(i64::MIN), i128::from(i64::MAX)),
        FieldType::U8 => (0, i128::from(u8::MAX)),
        FieldType::U16 => (0, i128::from(u16::MAX)),
        FieldType::U32 => (0, i128::from(u32::MAX)),
        FieldType::U64 => (0, i128::from(u64::MAX)),
        _ => return None,
    })
}

fn is_numeric(ty: &FieldType) -> bool {
    int_range(ty).is_some() || matches!(ty, FieldType::F32 | FieldType::F64)
}

/// Types a literal against a numeric field type (or a list's element type).
pub(crate) fn num_lit(ty: &FieldType, lit: &Lit) -> Result<NumLit, String> {
    let as_f64 = match lit {
        Lit::Int(i) => Some(*i as f64),
        Lit::UInt(u) => Some(*u as f64),
        Lit::Float(f) => Some(*f),
        _ => None,
    };
    if let Some((lo, hi)) = int_range(ty) {
        let v = match lit {
            Lit::Int(i) => i128::from(*i),
            Lit::UInt(u) => i128::from(*u),
            Lit::Float(_) => {
                return Err(format!(
                    "{ty:?} field needs an integer literal; write the number without a fraction"
                ));
            }
            other => {
                return Err(format!(
                    "{ty:?} field needs an integer literal, got {other:?}"
                ));
            }
        };
        return if (lo..=hi).contains(&v) {
            Ok(NumLit::Int(v))
        } else {
            Err(format!("literal {v} does not fit {ty:?}"))
        };
    }
    let Some(x) = as_f64 else {
        return Err(format!("{ty:?} field needs a numeric literal, got {lit:?}"));
    };
    if !x.is_finite() {
        return Err("literal is not finite".to_owned());
    }
    match ty {
        FieldType::F32 => {
            let narrowed = x as f32;
            if narrowed.is_finite() {
                Ok(NumLit::F32(narrowed))
            } else {
                Err("literal does not fit f32".to_owned())
            }
        }
        FieldType::F64 => Ok(NumLit::F64(x)),
        other => Err(format!("{other:?} is not a numeric type")),
    }
}

/// Compiles an optional min/max pair for a field of `ty`.
pub(crate) fn compile_range(
    ty: &FieldType,
    min: Option<&Lit>,
    max: Option<&Lit>,
) -> Result<Range, String> {
    let elem = match ty {
        FieldType::List(inner) => inner.as_ref(),
        other => other,
    };
    if !is_numeric(elem) {
        return Err(format!(
            "`min`/`max` need a numeric field or a list of numbers; this field is {ty:?}"
        ));
    }
    let min = min.map(|l| num_lit(elem, l)).transpose()?;
    let max = max.map(|l| num_lit(elem, l)).transpose()?;
    if min.is_none() && max.is_none() {
        return Err("give `min`, `max`, or both".to_owned());
    }
    if let (Some(a), Some(b)) = (min, max)
        && let (NumLit::Int(a), NumLit::Int(b)) = (a, b)
        && a > b
    {
        return Err("`min` is greater than `max`".to_owned());
    }
    Ok(Range { min, max })
}

impl Range {
    fn holds_for(&self, v: &Value) -> bool {
        let lo = self
            .min
            .is_none_or(|m| matches!(order(v, m), Some(Ordering::Greater | Ordering::Equal)));
        let hi = self
            .max
            .is_none_or(|m| matches!(order(v, m), Some(Ordering::Less | Ordering::Equal)));
        lo && hi
    }

    /// `None` for a null value (the rule does not apply); otherwise whether
    /// the value, or every element of a list, is within the range.
    pub(crate) fn check(&self, v: &Value) -> Option<bool> {
        match v {
            Value::Null => None,
            Value::F32s(a) => Some(a.iter().all(|x| self.holds_for(&Value::F32(*x)))),
            Value::F64s(a) => Some(a.iter().all(|x| self.holds_for(&Value::F64(*x)))),
            Value::I32s(a) => Some(a.iter().all(|x| self.holds_for(&Value::I32(*x)))),
            Value::I64s(a) => Some(a.iter().all(|x| self.holds_for(&Value::I64(*x)))),
            Value::List(items) => Some(items.iter().all(|x| self.holds_for(x))),
            scalar => Some(self.holds_for(scalar)),
        }
    }
}

struct Compiler<'a> {
    env: &'a Env<'a>,
    limits: &'a ChainLimits,
    nodes: usize,
    errs: CompileErrors,
}

pub(crate) fn compile(
    spec: &PredicateSpec,
    env: &Env<'_>,
    limits: &ChainLimits,
    path: &str,
) -> Result<Predicate, CompileErrors> {
    let mut c = Compiler {
        env,
        limits,
        nodes: 0,
        errs: Vec::new(),
    };
    let root = c.node(spec, path, 1);
    match root {
        Some(root) if c.errs.is_empty() => Ok(Predicate { root }),
        _ => Err(c.errs),
    }
}

impl Compiler<'_> {
    fn err(&mut self, path: &str, msg: impl Into<String>) {
        self.errs.push((path.to_owned(), msg.into()));
    }

    fn node(&mut self, s: &PredicateSpec, path: &str, depth: usize) -> Option<Node> {
        self.nodes += 1;
        if self.nodes > self.limits.max_predicate_nodes {
            if self.nodes == self.limits.max_predicate_nodes + 1 {
                self.err(
                    path,
                    format!(
                        "predicate has more than {} nodes",
                        self.limits.max_predicate_nodes
                    ),
                );
            }
            return None;
        }
        if depth > self.limits.max_predicate_depth {
            self.err(
                path,
                format!(
                    "predicate nests deeper than {}",
                    self.limits.max_predicate_depth
                ),
            );
            return None;
        }
        let heads = [
            s.all.is_some(),
            s.any.is_some(),
            s.not.is_some(),
            s.field.is_some(),
        ];
        if heads.iter().filter(|h| **h).count() != 1 {
            self.err(
                path,
                "exactly one of `all`, `any`, `not`, or `field` is required",
            );
            return None;
        }
        if let Some(children) = &s.all {
            return self.children(children, path, "all", depth).map(Node::All);
        }
        if let Some(children) = &s.any {
            return self.children(children, path, "any", depth).map(Node::Any);
        }
        if let Some(inner) = &s.not {
            return self
                .node(inner, &format!("{path}.not"), depth + 1)
                .map(|n| Node::Not(Box::new(n)));
        }
        self.leaf(s, path)
    }

    fn children(
        &mut self,
        children: &[PredicateSpec],
        path: &str,
        key: &str,
        depth: usize,
    ) -> Option<Vec<Node>> {
        if children.is_empty() {
            self.err(path, format!("`{key}` needs at least one condition"));
            return None;
        }
        let nodes: Vec<Option<Node>> = children
            .iter()
            .enumerate()
            .map(|(i, c)| self.node(c, &format!("{path}.{key}[{i}]"), depth + 1))
            .collect();
        nodes.into_iter().collect()
    }

    fn leaf(&mut self, s: &PredicateSpec, path: &str) -> Option<Node> {
        let name = s.field.as_deref().unwrap_or_default();
        let fpath = format!("{path}.field");
        let (sel, ty) = if let Some(i) = self.env.shape.index_of(name) {
            let def = self.env.shape.field(i)?;
            (Sel::Field(i), def.ty.clone())
        } else if let Some(i) = self.env.levels.iter().position(|l| &**l == name) {
            (Sel::Level(i), FieldType::Str)
        } else if name == "quality.status" {
            (Sel::QStatus, FieldType::Str)
        } else if name == "quality.flags" {
            (Sel::QFlags, FieldType::U32)
        } else {
            self.err(&fpath, format!("unknown field `{name}`"));
            return None;
        };

        let before = self.errs.len();
        let mut tests = Vec::new();
        let ops: [(&str, &Option<Lit>); 6] = [
            ("eq", &s.eq),
            ("ne", &s.ne),
            ("lt", &s.lt),
            ("le", &s.le),
            ("gt", &s.gt),
            ("ge", &s.ge),
        ];
        for (key, lit) in ops {
            if let Some(lit) = lit {
                let kpath = format!("{path}.{key}");
                if let Some(t) = self.compare(sel, &ty, name, key, lit, &kpath) {
                    tests.push(t);
                }
            }
        }
        if let Some(list) = &s.in_ {
            let kpath = format!("{path}.in");
            if list.is_empty() {
                self.err(&kpath, "`in` needs at least one literal");
            } else if list.len() > self.limits.max_in_set {
                self.err(
                    &kpath,
                    format!("`in` has more than {} literals", self.limits.max_in_set),
                );
            } else if let Some(t) = self.membership(sel, &ty, name, list, &kpath) {
                tests.push(t);
            }
        }
        if s.min.is_some() || s.max.is_some() {
            match sel {
                Sel::Field(_) | Sel::QFlags => {
                    match compile_range(&ty, s.min.as_ref(), s.max.as_ref()) {
                        Ok(r) => tests.push(Test::Range(r)),
                        Err(e) => self.err(path, format!("field `{name}`: {e}")),
                    }
                }
                _ => self.err(path, format!("`min`/`max` do not apply to `{name}`")),
            }
        }
        if let Some(is_null) = s.null {
            tests.push(Test::IsNull(is_null));
        }
        if tests.is_empty() {
            if self.errs.len() == before {
                self.err(path, format!("no comparison given for field `{name}`"));
            }
            return None;
        }
        if self.errs.len() != before {
            return None;
        }
        self.nodes += tests.len().saturating_sub(1);
        Some(if tests.len() == 1 {
            Node::Leaf(sel, tests.remove(0))
        } else {
            Node::All(tests.into_iter().map(|t| Node::Leaf(sel, t)).collect())
        })
    }

    fn status_text<'l>(&mut self, lit: &'l Lit, path: &str) -> Option<&'l str> {
        match lit {
            Lit::Str(s) if matches!(s.as_str(), "good" | "uncertain" | "bad") => Some(s),
            other => {
                self.err(
                    path,
                    format!("`quality.status` is good, uncertain, or bad; got {other:?}"),
                );
                None
            }
        }
    }

    fn compare(
        &mut self,
        sel: Sel,
        ty: &FieldType,
        name: &str,
        key: &str,
        lit: &Lit,
        path: &str,
    ) -> Option<Test> {
        let equality = matches!(key, "eq" | "ne");
        match ty {
            FieldType::Str => {
                if !equality {
                    self.err(
                        path,
                        format!("`{key}` is not defined for string field `{name}`"),
                    );
                    return None;
                }
                let text = if matches!(sel, Sel::QStatus) {
                    self.status_text(lit, path)?
                } else if let Lit::Str(s) = lit {
                    s
                } else {
                    self.err(path, format!("field `{name}` is a string; got {lit:?}"));
                    return None;
                };
                let text: Box<str> = text.into();
                Some(if key == "eq" {
                    Test::StrEq(text)
                } else {
                    Test::StrNe(text)
                })
            }
            FieldType::Bool => {
                let (true, Lit::Bool(b)) = (equality, lit) else {
                    self.err(
                        path,
                        format!("boolean field `{name}` supports `eq` and `ne` with true or false"),
                    );
                    return None;
                };
                Some(if key == "eq" {
                    Test::BoolEq(*b)
                } else {
                    Test::BoolNe(*b)
                })
            }
            t if is_numeric(t) => {
                let n = match num_lit(t, lit) {
                    Ok(n) => n,
                    Err(e) => {
                        self.err(path, format!("field `{name}`: {e}"));
                        return None;
                    }
                };
                Some(match key {
                    "eq" => Test::NumEq(n),
                    "ne" => Test::NumNe(n),
                    "lt" => Test::Cmp(Cmp::Lt, n),
                    "le" => Test::Cmp(Cmp::Le, n),
                    "gt" => Test::Cmp(Cmp::Gt, n),
                    _ => Test::Cmp(Cmp::Ge, n),
                })
            }
            other => {
                self.err(
                    path,
                    format!("`{key}` is not supported for field `{name}` of type {other:?}"),
                );
                None
            }
        }
    }

    fn membership(
        &mut self,
        sel: Sel,
        ty: &FieldType,
        name: &str,
        list: &[Lit],
        path: &str,
    ) -> Option<Test> {
        match ty {
            FieldType::Str => {
                let mut out = Vec::with_capacity(list.len());
                for lit in list {
                    let text = if matches!(sel, Sel::QStatus) {
                        self.status_text(lit, path)?
                    } else if let Lit::Str(s) = lit {
                        s
                    } else {
                        self.err(
                            path,
                            format!("`in` needs strings for string field `{name}`"),
                        );
                        return None;
                    };
                    out.push(Box::<str>::from(text));
                }
                Some(Test::StrIn(out))
            }
            t if is_numeric(t) => {
                let mut out = Vec::with_capacity(list.len());
                for lit in list {
                    match num_lit(t, lit) {
                        Ok(n) => out.push(n),
                        Err(e) => {
                            self.err(path, format!("field `{name}`: {e}"));
                            return None;
                        }
                    }
                }
                Some(Test::NumIn(out))
            }
            other => {
                self.err(
                    path,
                    format!("`in` is not supported for field `{name}` of type {other:?}"),
                );
                None
            }
        }
    }
}

enum Operand<'e> {
    Null,
    Value(&'e Value),
    Text(&'e str),
}

impl Predicate {
    /// Evaluates the predicate against one event.
    pub(crate) fn eval(&self, event: &Event) -> Result<bool, EvalError> {
        eval_node(&self.root, event)
    }
}

fn eval_node(node: &Node, event: &Event) -> Result<bool, EvalError> {
    match node {
        Node::All(children) => {
            for c in children {
                if !eval_node(c, event)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Node::Any(children) => {
            for c in children {
                if eval_node(c, event)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Node::Not(inner) => eval_node(inner, event).map(|b| !b),
        Node::Leaf(sel, test) => eval_leaf(*sel, test, event),
    }
}

fn eval_leaf(sel: Sel, test: &Test, event: &Event) -> Result<bool, EvalError> {
    let flags_value;
    let operand = match sel {
        Sel::Field(idx) => {
            let Payload::Record(record) = event.payload() else {
                return Err(EvalError("event payload is not a record"));
            };
            match record.try_get_at(idx) {
                Some(Value::Null) => Operand::Null,
                Some(v) => Operand::Value(v),
                None => {
                    return Err(EvalError(
                        "event record does not match the predicate's shape",
                    ));
                }
            }
        }
        Sel::Level(i) => match event.asset().levels().get(i) {
            Some((_, v)) => Operand::Text(v.as_str()),
            None => Operand::Null,
        },
        Sel::QStatus => Operand::Text(match event.quality().status {
            QualityStatus::Good => "good",
            QualityStatus::Uncertain => "uncertain",
            QualityStatus::Bad => "bad",
            _ => "unknown",
        }),
        Sel::QFlags => {
            flags_value = Value::U32(event.quality().flags.bits());
            Operand::Value(&flags_value)
        }
    };
    Ok(apply(test, &operand))
}

fn text_of<'e>(op: &Operand<'e>) -> Option<&'e str> {
    match op {
        Operand::Text(s) => Some(s),
        Operand::Value(Value::Str(s)) => Some(s.as_str()),
        _ => None,
    }
}

fn apply(test: &Test, op: &Operand<'_>) -> bool {
    let value = match op {
        Operand::Value(v) => Some(*v),
        _ => None,
    };
    match test {
        Test::IsNull(want) => matches!(op, Operand::Null) == *want,
        Test::StrEq(t) => text_of(op).is_some_and(|s| s == &**t),
        Test::StrNe(t) => text_of(op).is_none_or(|s| s != &**t),
        Test::StrIn(ts) => text_of(op).is_some_and(|s| ts.iter().any(|t| s == &**t)),
        Test::BoolEq(b) => value.and_then(Value::as_bool) == Some(*b),
        Test::BoolNe(b) => value.and_then(Value::as_bool) != Some(*b),
        Test::Cmp(c, lit) => value.and_then(|v| order(v, *lit)).is_some_and(|o| match c {
            Cmp::Lt => o == Ordering::Less,
            Cmp::Le => o != Ordering::Greater,
            Cmp::Gt => o == Ordering::Greater,
            Cmp::Ge => o != Ordering::Less,
        }),
        Test::NumEq(lit) => value.and_then(|v| order(v, *lit)) == Some(Ordering::Equal),
        Test::NumNe(lit) => value.and_then(|v| order(v, *lit)) != Some(Ordering::Equal),
        Test::NumIn(lits) => {
            value.is_some_and(|v| lits.iter().any(|l| order(v, *l) == Some(Ordering::Equal)))
        }
        Test::Range(r) => value.and_then(|v| r.check(v)).unwrap_or(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use thadam_core::{
        AssetPath, EventId, FieldDef, Quality, QualityFlags, Record, ShapeRef, TimeDomain, Times,
        Timestamp, WalPos,
    };
    use serde_json::json;

    fn shape() -> ShapeRef {
        ShapeRef::new(
            Shape::new(vec![
                FieldDef::new("level", FieldType::Str),
                FieldDef::new("soc_pct", FieldType::F32),
                FieldDef::new("seq", FieldType::U64),
                FieldDef::new("temps", FieldType::List(Box::new(FieldType::F32))),
                FieldDef::new("count", FieldType::I32),
                FieldDef::new("ok", FieldType::Bool),
            ])
            .unwrap(),
        )
    }

    struct Row {
        level: Value,
        soc: Value,
        seq: Value,
        temps: Value,
        count: Value,
        ok: Value,
    }

    impl Default for Row {
        fn default() -> Self {
            Row {
                level: Value::str("info"),
                soc: Value::F32(50.0),
                seq: Value::U64(7),
                temps: Value::F32s(vec![1.0f32, 2.0].into()),
                count: Value::I32(3),
                ok: Value::Bool(true),
            }
        }
    }

    fn event(row: Row, robot: &str, quality: Quality) -> Event {
        let record = Record::new(
            shape(),
            vec![row.level, row.soc, row.seq, row.temps, row.count, row.ok],
        )
        .unwrap();
        Event::builder(
            EventId::from_u128(1),
            WalPos::new(0),
            Times::new(Timestamp::from_nanos(0), TimeDomain::Utc),
        )
        .asset(AssetPath::new([("robot", robot)]).unwrap())
        .quality(quality)
        .payload(record)
        .build()
        .unwrap()
    }

    fn compile_json(v: serde_json::Value) -> Result<Predicate, CompileErrors> {
        let spec: PredicateSpec = serde_json::from_value(v).unwrap();
        let shape = shape();
        let levels: [Sym; 1] = [Sym::from("robot")];
        let env = Env {
            shape: &shape,
            levels: &levels,
        };
        compile(&spec, &env, &ChainLimits::default(), "where")
    }

    fn run(p: &serde_json::Value, row: Row) -> bool {
        compile_json(p.clone())
            .unwrap()
            .eval(&event(row, "robot-01", Quality::good()))
            .unwrap()
    }

    #[test]
    fn demo_filters() {
        let level_ne_debug = json!({ "field": "level", "ne": "debug" });
        assert!(run(&level_ne_debug, Row::default()));
        let debug = Row {
            level: Value::str("debug"),
            ..Row::default()
        };
        assert!(!run(&level_ne_debug, debug));

        let buggy = json!({ "field": "robot", "ne": "robot-03" });
        let p = compile_json(buggy).unwrap();
        assert!(
            p.eval(&event(Row::default(), "robot-01", Quality::good()))
                .unwrap()
        );
        assert!(
            !p.eval(&event(Row::default(), "robot-03", Quality::good()))
                .unwrap()
        );

        let good = compile_json(json!({ "field": "quality.status", "eq": "good" })).unwrap();
        let bad = Quality {
            status: QualityStatus::Bad,
            ..Quality::good()
        };
        assert!(
            good.eval(&event(Row::default(), "r", Quality::good()))
                .unwrap()
        );
        assert!(!good.eval(&event(Row::default(), "r", bad)).unwrap());
    }

    #[test]
    fn adr_combo_expression() {
        let p = json!({ "all": [
            { "field": "level", "ne": "debug" },
            { "any": [
                { "field": "robot", "ne": "robot-03" },
                { "field": "quality.status", "eq": "good" } ] },
            { "field": "soc_pct", "ge": 10.0 } ] });
        let c = compile_json(p).unwrap();
        let ok = event(Row::default(), "robot-03", Quality::good());
        assert!(c.eval(&ok).unwrap());
        let low = Row {
            soc: Value::F32(9.9),
            ..Row::default()
        };
        assert!(!c.eval(&event(low, "robot-01", Quality::good())).unwrap());
    }

    #[test]
    fn f32_fields_compare_in_the_f32_domain() {
        let eq = json!({ "field": "soc_pct", "eq": 0.1 });
        let near = Row {
            soc: Value::F32(0.1),
            ..Row::default()
        };
        assert!(run(&eq, near));
        let widened = json!({ "field": "soc_pct", "eq": 0.100_000_001_490_116_12 });
        let near = Row {
            soc: Value::F32(0.1),
            ..Row::default()
        };
        assert!(run(&widened, near));
    }

    #[test]
    fn u64_above_i64_max_is_exact() {
        let big = Row {
            seq: Value::U64(u64::MAX),
            ..Row::default()
        };
        assert!(run(
            &json!({ "field": "seq", "gt": 9_223_372_036_854_775_807u64 }),
            Row {
                seq: Value::U64(u64::MAX),
                ..Row::default()
            }
        ));
        assert!(run(&json!({ "field": "seq", "eq": u64::MAX }), big));
        let almost = Row {
            seq: Value::U64(u64::MAX),
            ..Row::default()
        };
        assert!(!run(&json!({ "field": "seq", "eq": u64::MAX - 1 }), almost));
    }

    #[test]
    fn null_semantics() {
        let null_level = || Row {
            level: Value::Null,
            ..Row::default()
        };
        assert!(!run(
            &json!({ "field": "level", "eq": "E-1" }),
            null_level()
        ));
        assert!(run(&json!({ "field": "level", "ne": "E-1" }), null_level()));
        assert!(!run(
            &json!({ "field": "level", "in": ["a"] }),
            null_level()
        ));
        assert!(run(
            &json!({ "field": "level", "null": true }),
            null_level()
        ));
        assert!(!run(
            &json!({ "field": "level", "null": false }),
            null_level()
        ));
        assert!(run(
            &json!({ "field": "level", "null": false }),
            Row::default()
        ));
        let null_soc = || Row {
            soc: Value::Null,
            ..Row::default()
        };
        assert!(!run(&json!({ "field": "soc_pct", "gt": 0 }), null_soc()));
        assert!(!run(&json!({ "field": "soc_pct", "min": 0 }), null_soc()));
        assert!(run(&json!({ "field": "soc_pct", "ne": 1 }), null_soc()));
        assert!(run(
            &json!({ "not": { "field": "soc_pct", "gt": 0 } }),
            null_soc()
        ));
    }

    #[test]
    fn nan_is_unordered() {
        let nan = || Row {
            soc: Value::F32(f32::NAN),
            ..Row::default()
        };
        assert!(!run(&json!({ "field": "soc_pct", "eq": 1.0 }), nan()));
        assert!(run(&json!({ "field": "soc_pct", "ne": 1.0 }), nan()));
        assert!(!run(&json!({ "field": "soc_pct", "lt": 1.0 }), nan()));
        assert!(!run(&json!({ "field": "soc_pct", "min": 0 }), nan()));
    }

    #[test]
    fn ranges_on_lists_apply_to_every_element() {
        let r = json!({ "field": "temps", "min": 0, "max": 10 });
        assert!(run(&r, Row::default()));
        let hot = Row {
            temps: Value::F32s(vec![1.0f32, 11.0].into()),
            ..Row::default()
        };
        assert!(!run(&r, hot));
        let empty = Row {
            temps: Value::F32s(Vec::<f32>::new().into()),
            ..Row::default()
        };
        assert!(run(&r, empty));
        let nan = Row {
            temps: Value::F32s(vec![f32::NAN].into()),
            ..Row::default()
        };
        assert!(!run(&r, nan));
    }

    #[test]
    fn membership_and_bool() {
        assert!(run(
            &json!({ "field": "level", "in": ["info", "warn"] }),
            Row::default()
        ));
        assert!(!run(
            &json!({ "field": "level", "in": ["warn"] }),
            Row::default()
        ));
        assert!(run(
            &json!({ "field": "count", "in": [1, 3] }),
            Row::default()
        ));
        assert!(run(&json!({ "field": "ok", "eq": true }), Row::default()));
        assert!(!run(&json!({ "field": "ok", "ne": true }), Row::default()));
        assert!(run(
            &json!({ "field": "quality.status", "in": ["good", "uncertain"] }),
            Row::default()
        ));
    }

    #[test]
    fn quality_flags_compare_as_numbers() {
        let p = compile_json(json!({ "field": "quality.flags", "eq": 0 })).unwrap();
        assert!(
            p.eval(&event(Row::default(), "r", Quality::good()))
                .unwrap()
        );
        let flagged = Quality {
            flags: QualityFlags::OUT_OF_RANGE,
            ..Quality::good()
        };
        assert!(!p.eval(&event(Row::default(), "r", flagged)).unwrap());
    }

    fn first_error(v: serde_json::Value) -> String {
        let errs = compile_json(v).unwrap_err();
        format!("{}: {}", errs[0].0, errs[0].1)
    }

    #[test]
    fn compile_errors_carry_the_predicate_path() {
        assert_eq!(
            first_error(json!({ "field": "nope", "eq": 1 })),
            "where.field: unknown field `nope`"
        );
        assert!(
            first_error(json!({ "field": "soc_pct", "gt": "hot" }))
                .starts_with("where.gt: field `soc_pct`")
        );
        assert!(
            first_error(json!({ "field": "level", "gt": "a" })).contains("not defined for string")
        );
        assert!(first_error(json!({ "field": "level" })).contains("no comparison"));
        assert!(
            first_error(json!({ "all": [], "field": "level", "ne": "x" })).contains("exactly one")
        );
        assert!(first_error(json!({ "all": [] })).contains("at least one"));
        assert!(first_error(json!({ "field": "count", "eq": 1.5 })).contains("integer literal"));
        assert!(
            first_error(json!({ "field": "count", "eq": 3_000_000_000u64 }))
                .contains("does not fit")
        );
        assert!(
            first_error(json!({ "field": "quality.status", "eq": "ok" }))
                .contains("good, uncertain, or bad")
        );
        assert!(first_error(json!({ "field": "level", "min": 1 })).contains("numeric"));
        assert!(
            first_error(json!({ "field": "count", "min": 5, "max": 1 })).contains("greater than")
        );
        assert!(first_error(json!({ "all": [{ "field": "level", "ne": "x" }, { "any": [{ "field": "zzz", "eq": 1 }] }] }))
            .starts_with("where.all[1].any[0].field"));
    }

    #[test]
    fn cost_limits_bound_size_and_depth() {
        let limits = ChainLimits {
            max_predicate_nodes: 4,
            max_predicate_depth: 3,
            max_in_set: 2,
            ..Default::default()
        };
        let shape = shape();
        let env = Env {
            shape: &shape,
            levels: &[],
        };
        let leaf = json!({ "field": "level", "ne": "x" });
        let try_compile = |v: serde_json::Value| {
            let spec: PredicateSpec = serde_json::from_value(v).unwrap();
            compile(&spec, &env, &limits, "where")
        };
        assert!(try_compile(json!({ "all": [leaf, leaf, leaf] })).is_ok());
        let e = try_compile(json!({ "all": [leaf, leaf, leaf, leaf] })).unwrap_err();
        assert!(e[0].1.contains("more than 4 nodes"));
        let deep = json!({ "not": { "not": { "not": leaf } } });
        assert!(
            try_compile(deep).unwrap_err()[0]
                .1
                .contains("deeper than 3")
        );
        let e = try_compile(json!({ "field": "level", "in": ["a", "b", "c"] })).unwrap_err();
        assert!(e[0].1.contains("more than 2 literals"));

        let mut nested: PredicateSpec = serde_json::from_value(leaf.clone()).unwrap();
        for _ in 0..200 {
            nested = PredicateSpec {
                not: Some(Box::new(nested)),
                ..PredicateSpec::default()
            };
        }
        let errs = compile(&nested, &env, &limits, "where").unwrap_err();
        assert_eq!(
            errs.len(),
            1,
            "compilation stops at the first limit, not at the leaf"
        );
    }

    #[test]
    fn a_foreign_record_is_an_eval_error_not_a_panic() {
        let other = ShapeRef::new(Shape::new(vec![FieldDef::new("only", FieldType::I64)]).unwrap());
        let record = Record::new(other, vec![Value::I64(1)]).unwrap();
        let ev = Event::builder(
            EventId::from_u128(1),
            WalPos::new(0),
            Times::new(Timestamp::from_nanos(0), TimeDomain::Utc),
        )
        .payload(record)
        .build()
        .unwrap();
        let p = compile_json(json!({ "field": "count", "eq": 1 })).unwrap();
        assert!(p.eval(&ev).is_err());
    }
}
