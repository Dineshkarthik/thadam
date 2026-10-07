//! Option (b): a minimal structured-predicate tree. YAML shape (illustrative, not a proposed
//! final syntax):
//!
//! ```yaml
//! all:
//!   - { field: level, ne: debug }
//!   - any:
//!       - { field: robot, ne: robot-03 }
//!       - { field: quality.status, eq: good }
//! ```
//!
//! Leaves are `{field, eq|ne|lt|le|gt|ge|in|min/max}`. Field names are resolved against the
//! dataset `Shape` at compile time (to indexes), and literal types are checked against the
//! declared field type, which is the main thing an untyped expression language cannot do.

use crate::model::{Event, FieldType, Shape, Value};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Lit {
    Bool(bool),
    Int(i64),
    // Needed so integer literals above i64::MAX stay exact. Without this variant the
    // untagged parse silently turns them into f64 (observed in the first run of this spike).
    UInt(u64),
    Float(f64),
    Str(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredSpec {
    pub all: Option<Vec<PredSpec>>,
    pub any: Option<Vec<PredSpec>>,
    pub not: Option<Box<PredSpec>>,
    pub field: Option<String>,
    pub eq: Option<Lit>,
    pub ne: Option<Lit>,
    pub lt: Option<Lit>,
    pub le: Option<Lit>,
    pub gt: Option<Lit>,
    pub ge: Option<Lit>,
    #[serde(rename = "in")]
    pub in_: Option<Vec<Lit>>,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

#[derive(Clone, Copy, Debug)]
pub enum Sel {
    Rec(usize),
    QStatus,
    QFlags,
}

#[derive(Clone, Copy, Debug)]
pub enum Cmp {
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Copy, Debug)]
pub enum Num {
    I(i128),
    F(f64),
}

#[derive(Debug)]
pub enum Test {
    StrEq(String),
    StrNe(String),
    StrIn(Vec<String>),
    Cmp(Cmp, Num),
    NumEq(Num),
    NumNe(Num),
    Range(Option<f64>, Option<f64>),
    RangeAll(Option<f64>, Option<f64>),
}

#[derive(Debug)]
pub enum Node {
    All(Vec<Node>),
    Any(Vec<Node>),
    Not(Box<Node>),
    Leaf(Sel, Test),
}

#[derive(Debug, PartialEq)]
pub struct CompileError {
    pub path: String,
    pub message: String,
}

pub fn parse_yaml(src: &str) -> Result<PredSpec, String> {
    serde_saphyr::from_str(src).map_err(|e| e.to_string())
}

pub fn compile(spec: &PredSpec, shape: &Shape) -> Result<Node, Vec<CompileError>> {
    let mut errs = Vec::new();
    let n = compile_at(spec, shape, "filter", &mut errs);
    match (n, errs.is_empty()) {
        (Some(n), true) => Ok(n),
        _ => Err(errs),
    }
}

fn err(errs: &mut Vec<CompileError>, path: &str, msg: impl Into<String>) {
    errs.push(CompileError {
        path: path.to_string(),
        message: msg.into(),
    });
}

fn compile_at(
    s: &PredSpec,
    shape: &Shape,
    path: &str,
    errs: &mut Vec<CompileError>,
) -> Option<Node> {
    let ops = [
        s.all.is_some(),
        s.any.is_some(),
        s.not.is_some(),
        s.field.is_some(),
    ];
    if ops.iter().filter(|b| **b).count() != 1 {
        err(
            errs,
            path,
            "exactly one of `all`, `any`, `not`, or `field` is required",
        );
        return None;
    }
    if let Some(list) = &s.all {
        let nodes: Vec<_> = list
            .iter()
            .enumerate()
            .map(|(i, c)| compile_at(c, shape, &format!("{path}.all[{i}]"), errs))
            .collect();
        return nodes.into_iter().collect::<Option<Vec<_>>>().map(Node::All);
    }
    if let Some(list) = &s.any {
        let nodes: Vec<_> = list
            .iter()
            .enumerate()
            .map(|(i, c)| compile_at(c, shape, &format!("{path}.any[{i}]"), errs))
            .collect();
        return nodes.into_iter().collect::<Option<Vec<_>>>().map(Node::Any);
    }
    if let Some(inner) = &s.not {
        return compile_at(inner, shape, &format!("{path}.not"), errs)
            .map(|n| Node::Not(Box::new(n)));
    }
    let fname = s.field.as_deref().unwrap();
    let fpath = format!("{path}.field");
    let (sel, ty) = match fname {
        "quality.status" => (Sel::QStatus, FieldType::Str),
        "quality.flags" => (Sel::QFlags, FieldType::U64),
        other => match shape.idx(other) {
            Some(i) => (Sel::Rec(i), shape.types[i]),
            None => {
                err(errs, &fpath, format!("unknown field `{other}`"));
                return None;
            }
        },
    };
    let numeric = matches!(
        ty,
        FieldType::I64 | FieldType::U64 | FieldType::F32 | FieldType::F64
    );
    let mut tests = Vec::new();
    let lit_str = |l: &Lit| match l {
        Lit::Str(s) => Some(s.clone()),
        _ => None,
    };
    let lit_num = |l: &Lit| match l {
        Lit::Int(i) => Some(Num::I(*i as i128)),
        Lit::UInt(u) => Some(Num::I(*u as i128)),
        Lit::Float(f) => Some(Num::F(*f)),
        _ => None,
    };
    let errs_before = errs.len();
    for (key, lit, cmp) in [
        ("eq", &s.eq, None),
        ("ne", &s.ne, None),
        ("lt", &s.lt, Some(Cmp::Lt)),
        ("le", &s.le, Some(Cmp::Le)),
        ("gt", &s.gt, Some(Cmp::Gt)),
        ("ge", &s.ge, Some(Cmp::Ge)),
    ] {
        let Some(l) = lit else { continue };
        let kpath = format!("{path}.{key}");
        if ty == FieldType::Str {
            match (lit_str(l), key) {
                (Some(v), "eq") => tests.push(Test::StrEq(v)),
                (Some(v), "ne") => tests.push(Test::StrNe(v)),
                (Some(_), _) => err(
                    errs,
                    &kpath,
                    format!("`{key}` is not defined for string field `{fname}`"),
                ),
                (None, _) => err(
                    errs,
                    &kpath,
                    format!("field `{fname}` is a string; got {l:?}"),
                ),
            }
        } else if numeric {
            match lit_num(l) {
                Some(n) => tests.push(match (key, cmp) {
                    ("eq", _) => Test::NumEq(n),
                    ("ne", _) => Test::NumNe(n),
                    (_, Some(c)) => Test::Cmp(c, n),
                    _ => unreachable!(),
                }),
                None => err(
                    errs,
                    &kpath,
                    format!("field `{fname}` is numeric ({ty:?}); got {l:?}"),
                ),
            }
        } else {
            err(
                errs,
                &kpath,
                format!("`{key}` is not supported for field type {ty:?}"),
            );
        }
    }
    if let Some(list) = &s.in_ {
        let strs: Vec<String> = list.iter().filter_map(lit_str).collect();
        if ty == FieldType::Str && strs.len() == list.len() {
            tests.push(Test::StrIn(strs));
        } else {
            err(
                errs,
                &format!("{path}.in"),
                format!("`in` needs string values for string field `{fname}`"),
            );
        }
    }
    if s.min.is_some() || s.max.is_some() {
        if numeric {
            tests.push(Test::Range(s.min, s.max));
        } else if ty == FieldType::F32s {
            tests.push(Test::RangeAll(s.min, s.max));
        } else {
            err(
                errs,
                path,
                format!("`min`/`max` need a numeric or list-of-float field; `{fname}` is {ty:?}"),
            );
        }
    }
    match tests.len() {
        0 => {
            if errs.len() == errs_before {
                err(
                    errs,
                    path,
                    format!("no comparison given for field `{fname}`"),
                );
            }
            None
        }
        1 => Some(Node::Leaf(sel, tests.pop().unwrap())),
        _ => Some(Node::All(
            tests.into_iter().map(|t| Node::Leaf(sel, t)).collect(),
        )),
    }
}

#[inline]
fn num_cmp(v: &Value, c: Cmp, rhs: Num) -> bool {
    use std::cmp::Ordering::*;
    let ord = match (v, rhs) {
        (Value::I64(a), Num::I(b)) => (*a as i128).cmp(&b),
        (Value::U64(a), Num::I(b)) => (*a as i128).cmp(&b),
        (Value::I64(a), Num::F(b)) => match (*a as f64).partial_cmp(&b) {
            Some(o) => o,
            None => return false,
        },
        (Value::U64(a), Num::F(b)) => match (*a as f64).partial_cmp(&b) {
            Some(o) => o,
            None => return false,
        },
        // f32 fields compare in the f32 domain, so `x == 0.1` holds for the f32 nearest 0.1.
        (Value::F32(a), Num::F(b)) => match a.partial_cmp(&(b as f32)) {
            Some(o) => o,
            None => return false,
        },
        (Value::F32(a), Num::I(b)) => match a.partial_cmp(&(b as f32)) {
            Some(o) => o,
            None => return false,
        },
        (Value::F64(a), Num::F(b)) => match a.partial_cmp(&b) {
            Some(o) => o,
            None => return false,
        },
        (Value::F64(a), Num::I(b)) => match a.partial_cmp(&(b as f64)) {
            Some(o) => o,
            None => return false,
        },
        _ => return false,
    };
    match c {
        Cmp::Lt => ord == Less,
        Cmp::Le => ord != Greater,
        Cmp::Gt => ord == Greater,
        Cmp::Ge => ord != Less,
    }
}

#[inline]
fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::I64(a) => Some(*a as f64),
        Value::U64(a) => Some(*a as f64),
        Value::F32(a) => Some(*a as f64),
        Value::F64(a) => Some(*a),
        _ => None,
    }
}

#[inline]
fn in_range(x: f64, min: Option<f64>, max: Option<f64>) -> bool {
    min.is_none_or(|m| x >= m) && max.is_none_or(|m| x <= m)
}

impl Node {
    /// Evaluate against one event. Null and missing values: `eq` is false, `ne` is true,
    /// ordered comparisons and ranges are false (an explicit, documented choice).
    pub fn eval(&self, e: &Event) -> bool {
        match self {
            Node::All(v) => v.iter().all(|n| n.eval(e)),
            Node::Any(v) => v.iter().any(|n| n.eval(e)),
            Node::Not(n) => !n.eval(e),
            Node::Leaf(sel, test) => eval_leaf(*sel, test, e),
        }
    }
}

fn eval_leaf(sel: Sel, test: &Test, e: &Event) -> bool {
    match sel {
        Sel::QStatus => {
            let s = e.quality.status.as_str();
            return match test {
                Test::StrEq(t) => s == t,
                Test::StrNe(t) => s != t,
                Test::StrIn(ts) => ts.iter().any(|t| s == t),
                _ => false,
            };
        }
        Sel::QFlags => {
            let v = Value::U64(e.quality.flags as u64);
            return match test {
                Test::Cmp(c, n) => num_cmp(&v, *c, *n),
                Test::NumEq(n) => num_cmp(&v, Cmp::Le, *n) && num_cmp(&v, Cmp::Ge, *n),
                Test::NumNe(n) => !(num_cmp(&v, Cmp::Le, *n) && num_cmp(&v, Cmp::Ge, *n)),
                _ => false,
            };
        }
        Sel::Rec(i) => {
            let v = &e.values[i];
            match (test, v) {
                (Test::StrEq(t), Value::Str(s)) => &**s == t,
                (Test::StrEq(_), _) => false,
                (Test::StrNe(t), Value::Str(s)) => &**s != t,
                (Test::StrNe(_), _) => true,
                (Test::StrIn(ts), Value::Str(s)) => ts.iter().any(|t| &**s == t),
                (Test::StrIn(_), _) => false,
                (Test::Cmp(c, n), v) => num_cmp(v, *c, *n),
                (Test::NumEq(n), v) => num_cmp(v, Cmp::Le, *n) && num_cmp(v, Cmp::Ge, *n),
                (Test::NumNe(n), v) => !(num_cmp(v, Cmp::Le, *n) && num_cmp(v, Cmp::Ge, *n)),
                (Test::Range(a, b), v) => as_f64(v).is_some_and(|x| in_range(x, *a, *b)),
                (Test::RangeAll(a, b), Value::F32s(xs)) => {
                    xs.iter().all(|x| in_range(*x as f64, *a, *b))
                }
                (Test::RangeAll(..), _) => false,
            }
        }
    }
}
