//! Option (c): `evalexpr`. NOTE: evalexpr 13.x is licensed AGPL-3.0-only (crates.io), which
//! the thadam license allow-list rejects (ARCHITECTURE.md section 16). Benchmarked as a data
//! point only.

use crate::model::{Event, Value};
use evalexpr::{
    ContextWithMutableVariables, DefaultNumericTypes, HashMapContext, Node, Value as EValue,
    build_operator_tree,
};

pub struct EvalExprFilter {
    node: Node<DefaultNumericTypes>,
    refs: Vec<(String, Option<usize>)>,
}

fn conv(v: &Value) -> EValue<DefaultNumericTypes> {
    match v {
        Value::Null => EValue::Empty,
        Value::Bool(b) => EValue::Boolean(*b),
        Value::I64(i) => EValue::Int(*i),
        Value::U64(u) => EValue::Int(*u as i64),
        Value::F32(f) => EValue::Float(*f as f64),
        Value::F64(f) => EValue::Float(*f),
        Value::Str(s) => EValue::String(s.to_string()),
        Value::F32s(xs) => EValue::Tuple(xs.iter().map(|x| EValue::Float(*x as f64)).collect()),
    }
}

impl EvalExprFilter {
    /// `names` are the identifiers the expression uses; `None` means the flattened
    /// `quality_status` identifier (evalexpr has no field access on structs).
    pub fn compile(src: &str, names: &[&str], shape: &crate::model::Shape) -> Result<Self, String> {
        let node = build_operator_tree::<DefaultNumericTypes>(src).map_err(|e| e.to_string())?;
        let refs = names
            .iter()
            .map(|n| {
                (
                    (*n).to_string(),
                    if *n == "quality_status" {
                        None
                    } else {
                        shape.idx(n)
                    },
                )
            })
            .collect();
        Ok(EvalExprFilter { node, refs })
    }

    pub fn eval(&self, e: &Event) -> Option<bool> {
        let mut ctx = HashMapContext::<DefaultNumericTypes>::new();
        for (name, idx) in &self.refs {
            let v = match idx {
                Some(i) => conv(&e.values[*i]),
                None => EValue::String(e.quality.status.as_str().to_string()),
            };
            ctx.set_value(name.clone(), v).ok()?;
        }
        self.node.eval_boolean_with_context(&ctx).ok()
    }
}
