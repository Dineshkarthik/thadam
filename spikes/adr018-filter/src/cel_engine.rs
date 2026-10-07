//! Option (a): CEL through the `cel` crate. The expression is compiled once. Per event, the
//! program needs its identifiers bound; the strategies below differ only in how.
//!
//! - `eval_only`   : contexts built once per event *outside* the timer; measures the interpreter.
//! - `ctx_all`     : build a context holding every field of the event, per event.
//! - `ctx_refs`    : build a context holding only the identifiers the program references.
//! - `resolver`    : a `VariableResolver` over the event; values are converted on lookup.

use crate::model::{Event, Quality, Value};
use cel::common::types::{CelDouble, CelString};
use cel::common::value::{CowVal, Val};
use cel::context::VariableResolver;
use cel::{Context, Env, Program};
use std::collections::HashMap;
use std::sync::Arc;

pub fn to_cel(v: &Value) -> cel::Value {
    match v {
        Value::Null => cel::Value::Null,
        Value::Bool(b) => cel::Value::Bool(*b),
        Value::I64(i) => cel::Value::Int(*i),
        Value::U64(u) => cel::Value::UInt(*u),
        // Widening f32 -> f64 is how a CEL double sees an f32 field (ADR-0018 hazard).
        Value::F32(f) => cel::Value::Float(*f as f64),
        Value::F64(f) => cel::Value::Float(*f),
        Value::Str(s) => cel::Value::String(Arc::new(s.to_string())),
        Value::F32s(xs) => cel::Value::List(Arc::new(
            xs.iter().map(|x| cel::Value::Float(*x as f64)).collect(),
        )),
    }
}

pub fn quality_to_cel(q: &Quality) -> cel::Value {
    let mut m: HashMap<String, cel::Value> = HashMap::with_capacity(3);
    m.insert(
        "status".to_string(),
        cel::Value::String(Arc::new(q.status.as_str().to_string())),
    );
    m.insert("flags".to_string(), cel::Value::UInt(q.flags as u64));
    m.insert(
        "reason".to_string(),
        match &q.reason {
            Some(r) => cel::Value::String(Arc::new(r.to_string())),
            None => cel::Value::Null,
        },
    );
    cel::Value::from(m)
}

#[derive(Clone, Copy)]
enum Src {
    Rec(usize),
    Quality,
}

pub struct CelFilter {
    env: Arc<Env>,
    pub program: Program,
    refs: Vec<(String, Src)>,
    root: Context<'static, 'static>,
}

impl CelFilter {
    pub fn compile(src: &str, shape: &crate::model::Shape) -> Result<Self, String> {
        let env = Arc::new(Env::stdlib());
        let program = env.compile(src).map_err(|e| e.to_string())?;
        let mut refs = Vec::new();
        for name in program.references().variables() {
            if name == "quality" {
                refs.push((name.to_string(), Src::Quality));
            } else if let Some(i) = shape.idx(name) {
                refs.push((name.to_string(), Src::Rec(i)));
            }
        }
        let root = Context::with_env(env.clone());
        Ok(CelFilter {
            env,
            program,
            refs,
            root,
        })
    }

    pub fn n_refs(&self) -> usize {
        self.refs.len()
    }

    fn result(r: cel::ResolveResult) -> Option<bool> {
        match r {
            Ok(cel::Value::Bool(b)) => Some(b),
            _ => None,
        }
    }

    pub fn prebuilt_context(&self, e: &Event) -> Context<'static, 'static> {
        let mut ctx = Context::with_env(self.env.clone());
        for (name, src) in &self.refs {
            match src {
                Src::Rec(i) => ctx.add_variable_from_value(name.clone(), to_cel(&e.values[*i])),
                Src::Quality => {
                    ctx.add_variable_from_value(name.clone(), quality_to_cel(&e.quality))
                }
            }
        }
        ctx
    }

    pub fn eval_prebuilt(&self, ctx: &Context<'_, '_>) -> Option<bool> {
        Self::result(self.program.execute(ctx))
    }

    pub fn ctx_all(&self, e: &Event) -> Option<bool> {
        let mut ctx = Context::with_env(self.env.clone());
        for (i, name) in e.shape.names.iter().enumerate() {
            ctx.add_variable_from_value(name.clone(), to_cel(&e.values[i]));
        }
        ctx.add_variable_from_value("quality", quality_to_cel(&e.quality));
        Self::result(self.program.execute(&ctx))
    }

    pub fn ctx_refs(&self, e: &Event) -> Option<bool> {
        let mut ctx = Context::with_env(self.env.clone());
        for (name, src) in &self.refs {
            match src {
                Src::Rec(i) => ctx.add_variable_from_value(name.clone(), to_cel(&e.values[*i])),
                Src::Quality => {
                    ctx.add_variable_from_value(name.clone(), quality_to_cel(&e.quality))
                }
            }
        }
        Self::result(self.program.execute(&ctx))
    }

    pub fn resolver(&self, e: &Event) -> Option<bool> {
        let res = EventResolver { e };
        let mut ctx = self.root.new_inner_scope();
        ctx.set_variable_resolver(&res);
        Self::result(self.program.execute(&ctx))
    }
}

struct EventResolver<'a> {
    e: &'a Event,
}

impl VariableResolver for EventResolver<'_> {
    fn resolve<'b>(&'b self, name: &str) -> Option<CowVal<'b, 'b>> {
        if name == "quality" {
            let b: Box<dyn Val> = quality_to_cel(&self.e.quality).try_into().ok()?;
            return Some(CowVal::Owned(b));
        }
        let i = self.e.shape.idx(name)?;
        match &self.e.values[i] {
            Value::Str(s) => Some(CowVal::owned(CelString::from(&**s))),
            Value::F32(f) => Some(CowVal::owned(CelDouble::from(*f as f64))),
            Value::F64(f) => Some(CowVal::owned(CelDouble::from(*f))),
            other => {
                let b: Box<dyn Val> = to_cel(other).try_into().ok()?;
                Some(CowVal::Owned(b))
            }
        }
    }
}
