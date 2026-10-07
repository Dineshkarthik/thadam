//! Record representations under test (decision D8).
//!
//! A. `Keyed<V, INTERN>`:  `Vec<(Arc<str>, V)>`
//!      INTERN=false -> a fresh `Arc<str>` allocation per field per event
//!      INTERN=true  -> field names are pre-interned `Arc<str>`s that are cloned
//!                      (best case for A: no allocation, one atomic inc per field)
//! B. `Shared<V>`:  `Record { shape: Arc<Shape>, values: Vec<V> }`
//! C. `ById<V>`:    `Record { shape: ShapeId(u32), values: Vec<V> }` + per-chain registry
//!
//! All three decode the same declared fields with the same `Val::from_json`,
//! so the only differences are in how field names are carried.

use crate::arrow_conv::{finish, new_cols};
use crate::value::{Ty, Val};
use crate::workload::{Field, Spec, Steps};
use arrow_array::RecordBatch;
use serde_json::Value as Json;
use std::marker::PhantomData;
use std::sync::Arc;

pub trait Repr: 'static {
    type V: Val;
    type Rec: Clone + Send + Sync + 'static;
    /// Per-chain decode/transform state. `Clone` shares any `Arc`s it holds
    /// (so a cloned ctx models a registry shared across threads); a fresh
    /// `new_ctx` models a private per-chain registry.
    type Ctx: Clone + Send + 'static;
    const NAME: &'static str;
    /// Whether a field index can be cached and reused across events.
    const CACHED_INDEX: bool;

    fn new_ctx(spec: &Arc<Spec>) -> Self::Ctx;
    fn build(ctx: &mut Self::Ctx, j: &Json) -> Self::Rec;
    fn get_by_name<'a>(ctx: &Self::Ctx, r: &'a Self::Rec, name: &str) -> Option<&'a Self::V>;
    fn resolve(ctx: &Self::Ctx, r: &Self::Rec, name: &str) -> Option<usize>;
    fn get_idx(r: &Self::Rec, idx: usize) -> &Self::V;
    /// Cheap check that a cached index (resolved on `a`) is valid for `b`.
    fn same_shape(a: &Self::Rec, b: &Self::Rec) -> bool;
    fn transform(ctx: &mut Self::Ctx, batch: &mut Vec<Self::Rec>);
    fn to_arrow(ctx: &mut Self::Ctx, recs: &[Self::Rec]) -> RecordBatch;
}

#[derive(Clone)]
pub struct Shape {
    pub names: Vec<Arc<str>>,
    pub types: Vec<Ty>,
}

impl Shape {
    fn from_decl(decl: &[Field]) -> Shape {
        Shape {
            names: decl.iter().map(|f| Arc::<str>::from(f.name)).collect(),
            types: decl.iter().map(|f| f.ty).collect(),
        }
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|n| &**n == name)
    }

    fn renamed(&self, renames: &[(&'static str, &'static str)]) -> Shape {
        let names = self
            .names
            .iter()
            .map(|n| {
                renames
                    .iter()
                    .find(|(old, _)| *old == &**n)
                    .map_or_else(|| n.clone(), |(_, new)| Arc::<str>::from(*new))
            })
            .collect();
        Shape { names, types: self.types.clone() }
    }
}

#[derive(Clone)]
struct Plan {
    conv: Vec<(usize, f64, f64)>,
    filter_idx: Option<usize>,
    thr: f64,
}

fn make_plan(out: &Shape, steps: &Steps) -> Plan {
    Plan {
        conv: steps
            .converts
            .iter()
            .filter_map(|(n, s, o)| out.index_of(n).map(|i| (i, *s, *o)))
            .collect(),
        filter_idx: out.index_of(steps.filter.0),
        thr: steps.filter.1,
    }
}

static NULL: Json = Json::Null;

#[inline]
fn field<'a>(obj: &'a serde_json::Map<String, Json>, name: &str) -> &'a Json {
    obj.get(name).unwrap_or(&NULL)
}

// ---------------------------------------------------------------- A: keyed

pub struct Keyed<V, const INTERN: bool>(PhantomData<V>);

#[derive(Clone)]
pub struct CtxA {
    spec: Arc<Spec>,
    names: Vec<Arc<str>>,
    new_names: Vec<Arc<str>>,
}

impl<V: Val, const INTERN: bool> Repr for Keyed<V, INTERN> {
    type V = V;
    type Rec = Vec<(Arc<str>, V)>;
    type Ctx = CtxA;
    const NAME: &'static str = if INTERN { "A-interned" } else { "A-fresh" };
    const CACHED_INDEX: bool = false;

    fn new_ctx(spec: &Arc<Spec>) -> CtxA {
        CtxA {
            spec: spec.clone(),
            names: spec.decl.iter().map(|f| Arc::<str>::from(f.name)).collect(),
            new_names: spec.steps.renames.iter().map(|(_, n)| Arc::<str>::from(*n)).collect(),
        }
    }

    fn build(ctx: &mut CtxA, j: &Json) -> Self::Rec {
        let Some(obj) = j.as_object() else { return Vec::new() };
        let mut out = Vec::with_capacity(ctx.spec.decl.len());
        for (i, f) in ctx.spec.decl.iter().enumerate() {
            let key = if INTERN { ctx.names[i].clone() } else { Arc::<str>::from(f.name) };
            out.push((key, V::from_json(field(obj, f.name), f.ty)));
        }
        out
    }

    fn get_by_name<'a>(_: &CtxA, r: &'a Self::Rec, name: &str) -> Option<&'a V> {
        r.iter().find(|(k, _)| &**k == name).map(|(_, v)| v)
    }

    fn resolve(_: &CtxA, _: &Self::Rec, _: &str) -> Option<usize> {
        None
    }

    fn get_idx(r: &Self::Rec, idx: usize) -> &V {
        &r[idx].1
    }

    fn same_shape(_: &Self::Rec, _: &Self::Rec) -> bool {
        false
    }

    fn transform(ctx: &mut CtxA, batch: &mut Vec<Self::Rec>) {
        let steps = &ctx.spec.steps;
        let new_names = &ctx.new_names;
        batch.retain_mut(|rec| {
            for (i, (old, new)) in steps.renames.iter().enumerate() {
                if let Some(slot) = rec.iter_mut().find(|(k, _)| &**k == *old) {
                    slot.0 = if INTERN { new_names[i].clone() } else { Arc::<str>::from(*new) };
                }
            }
            for (name, scale, off) in &steps.converts {
                if let Some(slot) = rec.iter_mut().find(|(k, _)| &**k == *name) {
                    slot.1.convert(*scale, *off);
                }
            }
            match rec.iter().find(|(k, _)| &**k == steps.filter.0) {
                Some((_, v)) => !v.gt_any(steps.filter.1),
                None => true,
            }
        });
    }

    fn to_arrow(ctx: &mut CtxA, recs: &[Self::Rec]) -> RecordBatch {
        let decl = &ctx.spec.decl;
        let mut cols = new_cols(decl, recs.len());
        for r in recs {
            for (c, f) in decl.iter().enumerate() {
                let v = r.iter().find(|(k, _)| &**k == f.name).map(|(_, v)| v);
                cols[c].push(v);
            }
        }
        finish(decl, cols)
    }
}

// -------------------------------------------------------- B: Arc<Shape>

pub struct Shared<V>(PhantomData<V>);

#[derive(Clone)]
pub struct RecB<V> {
    pub shape: Arc<Shape>,
    pub values: Vec<V>,
}

#[derive(Clone)]
struct CacheB {
    input: Arc<Shape>,
    out: Arc<Shape>,
    plan: Plan,
}

#[derive(Clone)]
pub struct CtxB {
    spec: Arc<Spec>,
    shape: Arc<Shape>,
    cache: CacheB,
}

fn derive_b(input: &Arc<Shape>, steps: &Steps) -> CacheB {
    let out = Arc::new(input.renamed(&steps.renames));
    let plan = make_plan(&out, steps);
    CacheB { input: input.clone(), out, plan }
}

impl<V: Val> Repr for Shared<V> {
    type V = V;
    type Rec = RecB<V>;
    type Ctx = CtxB;
    const NAME: &'static str = "B-shape-arc";
    const CACHED_INDEX: bool = true;

    fn new_ctx(spec: &Arc<Spec>) -> CtxB {
        let shape = Arc::new(Shape::from_decl(&spec.decl));
        let cache = derive_b(&shape, &spec.steps);
        CtxB { spec: spec.clone(), shape, cache }
    }

    fn build(ctx: &mut CtxB, j: &Json) -> RecB<V> {
        let Some(obj) = j.as_object() else {
            return RecB { shape: ctx.shape.clone(), values: Vec::new() };
        };
        let mut values = Vec::with_capacity(ctx.spec.decl.len());
        for f in &ctx.spec.decl {
            values.push(V::from_json(field(obj, f.name), f.ty));
        }
        RecB { shape: ctx.shape.clone(), values }
    }

    fn get_by_name<'a>(_: &CtxB, r: &'a RecB<V>, name: &str) -> Option<&'a V> {
        r.shape.index_of(name).map(|i| &r.values[i])
    }

    fn resolve(_: &CtxB, r: &RecB<V>, name: &str) -> Option<usize> {
        r.shape.index_of(name)
    }

    fn get_idx(r: &RecB<V>, idx: usize) -> &V {
        &r.values[idx]
    }

    fn same_shape(a: &RecB<V>, b: &RecB<V>) -> bool {
        Arc::ptr_eq(&a.shape, &b.shape)
    }

    fn transform(ctx: &mut CtxB, batch: &mut Vec<RecB<V>>) {
        let steps = &ctx.spec.steps;
        let cache = &mut ctx.cache;
        batch.retain_mut(|r| {
            if !Arc::ptr_eq(&r.shape, &cache.input) {
                *cache = derive_b(&r.shape, steps);
            }
            r.shape = cache.out.clone();
            for &(i, s, o) in &cache.plan.conv {
                r.values[i].convert(s, o);
            }
            match cache.plan.filter_idx {
                Some(i) => !r.values[i].gt_any(cache.plan.thr),
                None => true,
            }
        });
    }

    fn to_arrow(ctx: &mut CtxB, recs: &[RecB<V>]) -> RecordBatch {
        let decl = &ctx.spec.decl;
        let mut cols = new_cols(decl, recs.len());
        let mut cur: Option<Arc<Shape>> = None;
        let mut map: Vec<Option<usize>> = Vec::new();
        for r in recs {
            if cur.as_ref().is_none_or(|s| !Arc::ptr_eq(s, &r.shape)) {
                map = decl.iter().map(|f| r.shape.index_of(f.name)).collect();
                cur = Some(r.shape.clone());
            }
            for (c, m) in map.iter().enumerate() {
                cols[c].push(m.map(|i| &r.values[i]));
            }
        }
        finish(decl, cols)
    }
}

// ------------------------------------------------------ C: ShapeId + registry

pub struct ById<V>(PhantomData<V>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShapeId(pub u32);

#[derive(Clone)]
pub struct RecC<V> {
    pub shape: ShapeId,
    pub values: Vec<V>,
}

#[derive(Clone)]
struct CacheC {
    out: ShapeId,
    plan: Plan,
}

#[derive(Clone)]
pub struct CtxC {
    spec: Arc<Spec>,
    shapes: Vec<Shape>,
    base: ShapeId,
    cache: Vec<Option<CacheC>>,
}

fn derive_c(shapes: &mut Vec<Shape>, cache: &mut Vec<Option<CacheC>>, steps: &Steps, id: usize) {
    let out = shapes[id].renamed(&steps.renames);
    let plan = make_plan(&out, steps);
    shapes.push(out);
    let out_id = ShapeId((shapes.len() - 1) as u32);
    if cache.len() <= id {
        cache.resize_with(id + 1, || None);
    }
    cache[id] = Some(CacheC { out: out_id, plan });
}

impl<V: Val> Repr for ById<V> {
    type V = V;
    type Rec = RecC<V>;
    type Ctx = CtxC;
    const NAME: &'static str = "C-shape-id";
    const CACHED_INDEX: bool = true;

    fn new_ctx(spec: &Arc<Spec>) -> CtxC {
        let mut shapes = vec![Shape::from_decl(&spec.decl)];
        let mut cache = Vec::new();
        derive_c(&mut shapes, &mut cache, &spec.steps, 0);
        CtxC { spec: spec.clone(), shapes, base: ShapeId(0), cache }
    }

    fn build(ctx: &mut CtxC, j: &Json) -> RecC<V> {
        let Some(obj) = j.as_object() else {
            return RecC { shape: ctx.base, values: Vec::new() };
        };
        let mut values = Vec::with_capacity(ctx.spec.decl.len());
        for f in &ctx.spec.decl {
            values.push(V::from_json(field(obj, f.name), f.ty));
        }
        RecC { shape: ctx.base, values }
    }

    fn get_by_name<'a>(ctx: &CtxC, r: &'a RecC<V>, name: &str) -> Option<&'a V> {
        ctx.shapes[r.shape.0 as usize].index_of(name).map(|i| &r.values[i])
    }

    fn resolve(ctx: &CtxC, r: &RecC<V>, name: &str) -> Option<usize> {
        ctx.shapes[r.shape.0 as usize].index_of(name)
    }

    fn get_idx(r: &RecC<V>, idx: usize) -> &V {
        &r.values[idx]
    }

    fn same_shape(a: &RecC<V>, b: &RecC<V>) -> bool {
        a.shape == b.shape
    }

    fn transform(ctx: &mut CtxC, batch: &mut Vec<RecC<V>>) {
        let CtxC { spec, shapes, cache, .. } = ctx;
        let steps = &spec.steps;
        batch.retain_mut(|r| {
            let id = r.shape.0 as usize;
            if cache.get(id).is_none_or(|c| c.is_none()) {
                derive_c(shapes, cache, steps, id);
            }
            let Some(c) = &cache[id] else { return true };
            r.shape = c.out;
            for &(i, s, o) in &c.plan.conv {
                r.values[i].convert(s, o);
            }
            match c.plan.filter_idx {
                Some(i) => !r.values[i].gt_any(c.plan.thr),
                None => true,
            }
        });
    }

    fn to_arrow(ctx: &mut CtxC, recs: &[RecC<V>]) -> RecordBatch {
        let decl = &ctx.spec.decl;
        let mut cols = new_cols(decl, recs.len());
        let mut maps: Vec<Option<Vec<Option<usize>>>> = Vec::new();
        for r in recs {
            let id = r.shape.0 as usize;
            if maps.get(id).is_none_or(|m| m.is_none()) {
                if maps.len() <= id {
                    maps.resize_with(id + 1, || None);
                }
                let shape = &ctx.shapes[id];
                maps[id] = Some(decl.iter().map(|f| shape.index_of(f.name)).collect());
            }
            let Some(map) = &maps[id] else { continue };
            for (c, m) in map.iter().enumerate() {
                cols[c].push(m.map(|i| &r.values[i]));
            }
        }
        finish(decl, cols)
    }
}
