//! Guards against benchmarking three representations that do different work:
//! all of them must produce the same transformed events and the same Arrow
//! batches for the same input.

use arrow_array::RecordBatch;
use d8_record::build_n;
use d8_record::repr::{ById, Keyed, Repr, Shared};
use d8_record::value::{Val, ext, min};
use d8_record::workload::{self, Workload};

type Row = Vec<(String, Option<f64>, Option<String>, Vec<f64>)>;

fn describe<V: Val>(name: &str, v: &V) -> (String, Option<f64>, Option<String>, Vec<f64>) {
    let mut b = arrow_array::builder::Float64Builder::new();
    let arr = if v.push_f64_list(&mut b) { b.finish().values().to_vec() } else { vec![] };
    (
        name.to_string(),
        v.as_f64(),
        v.as_str().map(str::to_string),
        arr,
    )
}

/// Transformed events as (name, value) rows, with a stable field order.
fn transformed<R: Repr>(w: &Workload) -> Vec<Row> {
    let mut ctx = R::new_ctx(&w.spec);
    let mut recs = build_n::<R>(&mut ctx, &w.events, w.events.len());
    R::transform(&mut ctx, &mut recs);
    recs.iter()
        .map(|r| {
            let mut row: Row = w
                .spec
                .steps
                .renames
                .iter()
                .map(|(_, n)| *n)
                .chain(w.spec.decl.iter().map(|f| f.name))
                .filter_map(|n| R::get_by_name(&ctx, r, n).map(|v| describe(n, v)))
                .collect();
            row.sort_by(|a, b| a.0.cmp(&b.0));
            row.dedup_by(|a, b| a.0 == b.0);
            row
        })
        .collect()
}

fn arrow<R: Repr>(w: &Workload) -> RecordBatch {
    let mut ctx = R::new_ctx(&w.spec);
    let recs = build_n::<R>(&mut ctx, &w.events, w.events.len());
    R::to_arrow(&mut ctx, &recs)
}

fn check(w: &Workload) {
    let a = transformed::<Keyed<ext::Value, true>>(w);
    assert_eq!(a, transformed::<Keyed<ext::Value, false>>(w));
    assert_eq!(a, transformed::<Shared<ext::Value>>(w));
    assert_eq!(a, transformed::<ById<ext::Value>>(w));
    let m = transformed::<Keyed<min::Value, true>>(w);
    assert_eq!(m, transformed::<Shared<min::Value>>(w));
    assert_eq!(m, transformed::<ById<min::Value>>(w));
    assert!(!a.is_empty() && a.len() < w.events.len(), "filter must drop some events");

    let base = arrow::<Keyed<ext::Value, true>>(w);
    assert_eq!(base, arrow::<Keyed<ext::Value, false>>(w));
    assert_eq!(base, arrow::<Shared<ext::Value>>(w));
    assert_eq!(base, arrow::<ById<ext::Value>>(w));
    assert_eq!(base, arrow::<Keyed<min::Value, true>>(w));
    assert_eq!(base, arrow::<Shared<min::Value>>(w));
    assert_eq!(base, arrow::<ById<min::Value>>(w));
    assert_eq!(base.num_rows(), w.events.len());
}

#[test]
fn w1_equivalent() {
    check(&workload::w1());
}

#[test]
fn w2_equivalent() {
    check(&workload::w2());
}
