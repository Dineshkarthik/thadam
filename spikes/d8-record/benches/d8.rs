//! Criterion benchmarks, single thread, system allocator.
//!
//! Group id  : "<op>/<workload>/<value set>"
//! Function  : representation name
//! Elements  : events per iteration (lookup_all: lookups per iteration)

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use d8_record::repr::Repr;
use d8_record::value::Ty;
use d8_record::workload::{self, Workload};
use d8_record::{build_n, for_each_repr};
use std::cell::RefCell;
use std::hint::black_box;
use std::time::Duration;

const N: usize = 1024;
const N_ARROW: usize = 10_000;

fn bench_repr<R: Repr>(c: &mut Criterion, w: &Workload, val: &str) {
    let wl = w.spec.name;
    let id = |op: &str| format!("{op}/{wl}/{val}");
    let spec = &w.spec;

    // construct
    {
        let mut g = c.benchmark_group(id("construct"));
        g.throughput(Throughput::Elements(N as u64));
        g.bench_function(R::NAME, |b| {
            let mut ctx = R::new_ctx(spec);
            b.iter_with_large_drop(|| build_n::<R>(&mut ctx, &w.events, N));
        });
        g.finish();
    }

    // lookup by name vs cached index
    {
        let mut ctx = R::new_ctx(spec);
        let recs = build_n::<R>(&mut ctx, &w.events, N);
        let target = spec
            .decl
            .iter()
            .rev()
            .find(|f| f.ty != Ty::Str)
            .map_or("", |f| f.name);
        let names: Vec<&str> = spec.decl.iter().map(|f| f.name).collect();

        let mut g = c.benchmark_group(id("lookup_one"));
        g.throughput(Throughput::Elements(N as u64));
        g.bench_function(format!("{}/by-name", R::NAME), |b| {
            b.iter(|| {
                for r in &recs {
                    black_box(R::get_by_name(&ctx, r, target));
                }
            })
        });
        if R::CACHED_INDEX {
            let sample = &recs[0];
            let idx = R::resolve(&ctx, sample, target).unwrap_or(0);
            g.bench_function(format!("{}/cached-idx", R::NAME), |b| {
                b.iter(|| {
                    for r in &recs {
                        if R::same_shape(sample, r) {
                            black_box(R::get_idx(r, idx));
                        } else {
                            black_box(R::get_by_name(&ctx, r, target));
                        }
                    }
                })
            });
        }
        g.finish();

        let mut g = c.benchmark_group(id("lookup_all"));
        g.throughput(Throughput::Elements((N * names.len()) as u64));
        g.bench_function(format!("{}/by-name", R::NAME), |b| {
            b.iter(|| {
                for r in &recs {
                    for n in &names {
                        black_box(R::get_by_name(&ctx, r, n));
                    }
                }
            })
        });
        if R::CACHED_INDEX {
            let sample = &recs[0];
            let idxs: Vec<usize> = names
                .iter()
                .map(|n| R::resolve(&ctx, sample, n).unwrap_or(0))
                .collect();
            g.bench_function(format!("{}/cached-idx", R::NAME), |b| {
                b.iter(|| {
                    for r in &recs {
                        if R::same_shape(sample, r) {
                            for &i in &idxs {
                                black_box(R::get_idx(r, i));
                            }
                        }
                    }
                })
            });
        }
        g.finish();
    }

    // transform: rename + unit conversion + filter, on uniquely-owned events
    {
        let mut g = c.benchmark_group(id("transform"));
        g.throughput(Throughput::Elements(N as u64));
        g.bench_function(R::NAME, |b| {
            let ctx = RefCell::new(R::new_ctx(spec));
            b.iter_batched(
                || build_n::<R>(&mut ctx.borrow_mut(), &w.events, N),
                |mut v| {
                    R::transform(&mut ctx.borrow_mut(), &mut v);
                    v
                },
                BatchSize::LargeInput,
            );
        });
        g.finish();
    }

    // clone and drop
    {
        let mut ctx = R::new_ctx(spec);
        let recs = build_n::<R>(&mut ctx, &w.events, N);
        let mut g = c.benchmark_group(id("clone"));
        g.throughput(Throughput::Elements(N as u64));
        g.bench_function(R::NAME, |b| b.iter_with_large_drop(|| recs.clone()));
        g.finish();

        let mut g = c.benchmark_group(id("drop"));
        g.throughput(Throughput::Elements(N as u64));
        g.bench_function(R::NAME, |b| {
            let ctx = RefCell::new(R::new_ctx(spec));
            b.iter_batched(
                || build_n::<R>(&mut ctx.borrow_mut(), &w.events, N),
                drop,
                BatchSize::LargeInput,
            );
        });
        g.finish();
    }

    // Arrow RecordBatch conversion, 10k events
    {
        let mut ctx = R::new_ctx(spec);
        let recs = build_n::<R>(&mut ctx, &w.events, N_ARROW);
        let mut g = c.benchmark_group(id("arrow"));
        g.throughput(Throughput::Elements(N_ARROW as u64));
        g.bench_function(R::NAME, |b| {
            b.iter_with_large_drop(|| R::to_arrow(&mut ctx, &recs));
        });
        g.finish();
    }
}

fn bench_parse(c: &mut Criterion, w: &Workload) {
    let ser = workload::serialized(w);
    let mut g = c.benchmark_group(format!("parse_only/{}", w.spec.name));
    g.throughput(Throughput::Elements(ser.len() as u64));
    g.bench_function("serde_json::from_slice->Value", |b| {
        b.iter_with_large_drop(|| {
            ser.iter()
                .map(|s| serde_json::from_slice::<serde_json::Value>(s).ok())
                .collect::<Vec<_>>()
        })
    });
    g.finish();
}

fn all(c: &mut Criterion) {
    for w in [workload::w1(), workload::w2()] {
        bench_parse(c, &w);
        macro_rules! run {
            ($v:literal, $r:ty) => {
                bench_repr::<$r>(c, &w, $v)
            };
        }
        for_each_repr!(run);
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_millis(1500))
        .sample_size(30);
    targets = all
}
criterion_main!(benches);
