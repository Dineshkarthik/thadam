//! Per-event evaluation cost. Time is reported per batch of N events; criterion's element
//! throughput (Melem/s) is events per second, so ns/event = 1000 / Melem/s.

use adr018_filter::cases::{CASES, Case, WIDE_CASE};
use adr018_filter::cel_engine::CelFilter;
use adr018_filter::model::{Event, gen_demo, gen_wide};
use adr018_filter::native;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::Duration;

const N: usize = 1024;

fn run_case(c: &mut Criterion, group: &str, case: &Case, events: &[Event]) {
    let shape = events[0].shape.clone();
    let cel = CelFilter::compile(case.cel, &shape).unwrap();
    let nat = native::compile(&native::parse_yaml(case.yaml).unwrap(), &shape).unwrap();
    let prebuilt: Vec<_> = events.iter().map(|e| cel.prebuilt_context(e)).collect();
    #[cfg(feature = "evalexpr")]
    let ee = case.evalexpr.map(|(src, names)| {
        adr018_filter::ee::EvalExprFilter::compile(src, names, &shape).unwrap()
    });

    let mut g = c.benchmark_group(format!("{group}/{}", case.name));
    g.throughput(Throughput::Elements(events.len() as u64));

    g.bench_function("native", |b| {
        b.iter(|| events.iter().filter(|e| nat.eval(black_box(e))).count())
    });
    g.bench_function("cel_eval_only", |b| {
        b.iter(|| {
            prebuilt
                .iter()
                .filter(|ctx| cel.eval_prebuilt(black_box(ctx)) == Some(true))
                .count()
        })
    });
    g.bench_function("cel_resolver", |b| {
        b.iter(|| {
            events
                .iter()
                .filter(|e| cel.resolver(black_box(e)) == Some(true))
                .count()
        })
    });
    g.bench_function("cel_ctx_refs", |b| {
        b.iter(|| {
            events
                .iter()
                .filter(|e| cel.ctx_refs(black_box(e)) == Some(true))
                .count()
        })
    });
    g.bench_function("cel_ctx_all", |b| {
        b.iter(|| {
            events
                .iter()
                .filter(|e| cel.ctx_all(black_box(e)) == Some(true))
                .count()
        })
    });
    #[cfg(feature = "evalexpr")]
    if let Some(ee) = &ee {
        g.bench_function("evalexpr_refs", |b| {
            b.iter(|| {
                events
                    .iter()
                    .filter(|e| ee.eval(black_box(e)) == Some(true))
                    .count()
            })
        });
    }
    g.finish();
}

fn benches(c: &mut Criterion) {
    let demo = gen_demo(N, 42);
    for case in CASES {
        run_case(c, "demo", case, &demo);
    }
    let wide = gen_wide(N, 7);
    run_case(c, "wide", &WIDE_CASE, &wide);

    // Compile cost, once per config load or reload (not per event).
    let shape = demo[0].shape.clone();
    let mut g = c.benchmark_group("compile");
    for case in CASES {
        g.bench_with_input(BenchmarkId::new("cel", case.name), case, |b, case| {
            b.iter(|| CelFilter::compile(black_box(case.cel), &shape).unwrap())
        });
        g.bench_with_input(
            BenchmarkId::new("native_yaml_parse_and_compile", case.name),
            case,
            |b, case| {
                b.iter(|| {
                    let spec = native::parse_yaml(black_box(case.yaml)).unwrap();
                    native::compile(&spec, &shape).unwrap()
                })
            },
        );
        #[cfg(feature = "evalexpr")]
        if let Some((src, names)) = case.evalexpr {
            g.bench_with_input(
                BenchmarkId::new("evalexpr", case.name),
                &(src, names),
                |b, (src, names)| {
                    b.iter(|| {
                        adr018_filter::ee::EvalExprFilter::compile(black_box(src), names, &shape)
                            .unwrap()
                    })
                },
            );
        }
    }
    g.finish();
}

criterion_group! {
    name = filters;
    config = Criterion::default().sample_size(40).warm_up_time(Duration::from_millis(800)).measurement_time(Duration::from_secs(3));
    targets = benches
}
criterion_main!(filters);
