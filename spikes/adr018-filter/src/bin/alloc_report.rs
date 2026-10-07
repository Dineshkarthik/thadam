//! Allocations and bytes requested per event for every engine and strategy (counting global
//! allocator, single thread). `cargo run --release --bin alloc_report`.

use adr018_filter::cases::{CASES, Case, WIDE_CASE};
use adr018_filter::cel_engine::CelFilter;
use adr018_filter::model::{Event, gen_demo, gen_wide};
use adr018_filter::native;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(l.size() as u64, Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(l.size() as u64, Relaxed);
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        BYTES.fetch_add(n as u64, Relaxed);
        unsafe { System.realloc(p, l, n) }
    }
}
#[global_allocator]
static A: Counting = Counting;

fn snap() -> (u64, u64) {
    (ALLOCS.load(Relaxed), BYTES.load(Relaxed))
}

fn per_event<F: FnMut(&Event) -> bool>(events: &[Event], mut f: F) -> (f64, f64) {
    // warm up lazy statics and caches, then measure.
    for e in events.iter().take(64) {
        std::hint::black_box(f(e));
    }
    let (a0, b0) = snap();
    let mut n = 0usize;
    for e in events {
        n += f(e) as usize;
    }
    std::hint::black_box(n);
    let (a1, b1) = snap();
    (
        (a1 - a0) as f64 / events.len() as f64,
        (b1 - b0) as f64 / events.len() as f64,
    )
}

fn row(name: &str, (a, b): (f64, f64)) {
    println!("| {name} | {a:.2} | {b:.0} |");
}

fn run(group: &str, case: &Case, events: &[Event]) {
    let shape = events[0].shape.clone();
    let cel = CelFilter::compile(case.cel, &shape).unwrap();
    let nat = native::compile(&native::parse_yaml(case.yaml).unwrap(), &shape).unwrap();
    let prebuilt: Vec<_> = events.iter().map(|e| cel.prebuilt_context(e)).collect();
    println!(
        "\n#### {group}/{} (`{}`), {} events\n",
        case.name,
        case.cel,
        events.len()
    );
    println!("| strategy | allocs/event | bytes requested/event |");
    println!("|---|---|---|");
    row("native", per_event(events, |e| nat.eval(e)));
    // eval-only: iterate indexes so each event uses its own pre-built context
    let mut i = 0usize;
    row(
        "cel_eval_only",
        per_event(events, |_| {
            let r = cel.eval_prebuilt(&prebuilt[i % prebuilt.len()]) == Some(true);
            i += 1;
            r
        }),
    );
    row(
        "cel_resolver",
        per_event(events, |e| cel.resolver(e) == Some(true)),
    );
    row(
        "cel_ctx_refs",
        per_event(events, |e| cel.ctx_refs(e) == Some(true)),
    );
    row(
        "cel_ctx_all",
        per_event(events, |e| cel.ctx_all(e) == Some(true)),
    );
    #[cfg(feature = "evalexpr")]
    if let Some((src, names)) = case.evalexpr {
        let ee = adr018_filter::ee::EvalExprFilter::compile(src, names, &shape).unwrap();
        row(
            "evalexpr_refs",
            per_event(events, |e| ee.eval(e) == Some(true)),
        );
    }
}

/// Which part of a CEL expression allocates during evaluation (bindings prebuilt, so only
/// the interpreter is measured).
fn isolate(events: &[Event]) {
    println!(
        "\n#### isolation: allocations of `cel_eval_only` per evaluation by expression form\n"
    );
    println!("| expression | allocs/event | bytes requested/event |");
    println!("|---|---|---|");
    let shape = events[0].shape.clone();
    for src in [
        "true",
        "cpu_temp_c >= 0.0",
        "cpu_temp_c >= -40.0",
        "cpu_temp_c >= 0.0 && cpu_temp_c <= 150.0",
        "cpu_temp_c >= -40.0 && cpu_temp_c <= 150.0",
        "level != 'debug'",
        "level == 'info'",
        "soc_pct > 10.0",
    ] {
        let cel = CelFilter::compile(src, &shape).unwrap();
        let prebuilt: Vec<_> = events.iter().map(|e| cel.prebuilt_context(e)).collect();
        let mut i = 0usize;
        let r = per_event(events, |_| {
            let r = cel.eval_prebuilt(&prebuilt[i % prebuilt.len()]) == Some(true);
            i += 1;
            r
        });
        row(&format!("`{src}`"), r);
    }
}

fn main() {
    let demo = gen_demo(10_000, 42);
    for c in CASES {
        run("demo", c, &demo);
    }
    run("wide", &WIDE_CASE, &gen_wide(10_000, 7));
    isolate(&demo);
}
