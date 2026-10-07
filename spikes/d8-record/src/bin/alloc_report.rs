//! Allocation / memory report. Uses the counting allocator, single-threaded.
//!
//! Usage: cargo run --release --bin alloc_report [-- N]   (default N = 10000)

use d8_record::alloc::{CountingAlloc, Snapshot};
use d8_record::repr::Repr;
use d8_record::value::{ext, min};
use d8_record::workload::{self, Workload};
use d8_record::{build_n, for_each_repr, spec_arc};
use std::mem::size_of;
use std::sync::Arc;

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

#[allow(dead_code)]
enum CompactAlt {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(Arc<str>),
    F64s(Arc<[f64]>),
    List(Box<[CompactAlt]>),
}

fn per(x: f64, n: usize) -> f64 {
    x / n as f64
}

fn report<R: Repr>(w: &Workload, val: &str, n: usize) {
    let mut ctx = R::new_ctx(&spec_arc(w));
    let len = w.events.len();

    let mut recs: Vec<R::Rec> = Vec::with_capacity(n);
    let s0 = Snapshot::now();
    for i in 0..n {
        recs.push(R::build(&mut ctx, &w.events[i % len]));
    }
    let build = Snapshot::now().since(s0);

    // Warm-up pass: the first clone of a `Bytes` built by copy_from_slice
    // promotes it to the shared representation (one extra allocation). Real
    // WAL-slice Bytes are already shared, so exclude that one-off cost.
    drop(recs.clone());

    let mut cloned: Vec<R::Rec> = Vec::with_capacity(n);
    let s0 = Snapshot::now();
    for r in &recs {
        cloned.push(r.clone());
    }
    let clone = Snapshot::now().since(s0);

    // Transform on events whose arrays are shared with `recs` (refcount > 1):
    // `Arc<[f64]>` cannot be converted in place and falls back to a copy.
    let s0 = Snapshot::now();
    R::transform(&mut ctx, &mut cloned);
    let xform_shared = Snapshot::now().since(s0);
    drop(cloned);

    // Transform on uniquely owned events (the normal pipeline case).
    let mut fresh = build_n::<R>(&mut ctx, &w.events, n);
    let s0 = Snapshot::now();
    R::transform(&mut ctx, &mut fresh);
    let xform = Snapshot::now().since(s0);
    let kept = fresh.len();
    drop(fresh);

    let s0 = Snapshot::now();
    let batch = R::to_arrow(&mut ctx, &recs);
    let arrow = Snapshot::now().since(s0);
    let rows = batch.num_rows();
    drop(batch);

    let inline = size_of::<R::Rec>();
    let heap = per(build.live_usable as f64, n);
    println!(
        "| {} | {} | {} | {} | {:.2} | {:.0} | {:.0} | {:.0} | {:.0} | {:.2} | {:.2} | {:.2} | {:.1}% | {:.2} | {:.0} |",
        w.spec.name,
        val,
        R::NAME,
        inline,
        per(build.allocs as f64, n),
        per(build.bytes_requested as f64, n),
        per(build.live_requested as f64, n),
        heap,
        inline as f64 + heap,
        per(clone.allocs as f64, n),
        per(xform.allocs as f64, n),
        per(xform_shared.allocs as f64, n),
        100.0 * kept as f64 / n as f64,
        per(arrow.allocs as f64, rows),
        per(arrow.bytes_requested as f64, rows),
    );
}

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);

    println!("## Value sizes (bytes, inline)\n");
    println!("| type | size_of |");
    println!("|---|---|");
    println!("| bytes::Bytes | {} |", size_of::<bytes::Bytes>());
    println!("| Arc<str> | {} |", size_of::<Arc<str>>());
    println!("| Arc<[f64]> | {} |", size_of::<Arc<[f64]>>());
    println!("| Vec<_> | {} |", size_of::<Vec<u8>>());
    println!("| min::Value (Str = Utf8Bytes(Bytes), as in plan) | {} |", size_of::<min::Value>());
    println!("| ext::Value (Str = Utf8Bytes(Bytes), as in plan) | {} |", size_of::<ext::Value>());
    println!("| compact alt (Str = Arc<str>, List = Box<[_]>), size only | {} |", size_of::<CompactAlt>());
    println!(
        "| Record A (Vec<(Arc<str>, ext::Value)>) | {} |",
        size_of::<<d8_record::repr::Keyed<ext::Value, true> as Repr>::Rec>()
    );
    println!(
        "| Record B (Arc<Shape> + Vec) | {} |",
        size_of::<<d8_record::repr::Shared<ext::Value> as Repr>::Rec>()
    );
    println!(
        "| Record C (u32 + Vec) | {} |",
        size_of::<<d8_record::repr::ById<ext::Value> as Repr>::Rec>()
    );

    println!("\n## Allocation and memory per event (N = {n}, single thread)\n");
    println!(
        "Columns: `live B req` = requested bytes still live after construction; `live B usable` = malloc_size of those blocks (includes size-class rounding); \
`total B/ev` = sizeof(Rec) + live B usable; `xform allocs` = uniquely owned events, `xform(shared)` = events whose arrays are shared with a clone.\n"
    );
    println!(
        "| wl | values | repr | sizeof(Rec) | build allocs/ev | build B req/ev | live B req/ev | live B usable/ev | total B/ev | clone allocs/ev | xform allocs/ev | xform(shared) allocs/ev | kept | arrow allocs/row | arrow B req/row |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");

    for w in [workload::w1(), workload::w2()] {
        macro_rules! run {
            ($v:literal, $r:ty) => {
                report::<$r>(&w, $v, n)
            };
        }
        for_each_repr!(run);
    }
}
