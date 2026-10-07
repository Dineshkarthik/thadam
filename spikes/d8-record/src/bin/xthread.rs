//! Cross-thread cost / Arc refcount contention. Plain system allocator.
//!
//! Scenario "ring": T threads. Each round a thread builds a batch of events
//! (decode from pre-parsed JSON), sends it to the next thread, then receives a
//! batch from the previous thread, runs the transform pass on it and drops it.
//! So every event is created on one thread and transformed + freed on another.
//!   shared  = every thread uses a clone of one ctx (shape / name Arcs shared
//!             by all threads: worst case for refcount contention)
//!   private = every thread has its own ctx (per-chain registry, as planned)
//!
//! Scenario "storm": T threads repeatedly clone + drop records out of one
//! shared, read-only set of 1024 records (fan-out of the same events to many
//! consumers: the worst case for refcounts that live inside records).
//!
//! Usage: cargo run --release --bin xthread [-- REPS RING_EVENTS_PER_THREAD STORM_OPS_PER_THREAD]

use d8_record::repr::Repr;
use d8_record::workload::{self, Workload};
use d8_record::{build_n, for_each_repr};
use std::hint::black_box;
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const BATCH: usize = 256;
const THREADS: [usize; 4] = [1, 2, 4, 8];

fn ring<R: Repr>(w: &Workload, shared: bool, threads: usize, per_thread_events: usize) -> f64 {
    let template = R::new_ctx(&w.spec);
    let events = Arc::new(w.events.clone());
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut txs = Vec::new();
    let mut rxs = Vec::new();
    for _ in 0..threads {
        let (tx, rx) = sync_channel::<Vec<R::Rec>>(4);
        txs.push(tx);
        rxs.push(Some(rx));
    }
    let rounds = per_thread_events / BATCH;
    let mut handles = Vec::new();
    for t in 0..threads {
        let rx = rxs[t].take().expect("rx");
        let tx = txs[(t + 1) % threads].clone();
        let mut ctx = if shared { template.clone() } else { R::new_ctx(&w.spec) };
        let events = events.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            let len = events.len();
            let mut idx = (t * 97) % len;
            barrier.wait();
            for _ in 0..rounds {
                let mut b = Vec::with_capacity(BATCH);
                for _ in 0..BATCH {
                    b.push(R::build(&mut ctx, &events[idx]));
                    idx = (idx + 1) % len;
                }
                tx.send(b).expect("send");
                let mut inb = rx.recv().expect("recv");
                R::transform(&mut ctx, &mut inb);
                drop(inb);
            }
            barrier.wait();
        }));
    }
    drop(txs);
    barrier.wait();
    let t0 = Instant::now();
    barrier.wait();
    let dt = t0.elapsed().as_secs_f64();
    for h in handles {
        h.join().expect("join");
    }
    (threads * rounds * BATCH) as f64 / dt
}

fn storm<R: Repr>(w: &Workload, threads: usize, ops_per_thread: usize) -> f64 {
    let mut ctx = R::new_ctx(&w.spec);
    let set: Arc<Vec<R::Rec>> = Arc::new(build_n::<R>(&mut ctx, &w.events, 1024));
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::new();
    for t in 0..threads {
        let set = set.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            let mut idx = (t * 128) & 1023;
            barrier.wait();
            for _ in 0..ops_per_thread {
                let c = set[idx].clone();
                black_box(&c);
                drop(c);
                idx = (idx + 1) & 1023;
            }
            barrier.wait();
        }));
    }
    barrier.wait();
    let t0 = Instant::now();
    barrier.wait();
    let dt = t0.elapsed().as_secs_f64();
    for h in handles {
        h.join().expect("join");
    }
    (threads * ops_per_thread) as f64 / dt
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    v[v.len() / 2]
}

struct Cell {
    med: f64,
    min: f64,
    max: f64,
}

fn measure(reps: usize, mut f: impl FnMut() -> f64) -> Cell {
    let _warm = f();
    let mut xs: Vec<f64> = (0..reps).map(|_| f()).collect();
    let min = xs.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = xs.iter().cloned().fold(0.0, f64::max);
    Cell { med: median(&mut xs), min, max }
}

fn fmt_row(prefix: &str, cells: &[Cell]) {
    let base = cells[0].med;
    let mut s = String::from(prefix);
    for (c, t) in cells.iter().zip(THREADS) {
        let eff = 100.0 * c.med / (base * t as f64);
        s.push_str(&format!(
            " {:.2} ({:.0}%) [{:.2}-{:.2}] |",
            c.med / 1e6,
            eff,
            c.min / 1e6,
            c.max / 1e6
        ));
    }
    println!("{s}");
}

fn main() {
    let args: Vec<usize> = std::env::args().skip(1).filter_map(|s| s.parse().ok()).collect();
    let reps = args.first().copied().unwrap_or(5);
    let ring_events = args.get(1).copied().unwrap_or(200_000);
    let storm_ops = args.get(2).copied().unwrap_or(1_000_000);

    println!("reps={reps} ring_events_per_thread={ring_events} storm_ops_per_thread={storm_ops}\n");
    println!("Cell format: median M ops/s total (parallel efficiency vs T=1 x T) [min-max over reps]\n");

    println!("## Scenario ring: build on thread i, transform + drop on thread i+1 (M events/s total)\n");
    println!("| wl | values | repr | ctx | T=1 | T=2 | T=4 | T=8 |");
    println!("|---|---|---|---|---|---|---|---|");
    for w in [workload::w1(), workload::w2()] {
        macro_rules! run_ring {
            ($v:literal, $r:ty) => {
                for shared in [true, false] {
                    let cells: Vec<Cell> = THREADS
                        .iter()
                        .map(|&t| measure(reps, || ring::<$r>(&w, shared, t, ring_events)))
                        .collect();
                    fmt_row(
                        &format!(
                            "| {} | {} | {} | {} |",
                            w.spec.name,
                            $v,
                            <$r as Repr>::NAME,
                            if shared { "shared" } else { "private" }
                        ),
                        &cells,
                    );
                }
            };
        }
        for_each_repr!(run_ring);
    }

    println!("\n## Scenario storm: clone + drop from a shared record set (M clone+drop/s total)\n");
    println!("| wl | values | repr | T=1 | T=2 | T=4 | T=8 |");
    println!("|---|---|---|---|---|---|---|");
    for w in [workload::w1(), workload::w2()] {
        macro_rules! run_storm {
            ($v:literal, $r:ty) => {
                let cells: Vec<Cell> = THREADS
                    .iter()
                    .map(|&t| measure(reps, || storm::<$r>(&w, t, storm_ops)))
                    .collect();
                fmt_row(
                    &format!("| {} | {} | {} |", w.spec.name, $v, <$r as Repr>::NAME),
                    &cells,
                );
            };
        }
        for_each_repr!(run_storm);
    }
}
