//! Correctness guard: every engine must agree with a hand-written Rust reference on every
//! event before any timing is meaningful. Run: `cargo run --release`.

use adr018_filter::cases::{CASES, Case, WIDE_CASE};
use adr018_filter::cel_engine::CelFilter;
use adr018_filter::model::{Event, gen_demo, gen_wide};
use adr018_filter::native;

fn check(case: &Case, events: &[Event]) {
    let shape = events[0].shape.clone();
    let cel = CelFilter::compile(case.cel, &shape).expect("cel compile");
    let spec = native::parse_yaml(case.yaml).expect("yaml parse");
    let nat = native::compile(&spec, &shape).expect("native compile");
    #[cfg(feature = "evalexpr")]
    let ee = case.evalexpr.map(|(src, names)| {
        adr018_filter::ee::EvalExprFilter::compile(src, names, &shape).expect("evalexpr compile")
    });

    let (mut pass, mut bad) = (0usize, [0usize; 6]);
    for e in events {
        let want = (case.reference)(e);
        pass += want as usize;
        if cel.ctx_all(e) != Some(want) {
            bad[0] += 1;
        }
        if cel.ctx_refs(e) != Some(want) {
            bad[1] += 1;
        }
        if cel.resolver(e) != Some(want) {
            bad[2] += 1;
        }
        if cel.eval_prebuilt(&cel.prebuilt_context(e)) != Some(want) {
            bad[3] += 1;
        }
        if nat.eval(e) != want {
            bad[4] += 1;
        }
        #[cfg(feature = "evalexpr")]
        if let Some(ee) = &ee {
            if ee.eval(e) != Some(want) {
                bad[5] += 1;
            }
        }
    }
    println!(
        "{:20} n={} pass={:5.1}% refs={} mismatches: cel_ctx_all={} cel_ctx_refs={} cel_resolver={} cel_eval_only={} native={} evalexpr={}",
        case.name,
        events.len(),
        100.0 * pass as f64 / events.len() as f64,
        cel.n_refs(),
        bad[0],
        bad[1],
        bad[2],
        bad[3],
        bad[4],
        if case.evalexpr.is_some() {
            bad[5].to_string()
        } else {
            "n/a".into()
        }
    );
    assert!(
        bad.iter().all(|b| *b == 0),
        "engines disagree with the reference"
    );
}

fn main() {
    let events = gen_demo(10_000, 42);
    for c in CASES {
        check(c, &events);
    }
    check(&WIDE_CASE, &gen_wide(10_000, 7));
    println!("all engines agree with the reference on every event");
}
