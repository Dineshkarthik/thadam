//! Pathological-input probes, one per process (a stack overflow aborts the process, so each
//! case runs as a child: see `scripts/stress.sh`). `stress <kind> <n>`.
//!
//! kinds: parens | and | unary | list | strcat | nested_all | pred_not | pred_all

use adr018_filter::model::gen_demo;
use adr018_filter::native;
use cel::{Context, Env};
use std::sync::Arc;
use std::time::Instant;

fn cel_case(src: &str, label: &str) {
    let env = Arc::new(Env::stdlib());
    let t = Instant::now();
    let prog = env.compile(src);
    let c_el = t.elapsed();
    match prog {
        Err(e) => {
            let msg = e.to_string();
            println!(
                "{label}: len={} compile ERR after {c_el:?}: {}",
                src.len(),
                msg.lines().next().unwrap_or("")
            );
        }
        Ok(p) => {
            let ctx = Context::with_env(env);
            let t = Instant::now();
            let r = p.execute(&ctx);
            let e_el = t.elapsed();
            let rs = match r {
                Ok(v) => format!("{v:?}"),
                Err(e) => format!("ERR {e}"),
            };
            let rs: String = rs.chars().take(60).collect();
            println!(
                "{label}: len={} compile OK {c_el:?}, execute {e_el:?} -> {rs}",
                src.len()
            );
        }
    }
}

fn main() {
    // Optional third argument: run on a thread with that stack size in KiB (tokio worker
    // threads default to 2 MiB; the process main thread has 8 MiB on macOS and Linux).
    let a: Vec<String> = std::env::args().collect();
    match a.get(3).and_then(|s| s.parse::<usize>().ok()) {
        Some(kib) => {
            let h = std::thread::Builder::new()
                .stack_size(kib * 1024)
                .spawn(move || run(a))
                .unwrap();
            h.join().unwrap();
        }
        None => run(a),
    }
}

fn run(a: Vec<String>) {
    let kind = a[1].as_str();
    let n: usize = a[2].parse().unwrap();
    let label = format!("{kind} n={n}");
    match kind {
        "parens" => cel_case(&format!("{}1{} == 1", "(".repeat(n), ")".repeat(n)), &label),
        "and" => cel_case(&vec!["true"; n].join(" && "), &label),
        "unary" => cel_case(&format!("{}true", "!".repeat(n)), &label),
        "list" => cel_case(&format!("size({}{})", "[".repeat(n), "]".repeat(n)), &label),
        "strcat" => cel_case(
            &format!("size({})", vec!["'aaaaaaaaaaaaaaaa'"; n].join(" + ")),
            &label,
        ),
        "nested_all" => {
            // k nested comprehensions over a 50-element list: work grows as 50^k.
            let list = format!(
                "[{}]",
                (0..50).map(|i| i.to_string()).collect::<Vec<_>>().join(",")
            );
            let mut e = String::from("true");
            for i in 0..n {
                e = format!("{list}.all(x{i}, {e})");
            }
            cel_case(&e, &label);
        }
        "pred_not" | "pred_all" => {
            let (open, close) = if kind == "pred_not" {
                ("{ not: ".repeat(n), " }".repeat(n))
            } else {
                ("{ all: [".repeat(n), "] }".repeat(n))
            };
            let y = format!("{open}{{ field: level, ne: debug }}{close}");
            let t = Instant::now();
            match native::parse_yaml(&y) {
                Err(e) => println!(
                    "{label}: yaml ERR after {:?}: {}",
                    t.elapsed(),
                    e.lines().next().unwrap_or("")
                ),
                Ok(spec) => {
                    let shape = gen_demo(1, 1)[0].shape.clone();
                    match native::compile(&spec, &shape) {
                        Ok(node) => {
                            let e = gen_demo(1, 1).remove(0);
                            println!(
                                "{label}: yaml+compile OK {:?}, eval -> {}",
                                t.elapsed(),
                                node.eval(&e)
                            );
                        }
                        Err(errs) => println!("{label}: compile ERR {:?}", errs.first()),
                    }
                }
            }
        }
        other => eprintln!("unknown kind {other}"),
    }
}
