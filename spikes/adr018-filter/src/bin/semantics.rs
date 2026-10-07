//! Semantics and diagnostics probes for ADR-0018: numeric comparison, null and missing
//! fields, determinism, evaluation errors, and compile-time error text.
//! `cargo run --release --bin semantics`

use adr018_filter::model::{Event, Value, gen_demo};
use adr018_filter::native;
use cel::{Context, Env};
use std::sync::Arc;

fn cel_run(expr: &str, vars: &[(&str, cel::Value)]) -> String {
    let env = Arc::new(Env::stdlib());
    let prog = match env.compile(expr) {
        Ok(p) => p,
        Err(e) => return format!("COMPILE ERROR: {}", e.to_string().replace('\n', " | ")),
    };
    let mut ctx = Context::with_env(env);
    for (k, v) in vars {
        ctx.add_variable_from_value(*k, v.clone());
    }
    match prog.execute(&ctx) {
        Ok(v) => format!("{v:?}"),
        Err(e) => format!("RUNTIME ERROR: {e}"),
    }
}

fn md(s: &str) -> String {
    s.replace('|', "\\|")
}

fn numeric_and_null() {
    println!("### CEL (`cel` 0.15.0): numeric, null, missing, error semantics\n");
    println!("| expression | bound variables | result |");
    println!("|---|---|---|");
    let f32_01 = cel::Value::Float(0.1f32 as f64);
    let rows: Vec<(&str, Vec<(&str, cel::Value)>)> = vec![
        ("1 < 1.5", vec![]),
        ("1 == 1.0", vec![]),
        ("x >= -40", vec![("x", cel::Value::Float(-39.5))]),
        ("9223372036854775808u > 9223372036854775807", vec![]),
        (
            "big > 9223372036854775807",
            vec![("big", cel::Value::UInt(u64::MAX))],
        ),
        (
            "big == 18446744073709551615u",
            vec![("big", cel::Value::UInt(u64::MAX))],
        ),
        ("x == 0.1", vec![("x", f32_01.clone())]),
        ("x == 0.10000000149011612", vec![("x", f32_01.clone())]),
        ("9223372036854775807 + 1", vec![]),
        ("1 / 0", vec![]),
        ("1.0 / 0.0", vec![]),
        ("code == null", vec![("code", cel::Value::Null)]),
        ("code == 'E-1'", vec![("code", cel::Value::Null)]),
        ("code != 'E-1'", vec![("code", cel::Value::Null)]),
        ("level != 'debug'", vec![("level", cel::Value::Null)]),
        ("nope == 1", vec![]),
        (
            "q.nope == 1",
            vec![(
                "q",
                cel::Value::from(std::collections::HashMap::from([("status", "good")])),
            )],
        ),
        (
            "has(q.nope)",
            vec![(
                "q",
                cel::Value::from(std::collections::HashMap::from([("status", "good")])),
            )],
        ),
        ("x > 'hot'", vec![("x", cel::Value::Float(1.0))]),
        ("false && (1 / 0 > 0)", vec![]),
        ("(1 / 0 > 0) || true", vec![]),
        ("(1 / 0 > 0) && false", vec![]),
        ("now()", vec![]),
        ("rand()", vec![]),
        ("random()", vec![]),
        (
            "timestamp('2026-01-01T00:00:00Z') < timestamp('2027-01-01T00:00:00Z')",
            vec![],
        ),
        (
            "'joint 3 over temperature'.matches('^joint [0-9]+ over')",
            vec![],
        ),
        ("'abc'.contains('b')", vec![]),
        (
            "t.all(v, v > 0.0)",
            vec![(
                "t",
                cel::Value::from(vec![cel::Value::Float(1.0), cel::Value::Float(2.0)]),
            )],
        ),
        (
            "t.exists(v, v > 1.5) && size(t) == 2",
            vec![(
                "t",
                cel::Value::from(vec![cel::Value::Float(1.0), cel::Value::Float(2.0)]),
            )],
        ),
    ];
    for (expr, vars) in rows {
        let vs: Vec<String> = vars.iter().map(|(k, v)| format!("{k}={v:?}")).collect();
        println!(
            "| `{}` | {} | {} |",
            md(expr),
            md(&vs.join(", ")),
            md(&cel_run(expr, &vars))
        );
    }
}

fn cel_compile_errors() {
    println!("\n### CEL compile-time diagnostics (`Env::compile`)\n");
    println!("| source | result |");
    println!("|---|---|");
    for src in [
        "level != ",
        "level = 'debug'",
        "level != 'debug' &&",
        "(level != 'debug'",
        "level != 'debug",
        "level != 'debug' and soc_pct > 1",
        "lvl != 'debug'",
        "unknown_fn(level)",
        "level.startsWith(",
        "level != 'debug' ? 1 : 2",
        "1 +",
        "[1, 2",
        "a.b.c.d == 1",
        "level != 'debug' &&\n  robot !=",
    ] {
        let env = Env::stdlib();
        let r = match env.compile(src) {
            Ok(p) => {
                let vars: Vec<String> = p
                    .references()
                    .variables()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect();
                format!("compiles; references variables {vars:?}")
            }
            Err(e) => format!("ERROR(s): {}", e.to_string().replace('\n', " ¶ ")),
        };
        println!("| `{}` | {} |", md(&src.replace('\n', "\\n")), md(&r));
    }
    let env = Env::stdlib();
    if let Err(e) = env.compile("level != 'debug' &&\n  robot !=") {
        println!(
            "\nStructured positions for the last case: {:?}",
            e.errors.iter().map(|x| x.pos).collect::<Vec<_>>()
        );
    }
}

fn event_with(f: impl FnOnce(&mut Event)) -> Event {
    let mut e = gen_demo(1, 1).remove(0);
    f(&mut e);
    e
}

fn native_probes() {
    println!("\n### Structured predicates (this spike's evaluator): the same questions\n");
    println!("| predicate | event override | result |");
    println!("|---|---|---|");
    let shape = gen_demo(1, 1)[0].shape.clone();
    let probes: Vec<(&str, &str, Event)> = vec![
        (
            "{ field: soc_pct, eq: 0.1 }",
            "soc_pct = f32 nearest 0.1",
            event_with(|e| e.values[8] = Value::F32(0.1)),
        ),
        (
            "{ field: soc_pct, eq: 0.10000000149011612 }",
            "soc_pct = f32 nearest 0.1",
            event_with(|e| e.values[8] = Value::F32(0.1)),
        ),
        (
            "{ field: seq, gt: 9223372036854775807 }",
            "seq = u64::MAX",
            event_with(|e| e.values[7] = Value::U64(u64::MAX)),
        ),
        (
            "{ field: seq, eq: 18446744073709551615 }",
            "seq = u64::MAX",
            event_with(|e| e.values[7] = Value::U64(u64::MAX)),
        ),
        (
            "{ field: seq, eq: 18446744073709551614 }",
            "seq = u64::MAX",
            event_with(|e| e.values[7] = Value::U64(u64::MAX)),
        ),
        (
            "{ field: code, eq: E-1 }",
            "code = null",
            event_with(|e| e.values[4] = Value::Null),
        ),
        (
            "{ field: code, ne: E-1 }",
            "code = null",
            event_with(|e| e.values[4] = Value::Null),
        ),
        (
            "{ field: cpu_temp_c, min: -40 }",
            "cpu_temp_c = 3",
            event_with(|e| e.values[11] = Value::F32(3.0)),
        ),
        ("{ field: nope, eq: 1 }", "-", event_with(|_| {})),
        ("{ field: cpu_temp_c, gt: hot }", "-", event_with(|_| {})),
        ("{ field: level, gt: debug }", "-", event_with(|_| {})),
        ("{ field: level, nee: debug }", "-", event_with(|_| {})),
        ("{ field: level }", "-", event_with(|_| {})),
        (
            "{ field: level, ne: debug, eq: info }",
            "level = info",
            event_with(|e| e.values[3] = Value::Str(Arc::from("info"))),
        ),
        (
            "{ all: [], field: level, ne: debug }",
            "-",
            event_with(|_| {}),
        ),
    ];
    for (yaml, over, ev) in probes {
        let r = match native::parse_yaml(yaml) {
            Err(e) => format!("YAML/SCHEMA ERROR: {}", e.lines().next().unwrap_or("")),
            Ok(spec) => match native::compile(&spec, &shape) {
                Err(errs) => format!(
                    "COMPILE ERROR: {}",
                    errs.iter()
                        .map(|e| format!("{}: {}", e.path, e.message))
                        .collect::<Vec<_>>()
                        .join("; ")
                ),
                Ok(n) => format!("{}", n.eval(&ev)),
            },
        };
        println!("| `{}` | {} | {} |", md(yaml), md(over), md(&r));
    }
}

#[cfg(feature = "evalexpr")]
fn evalexpr_probes() {
    use evalexpr::{
        ContextWithMutableVariables, DefaultNumericTypes, HashMapContext, Value as V,
        eval_with_context,
    };
    println!("\n### evalexpr 13.1.0 (AGPL-3.0-only)\n");
    println!("| expression | variables | result |");
    println!("|---|---|---|");
    let rows: Vec<(&str, Vec<(&str, V)>)> = vec![
        ("1 < 1.5", vec![]),
        ("1 == 1.0", vec![]),
        ("x == 0.1", vec![("x", V::Float(0.1f32 as f64))]),
        ("1 / 0", vec![]),
        ("9223372036854775807 + 1", vec![]),
        ("nope == 1", vec![]),
        ("x > \"hot\"", vec![("x", V::Float(1.0))]),
        ("random()", vec![]),
        ("now()", vec![]),
        ("level != \"debug\"", vec![("level", V::Empty)]),
        ("level != ", vec![("level", V::String("x".into()))]),
        ("level = \"debug\"", vec![("level", V::String("x".into()))]),
        (
            "(level != \"debug\"",
            vec![("level", V::String("x".into()))],
        ),
    ];
    for (expr, vars) in rows {
        let mut ctx = HashMapContext::<DefaultNumericTypes>::new();
        for (k, v) in &vars {
            ctx.set_value((*k).to_string(), v.clone()).unwrap();
        }
        let r = match eval_with_context(expr, &ctx) {
            Ok(v) => format!("{v:?}"),
            Err(e) => format!("ERROR: {e}"),
        };
        let vs: Vec<String> = vars.iter().map(|(k, v)| format!("{k}={v:?}")).collect();
        println!("| `{}` | {} | {} |", md(expr), md(&vs.join(", ")), md(&r));
    }
}

fn main() {
    numeric_and_null();
    cel_compile_errors();
    native_probes();
    #[cfg(feature = "evalexpr")]
    evalexpr_probes();
}
