//! ADR-0012 spike: compare `serde_norway`, `serde_yaml_ng` and `serde-saphyr` as the
//! YAML layer under `thadam-config`. Subcommands:
//!   report            all diagnostics sections (default)
//!   bomb <crate> <n>  alias-expansion ("billion laughs") input with n levels, run as a child
//!   deep <crate> <n>  n-deep nested flow sequence, run as a child (stack overflow check)
//!   bench [iters]     parse time of the demo file per crate (build with --release)

mod collect;
mod model;
mod spans;

use model::PipelineSpec;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt::Debug;
use std::time::Instant;

const DEMO: &str = include_str!("../fixtures/pipeline.s3.yaml");

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Backend {
    Norway,
    YamlNg,
    Saphyr,
    SaphyrStrict,
}
const ALL: [Backend; 3] = [Backend::Norway, Backend::YamlNg, Backend::Saphyr];

impl Backend {
    fn name(self) -> &'static str {
        match self {
            Backend::Norway => "serde_norway",
            Backend::YamlNg => "serde_yaml_ng",
            Backend::Saphyr => "serde-saphyr",
            Backend::SaphyrStrict => "saphyr(strict)",
        }
    }
}

#[derive(Debug)]
struct Diag {
    path: String,
    msg: String,
    loc: Option<(u64, u64)>,
}

/// Parse with serde_path_to_error wrapped around each crate's deserializer.
fn parse<T: DeserializeOwned>(b: Backend, s: &str) -> Result<T, Diag> {
    match b {
        Backend::Norway => {
            let de = serde_norway::Deserializer::from_str(s);
            serde_path_to_error::deserialize::<_, T>(de).map_err(|e| {
                let path = e.path().to_string();
                let inner = e.into_inner();
                Diag {
                    path,
                    loc: inner
                        .location()
                        .map(|l| (l.line() as u64, l.column() as u64)),
                    msg: inner.to_string(),
                }
            })
        }
        Backend::YamlNg => {
            let de = serde_yaml_ng::Deserializer::from_str(s);
            serde_path_to_error::deserialize::<_, T>(de).map_err(|e| {
                let path = e.path().to_string();
                let inner = e.into_inner();
                Diag {
                    path,
                    loc: inner
                        .location()
                        .map(|l| (l.line() as u64, l.column() as u64)),
                    msg: inner.to_string(),
                }
            })
        }
        Backend::Saphyr | Backend::SaphyrStrict => {
            let opts = if b == Backend::SaphyrStrict {
                serde_saphyr::options! { with_snippet: false, strict_booleans: true }
            } else {
                serde_saphyr::options! { with_snippet: false }
            };
            // The closure must return the real error: returning Ok after a failed
            // deserialize leaves unread events and surfaces as "multiple documents".
            let mut path = String::new();
            let r = serde_saphyr::with_deserializer_from_str_with_options(s, opts, |de| {
                serde_path_to_error::deserialize::<_, T>(de).map_err(|e| {
                    path = e.path().to_string();
                    e.into_inner()
                })
            });
            r.map_err(|e| Diag {
                path,
                loc: e.location().map(|l| (l.line(), l.column())),
                msg: e.to_string(),
            })
        }
    }
}

/// Native (no serde_path_to_error) parse; returns the crate's own rendering.
fn native<T: DeserializeOwned>(b: Backend, s: &str) -> Result<T, String> {
    match b {
        Backend::Norway => serde_norway::from_str(s).map_err(|e| e.to_string()),
        Backend::YamlNg => serde_yaml_ng::from_str(s).map_err(|e| e.to_string()),
        Backend::Saphyr | Backend::SaphyrStrict => {
            serde_saphyr::from_str(s).map_err(|e| e.to_string())
        }
    }
}

fn one_line(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn show<T: DeserializeOwned + Serialize>(title: &str, yaml: &str) {
    println!("\n### {title}");
    for b in ALL {
        match parse::<T>(b, yaml) {
            Ok(v) => {
                let j = serde_json::to_string(&v).unwrap();
                println!("- {:14} OK ({} bytes json)", b.name(), j.len());
            }
            Err(d) => println!(
                "- {:14} ERR at {} path=`{}` :: {}",
                b.name(),
                d.loc.map_or("none".into(), |(l, c)| format!("{l}:{c}")),
                d.path,
                one_line(&d.msg)
            ),
        }
    }
}

fn replace_once(src: &str, from: &str, to: &str) -> String {
    assert!(
        src.contains(from),
        "fixture edit target not found: {from:?}"
    );
    src.replacen(from, to, 1)
}

#[derive(Deserialize, Serialize, Debug)]
struct W<T> {
    k: T,
}

fn scalar_table<T: DeserializeOwned + Debug>(target: &str, inputs: &[&str]) {
    println!("\n#### target `{target}`\n");
    println!(
        "| YAML `k: <input>` | serde_norway | serde_yaml_ng | serde-saphyr | serde-saphyr strict_booleans |"
    );
    println!("|---|---|---|---|---|");
    for inp in inputs {
        let y = format!("k: {inp}\n");
        let cells: Vec<String> = [
            Backend::Norway,
            Backend::YamlNg,
            Backend::Saphyr,
            Backend::SaphyrStrict,
        ]
        .iter()
        .map(|b| match parse::<W<T>>(*b, &y) {
            Ok(v) => format!("`{:?}`", v.k),
            Err(d) => format!("ERR: {}", short(&one_line(&d.msg))),
        })
        .collect();
        println!("| `{}` | {} |", inp, cells.join(" | "));
    }
}

fn short(s: &str) -> String {
    let s = s.replace('|', "/");
    if s.chars().count() > 70 {
        format!("{}...", s.chars().take(70).collect::<String>())
    } else {
        s
    }
}

fn section(title: &str) {
    println!("\n\n## {title}");
}

fn report() {
    section("A. Demo pipeline into PipelineSpec");
    let mut jsons = Vec::new();
    for b in ALL {
        match parse::<PipelineSpec>(b, DEMO) {
            Ok(v) => {
                let j = serde_json::to_string(&v).unwrap();
                println!(
                    "- {:14} OK: {} sources, {} datasets, {} transforms, {} sinks; canonical json {} bytes",
                    b.name(),
                    v.sources.len(),
                    v.datasets.len(),
                    v.transforms.len(),
                    v.sinks.len(),
                    j.len()
                );
                jsons.push(j);
            }
            Err(d) => println!("- {:14} ERR {:?}", b.name(), d),
        }
    }
    println!(
        "- all three produce identical canonical JSON: {}",
        jsons.len() == 3 && jsons[0] == jsons[1] && jsons[1] == jsons[2]
    );

    section("B. Malformed variants of the demo (typed PipelineSpec; location = line:col, 1-based)");
    let v = |from: &str, to: &str| replace_once(DEMO, from, to);
    show::<PipelineSpec>(
        "B1 misspelled top-level key (`wal:` -> `wall:`)",
        &v("\nwal:\n", "\nwall:\n"),
    );
    show::<PipelineSpec>(
        "B2 unknown key nested in a list item (`from:` -> `fromm:` in a dataset field)",
        &v(
            "{ name: soc_pct,      type: f32,       from: /soc_pct }",
            "{ name: soc_pct,      type: f32,       fromm: /soc_pct }",
        ),
    );
    show::<PipelineSpec>(
        "B3 wrong type (`worker_threads: two`)",
        &v("worker_threads: 2", "worker_threads: two"),
    );
    show::<PipelineSpec>(
        "B4 wrong type, sequence where scalar expected (`on_full: [block]`)",
        &v("on_full: block ", "on_full: [block] "),
    );
    show::<PipelineSpec>(
        "B5 missing required key (`on_full` removed)",
        &v(
            "  on_full: block                          # explicit; see scenario F7 for drop_oldest\n",
            "",
        ),
    );
    show::<PipelineSpec>(
        "B6 invalid enum value (`on_full: blok`)",
        &v("on_full: block ", "on_full: blok "),
    );
    show::<PipelineSpec>(
        "B7 bad duration (`retain_for: 1hr30`)",
        &v("retain_for: 1h ", "retain_for: 1hr30 "),
    );
    show::<PipelineSpec>(
        "B8 bad size (`max_disk: 2GiBB`)",
        &v("max_disk: 2GiB", "max_disk: 2GiBB"),
    );
    show::<PipelineSpec>(
        "B9 bad count (`max_keys: 5oo`)",
        &v(
            "max_keys: 500k }          # message level",
            "max_keys: 5oo }          # message level",
        ),
    );
    show::<PipelineSpec>(
        "B10 duplicate key in a struct (`retain_for` twice)",
        &v("  retain_for: 1h ", "  retain_for: 2h\n  retain_for: 1h "),
    );
    show::<PipelineSpec>(
        "B11 unknown transform kind (`kind: validte`)",
        &v("kind: validate", "kind: validte"),
    );
    show::<PipelineSpec>(
        "B12 unknown key inside internally tagged variant (`expr:` -> `exprr:`)",
        &v("expr: \"level != 'debug'\"", "exprr: \"level != 'debug'\""),
    );
    show::<PipelineSpec>(
        "B13 wrong type inside internally tagged variant (`heartbeat: [5m]`)",
        &v("heartbeat: 5m", "heartbeat: [5m]"),
    );
    show::<PipelineSpec>(
        "B14 version as string (`version: \"1\"`)",
        &v("version: 1\n", "version: \"1\"\n"),
    );
    show::<PipelineSpec>(
        "B15 syntax error: unclosed flow mapping",
        &v(
            "logs: { format: json, level: info }",
            "logs: { format: json, level: info",
        ),
    );
    show::<PipelineSpec>(
        "B16 syntax error: tab indentation",
        &v("\n  name: edge-capture", "\n\tname: edge-capture"),
    );
    show::<PipelineSpec>(
        "B17 syntax error: bad indentation inside sequence",
        &v(
            "  - id: fleet-mqtt\n    kind: mqtt",
            "  - id: fleet-mqtt\n   kind: mqtt",
        ),
    );
    show::<PipelineSpec>(
        "B18 accidental second document (`---` before `sinks:`)",
        &v("\nsinks:\n", "\n---\nsinks:\n"),
    );
    show::<PipelineSpec>("B19 empty file", "");
    show::<PipelineSpec>(
        "B20 unresolved interpolation placeholder is just a string (no YAML-level failure)",
        &v(
            "\"${file:/run/secrets/minio_uploader_key}\"",
            "\"${env:NOPE\"",
        ),
    );

    section(
        "C. Error text with the crates' own rendering (no serde_path_to_error), case B2 and B13",
    );
    for (name, y) in [
        (
            "B2",
            v(
                "{ name: soc_pct,      type: f32,       from: /soc_pct }",
                "{ name: soc_pct,      type: f32,       fromm: /soc_pct }",
            ),
        ),
        ("B13", v("heartbeat: 5m", "heartbeat: [5m]")),
    ] {
        for b in ALL {
            match native::<PipelineSpec>(b, &y) {
                Ok(_) => println!("- {name} {:14} OK", b.name()),
                Err(e) => println!("- {name} {:14} native Display:\n{}", b.name(), indent(&e)),
            }
        }
    }
    println!("\nsaphyr with default options (snippet enabled), B2:");
    let y = v(
        "{ name: soc_pct,      type: f32,       from: /soc_pct }",
        "{ name: soc_pct,      type: f32,       fromm: /soc_pct }",
    );
    if let Err(e) = serde_saphyr::from_str::<PipelineSpec>(&y) {
        println!("{}", indent(&e.to_string()));
    }
    println!(
        "\nsaphyr with default options (snippet enabled), B7 (custom error from a Deserialize impl):"
    );
    let y = v("retain_for: 1h ", "retain_for: 1hr30 ");
    if let Err(e) = serde_saphyr::from_str::<PipelineSpec>(&y) {
        println!("{}", indent(&e.to_string()));
    }

    section("D. Multi-error collection: a fixture with three unrelated errors");
    let three = v("worker_threads: 2", "worker_threads: two")
        .replacen("retain_for: 1h ", "retain_for: 1hr30 ", 1)
        .replacen("\nwal:\n", "\nwall:\n", 1);
    show::<PipelineSpec>(
        "D1 three independent errors (misspelled key, bad duration, bad number)",
        &three,
    );
    let five = five_errors();
    show::<PipelineSpec>(
        "D2 five independent errors in five sections (single serde pass)",
        &five,
    );

    section("E. YAML 1.1 vs 1.2 scalar coercion (the Norway problem)");
    scalar_table::<String>(
        "String (typed config field the user intends as text, e.g. client_id, site)",
        &[
            "no",
            "No",
            "NO",
            "yes",
            "on",
            "off",
            "y",
            "n",
            "true",
            "null",
            "~",
            "12345",
            "1e3",
            "0x1F",
            "014",
            "0o14",
            "1_000",
            "12:30:45",
            "1.10",
            ".inf",
            "2024-01-31",
            "'no'",
            "\"no\"",
        ],
    );
    scalar_table::<bool>(
        "bool (typed config field)",
        &[
            "true", "True", "TRUE", "false", "no", "No", "yes", "on", "off", "y", "n", "1", "0",
        ],
    );
    scalar_table::<i64>(
        "i64",
        &[
            "42",
            "-7",
            "0x1F",
            "0o14",
            "014",
            "1_000",
            "1e3",
            "1.0",
            "12:30:45",
            "+5",
            "0b101",
            "'42'",
            "42.0",
            "9223372036854775808",
        ],
    );
    scalar_table::<f64>(
        "f64",
        &[
            "1.5", "1e3", "1E3", ".5", "5.", ".inf", "-.Inf", ".nan", "0x1F", "014", "1_000",
            "12:30:45", "42", "'1.5'",
        ],
    );
    scalar_table::<Option<String>>(
        "Option<String>",
        &["null", "Null", "NULL", "~", "''", "", "nil", "none"],
    );
    scalar_table::<serde_json::Value>(
        "serde_json::Value (the D22 connector `config` boundary)",
        &[
            "no",
            "yes",
            "on",
            "off",
            "y",
            "n",
            "true",
            "True",
            "null",
            "~",
            "12345",
            "1e3",
            "0x1F",
            "014",
            "0o14",
            "1_000",
            "12:30:45",
            "1.10",
            "2024-01-31",
            ".inf",
            "'no'",
        ],
    );

    section("F. Duplicate keys");
    #[derive(Deserialize, Serialize, Debug)]
    struct S {
        a: u32,
    }
    for b in ALL {
        let r1 = parse::<S>(b, "a: 1\na: 2\n");
        let r2 = parse::<serde_json::Value>(b, "a: 1\na: 2\n");
        let r3 = parse::<std::collections::BTreeMap<String, u32>>(b, "a: 1\na: 2\n");
        println!(
            "- {:14} struct: {} | serde_json::Value: {} | BTreeMap: {}",
            b.name(),
            fmt_res(&r1),
            fmt_res(&r2),
            fmt_res(&r3)
        );
    }
    println!(
        "- serde-saphyr duplicate_keys option exists: Error (default) | FirstWins | LastWins; serde_norway/serde_yaml_ng have no option."
    );

    section("G. Anchors, aliases and merge keys");
    #[derive(Deserialize, Serialize, Debug)]
    #[serde(deny_unknown_fields)]
    struct Fld {
        name: String,
        #[serde(rename = "type")]
        ty: String,
        from: String,
    }
    #[derive(Deserialize, Serialize, Debug)]
    #[serde(deny_unknown_fields)]
    struct Doc {
        fields: Vec<Fld>,
    }
    let base = "fields:\n  - &base { name: a, type: f32, from: /a }\n  - { <<: *base, name: b, from: /b }\n";
    show::<Doc>(
        "G1 merge key `<<: *base` into a struct with deny_unknown_fields",
        base,
    );
    for b in ALL {
        if let Ok(d) = parse::<Doc>(b, base) {
            println!("    {:14} fields[1] = {:?}", b.name(), d.fields[1]);
        }
    }
    let alias_only = "fields:\n  - &f { name: a, type: f32, from: /a }\n  - *f\n";
    show::<Doc>("G2 plain alias to a whole mapping (`- *f`)", alias_only);
    let top = "x-common: &c { type: f32 }\nfields: []\n";
    show::<Doc>(
        "G3 top-level `x-common:` anchor holder with deny_unknown_fields",
        top,
    );
    show::<serde_json::Value>("G4 merge key into serde_json::Value", base);
    for b in ALL {
        if let Ok(v) = parse::<serde_json::Value>(b, base) {
            println!("    {:14} Value[fields][1] = {}", b.name(), v["fields"][1]);
        }
    }
    let undefined = "fields:\n  - *nope\n";
    show::<Doc>("G5 alias to an undefined anchor", undefined);
    let tagged = "fields:\n  - !!python/object:os.system { name: a, type: f32, from: /a }\n";
    show::<Doc>("G6 unknown local/global tag on a mapping", tagged);

    spans_section();
    collect_section();

    scalar_helpers();

    section("K. Interaction with serde_path_to_error");
    for b in ALL {
        let y = v("heartbeat: 5m", "heartbeat: [5m]");
        match parse::<PipelineSpec>(b, &y) {
            Err(d) => println!(
                "- {:14} path=`{}` (expect transforms[1].heartbeat)",
                b.name(),
                d.path
            ),
            Ok(_) => println!("- {:14} unexpectedly OK", b.name()),
        }
        let y = v(
            "{ name: soc_pct,      type: f32,       from: /soc_pct }",
            "{ name: soc_pct,      type: f32,       fromm: /soc_pct }",
        );
        match parse::<PipelineSpec>(b, &y) {
            Err(d) => println!("- {:14} unknown-field path=`{}`", b.name(), d.path),
            Ok(_) => println!("- {:14} unexpectedly OK", b.name()),
        }
    }
}

fn spans_section() {
    section(
        "H. Path -> span index from the low-level parser (PoC, independent of the serde crate)",
    );
    let v = |from: &str, to: &str| replace_once(DEMO, from, to);
    let cases: Vec<(&str, String)> = vec![
        (
            "B2 unknown key",
            v(
                "{ name: soc_pct,      type: f32,       from: /soc_pct }",
                "{ name: soc_pct,      type: f32,       fromm: /soc_pct }",
            ),
        ),
        (
            "B5 missing field",
            v(
                "  on_full: block                          # explicit; see scenario F7 for drop_oldest\n",
                "",
            ),
        ),
        (
            "B7 bad duration",
            v("retain_for: 1h ", "retain_for: 1hr30 "),
        ),
        (
            "B9 bad count",
            v(
                "max_keys: 500k }          # message level",
                "max_keys: 5oo }          # message level",
            ),
        ),
        (
            "B12 unknown key in tagged variant",
            v("expr: \"level != 'debug'\"", "exprr: \"level != 'debug'\""),
        ),
        (
            "B13 wrong type in tagged variant",
            v("heartbeat: 5m", "heartbeat: [5m]"),
        ),
    ];
    for (name, y) in &cases {
        let idx = spans::index(y).expect("index");
        println!("\n- {name}");
        for b in ALL {
            if let Err(d) = parse::<PipelineSpec>(b, y) {
                let fmt = |p: Option<(usize, usize)>| {
                    p.map_or("none".to_string(), |(l, c)| format!("{l}:{c}"))
                };
                println!(
                    "    {:14} reported {:>8}  path `{}` -> index {:>8}",
                    b.name(),
                    fmt(d.loc.map(|(l, c)| (l as usize, c as usize))),
                    d.path,
                    fmt(idx.get(&d.path).copied())
                );
            }
        }
    }
}

fn scalar_helpers() {
    section("J. Duration, size and count scalars named in the ADR checklist");
    #[derive(Deserialize, Debug)]
    #[allow(dead_code)]
    struct Scalars {
        d1: model::Dur,
        d2: model::Dur,
        d3: model::Dur,
        d4: model::Dur,
        s1: model::Size,
        c1: model::Count,
    }
    let y = "d1: 50ms\nd2: 5m\nd3: 1h\nd4: 1d\ns1: 128MiB\nc1: 500k\n";
    for b in ALL {
        match parse::<Scalars>(b, y) {
            Ok(v) => println!(
                "- {:14} OK d1={:?} d4(1d)={:?} s1={:?} c1={:?}",
                b.name(),
                v.d1.0,
                v.d4.0,
                v.s1.0,
                v.c1.0
            ),
            Err(d) => println!("- {:14} ERR {}", b.name(), d.msg),
        }
    }
}

fn five_errors() -> String {
    let v = |src: &str, from: &str, to: &str| replace_once(src, from, to);
    let y = v(
        DEMO,
        "{ name: soc_pct,      type: f32,       from: /soc_pct }",
        "{ name: soc_pct,      type: f32,       fromm: /soc_pct }",
    );
    let y = v(&y, "retain_for: 1h ", "retain_for: 1hr30 ");
    let y = v(&y, "worker_threads: 2", "worker_threads: two");
    let y = v(&y, "heartbeat: 5m", "heartbeat: [5m]");
    format!("{y}\nwall2: 1\n")
}

fn collect_section() {
    section(
        "I. Multi-error collection PoC: parse to serde_json::Value, validate per section/item, spans from the index",
    );
    let y = five_errors();
    let idx = spans::index(&y).expect("index");
    for b in ALL {
        match parse::<serde_json::Value>(b, &y) {
            Err(d) => println!("- {:14} Value parse failed: {}", b.name(), d.msg),
            Ok(root) => {
                let found = collect::collect(&root, &idx);
                println!("- {:14} {} errors collected:", b.name(), found.len());
                for f in found {
                    println!(
                        "    {:>7}  {:34} {}",
                        f.pos
                            .map_or("none".to_string(), |(l, c)| format!("{l}:{c}")),
                        f.path,
                        short(&one_line(&f.msg))
                    );
                }
            }
        }
    }
}

fn indent(s: &str) -> String {
    s.lines()
        .map(|l| format!("    {l}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn fmt_res<T: Debug>(r: &Result<T, Diag>) -> String {
    match r {
        Ok(v) => format!("OK {}", short(&one_line(&format!("{v:?}")))),
        Err(d) => format!("ERR {}", short(&one_line(&d.msg))),
    }
}

fn bomb(b: Backend, levels: usize) {
    // Nine-way fan-out per level: expanded size is 9^levels leaves.
    let mut y = String::from("a: &a0 [lol, lol, lol, lol, lol, lol, lol, lol, lol]\n");
    for i in 1..levels {
        let prev = format!("*a{}", i - 1);
        let items = vec![prev; 9].join(", ");
        y.push_str(&format!(
            "{}: &a{} [{}]\n",
            (b'a' + i as u8) as char,
            i,
            items
        ));
    }
    let t = Instant::now();
    let r = parse::<serde_json::Value>(b, &y);
    let el = t.elapsed();
    match r {
        Ok(v) => println!(
            "{} bomb levels={levels}: OK after {el:?}; json size {}",
            b.name(),
            serde_json::to_string(&v).map_or(0, |s| s.len())
        ),
        Err(d) => println!(
            "{} bomb levels={levels}: ERR after {el:?}: {}",
            b.name(),
            short(&one_line(&d.msg))
        ),
    }
}

fn deep(b: Backend, depth: usize) {
    let y = format!("k: {}{}\n", "[".repeat(depth), "]".repeat(depth));
    let t = Instant::now();
    let r = parse::<serde_json::Value>(b, &y);
    let el = t.elapsed();
    match r {
        Ok(_) => println!("{} deep depth={depth}: OK after {el:?}", b.name()),
        Err(d) => println!(
            "{} deep depth={depth}: ERR after {el:?}: {}",
            b.name(),
            short(&one_line(&d.msg))
        ),
    }
}

fn bench(iters: usize) {
    for b in ALL {
        let t = Instant::now();
        let mut ok = 0;
        for _ in 0..iters {
            if parse::<PipelineSpec>(b, std::hint::black_box(DEMO)).is_ok() {
                ok += 1;
            }
        }
        let el = t.elapsed();
        println!(
            "{:14} typed PipelineSpec: {:.1} us/parse ({} iters, {} ok, {} bytes input)",
            b.name(),
            el.as_secs_f64() * 1e6 / iters as f64,
            iters,
            ok,
            DEMO.len()
        );
    }
}

fn backend_arg(s: &str) -> Backend {
    match s {
        "norway" => Backend::Norway,
        "ng" => Backend::YamlNg,
        "saphyr" => Backend::Saphyr,
        other => panic!("unknown backend {other}"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        None | Some("report") => report(),
        Some("bomb") => bomb(backend_arg(&args[2]), args[3].parse().unwrap()),
        Some("deep") => deep(backend_arg(&args[2]), args[3].parse().unwrap()),
        Some("bench") => bench(args.get(2).map_or(2000, |s| s.parse().unwrap())),
        Some(x) => eprintln!("unknown subcommand {x}"),
    }
}
