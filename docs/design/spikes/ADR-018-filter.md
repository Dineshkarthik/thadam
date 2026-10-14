# Spike report: ADR-0018 filter and expression language

- **Date:** 2026-10-07
- **Feeds:** ADR-0018 (filter and expression language), readiness-gate blocker B7 (decide before PR9 and before the demo YAML is published)
- **Code and raw output:** `spikes/adr018-filter/` (own `[workspace]`; the root workspace excludes `spikes/`)

## 1. Summary

**Recommendation: ship structured YAML predicates for the MVP, define them so each has an exact CEL equivalent, reserve the `expr:` key, and add CEL (`cel` crate, behind the same `thadam-transform` boundary) after the MVP.** This is the ADR's own fallback branch, chosen here on safety, diagnostics and dependency grounds, not because CEL failed on speed. Confidence: **medium**.

**CEL passes a per-event speed bar only if it is bound correctly**, and fails it if it is bound naively. Per-event costs on this host (arm64 laptop, macOS, relative numbers only), median ns per event:

| | structured predicate | CEL, interpreter only | CEL via `VariableResolver` | CEL, context of referenced variables | CEL, context of every field |
|---|---|---|---|---|---|
| `level != 'debug'` | 3 | 21 | 44 | 111 | 1,698 |
| `quality.status == 'good'` | 4 | 91 | 682 | 730 | 1,783 |
| `cpu_temp_c >= -40.0 && cpu_temp_c <= 150.0` | 3 | 231 | 272 | 288 | 1,954 |
| `motor_temp_c.all(t, ...)` (list of 7) | 4 | 1,471 | 1,705 | 1,740 | 3,225 |
| combination (3 clauses, 4 variables) | 13 | 150 | 253 | 982 | 1,836 |
| same range check, 67-field event | 3 | 229 | 270 | 279 | **6,046** |

Allocations per event: the structured predicate **0** in every case; CEL 0 to 70 for the interpreter alone, 1 to 82 through the resolver, and **71 to 182** for a context of every field (3.7 to 7.0 KB per event).

Other findings, all measured here:

1. **Building the context is the bottleneck, as expected.** Binding every field costs 1.7 to 1.95 µs for a 13-field event and 6.0 µs for 67 fields; binding only the referenced variables or using a resolver stays flat (about 0.27 µs for a single variable regardless of width). For scale, the D8 spike measured about 250 ns to parse a W1 JSON event and about 100 ns to build its record on the same machine, so naive CEL binding costs about 7 times the parse, and the structured predicate costs less than the noise.
2. **CEL has no static checking of identifiers, functions or types.** `lvl != 'debug'` and `unknown_fn(level)` compile; `x > 'hot'` fails per event with "no matching overload". Thadam would need its own pass (the `references()` API helps, but names bound by macros such as `t` in `all(t, ...)` show up as variables). The structured predicate resolves field names and checks literal types against the declared schema at compile time, with the path of the offending key.
3. **`f32` fields widen to `f64` in CEL.** An `f32` field holding the nearest value to `0.1` is not equal to `0.1` in CEL, and is equal to `0.10000000149011612`. The structured evaluator compares `f32` fields in the `f32` domain, so `eq: 0.1` is true. This is the hazard ADR-0018 listed; it is real.
4. **Non-Turing-complete does not mean bounded cost.** A 608-byte CEL expression of four nested `all` comprehensions over 50-element literal lists took about 0.7 s for one evaluation (50^4 iterations); three levels took 14 ms. The crate has no step or cost budget. A `+` chain of 1,000 terms (21 KB) **overflowed a 2 MiB thread stack and aborted the process**; 5,000 terms overflowed 8 MiB. The parser's own recursion limit (96) covers nested parentheses and lists but not long operator chains. An expression length cap and a limit on comprehension nesting are required with CEL.
5. **Diagnostics.** CEL reports several syntax errors per expression with 1-based line and column and a caret snippet, which can be mapped to a YAML line and column. The wording is parser-generated ("mismatched input '<EOF>' expecting {'[', '{', '(', ...}"), a recursion-limit error prints as a debug dump (`OtherError(ParseError { ... })`), and `ParseError`'s `Display` **panicked** on a 200,000-character single-line expression (`Formatting argument out of range`).
6. **License and maintenance.** `cel` is MIT, MSRV 1.86, under the allow-list with its 42-crate tree (`cargo deny` passes). It is fast-moving: 0.14.1 on 2026-07-27, four more 0.14.x releases, then 0.15.0 on **2026-10-06, the day before this evaluation**; one author has 469 of 746 commits. `evalexpr` 13.1.0 is **AGPL-3.0-only** (every release from 12.0.0), which the allow-list rejects, so it is out regardless of its numbers (146 to 228 ns for a single variable, 626 ns for the combination, one crate).

## 2. Environment

| Item | Value |
|---|---|
| Host | arm64 laptop, 18 cores, 48 GB, macOS |
| Toolchain | rustc 1.99.0 (pinned by the repository's `rust-toolchain.toml`); Rust 1.88.0 installed for the MSRV check |
| Crates (exact) | `cel` 0.15.0 (default features `regex`, `chrono`, `ext_encoders`), `evalexpr` 13.1.0 (feature-gated, default on), `criterion` 0.8.2, `serde-saphyr` 1.3.0 (parses the predicate YAML), `serde` 1.0.229, `serde_json` 1.0.151 |
| Profile | `release`: `lto = "thin"`, `codegen-units = 1`, `debug = "line-tables-only"` (as in the D8 spike) |
| Allocator | macOS system `malloc`; the counting allocator is installed only in `alloc_report` |
| Machine load | Not idle. Before the final criterion run: load average 3.3 / 4.9 / 5.1, 79% idle. Two full criterion runs agree within about 5% (`results/criterion_summary_run1.md` and `results/criterion_summary.md`). |
| Not available | Raspberry Pi 5 class ARM, Jetson, x86 Linux. The ADR's ARM criterion is **not met** by this spike. |

## 3. What was compared

- **(a) CEL through the `cel` crate**: `Env::stdlib().compile(...)` once, `Program::execute` per event, with five ways of supplying variables (section 4.2).
- **(b) A minimal structured-predicate tree**: YAML `{field, eq|ne|lt|le|gt|ge|in|min/max}` leaves under `all`, `any`, `not`, parsed with `serde-saphyr` and `deny_unknown_fields`, compiled against the dataset `Shape` into index-based selectors, evaluated natively (`src/native.rs`). This is a sketch for measurement, not a proposed final syntax.
- **(c) `evalexpr` 13.1.0**: a context rebuilt per event from the referenced variables.

Not compared: VRL (MPL-2.0, rejected by the allow-list per the ADR), Rhai (rejected on sandboxing in the ADR), and a custom mini-DSL (no code written).

## 4. Method

### 4.1 Event and filters

`src/model.rs` is a trimmed copy of the canonical model: `Record` as `Arc<Shape>` plus `Vec<Value>` (D8 representation B), the D9 extended value set (`I64`, `U64`, `F32`, `F64`, `Str(Arc<str>)`, `F32s`), a `Quality` envelope (`status`, `flags`, `reason`), and event times.

- **Demo shape (13 fields):** `site`, `robot`, `event_type`, `level`, `code` (30% null), `message`, `cycle_id` (40% null), `seq`, `soc_pct`, `voltage_v`, `current_a`, `cpu_temp_c` (all `f32`), `motor_temp_c` (7 `f32`). One shape carries what the `events` and `telemetry` datasets each need, so each filter has fields to read.
- **Wide shape (67 fields):** three identity fields plus 64 `f64` sensors, as a W2-like width.
- Events come from a SplitMix64 generator with a fixed seed (levels 25% debug, 50% info, 15% warn, 10% error; 8 robots; quality 80% good, 15% uncertain, 5% bad). Criterion cycles through 1,024 events; verification and allocation counting use 10,000.

Filters (`src/cases.rs`), each written for all three engines with a hand-written Rust reference:

| Case | CEL | Structured predicate (YAML) |
|---|---|---|
| `level_ne_debug` (75.0% pass) | `level != 'debug'` | `{ field: level, ne: debug }` |
| `quality_good` (79.6%) | `quality.status == 'good'` | `{ field: quality.status, eq: good }` |
| `range_scalar` (94.9%) | `cpu_temp_c >= -40.0 && cpu_temp_c <= 150.0` | `{ field: cpu_temp_c, min: -40, max: 150 }` |
| `range_list` (26.1%) | `motor_temp_c.all(t, t >= -40.0 && t <= 150.0)` | `{ field: motor_temp_c, min: -40, max: 150 }` (applies to every element) |
| `combo` (66.1%) | `level != 'debug' && (robot != 'robot-03' \|\| quality.status == 'good') && soc_pct >= 10.0` | `all: [{field: level, ne: debug}, {any: [{field: robot, ne: robot-03}, {field: quality.status, eq: good}]}, {field: soc_pct, ge: 10.0}]` |
| `wide_range_scalar` (82.2%) | `s17 >= -40.0 && s17 <= 150.0` | `{ field: s17, min: -40, max: 150 }` |

`cargo run --release` first checks that **every engine agrees with the reference on every event** (10,000 events per case, zero mismatches), so the timings compare engines that return the same answers. `evalexpr` has no list functions, so `range_list` is not expressible there.

### 4.2 How CEL is bound (the part that dominates cost)

| Strategy | What happens per event |
|---|---|
| `cel_eval_only` | Contexts are built once, per event, **outside** the timer; measures the interpreter alone. |
| `cel_ctx_all` | A `Context` is created and every field is added with `add_variable_from_value`, plus `quality` as a map. |
| `cel_ctx_refs` | A `Context` with only the identifiers listed by `Program::references()`. |
| `cel_resolver` | A child scope with a `VariableResolver` over the event; values are converted on lookup (`CowVal::owned`, a box per lookup). `quality` is built as a map on first use. |
| `evalexpr_refs` | A `HashMapContext` of the referenced variables; `quality` is flattened to `quality_status`. |

Conversions are the obvious ones: `f32` widens to `f64`, strings become `Arc<String>`, `F32s` becomes a list of floats, `Quality` becomes a three-entry map.

### 4.3 Measurements

- **Time:** criterion 0.8.2, 40 samples, 0.8 s warm-up, 3 s measurement, per batch of 1,024 events; ns per event is the batch time divided by 1,024. Compile cost is measured separately.
- **Allocations:** a counting global allocator (`allocs` and bytes requested per event over 10,000 events, single thread, after a 64-event warm-up).
- **Semantics and diagnostics:** `src/bin/semantics.rs` evaluates probe expressions on the three engines.
- **Pathological input:** `src/bin/stress.rs` and `scripts/stress.sh`, one child process per case.

## 5. Results

### 5.1 Per-event time (ns/event; criterion median, [low, high] in brackets)

From `results/criterion_summary.md` (final run). The first run is in `results/criterion_summary_run1.md`; the two agree within about 5%.

| filter | native | cel_eval_only | cel_resolver | cel_ctx_refs | cel_ctx_all | evalexpr_refs |
|---|---|---|---|---|---|---|
| level_ne_debug | 3 [3, 3] | 21 [21, 22] | 44 [44, 45] | 111 [110, 112] | 1,698 [1,687, 1,711] | 158 [157, 160] |
| quality_good | 4 [4, 4] | 91 [91, 92] | 682 [678, 690] | 730 [726, 735] | 1,783 [1,776, 1,791] | 146 [146, 147] |
| range_scalar | 3 [3, 3] | 231 [229, 234] | 272 [270, 275] | 288 [282, 297] | 1,954 [1,933, 1,977] | 228 [224, 232] |
| range_list | 4 [4, 4] | 1,471 [1,456, 1,490] | 1,705 [1,692, 1,723] | 1,740 [1,729, 1,754] | 3,225 [3,203, 3,248] | n/a |
| combo | 13 [13, 13] | 150 [149, 152] | 253 [252, 255] | 982 [979, 985] | 1,836 [1,829, 1,844] | 626 [621, 633] |
| wide (67 fields) range_scalar | 3 [3, 3] | 229 [227, 233] | 270 [267, 274] | 279 [276, 283] | 6,046 [6,005, 6,104] | 221 [220, 223] |

Ratio to the structured predicate (median): interpreter only 7x to 381x, resolver 14x to 442x, referenced-variable context 35x to 451x, every-field context 140x to 1,765x. A 3 ns native figure includes loop overhead and is a ceiling on the evaluator, not a precise cost; read it as "negligible".

Reading:

- **Interpreter alone** is cheap for string comparisons (21 ns) and costs 90 to 230 ns for the other scalar cases. The list `all` is 1.5 µs for 7 elements (child scope per element).
- **The resolver** adds 20 to 40 ns for scalars. It costs 0.68 µs for `quality.status` because the three-entry map is rebuilt on lookup; and for the combination it is 253 ns against 982 ns for a referenced-variable context because `quality` is only built when the earlier clauses do not short-circuit (only `robot-03` events that are not `debug`, roughly one event in ten, reach it). A lazy `Val` for `Quality` implementing `Indexer` would remove the map cost; that was not built.
- **Context of every field** is the 1.7 to 2.0 µs regime, and it scales with width: 1,954 ns at 13 fields, 6,046 ns at 67. A CEL integration that does this is the failure case.
- **`evalexpr`** is 146 to 228 ns for a single variable and 626 ns for the combination, comparable to the CEL referenced-variable context, but it is unusable on license grounds (section 5.7).

### 5.2 Allocations per event (`results/alloc_report.md`)

| filter | native | cel_eval_only | cel_resolver | cel_ctx_refs | cel_ctx_all | evalexpr_refs |
|---|---|---|---|---|---|---|
| level_ne_debug | 0 | 0 | 1.0 (24 B) | 5.0 (533 B) | 71.3 (3,707 B) | 7.0 (514 B) |
| quality_good | 0 | 2.0 (78 B) | 22.8 (1,161 B) | 24.8 (1,624 B) | 73.3 (3,785 B) | 7.0 (519 B) |
| range_scalar | 0 | 11.7 (327 B) | 13.6 (343 B) | 14.7 (801 B) | 83.0 (4,034 B) | 7.0 (886 B) |
| range_list | 0 | 70.4 (1,919 B) | 82.4 (2,535 B) | 84.4 (3,003 B) | 141.7 (5,626 B) | n/a |
| combo | 0 | 4.6 (130 B) | 9.1 (282 B) | 37.4 (1,846 B) | 75.9 (3,837 B) | 24.0 (1,938 B) |
| wide range_scalar | 0 | 11.5 (321 B) | 13.4 (336 B) | 14.5 (788 B) | 182.5 (6,984 B) | 7.0 (879 B) |

Which interpreter operations allocate was isolated on prebuilt contexts: `true` and the string comparisons (`level != 'debug'`, `level == 'info'`) allocate 0; **each float comparison allocates 6** (168 B); `cpu_temp_c >= 0.0 && cpu_temp_c <= 150.0` allocates 10.5 and a leading minus sign (`-40.0`) adds about 1.2 more. The cause inside the crate was not traced.

### 5.3 Compile cost (once per config load)

Microseconds per compile (median), `results/criterion_summary.md`:

| filter | `cel` | structured (YAML parse plus compile) | `evalexpr` |
|---|---|---|---|
| level_ne_debug | 22.8 | 2.5 | 0.5 |
| quality_good | 23.8 | 2.5 | 0.6 |
| range_scalar | 44.1 | 2.6 | 1.3 |
| range_list | 51.4 | 2.6 | n/a |
| combo | 38.9 | 11.7 | 2.3 |

Irrelevant for per-event cost. It matters only for how long `thadam validate` takes on a config with hundreds of filters (about 50 µs each).

### 5.4 Semantics (`results/semantics.md`)

CEL, `cel` 0.15.0:

| Question | Result |
|---|---|
| Int versus double | `1 < 1.5` true; `1 == 1.0` true; double variable `>= -40` works |
| `uint` above `i64::MAX` | exact with `u` literals: `18446744073709551615u` equals `u64::MAX`; `> 9223372036854775807` true |
| `f32` field versus decimal literal | **`x == 0.1` is false** for `0.1f32` widened; `x == 0.10000000149011612` is true |
| Integer overflow, division by zero | runtime errors (`Overflow from binary operator`, `Division by zero`); `1.0 / 0.0` is `inf` |
| Null field | `code == null` true; `code == 'E-1'` false; `code != 'E-1'` true; `level != 'debug'` with a null `level` true. No error. |
| Missing identifier, missing map key | runtime errors (`Undeclared reference to 'nope'`, `No such key: nope`); `has(q.nope)` is false |
| Type error | `x > 'hot'`: runtime error `found no matching overload for '_>_' applied to '(double, string)'` |
| Short-circuit with an error | `false && (1/0 > 0)` is false; `(1/0 > 0) \|\| true` is true; `(1/0 > 0) && false` is false (errors are absorbed as in the CEL spec) |
| Determinism | `now()`, `rand()`, `random()` are undeclared. `timestamp('...')` of a constant works. `matches` uses the `regex` crate (upstream guarantees linear-time matching; not tested here). |
| Lists | `all`, `exists`, `size` work on list variables |

Structured predicates (this spike's evaluator, where the semantics are defined here): `f32` fields compare in the `f32` domain (`eq: 0.1` true); null: `eq` false, `ne` true, ordered comparisons and ranges false; `u64` literals above `i64::MAX` need their own literal type. **The first run of this spike parsed such literals as `f64` and `eq: 18446744073709551614` matched `u64::MAX`**; adding a `UInt` literal variant fixed it. A structured grammar has to model integer literal types explicitly, as CEL does with `u`.

`evalexpr`: `1 < 1.5` true but `1 == 1.0` **false**; mixed-type operations return errors; `x == 0.1` false for the widened `f32`; no `all`/`exists`; missing variable is an error.

### 5.5 Compile-time diagnostics

CEL (`Env::compile`), selected:

| Source | Result |
|---|---|
| `level != ` | `ERROR: <input>:1:10: Syntax error: mismatched input '<EOF>' expecting {'[', '{', '(', '.', '-', '!', 'true', 'false', 'null', NUM_FLOAT, NUM_INT, NUM_UINT, STRING, BYTES, IDENTIFIER}` plus a source line and a caret |
| `level = 'debug'` | two errors: `1:7 token recognition error at: '= '` and `1:9 extraneous input ''debug'' expecting <EOF>` |
| `(level != 'debug'` | `1:18: missing ')' at '<EOF>'` |
| `level != 'debug' and soc_pct > 1` | `1:18: mismatched input 'and' expecting {<EOF>, '==', '!=', ...}` |
| multi-line, error on line 2 | `2:11`; the structured position is `ParseError.pos = (2, 11)` |
| `lvl != 'debug'`, `unknown_fn(level)`, `a.b.c.d == 1` | **compile without error** |
| 100 nested parentheses | `1:97 Syntax error: OtherError(ParseError { ... msg: "Recursion limit of 96 exceeded" })` |
| `true ? true ? 1 : 2 : 2` (nested ternary without parentheses, an invalid CEL form) | `Incomplete ExprContext for _?_:_ expression!` |

Every compile error carries a 1-based line and column inside the expression, so mapping to a YAML line and column (YAML scalar position plus the expression offset) is feasible; it was **not built** here. The YAML crate spike's path-to-span index gives the scalar's position.

Structured predicates (`results/semantics.md`): a misspelled key is a YAML schema error with line and column and the list of allowed keys (`unknown field 'nee', expected one of all, any, not, field, eq, ne, lt, le, gt, ge, in, min, max`); an unknown field is `filter.field: unknown field 'nope'`; a literal of the wrong type is `filter.gt: field 'cpu_temp_c' is numeric (F32); got Str("hot")`; an operator not defined for the field type, a leaf with no comparison, and two selectors on one node are each reported with their predicate path. These are produced against the declared schema, which CEL cannot do without extra code.

### 5.6 Sandboxing and termination (`scripts/stress.sh`, `results/stress.txt`)

| Input | CEL | Structured predicate |
|---|---|---|
| Nested parentheses, 95 deep / 100 deep / 1,000 deep | ok / error "Recursion limit of 96 exceeded" / same | n/a |
| Nested lists, 100 deep | same recursion-limit error | n/a |
| Unary `!` chain, 100,000 | ok in 16 ms compile | n/a |
| `&&` chain, 100,000 terms (800 KB) | compile 217 to 235 ms, evaluate 1.3 to 1.6 ms (a single earlier run under `/usr/bin/time -l` showed about 340 MiB peak RSS; that output was not saved) | n/a |
| `+` chain of strings, 3,000 terms (63 KB), 8 MiB stack | ok | n/a |
| `+` chain, 5,000 terms, 8 MiB stack | **stack overflow, process aborts** | n/a |
| `+` chain, 500 terms (10.5 KB), 2 MiB thread stack | ok | n/a |
| `+` chain, 1,000 / 2,000 / 3,000 terms, 2 MiB stack | **stack overflow, process aborts** | n/a |
| Single-line expression of 200,000 characters (100,000 nested parentheses) | **panic** in `ParseError` Display (`parser.rs:106`, `Formatting argument out of range`) | n/a |
| Four nested `all` over 50-element literal lists (608 bytes) | compile 1.1 ms, **evaluate about 0.7 s** (three levels 14 ms, two levels 0.3 ms) | n/a |
| Nested `not` / `all` in YAML, 50, 500, 5,000, 50,000 deep | n/a | at 50 `not` levels ok; beyond: `budget breached: Depth { depth: 65 }` or `recursion limit exceeded` in about 0.1 to 0.3 ms; no overflow or panic |

Notes: there is no wall clock, random, I/O or recursion in the CEL language as exposed here, so termination is guaranteed by the language (comprehensions iterate finite data). Cost is not bounded by expression size, as the nested comprehension shows, and the crate has no step budget (no `cost`, `budget` or step-limit option found in its source). The overflow is in left-associative operator chains, which the parser's recursion limit does not count; a stack overflow is a process abort that `catch_unwind` panic isolation (ARCHITECTURE.md section 7) cannot contain. Tokio worker threads default to 2 MiB.

### 5.7 License, maintenance, weight

| | `cel` 0.15.0 | `evalexpr` 13.1.0 | structured predicate |
|---|---|---|---|
| License | MIT (tree: MIT, Apache-2.0, BSD-3-Clause for `antlr4rust`, Unlicense, Unicode-3.0); `cargo deny` licenses, advisories, bans, sources: ok | **AGPL-3.0-only** (all of 12.0.0 through 13.1.0); `cargo deny` licenses: FAILED | none (code in `thadam-transform`) |
| RustSec advisories (advisory-db `b0797f5`) | none for `cel`, `cel-interpreter`, `cel-parser` | none | n/a |
| Latest release | 2026-10-06; 18 releases; 0.14.1 on 2026-07-27, then 0.14.2 to 0.14.5, then 0.15.0 | 2025-11-26; 64 releases | n/a |
| GitHub | `cel-rust/cel-rust`, last push 2026-10-06, 35 contributors, top author 469 of 746 commits, 24 open issues and PRs | not checked | n/a |
| Declared MSRV | 1.86 | 1.65 | n/a |
| Builds on Rust 1.88.0 | a fresh resolve fails: transitive `uuid` 1.27.0 requires 1.89 (an MSRV-aware resolver would pick an older `uuid`) | yes | yes |
| Crates in the normal dependency tree (excluding itself) | 42 (`antlr4rust`, `chrono`, `chrono-tz`, `regex`, `base64`, `uuid`, ...) | 0 | 0 beyond `serde` |
| Clean release build of a one-dependency project (one run) | 8.7 s | 0.6 s | n/a |

`cel` has a `parser_pratt` feature and feature flags for `regex`, `chrono` and `ext_encoders`; disabling defaults was not measured.

### 5.8 Structured-predicate expressiveness

Everything the demo needs is expressible: the three demo filters, the buggy and fixed robot filter (`robot != 'robot-03'`), the `validate` rule shape (`{field, min, max, flag, status}` is already structured), and compound conditions. The grammar in this spike cannot express, and a later CEL would add:

- arithmetic and cross-field comparison (`voltage_v * current_a > 100`, `event_time - ingest_time > 5s`);
- string functions beyond equality and `in` (`startsWith`, `contains`, `matches`);
- list quantifiers beyond "every element within min/max" (any element, count, size);
- null tests as a separate operator (`has`, `is_null`) rather than through `eq` and `ne`;
- numeric `in` sets;
- stateful and window rules (rate of change, anomaly rules in product.md section 10 P1, UC-05 windowing and downsampling), which no single-event expression language covers.

UC-05's quality validation fits the structured form; its windowing and downsampling are outside the MVP (ARCHITECTURE.md section 1) and are separate transform steps either way. This list is from reading product.md section 10 and UC-05, not from user interviews.

## 6. Limitations (read before using the numbers)

- **ARM criterion not met.** One host, an arm64 laptop. No Raspberry Pi 5, no Jetson, no x86 Linux. Relative ordering is the transferable result; absolute nanoseconds are not. The ADR said the criterion must be set before the benchmark ran; it was not, so the "bar" above is a post-hoc reading against the D8 numbers and the withdrawn 50,000 events/s hypothesis (at that rate a 1.7 µs binding is about 8.5% of one core on this host).
- **Machine not idle.** Other processes were running. Two runs agree within about 5%, and the confidence intervals are tight, but a quiet-machine rerun was not done.
- **The structured evaluator is a sketch** written for the benchmark, with semantics chosen here. A production one would differ in details and cost. It found a precision bug in its own literal parsing (section 5.4), which indicates the semantic surface a real design must pin down. Its 3 to 13 ns is real for this code and this event layout (index-resolved selectors on a shape-shared record), not a property of "structured predicates" in general.
- **CEL binding strategies were written for this spike.** A lazy `Val` for `quality` or for the whole event (implementing `Indexer`) could beat the resolver numbers; it was not built. The per-lookup `Box` in `CowVal::owned` is an artifact of the resolver API as used.
- **`cel` internals not traced:** why float comparisons allocate 6 times each, and where exactly the `+` chain overflows (compile, evaluate or drop of the AST).
- **No replay-equivalence test**, no fuzzing of the CEL parser, no `thadam validate` integration, no YAML-position mapping of CEL errors, no owner or user ergonomics review. All remain open in the ADR checklist.
- Only five filters and two event shapes. Larger expressions, string-heavy events, and nested `Value::Record` access were not measured.
- `evalexpr` was not evaluated beyond its AGPL license, one benchmark column, and the semantic probes.

## 7. Recommendation for ADR-0018

### 7.1 Decision proposed

1. **MVP: structured YAML predicates** under a dedicated key (for example `where:`), with `all`, `any`, `not`, and leaves `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `min`/`max`. Names resolve against the dataset schema at compile time, literal types are checked against declared field types, and integer literal types (including `u64`) are explicit. Every construct is defined as an exact CEL equivalent (for example `{field: f, min: a, max: b}` is `f >= a && f <= b`; for a list field `f.all(x, x >= a && x <= b)`), including null and missing-field behavior, so a later move to CEL is mechanical.
2. **Reserve `expr:`** for a CEL string. Do not ship it until the items below are done. If it ships, put the `cel` crate in `thadam-transform` only, pin the exact version (0.15.0 is a day old and the crate moved from 0.14.1 to 0.15.0 in ten weeks), and require:
   - a length cap on the expression (suggest 2 KiB; the overflow starts between 10 KB and 21 KB on a 2 MiB stack) and a limit of one level of comprehension nesting, both checked before compile;
   - a static pass that rejects unknown identifiers and functions against the schema (accounting for macro-bound names);
   - a binding strategy that does not build every field per event: a `VariableResolver`, ideally with a lazy value for `quality`;
   - `f32` handling that avoids the widening hazard (document it, or bind through a decimal-aware rule);
   - a `cargo fuzz` target on compile, run under a small stack, per ADR-0020.
3. **Namespace (proposal for the syntax review).** Plain names for dataset fields and identity levels; `quality.status` and `quality.flags` for the envelope; no JSON Pointer and no `^/` in filters, since filters run on typed records after the parent message is gone.

### 7.2 Why

In the ADR's own terms, the lean was "CEL only if the per-event benchmark on ARM passes". On this host the benchmark passes for CEL bound through a resolver or referenced variables (44 to 1,740 ns) and fails for the straightforward integration (1.7 to 6.0 µs, 71 to 182 allocations per event, growing with event width); it is **not conclusive for ARM**. Speed therefore does not decide it. What decides it:

- the structured form is 7x to 450x cheaper and allocation-free, and its cost does not depend on event width;
- it is checkable against the declared schema at `thadam validate` (unknown fields, wrong literal types, undefined operators), which CEL is not without extra code;
- it has no process-abort or cost-explosion modes (section 5.6), and no new dependency;
- it keeps f32, u64 and null semantics in Thadam's hands (section 5.4);
- everything the MVP and the demo need is expressible (section 5.8); the gaps are post-MVP features.

Against: a second syntax later, verbosity of compound conditions, and the divergence from product.md section 14, which writes `expression: "quality == 'good'"`. That divergence is the owner's to confirm in the syntax review (section 7.4).

### 7.3 What would change this

- The owner prefers expression strings for users, and accepts the CEL requirements in 7.1.2 as MVP scope.
- ARM measurements on reference hardware show the structured evaluator is not materially cheaper than CEL via a resolver (not expected from the ratios here).
- The MVP gains a requirement that the structured grammar cannot express (arithmetic across fields, string functions).

### 7.4 ADR status

**Update 2026-10-07 (after this report):** the owner chose structured YAML predicates for the MVP, reserved CEL for a future ADR, and accepted ADR-0018. The outstanding items below are recorded there as PR9 implementation details; the CEL-only items no longer gate. The text that follows is the report's original recommendation.

Keep ADR-0018 **Draft**. Outstanding before Proposed: the owner ergonomics review of the three demo filters in both syntaxes (section 4.1 lists them), the key spelling decision, ARM hardware numbers if CEL stays on the table, a replay-equivalence test, and the parser fuzz target if CEL is adopted.

## 8. Reproduction

From `spikes/adr018-filter/` (toolchain from the root `rust-toolchain.toml`; use `CARGO_TARGET_DIR=target` if your environment sets a shared target directory):

```bash
cargo build --release
./target/release/adr018-filter                       # agreement check against the Rust reference (10,000 events per case)
cargo bench --bench filters > /tmp/criterion_raw.log   # about 4 minutes
python3 scripts/summarize.py /tmp/criterion_raw.log > results/criterion_summary.md
./target/release/alloc_report > results/alloc_report.md
./target/release/semantics > results/semantics.md
./scripts/stress.sh | tee results/stress.txt         # one child process per case
cargo build --release --no-default-features          # without evalexpr (AGPL)
cargo deny --config ../../deny.toml check advisories licenses bans sources   # fails licenses with evalexpr
cargo deny --config ../../deny.toml --no-default-features check advisories licenses bans sources
```

Metadata and MSRV checks:

```bash
curl -s -A thadam-spike https://crates.io/api/v1/crates/cel     # also cel-interpreter, evalexpr
git clone --depth 1 https://github.com/rustsec/advisory-db.git # evaluated at b0797f5 (2026-10-07)
cargo +1.88.0 check                                            # in a scratch project with cel: uuid 1.27.0 requires 1.89
```

Results: `results/criterion_summary*.md`, `results/alloc_report.md`, `results/semantics.md`, `results/stress.txt`. Raw criterion output is not committed.
