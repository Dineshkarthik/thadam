# ADR-0018: Filter and expression language

- **Status:** Accepted
- **Date:** 2026-10-07
- **Updated:** 2026-10-07 (spike ADR-018-filter evidence folded in; owner decision recorded; accepted)
- **Deciders:** Project owner

**Acceptance.** Accepted by project owner on 2026-10-07 (`docs/design/owner-decisions.md`, B7). The owner chose **structured YAML predicates (`field`, operator, value) for the MVP**; **CEL is reserved for a future ADR**. The residual items from the spike report (`docs/design/spikes/ADR-018-filter.md`, section 7.4), chiefly the filter key spelling and the namespace, are PR9 implementation details, listed under "PR9 implementation details" in the Decision section. The ARM benchmark of CEL, the CEL parser fuzz target, and the other CEL-only checks are no longer gating: they move to the future CEL ADR if one is written. This decision replaces the `expression: "quality == 'good'"` example in product.md section 14 (`docs/design/product-amendments.md`, A.11).

## Context

The `filter` transform drops events that do not match a user-written predicate, and the `validate` transform assigns quality flags from per-field rules. Both need a user-facing language for conditions, plus a syntax for naming fields. The original ADR plan titled this "Expression and field-path language"; this ADR keeps that scope under the title "Filter and expression language".

**A decision is needed before PR9** (the filter step in `thadam-transform`, part 2) **and before the demo pipeline YAML is published.** The demo plan marks its filter syntax provisional until this ADR decides.

**Reversibility is low.** The language is user-facing and cannot be hidden behind an interface the way a library can (D19). Once users write configs, changing the language is a breaking change handled only by a config `version` bump (ADR-0012).

### Examples every candidate must express

- `quality.status == 'good'` (implementation plan sketch).
- `level != 'debug'` (demo `events` dataset, drops debug events).
- `robot != 'robot-03'` (the demo's deliberately buggy filter, written to exclude a test robot but excluding production data; the replay demo corrects it from the WAL).
- `validate` rules with per-field minimum and maximum, for example `{field: motor_temp_c, min: -40, max: 150, flag: OUT_OF_RANGE, status: bad}`.

product.md section 14 shows `quality == 'good'`, which differs from the implementation plan's `quality.status == 'good'`. `quality` is a struct (status, flags, reason) in the model, so the latter is the working form, but the identifier namespace is open (below).

### Field-path syntax today

`from:` in dataset fields uses **JSON Pointer** (RFC 6901), for example `/ts_us`. A `^/` prefix reads from the parent message of an exploded sample, for example `^/seq`. Whether filters share this syntax is part of this decision.

### Where a filter runs

Within a dataset, the order is: decode, event ID and deduplication, `explode`, field extraction and unit conversion, time extraction and skew flagging, then the `transforms:` list in order (`validate`, `on_change`, `filter`, sampling) (ARCHITECTURE.md section 7). A filter therefore sees typed, unit-converted `Record` values and the envelope fields, not raw payload JSON. Filtered events are counted (`thadam_chain_events_total{outcome="filtered"}`) and are not rejects.

### Requirements

The language must satisfy all of these. They are acceptance criteria for any candidate, not findings about any crate.

- **Determinism.** No wall-clock or random functions. Replay must give row-set equivalence for the same WAL range, configuration hash, and enrichment-file hash (ARCHITECTURE.md section 7).
- **Null and missing-field semantics** defined explicitly, including a field absent from a shape, a field present with value null, and `required` fields.
- **Integer and float comparison semantics** defined explicitly across Thadam's value types (narrow integers, `f32`, `f64`, unsigned 64-bit above `i64::MAX`). A decimal literal compared with an `f32` field is a known hazard because the `f32` value widens to a different `f64`.
- **Error behavior.** An evaluation error on one event becomes a reject routed to quarantine with the raw bytes retained. It never panics and never aborts the pipeline (ARCHITECTURE.md section 13).
- **Compile-time validation** at `thadam validate`, with error spans. Chains compile in `thadam-transform`, so expression errors are reported through chain compilation (ADR-0012).
- **Bounded evaluation cost.** Per-event cost and expression size are bounded. [OPEN: limits to be set before the benchmark runs; the `ChainLimits` mechanism (ARCHITECTURE.md section 7) is the natural home.]
- **No user-defined step trait** until the plugin ADR (ADR-0017, Reserved). The language adds no extension point for user code.
- **Dependency rule.** If a crate is chosen, it lives in `thadam-transform` only (ARCHITECTURE.md section 9).

### Open questions

- **Identifier namespace [OPEN].** Whether expressions name dataset fields (`soc_pct`), identity levels (`robot`), and envelope fields (`quality.status`, `ingest_time`) in one namespace, and how this maps to the Parquet envelope columns, which are prefixed (`_quality_status`).
- **Nested record access [OPEN].** Dot access (`a.b`) versus JSON Pointer (`/a/b`) for fields inside `Value::Record`, and whether `^/` is meaningful in filters, which run after the parent message is gone.
- **Filter versus validate [OPEN].** Whether `validate` rules share the language, or stay a structured `{field, min, max, flag, status}` form.

## Decision

**Decided by the owner on 2026-10-07: structured YAML predicates (`field`, operator, value, combined with `all`, `any`, and `not`) are the MVP filter language, defined so each construct has an exact CEL equivalent. CEL is reserved for a future ADR, and the `expr:` key is reserved for a CEL string that may be added after the MVP.** Confidence: medium. Evidence: `docs/design/spikes/ADR-018-filter.md` (cost measured on Apple Silicon (arm64), macOS arm64, `cel` 0.15.0; the original D19 criterion, a CEL benchmark on ARM hardware, was not met and is no longer gating).

The original lean (D19) was CEL through the `cel` crate if the per-event benchmark on ARM passed, otherwise structured predicates for the MVP and CEL later. Structured predicates are the second branch of that lean, chosen without waiting for ARM numbers.

What the evidence says, in short:

- **Speed alone does not rule CEL out, and does not decide it.** Median ns per event: structured predicate 3 to 13 and zero allocations; CEL interpreter alone 21 to 1,471; CEL through a `VariableResolver` 44 to 1,705; CEL with a context of the referenced variables 111 to 1,740; CEL with a context of every field (the straightforward integration) **1,698 to 3,225 for 13 fields and 6,046 for 67 fields**, with 71 to 182 allocations per event. Building the activation per event is the bottleneck, as expected. For scale, the D8 spike measured about 250 ns to parse a W1 JSON event on the same machine.
- **CEL needs work Thadam would have to own:** no static check of identifiers, functions or types (`lvl != 'debug'` and `unknown_fn(level)` compile; `x > 'hot'` fails per event); `f32` fields widen to `f64` (`x == 0.1` is false for the `f32` nearest 0.1); no cost budget (four nested `all` comprehensions in 608 bytes took about 0.7 s per evaluation); a `+` chain of 1,000 terms (21 KB) overflows a 2 MiB thread stack and aborts the process, and a 200,000-character expression made the crate's error `Display` panic; the crate is fast-moving (0.15.0 was released the day before the evaluation) with a 42-crate dependency tree.
- **What it gives:** an open, documented language; heterogeneous int, uint and double comparison with exact `uint`; defined null behavior (`null == 'x'` is false, no error); deterministic stdlib (`now()` and `rand()` are undeclared); error positions with line and column. `evalexpr` is excluded by its AGPL-3.0-only license.
- **What the structured form gives:** schema-aware compile-time checks with predicate paths, Thadam-defined `f32`, `u64` and null semantics, no process-abort or cost-explosion modes, no new dependency, and every demo filter plus the `validate` rule shape. It cannot express arithmetic or cross-field comparison, string functions, or stateful rules; those are post-MVP.
- **Divergence from product.md, resolved:** product.md section 14 writes `expression: "quality == 'good'"`. The owner chose structured predicates, so that example is replaced by the structured form (`docs/design/product-amendments.md`, A.11, Approved 2026-10-07; the owner applies it to `product.md`).

The accepted MVP shape (the key spelling is a PR9 detail; `where:` is the working name):

```yaml
transforms:
  - kind: filter
    dataset: telemetry
    where: { field: quality.status, eq: good }
```

A compound condition combines leaves with `all`, `any`, and `not`; leaves use `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, and `min`/`max`. For example `level != 'debug' && (robot != 'robot-03' || quality.status == 'good') && soc_pct >= 10.0` is `{ all: [{ field: level, ne: debug }, { any: [{ field: robot, ne: robot-03 }, { field: quality.status, eq: good }] }, { field: soc_pct, ge: 10.0 }] }`. Names resolve against the dataset schema at compile time, literal types are checked against declared field types, and integer literal types (including `u64`) are explicit.

If CEL is adopted in a future ADR, it needs: the `cel` crate pinned to an exact version and confined to `thadam-transform`; an expression length cap (suggest 2 KiB) and a limit of one level of comprehension nesting, checked before compile; a static pass rejecting unknown identifiers and functions against the schema (names bound by macros excepted); a `VariableResolver` binding (ideally with a lazy value for `quality`) instead of building every field per event; a defined `f32` rule; and a `cargo fuzz` target on compile that runs under a small stack.

Working proposals carried into PR9 (see "PR9 implementation details"): plain names for dataset fields and identity levels, `quality.status` and `quality.flags` for the envelope, no JSON Pointer and no `^/` in filters because filters run on typed records after the parent message is gone, and explicit integer literal types in the structured form (a spike run that parsed `u64` literals above `i64::MAX` as `f64` silently matched the wrong value).

Constraints that follow from the decision:

- `expr:` is reserved for a future CEL string, and the structured form uses a distinct key, so adding CEL later is additive and the two never share a field.
- Each structured construct is defined with an exact equivalent in the expression language (for example `{field: f, min: a, max: b}` is `f >= a && f <= b`, and for a list field `f.all(x, x >= a && x <= b)`), including null and missing-field behavior, so a later move to CEL is mechanical.
- The predicate representation is typed config; where its types live follows the O1 resolution (declared-field types in `thadam-core`, ARCHITECTURE.md section 9).
- Field paths in `from:` stay JSON Pointer with the `^/` parent prefix. The demo's `from:` keys are already written and are not changed by this ADR.

### PR9 implementation details

These items were left open by the spike report (`docs/design/spikes/ADR-018-filter.md`, sections 5 to 7) and by the ADR text. None blocks acceptance; PR9 (`thadam-transform` part 2) closes each one and records the outcome here.

1. **Key spelling.** The key that carries a structured predicate (`where:` is the working name) and the spelling of the operator keys, confirmed with the ADR-0012 key-spelling review (PR5), and a distinct reserved `expr:`.
2. **Identifier namespace.** Plain names for dataset fields and identity levels, `quality.status` and `quality.flags` for the envelope, and how these map to the prefixed Parquet envelope columns (`_quality_status`).
3. **Nested record access.** Dot access (`a.b`) versus JSON Pointer for fields inside `Value::Record`, and confirming that `^/` has no meaning in filters.
4. **Filter versus `validate`.** Whether `validate` rules share the predicate grammar or stay the structured `{field, min, max, flag, status}` form (the spike found the `validate` shape expressible).
5. **Literal typing.** Explicit integer literal types including `u64` above `i64::MAX`; the `f32` rule (compare in the `f32` domain); the numeric `in` set (not expressible in the spike's grammar) and a separate null test.
6. **Null and missing-field semantics** as written in `spikes/adr018-filter/results/semantics.md`, converted to tests, including `required` fields and the `on_change` interaction.
7. **Bounded cost.** Expression size and evaluation limits through `ChainLimits`.
8. **Error behavior.** An evaluation error becomes a quarantine reject with the raw bytes retained, with no panic; compile errors carry the predicate path and the YAML line and column.
9. **Replay equivalence test** under the structured evaluator (the same frames evaluated twice give an equal row set).
10. **Config-ergonomics review** of the demo's filters and `validate` rules with the owner and one or two target users.
11. **Documented expressiveness gaps** (arithmetic and cross-field comparison, string functions beyond equality and `in`, list quantifiers beyond "every element within min and max", stateful and window rules), all post-MVP.

## Alternatives considered

- **CEL through the `cel` crate.**
  - Pros: open standard, non-Turing-complete, widely used by Kubernetes and Envoy, an active crate (per the original plan), and familiar to infrastructure users.
  - Cons: evaluation cost on ARM is unmeasured. Support for Thadam's value types (`f32`, narrow integers, typed arrays, timestamps, bytes) and null semantics is unknown **[V]**. Error span quality, MSRV, license, and dependency weight are unchecked **[V]**.
- **Structured YAML predicates** (trees such as `{field, op, value}` combined with `all` and `any`).
  - Pros: no expression parser to maintain or fuzz, spans come from the YAML parser, evaluation cost is small and predictable, editor schema completion works through JSON Schema.
  - Cons: verbose for compound conditions, limited expressiveness (no arithmetic or string functions without extending the grammar), and a second syntax to maintain if CEL is added later.
  - Illustrative only (not a proposed syntax) for `level != 'debug'`: `filter: { field: level, ne: debug }`.
- **VRL (Vector Remap Language).**
  - Pros: powerful and proven in a data pipeline.
  - Cons: heavy, Vector-specific, and MPL-2.0. The license allow-list excludes MPL-2.0 until OPC UA lands (ARCHITECTURE.md section 16), so it would need an allow-list change.
- **Rhai.**
  - Pros: full scripting language embedded in Rust.
  - Cons: sandboxing concerns (original plan), and scripting power opens user-defined logic that conflicts with the "no user-defined step trait" rule and with determinism.
- **A custom mini-DSL.**
  - Pros: exactly the semantics Thadam wants, no dependency.
  - Cons: ongoing maintenance, parser fuzzing, documentation, and editor-support cost for a feature that is not Thadam's differentiator (product.md principle 11, "do not reinvent mature infrastructure").
- **No filter in the MVP.** Rejected: the demo's replay story depends on a filter that is wrong and then corrected from the WAL, and UC-05 data reduction depends on filtering.

## Consequences

- A configuration written against the wrong choice is costly to migrate. Structured predicates, each with an exact CEL equivalent, keep a later move to CEL mechanical and additive.
- `thadam-transform` takes on no expression crate in the MVP, and there is no expression parser to fuzz. If a future ADR adopts CEL, the `cel` crate and its dependency tree arrive then, and expression parsing becomes an untrusted-input surface that needs a fuzz target (ADR-0020).
- The demo YAML uses the structured form for `level != 'debug'` and for the buggy filter `robot != 'robot-03'` (and its fixed version), instead of `expr:` strings. A later CEL addition must not break them.
- Either option makes filter semantics part of the replay contract: chains use the current configuration, so correcting a filter and replaying changes which rows are produced.
- Filtered events are counted but not preserved by the chain. The WAL still holds the raw data within the retention window, which is the recovery path the demo shows.
- Updated earlier decisions: the original plan listed `cel` 0.15 and CEL-based filters in PR8 as a given. The architecture review made it conditional (D19), and ARCHITECTURE.md assigns the work to PR9 behind this ADR. The maturity review leaves this ADR "unchanged, CEL still pending its benchmark".

## Evidence and validation required

Evaluated 2026-10-07 in `docs/design/spikes/ADR-018-filter.md` (code and raw output in `spikes/adr018-filter/`; host Apple Silicon (arm64), macOS arm64). Items marked `[x]` have the evidence described; `[ ]` items were not done, failed, or need the owner. The spike set its criterion after seeing the numbers, not before, so it is a reading, not a pass or fail test. The decision was made by the owner on 2026-10-07 (structured predicates). Unticked items that concern only CEL (the ARM benchmark, CEL error spans, CEL bounded cost, the expression parser fuzz target) are no longer gating and move to the future CEL ADR; unticked items that concern the structured form or the filter step are PR9 implementation details (see "PR9 implementation details").

- [x] **Owner decision on the MVP filter language**, recorded 2026-10-07 in `docs/design/owner-decisions.md` (B7): structured predicates; CEL reserved.
- [ ] **CEL per-event evaluation cost on ARM** (Raspberry Pi 5 class, Jetson-class if available) versus x86, at the W1 and W2 shapes, including compile cost and allocations per event. **Partly done:** measured on Apple Silicon (arm64) only, on a 13-field demo shape and a 67-field shape, five filters; ns per event, allocations and compile cost are in the report (CEL compile 23 to 51 µs, structured 2.5 to 11.7 µs). Reference ARM hardware and x86 Linux not measured. [OPEN: criterion; the spike read the results against the D8 parse cost and the withdrawn 50,000 events/s hypothesis]
- [x] **Value-type support.** `cel` values are `Int(i64)`, `UInt(u64)`, `Float(f64)`, `String`, `Bytes`, `Bool`, `Null`, `List`, `Map`, plus `Timestamp` and `Duration` with the `chrono` feature. No `f32` (widens to `f64`), no narrow integers, and typed arrays become lists of floats (a per-event conversion cost). `uint` is exact above `i64::MAX` with `u` literals. Null is `Null`. Timestamps and bytes were not exercised.
- [x] **Determinism.** `now()`, `rand()` and `random()` are undeclared in `Env::stdlib()`; `timestamp('...')` of a constant works. Every engine agreed with a Rust reference on 10,000 events per case in two runs. The full function list was not audited.
- [x] **Numeric comparison semantics** written as probes (`spikes/adr018-filter/results/semantics.md`; to be converted to tests in PR9): int versus double and `uint` versus `int` compare correctly in CEL; `f32` versus a decimal literal is a hazard in CEL (`x == 0.1` false) and handled in the structured evaluator by comparing in the `f32` domain; `evalexpr` has `1 == 1.0` false.
- [x] **Null and missing-field semantics** written as probes for CEL, the structured evaluator and `evalexpr`: in CEL a null compares false for `==` and true for `!=` with no error, a missing identifier or map key is a runtime error, `has()` guards a key. `required` fields and `on_change` interaction not covered.
- [ ] **Error behavior:** an evaluation error yields a quarantine reject with raw bytes retained and no panic. Partly done: CEL returns `Err` (no panic) for overflow, division by zero, missing names and type errors, which a chain could route to quarantine; no chain exists yet to show the quarantine path. Two crash modes were found outside normal evaluation: stack overflow on long operator chains and a `Display` panic on very long single-line expressions.
- [ ] **Error message quality with spans.** CEL: several syntax errors per expression, each with a 1-based line and column (`ParseError.pos`) and a caret snippet; wording is parser-generated and the recursion-limit error prints as a debug dump; unknown identifiers and functions are not compile errors. Structured: YAML schema errors with line and column, and compile errors with predicate paths. **Mapping an offset in an expression to a YAML line and column was not built.**
- [x] **MSRV and license of the crate, advisory check, `cargo deny`, dependency weight.** `cel` 0.15.0: MIT, declared MSRV 1.86 (a fresh resolve on Rust 1.88.0 fails because `uuid` 1.27.0 needs 1.89; an MSRV-aware resolver would pick an older one), no RustSec advisory (advisory-db `b0797f5`, 2026-10-07), `cargo deny check advisories licenses bans sources` passes, 42 crates in the tree, clean release build 8.7 s on macOS arm64. `evalexpr` 13.1.0 is AGPL-3.0-only and fails the license check.
- [ ] **Bounded cost:** an expression-size and evaluation-cost limit exists, and a pathological expression is rejected at compile time or bounded at evaluation. **Fails for `cel` as shipped:** no cost budget; 608 bytes of nested comprehensions took about 0.7 s per evaluation; a 1,000-term `+` chain (21 KB) aborted the process on a 2 MiB stack and a 5,000-term chain on 8 MiB; the parser recursion limit (96) covers parentheses and lists only. The structured form is bounded by the YAML budget (depth 64) and compiles in microseconds. [OPEN: limits]
- [ ] **Config-ergonomics review** by the owner and one or two target users, with the demo's three filters and the `validate` rules written in both syntaxes. The two forms of each filter are in the spike report, section 4.1; no review was done.
- [x] **Structured-predicate expressiveness gaps:** the demo, the buggy and fixed robot filter and the `validate` rule shape are all expressible. Not expressible: arithmetic and cross-field comparison, string functions beyond equality and `in`, list quantifiers beyond "every element within min and max", a separate null test, numeric `in` sets, and stateful or window rules (product.md section 10 P1 anomaly rules, UC-05 windowing and downsampling). From reading product.md, not from users.
- [ ] **Parser fuzzing** of the expression parser, as a `cargo fuzz` target (ADR-0020). Not done. Hand-made inputs already found the stack overflow and the `Display` panic above.
- [ ] **Replay equivalence:** a test that evaluating the same frames twice yields an equal row set, under the chosen language. Not done.
- [ ] **Decision on the field-path syntax** for filters and the identifier namespace, recorded here. A proposal is in the Decision section; PR9 detail 2 and 3.

## Revisit when

- Users need expressions beyond the structured grammar (arithmetic or cross-field comparison, string functions); that is the trigger for the future CEL ADR, which would also need ARM measurements on reference hardware.
- The `cel` crate stalls or gets a RustSec advisory, if a CEL ADR is in progress.
- ARM measurements on reference hardware show the structured evaluator is not materially cheaper than CEL through a resolver.
- Windowing or aggregation (post-MVP) arrives, which needs expressions over groups of events rather than single events.
- A plugin mechanism (ADR-0017) changes where user logic can live.
- Real configs show the identifier namespace is confusing or too restrictive.

## References

- ARCHITECTURE.md sections 3 (D19), 7 (transform order, determinism), 9 (crate rules, O1), 13 (error model), 15 (configuration), 16 (license allow-list), 21 (PR9), 22.
- ADR-0001 (canonical model and value types), ADR-0003 (chains), ADR-0011 (deduplication responsibility), ADR-0012 (config format; compile-time validation, key spelling), ADR-0017 (plugin mechanism, Reserved), ADR-0020 (fuzzing, license and dependency policy).
- product.md sections 5 (UC-05), 10, 14, 36 (principle 11), 37.
- Architecture review, D19. Original plan, section H (expression language). Implementation plan, section 5.4. Demo plan, section 6 (pipeline YAML) and the Level 4 replay scenario (buggy and fixed filter).
- Upstream: the Common Expression Language specification and the `cel` crate documentation; RFC 6901 (JSON Pointer).
