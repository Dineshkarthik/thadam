# Spike report: ADR-0012 YAML crate for `thadam-config`

- **Date:** 2026-10-07
- **Feeds:** ADR-0012 (config format and YAML crate), readiness-gate blocker B7 (decide before PR5)
- **Code and raw output:** `spikes/adr012-yaml/` (own `[workspace]`; the root workspace excludes `spikes/`)

## 1. Summary

**Recommendation: `serde-saphyr` 1.x behind `thadam-config`, configured with `strict_booleans: true`, the default duplicate-key error, a tightened `Budget`, and `reject_unsupported_tags: true`. Keep `serde_norway` as the documented fallback.** Confidence: **medium**. The evidence on maintenance, diagnostics and input safety is clear. Confidence is held down by three things: `serde-saphyr` is young (first release 2025-09-27, 1.0.0 on 2026-07-31), it is essentially one maintainer (769 of 845 commits), and it needs Rust 1.89 where the workspace declares 1.88.

Headline findings, all measured in this spike:

1. **`serde_norway` 0.9.42 and `serde_yaml_ng` 0.10.0 behave identically** in every test (same text, same locations, byte-identical output). Both are small forks of `serde_yaml` 0.9.x (their READMEs say so; the fork maintainer's own history is 43 and 36 of about 975 commits). Choosing between them is a question of release recency and license only. `serde_norway`'s last crates.io release is 2024-12-21 (9.5 months before the evaluation date); `serde_yaml_ng`'s is 2024-05-26 (16 months).
2. **The Norway problem does not occur** with the forks. Unquoted `no`, `yes`, `on`, `off`, `y`, `n` are strings for `String` and `serde_json::Value` targets, and an error (not a bool) for `bool` targets. `serde-saphyr` with default options does the opposite: `no` becomes `false` for a `bool` field **and `Bool(false)` in a `serde_json::Value`**, which is the D22 connector boundary. `strict_booleans: true` fixes this and makes it match the forks. It must be set.
3. **Error locations.** For unknown fields, wrong scalar types and syntax errors, all three report the right line and column. For errors raised by a custom `Deserialize` impl (durations, sizes) and for errors inside an internally tagged enum (`transforms[].kind`), **the forks report the wrong line**: the start of the enclosing mapping (4 lines off in `retain_for`), or the first item of the sequence (3 to 7 lines off for `transforms[1]` and `[2]`). `serde-saphyr` is exact for the first and gets the right item for the second, and renders a compiler-style snippet with a caret.
4. **`serde_path_to_error` works with all three**, giving full paths (`wal.retain_for`, `datasets[0].fields[1].fromm`). Inside internally tagged enums it stops at the item (`transforms[1]`), a serde limitation that affects all three.
5. **None collects multiple errors.** Each stops at the first. A proof of concept in this spike collects all five errors of a five-error fixture with correct line and column, from either crate, by parsing once to `serde_json::Value`, validating each section and list item separately with `serde_path_to_error`, and getting spans from a small path-to-span index built from the `granit-parser` event stream (about 70 lines).
6. **Duplicate keys, merge keys, anchors.** The forks silently keep the last duplicate key in any map or `serde_json::Value` (struct targets do error), do not apply `<<` merge keys when deserializing into typed structs (they error under `deny_unknown_fields`, and keep a literal `<<` key in a `Value`), and have no option to change either. `serde-saphyr` errors on duplicates by default and merges `<<` natively. All three support plain anchors and aliases and reject undefined anchors with line and column.
7. **Input safety.** All three bound alias expansion and recursion. The forks take quadratic time on very deep nesting (13 s for a 100,000-deep flow sequence, 130 ms at 10,000); `serde-saphyr` rejects it in about 0.1 ms. Unknown YAML tags (`!!python/object:...`) are ignored by all three, with no construction of objects.
8. **Advisories, licenses.** No RustSec advisory exists for any of the three crates or for `granit-parser` and `unsafe-libyaml-norway` as of advisory-db commit `b0797f5` (2026-10-07). The one advisory on a backend is RUSTSEC-2023-0075 on `unsafe-libyaml` (unsound unaligned write on 32-bit and 16-bit targets, fixed in 0.2.10); `serde_yaml_ng` resolves 0.2.11. `cargo deny check advisories licenses bans sources` with the repository's `deny.toml` passes for all three. The forks sit on `unsafe-libyaml(-norway)`, a c2rust translation of libyaml; `serde-saphyr` and its parser `granit-parser` both declare `#![forbid(unsafe_code)]`.
9. **Swapping is cheap.** Parsing the demo into the same `PipelineSpec` gives **identical canonical JSON (3,619 bytes) from all three**, so anything computed from the typed spec (such as the config hash, not implemented here) does not depend on the crate.

## 2. Environment

| Item | Value |
|---|---|
| Host | arm64 laptop, 18 cores, 48 GB, macOS |
| Toolchain | rustc/cargo 1.99.0 (pinned by the repository's `rust-toolchain.toml`); Rust 1.88.0 installed separately for the MSRV check |
| Crates (exact) | `serde_norway` 0.9.42, `serde_yaml_ng` 0.10.0, `serde-saphyr` 1.3.0 (parser `granit-parser` 1.3.0), `serde_path_to_error` 0.1.20, `serde` 1.0.229, `serde_json` 1.0.151, `humantime` 2.4.0, `bytesize` 2.7.0 |
| Date of evaluation | 2026-10-07 (crates.io API, GitHub API 13 unauthenticated calls, RustSec advisory-db clone at commit `b0797f5`, 2026-10-07) |
| Machine load | Not idle. `top` before the second ADR-018 run read load average 3.3 / 4.9 / 5.1 and 79% idle. The YAML runs are single-threaded and short, so only the timings in section 5.9 are sensitive. |

Not measured: Linux x86_64, Linux aarch64 or Cortex-A class ARM, Windows.

## 3. Candidates and sources

| | `serde_norway` | `serde_yaml_ng` | `serde-saphyr` |
|---|---|---|---|
| Latest version, crates.io date | 0.9.42, **2024-12-21** | 0.10.0, **2024-05-26** | 1.3.0, **2026-09-16** |
| Releases | 7 since 2024-06-11 | 3 since 2024-05-03 | 37 since 2025-09-27 (1.0.0 on 2026-07-31) |
| License (crates.io) | MIT OR Apache-2.0 | MIT (0.10.0; 0.9.x were MIT OR Apache-2.0) | MIT OR Apache-2.0 |
| Declared MSRV | 1.71.1 | 1.64 | **1.89** (refuses on 1.88; `cargo check --ignore-rust-version` on 1.88.0 succeeded) |
| Parser backend | `unsafe-libyaml-norway` 0.2.15 (2024-12-21), c2rust libyaml | `unsafe-libyaml` 0.2.11 (2024-03-17), c2rust libyaml | `granit-parser` 1.3.0 (2026-09-16, MIT OR Apache-2.0, MSRV 1.81), fork of `saphyr-parser`, pure Rust |
| `unsafe` in own source | not forbidden | not forbidden | `#![forbid(unsafe_code)]` (crate and parser) |
| GitHub, last push (default branch last commit) | 2025-08-04 (2024-12-21) | 2025-09-14 (2025-09-14, a merge) | 2026-10-06 |
| GitHub, contributors | 39; 873 of 976 commits are the original author's; fork maintainer 43 | 41; 873 of 973 are the original author's; fork maintainer 36 | 18; **769 of 845 by one person**, 40 by dependabot |
| Open issues plus PRs | 11 | 20 | 8 |
| crates.io downloads, last 90 days | 5.7 M | 9.0 M | 6.9 M |
| RustSec advisories (advisory-db `b0797f5`) | none (backend: none) | none (backend `unsafe-libyaml`: RUSTSEC-2023-0075, fixed in 0.2.10, 0.2.11 is resolved) | none (backend: none) |

Other candidates considered briefly, not tested here:

- **`serde_yaml`**: deprecated (0.9.34+deprecated), banned by ADR-0020. Its only advisory is RUSTSEC-2018-0005, long patched.
- **`serde_yml` and `libyml`**: RUSTSEC-2025-0068 and RUSTSEC-2025-0067 (unsound, repositories archived). Must not be used.
- **`saphyr` 0.1.0 (2026-09-19) and `yaml-rust2` 0.13.0 (2026-09-11)**: maintained, no advisories, but they are low-level YAML libraries with no serde integration. They would only matter if Thadam wrote its own typed layer. `granit-parser` plays that role for the path-to-span index below.
- **TOML, JSON5, a custom YAML subset**: already weighed in ADR-0012's alternatives. Nothing in this evidence changes that.

## 4. Method

`spikes/adr012-yaml/` defines a typed `PipelineSpec` (`src/model.rs`) for the demo pipeline (demo plan section 6, copied to `fixtures/pipeline.s3.yaml`), with `deny_unknown_fields` on every struct, `serde_json::Value` for connector `config` blocks (D22), an internally tagged `Transform` enum (`kind: validate | on_change | filter`), and custom `Deserialize` impls for durations (`humantime`), sizes (`bytesize`) and counts (`500k`) so that custom errors are exercised.

Each crate is driven through `serde_path_to_error::deserialize` over its own `Deserializer` (for `serde-saphyr` through `with_deserializer_from_str_with_options`, returning the real error from the closure). The harness prints, per crate, the line and column, the serde path, and the message. `serde-saphyr` runs with `with_snippet: false` in tables, and with default options where snippets are shown.

Experiments, with the section letters used in `results/report.md`:

- **A**: the demo into `PipelineSpec` for each crate; canonical JSON equality.
- **B, C**: 20 malformed variants of the demo (misspelled keys, wrong types, missing key, bad enum, bad duration, size and count, duplicate key, bad tag, internally tagged errors, syntax errors, stray `---`, empty file) with location, path, and the crate's own rendering.
- **D, I**: multi-error fixtures; single pass versus the collection PoC.
- **E**: scalar coercion tables (`String`, `bool`, `i64`, `f64`, `Option<String>`, `serde_json::Value`) over 13 to 23 inputs each, for each crate and for `serde-saphyr` with `strict_booleans`.
- **F, G**: duplicate keys; anchors, aliases, merge keys, undefined anchors, unknown tags.
- **H**: path-to-span index from the `granit-parser` event stream (`src/spans.rs`).
- **J**: duration, size and count scalars from the ADR checklist (`50ms`, `5m`, `1h`, `1d`, `128MiB`, `500k`).
- **K**: `serde_path_to_error` paths.
- `scripts/stress.sh`: alias-expansion ("billion laughs", fan-out 9) and deep-nesting inputs, one child process per case; `bench`: parse time.
- Dependency weight: a fresh one-dependency project per crate, `cargo tree` and a clean release build. MSRV: `cargo +1.88.0 check`. Policy: `cargo deny --config ../../deny.toml check advisories licenses bans sources`.

## 5. Results

### 5.1 The demo parses identically (A)

All three parse `pipeline.s3.yaml` into `PipelineSpec` (1 source, 2 datasets, 3 transforms, 2 sinks) with no diagnostics, and serialize to the same 3,619-byte canonical JSON.

### 5.2 Error location (B, C, H)

Positions are `line:column`, 1-based, in `fixtures/pipeline.s3.yaml` after the edit. "True" is the node the user must change.

| Case | True position | `serde_norway` and `serde_yaml_ng` | `serde-saphyr` |
|---|---|---|---|
| B1 misspelled top-level key `wall` | 20:1 | 20:1 | 20:1 |
| B2 unknown key `fromm` in a list item | 39:48 | 39:48 | 39:48 |
| B3 `worker_threads: two` (type) | 100:28 | 100:28 | 100:28 |
| B6 `on_full: blok` (enum variant) | 24:12 | 24:12 | 24:12 |
| B5 missing required key `on_full` | the `wal` mapping (20:1 key, 21:3 first entry) | 21:3 | 24:3 (last entry) |
| B10 duplicate key `retain_for` | 26:3 | **21:3** (mapping start, message names the field) | 26:3 |
| B7 custom error, `retain_for: 1hr30` | 25:15 | **21:3** (4 lines off) | 25:15 |
| B8 custom error, `max_disk: 2GiBB` | 21:13 | 21:3 (right line by coincidence) | 21:13 |
| B9 custom error, `max_keys: 5oo` inside a flow mapping | 35:37 | **35:12** (start of the flow mapping) | 35:37 |
| B11 unknown variant `kind: validte` | 60:11 | 60:11 | 60:5 (item start) |
| B12 unknown key in `transforms[2]` (internally tagged) | 67:5 (item) | **60:3** (first item, 7 lines off) | 67:5 |
| B13 wrong type in `transforms[1].heartbeat` | 63:5 (item) | **60:3** (first item, 3 lines off) | 63:5 |
| B15 unclosed flow mapping (syntax) | 103:8 | 103:8, plus the opening mark 102:9 | 103:8 |
| B16 tab indentation (syntax) | 3:1 (the tab) | 3:1, "found character that cannot start any token" | 3:2, "tabs disallowed within this context (block indentation)" |
| B18 stray `---` before `sinks:` | the `---` | 1:1, "missing field `sinks`" (no hint about a second document) | 59:1, "missing field `sinks`" |
| B19 empty file | 1:1 | 1:1, "missing field `version`" | 1:1, "unexpected end of input" |

Observations:

- The forks attach the line and column of the mapping that was being read when a custom `Deserialize` impl failed, because their `custom` errors are located after the fact. Anything the config does through a custom impl (durations, sizes, expression strings in ADR-0018) therefore points at the parent. Their own `Display` also omits the sequence index (`transforms: invalid type...`), though `serde_path_to_error` restores it.
- Internally tagged enums buffer their content, so no crate can locate a field inside one. `serde-saphyr` still reports the item. The path stops at `transforms[1]` for all three, so a user sees the item but not the key. If a better path is wanted, `Transform` should be dispatched manually on `kind`, not with `#[serde(tag = "kind")]`.
- **A path-to-span index fixes the location gap for any crate (H).** `src/spans.rs` walks the `granit-parser` event stream (`Parser::new_from_str`, re-exported as `serde_saphyr::granit_parser`) and maps each `serde_path_to_error` path (`wal.retain_for`, `transforms[2]`) to the key or item position. Looking up the paths the forks reported gave 25:3 for B7 (true line 25), 35:27 for B9, 67:5 and 63:5 for B12 and B13, and 39:48 for B2. This is a position of the key for map entries and of the item for sequence entries, not of the value: close enough to point a user to the right line, not the exact caret. It does not expand aliases or merge keys.
- `serde-saphyr` Display renders a snippet with a caret (case C), and offers `render_with_formatter` and a `miette` feature. The forks give one line of text.

### 5.3 `serde_path_to_error` (K)

Works with all three. Paths match for unknown fields (`datasets[0].fields[1].fromm`), type errors (`runtime.worker_threads`), and custom errors (`wal.retain_for`). Inside the internally tagged enum all three stop at `transforms[1]`. `serde-saphyr`'s own message does not contain the path, so a path comes only from `serde_path_to_error` (or its own `path_map` for the `garde` and `validator` features, not used here).

### 5.4 Multi-error collection (D, I)

A fixture with five unrelated errors (stray top-level key, bad duration, unknown nested key, wrong type in a tagged transform, wrong number) reports **one** error in a single pass for every crate (`wal.retain_for`, in field order). With the PoC (parse to `serde_json::Value`, validate each section and each list item with `serde_path_to_error`, spans from the index), all three crates report all five, each with line and column:

| Position | Path | Message |
|---|---|---|
| 105:1 | `wall2` | unknown top-level key |
| 25:3 | `wal.retain_for` | bad duration "1hr30" |
| 39:48 | `datasets[0].fields[1].fromm` | unknown field `fromm` |
| 63:5 | `transforms[1]` | invalid type: sequence, expected a string |
| 100:12 | `runtime.worker_threads` | invalid type: string "two", expected u32 |

Limits of the PoC: the unknown-top-level check is hand-written; a list section reports one error per item, not several per item; duplicate keys are invisible to it with the forks (the `Value` keeps the last), so the index pass should flag duplicate keys itself.

### 5.5 YAML 1.1 versus 1.2 (E)

Full tables in `results/report.md` section E. Rows that matter:

| Input | Target | `serde_norway`, `serde_yaml_ng` | `serde-saphyr` (default) | `serde-saphyr` `strict_booleans` |
|---|---|---|---|---|
| `no`, `yes`, `on`, `off`, `y`, `n` | `String` | the text | the text | the text |
| same | `bool` | **error** (`invalid type: string "no", expected a boolean`) | **`no` is `false`, `yes` is `true`**, and so on | error |
| same | `serde_json::Value` | `String("no")` | **`Bool(false)`** | `String("no")` |
| `true`, `True`, `TRUE` | `bool` | `true` | `true` | `true` |
| `1`, `0` | `bool` | error | error | error |
| `014` | `serde_json::Value` | `String("014")` | **`Number(14.0)`** (leading zero lost) | `Number(14.0)` |
| `1_000` | `serde_json::Value` | `String("1_000")` | **`Number(1000)`** | `Number(1000)` |
| `1_000` | `i64` | error | 1000 | 1000 |
| `'42'` and `'1.5'` (quoted) | `i64`, `f64` | error | **42** (quoted string accepted as a number) | 42 |
| `version: "1"` (quoted) | `u32` | error | **accepted** | accepted |
| `12:30:45`, `2024-01-31`, `1.10` | `String` | the text | the text | the text |
| `12:30:45` | `serde_json::Value` | `String` | `String` | `String` |
| `0x1F`, `0o14`, `0b101`, `+5` | `i64` | 31, 12, 5, 5 | 31, 12, 5, 5 | same |
| `1e3` | `i64` | error | error | error |
| `.inf`, `.nan` | `f64` | inf, NaN | inf, NaN | inf, NaN |
| `.inf` | `serde_json::Value` | **`Null`** (silently) | error (non-finite) | error |
| `null`, `~` | `String` | the text for `null` and `~` (the forks) | error ("cannot deserialize null into string") | error |
| `null`, `~`, `Null`, empty | `Option<String>` | `None` | `None` | `None` |

Reading:

- The forks are effectively YAML 1.2 core for booleans: only `true` and `false` in three spellings. This is the safer default for hand-written config.
- `serde-saphyr` is type-driven: the Rust field type decides how a plain scalar is read. That avoids the Norway problem for `String` fields in every mode. Its two surprises are the YAML 1.1 boolean words for `bool` targets and `serde_json::Value`, fixed by `strict_booleans`, and a few number forms at the `Value` boundary (`014`, `1_000`) plus acceptance of quoted numbers. Config types should declare `String` where text is intended, as ADR-0012 already says.
- With D22, connector configs reach connectors as `serde_json::Value`. A scalar like `client_id: 014` becomes the number 14.0 under `serde-saphyr` and the string `"014"` under the forks, and a connector with a `String` field fails under the first and succeeds under the second. Either way, the documentation must say: quote identifiers.
- Durations, sizes and counts in the demo are plain strings in all three crates. `50ms`, `5s`, `10m`, `1h`, `2GiB`, `256MiB` and `500k` parsed in A, and `50ms`, `5m`, `1h`, `1d` (86,400 s), `128MiB` and `500k` parsed identically by all three in J, through `humantime` 2.4.0, `bytesize` 2.7.0 and the spike's count parser. `session_expiry: 1d` itself sits inside a connector `config` `Value` block, so the demo file does not duration-parse it; the typed check is in J.

### 5.6 Duplicate keys (F)

| Target | `serde_norway`, `serde_yaml_ng` | `serde-saphyr` |
|---|---|---|
| Struct | error `duplicate field` (located at the struct, see B10) | error `duplicate mapping key`, exact position |
| `BTreeMap` | **last wins, silently** | error |
| `serde_json::Value` | **last wins, silently** | error |
| Configurable | no | `DuplicateKeyPolicy::{Error, FirstWins, LastWins}` |

Connector `config` blocks are `serde_json::Value`, so with the forks a duplicate key inside `sources[].config` is silently accepted.

### 5.7 Anchors, aliases, merge keys (G)

| Case | Forks | `serde-saphyr` |
|---|---|---|
| Plain alias to a mapping (`- *f`) | works | works |
| Merge key `<<: *base` into a `deny_unknown_fields` struct | **error: unknown field `<<`** | merged, struct filled |
| Merge key into `serde_json::Value` | keeps a literal `"<<"` key | merged |
| Alias to an undefined anchor | error, `unknown anchor at line 2 column 5` | error, same position |
| Top-level `x-common: &c {...}` with `deny_unknown_fields` | error: unknown field `x-common` | error |
| Tag `!!python/object:os.system` on a mapping | ignored | ignored |

The forks' `Value::apply_merge` exists but works on the untyped tree, so it loses the location link. `deny_unknown_fields` also rejects a top-level `x-` section used only to hold anchors, in every crate. If anchors are to be documented as a feature, ADR-0012 needs a named place for reusable fragments. If not, the documentation should say anchors and merge keys are unsupported, which the forks enforce for merge keys in typed structs as a side effect.

### 5.8 Safety limits (`scripts/stress.sh`)

| Input | Forks | `serde-saphyr` |
|---|---|---|
| Alias fan-out 9, 5 levels (about 59,000 expanded scalars), into `Value` | error `repetition limit exceeded` in about 4 ms, 6 MiB | succeeds in 16.6 ms, 16.6 MiB (415 KB of JSON) |
| Fan-out 9, 7 and 9 levels | same error in 5 to 7 ms, 7.7 to 9.2 MiB | error `budget breached: Nodes { nodes: 250001 }` (limit 250,000) in 54 to 60 ms, 45 to 46 MiB |
| Flow sequence 100 deep | ok | error `budget breached: Depth { depth: 65 }` (the default budget allows 64 levels) |
| 1,000 deep | error `recursion limit exceeded` in 1.7 to 2.3 ms | error `recursion limit exceeded` in 0.1 ms |
| 10,000 deep | error in 130 to 137 ms | error in 0.1 ms |
| 100,000 deep | error after **12.9 to 13.1 s**, 41 MiB | error in 0.1 ms, 3 MiB |
| Process exits | normal in all runs | normal in all runs |

The forks' time grows roughly with the square of the depth (1.7 ms, 130 ms, 13 s at 1,000, 10,000 and 100,000 levels), which is consistent with parsing the whole input before the recursion check; the cause was not traced. The input is the operator's own file, so this is a robustness issue, not an attack surface, but `thadam validate` on a mangled file should not hang. `serde-saphyr`'s defaults (250,000 nodes, depth 64) are far above any real config (the demo has a few hundred nodes and about 7 levels) and are configurable through `budget!`.

### 5.9 Weight, toolchain, speed

| | `serde_norway` | `serde_yaml_ng` | `serde-saphyr` (default features) |
|---|---|---|---|
| Crates added beyond `serde` and its derive baseline | 6 (`equivalent`, `hashbrown`, `indexmap`, `itoa`, `ryu`, `unsafe-libyaml-norway`) | 6 (same, `unsafe-libyaml`) | 16 (`annotate-snippets`, `anstyle`, `arraydeque`, `base64`, `cfg-if`, `encoding_rs`, `encoding_rs_io`, `granit-parser`, `multiversion_no_op`, `nohash-hasher`, `num-traits`, `scopeguard`, `simdutf8`, `smallvec`, `unicode-width`, `zmij`) |
| Clean release build of a one-dependency project (includes `serde` derive, one run) | 4.2 s | 3.5 s | 6.3 s |
| Builds on Rust 1.88.0 | yes | yes | no, declared 1.89; the code compiled on 1.88.0 with `--ignore-rust-version` |
| `cargo deny check advisories licenses bans sources` (repo `deny.toml`) | ok | ok | ok |
| Typed parse of the 4,007-byte demo, with `serde_path_to_error` (5,000 iterations, three runs) | 80.6 to 83.3 µs | 80.5 to 81.0 µs | 133.4 to 138.2 µs |

Parse speed is irrelevant for a file read once at startup; it is reported for completeness. `serde-saphyr` has `default-features = false, features = ["deserialize"]`, which should remove the serializer; that was not measured. ADR-0020 says the declared `rust-version` rises to 1.89 when the P1 crates land (`clickhouse` and `socketcan` need 1.89), and the pinned toolchain is 1.99.0, so the 1.89 floor is a timing question, not a new constraint.

### 5.10 Items from the ADR checklist not covered here

Not tested: `insta` snapshot fixtures, interpolation (`${env:}`, `${file:}`), `Secret<T>` redaction, `schemars` JSON Schema, config hash stability, version-policy tests, and the owner syntax review. These are PR5 work, independent of the crate choice, except for hashing and interpolation, which operate on the typed spec or on scalars after parsing and so do not depend on the YAML crate. `serde-saphyr` ships an opt-in `properties` interpolation feature (`${NAME}`, plain scalars only); it was not evaluated and its syntax does not match `${env:X}`.

## 6. Limitations (read before relying on the numbers)

- One host, macOS arm64, one run for build times and the stress inputs; the parse timing has three runs. No Linux, no Cortex-A class device.
- **Release-recency threshold was not set before the evaluation.** ADR-0012 left it [OPEN]. A threshold of "a release in the last 12 months" gives `serde_norway` a pass (9.5 months) and `serde_yaml_ng` a fail (16 months), but it was chosen after seeing the dates.
- The custom errors come from this spike's own `Dur`, `Size`, `Count` impls. Any `Deserialize` impl that fails through `Error::custom` will behave the same way.
- `serde-saphyr` was run with default features except `with_snippet: false` in tables, and `strict_booleans: true` where stated. The `properties`, `include`, `robotics`, `garde`, `validator` and `miette` features were not enabled.
- The path-to-span PoC handles block and flow mappings and sequences with scalar keys. It does not expand aliases or merge keys, does not handle complex keys, and was not fuzzed. It maps to the key, not the value, of a mapping entry.
- GitHub activity is a snapshot from 13 unauthenticated API calls; counts include bots and merge commits.
- Only the demo shape of config was exercised. Large or deeply anchored configs were not.

## 7. Recommendation for ADR-0012

### 7.1 The YAML crate

**Adopt `serde-saphyr` 1.x** behind `thadam-config`, with these settings, and name them in the ADR so they are tested:

- `strict_booleans: true` (otherwise `no` is `false` in a connector `Value`).
- Duplicate keys: keep the default (`Error`).
- `reject_unsupported_tags: true`; no `include`, `robotics` or `properties` features.
- A `Budget` tightened to about 10,000 nodes and depth 32 (any real config is far below this; the numbers are a suggestion, not a measurement).
- A `granit-parser` path-to-span index (the PoC in `spikes/adr012-yaml/src/spans.rs`) for locations that `serde-saphyr` does not give natively (tagged enums, `Value`-derived errors) and for duplicate-key detection, and a collection pass like the PoC for multi-error output.
- Decide on anchors and merge keys: either document them as supported (then add a named section for fragments, since `deny_unknown_fields` rejects `x-` keys) or reject them with a budget of zero aliases.

Why this over the forks, in the ADR's own order:

1. **Maintenance and advisories.** `serde-saphyr` and `granit-parser` released within the last month, 37 releases in a year, 8 open issues and PRs. The forks are dormant: last releases 9.5 and 16 months old, and both are small deltas over an archived upstream. No advisories for any of them today, so this criterion is about direction, not current risk.
2. **Diagnostics.** Exact locations for custom errors, duplicate keys, and snippets with a caret. The forks are wrong on exactly the errors that ADR-0018 will add (expression strings) unless the path index is built regardless. The index is built anyway for multi-error output, which narrows this gap, but only `serde-saphyr` gives it natively.
3. **Safety on untrusted-shaped input.** Duplicate-key errors including inside `Value`, native merge keys, bounded budgets with fast failure, `unsafe_code` forbidden. The forks silently accept duplicate keys at the connector boundary and take quadratic time on deep nesting.
4. **Toolchain fit.** The forks win: MSRV 1.88, 6 versus 16 extra crates, 2 to 3 s less clean build time. `serde-saphyr` needs 1.89, which ADR-0020 expects to adopt anyway.
5. **Replaceability.** Equal. The typed model is plain serde, and the canonical JSON of the demo is identical across crates, so a swap is a local change in `thadam-config` and the fixture suite catches differences.

**What would change this:** the owner declining a 1.89 floor before the P1 crates land (then `serde_norway`, with the path index and an explicit "no anchors, no merge keys" rule), or `serde-saphyr` failing a longer-run test of its stability or maintainer continuity (769 of 845 commits are one person; the forks' maintenance is no better, but their code is frozen rather than moving).

### 7.2 Fallback

`serde_norway` over `serde_yaml_ng`: same behavior, a more recent release, `MIT OR Apache-2.0`, and MSRV 1.71.1. The fallback carries three requirements the primary does not: a path-to-span index for custom errors; an explicit check for duplicate keys in `Value`; and a documented rule against merge keys.

### 7.3 ADR status

**Update 2026-10-07 (after this report):** the owner chose `serde-saphyr` with `strict_booleans: true`, raised the workspace MSRV to 1.89, kept `serde_norway` as the fallback, and accepted ADR-0012. The items below are recorded there as PR5 implementation details. The text that follows is the report's original recommendation.

Keep ADR-0012 **Draft**. The YAML-crate evidence is largely conclusive. Moving to Proposed needs the owner to settle items this spike cannot: the 1.89 floor, the release-recency criterion, the key spelling and the hash scope (still [OPEN] in the ADR), and a syntax review of the demo file. The checklist items in section 5.10 are PR5 acceptance tests.

## 8. Reproduction

From `spikes/adr012-yaml/` (the repository's pinned toolchain is picked up from the root `rust-toolchain.toml`). Use `CARGO_TARGET_DIR=target` if your environment sets a shared target directory.

```bash
cargo build --release
./target/release/adr012-yaml report > results/report.md      # sections A to K
./scripts/stress.sh | tee results/stress.txt                  # alias bombs and deep nesting (macOS: /usr/bin/time -l, perl)
./target/release/adr012-yaml bench 5000 | tee results/bench.txt
cargo deny --config ../../deny.toml check advisories licenses bans sources
```

MSRV and weight (each in a scratch project with one dependency):

```bash
rustup toolchain install 1.88.0 --profile minimal
cargo +1.88.0 check                       # serde-saphyr: "requires rustc 1.89"
cargo +1.88.0 check --ignore-rust-version # serde-saphyr: compiles
cargo tree -e normal --prefix none | sort -u
```

Metadata used (re-run to refresh):

```bash
curl -s -A thadam-spike https://crates.io/api/v1/crates/serde_norway       # also serde_yaml_ng, serde-saphyr, granit-parser, ...
git clone --depth 1 https://github.com/rustsec/advisory-db.git            # evaluated at b0797f5
curl -s https://api.github.com/repos/bourumir-wyngs/serde-saphyr           # unauthenticated GitHub API, about 45 calls per hour budget
```
