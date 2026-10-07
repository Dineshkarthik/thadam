# ADR-0012: Config format and YAML crate

- **Status:** Accepted
- **Date:** 2026-10-07
- **Updated:** 2026-10-07 (spike ADR-012-yaml evidence folded in; owner decision recorded; accepted)
- **Deciders:** Project owner

**Acceptance.** Accepted by project owner on 2026-10-07 (`docs/design/owner-decisions.md`, B7). The owner chose **`serde-saphyr` with `strict_booleans: true`**, raised the workspace MSRV to **Rust 1.89** (ADR-0020, root `Cargo.toml`), and kept **`serde_norway` as the documented fallback**. The YAML format with a mandatory top-level `version: 1` is accepted as written below. The items the spike report (`docs/design/spikes/ADR-012-yaml.md`, sections 5.10, 6, and 7) left open are not blockers: they are PR5 implementation details, listed under "PR5 implementation details" in the Decision section and in the unticked items of "Evidence and validation required".

## Context

Thadam is configured by one declarative file per process. product.md section 14 requires that "the configuration format must remain stable and versionable", and section 31 lists YAML configuration as an MVP interface. The crate that parses it is `thadam-config` (ARCHITECTURE.md section 15, PR5).

**A decision is needed before PR5** (`thadam-config`, `thadam validate`, `thadam version`); ARCHITECTURE.md section 21 lists ADR-0012 among the READY gates.

The decision has two parts with very different reversibility:

- **The YAML library** is hidden behind `thadam-config`. No YAML type crosses the connector boundary (D22) and `thadam-core` has no YAML crate (ARCHITECTURE.md section 9). Replacing it is cheap.
- **The config syntax** (key names, nesting, value formats, the filter language) is user-facing. Once users write files it is effectively permanent, and the version axis policy below is the only way to change it.

### Scope

- The user-facing format (YAML with `version: 1`) and the YAML library.
- Interpolation: `${env:X}` and `${file:/path}`.
- Deserialization with `deny_unknown_fields`.
- Diagnostics that collect all errors, each with path, line and column, and a hint (`ConfigReport`).
- JSON Schema output for editors (`schemars`).
- Config hash stability. The hash is stored in `data_dir/thadam.json` and in Parquet footers (`thadam.config_hash`).
- `Secret<T>` redaction.
- The `serde_json::Value` boundary to connector factories (D22).
- No hot reload and no multiple pipelines per file.
- The version axis policy for config files.

### Constraints from other documents

- `serde_yaml` is deprecated and banned (ADR-0020, `deny.toml`). The plans name two maintained forks, `serde_norway` 0.9 and `serde_yaml_ng` 0.10 (implementation plan, section 5.2). The original plan rated the YAML choice low-medium confidence because the Rust YAML ecosystem is fragmented. No source compares the candidates.
- The workspace declares MSRV Rust 1.89 (raised from 1.88 by this decision, because `serde-saphyr` declares 1.89) and a license allow-list (ARCHITECTURE.md section 16, ADR-0020).
- Replay row-set equivalence is defined for the same WAL range and the same configuration hash (ARCHITECTURE.md section 7), and chains always use the configuration from their cursor. The hash must therefore be stable across runs and across Thadam versions that claim equivalence.
- Factories receive `serde_json::Value`, with typed structs inside each connector (D22, ARCHITECTURE.md section 5). Connector-specific validation runs through the registry after the generic pass, and diagnostics must still report paths such as `sinks[1].config.bucket`.
- Chains compile in `thadam-transform`. The `cel` crate, if ADR-0018 chooses it, lives only there, so expression errors surface through chain compilation rather than through `thadam-config`.

### Configuration scope that PR5 must carry

The implementation plan's config sketch was written before the demo. The demo pipeline YAML (demo plan, section 6) uses these options that the sketch lacked, and ARCHITECTURE.md section 15 puts them in PR5 scope:

- `required: true` on fields, and `convert_to` next to `unit`.
- `time.max_skew` and `timestamp_unit`.
- `explode: {path, index_field}` and the `^/` parent-path prefix in `from:`.
- Dataset-level `event_id: {key}` and `dedup: {window, max_keys}`.
- `validate` and `on_change` transforms.
- A top-level `quarantine` block.
- `compression: auto`, `max_open_files`, per-sink `max_disk`, `finalize_on_shutdown`.
- `local_retention: {mode, keep_for}` and `conditional_put: auto`.
- `runtime.memory_target`.

Also fixed: `on_full` is a required explicit key with no default (D4, decided by the owner on 2026-10-07, B3; validation fails if the key is missing), and `thadam init` writes `block` with a comment.

**Open item O2 [OPEN]:** how a transform binds to one sink (the P1 demo downsamples IMU for ClickHouse only). Proposed selector: `transforms[].sinks: [ids]`.

**Key spelling [OPEN, PR5 detail].** product.md section 14 uses `type`, `expression`, and `buffer`. The implementation and demo plans use `kind`, `expr`, and `wal`. The plans' spelling is the working assumption (ARCHITECTURE.md section 20 follows the demo for `event_id`, and `docs/design/product-amendments.md` Part A.6 uses it). Because this is user-facing, it is frozen with the first published fixture set in PR5, and the filter key is settled with ADR-0018 in PR9.

## Decision

Accepted by the owner on 2026-10-07. Items still marked **[OPEN]** or **[A]** below are PR5 implementation details (collected in "PR5 implementation details" at the end of this section), not undecided design questions.

### Format and structure

- **YAML**, with a mandatory top-level `version: 1`. Top-level keys: `version`, `pipeline`, `sources`, `wal`, `quarantine`, `datasets`, `transforms`, `sinks`, `runtime`, `observability`. The demo pipeline is the reference sketch.
- **Lifecycle:** load YAML, interpolate, deserialize with `deny_unknown_fields`, run connector-specific validation, check capabilities, compile into a plan. `thadam validate` stops after validation and prints every diagnostic plus the memory estimate.
- **Interpolation** applies to string values. The working assumption **[A]** is that it runs on parsed string scalars, not on raw text, so an environment value cannot change the document structure. Syntax for escaping a literal `${`, nesting, and the failure message for a missing variable or unreadable file are **[OPEN]**.
- **Diagnostics:** `ConfigReport { errors, warnings }`, each `Diagnostic { path, span: Option<LineCol>, message, hint }`. All problems are reported, not only the first. Maps to `ErrorDomain::Config` and exit code 2.
- **JSON Schema** is produced by `schemars` for editors (`json_schema()`). Connector config is opaque `serde_json::Value`, so whether factories contribute schemas is **[OPEN]**.
- **Secrets** arrive through `${env:}` or `${file:}` and are wrapped in `Secret<T>`, with redacted `Debug` and `Display` and an explicit `.expose()`.
- **Boundary type (D22):** `serde_json::Value`. Typed structs live inside each connector. No YAML type is exported.
- **Hot reload:** none. **Multiple pipelines per file:** none. A restart reloads the file.

### YAML library (decided by the owner, 2026-10-07)

Evidence: `docs/design/spikes/ADR-012-yaml.md` (crates compared on 2026-10-07: `serde_norway` 0.9.42, `serde_yaml_ng` 0.10.0, `serde-saphyr` 1.3.0).

- **Decided: `serde-saphyr` 1.x behind `thadam-config`, with `strict_booleans: true`.** Confidence medium. Settings fixed by this ADR and covered by PR5 tests: `strict_booleans: true` (otherwise a bare `no` is `false` in a connector `serde_json::Value`, which is the D22 boundary); duplicate keys left as an error (the default); `reject_unsupported_tags: true`; no `include`, `robotics` or `properties` features. The parse `Budget` is tightened; the spike's figures (about 10,000 nodes and depth 32) are a suggestion, not a measurement, and the final numbers are a PR5 detail.
- **MSRV: the workspace `rust-version` is raised from 1.88 to 1.89** with this decision (root `Cargo.toml`, ADR-0020), because `serde-saphyr` declares 1.89.
- **Documented fallback: `serde_norway`.** It behaves identically to `serde_yaml_ng` in every test and has the newer release (2024-12-21 against 2024-05-26), `MIT OR Apache-2.0`, MSRV 1.71.1. Using it needs three things the primary does not: a path-to-span index for errors raised by custom `Deserialize` impls, an explicit duplicate-key check for `serde_json::Value` (the forks keep the last key silently), and a documented rule against merge keys (the forks do not apply `<<` to typed structs).
- **What decided it, in the criteria order below:** maintenance (serde-saphyr released 2026-09-16 with 37 releases in a year; both forks have had no release for 9.5 and 16 months and are small deltas over the archived `serde_yaml`); diagnostics (exact positions for custom errors, duplicate keys and a caret snippet; the forks report the start of the enclosing mapping or of the first sequence item, up to seven lines off); input safety (duplicate keys rejected everywhere including `Value`, native merge keys, bounded budgets, `#![forbid(unsafe_code)]`, where the forks sit on c2rust-translated libyaml and take 13 s on a 100,000-deep flow sequence); toolchain fit favors the forks (MSRV 1.88 against 1.89, 6 against 16 extra crates); replaceability is equal (the demo parses to identical canonical JSON with all three).
- **Risks that keep confidence at medium:** `serde-saphyr` is young (first release 2025-09-27, 1.0.0 on 2026-07-31) and essentially one maintainer (769 of 845 commits); it needs Rust 1.89, which the owner accepted by raising `rust-version` from 1.88 to 1.89; and by default it reads YAML 1.1 boolean words for `bool` fields, accepts quoted numbers (`version: "1"`), and turns `014` and `1_000` into numbers at the `Value` boundary. Config documentation must say: quote identifiers.
- **Owner decisions the spike asked for, resolved 2026-10-07:** the 1.89 floor is accepted (MSRV raised, see above), and `serde-saphyr` is chosen with `serde_norway` as the fallback. The release-recency criterion the spike applied after seeing the dates ("a release within 12 months") is moot for the chosen crate (released 2026-09-16) and is not adopted as a policy. The key spelling and the hash scope remain open as PR5 details (below).
- **Fallback trigger:** switch to `serde_norway` if `serde-saphyr` loses maintenance, gets an unfixed advisory, or fails a longer-run stability check. The fallback carries the three extra requirements listed above (path-to-span index, explicit duplicate-key check for `Value`, documented rule against merge keys).

Selection criteria, in the order they were applied, with the result:

1. **Maintenance and advisories:** no RustSec advisory on any of the three or on their parsers (advisory-db `b0797f5`, 2026-10-07); serde-saphyr clearly the most active.
2. **Diagnostics:** `serde_path_to_error` works with all three. Spans: native and exact in serde-saphyr except inside internally tagged enums (item level); in the forks wrong for custom errors and tagged enums. A path-to-span index built from the `granit-parser` event stream (about 70 lines, in the spike) gives the right line for every case with any of the crates.
3. **Safety on untrusted-shaped input:** see above; unknown YAML tags are ignored by all three; alias expansion and recursion are bounded by all three.
4. **Toolchain fit:** see above; `cargo deny` with the repository policy passes for all three.
5. **Replaceability:** the spike's typed model is plain serde, and `thadam-config` uses only `from_str`-style entry points plus the index, so a swap is a local change protected by the fixture suite.

### Diagnostics and config-type guidance from the spike

- **Collecting all errors.** One serde pass stops at the first error in every candidate. The spike's approach, which reported all five errors of a five-error fixture with the right line and column from any of the three crates: parse once to `serde_json::Value`, validate each top-level section and each item of a list section separately with `serde_path_to_error`, and map the path to a position with the span index. The index should also flag duplicate keys, which a `Value` parse hides in the forks.
- **Internally tagged enums** (`#[serde(tag = "kind")]`) buffer their content, so no crate can give a path or position below the item (`transforms[1]`, not `transforms[1].heartbeat`). Dispatch `kind` by hand if key-level diagnostics are wanted.
- **Anchors and merge keys.** Plain anchors and aliases work in all three; `<<` merges only in serde-saphyr; `deny_unknown_fields` rejects a top-level `x-` section used only to hold anchors. Either document anchors as supported and name a section for reusable fragments, or reject aliases through the budget. Not decided here.

### Hash and version policy

- The config hash is computed from a canonical form of the typed spec, independent of key order and whitespace. What it covers is **[OPEN]**: a hash taken after interpolation would change when a secret rotates and would place secret-derived data in a digest. The hash scope, the canonicalization, and the hash algorithm must be frozen and versioned before the first release, because `thadam.config_hash` is recorded in Parquet footers.
- **Version axis policy.** A new binary reads older config versions where it promises to (N reads N-1). An unknown or newer version is refused with a clear error and exit code 2; the binary never guesses. `docs/design/version-inventory.md` lists the other version axes (WAL, cursor, catalog, object-key layout); this ADR owns only the config axis.

### PR5 implementation details

These items were left open by the spike report (`docs/design/spikes/ADR-012-yaml.md`, sections 5.10, 6, and 7) and by the ADR text. None blocks acceptance; PR5 (`thadam-config`) closes each one and records the outcome here or in `thadam-config` docs.

1. **Key spelling.** Confirm `kind`, `expr`/`where`, and `wal` (the plans' spelling) against `type`, `expression`, and `buffer` (product.md section 14), and freeze it with the first fixture set. The filter key itself is settled with ADR-0018 (PR9).
2. **Transform-to-sink binding (O2).** Confirm `transforms[].sinks: [ids]` or an alternative.
3. **Config hash.** Freeze the scope (before or after interpolation; a post-interpolation hash changes when a secret rotates and puts secret-derived data in a digest), the canonical form, and the algorithm, and version them, before the first release because `thadam.config_hash` is written to Parquet footers.
4. **Interpolation.** Escaping a literal `${`, nesting, and the messages for a missing variable or an unreadable file. Interpolation runs on parsed string scalars, not raw text **[A]**. The opt-in `properties` feature of `serde-saphyr` is not used (its syntax is not `${env:X}`).
5. **Parse `Budget` numbers.** Settle the node and depth limits (the spike's 10,000 nodes and depth 32 are a suggestion) and test them against the real demo configuration.
6. **Anchors and merge keys.** Either document them as supported (with a named section for reusable fragments, because `deny_unknown_fields` rejects a top-level `x-` key) or reject aliases with a zero-alias budget.
7. **Path-to-span index** (the `granit-parser` proof of concept in `spikes/adr012-yaml/src/spans.rs`) for locations `serde-saphyr` does not give natively (internally tagged enums, errors raised from `Value`-derived data) and for duplicate-key detection, plus the multi-error collection pass (parse once to `serde_json::Value`, validate each section and list item with `serde_path_to_error`).
8. **Internally tagged enums.** Dispatch `kind` by hand where key-level diagnostics are wanted, because no crate reports a path below the item for `#[serde(tag = "kind")]`.
9. **Scalar hazards in documentation.** Config documentation says "quote identifiers"; PR5 tests pin the `strict_booleans`, quoted-number, `014`, and `1_000` behavior at the `Value` boundary.
10. **The unticked checklist items below:** `insta` fixtures per invalid-config category (including unresolved interpolation and a bad `version`), JSON Schema snapshot, secret redaction tests, interpolation edge cases, version-policy tests, hash stability tests, and the syntax review of the demo file with one or two target users.

## Alternatives considered

- **TOML.** Common in Rust tooling and has no YAML coercion hazards. Thadam's configs are deeply nested lists of mappings (topics, fields, transforms), which TOML expresses with array-of-tables syntax that is harder to read at that nesting. The product requirement and every example already assume YAML. Not chosen, but a fallback if YAML crate evaluation fails.
- **JSON or JSON5.** Strict and unambiguous, but JSON has no comments, and comments are central to `thadam init` (which writes explanatory comments next to `on_full`). JSON5 adds comments but trades the ecosystem familiarity of YAML for another less common dependency.
- **A custom restricted YAML subset.** Removes coercion and alias hazards by construction, but means owning a parser and an editor-support story. The cost is ongoing; consider only if every maintained crate fails the criteria.
- **YAML through a deprecated crate (`serde_yaml`).** Rejected: banned by ADR-0020 and the PR1 `cargo deny` configuration.
- **A typed `ConfigValue` exposing the YAML crate to connectors.** Rejected (D22): it couples every connector, and later plugin authors, to the YAML library.
- **An erased deserializer at the connector boundary.** Considered under D22. Rejected: it adds machinery and keeps connector code coupled to the deserialization layer, while `serde_json::Value` is a plain, well-known type.
- **Multiple pipelines per file.** Rejected for the MVP (one pipeline per process, ADR-0003).
- **Hot reload.** Rejected for the MVP; restart reloads. It would need a story for cursors, chain state, and in-flight batches.

## Consequences

- Choosing YAML keeps the demo, product examples, and user expectations aligned, and places the main risk on the library, which is replaceable.
- The syntax becomes the compatibility surface. Each published key name needs a test, and PR5 carries the full demo option list listed above so the first published syntax is not immediately extended.
- Going through `serde_json::Value` loses YAML source spans unless `thadam-config` keeps a separate path-to-span index. Span quality must be demonstrated by the evaluation.
- Collecting all errors needs a deliberate strategy, since a single deserialization pass typically stops at the first error **[A]**. For example, deserializing each top-level section separately from an intermediate tree.
- `deny_unknown_fields` and `#[non_exhaustive]` on config structs mean adding a key is a new minor release, never a silent accept. Known serde limitations when combining `deny_unknown_fields` with `flatten` are to be avoided in config types **[V]**.
- Expression-language errors (ADR-0018) are produced when a chain compiles in `thadam-transform`, so `thadam validate` must run that compile step to report them with spans.
- Updated earlier decisions: ARCHITECTURE.md section 15 and D22 are the sources of record. The original plan stored the config hash in `data_dir/pipeline.json`; ARCHITECTURE.md section 11 uses `data_dir/thadam.json`, and this ADR follows it. The original plan's "typed `ConfigValue`" is superseded by `serde_json::Value`. The implementation plan's config sketch is superseded, for option coverage, by the list in "Configuration scope that PR5 must carry".

## Evidence and validation required

Evaluated 2026-10-07 in `docs/design/spikes/ADR-012-yaml.md` (code and raw output in `spikes/adr012-yaml/`). Items marked `[x]` have the evidence described; `[ ]` items were not done and are PR5 implementation details or acceptance tests (see "PR5 implementation details"), not blockers. The YAML-crate decision was made by the owner on 2026-10-07.

- [x] **Maintenance status and RustSec advisory check**, 2026-10-07. No advisory for `serde_norway`, `serde_yaml_ng`, `serde-saphyr`, `granit-parser`, `unsafe-libyaml-norway`; `unsafe-libyaml` has RUSTSEC-2023-0075 (fixed in 0.2.10; `serde_yaml_ng` resolves 0.2.11). Last releases: `serde_norway` 2024-12-21, `serde_yaml_ng` 2024-05-26, `serde-saphyr` 2026-09-16. [OPEN: the minimum release-recency criterion was not set before the evaluation; the spike used "a release within 12 months" afterwards.]
- [x] **Line and column spans.** `serde_path_to_error` works with all three. Locations are right for unknown fields, scalar type errors and syntax errors in all three; for custom-`Deserialize` errors and errors inside internally tagged enums the forks are wrong (start of the enclosing mapping or of the first sequence item) and `serde-saphyr` is exact or item-level. A `granit-parser` path-to-span index maps any `serde_path_to_error` path to the key or item position, for scalar, mapping and sequence nodes (aliases and merge keys not expanded).
- [ ] **Error quality** for each invalid-config category, snapshot-tested with `insta`. Done by hand, not snapshot-tested: unknown key, wrong type, missing required key, bad enum, bad duration, size and count, duplicate key, syntax errors, stray `---`, empty file. Not done: unresolved interpolation, bad `version`.
- [x] **Duplicate keys.** Forks: error for structs, **silent last-wins** for maps and `serde_json::Value`, no option. `serde-saphyr`: error by default everywhere, `DuplicateKeyPolicy` option.
- [x] **Unknown fields** under `deny_unknown_fields`, including nested structs and an internally tagged enum: rejected by all three, with the allowed names listed. Position inside the tagged enum is item level at best.
- [x] **Anchors and aliases.** Supported by all three; undefined anchors error with position; expansion bounded (forks: `repetition limit exceeded`; `serde-saphyr`: node budget 250,000 by default). Merge keys: only `serde-saphyr`. Deep nesting: forks quadratic (13 s at 100,000 levels), `serde-saphyr` 0.1 ms.
- [x] **YAML 1.1 versus 1.2 scalar coercion.** Tested `no`, `yes`, `on`, `off`, `y`, `n`, `014`, `0o14`, `0x1F`, `1_000`, `1e3`, `12:30:45`, dates, `.inf`, `null`, `~` against `String`, `bool`, `i64`, `f64`, `Option<String>` and `serde_json::Value`. Forks: bool words are strings or errors, no Norway problem. `serde-saphyr` default: `no` is `false` for `bool` fields and in a `Value` unless `strict_booleans: true`; it also accepts quoted numbers and reads `014` and `1_000` as numbers in a `Value`.
- [x] **Duration and size scalars.** `50ms`, `5m`, `1h`, `1d`, `128MiB` and `500k` parse identically with all three through `humantime` 2.4.0, `bytesize` 2.7.0 and a count parser. `session_expiry: 1d` is inside a connector `Value` block, so the demo file does not duration-parse it.
- [x] **Round trip of the demo pipeline** into a typed spec with no diagnostics and identical canonical JSON from all three crates; a copy with one key misspelled is rejected with position and path. The buggy-filter and fixed-filter variants were not run separately (they differ only in a string value).
- [x] **Multi-error collection.** A five-error fixture reports one error in a single pass and all five, each with path and line and column, with the Value-based per-section approach (see Decision).
- [x] **MSRV, license and dependency weight.** `serde_norway` and `serde_yaml_ng` build on Rust 1.88.0; `serde-saphyr` 1.3.0 declares 1.89 (it compiled on 1.88.0 with `--ignore-rust-version`), which is why the workspace MSRV is now 1.89. All licenses pass `deny.toml`. Dependency weight on aarch64-apple-darwin: 6, 6 and 16 crates beyond `serde`; clean release builds 4.2 s, 3.5 s and 6.3 s (one run). Linux aarch64 not measured.
- [ ] **Stable config hash:** two files differing only in key order, whitespace, and comments hash identically; a one-value change hashes differently; hash scope with respect to interpolation is decided. The only evidence is that the demo's typed spec serializes to identical canonical JSON from all three crates. [OPEN: hash scope and algorithm]
- [ ] **JSON Schema snapshot test** and a check that the schema rejects the same unknown keys as `deny_unknown_fields`.
- [ ] **Fixture directory** with one fixture per invalid-config category and `insta`-snapshotted diagnostics (implementation plan, section 5.2).
- [ ] **Secret redaction tests:** `Debug`, `Display`, `tracing` output, and `ConfigReport` text never contain a secret value, including when the secret came from `${file:}`. (`serde-saphyr` has an opt-in `properties` feature with its own `${NAME}` syntax and redaction; not evaluated, and its syntax is not `${env:X}`.)
- [ ] **Interpolation edge cases:** missing variable, unreadable file, nesting, escaping, and a value containing YAML syntax characters.
- [ ] **Version policy tests:** N reads N-1 where promised; N-1 refuses N (architecture review, section 9).
- [ ] **Syntax review** by the owner and one or two target users of the demo config, including key spelling (`kind` or `type`, `expr` or `expression`) and the transform-to-sink binding (O2). A PR5 detail.
- [x] **Owner decision on the YAML crate and the 1.89 floor**, recorded 2026-10-07 in `docs/design/owner-decisions.md` (B7).

## Revisit when

- The chosen YAML crate loses maintenance or gets an advisory.
- Diagnostics quality proves inadequate in PR5 fixtures or in user reports.
- User feedback shows the syntax needs a breaking change (which triggers a new config `version`).
- Plugin connectors (ADR-0017, Reserved) need typed config beyond `serde_json::Value`.
- Hot reload or multiple pipelines per process becomes a requirement.

## References

- ARCHITECTURE.md sections 3 (D4, D22), 5 (configuration boundary), 7 (determinism), 9 (crate rules), 11 (`thadam.json`), 15 (configuration), 16 (bans), 21 (PR5), 22 (O2).
- ADR-0003 (execution model), ADR-0004 (durable buffer, `on_full`), ADR-0011 (delivery guarantees), ADR-0013 (Parquet layout, footer metadata), ADR-0017 (plugin mechanism, Reserved), ADR-0018 (filter language), ADR-0020 (dependency policy).
- product.md sections 14 and 31.
- `docs/design/version-inventory.md` (if present) and `docs/design/owner-decisions.md` (B3).
- Implementation plan, section 5.2. Architecture review, section 9 and D22. Original plan, section H. Demo plan, section 6.
- Upstream: the `serde_norway`, `serde_yaml_ng`, `serde_path_to_error`, `schemars`, `humantime`, and `bytesize` crate documentation.
