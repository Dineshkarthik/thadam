# Version inventory

Status: Proposed. The axes come from the architecture review (section 9) and the implementation plan. The compatibility rules are tied to the ADRs named in each row, and each becomes binding when that ADR is accepted. As of 2026-10-07 ADR-0001, 0002, 0012, 0013, and 0020 are Accepted; the WAL and cursor rows depend on ADR-0004, which stays Draft until the S1 re-run on reference hardware.

Date: 2026-10-07.

## Why this document exists

Thadam has many independently versioned artifacts. Without one list, each gets an implicit and inconsistent compatibility policy. Each axis below has one owner, one place where its version is recorded, one compatibility rule, and one kind of test.

## General rules

- A newer binary reads every released version of an on-disk format it supports. An older binary refuses a newer version with a clear error and never guesses. "N reads N-1, and N-1 refuses N" is the baseline, and each axis states where it promises more.
- Before the first release there is no on-disk compatibility promise. The capture WAL format freezes when ADR-0004 is accepted and the format is released **[A]**.
- Every axis has fixture files for each released version, checked in and exercised by tests.
- The capture-first design keeps the canonical in-memory model off the on-disk axes. This removes the largest source of migrations.

## Axes

| Axis | Owner | Where the version lives | Compatibility rule (proposed) | Test |
|---|---|---|---|---|
| Configuration schema | `thadam-config`, ADR-0012 | `version:` key in the YAML file | Newer binary accepts older schema versions it still supports; unknown or newer version is refused with exit code 2 | Fixture per version; JSON Schema snapshot |
| Data directory metadata | `thadam-runtime` (owns `thadam.json`), `thadam-wal` (owns `wal.json`) | `data_dir/thadam.json` (owned by the runtime; contents as in `ARCHITECTURE.md` section 11, to be settled with the runtime in PR10) and `data_dir/wal/wal.json` (WAL `kind`, format versions, `epoch`, `node_id`, and `machine_id` binding; see ADR-0004, "Implementation notes") | Any mismatch is refused with exit code 3 unless `--repair` is passed; the binding to `machine_id` detects a copied directory | Open a directory with each mismatch type |
| WAL segment and frame format | `thadam-wal`, ADR-0004 | Segment header `format_version`; frame `version` byte; `wal_format_version` and `frame_format_version` in `wal.json` | **Versioned but not frozen** while ADR-0004 is Draft (frozen at first release after the hardware re-run); a binary refuses a version it does not know; never rewritten in place | Golden fixtures; torn-tail and corruption tests |
| Cursor files | `thadam-wal`, ADR-0004 | Cursor format version in each file (and `cursor_format_version` in `wal.json`); epoch in each file | Same as WAL; a cursor with a different epoch is refused; a cursor beyond the WAL end is skipped past and reported (ADR-0004, "Implementation notes") | Epoch-mismatch rejection test |
| Segment catalog log | `thadam-segments`, ADR-0013 | `thadam.json` format version | Same as WAL; the catalog can be rebuilt from Parquet footers, so repair is possible | Catalog rebuild from footers |
| Canonical in-memory model | `thadam-core`, ADR-0001 | Crate version only (nothing persisted) | No persistence compatibility; the Rust API is 0.x and unstable until the first release | None needed on disk |
| Parquet footer metadata | `thadam-segments`, ADR-0013 | Keys `thadam.version`, `thadam.dataset`, `thadam.schema_fp`, `thadam.wal_range`, `thadam.config_hash`, `thadam.enrichment_hash`, `thadam.clock_synced` | Additive only; readers ignore unknown keys; a changed meaning needs a new key | Read files from each release |
| Dataset output schema and Parquet field IDs | `thadam-arrow`, ADR-0013 | `thadam.schema_fp` plus dataset version (`dataset@v2`) | Iceberg-compatible evolution only: add optional columns, widen `int` to `long` and `float` to `double`, never rename or narrow; anything else is a new dataset version; field IDs are never reused | Evolution-rule enforcement; multi-file reads in DuckDB and pyarrow across additive changes |
| Object key layout | `thadam-object-store`, ADR-0013 | Path segment `thadam/v1/` | `v1` keys are never rewritten; an incompatible layout uses a `v2` prefix and both can coexist | Key-derivation tests |
| Quarantine schema | `thadam-segments`, ADR-0013 | Parquet footer metadata | Additive only | Schema snapshot |
| Connector configuration | each connector crate | `kind` plus the `config:` block, validated by the connector factory | Follows the configuration schema rules; new keys optional; removing a key needs a deprecation period **[A]** | Fixtures per connector |
| Connector traits (`Source`, `BatchSink`, `SegmentSink`) | `thadam-core`, ADR-0002 | Crate version; no `contract_version` | Unstable (0.x) until the first release; a versioned contract is deferred to the plugin ADR (ADR-0017, not yet written) | Conformance suites |
| Error codes | `thadam-core` | Stable `&'static str` constants | A code is never reused for a different meaning; new codes may be added | Snapshot of all codes |
| Quality flags | `thadam-core`, ADR-0001 | Bit positions in `_quality_flags` | A bit is never reassigned | Snapshot of the registry |
| Event ID derivation | `thadam-transform`, ADR-0001 | Documented algorithm (`xxh3_128` of epoch, frame sequence, record index; or the configured content key) | A change breaks deduplication and replay equivalence, so it needs an algorithm identifier in lineage **[A]** | Determinism and replay tests |
| Metric names and `/status` JSON | `thadam-runtime` | Names in `ARCHITECTURE.md` section 14; JSON schema snapshot | Additive within a release line; renames need a deprecation period **[A]** | Metric-name and `/status` snapshots |
| CLI commands and exit codes | `apps/thadam-cli` | `thadam --help` snapshot; exit-code table | Exit codes 0, 1, 2, 3, 130 are stable; commands may be added | Exit-code matrix; help snapshot |
| Simulator oracle and benchmark result JSON | `tools/` | A schema version field in the file | Informational; consumers check the version | Result-schema test |
| Toolchain and MSRV | workspace, ADR-0020 | `rust-version` (1.89, raised from 1.88 on 2026-10-07 for `serde-saphyr`, ADR-0012) and `rust-toolchain.toml` | Raise only for a dependency requirement, never below the highest dependency MSRV | CI on pinned and MSRV toolchains **[V]** (the MSRV job is not in `ci.yml` yet) |

## Ownership and change process

- Changing any row's compatibility rule requires an ADR (see ADR-0000). Adding a new on-disk artifact requires a new row in the same pull request.
- The claims register (ADR-0011) and the compatibility matrix (Linux versions, architectures, brokers, S3-compatible stores, ROS 2 distributions, ClickHouse versions) are separate documents; this file covers formats Thadam itself writes or reads.

## Open points

- The WAL header size question (stated as 24 bytes with fields summing to 20) is resolved by spike S1 and recorded in ADR-0004: a 24-byte frame header including a reserved word, a 64-byte segment header, and an all-zero 24-byte end-of-data marker. The layout is frozen only when ADR-0004 is accepted, which needs the S1 re-run on x86 NVMe and ARM eMMC.
- The Parquet field-ID assignment policy is closed in ADR-0013 (PR13): envelope IDs 1 to 99 fixed by `thadam-arrow`, identity level IDs 100 to 199 and declared top-level IDs 1000 to 4,194,303 explicit in configuration, nested IDs derived, IDs never reused after removal. What remains open is the configuration key that carries the explicit IDs and how "never reused" is enforced across edits to the configuration.
- The `_wal_record_idx` lineage column is closed as not written (ADR-0013, PR13); field ID 13 is reserved, so adding it later is additive.
