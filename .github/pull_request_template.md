## What and why

<!-- Link the issue or ADR this implements. One PR, one focus; large changes should be split along PR-sequence lines (ARCHITECTURE.md section 21). -->

## Changes

<!-- Bullet the behavior changes. Note any change to versioned surfaces: WAL format, cursor files, thadam.json/wal.json, config keys/schema, metric names, error codes, connector contracts. -->

## User-facing impact

- [ ] No user-facing change
- [ ] User-facing change — documentation updated in `website/docs/src/` as needed
- [ ] New or changed public claim — it is consistent with the ADR-0011 claims register (or the register row is added/marked pending evidence)

## Checks

<!-- CI runs these; confirm you ran them locally. -->

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets --locked -- -D warnings`
- [ ] `cargo nextest run --workspace --locked`
- [ ] `scripts/check-deps.sh` (and `--self-test` if the guard itself changed)
- [ ] `cargo deny --locked check`
- [ ] Tests added/updated for the new behavior (unit / integration / failure as applicable)
- [ ] Site build and check (if the site changed): `website/scripts/build.sh` and `python3 website/scripts/check.py`

## Notes for reviewers

<!-- Evidence for anything measurement-sensitive (spike data, harness output); risk areas; suggested follow-ups. -->
