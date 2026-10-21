# ADR-0016: Naming and publishing

- **Status:** Accepted
- **Date:** 2026-10-14 (accepted 2026-10-20)
- **Deciders:** Maintainer (@Dineshkarthik)

## Context

"Thadam" (`thadam`, Tamil: தடம் — track, trail, trace, path) is the selected project identity. The name is used for the `thadam` binary, the `thadam-*` workspace crates, the Python package (`thadam` in `sdk/python/pyproject.toml`), the repository, and the brand assets (`thadam-assets/`).

When this ADR was drafted, nothing had been published: every workspace crate was `publish = false`, and no package was on crates.io or PyPI.

## Decision

- "Thadam" (`thadam`) is the name of the project. The registry and GitHub checks below found no conflict. The trademark search was not done; the maintainer accepted that risk on 2026-10-20.
- The CLI package is named `thadam` (directory `apps/thadam-cli`), so it installs with `cargo install thadam`.
- Published to crates.io: the `thadam` CLI and the 22 library crates under `crates/` it depends on, all except `thadam-testkit`. `thadam-testkit`, `thadam-simulator`, `thadam-bench`, and `thadam-python` stay `publish = false`.
- Published to PyPI: the `thadam` Python package built from `sdk/python`.
- Publishing runs from `.github/workflows/release.yml` on a version tag, through trusted publishing with no stored token, after approval in the `crates-io` and `pypi` GitHub environments. All crates and the Python package share one version.
- Brand assets in `thadam-assets/` are covered by the repository's Apache-2.0 license. The name and logos are governed by the trademark-use policy in `thadam-assets/README.md`: use them only to refer to Thadam, and never in a way that implies endorsement.

## Alternatives considered

- **Wait for a trademark search before publishing.** Not chosen (2026-10-20): the registry and GitHub checks found no conflict, and the maintainer accepted the remaining risk.
- **Keep the CLI package name `thadam-cli`.** Not chosen: `cargo install thadam` matches the binary and the Python package name.
- **Publish `thadam-testkit` for third-party connector authors.** Not chosen for now: it is only a dev-dependency of the workspace crates, and publishing it would make its API a public surface.

## Consequences

- The name stays in code, docs, and the brand kit.
- The first publish reserves 23 crate names and one PyPI name for good: a release can be yanked but a name cannot be given back.
- The trademark-use policy applies to the name and logos.

## Evidence and validation required

The checks the maintainer set before acceptance:

1. **crates.io:** availability of the workspace crate names (the `thadam-*` crates and the `thadam` binary crate).
2. **PyPI:** availability of the package name `thadam` used by `sdk/python/pyproject.toml`.
3. **GitHub:** existing projects, organizations, or repositories that use "Thadam" in the same space.
4. **Trademark:** a search of relevant trademark databases for "Thadam" in software and data-infrastructure classes.

If any check finds a conflict, this ADR is reopened and the name decision is revisited before anything is published.

### Results recorded so far

Checked 2026-10-20 against the public registry APIs. Availability can change until a name is reserved by publishing.

- [x] **crates.io:** `thadam` and all 27 workspace package names (every `thadam-*` crate, including `thadam-cli` and `thadam-python`) return 404 from `https://crates.io/api/v1/crates/<name>`: none is taken.
- [x] **PyPI:** `thadam` and `thadam-python` return 404 from `https://pypi.org/pypi/<name>/json`: neither is taken.
- [x] **GitHub:** a repository search for "thadam" returns 86 results, all unrelated (travel, carbon tracking, biomarker, and mobile apps, or personal repositories). Searches for "thadam" with "telemetry", "robotics", or "mqtt" return none; with "data", three unrelated personal repositories.
- [ ] **Trademark:** not searched. The maintainer accepted this ADR without it on 2026-10-20; a conflict found later reopens the name decision.

## Revisit when

- Any check in the list above, including a later trademark search, finds a conflicting name or registration.

## References

- [ADR-0000: ADR process](0000-adr-process.md)
- `thadam-assets/README.md` (naming / legal note and trademark-use policy)
- `thadam-assets/brand/brand-guidelines.md` (naming and legal note)
- `sdk/python/pyproject.toml` (Python package name)
- `ARCHITECTURE.md` (cites ADR-0016 for the publishing rules)
