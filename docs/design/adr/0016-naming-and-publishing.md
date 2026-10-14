# ADR-0016: Naming and publishing

- **Status:** Draft
- **Date:** 2026-10-14
- **Deciders:** Maintainer (@Dineshkarthik)

## Context

"Thadam" (`thadam`, Tamil: தடம் — track, trail, trace, path) is the selected project identity, but its availability has not been formally recorded. The name is used for the `thadam` binary, the `thadam-*` workspace crates, the Python package (`thadam` in `sdk/python/pyproject.toml`), the repository, and the brand assets (`thadam-assets/`). `thadam-assets/README.md` and `thadam-assets/brand/brand-guidelines.md` both state that final name and trademark clearance must be completed before public launch.

Nothing has been published. Every workspace crate is `publish = false`, and no package is on crates.io or PyPI.

## Decision

- "Thadam" (`thadam`) is the selected name of the project. This ADR stays Draft until final trademark and domain clearance is recorded.
- Until this ADR is Accepted, every workspace crate stays `publish = false` and no package is published to crates.io or PyPI.
- Brand assets in `thadam-assets/` are covered by the repository's Apache-2.0 license. The name and logos are governed by the trademark-use policy in `thadam-assets/README.md`: use them only to refer to Thadam, and never in a way that implies endorsement.

## Alternatives considered

- **Publish under the current name now.** Not chosen. A published crate or package name is hard to take back, and the clearance checklist below must be recorded first.
- **Rename the project before clearance.** Not chosen. The name stays provisional until the checks are done. If a check fails, a rename becomes a new decision under a new ADR.

## Consequences

- The name can stay in code, docs, and the brand kit while the checks run, and nothing has to change before the checks are done.
- Public launch and the first publish both wait for this ADR to be Accepted.
- Fixed now: the workspace stays `publish = false`, and the trademark-use policy applies to the name and logos.

## Evidence and validation required

The maintainer must make these checks and record the result before this ADR can be Accepted:

1. **crates.io:** availability of the workspace crate names (the `thadam-*` crates and the `thadam` binary crate).
2. **PyPI:** availability of the package name `thadam` used by `sdk/python/pyproject.toml`.
3. **GitHub:** existing projects, organizations, or repositories that use "Thadam" in the same space.
4. **Trademark:** a search of relevant trademark databases for "Thadam" in software and data-infrastructure classes.

If any check finds a conflict, this ADR is reopened and the name decision is revisited before anything is published.

## Revisit when

- Any check in the list above finds a conflicting name or registration.
- Before the first crate or Python package is published.
- Before public launch.

## References

- [ADR-0000: ADR process](0000-adr-process.md)
- `thadam-assets/README.md` (naming / legal note and trademark-use policy)
- `thadam-assets/brand/brand-guidelines.md` (naming and legal note)
- `sdk/python/pyproject.toml` (Python package name)
- `ARCHITECTURE.md` (cites ADR-0016 for the publishing rules)
