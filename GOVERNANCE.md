# Governance

Thadam is maintained by a single maintainer, [@Dineshkarthik](https://github.com/Dineshkarthik). The maintainer makes the final decisions on the project: which changes are merged, which designs are accepted, and how the project is run.

## How decisions are made

- Contributions are reviewed and merged by the maintainer. Discussion happens in GitHub issues and pull requests.
- Significant design decisions, including changes to versioned surfaces (the WAL format, connector contracts, and configuration syntax), are recorded as ADRs before major code lands. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Architecture decision records

ADRs live in `docs/design/adr/` and follow the template in [ADR-0000](docs/design/adr/0000-adr-process.md).

1. Anyone may propose an ADR by opening a pull request that adds it with status Draft.
2. The author moves the ADR to Proposed when it is complete and recommended.
3. Only the maintainer accepts an ADR. The maintainer sets the status to Accepted and records that approval in the pull request. Contributors do not set Accepted.
4. An accepted ADR is not rewritten. A changed decision is recorded in a new ADR that supersedes it.

Status lifecycle: Draft, Proposed, Accepted, Superseded by ADR-XXXX.

## Security and conduct

- Report security vulnerabilities privately through GitHub, as described in [SECURITY.md](SECURITY.md).
- Report conduct concerns to the maintainer on GitHub, as described in the [Code of Conduct](CODE_OF_CONDUCT.md).

## Changes to this document

This model fits a single maintainer. If the project gets more maintainers, this document will be updated to describe how decisions are shared.
