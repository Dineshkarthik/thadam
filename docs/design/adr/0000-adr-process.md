# ADR-0000: ADR process

- **Status:** Proposed
- **Date:** 2026-10-07
- **Deciders:** Project owner

## Context

Thadam's architecture decisions are expensive to reverse: the WAL on-disk format, the connector contracts, and the user-facing configuration syntax become permanent once released. Decisions need a durable, reviewable record in the repository, with explicit evidence requirements and an explicit owner sign-off, so implementers never build against a stale or unaccepted design.

## Decision

Significant architecture decisions are recorded as Architecture Decision Records (ADRs) in `docs/design/adr/`.

### File naming

`docs/design/adr/NNNN-kebab-title.md`, where `NNNN` is a zero-padded sequence number assigned in order and never reused. The heading is `# ADR-NNNN: Title`.

### Status lifecycle

| Status | Meaning |
|---|---|
| Draft | Being written. Options and evidence requirements are listed; no recommendation is binding. Nothing may be built on it. |
| Proposed | Complete and recommended, awaiting the project owner's decision. |
| Accepted | Approved by the project owner. Implementation may rely on it. |
| Superseded by ADR-XXXX | Replaced by a later ADR. Kept in place, with only its Status line changed. |

Transitions: `Draft -> Proposed -> Accepted -> Superseded by ADR-XXXX`. A Proposed ADR that is rejected is edited back to Draft, or superseded by the ADR that replaces it.

**Only the project owner accepts an ADR.** Authors, reviewers, and coding agents may draft and propose; none may set the status to Accepted. A pull request that changes a status to Accepted must record the owner's approval.

### Rules

- An accepted ADR is not rewritten. Change a decision by writing a new ADR that supersedes it.
- Decisions that depend on measurements list the required spike or benchmark under "Evidence and validation required", and stay Draft or Proposed until that evidence is attached.
- Every ADR states when it should be reopened under "Revisit when".

### Template

Copy this block into `docs/design/adr/NNNN-kebab-title.md`. Keep the sections exactly as listed and in this order.

```markdown
# ADR-NNNN: Title

- **Status:** Draft | Proposed | Accepted | Superseded by ADR-XXXX
- **Date:** YYYY-MM-DD
- **Deciders:** names or roles

## Context

The problem, the forces at play, and the constraints.

## Decision

What is decided, stated in the active voice.

## Alternatives considered

Each option and why it was not chosen.

## Consequences

What becomes easier, what becomes harder, and what is now fixed.

## Evidence and validation required

Spikes, benchmarks, and tests that must pass before this ADR can be accepted or relied on.

## Revisit when

The conditions that should reopen this decision.

## References

Links to related ADRs, product requirements, spike reports, and external sources.
```

## Alternatives considered

- **Decisions kept in planning documents outside the repository.** Rejected: later plans partly supersede earlier ones and there is no single accepted baseline.
- **A lighter-weight decision log without status or evidence fields.** Rejected: it cannot distinguish an accepted decision from an unreviewed recommendation.

## Consequences

- Every irreversible decision has a stable identifier, an owner sign-off, and an evidence trail.
- Authors must keep new ADRs on this template so tooling and reviewers can rely on the section layout.
- Acceptance is a deliberate gate that waits on the project owner.

## Evidence and validation required

None. This ADR defines the process itself.

## Revisit when

- The team grows beyond a single decision-making owner.
- ADR volume makes a flat directory unwieldy and an index becomes necessary.

## References

- Michael Nygard, "Documenting Architecture Decisions" (2011).
