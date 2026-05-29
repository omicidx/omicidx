# 0003 — OmicIDX schema versioning policy

- **Status:** Accepted
- **Date:** 2026-05-29
- **Deciders:** Sean Davis
- **Depends on:** [ADR-0002](./0002-schema-v1-at-publication.md)

## Context

ADR-0002 declares the OmicIDX schema v1.0 at publication. That declaration is only credible if there is a published rule for how subsequent versions evolve. Without one, "v1.0-at-publication" reduces to "we'll figure it out later" — which is the same gate ADR-0002 was meant to dissolve.

This ADR specifies the rule.

## Decision

### Versioning policy (semver-adjacent)

OmicIDX schema versions follow a semver-adjacent pattern:

- **Major (`vN.0`)** — Breaking changes:
  - Removed columns or tables
  - Type changes on existing columns
  - Renamed accession-bearing fields
  - Changes to canonical-form rules (how an accession is normalized)
- **Minor (`v1.x`)** — Additive changes:
  - New columns on existing tables
  - New derived fields
  - New tables
  - New sources added to the index
- **Patch (`v1.0.x`)** — Bug fixes that do not change schema shape or semantics:
  - Corrected null-handling
  - Fixed encoding (UTF-8 sanitization, etc.)
  - Documentation corrections

Each release exposes a `schema_version` value at the dataset level so consumers can pin or branch.

### Snapshotting

A frozen `schema/v1.0/` directory in the OmicIDX repo holds the schema definition exactly as described in the published paper. Future major versions add `schema/v2.0/`, etc., without rewriting prior snapshots. This preserves the citability of the paper across the system's lifetime: a reader of the v1.0 paper can always find the v1.0 schema unchanged.

### Discipline

Each PR that touches schema must declare the version bump explicitly in its description. Captureable as a PR template field or a CODEOWNERS check.

## Consequences

**Positive:**
- Removes the "later" handwave from ADR-0002. The policy is now committed, not pending.
- Provides a clear rule for contributors and reviewers — "is this change major, minor, or patch?" is the only question to answer.
- Snapshot directory preserves the paper's reproducibility long after subsequent versions ship.

**Negative:**
- Adds a small per-PR discipline (declare the bump). Worth the cost.
- "Semver-adjacent" rather than strict semver — minor versions can add behavior that downstream queries must account for. Mitigation: clear changelog discipline.

## References

- [ADR-0002 — OmicIDX schema is v1.0 at publication](./0002-schema-v1-at-publication.md)
- Semantic Versioning: <https://semver.org/>
