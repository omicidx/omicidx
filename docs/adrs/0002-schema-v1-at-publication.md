# 0002 — OmicIDX schema is v1.0 at publication, not stable-forever

- **Status:** Accepted
- **Date:** 2026-05-29
- **Deciders:** Sean Davis

## Context

The OmicIDX paper has been gated for months on an implicit "lock the schema before publishing" criterion. The schema-stability gate is a strawman:

- The upstream sources (NCBI SRA, GEO, BioSample, BioProject, PubMed, EBI BioSample) evolve continuously. The OmicIDX schema must respond to that evolution; freezing it forever guarantees obsolescence.
- No widely-cited published bioinformatics data system holds a truly stable schema across years. Ensembl, UCSC, RefSeq, ENA, and similar all version their schemas explicitly and ship updates on a release cadence.
- The reader's actual need is a *describable* schema at the moment of publication — not a permanent freeze.

The gate has been blocking the deliverable; the deliverable would benefit far more readers than a deferred "perfect" schema ever will.

## Decision

**The OmicIDX schema is declared v1.0 at the moment of publication.** The paper describes the v1.0 schema as it exists at submission. Subsequent schema changes are versioned, not blocked.

The mechanics of how subsequent versions evolve are specified separately in [ADR-0003](./0003-schema-versioning-policy.md).

## Consequences

**Positive:**
- Unblocks the paper. The schema-stability gate was load-bearing; declaring v1.0 dissolves it.
- Sets up a clean evolution story: v1.0 in print → versioned changes visible in commit history → future major versions only if and when breaking changes are justified.
- Honest about the underlying constraint: upstream sources evolve, so the index does too.

**Negative:**
- Future readers of the paper may need v1.0-specific docs after v2 ships. Mitigation: see ADR-0003 (snapshotting).
- Each PR that touches schema must declare the version bump explicitly. Mitigation: PR template requirement (see ADR-0003).

## References

- Ensembl release archive: <https://www.ensembl.org/info/website/archives/index.html>
- ENA documentation (versioned): <https://ena-docs.readthedocs.io/>
