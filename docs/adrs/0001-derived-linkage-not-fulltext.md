# 0001 — Serve derived publication↔accession linkage, not full text

- **Status:** Accepted
- **Date:** 2026-05-29
- **Deciders:** Sean Davis

## Context

OmicIDX indexes accession metadata across SRA, GEO, BioSample, BioProject, PubMed, and EBI BioSample. A natural extension is connecting accessions back to publications — answering "which papers use dataset X, and how?"

Empirical observation: accession mentions cluster in publication full text (Methods, Data Availability sections), **not in abstracts**. Abstract-only mining is a strict lower bound on the dataset↔publication graph.

Three implementation paths exist for full-text linkage:

1. **Bundle and redistribute PMC OA full text alongside OmicIDX.** Inherits a per-article license matrix (CC-BY / CC-BY-NC / CC-BY-ND / CC0), 10–100× storage/egress vs. current metadata-only operation, and a major ETL + legal review workstream. Delays publication by months.
2. **Mine third-party-served full text and serve only derived links.** Europe PMC's SciLite annotation API already mines OA for GEO + BioProject mentions, but **not SRA or BioSample** (verified 2026-05-28). Combine SciLite with OmicIDX's own regex over **NCBI BioC sectioned passages** (already ingested) for SRA + BioSample. Merge into a single canonical accession↔PMID linkage table.
3. **Defer full-text mining entirely.** Ship the paper on abstract-only / metadata-only links; full text becomes future work.

## Decision

Adopt path (2): **OmicIDX serves a derived `publication↔accession` linkage table mined from PMC OA full text; OmicIDX does not redistribute whole articles.**

Mining sources:
- Europe PMC SciLite annotations for **GEO + BioProject**
- OmicIDX regex over NCBI BioC sectioned passages for **SRA + BioSample**
- Merge into a single canonical linkage table

Served unit per linkage row:
- Canonical accession (OmicIDX form)
- PMID + PMCID
- **Section** (from BioC: Title / Abstract / Methods / Data Availability / Results / etc.)
- Short **context window** — prefix / exact / postfix, matching the public shape of EPMC annotations
- Derived **mention-type class** (deposition / reuse / comparison / passing) — captures *how* the data are used, not just *that* they were
- Per-article source license tag (carried from PMC OA metadata)

## Deferred

- Bundling full text as a served corpus (full-text search; per-paragraph embeddings) is scoped as a future v2 decision with explicit cost + licensing models. See ADR-0003+ (TBD).

## Consequences

**Positive:**
- Full-text-depth signal with negligible storage overhead and no redistribution liability.
- Mention-type classification is a novel, research-grade contribution that materially strengthens §3.2b of the paper.
- Reproducible: cite PMC OA, publish extraction + classification code, validate against EPMC annotations where they overlap (GEO + BioProject).

**Negative:**
- Need a defensible mention-type taxonomy + validation (classify a sample, report classifier-vs-annotator agreement).
- Adds a classification step to the ETL. Mitigation: keep coarse (3–4 classes) for v1.

## References

- Europe PMC SciLite annotations: <https://europepmc.org/AnnotationsApi>
- PMC OA subset: <https://www.ncbi.nlm.nih.gov/pmc/tools/openftlist/>
- NCBI BioC for PMC: <https://www.ncbi.nlm.nih.gov/research/bionlp/APIs/BioC-PMC/>
