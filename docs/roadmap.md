# OmicIDX Roadmap

- **Status:** Active. Maintained alongside the paper outline and the ADRs.
- **Last updated:** 2026-05-29
- **Owner:** Sean Davis

## Scope

This document tracks the path from "OmicIDX as of 2026-05-29 (design locked across ADR-0001..ADR-0004)" to "OmicIDX v1.0 paper submitted with a published v1.0 snapshot." Strategic threads live here; executable units spin out to GitHub Issues when scope warrants (current example: [#105](https://github.com/omicidx/omicidx/issues/105) for §3.2b).

This is not a status report. It's the dependency map for the work remaining.

## Tracks

Four parallel-ish tracks. Their dependencies and gating relationships are spelled out under "Dependency graph" below.

- **Track A — Public v1.0 snapshot.** One Parquet file per asset on R2, co-published `views.sql` + `README.md`. Public consumer contract per [ADR-0004](./adrs/0004-public-serving-parquet-plus-views-sql.md).
- **Track B — §3.2b accession-mining + headline number.** Executable unit lives at [#105](https://github.com/omicidx/omicidx/issues/105). Drives venue choice and §3.2b paper section.
- **Track C — Internal DuckLake migration.** Move the ETL substrate from the current Prefect + flat-Parquet target onto a Postgres-backed DuckLake catalog (consistent with Sean's 2026-05-22 portfolio substrate decision). Independent of the paper.
- **Track D — Paper writing.** OmicIDX paper. Most sections are independent; §3.2b and §4 are gated; venue choice deferred pending Track B.

## Dependency graph

```text
Track A (v1.0 snapshot, one Parquet per asset, views.sql)
   └── gates Paper §4 (Comparison — needs actual row counts)
   └── gates Paper §5 (Availability — points at this URL)

Track B (§3.2b mining, #105)
   └── gates Paper §3.2b (headline number)
   └── gates Venue choice
        (Bioinformatics rolling | NAR DB Issue Oct | GigaScience)

Track C (internal DuckLake migration)
   └── independent of paper — public contract (Track A) unchanged
   └── enables: cleaner internal MERGE semantics for §3.2b re-runs and
       v1.x evolution; mention-type classification iteration; future
       full-text-as-corpus exploration

Track D (paper writing) ── most sections independent:
   §1 Introduction              — no deps
   §2 Implementation (updated)  — no deps (ADRs already lock the story)
   §3.1 Cross-source joins      — no deps
   §3.2a Embedded-metadata      — no deps
   §3.2b Full-text mining       — blocks on Track B
   §3.3, §3.4                   — already moved to §5
   §4 Comparison                — blocks on Track A (row counts)
   §5 Availability + Future     — no deps (already updated)
   §6 Conclusions               — last

Venue choice ── gates submission framing
   └── gated on Track B headline N
```

## Track A — Public v1.0 snapshot

### Goal

Produce the v1.0 public artifact described in [ADR-0004](./adrs/0004-public-serving-parquet-plus-views-sql.md): one Parquet file per asset under `s3://data-omicidx/v1.0/`, plus `views.sql` and `README.md`. One file per asset (not partitioned, not chunked) so consumers can `wget`/`curl` a single URL per dataset.

### Work units

1. **Single-file Parquet writer per source** (sra, geo, biosample, bioproject, pubmed, ebi_biosample, publication_accession_linkage). Likely a single DuckDB `COPY ... TO '...parquet' (FORMAT PARQUET)` per asset. Size sanity-check: if any single asset is >10–20GB, decide whether to split (and document the split) or keep monolithic.
2. **`views.sql` generation** — DuckDB `CREATE VIEW` statements over the published URLs. The generated file is what a consumer runs via `.read views.sql`. Smoke-test from a clean machine: `duckdb -c ".read https://.../v1.0/views.sql; SELECT COUNT(*) FROM sra"` must work.
3. **`README.md` at the snapshot prefix** — citation, schema notes pointing at the docs site, "how to query" examples for DuckDB, Polars, DataFusion, R `arrow`. The Parquet+views.sql contract is universal; document the universality.
4. **Naive-engine smoke tests** — `polars.read_parquet(url)`, `pyarrow.parquet.read_table(url)`, `arrow::read_parquet(url)` in R. Confirm each works without any OmicIDX-specific code path.
5. **Publish step in CI or scheduled job** — runs the writer + views generation, uploads to R2 under the `v1.0/` prefix, marks the snapshot date.

### Acceptance criteria

- [ ] `s3://data-omicidx/v1.0/{sra,geo,biosample,bioproject,pubmed,ebi_biosample,publication_accession_linkage}.parquet` exist
- [ ] `s3://data-omicidx/v1.0/views.sql` exists and runs cleanly against the URL endpoints
- [ ] `s3://data-omicidx/v1.0/README.md` documents citation + 4-engine usage examples
- [ ] Row counts for each asset are documented in `README.md` (these become the §4 numbers)
- [ ] Snapshot is reproducible from a `Makefile` target or scheduled job in the repo

### Dependencies

- Independent of Tracks B, C, D. Can start now.

### Cross-references

- [ADR-0002](./adrs/0002-schema-v1-at-publication.md), [ADR-0003](./adrs/0003-schema-versioning-policy.md), [ADR-0004](./adrs/0004-public-serving-parquet-plus-views-sql.md)

## Track B — §3.2b accession-mining + headline number

### Goal

Compute the headline number — "abstract/full-text mining recovers N× more dataset↔publication links than embedded metadata alone" — and ship the derived linkage table per ADR-0001.

### Executable unit

GitHub Issue [#105 — §3.2b: Accession-mining headline number + EPMC validation](https://github.com/omicidx/omicidx/issues/105). Full scoping there: inputs, pipeline, validation against EPMC SciLite on GEO + BioProject overlap, acceptance criteria.

### Implementation note (architecture)

Per ADR-0004, the analysis runs against the **internal** Postgres-backed DuckLake (where the BioC passages already live). The resulting linkage table is one of the assets published in Track A's v1.0 snapshot.

**Can run on current architecture or post-migration.** The internal DuckLake destination doesn't require Track C to be complete; the analysis can run against the current Prefect + flat-Parquet target and produce the same result. If Track C lands first, the analysis benefits from cleaner MERGE semantics on re-runs.

### Cross-references

- [ADR-0001](./adrs/0001-derived-linkage-not-fulltext.md), [ADR-0004](./adrs/0004-public-serving-parquet-plus-views-sql.md)

## Track C — Internal DuckLake migration

### Goal

Move the ETL substrate from current Prefect + flat-Parquet target to a Postgres-backed DuckLake catalog (consistent with [Sean's 2026-05-22 portfolio substrate decision](../../../seandavis/decisions/2026-05-22-portfolio-data-substrate-ducklake.md)). DuckLake becomes the analytics surface and the source for Track A's snapshot generation.

### Why this track exists separate from the paper

[ADR-0004](./adrs/0004-public-serving-parquet-plus-views-sql.md) explicitly decouples internal catalog from public contract. The paper can ship on the current architecture (Prefect + flat-Parquet → R2) by describing the public contract correctly. The internal DuckLake migration is a substrate upgrade for Sean's own analytics work and portfolio coherence — not a paper-blocking dependency.

### Work units

1. **DuckLake schema bootstrap** — initialize the Postgres-backed DuckLake catalog on `onclappc02` or shared infra; verify schema, namespacing, snapshot capability internally.
2. **Per-source MERGE target** — for each source (sra, geo, biosample, bioproject, pubmed, ebi_biosample), port the current Prefect flow from "write Parquet, semaphore-file partition" → "MERGE INTO ducklake.<source> USING raw_<source>". One source per week is a sustainable cadence.
3. **Parity verification** — for each migrated source, verify the DuckLake table matches the prior flat-Parquet output (row count, key distribution, sample joins). Document the diff. Only after parity is confirmed, retire the old flat-Parquet writer for that source.
4. **Snapshot generation refactor** — once at least one source is on DuckLake, refactor Track A's writer to `COPY ... TO 'sra.parquet'` from DuckLake instead of from the current flat-Parquet path.
5. **§3.2b re-execution discipline** — once the migration is complete, the §3.2b analysis can be re-run with cleaner MERGE semantics for v1.x updates of the linkage table.

### Acceptance criteria

- [ ] DuckLake catalog initialized; Postgres metadata backing confirmed
- [ ] All 6 sources on DuckLake MERGE targets, parity-verified against prior flat-Parquet output
- [ ] Snapshot generation (Track A) reads from DuckLake instead of the prior flat-Parquet path
- [ ] Old Dagster/flat-Parquet writers retired from main
- [ ] CI/scheduled jobs updated to use the new path

### Sequencing reality check

Six sources, one per week = ~6 weeks at sustainable pace. Could compress if Sean is heads-down on this. Realistically with the paper deadline + quartobot + CC Data Science continuing, plan 8 weeks elapsed for full migration. **The paper does not block on this**; ship the paper on whichever architecture is in production when §3.2b's number is ready.

### Cross-references

- [ADR-0004](./adrs/0004-public-serving-parquet-plus-views-sql.md)
- Vault: [2026-05-22 portfolio data substrate (DuckLake)](../../../seandavis/decisions/2026-05-22-portfolio-data-substrate-ducklake.md)

## Track D — Paper writing (OmicIDX paper)

### Goal

Submit a paper describing OmicIDX v1.0, with §3.2b accession-mining as the load-bearing demonstrated-utility result, to whichever venue the §3.2b magnitude justifies (see Track B + the deferred-venue decision record).

### What's already decided

- The four ADRs (0001–0004) lock the design story §2 needs.
- §3.3 (embeddings) and §3.4 (MCP server) moved to §5 Future Directions.
- Schema-stability gate dissolved (ADR-0002 + ADR-0003).
- Public contract specified (ADR-0004).
- Venue decision deferred until Track B headline N is in hand. See vault: `decisions/2026-05-29-omicidx-venue-deferred.md`.

### Section-by-section status

| Section | Dependency | Status |
|---------|-----------|--------|
| §1 Introduction | none | Outline exists; needs prose. Can start anytime. |
| §2 Implementation | ADR-0004 (done) | Outline exists; needs refresh for the internal/public boundary. |
| §3.1 Cross-source joins | none | Outline exists; needs working examples. |
| §3.2a Embedded-metadata baseline | none | Outline exists; can compute baseline anytime. |
| §3.2b Full-text mining (the result) | **Track B (#105)** | Blocks on headline N. |
| §3.3 / §3.4 | n/a | Moved to §5. Done. |
| §4 Comparison table | **Track A** | Blocks on actual row counts from the v1.0 snapshot. |
| §5 Availability + Future Directions | none | Refreshed 2026-05-29. Done. |
| §6 Conclusions | none (write last) | Last. |

### Co-author + acknowledgments question

Open: who contributed substantially to the ETL or warehouse code? Worth deciding before §1 final draft.

### Cross-references

- [ADR-0001](./adrs/0001-derived-linkage-not-fulltext.md), [ADR-0002](./adrs/0002-schema-v1-at-publication.md), [ADR-0003](./adrs/0003-schema-versioning-policy.md), [ADR-0004](./adrs/0004-public-serving-parquet-plus-views-sql.md)
- Vault: `research/omicidx-paper-outline.md`, `decisions/2026-05-29-omicidx-venue-deferred.md`

## Critical path

Reading the dependency graph:

1. **§3.2b headline N is the critical-path single point.** It gates the venue choice and the §3.2b paper section. Everything else can move in parallel.
2. **Track A (v1.0 snapshot) gates §4 (row counts).** If §4 needs concrete numbers in the paper draft, Track A has to land before §4 can be finalized. But §4 can be drafted with placeholders and filled in last.
3. **Track C is decoupled.** Ship the paper on whatever architecture is in production when §3.2b lands.

Cleanest sequencing:

- **Now → ~2 weeks:** Track A v1.0 snapshot generation lands. Paper §1 + §2-refreshed-for-ADRs drafted (in parallel with quartobot paper).
- **~2–4 weeks:** §3.2b mining runs (Track B). §3 sections written as the data comes in.
- **~4–6 weeks:** Headline N in hand. Venue chosen. §4 numbers filled in from the v1.0 snapshot. §5, §6 finalized.
- **~6–8 weeks:** Draft circulated for co-author review. Submitted.

Track C migrates in parallel throughout, on its own timeline, transparent to the paper deliverable.

## Open questions

- **Co-authors** — who contributed substantially to ETL or warehouse code? Open since the original paper outline. Worth deciding before §1 final draft.
- **Bridge2AI angle** — is there a connection worth mentioning? Open since the original outline.
- **Snapshot cadence post-v1.0** — ADR-0003 says periodic, initial target quarterly. First post-v1.0 snapshot date is implicit. Decide when v1.0 is published.

## Cross-references

- ADRs: [0001](./adrs/0001-derived-linkage-not-fulltext.md), [0002](./adrs/0002-schema-v1-at-publication.md), [0003](./adrs/0003-schema-versioning-policy.md), [0004](./adrs/0004-public-serving-parquet-plus-views-sql.md)
- GitHub issue: [#105](https://github.com/omicidx/omicidx/issues/105) — §3.2b accession-mining
- Vault — paper outline: `research/omicidx-paper-outline.md`
- Vault — venue decision: `decisions/2026-05-29-omicidx-venue-deferred.md`
- Vault — substrate decision: `decisions/2026-05-22-portfolio-data-substrate-ducklake.md`
