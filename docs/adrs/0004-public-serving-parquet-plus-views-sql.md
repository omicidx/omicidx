# 0004 — OmicIDX serves Parquet snapshots + DuckDB views.sql publicly; internal catalog is private

- **Status:** Accepted
- **Date:** 2026-05-29
- **Deciders:** Sean Davis
- **Related:** [ADR-0002](./0002-schema-v1-at-publication.md), [ADR-0003](./0003-schema-versioning-policy.md)

## Context

OmicIDX has two natural surfaces:

1. **Internal substrate** — where ETL, MERGE semantics, time-travel for analytics, and joins across PubMed / SRA / GEO / BioSample / BioProject / EBI BioSample happen. Sean's portfolio substrate decision (2026-05-22) makes DuckLake the internal catalog, backed by Postgres metadata. This catalog is private — exposing the Postgres metadata layer publicly would couple consumers to Sean's infrastructure availability, raise security questions, and add operational complexity to the public deliverable.

2. **Public consumption surface** — what an external researcher, an analytics notebook, or an MCP server queries when using OmicIDX. The paper's "no auth, no install, no infrastructure to manage" pitch depends on this surface staying low-dependency.

Public-serving options considered:

- **(α) Frozen DuckLake catalog with DuckDB as the backing metadata store, served from a public R2 prefix.** Gives consumers DuckLake's catalog features (richer schema discovery, named snapshots) without exposing Sean's Postgres. Downside: anchors a published artifact to DuckLake's adoption curve; readers must know DuckLake; non-DuckDB engines lose the catalog benefit.
- **(β) Plain Parquet files on R2 plus a co-published `views.sql` defining DuckDB views over them.** Maximally simple; Parquet is the universal format. Naive DuckDB-against-URL works (`duckdb -c '.read views.sql; SELECT ...'`); non-DuckDB engines (Polars, DataFusion, Spark, R `arrow`, pandas + pyarrow) read the same Parquet without any OmicIDX-specific tooling. The `views.sql` is itself documentation of the schema.

Time-travel and live mirroring of the internal catalog are explicitly out of scope — the public contract is "snapshots exist; older snapshots are preserved but not guaranteed queryable as upstream sources or query engines evolve."

## Decision

Adopt **(β)**: **OmicIDX serves versioned Parquet snapshots on R2 plus a co-published `views.sql`. The internal Postgres-backed DuckLake catalog is not exposed.**

### Public artifact layout

```
s3://data-omicidx/
  v1.0/
    sra.parquet
    geo.parquet
    biosample.parquet
    bioproject.parquet
    pubmed.parquet
    ebi_biosample.parquet
    publication_accession_linkage.parquet   # per ADR-0001
    views.sql                                # DuckDB CREATE VIEW statements
    README.md                                # schema notes + citation
  v1.1/
    ...
```

### Consumer interface

- **DuckDB users:** `duckdb -c ".read https://data-omicidx.../v1.0/views.sql; SELECT ..."`
- **Polars / DataFusion / Spark / R `arrow` / pandas users:** read Parquet files directly by URL; consult `views.sql` and `README.md` as schema documentation rather than as executable code.
- The Parquet files are the contract. The `views.sql` is a convenience layer for DuckDB users, not a dependency for anyone else.

### Snapshot cadence

- Initial target: **quarterly snapshots** (`v1.0`, `v1.1`, `v1.2`, …), realized as new directory prefixes per ADR-0003's versioning policy.
- Cadence is operational, not contractual — may adjust based on upstream-source change frequency.

### Preservation, not time-travel

- Past snapshots (e.g., `v1.0/`) are **not deleted** from the bucket. The discipline is preservation, not guaranteed-queryable indefinitely.
- No commitment to time-travel semantics; no commitment that v1.0 will remain queryable as DuckDB, Polars, or other engines evolve.
- Consumers needing legacy access in year N+5 are expected to archive the snapshot themselves at the time they need it.

### Internal catalog (out of scope of public contract)

- Internal DuckLake (Postgres-backed) is the ETL substrate, the MERGE target, and where §3.2b accession-mining runs.
- Snapshots are produced via scheduled COPY-out from the internal catalog into the public bucket.
- Internal time-travel, branching, or live-mirror capabilities are Sean's own tooling and not part of the public contract.

## Consequences

**Positive:**
- Public contract is minimal: Parquet + a SQL file. Survives engine churn; the next year's hot query tool reads Parquet by URL with no OmicIDX-specific work.
- Multi-engine support is preserved by construction (Polars, DataFusion, Spark, pandas, R `arrow`, raw `pyarrow.parquet`).
- Internal catalog migration (current Prefect + flat-Parquet branch → DuckLake substrate) is independent of the paper deliverable. The public consumer surface stays stable across the internal migration.
- Paper §2 substrate paragraph gains a sharper distinction: "OmicIDX uses an internal DuckLake catalog for ETL and analytics; public consumption is via versioned Parquet snapshots and a co-published `views.sql`."
- Storage cost of preserving past snapshots is near-zero on R2; "we don't delete" is a defensible, soft promise that costs nothing.

**Negative:**
- No automatic schema discovery beyond `views.sql` and `README.md`. Acceptable: those files are themselves human-readable schema docs.
- Snapshot cadence vs. continuous-freshness tradeoff: consumers see periodic releases, not live data. This is a feature for reproducibility, not a bug.
- DuckLake's catalog features (named snapshots, branching, time-travel) are not accessible to public consumers. Re-evaluate if/when DuckLake adoption changes the calculus.

## References

- [ADR-0001](./0001-derived-linkage-not-fulltext.md) — derived publication↔accession linkage table that flows through this pipeline
- [ADR-0002](./0002-schema-v1-at-publication.md) — v1.0 declaration anchored to these public snapshots
- [ADR-0003](./0003-schema-versioning-policy.md) — versioning policy realized via the `vN.N/` directory prefix pattern
- Sean Davis (2026-05-22) — Portfolio data substrate decision (DuckLake catalog; Prefect orchestrator; Postgres-per-product serving layer; shared extract monorepo)
