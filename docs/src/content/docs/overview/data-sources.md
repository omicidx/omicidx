---
title: Data sources
description: Origin, format, and refresh cadence of each source OmicIDX indexes.
---

OmicIDX pulls from six upstream sources. Each is independently refreshed and consolidated into a Parquet snapshot before being loaded into the API's PostgreSQL backing store.

## Sources

### NCBI SRA

- **What:** Sequence Read Archive metadata — studies, samples, experiments, runs.
- **Source:** NCBI FTP mirror of `Metadata/` XML files, plus the `SRA_Run_Members` and `SRA_Accessions` accession tables.
- **Cadence:** Daily.
- **Format upstream:** XML (samples, studies, experiments, runs); TSV (accessions/run members).
- **Format here:** Parquet, one file per entity (`sra_studies.parquet`, `sra_samples.parquet`, etc.).

### NCBI GEO

- **What:** Gene Expression Omnibus — series (GSE), samples (GSM), platforms (GPL), and the RNA-seq counts overlay.
- **Source:** NCBI GEO FTP, monthly-partitioned mirrors.
- **Cadence:** Monthly fetch; daily for the RNA-seq counts overlay.
- **Format upstream:** SOFT (text) and per-record XML.
- **Format here:** Parquet per entity, deduplicated to the latest version per accession.

### NCBI BioSample

- **What:** NCBI's descriptive sample metadata.
- **Source:** `https://ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz`.
- **Cadence:** Daily.
- **Format upstream:** Single gzipped XML dump.
- **Format here:** JSONL.gz raw, then Parquet.

### NCBI BioProject

- **What:** NCBI's project-level metadata (the umbrella above samples and experiments).
- **Source:** `https://ftp.ncbi.nlm.nih.gov/bioproject/bioproject.xml`.
- **Cadence:** Daily.
- **Format upstream:** Single XML dump.
- **Format here:** JSONL.gz raw, then Parquet.

### NCBI PubMed

- **What:** Citation records (titles, authors, journals, MeSH terms, abstracts).
- **Source:** PubMed baseline + daily update files via NCBI FTP.
- **Cadence:** Hourly file sensor → daily consolidation. New baseline ingest every January when NCBI re-publishes the corpus.
- **Format upstream:** Per-day XML files.
- **Format here:** Parquet, partitioned by file then consolidated daily.

### EBI BioSamples

- **What:** The European complement to NCBI BioSample. Many samples appear here that aren't in NCBI BioSample.
- **Source:** `https://www.ebi.ac.uk/biosamples/samples` (REST API with `dt:update` filter).
- **Cadence:** Daily-partitioned fetch starting 2021-01-01.
- **Format upstream:** JSON via paginated REST.
- **Format here:** NDJSON.gz per partition, consolidated to a single Parquet.

## Where the data lives

- **Raw + Parquet:** Cloudflare R2, S3-compatible.
- **Analytical snapshot:** `omicidx.duckdb` — a single DuckDB file containing all the views; rebuilt daily, served from R2.
- **API backing store:** PostgreSQL, loaded via DuckDB's `postgres_scanner` extension with a zero-downtime A/B view swap.

See [Architecture](/overview/architecture/) for how this all fits together.
