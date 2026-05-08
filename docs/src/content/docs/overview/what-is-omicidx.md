---
title: What is OmicIDX?
description: A unified, queryable index of biological metadata aggregated from NCBI, EBI, and PubMed.
---

OmicIDX is a public, read-only index that aggregates biological metadata from a handful of authoritative sources into a single, queryable shape.

## What it indexes

- **NCBI SRA** — sequencing studies, samples, experiments, runs.
- **NCBI GEO** — gene expression series, samples, and platforms.
- **NCBI BioSample** and **BioProject** — descriptive metadata that ties everything together.
- **NCBI PubMed** — citation records, used to link publications back to the experimental data they describe.
- **EBI BioSamples** — the European complement to NCBI BioSample, daily-partitioned.

See [Data sources](/overview/data-sources/) for source URLs, refresh cadences, and what each contributes.

## What it isn't

- **Not a sequence archive.** OmicIDX indexes the metadata around sequencing data — the studies, samples, and experiments — not the raw reads. For sequence retrieval, use the upstream archives directly.
- **Not real-time.** The index refreshes daily on cron-driven cascades. See [Architecture](/overview/architecture/).
- **Not a write API.** All endpoints are read-only.

## Why it exists

Querying biological metadata across the major archives means dealing with five different XML/SOFT formats, several FTP layouts, inconsistent identifier conventions, and no usable join keys. OmicIDX normalizes those into a single relational model with stable accessions and cross-source links, then exposes it as a REST API and as a single downloadable DuckDB file.

## Who uses it

Researchers, bioinformaticians, and data engineers who want to:

- Search across multiple archives without writing per-archive XML parsers.
- Build pipelines that resolve study/sample/experiment relationships across sources.
- Get a downloadable analytical snapshot (DuckDB) for offline work.

The API is open and rate-limited; no API key required. See [Rate limits](/api/rate-limits/).
