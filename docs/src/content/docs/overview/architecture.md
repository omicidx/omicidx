---
title: Architecture
description: How raw NCBI/EBI feeds become a queryable index — Dagster → R2 → DuckDB → PostgreSQL → REST.
---

OmicIDX is a five-layer pipeline: each upstream source becomes Parquet on object storage, all sources collapse into a single DuckDB file, the same data lands in PostgreSQL for the API, and a FastAPI service exposes it as REST.

## The pipeline at a glance

```
NCBI / EBI feeds                                       (XML, SOFT, JSON)
        │
        ▼
omicidx-parsers     ── XML → Pydantic models
        │
        ▼
omicidx-dagster     ── per-source raw assets    →  R2 (NDJSON.gz, JSONL.gz, Parquet)
        │
        ▼
consolidation       ── per-entity DuckDB COPY   →  R2 (one Parquet per entity)
        │
        ├──────────────────────────┐
        ▼                          ▼
omicidx_duckdb              postgres loaders
(monolithic .duckdb file    (DuckDB postgres_scanner
 published daily to R2)      with A/B view swap)
                                   │
                                   ▼
                            PostgreSQL  ←── omicidx-api (FastAPI)
                                                │
                                                ▼
                            api-omicidx.cancerdatasci.org
```

## Layers

### 1. Parsing — `omicidx-parsers`

Pure-Python parsers for NCBI SRA XML, GEO SOFT, BioSample/BioProject XML, and PubMed XML. Output is typed Pydantic v2 models. No I/O concerns; the parsers are reusable as a library.

### 2. Extraction — `omicidx-dagster` (raw layer)

Daily Dagster assets fetch from upstream FTP/REST and write JSONL.gz / NDJSON.gz / Parquet to Cloudflare R2. One asset per source. Partitioned where it makes sense (GEO monthly, EBI BioSample daily, PubMed per-file).

### 3. Consolidation — `omicidx-dagster` (consolidate layer)

Per-entity Parquet rebuilds. Each consolidation asset reads the raw layer via DuckDB's `read_ndjson_auto` / `read_parquet`, applies trimming and deduplication, and writes a single Parquet file. Most run via `AutomationCondition.eager()` and cascade automatically when their raw upstream lands.

PubMed is the exception — its hourly file sensor would otherwise trigger hourly cascades. PubMed's consolidation, Postgres load, and the DuckDB build are gated to once-daily via `on_cron(...) & any_deps_updated()`. See [Automation cadence](/contributing/automation-cadence/).

### 4a. DuckDB publication — `omicidx_duckdb`

A single `omicidx.duckdb` file is built daily from the consolidated Parquet via the SQL files in `packages/omicidx-dagster/src/omicidx/dagster/sql/`. The file is uploaded to R2 and is the canonical analytical snapshot — anyone can download it and query the entire index offline.

### 4b. PostgreSQL load — `*_postgres` assets

The same consolidated Parquet is loaded into PostgreSQL via DuckDB's `postgres_scanner` extension. Each entity uses a **zero-downtime A/B view swap**: the API reads from a view (`biosample`, `sra_study`, ...); two backing tables (`{entity}_a`, `{entity}_b`) alternate. Each reload writes to whichever isn't currently live, then `CREATE OR REPLACE VIEW` atomically points the view at the fresh table. Reads are never blocked.

### 5. REST API — `omicidx-api`

FastAPI app at `https://api-omicidx.cancerdatasci.org`. SQLAlchemy 2.0 + asyncpg, base64url keyset cursor pagination, slowapi rate limiting. Auto-generated OpenAPI spec at `/openapi.json` is the source of truth for the [API reference](/api/reference/).

## Where things live

| Layer        | Storage                | Format                     |
| ------------ | ---------------------- | -------------------------- |
| Raw          | Cloudflare R2          | NDJSON.gz, JSONL.gz, XML   |
| Consolidated | Cloudflare R2          | Parquet (one per entity)   |
| Analytical   | Cloudflare R2          | `omicidx.duckdb` (single file) |
| Serving      | PostgreSQL             | Tables behind A/B views    |
| API          | FastAPI / Traefik / TLS | JSON over HTTPS           |

## Why this shape

- **Object storage as the spine.** Parquet on R2 is cheap, queryable directly via DuckDB, and serves as the integration point between every layer. No layer "owns" the data.
- **DuckDB everywhere.** Same engine for consolidation, the published snapshot, and the loaders into Postgres. Single set of SQL idioms across the pipeline.
- **PostgreSQL only for serving.** Postgres handles concurrent reads, indexes, and online schema operations. The compute happens upstream in DuckDB.
- **Dagster for orchestration.** Declarative automation conditions handle the cascade; the cron-paced `on_cron(...) & any_deps_updated()` pattern (see [Automation cadence](/contributing/automation-cadence/)) prevents high-frequency upstreams from triggering full-graph rebuilds.
