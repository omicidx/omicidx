# OmicIDX architecture

This document captures the architectural decisions for OmicIDX and how it fits into the broader biomedical ELT platform (monode). It covers the orchestration model, data backends, API design, infrastructure, and rationale for each choice.

If you're reading this for the first time, read the **System overview** and **Decision principles** sections first. The detailed sections below are reference material.

---

## System overview

OmicIDX ingests biomedical metadata from NCBI (SRA, GEO, BioSample, PubMed), EBI BioSample, and related sources, transforms it into clean parquet datasets, serves it via a public REST API, and emits canonical parquet data products for downstream consumers (DuckDB views, the omicidx.duckdb file, etc.).

```
                        NCBI / EBI / PubMed
                                │
                                │  (extract via httpx)
                                ▼
                    ┌─────────────────────────┐
                    │  Raw parquet on R2      │  ◄── source of truth
                    │  s3://omicidx/raw/...   │
                    └─────────────────────────┘
                                │
                                │  (Dagster + DuckDB transforms)
                                ▼
                    ┌─────────────────────────┐
                    │  Clean parquet on R2    │  ◄── canonical data product
                    │  s3://omicidx/clean/... │
                    └─────────────────────────┘
                          │              │
                          │              │  (load)
                          │              ▼
                          │  ┌────────────────────────┐
                          │  │  Serving layer:        │
                          │  │  - ClickHouse (now)    │
                          │  │  - StarRocks (later)   │
                          │  └────────────────────────┘
                          │              │
                          │              │  (HTTP / MySQL protocol)
                          │              ▼
                          │  ┌────────────────────────┐
                          │  │  FastAPI on Cloud Run  │
                          │  │  api.omicidx.org       │
                          │  └────────────────────────┘
                          │              │
                          │              │  (request logs)
                          │              ▼
                          │  ┌────────────────────────┐
                          │  │  ClickHouse: api_usage │
                          │  └────────────────────────┘
                          │
                          │  (DuckDB SQL views, monthly export)
                          ▼
                    ┌─────────────────────────┐
                    │  omicidx.duckdb         │
                    │  Public data product    │
                    └─────────────────────────┘
```

---

## Decision principles

Two principles shape every choice below:

1. **Parquet on R2 is the source of truth.** Serving databases (ClickHouse, StarRocks) are read-optimized projections that can be rebuilt from parquet at any time. The reverse-ETL output is nearly free because the canonical parquet *is* the output.

2. **Platform distinct from ETL.** The Dagster deployment, shared resources, and cross-project assets live in the platform repo (monode). Project-specific ETL lives in project repos (omicidx, bugsigdb, cmgd) as Dagster code locations. Default to loose coupling between projects (via data products). Tight coupling (cross-location asset deps) is the exception.

---

## Repositories and their roles

| Repo | Role |
|------|------|
| `omicidx` (this repo) | omicidx-parsers (library), omicidx-api (Cloud Run service), omicidx-dagster (Dagster code location), omicidx-etl (CLI for ad-hoc dev use, slated for deprecation/simplification) |
| `monode` | Dagster deployment infrastructure (Docker Compose), shared resource libraries (StorageManager, ClickHouse client, StarRocks client), cross-project assets (sample mapping across projects), Traefik config |
| `bugsigdb`, `cmgd`, etc. | Future biomedical projects, each contributing their own Dagster code location to monode's deployment |

---

## Decisions

### D1. DuckDB for the ELT (Extract → Load → Transform) layer

**Decision**: Dagster assets that fetch raw data, parse it, and produce clean parquet use DuckDB in-process inside the asset. The output is parquet on R2.

**Why DuckDB and not StarRocks/ClickHouse for transforms:**

- **No load step.** Raw parquet on R2 IS the source. DuckDB queries it directly via the S3 extension. StarRocks would require Stream Load or Broker Load for every transform run.
- **Schema-on-read.** No DDL migrations to manage. Pydantic models in `omicidx-parsers` carry schema; parquet files carry runtime schema; DuckDB infers what it needs.
- **In-process in Dagster.** No network hop, no connection pool, no server availability requirement. The transform succeeds or fails based on the raw data alone.
- **Disposable serving layer.** ClickHouse and StarRocks are derived from parquet. If they break or need rebuilding, parquet is intact.
- **Reverse ETL is nearly free.** The clean parquet is the data product. No explicit export step.

**Cost accepted:** Each Dagster run re-reads from R2 (network bandwidth, but R2 egress to the server's compute is free if both are colocated, or fast otherwise). Incremental detection ("which BioSample records changed since last run") requires manual logic in Dagster — but this would be true with StarRocks-based ELT too.

**Rejected alternative**: StarRocks-centric ELT (load raw → staging tables → SQL transform → production tables → export parquet). Adds an extra data copy, makes parquet a derived second-class output, and requires the server to be up for the transform pipeline to work. The intermediate state in StarRocks tables would be richer but also a separate thing to maintain.

### D2. Serving layer: ClickHouse now, StarRocks evaluated in parallel

**Decision**: ClickHouse is the initial serving backend for the API. StarRocks gets evaluated in parallel (loading the same dataset, comparing query patterns) before any commitment.

**Why ClickHouse first:**

- It is already running at `clickhouse.cancerdatasci.org` and has GEO data loaded (8.5M samples, 282K series, 28K platforms).
- ClickHouse's HTTP interface and S3 table function (`SELECT * FROM s3('s3://...', 'Parquet')`) make the load path trivial.
- DuckDB has a ClickHouse extension; loading from clean parquet is one query.
- Schema-on-load via `INSERT INTO t FORMAT Parquet` works without DDL fuss for initial iteration.

**Why StarRocks is worth evaluating:**

- **Primary key tables with native upserts.** Records change over time (new SRA runs, updated BioSample annotations). ClickHouse's `ReplacingMergeTree` handles this lazily and forces `FINAL` on queries for consistency, which serializes execution. StarRocks does merge-on-read cleanly.
- **MySQL wire protocol.** SQLAlchemy MySQL dialect just works. Better Python ergonomics for the API than HTTP.
- **Better multi-table joins.** Cross-referencing SRA → BioSample → GEO is a real query pattern.
- **Mixed read/write workload.** Better than ClickHouse for the combination of ingestion + serving + analytics.

**Cost of evaluating both:** Stream Load complexity in StarRocks, current bad gateway issue, and the fact that StarRocks tooling in the Python ecosystem is less polished than `clickhouse-connect`. Worth the friction for a learning project; load one dataset (GEO series at 282K rows) and compare.

**Rejected alternative**: Cloud SQL PostgreSQL. PostgreSQL would work for accession-based point lookups but is not designed for analytical queries on 50M+ row tables. ClickHouse and StarRocks both vastly outperform Postgres for the filter-and-list query patterns the API needs. Cost would also be higher (managed Cloud SQL vs self-hosted on the existing big server).

### D3. API: FastAPI on Cloud Run, async, SQLAlchemy 2.0 ORM with Core-style queries

**Decision**: New `packages/omicidx-api/` package. FastAPI app with async endpoints. Connection to the serving DB is via async SQLAlchemy 2.0 using `select()` statements (ORM models for type safety, but no `session.query()` patterns). Deployed to Cloud Run with min-instances=1.

**Endpoint pattern:**

- `GET /v1/{entity}/{accession}` — single lookup
- `GET /v1/{entity}` — list with cursor pagination and filters

**Pagination**: keyset-based on the primary accession key. Cursor is opaque base64url-encoded JSON. No `total` count returned (too expensive at 50M rows). See issue #41 for full envelope spec.

**Cross-references**: each entity response includes a `relationships` block linking related entities (SRA Run → Experiment → Study → BioSample, etc.). Implements REST best practices.

**Why Cloud Run not the big server:**
- Stateless, autoscaling, no instance management.
- HTTPS termination handled by Cloud Run.
- The big server has no inbound tunneling allowed by IT — but it's reachable as `clickhouse.cancerdatasci.org` over HTTPS via Traefik. Cloud Run can connect to that endpoint without any special networking.
- Separation of concerns: data plane (server) and serving plane (Cloud Run) scale independently.

**Why async SQLAlchemy ORM (not pure Core, not raw asyncpg):**
- ORM gives type-safe row-to-Pydantic mapping.
- `select()` statements give Core-level query control without `session.query()` ergonomic baggage.
- Standard pattern, well-documented, plenty of community examples.

### D4. Usage logging: ClickHouse direct from FastAPI

**Decision**: Per-request structured events written directly to ClickHouse via `clickhouse-connect` async client. Loguru handles operational logs (errors, startup) to stdout → Cloud Logging. Usage events bypass Cloud Logging entirely.

**Schema and full design**: see issue #43.

**Why direct to ClickHouse, not Cloud Logging → BigQuery:**
- ClickHouse is already running and is purpose-built for this workload.
- BigQuery is overkill for the volume; ClickHouse is faster and cheaper for the analytical queries that matter (monthly grant reports).
- Cuts an entire managed-service tier (Cloud Logging sinks → Pub/Sub → BigQuery) out of the architecture.
- Operational logs still go to Cloud Logging for alerting and ops; that's not what this is replacing.

**Cost accepted**: if ClickHouse is unavailable, usage events can be lost (queue overflow, no durable buffer). This is acceptable because usage logging is non-critical-path; operational logs that *do* matter still go to Cloud Logging.

### D5. Orchestration: Dagster, with project repos owning code locations

**Decision**: Dagster runs as a centralized deployment hosted by monode. Each project (omicidx, bugsigdb, cmgd, etc.) contributes a code location loaded by monode's Dagster webserver via gRPC. Cross-project work that requires tight coupling lives in monode as cross-location assets.

**Why Dagster:**
- Asset-centric model fits biomedical data: each dataset (BioSample table, monthly GEO partition, PubMed baseline) is a Dagster asset with lineage, metadata, freshness tracking.
- Native partition support (`MonthlyPartitionsDefinition`, etc.) replaces hand-rolled partition logic.
- The asset catalog provides the metadata layer for grant reporting (what ran, when, how many rows, freshness).
- The historical concern about Dagster's "implementation complexity slowing iteration" is materially lower in the agentic-coding era — the iteration cost on assets/resources/partitions is now small.

**Why code locations per project, not absorbed into monode:**
- omicidx is a library + data product with external visibility. It deserves to be self-contained.
- Code locations are Dagster's designed mechanism for multi-team / multi-project deployments.
- Each project keeps its own release cadence and identity.
- Pattern scales: bugsigdb, cmgd, future projects each contribute a code location.

**Public read-only dashboard**: `dagster-webserver --read-only` runs as a second instance on the same Dagster postgres database, exposed at `dagster.cancerdatasci.org` behind Cloudflare Access. The asset catalog becomes living documentation for collaborators and grant reviewers.

**Migration plan**: see issue #44 for the six-phase plan.

**Rejected alternatives**:
- Keep using GitHub Actions cron + Click CLI as the orchestration layer. Works but doesn't give the unified asset catalog, partition observability, or cross-project capability.
- Other orchestrators (Prefect, Temporal, Dagger, SQLMesh, Hamilton, DBOS). All have been tried in this project's history. The user's verdict and the analysis above land on Dagster as the right fit for asset-centric biomedical ELT.

### D6. Reverse ETL: parquet on R2 with optional DuckDB views

**Decision**: The clean parquet output of D1 is the canonical reverse-ETL product. DuckDB view SQL files (currently in `omicidx-etl/sql/`) are run as Dagster assets to produce the `omicidx.duckdb` file periodically.

**Why this works:** since parquet on R2 is the source of truth (D1), the reverse-ETL doesn't require copying data out of any serving database. The view SQL just generates a denormalized DuckDB file that points at the parquet.

---

## Infrastructure

### The big server

- **Specs**: 64 cores, 512 GB RAM, 20 TB storage, mostly always-on (~10 min/month downtime for updates)
- **Network**: full outbound access. No inbound tunneling allowed. Public HTTPS via Traefik + Cloudflare.
- **Existing services**:
  - ClickHouse at `clickhouse.cancerdatasci.org` (port 443 via Traefik proxy to ClickHouse port 8123)
  - StarRocks at `starrocks.cancerdatasci.org` (currently bad gateway; needs Traefik config or container fix)
  - Traefik with Cloudflare TLS-ALPN cert resolver
  - PostgreSQL (operational; can be reused for Dagster's metadata DB)
- **Will host**:
  - Dagster deployment (webserver, daemon, postgres) via Docker Compose
  - Public read-only Dagster UI at `dagster.cancerdatasci.org` behind Cloudflare Access

### Cloud Run

- Hosts the API (`api.omicidx.org`)
- Connects to ClickHouse / StarRocks over public HTTPS (no VPC, no special networking)
- 2 vCPU / 4 GB RAM, min-instances=1, max-concurrency=100
- Multi-stage Docker build, non-root, health check on `/v1/health`

### R2 (Cloudflare object storage)

- All raw and clean parquet
- Accessed via S3-compatible API by DuckDB and ClickHouse
- Configured via `PUBLISH_DIRECTORY` env var and AWS-style credentials

### Domains and networking

- `clickhouse.cancerdatasci.org` — ClickHouse HTTP interface
- `starrocks.cancerdatasci.org` — StarRocks (FE HTTP port; MySQL protocol on 9030 needs separate exposure if used)
- `dagster.cancerdatasci.org` — Dagster read-only UI (planned)
- `api.omicidx.org` — public API (planned, on Cloud Run)
- All TLS handled by Traefik + Cloudflare for the server, by Cloud Run for the API

---

## Repository structure (target state)

```
omicidx/                                   # this repo
├── docs/
│   └── architecture.md                    # this file
├── pyproject.toml                         # uv workspace root
├── packages/
│   ├── omicidx-parsers/                   # library: XML parsers + Pydantic models
│   ├── omicidx-etl/                       # CLI for ad-hoc dev use (slated for simplification)
│   ├── omicidx-dagster/                   # Dagster code location (issue #44)
│   │   └── src/omicidx/dagster/
│   │       ├── definitions.py
│   │       ├── assets/                    # ingestion + transform assets
│   │       └── resources/                 # NCBI client etc.
│   └── omicidx-api/                       # FastAPI on Cloud Run (issue #41)
│       └── src/omicidx/api/
│           ├── main.py
│           ├── routers/                   # one per entity domain
│           ├── schemas/                   # Pydantic response models
│           └── db.py                      # async SQLAlchemy engine
└── .github/workflows/                     # CI for tests; ETL cron likely retired post-Dagster

monode/                                    # platform repo
├── infrastructure/
│   ├── compose/
│   │   ├── dagster/                       # Dagster Docker Compose
│   │   │   ├── docker-compose.yml
│   │   │   └── workspace.yaml             # registers code locations
│   │   ├── traefik/                       # exists
│   │   └── postgresql/                    # exists
│   └── ansible/                           # exists
├── libs/
│   ├── monode-resources/                  # StorageManager, CH/SR clients
│   └── monode-types/                      # shared Pydantic types
└── projects/
    └── cross-project/                     # sample mapping, harmonization

bugsigdb/, cmgd/, ...                      # future project repos, each with their own code location
```

---

## Open questions

These are decisions that don't need answers right now but should be revisited:

1. **StarRocks vs ClickHouse for serving.** Decided in parallel evaluation, no permanent commitment until both have been used with real data. (D2)

2. **omicidx-etl CLI fate.** Deprecate entirely once Dagster covers everything, or keep as a thin debug wrapper for parser-level dev work? Leaning toward deprecation. (issue #44 phase 6)

3. **Where do shared resources actually live?** monode currently has `StorageManager` inside the dags package. To be consumed by external code locations like omicidx-dagster, it needs to be factored out into `monode/libs/monode-resources/` and made pip-installable.

4. **Workspace dependencies vs published packages.** When monode loads omicidx-dagster as a code location, how does it find omicidx-parsers and monode-resources? Probably `pip install -e` from co-located checkouts on the server. May need to publish to PyPI or a private index later.

5. **Partition strategy alignment.** monode's Dagster code uses `MonthlyPartitionsDefinition` for GEO. omicidx-etl uses different partition logic for SRA. Pick canonical approaches per data source during the migration.

6. **API authentication and rate limiting.** Lower priority for v1 launch (academic open API). Plan for `slowapi`-based per-IP limits and an optional API key system before any public-launch announcement.

---

## Migration plan and priorities

The work breaks into independent tracks that can run in parallel:

### Track A — server prep (no code changes here)

- Stand up monode's Dagster deployment on the big server via Docker Compose (webserver, daemon, postgres)
- Fix StarRocks bad gateway (likely Traefik route or stopped container)
- Wire `dagster.cancerdatasci.org` behind Cloudflare Access
- Factor `StorageManager` out into `monode/libs/monode-resources/` and make it pip-installable

### Track B — omicidx code review cleanup

- Land PR #42 (critical parser bugs)
- Address issues #38–#40 (ETL bugs, dead code, test gaps)
- These touch code that will be ported to omicidx-dagster, so easier to fix before porting

### Track C — omicidx-dagster code location (issue #44)

- Six-phase migration: scaffold package → port assets → transforms → loaders → register with monode → cleanup
- Depends on Track A finishing for full deployment, but development can proceed locally

### Track D — omicidx-api (issue #41)

- Independent of A and C; can start any time
- ClickHouse-backed (D2, D4); reuses existing GEO data for first endpoints
- Phased: BioSample lookup → SRA endpoints → GEO + PubMed → filters/cursors/usage logging → Cloud Run deploy → rate limits

The API track (D) has the property of being end-to-end testable without any of the other tracks completing — it's the fastest path to a demonstrable user-facing artifact.

---

## References

- **Issues**:
  - #36 — Pydantic v2 model bugs (mostly fixed in PR #42)
  - #37 — Parser logic crashes (mostly fixed in PR #42)
  - #38 — ETL bugs: heartbeat hang, biosample skip-check, urllib in pubmed
  - #39 — Dead code, typos, style
  - #40 — Test coverage gaps
  - #41 — REST API spec (FastAPI + Cloud SQL → updated to ClickHouse + Cloud Run)
  - #42 — PR: critical parser fixes
  - #43 — ClickHouse usage logging design
  - #44 — omicidx-dagster code location migration plan

- **CLAUDE.md** — codebase conventions and current commands

---

## Glossary

- **R2**: Cloudflare's S3-compatible object storage; cheap egress to the public internet, free egress to Cloudflare-hosted services
- **Code location**: in Dagster, a separate Python package loaded by the central webserver/daemon via gRPC. Each code location contains its own assets, resources, jobs.
- **monode**: the user's platform-level monorepo for biomedical ELT infrastructure
- **omicidx-parsers**: the library package; XML/SOFT parsers + Pydantic models, no orchestration
- **omicidx-dagster**: planned Dagster code location for omicidx ETL (issue #44)
- **omicidx-api**: planned FastAPI service for read-only API (issue #41)
- **omicidx-etl**: existing CLI + GitHub Actions cron orchestration; slated for deprecation
- **Reverse ETL**: in this project, the production of canonical parquet data products and the `omicidx.duckdb` file from the serving layer or directly from clean parquet
