# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repo structure

This is a **uv workspace** consolidating four packages:

```
omicidx/                        # workspace root (no package of its own)
├── pyproject.toml              # workspace root: members = ["packages/*"]
├── uv.lock
└── packages/
    ├── omicidx-parsers/        # XML parsers + Pydantic models for NCBI SRA, GEO, BioSample
    │   └── src/omicidx/parsers/
    ├── omicidx-etl/            # ETL pipelines that extract raw data to S3/R2 as Parquet
    │   └── src/omicidx/etl/
    ├── omicidx-dagster/        # Dagster code location for ETL orchestration + postgres loads
    │   └── src/omicidx/dagster/
    ├── omicidx-prefect/        # Prefect 3 reimplementation of the ETL pipeline (DuckLake)
    │   └── src/omicidx/prefect/
    └── omicidx-api/            # Read-only FastAPI REST API backed by PostgreSQL
        └── src/omicidx/api/
```

All packages share the `omicidx` **namespace package** (PEP 420 implicit — no `__init__.py` at `src/omicidx/`). `omicidx-etl` depends on `omicidx-parsers` as a workspace-local reference (`tool.uv.sources`).

## Commands

All commands run from the workspace root.

```bash
# Install all workspace packages and dependencies
uv sync

# Run tests (parsers has network-hitting tests against live NCBI APIs)
uv run pytest packages/omicidx-parsers/tests/
uv run pytest packages/omicidx-etl/tests/
uv run pytest packages/omicidx-api/tests/

# Run a single test file
uv run pytest packages/omicidx-parsers/tests/geo/test_parser.py

# ETL CLI (requires .env with AWS/S3 credentials)
uv run oidx --help
uv run oidx sra extract --dest s3://${OMICIDX_DATA_ROOT}/sra/raw
uv run oidx geo extract s3://${OMICIDX_DATA_ROOT}
uv run oidx biosample extract s3://${OMICIDX_DATA_ROOT}
uv run oidx pubmed extract s3://${OMICIDX_DATA_ROOT}

# Parser CLI (GEO entry point)
uv run omicidx_tool --help

# Just recipes (omicidx-etl) — wraps oidx commands with .env loading
just sra-extract
just geo-extract
just extract-all
```

## Architecture

### omicidx-parsers

Parses raw XML from NCBI FTP/API into typed Pydantic v2 models. Key submodules:

- `omicidx.parsers.sra` — SRA Study/Sample/Experiment/Run XML → `SraStudy`, `SraSample`, etc.
- `omicidx.parsers.geo` — GEO SOFT format → `GEOSeries`, `GEOSample`, `GEOPlatform`
- `omicidx.parsers.biosample` — BioSample/BioProject XML → `BioSampleParser`, `BioProjectParser`
- `omicidx.parsers.scripts.geo` — Click CLI (`omicidx_tool`), exposed as entry point

Parsers return iterators of dicts or Pydantic models. The `sra.parser` module is the primary entry point; it detects entity type from filename.

### omicidx-etl

Long-running extraction jobs that write Parquet/NDJSON to S3-compatible storage. Each data source is a submodule with an `extract` Click command registered in `omicidx.etl.cli:cli` (`oidx`):

- `omicidx.etl.sra` — mirrors NCBI SRA XML, converts to Parquet partitioned by date/stage
- `omicidx.etl.geo` — fetches GEO SOFT files, writes NDJSON to partitioned paths
- `omicidx.etl.biosample` — streams BioSample XML, writes JSONL.gz
- `omicidx.etl.etl.pubmed` — downloads PubMed baseline + updates → Parquet
- `omicidx.etl.sql` — DuckDB SQL runner; SQL files are bundled as package data in `omicidx/etl/sql/*.sql`
- `omicidx.etl.build_db` — assembles the DuckDB database from Parquet via the view SQL files

Configuration is via `omicidx.etl.config.Settings` (pydantic-settings), loaded from environment or `.env`. Key variable: `PUBLISH_DIRECTORY` (default `/data/omicidx`, supports S3 URIs via `universal-pathlib`).

### SQL layer (ETL)

SQL files in `packages/omicidx-etl/src/omicidx/etl/sql/` define a two-stage DuckDB pipeline:

- `010_raw_to_parquet.sql` — raw data consolidation (run via `oidx sql run`)
- `020_`–`050_*.sql` — view definitions (`src_*`, `stg_*`, `geometadb.*`, `sradb.*`) built by `oidx build-db`

### omicidx-prefect

Prefect 3 reimplementation of the omicidx-dagster pipeline on the DuckLake
substrate. Partition state lives in **semaphore JSON files** in the storage
bucket (not Dagster's event log). Pipeline:

```
raw-extract → ducklake-load → parquet-export → postgres-load → duckdb-build
```

| Stage | Module | What it does |
|---|---|---|
| `raw-extract` | `flows/{sra,geo,biosample,ebi_biosample,pubmed}.py` | NCBI/EBI → raw Parquet/NDJSON on R2 (`PUBLISH_ROOT`), semaphore-gated |
| `ducklake-load` | `flows/ducklake*.py` | MERGE raw → `lake.omicidx.*` (hash-gated, copy-on-write; SRA high-water-mark incremental) |
| `parquet-export` | `flows/parquet_export.py` | Reverse-ETL: COPY lake tables → public Parquet `r2://data-omicidx/latest/*.parquet` (ADR-0004) |
| `postgres-load` | `flows/postgres.py` | Reload API-serving Postgres tables from the lake (A/B-slot swap) |
| `duckdb-build` | `flows/sql.py` + `sql/020–050` | Build `omicidx.duckdb` from the public Parquet via view SQL |

- Config: `config.py` (`Settings` + `get_ducklake_connection`, `get_public_parquet_path`, etc.). Key env: `PUBLISH_ROOT`, `DUCKLAKE_URI`, `DUCKLAKE_DATA_PATH`, `PUBLIC_PARQUET_ROOT`, `PUBLIC_PARQUET_HTTPS_BASE`, `POSTGRES_URI`.
- Catalog topology + MERGE/maintenance conventions: `DUCKLAKE.md`. Public-serving contract: `docs/adrs/0004`.
- Operator CLI `omicidx-prefect` (`cli.py`); deployments in `prefect.yaml`; worker-only `docker-compose.yml` joins the shared monode `prefect-server`.

### omicidx-api

Read-only REST API for entity lookups, deployed at `api-omicidx.cancerdatasci.org`. Key modules:

- `omicidx.api.main` — FastAPI app, lifespan, middleware registration
- `omicidx.api.models.tables` — SQLAlchemy 2.0 ORM models (BioProject, BioSample, SRA, GEO, PubMed)
- `omicidx.api.routers` — endpoint routers per entity type
- `omicidx.api.pagination` — base64url keyset cursor encode/decode
- `omicidx.api.schemas.envelope` — consistent response envelope (data, meta, links, relationships)
- `omicidx.api.config` — pydantic-settings with `OMICIDX_API_` env prefix

Configuration via `OMICIDX_API_DATABASE_URL` (standard `postgresql://` URI, `+asyncpg` added internally).

### omicidx-dagster

Dagster code location orchestrating ETL pipelines. See `packages/omicidx-dagster/` for details. Key resources:

- `OmicidxStorage` — R2/S3 paths via UPath (`get_upath()`) or DuckDB (`get_duckdb_path()`)
- `DuckDBResource` — DuckDB connections with R2 credentials
- `PostgresResource` — PostgreSQL via `POSTGRES_URI` env var; provides `execute_sql()` (asyncpg DDL) and `attach()` (DuckDB postgres_scanner)

### Data flow

```
NCBI FTP/API → omicidx-parsers (XML → Pydantic) → omicidx-dagster (→ Parquet on S3/R2)
                                                         ↓
                                              DuckDB views (010–050 SQL)
                                                         ↓
                                         omicidx.duckdb (public data file)
                                                         ↓
                                              PostgreSQL (via Dagster assets)
                                                         ↓
                                         omicidx-api (FastAPI REST endpoints)
```

### GitHub Actions

Workflows live in `packages/omicidx-etl/.github/workflows/` and run the ETL pipelines on a daily cron. AWS credentials and `PUBLISH_DIRECTORY` are injected as repository secrets.

## Environment / secrets

ETL requires a `.env` in `packages/omicidx-etl/` (loaded automatically by `python-dotenv` and `just`):

```
PUBLISH_DIRECTORY=s3://your-bucket/omicidx
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_ENDPOINT_URL=...        # for S3-compatible stores (Cloudflare R2, etc.)
AWS_URL_STYLE=path
AWS_REGION=...
```
