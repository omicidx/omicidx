# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture and design

For the system architecture, design decisions (DuckDB for ELT, ClickHouse/StarRocks for serving, Dagster code location pattern, FastAPI on Cloud Run, etc.), and migration plan, see [docs/architecture.md](docs/architecture.md). That document is the canonical source of design context — start there before making cross-cutting changes.

## Repo structure

This is a **uv workspace** consolidating three packages:

```
omicidx/                        # workspace root (no package of its own)
├── pyproject.toml              # workspace root: members = ["packages/*"]
├── uv.lock
└── packages/
    ├── omicidx-parsers/        # XML parsers + Pydantic models for NCBI SRA, GEO, BioSample
    │   └── src/omicidx/parsers/
    └── omicidx-etl/            # ETL pipelines that extract raw data to S3/R2 as Parquet
        └── src/omicidx/etl/
```

Both packages share the `omicidx` **namespace package** (PEP 420 implicit — no `__init__.py` at `src/omicidx/`). `omicidx-etl` depends on `omicidx-parsers` as a workspace-local reference (`tool.uv.sources`).

## Commands

All commands run from the workspace root.

```bash
# Install all workspace packages and dependencies
uv sync

# Run tests (parsers has network-hitting tests against live NCBI APIs)
uv run pytest packages/omicidx-parsers/tests/
uv run pytest packages/omicidx-etl/tests/

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

### Data flow

```
NCBI FTP/API → omicidx-parsers (XML → Pydantic) → omicidx-etl (→ Parquet on S3/R2)
                                                         ↓
                                              DuckDB views (010–050 SQL)
                                                         ↓
                                         omicidx.duckdb (public data file)
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
