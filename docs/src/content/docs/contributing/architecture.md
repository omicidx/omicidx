---
title: Architecture (contributor's view)
description: Repo layout, package boundaries, and where to make changes.
---

This page is for contributors hacking on the OmicIDX codebase. For the user-facing architecture overview, see [Overview / Architecture](/overview/architecture/).

## Repo layout

OmicIDX is a [uv workspace](https://docs.astral.sh/uv/concepts/workspaces/) with four packages, sharing the implicit `omicidx` namespace:

```
packages/
├── omicidx-parsers/   # XML/SOFT parsers + Pydantic v2 models
├── omicidx-etl/       # Click CLIs, DuckDB SQL bundles, ad-hoc extractors
├── omicidx-dagster/   # Dagster code location: assets, schedules, sensors
└── omicidx-api/       # FastAPI REST API
```

`docs/` is a sibling Astro/Starlight project, not a workspace member.

## Package boundaries

- **`omicidx-parsers`** — pure functions, no I/O, no external dependencies beyond stdlib + Pydantic + lxml. Imported by both `omicidx-etl` and `omicidx-dagster`.
- **`omicidx-etl`** — `oidx` CLI + the legacy pre-Dagster ETL flows. The `omicidx/etl/sql/*.sql` bundle here is a duplicate kept for the `oidx sql run` CLI; production reads from the dagster copy below. Dedupe is tracked in [#78](https://github.com/omicidx/omicidx/issues/78).
- **`omicidx-dagster`** — orchestration and the production source of truth for the consolidated DuckDB views. Per-source raw assets, per-entity consolidation assets, the `omicidx_duckdb` build asset (which reads SQL from `packages/omicidx-dagster/src/omicidx/dagster/sql/*.sql`), and per-entity Postgres loaders. Resources: `OmicidxStorage` (R2/S3), `DuckDBResource`, `PostgresResource`.
- **`omicidx-api`** — read-only FastAPI service. SQLAlchemy 2.0 + asyncpg, base64url cursor pagination, slowapi rate limiting.

## Where to make changes

- **New data source?** Add a parser to `omicidx-parsers`, raw + consolidation assets to `omicidx-dagster`, optionally a Postgres loader, optionally an API router.
- **Schema change?** Update the Postgres asset's DDL and the API's SQLAlchemy model in `omicidx-api/src/omicidx/api/models/tables.py`. The A/B view swap means existing API readers see a clean cutover when the next reload runs.
- **Pipeline cadence?** See [Automation cadence](/contributing/automation-cadence/).
- **API endpoint?** Add a router in `omicidx-api/src/omicidx/api/routers/`. The OpenAPI spec at `/openapi.json` regenerates automatically and the [API reference](/api/reference/) page reflects it on next page load.

## Local development

```bash
# Install all workspace packages
uv sync

# Run a specific package's tests
uv run pytest packages/omicidx-parsers/tests/
uv run pytest packages/omicidx-api/tests/

# Run the API locally
cd packages/omicidx-api && uv run uvicorn omicidx.api.main:app --reload

# Run the docs locally
cd docs && npm run dev
```

## CI

GitHub Actions in `.github/workflows/` and `packages/omicidx-etl/.github/workflows/`. PRs run lint + tests; merges to `main` trigger ETL workflows on cron.
