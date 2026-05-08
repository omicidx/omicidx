# omicidx-dagster coding style

The implicit conventions in this code location, written down so they
stop being implicit. **Explicit, asset-per-entity, convention-over-magic**
is the through-line: no IOManagers, no `dg` components system, resources
injected by name, all storage operations spelled out. If a Dagster
feature lets you skip writing things down, we usually don't use it.

## Imports

- Always `import dagster as dg`. Never `from dagster import ...` for
  user code (it's fine inside `factories.py` for forward refs).
- No `from __future__ import annotations` in `defs/*.py` — Dagster's
  decorator validation resolves type hints at decoration time, and
  stringified annotations have caused load failures on some versions.
  The one exception is `factories.py`, which is a leaf utility module.
- Imports at module scope only. No imports inside functions. No `import`
  inside a hot loop.

## Asset declaration

- One asset per entity per layer. Assets are top-level functions
  decorated with `@dg.asset`. Use `@dg.multi_asset` only when one
  computation legitimately produces several entities (e.g., `geo_raw`
  emits gse / gsm / gpl together).
- `dg.AssetSpec` is for **external** assets (sources outside our
  control, like a remote FTP file). Keep it in `factories.py` and the
  module that wires the spec into a sensor.
- Helper functions are private (`_` prefix), live at module scope,
  and stay in the module that calls them. No central `utils.py`.

## Group naming

`group_name` is the entity domain name (or pipeline stage), in lower
snake_case. Current vocabulary:

| group_name | meaning |
|---|---|
| `sra`, `geo`, `biosample`, `pubmed`, `ebi_biosample` | per-entity raw extraction |
| `consolidate` | DuckDB-backed parquet consolidation (per-entity) |
| `postgres` | postgres load assets (per-entity) |
| `sql` | the unified DuckDB build (`omicidx_duckdb`) |

Add a new group only when you have a new domain or a new pipeline
stage — not for one-off categorization.

## Tags vocabulary

Tags are the observability surface. Stick to the keys below; add a new
key only after discussion. Values are closed sets — don't invent new
ones casually.

| key | values |
|---|---|
| `layer` | `raw`, `consolidated`, `serving`, `published` |
| `cost` | `low`, `medium`, `high` |
| `sla` | `daily`, `monthly` |
| `source` | `ncbi_ftp`, `ncbi_api`, `ebi_api`, `pubmed_ftp`, `derived`, `remote` |
| `storage` | `json`, `jsonl`, `parquet`, `duckdb`, `postgres` |

Reusable bundles (e.g., `_CONSOLIDATE_TAGS` in `consolidate.py`) live
near the assets that use them.

## Kinds vocabulary

`kinds={...}` describes the technical kinds Dagster surfaces in the UI.
Use the smallest set that's true. Common combinations:

- `{"python"}` — pure Python work, no external storage write
- `{"python", "json"}` / `{"python", "jsonl"}` — XML/JSON ingest
- `{"python", "parquet", "s3"}` — extracts that write parquet to R2/S3
- `{"duckdb", "parquet", "s3"}` — DuckDB-driven consolidation
- `{"duckdb", "sql", "s3"}` — the omicidx_duckdb build
- `{"postgres"}` — postgres loads

## Automation

- Prefer `dg.AutomationCondition` over `ScheduleDefinition` for asset-
  level triggers. Schedules exist for jobs that span multiple assets
  (`define_asset_job` + `ScheduleDefinition`).
- Cron strings are always 5-field crontab (`"0 2 * * *"`), never
  shorthand like `"@daily"`. Shorthand mixes with explicit crons make
  staggering hard to see.
- Standard staggering for nightly cascade:

| time | layer |
|---|---|
| `0 2 * * *` | raw extracts |
| `0 3 * * *` | per-entity parquet consolidation |
| `0 4 * * *` | postgres loads |
| `0 5 * * *` | omicidx.duckdb build |

- For change-driven assets, prefer `etag_change_sensor` (in
  `factories.py`) plus `dg.AutomationCondition.any_deps_updated()`
  over blind cron polling.

## Retry policy

| asset class | retry policy |
|---|---|
| raw extracts (network-dependent) | `RetryPolicy(max_retries=2, delay=30)` |
| consolidate / postgres / sql | `RetryPolicy(max_retries=1, delay=60)` |

Don't deviate without a reason in a comment.

## Resources

- `OmicidxStorage` — S3/R2 paths via `get_upath()` (fsspec) or
  `get_duckdb_path()` (DuckDB SQL with the `r2:` secret). Never mix:
  use UPath for fsspec/Python writes, use the duckdb path for SQL
  string interpolation.
- `DuckDBResource` — DuckDB connections with the R2 secret pre-loaded.
  Always use as a context manager.
- `PostgresResource` — `execute_sql()` for DDL/DML via asyncpg, or
  `attach()` as a DuckDB attachment for cross-system reads.

Resource parameter names are fixed by the `Definitions` registration
in `definitions.py` and are reused throughout:

| parameter | resource |
|---|---|
| `storage` | `OmicidxStorage` |
| `duckdb_res` | `DuckDBResource` |
| `postgres` | `PostgresResource` |

## SQL execution

- **DuckDB**: f-string the query. Path values come from
  `storage.get_duckdb_path(...)` and are trusted; literal user input
  must be escaped with the `_q()` pattern from `resources.py`.
  All COPY statements include `(FORMAT PARQUET, COMPRESSION ZSTD)`.
- **Postgres**: never use raw multi-statement strings with asyncpg. Run
  through `PostgresResource.execute_sql()` which splits with `sqlglot`.
  Identifiers must match `[A-Za-z_][A-Za-z0-9_]*` — see the validation
  in `resources.py`.

## Logging

- `context.log.info(f"...")` is the default. Use `.warning` for
  degraded-but-continuing situations, `.error` for cases where you
  return early without a Materialize.
- Format integers with `{n:,}` for readability.
- Log progress at chunk boundaries (every N records flushed) — not
  every record.
- No structured/JSON logging. Messages are human-readable narrative.

## Materialize results

Every asset returns `dg.MaterializeResult(metadata={...})`. Standard
metadata keys (use these names; add new ones only as needed):

| key | type | when |
|---|---|---|
| `row_count` | int | always (or 0 if no rows) |
| `output_path` / `s3_path` | text | wherever output landed |
| `source_url` | url | for assets that pull from a known URL |
| `parquet_parts` | int | for chunked-parquet writes |
| `files_processed` | int | for assets iterating multiple sources |
| `partition_date` | text | for partitioned daily/monthly assets |

## Path layout in storage

Stable, hive-style path conventions:

```
{publish_root}/
  sra/
    raw/{entity}/date={YYYY-MM-DD}/stage={Full|Incremental}/data_{NNNNN}.parquet
    parquet/{sra_studies,sra_samples,...}.parquet
  geo/
    raw/{gse,gsm,gpl}/year={YYYY}/month={MM}/...
    parquet/geo_{series,samples,platforms,...}.parquet
  biosample/
    raw/data.jsonl.gz
    parquet/biosamples.parquet
  pubmed/
    raw/{file}.xml.gz
    parquet/pubmed.parquet
  duckdb/
    omicidx.duckdb
```

Don't introduce a new layout without updating both the writer and the
SQL views (`packages/omicidx-dagster/src/omicidx/dagster/sql/`).

## Validation before deploy

- `uv run --package omicidx-dagster pytest packages/omicidx-dagster/tests/test_definitions.py`
  — fast smoke test, runs in CI.
- Pre-commit hook runs `dagster definitions validate` on changes to
  `packages/omicidx-dagster/src/**/*.py`.
- After editing source on the deployed dev compose, reload via
  `scripts/dagster-reload.sh omicidx`.

## What we don't do

- **No IOManagers.** Storage is explicit.
- **No `dg` CLI / components system.** Plain `Definitions(...)` wiring.
- **No `from __future__ import annotations` in defs files.**
- **No private/inline imports** except where absolutely necessary
  (deferred heavy-import workarounds).
- **No nested `Config` classes** for asset config — use resources.
- **No silent fallbacks** for misconfiguration. Raise loudly; let the
  daemon log it.
