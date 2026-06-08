# omicidx-prefect

Prefect 3 flows for OmicIDX ETL — the same pipeline as `omicidx-dagster`,
re-implemented on top of Prefect with **semaphore-file partitioning** in
place of Dagster's built-in partition state.

## Why semaphore files

Dagster tracks partition completion in its own Postgres-backed event
log. That ties the pipeline's "what's done" state to the orchestrator's
metadata DB. Prefect doesn't model partitions natively, and we don't
want to lean on Prefect's run history for it either.

Instead, each partition writes a small JSON marker — a *semaphore* — to
the same storage bucket as the data:

```
{PUBLISH_ROOT}/_semaphores/
  sra/
    study/
      2024-09-12_Full.json
      2024-09-13_Incremental.json
      ...
    sample/...
    experiment/...
    run/...
  geo/
    2024-01.json
    2024-02.json
    ...
  pubmed/
    pubmed25n0001.json
    pubmed25n0002.json
    ...
  biosample/
    2024-09-13.json
  bioproject/
    2024-09-13.json
  ebi_biosample/
    2024-09-13.json
    ...
  sra_accessions_etag/
    latest.json   # stores the most-recently-seen ETag
```

Each semaphore file is `~200 bytes` of JSON: completion timestamp +
caller-supplied metadata (row_count, output_path, etc.).

**Rules:**

- A flow processes a partition only if its semaphore is missing (or
  `force=True` is passed).
- After the partition output is durably written, the flow writes the
  semaphore.
- Backfills are "delete the semaphores you want to redo, then re-run":

  ```bash
  omicidx-prefect semaphores clear sra/study --all          # whole entity
  omicidx-prefect semaphores clear pubmed pubmed25n0042     # one file
  ```
- Inspect with `omicidx-prefect semaphores list <namespace>` and
  `omicidx-prefect semaphores show <namespace> <key>`.

The current-period partition (GEO current month, EBI current day) is
always re-run by default, because upstream data accumulates within the
period. Pass `rerun_current_month=False` / `rerun_current_day=False`
to skip even those.

## Layout

```
packages/omicidx-prefect/
├── pyproject.toml
├── prefect.yaml             # deployments (schedules)
├── Dockerfile               # worker image
├── docker-compose.yml       # worker (joins shared monode prefect-server)
├── src/omicidx/prefect/
│   ├── config.py            # Settings + storage / duckdb / postgres helpers
│   ├── semaphore.py         # SemaphoreStore
│   ├── cli.py               # `omicidx-prefect` operator CLI
│   ├── flows/
│   │   ├── sra.py
│   │   ├── geo.py
│   │   ├── biosample.py
│   │   ├── pubmed.py
│   │   ├── ebi_biosample.py
│   │   ├── consolidate.py
│   │   ├── postgres.py
│   │   ├── sql.py
│   │   └── main.py          # daily_pipeline_flow
│   └── sql/                 # DuckDB view SQL (020–050)
└── tests/
```

## Quick start

```bash
# 1) From the workspace root
uv sync

# 2) Run a flow directly (no scheduler)
uv run omicidx-prefect run sra
uv run omicidx-prefect run geo --start-month 2024-01
uv run omicidx-prefect run pubmed
uv run omicidx-prefect run daily

# 3) Inspect semaphores
uv run omicidx-prefect semaphores list sra/study
uv run omicidx-prefect semaphores show pubmed pubmed25n0001
```

## Prefect worker

The Prefect server + UI + API is the **shared** instance from monode
`infrastructure/compose/prefect` (container `prefect-server`, backed by
pg_main, reached over the tailnet). This package ships a **worker only**:
`docker-compose.yml` builds the omicidx worker image and joins the shared
`pg_main_stack_default` network, which is how it reaches both
`prefect-server:4200` (the API) and `pg_main:5432` (the DuckLake catalog
+ serving Postgres).

```bash
cd packages/omicidx-prefect

# .env supplies S3_*, PUBLISH_ROOT, POSTGRES_URI, DUCKLAKE_URI, DUCKLAKE_DATA_PATH
cp .env.example .env  # (create one if you haven't)

# build + start the worker (shared prefect-server must already be running)
docker compose up -d --build

# register/refresh deployments (schedules) on the shared server
docker compose exec worker prefect deploy --all
docker compose exec worker prefect deployment ls
```

The pipeline is `raw-extract → ducklake-load → parquet-export →
postgres-load → duckdb-build`; schedules live in `prefect.yaml`.
`parquet-export` is the reverse-ETL (lake → public Parquet, ADR-0004).

## Mapping from Dagster

| Dagster concept                  | Prefect equivalent here                                |
|----------------------------------|--------------------------------------------------------|
| `@dg.asset`                      | `@task` inside a `@flow`                               |
| `dg.Definitions(...)`            | `prefect.yaml` (deployments)                           |
| `dg.ScheduleDefinition`          | `schedules:` in `prefect.yaml`                         |
| `StaticPartitionsDefinition`     | semaphore namespace per static value                   |
| `MonthlyPartitionsDefinition`    | `_enumerate_months()` + semaphore per `YYYY-MM` key    |
| `DailyPartitionsDefinition`      | `_enumerate_days()` + semaphore per `YYYY-MM-DD` key   |
| `DynamicPartitionsDefinition`    | flow lists the source itself, gates by semaphore       |
| `OmicidxStorage` resource        | `config.get_upath()` / `config.get_duckdb_path()`      |
| `DuckDBResource` resource        | `config.get_duckdb_connection()`                       |
| `PostgresResource` resource      | `config.execute_postgres_sql()` / `attach_postgres()`  |
| ETag-change sensor               | `sra_accessions_if_changed` task (semaphore stores ETag)|
| `dg.AutomationCondition`         | sequential subflows in `daily_pipeline_flow`           |

## Environment variables

Same as omicidx-dagster:

```
PUBLISH_ROOT=s3://omicidx
S3_ACCESS_KEY_ID=...
S3_SECRET_ACCESS_KEY=...
S3_ENDPOINT=https://....r2.cloudflarestorage.com
S3_REGION=auto
S3_URL_STYLE=path
POSTGRES_URI=postgresql://omicidx@host:5432/omicidx

# Public Parquet export (reverse-ETL; ADR-0004)
PUBLIC_PARQUET_ROOT=r2://data-omicidx                       # dedicated public bucket
PUBLIC_PARQUET_HTTPS_BASE=https://data-omicidx.cancerdatasci.org  # base for views.sql URLs
```

## Tests

```bash
uv run pytest packages/omicidx-prefect/tests/
```

## Operational notes

- **Concurrency**: each flow uses `ThreadPoolTaskRunner(max_workers=...)`.
  Tune per flow if a source is rate-limited (GEO uses 2 to be polite to
  the eutils API).
- **Retries**: raw extract tasks have `retries=2, retry_delay_seconds=60`;
  consolidate / postgres / sql tasks have `retries=1, retry_delay_seconds=60`.
- **Failure semantics**: if a partition task fails after retries, the
  semaphore is **not** written — the next run picks it up automatically.
- **Force re-run**: every flow accepts a `force: bool = False` parameter
  that bypasses semaphores.
