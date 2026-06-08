"""Parquet export flow: lake.<schema>.* → public Parquet (reverse-ETL).

The public-serving half of ADR-0004. Each core lake table is COPY'd out of
the DuckLake catalog to a flat Parquet file under the dedicated public bucket
(`PUBLIC_PARQUET_ROOT`, e.g. `r2://data-omicidx`) at `latest/<file>.parquet`.

This replaces the old `consolidate.py` path (raw → consolidated parquet) as
the source for `omicidx-duckdb-build` and the co-published `views.sql`: the
lake is now the single source of truth, and the export is a straight COPY of
the merged tables (no re-aggregation from raw).

Rolling-`latest/` only — overwritten each run. Versioned `vN.N/` snapshots
(ADR-0004) are a later follow-up that freezes `latest/` into an immutable
prefix.

The R2 secret built by `get_ducklake_connection()` is account-scoped, so the
COPY into a *different* bucket than the lake's `cdsci-lake` reuses it with no
extra credentials.
"""

from omicidx.prefect.config import get_ducklake_connection, get_public_parquet_path
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA

from prefect import flow, get_run_logger, task

# (lake table, public parquet filename). Names differ: lake tables are
# singular; the public files (and the 020 src_* views) use the plural form.
# Authoritative against sql/020_base_parquet_views.sql + the ducklake_* loaders.
EXPORTS: list[tuple[str, str]] = [
    ("bioproject", "bioprojects.parquet"),
    ("biosample", "biosamples.parquet"),
    ("geo_series", "geo_series.parquet"),
    ("geo_sample", "geo_samples.parquet"),
    ("geo_platform", "geo_platforms.parquet"),
    ("sra_study", "sra_studies.parquet"),
    ("sra_sample", "sra_samples.parquet"),
    ("sra_experiment", "sra_experiments.parquet"),
    ("sra_run", "sra_runs.parquet"),
    ("pubmed_article", "pubmed_articles.parquet"),
]


@task(retries=1, retry_delay_seconds=60)
def export_table(
    lake_table: str, filename: str, lake_schema: str = LAKE_SCHEMA
) -> dict:
    """COPY lake.<schema>.<lake_table> → <public root>/latest/<filename>."""
    log = get_run_logger()
    output = get_public_parquet_path("latest", filename)
    with get_ducklake_connection() as con:
        log.info(f"Exporting lake.{lake_schema}.{lake_table} → {output}")
        con.execute(
            f"COPY (SELECT * FROM lake.{lake_schema}.{lake_table}) "
            f"TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        row_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{output}')"
        ).fetchone()[0]
    log.info(f"Wrote {row_count:,} rows to {output}")
    return {
        "table": f"{lake_schema}.{lake_table}",
        "file": filename,
        "row_count": row_count,
    }


@flow(name="parquet-export")
def parquet_export_flow(lake_schema: str = LAKE_SCHEMA) -> None:
    """Export every core lake table to the public Parquet bucket (latest/).

    Sits between `ducklake-load` and `duckdb-build` in the daily pipeline:
    the build and the public `views.sql` read these files.
    """
    for lake_table, filename in EXPORTS:
        export_table(lake_table, filename, lake_schema=lake_schema)


if __name__ == "__main__":
    parquet_export_flow()
