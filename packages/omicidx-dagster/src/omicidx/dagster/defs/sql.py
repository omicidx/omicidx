"""DuckDB build asset.

Builds the omicidx.duckdb file from consolidated parquet views (020-050 SQL).
The consolidation step is now handled by per-entity assets in consolidate.py.
"""

import json
import shutil
from datetime import datetime
from pathlib import Path

import duckdb
import sqlglot
from omicidx.dagster.resources import DuckDBResource, OmicidxStorage

import dagster as dg

SQL_DIR = Path(__file__).parent.parent / "sql"


def _list_sql_files() -> list[str]:
    """List available SQL files in order."""
    return sorted(p.name for p in SQL_DIR.glob("*.sql"))


def _get_sql(name: str) -> str:
    """Load SQL file content by name."""
    path = SQL_DIR / name
    if not path.exists():
        available = ", ".join(_list_sql_files())
        raise FileNotFoundError(f"SQL file '{name}' not found. Available: {available}")
    return path.read_text()


def _q(value: str) -> str:
    """Escape a string for safe use in a SQL single-quoted literal."""
    return value.replace("'", "''")


def _run_sql_file(
    name: str,
    con: duckdb.DuckDBPyConnection,
    context: dg.AssetExecutionContext,
) -> None:
    """Run a SQL file, executing each statement individually."""
    sql = _get_sql(name)
    statements = sqlglot.transpile(sql, read="duckdb")
    context.log.info(f"Running {name} ({len(statements)} statements)")

    for i, stmt in enumerate(statements, 1):
        preview = stmt[:100].replace("\n", " ")
        context.log.info(f"  [{i}/{len(statements)}] {preview}...")
        con.execute(stmt)

    context.log.info(f"Completed {name}")


DB_FILE = "omicidx.duckdb"


@dg.asset(
    group_name="sql",
    kinds={"duckdb", "sql", "s3"},
    tags={
        "layer": "published",
        "cost": "medium",
        "sla": "daily",
        "source": "derived",
        "storage": "duckdb",
    },
    deps=[
        "bioproject_parquet",
        "biosample_parquet",
        "sra_studies_parquet",
        "sra_samples_parquet",
        "sra_experiments_parquet",
        "sra_runs_parquet",
        "sra_accessions_parquet",
        "geo_series_parquet",
        "geo_samples_parquet",
        "geo_platforms_parquet",
        "geo_rnaseq_counts_parquet",
        "pubmed_parquet",
    ],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def omicidx_duckdb(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
) -> dg.MaterializeResult:
    """Build the OmicIDX DuckDB database from view SQL files (020-050)."""
    db_path = Path(DB_FILE)
    if db_path.exists():
        db_path.unlink()

    with duckdb_res.get_connection(database=DB_FILE) as con:
        view_files = [f for f in _list_sql_files() if f >= "020"]
        context.log.info(f"Building {DB_FILE} from {len(view_files)} SQL files")

        for name in view_files:
            _run_sql_file(name, con, context)

        # Add creation metadata
        con.execute("DROP TABLE IF EXISTS db_creation_metadata")
        con.execute("CREATE TABLE db_creation_metadata AS SELECT now() AS created_at")

        # Gather table summaries
        summaries = []
        for schema in ["main", "geometadb", "sradb"]:
            try:
                tables = con.execute(
                    f"SELECT table_name FROM information_schema.tables "
                    f"WHERE table_schema = '{schema}'"
                ).fetchall()
            except Exception:
                continue

            for (table,) in tables:
                qualified = f"{schema}.{table}" if schema != "main" else table
                try:
                    count = con.execute(f"SELECT COUNT(*) FROM {qualified}").fetchone()[
                        0
                    ]
                except Exception:
                    count = -1
                summaries.append({"schema": schema, "table": table, "row_count": count})

        for s in summaries:
            qualified = (
                f"{s['schema']}.{s['table']}" if s["schema"] != "main" else s["table"]
            )
            context.log.info(f"  {qualified}: {s['row_count']:,} rows")

        # Write metadata JSON
        metadata = {
            "created_at": datetime.now().isoformat(),
            "tables": summaries,
        }
        metadata_path = db_path.with_suffix(".metadata.json")
        metadata_path.write_text(json.dumps(metadata, indent=2))

    # Upload to S3 if configured
    s3_path = storage.get_upath("duckdb", "omicidx.duckdb")
    context.log.info(f"Uploading {db_path} to {s3_path}")

    with db_path.open("rb") as src, s3_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)

    total_rows = sum(s["row_count"] for s in summaries if s["row_count"] > 0)

    return dg.MaterializeResult(
        metadata={
            "sql_files": dg.MetadataValue.text(", ".join(view_files)),
            "table_count": dg.MetadataValue.int(len(summaries)),
            "total_rows": dg.MetadataValue.int(total_rows),
            "s3_path": dg.MetadataValue.text(str(s3_path)),
        }
    )
