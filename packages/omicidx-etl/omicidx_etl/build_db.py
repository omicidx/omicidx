"""Build the OmicIDX DuckDB database from SQL view definitions.

Creates a DuckDB file with views over the public parquet endpoints.
The views are defined in SQL files 020-050 in the sql/ directory.

Usage (CLI):
    # Build locally
    uv run oidx build-db

    # Build and upload to S3
    uv run oidx build-db --upload

    # Run specific SQL files
    uv run oidx build-db --files 020_base_parquet_views.sql 030_staging_views.sql
"""

import json
import shutil
from datetime import datetime
from pathlib import Path

import click
import duckdb

from omicidx_etl.sql import get_sql, list_sql_files
from omicidx_etl.log import logger

DB_FILE = "omicidx.duckdb"


def get_view_sql_files() -> list[str]:
    """List SQL files for view definitions (020+), excluding consolidation."""
    return [f for f in list_sql_files() if f >= "020"]


def run_sql_file(name: str, con: duckdb.DuckDBPyConnection) -> None:
    """Run a SQL file, executing each statement individually."""
    import sqlglot

    sql = get_sql(name)
    statements = sqlglot.transpile(sql, read="duckdb")

    logger.info(f"Running {name} ({len(statements)} statements)")

    for i, stmt in enumerate(statements, 1):
        preview = stmt[:100].replace("\n", " ")
        logger.info(f"  [{i}/{len(statements)}] {preview}...")
        con.execute(stmt)

    logger.info(f"Completed {name}")


def get_table_summaries(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Get row counts for all tables and views."""
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
                count = con.execute(f"SELECT COUNT(*) FROM {qualified}").fetchone()[0]
            except Exception:
                count = -1
            summaries.append({"schema": schema, "table": table, "row_count": count})

    return summaries


def build(files: list[str] | None = None) -> duckdb.DuckDBPyConnection:
    """Build the DuckDB database from SQL view files."""
    db_path = Path(DB_FILE)
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL httpfs; LOAD httpfs;")

    sql_files = files or get_view_sql_files()
    logger.info(f"Building {DB_FILE} from {len(sql_files)} SQL files...")

    for name in sql_files:
        run_sql_file(name, con)

    # Add metadata
    con.execute("DROP TABLE IF EXISTS db_creation_metadata")
    con.execute("CREATE TABLE db_creation_metadata AS SELECT now() AS created_at")

    # Print summaries
    summaries = get_table_summaries(con)
    logger.info(f"Database built with {len(summaries)} tables/views:")
    for s in summaries:
        qualified = (
            f"{s['schema']}.{s['table']}" if s["schema"] != "main" else s["table"]
        )
        logger.info(f"  {qualified}: {s['row_count']:,} rows")

    # Write metadata JSON
    metadata = {
        "created_at": datetime.now().isoformat(),
        "tables": summaries,
    }
    metadata_path = db_path.with_suffix(".metadata.json")
    metadata_path.write_text(json.dumps(metadata, indent=2))
    logger.info(f"Metadata written to {metadata_path}")

    return con


def upload(con: duckdb.DuckDBPyConnection) -> None:
    """Upload the DuckDB file to S3."""
    from upath import UPath

    s3_path = UPath("s3://omicidx/duckdb/omicidx.duckdb")
    local_path = Path(DB_FILE)

    s3_metadata = UPath("s3://omicidx/duckdb/metadata")
    s3_metadata.mkdir(parents=True, exist_ok=True)

    metadata_path = local_path.with_suffix(".metadata.json")
    ts = datetime.now().isoformat()

    con.close()

    logger.info(
        f"Uploading {local_path} ({local_path.stat().st_size / 1e6:.1f} MB) "
        f"to {s3_path}..."
    )
    with local_path.open("rb") as src, s3_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)

    s3_meta_dest = s3_metadata / f"{ts}_metadata.json"
    with metadata_path.open("rb") as src, s3_meta_dest.open("wb") as dst:
        shutil.copyfileobj(src, dst)

    logger.info(f"Uploaded to {s3_path}")
    logger.info(f"Metadata uploaded to {s3_meta_dest}")


@click.command("build-db")
@click.option("--upload-db", is_flag=True, help="Upload to S3 after building")
@click.argument("files", nargs=-1)
def build_db(upload_db: bool, files: tuple[str, ...]):
    """Build the OmicIDX DuckDB database from view SQL files.

    If no FILES specified, runs all view SQL files (020+).
    """
    file_list = list(files) if files else None
    con = build(file_list)

    if upload_db:
        upload(con)
    else:
        con.close()
        logger.info(f"Database ready: {DB_FILE}")
        logger.info("Run with --upload-db to push to S3")
