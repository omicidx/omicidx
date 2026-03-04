#!/usr/bin/env python3
"""Build the OmicIDX DuckDB database from SQL view definitions.

Usage:
    # Build locally
    uv run build_db.py

    # Build and upload to S3
    uv run build_db.py --upload

    # Run specific SQL files
    uv run build_db.py --files 020_base_parquet_views.sql 030_staging_views.sql
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import sqlglot


SQL_DIR = Path(__file__).parent / "sql"
DB_FILE = "omicidx.duckdb"


def list_sql_files() -> list[str]:
    """List available SQL files in order."""
    return sorted(p.name for p in SQL_DIR.glob("*.sql"))


def get_sql(name: str) -> str:
    """Load SQL file content by name."""
    path = SQL_DIR / name
    if not path.exists():
        available = ", ".join(list_sql_files())
        raise FileNotFoundError(f"SQL file '{name}' not found. Available: {available}")
    return path.read_text()


def run_sql_file(name: str, con: duckdb.DuckDBPyConnection) -> None:
    """Run a SQL file, executing each statement individually."""
    sql = get_sql(name)
    statements = sqlglot.transpile(sql, read="duckdb")

    print(f"  Running {name} ({len(statements)} statements)")

    for i, stmt in enumerate(statements, 1):
        preview = stmt[:100].replace("\n", " ")
        print(f"    [{i}/{len(statements)}] {preview}...")
        con.execute(stmt)

    print(f"  Completed {name}")


def get_table_summaries(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Get row counts for all tables and views."""
    summaries = []

    print("Performing table summaries")
    for schema in ["main", "geometadb", "sradb"]:
        try:
            tables = con.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
            ).fetchall()
        except Exception:
            continue

        for (table,) in tables:

            qualified = f"{schema}.{table}" if schema != "main" else table
            print(f"    Table summary for {qualified}")
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {qualified}").fetchone()[0]
            except Exception:
                count = -1
            summaries.append({"schema": schema, "table": table, "row_count": count})
    
    print("All table summaries complete")
    return summaries


def build(files: list[str] | None = None) -> duckdb.DuckDBPyConnection:
    """Build the DuckDB database from SQL files."""
    # Remove existing DB to start fresh
    db_path = Path(DB_FILE)
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(DB_FILE)
    con.execute("INSTALL httpfs; LOAD httpfs;")

    sql_files = files or list_sql_files()
    print(f"Building {DB_FILE} from {len(sql_files)} SQL files...")

    for name in sql_files:
        run_sql_file(name, con)

    # Add metadata
    con.execute("DROP TABLE IF EXISTS db_creation_metadata")
    con.execute("CREATE TABLE db_creation_metadata AS SELECT now() AS created_at")

    # Print summaries
    summaries = get_table_summaries(con)
    print(f"\nDatabase built with {len(summaries)} tables/views:")
    for s in summaries:
        qualified = f"{s['schema']}.{s['table']}" if s["schema"] != "main" else s["table"]
        print(f"  {qualified}: {s['row_count']:,} rows")

    # Write metadata JSON alongside the DB
    metadata = {
        "created_at": datetime.now().isoformat(),
        "tables": summaries,
    }
    metadata_path = db_path.with_suffix(".metadata.json")
    metadata_path.write_text(json.dumps(metadata, indent=2))
    print(f"\nMetadata written to {metadata_path}")

    return con


def upload(con: duckdb.DuckDBPyConnection) -> None:
    """Upload the DuckDB file to S3."""
    from upath import UPath
    import shutil

    s3_path = UPath("s3://omicidx/duckdb/omicidx.duckdb")
    local_path = Path(DB_FILE)

    # Also upload metadata
    s3_metadata = UPath("s3://omicidx/duckdb/metadata")
    s3_metadata.mkdir(parents=True, exist_ok=True)

    metadata_path = local_path.with_suffix(".metadata.json")
    ts = datetime.now().isoformat()

    # Close connection before uploading
    con.close()

    print(f"\nUploading {local_path} ({local_path.stat().st_size / 1e6:.1f} MB) to {s3_path}...")
    with local_path.open("rb") as src, s3_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)

    s3_meta_dest = s3_metadata / f"{ts}_metadata.json"
    with metadata_path.open("rb") as src, s3_meta_dest.open("wb") as dst:
        shutil.copyfileobj(src, dst)

    print(f"Uploaded to {s3_path}")
    print(f"Metadata uploaded to {s3_meta_dest}")


def main():
    parser = argparse.ArgumentParser(description="Build the OmicIDX DuckDB database")
    parser.add_argument(
        "--upload", action="store_true", help="Upload to S3 after building"
    )
    parser.add_argument(
        "--files", nargs="*", help="Specific SQL files to run (default: all)"
    )
    args = parser.parse_args()

    con = build(args.files)

    if args.upload:
        upload(con)
    else:
        con.close()
        print(f"\nDatabase ready: {DB_FILE}")
        print("Run with --upload to push to S3")


if __name__ == "__main__":
    main()
