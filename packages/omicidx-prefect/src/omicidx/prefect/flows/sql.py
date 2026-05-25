"""DuckDB build flow.

Builds the omicidx.duckdb file from consolidated parquet views
(020-050 SQL) and uploads it to R2/S3.
"""

import json
import shutil
from datetime import datetime
from pathlib import Path

import duckdb
import sqlglot
from omicidx.prefect.config import get_duckdb_connection, get_upath

from prefect import flow, get_run_logger, task

SQL_DIR = Path(__file__).parent.parent / "sql"
DB_FILE = "omicidx.duckdb"


def _list_sql_files() -> list[str]:
    return sorted(p.name for p in SQL_DIR.glob("*.sql"))


def _get_sql(name: str) -> str:
    path = SQL_DIR / name
    if not path.exists():
        available = ", ".join(_list_sql_files())
        raise FileNotFoundError(f"SQL file '{name}' not found. Available: {available}")
    return path.read_text()


def _run_sql_file(name: str, con: duckdb.DuckDBPyConnection) -> None:
    log = get_run_logger()
    sql = _get_sql(name)
    statements = sqlglot.transpile(sql, read="duckdb")
    log.info(f"Running {name} ({len(statements)} statements)")
    for i, stmt in enumerate(statements, 1):
        preview = stmt[:100].replace("\n", " ")
        log.info(f"  [{i}/{len(statements)}] {preview}...")
        con.execute(stmt)
    log.info(f"Completed {name}")


@task(retries=1, retry_delay_seconds=60)
def build_omicidx_duckdb() -> dict:
    """Build the omicidx.duckdb file and upload it to R2/S3."""
    log = get_run_logger()
    db_path = Path(DB_FILE)
    if db_path.exists():
        db_path.unlink()

    summaries: list[dict] = []
    with get_duckdb_connection(database=DB_FILE) as con:
        view_files = [f for f in _list_sql_files() if f >= "020"]
        log.info(f"Building {DB_FILE} from {len(view_files)} SQL files")

        for name in view_files:
            _run_sql_file(name, con)

        con.execute("DROP TABLE IF EXISTS db_creation_metadata")
        con.execute("CREATE TABLE db_creation_metadata AS SELECT now() AS created_at")

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
                    count = con.execute(
                        f"SELECT COUNT(*) FROM {qualified}"
                    ).fetchone()[0]
                except Exception:
                    count = -1
                summaries.append(
                    {"schema": schema, "table": table, "row_count": count}
                )

        for s in summaries:
            qualified = (
                f"{s['schema']}.{s['table']}"
                if s["schema"] != "main"
                else s["table"]
            )
            log.info(f"  {qualified}: {s['row_count']:,} rows")

        metadata = {
            "created_at": datetime.now().isoformat(),
            "tables": summaries,
        }
        metadata_path = db_path.with_suffix(".metadata.json")
        metadata_path.write_text(json.dumps(metadata, indent=2))

    s3_path = get_upath("duckdb", "omicidx.duckdb")
    log.info(f"Uploading {db_path} to {s3_path}")
    with db_path.open("rb") as src, s3_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)

    total_rows = sum(s["row_count"] for s in summaries if s["row_count"] > 0)
    return {
        "sql_files": ", ".join(_list_sql_files()),
        "table_count": len(summaries),
        "total_rows": total_rows,
        "s3_path": str(s3_path),
    }


@flow(name="omicidx-duckdb-build")
def omicidx_duckdb_flow() -> None:
    build_omicidx_duckdb()


if __name__ == "__main__":
    omicidx_duckdb_flow()
