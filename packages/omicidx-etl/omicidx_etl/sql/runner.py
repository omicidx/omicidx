"""Run raw-to-parquet SQL consolidation using DuckDB.

This module consolidates raw extracted data (ndjson, csv, parquet shards)
into single parquet files on R2. Only runs consolidation files (010_*).

For building the user-facing DuckDB database (views 020-050), use:
    uv run oidx build-db

Usage (CLI):
    # Run parquet consolidation
    uv run oidx sql run

    # List available SQL files
    uv run oidx sql list
"""

import os

from ..log import logger

import click
import duckdb

from omicidx_etl.sql import get_sql, list_sql_files


def get_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with R2 credentials."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")

    try:
        aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
        aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        aws_endpoint_url = (
            os.environ["AWS_ENDPOINT_URL"].replace("https://", "").split(".")[0]
        )
        sql = f"""
create or replace secret r2 (
    TYPE r2,
    KEY_ID '{aws_access_key_id}',
    SECRET '{aws_secret_access_key}',
    ACCOUNT_ID '{aws_endpoint_url}'
);"""
        con.execute(sql)
        logger.info("R2 secret created successfully")
        return con
    except KeyError as e:
        logger.error(f"Missing AWS environment variable: {e}")
        raise


def run_sql_file(
    name: str,
    con: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyConnection:
    """Run a SQL file, executing each statement individually."""
    import sqlglot

    if con is None:
        con = get_connection()

    sql = get_sql(name)
    statements = sqlglot.transpile(sql, read="duckdb")

    logger.info(f"Running SQL file: {name} ({len(statements)} statements)")

    for i, stmt in enumerate(statements, 1):
        preview = stmt[:120].replace("\n", " ")
        logger.info(f"[{name}] Statement {i}/{len(statements)}: {preview}")
        con.execute(stmt)
        logger.info(f"[{name}] Statement {i}/{len(statements)} completed")

    logger.info(f"Completed SQL file: {name}")
    return con


@click.group()
def sql():
    """Run raw-to-parquet SQL consolidation."""
    pass


@sql.command("list")
def list_cmd():
    """List available SQL files."""
    click.echo("Available SQL files:")
    for name in list_sql_files():
        click.echo(f"  {name}")


@sql.command("run")
@click.argument("files", nargs=-1)
def run_cmd(files: tuple[str, ...]):
    """Run SQL consolidation files.

    If no FILES specified, runs all SQL files in order.
    """
    con = get_connection()

    # Default to consolidation files only (010_*); view files (020+) are
    # handled by the build-db command.
    sql_files = (
        list(files)
        if files
        else [f for f in list_sql_files() if f < "020"]
    )
    for name in sql_files:
        run_sql_file(name, con=con)

    logger.info("SQL consolidation completed")
