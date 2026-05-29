"""Configuration for omicidx-prefect.

Mirrors the three Dagster resources (OmicidxStorage, DuckDBResource,
PostgresResource) as a single pydantic-settings object plus per-domain
helper functions. Prefect flows pull these from module scope rather than
injecting them as resource parameters.
"""

import asyncio
import re
from contextlib import contextmanager
from functools import lru_cache
from urllib.parse import urlparse

import duckdb
import sqlglot
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from upath import UPath


class Settings(BaseSettings):
    """Environment-backed configuration.

    Env var names match the Dagster setup so the same .env works for
    both packages during transition.
    """

    publish_root: str
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_endpoint: str
    s3_region: str = "auto"
    s3_url_style: str = "path"
    postgres_uri: str | None = None
    ducklake_uri: str | None = None
    ducklake_data_path: str | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache(maxsize=1)
def settings() -> Settings:
    return Settings()


# ---------------------------------------------------------------------------
# Storage helpers (ports OmicidxStorage)
# ---------------------------------------------------------------------------


def storage_options() -> dict:
    s = settings()
    return {
        "key": s.s3_access_key_id,
        "secret": s.s3_secret_access_key,
        "endpoint_url": s.s3_endpoint,
        "client_kwargs": {"region_name": s.s3_region},
    }


def get_upath(*parts: str) -> UPath:
    """Build a UPath under the publish root (for fsspec/UPath operations)."""
    return UPath(settings().publish_root, *parts, **storage_options())


def get_duckdb_path(*parts: str) -> str:
    """Build an r2:// path for use in DuckDB SQL with the r2 secret."""
    upath = UPath(settings().publish_root, *parts)
    return str(upath).replace("s3://", "r2://", 1)


# ---------------------------------------------------------------------------
# DuckDB helpers (ports DuckDBResource)
# ---------------------------------------------------------------------------


def _q(value: str) -> str:
    return value.replace("'", "''")


def get_duckdb_connection(database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with the R2 secret pre-loaded."""
    s = settings()
    con = duckdb.connect(database)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET http_retries = 8;")
    con.execute("SET http_retry_wait_ms = 1000;")
    con.execute("SET http_retry_backoff = 2.0;")
    con.execute("SET http_keep_alive = true;")

    account_id = s.s3_endpoint.replace("https://", "").split(".")[0]
    sql = f"""
    CREATE OR REPLACE SECRET r2 (
        TYPE r2,
        KEY_ID '{_q(s.s3_access_key_id)}',
        SECRET '{_q(s.s3_secret_access_key)}',
        ACCOUNT_ID '{_q(account_id)}'
    );"""
    con.execute(sql)
    return con


def _parse_libpq(uri: str) -> dict[str, str]:
    """Parse a `postgres:key=val key=val ...` connection string.

    Mirrors the libpq-style string stored in DUCKLAKE_URI. Returns a dict
    keyed by the libpq keyword (host, port, dbname, user, password).
    """
    body = uri.split(":", 1)[1] if uri.startswith("postgres:") else uri
    params: dict[str, str] = {}
    for token in body.split():
        if "=" not in token:
            raise ValueError(f"Malformed DUCKLAKE_URI token: {token!r}")
        key, value = token.split("=", 1)
        params[key.strip().lower()] = value.strip()
    for required in ("host", "dbname", "user"):
        if required not in params:
            raise ValueError(f"DUCKLAKE_URI missing required key: {required!r}")
    return params


def get_ducklake_connection() -> duckdb.DuckDBPyConnection:
    """Attach the DuckLake catalog as `lake` and return the connection.

    Builds the three secrets the catalog needs as TEMPORARY (session)
    secrets so the flow is self-contained in any fresh worker:

    - `r2`      — R2 data access (created by get_duckdb_connection)
    - `pg_main` — postgres catalog metadata store (db `lake`)
    - `lake`    — the ducklake secret tying metadata + data_path together

    The catalog's own stored data_path governs existing tables; the
    DATA_PATH below only matters for first-time catalog init. The
    `cdsci-lake` bucket is ducklake-controlled exclusively — raw inputs
    are read from PUBLISH_ROOT (a different bucket), never written here.
    """
    s = settings()
    if not s.ducklake_uri:
        raise RuntimeError("DUCKLAKE_URI is not set")
    if not s.ducklake_data_path:
        raise RuntimeError("DUCKLAKE_DATA_PATH is not set")
    pg = _parse_libpq(s.ducklake_uri)

    con = get_duckdb_connection()  # httpfs + r2 secret already loaded
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"""
        CREATE OR REPLACE SECRET pg_main (
            TYPE postgres,
            HOST '{_q(pg["host"])}',
            PORT {int(pg.get("port", "5432"))},
            DATABASE '{_q(pg["dbname"])}',
            USER '{_q(pg["user"])}',
            PASSWORD '{_q(pg.get("password", ""))}'
        );""")
    con.execute(f"""
        CREATE OR REPLACE SECRET lake (
            TYPE ducklake,
            METADATA_PATH '',
            DATA_PATH '{_q(s.ducklake_data_path)}',
            METADATA_PARAMETERS MAP {{'TYPE': 'postgres', 'SECRET': 'pg_main'}}
        );""")
    con.execute("ATTACH 'ducklake:lake'")
    return con


# ---------------------------------------------------------------------------
# Postgres helpers (ports PostgresResource)
# ---------------------------------------------------------------------------


def _require_postgres_uri() -> str:
    uri = settings().postgres_uri
    if not uri:
        raise RuntimeError("POSTGRES_URI is not set")
    return uri


def postgres_async_uri() -> str:
    return _require_postgres_uri().replace(
        "postgresql://", "postgresql+asyncpg://", 1
    )


def _split_postgres_statements(sql: str) -> list[str]:
    if not sql.strip():
        raise ValueError("SQL statement cannot be empty")
    parsed = [
        expr.sql(dialect="postgres")
        for expr in sqlglot.parse(sql, read="postgres")
        if expr is not None
    ]
    if not parsed:
        raise ValueError(
            "SQL parsing produced no executable statements "
            "(may contain only comments/whitespace or be malformed)"
        )
    return parsed


def execute_postgres_sql(*statements: str) -> None:
    """Run SQL statements against Postgres via SQLAlchemy async + asyncpg."""

    async def _run() -> None:
        engine = create_async_engine(postgres_async_uri())
        async with engine.begin() as conn:
            for sql in statements:
                for stmt in _split_postgres_statements(sql):
                    await conn.execute(text(stmt))
        await engine.dispose()

    asyncio.run(_run())


@contextmanager
def attach_postgres(con: duckdb.DuckDBPyConnection, schema: str = "public"):
    """Attach the configured Postgres database to a DuckDB connection."""
    uri = _require_postgres_uri()
    parsed = urlparse(uri)
    if parsed.scheme != "postgresql" or not parsed.hostname:
        raise ValueError("POSTGRES_URI must be a valid postgresql:// URI")
    if any(ch in uri for ch in ("'", ";", "\n", "\r", "\x00")):
        raise ValueError("POSTGRES_URI contains unsupported characters")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError("Schema name must be a valid SQL identifier")

    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{_q(uri)}' AS pg (TYPE POSTGRES, SCHEMA '{_q(schema)}')")
    try:
        yield
    finally:
        con.execute("DETACH pg")
