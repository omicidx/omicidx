"""Configuration for omicidx-prefect.

Mirrors the three Dagster resources (OmicidxStorage, DuckDBResource,
PostgresResource) as a single pydantic-settings object plus per-domain
helper functions. Prefect flows pull these from module scope rather than
injecting them as resource parameters.
"""

import asyncio
import os
import re
import tempfile
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
    # Public Parquet export (reverse-ETL target; ADR-0004). The dedicated
    # public bucket, separate from PUBLISH_ROOT (raw) and cdsci-lake (lake).
    public_parquet_root: str | None = None  # e.g. r2://data-omicidx
    public_parquet_https_base: str | None = (
        None  # e.g. https://data-omicidx.cancerdatasci.org
    )
    # DuckDB per-connection resource caps. Important when several
    # connections run concurrently (e.g. parallel postgres-load): leave
    # unset and each connection grabs most of RAM / all cores, so N in
    # parallel oversubscribe and OOM. Set memory_limit ~= RAM/concurrency
    # and threads ~= cores/concurrency.
    duckdb_memory_limit: str | None = None  # e.g. "32GB"
    duckdb_threads: int | None = None  # e.g. 16
    # Parallelism for postgres-load (independent per-entity table loads).
    postgres_load_concurrency: int = 4

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


def get_public_parquet_path(*parts: str) -> str:
    """Build an r2:// path under the public parquet root for DuckDB COPY.

    The R2 secret created in `get_duckdb_connection()` is account-scoped
    (no SCOPE), so a COPY to this bucket reuses it — no extra credentials.
    """
    root = settings().public_parquet_root
    if not root:
        raise RuntimeError("PUBLIC_PARQUET_ROOT is not set")
    # UPath has no r2 filesystem; build with s3:// then emit r2:// for DuckDB
    # (same trick as get_duckdb_path). Accepts an s3:// or r2:// root.
    root_s3 = root.replace("r2://", "s3://", 1)
    upath = UPath(root_s3, *parts)
    return str(upath).replace("s3://", "r2://", 1)


# ---------------------------------------------------------------------------
# DuckDB helpers (ports DuckDBResource)
# ---------------------------------------------------------------------------


def _q(value: str) -> str:
    return value.replace("'", "''")


def get_duckdb_connection(database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with the R2 secret pre-loaded.

    Point `secret_directory` at a dedicated, app-owned path instead of the
    default `~/.duckdb/stored_secrets`. The flow only ever creates
    TEMPORARY (in-memory) r2/pg_main/lake secrets, so this directory stays
    empty — which sidesteps two failures caused by a stale persistent
    `pg_main` secret left in the default store by interactive catalog
    bootstrap:

    - DuckLake ATTACH: "Ambiguity detected for secret name 'pg_main',
      secret occurs in multiple storage backends" (temp + persistent).
    - Postgres ATTACH: "Unknown secret storage found: 'local_file'" — the
      postgres secret scan reads the persisted file's storage tag.

    An isolated empty directory means neither stale file is ever seen, so
    persistent secrets can stay enabled (disabling them is the wrong lever
    — it still scans the file and throws the local_file error).
    """
    s = settings()
    secret_dir = os.path.join(tempfile.gettempdir(), "omicidx-duckdb-secrets")
    os.makedirs(secret_dir, exist_ok=True)
    con = duckdb.connect(database, config={"secret_directory": secret_dir})
    if s.duckdb_memory_limit:
        con.execute(f"SET memory_limit = '{_q(s.duckdb_memory_limit)}';")
    if s.duckdb_threads:
        con.execute(f"SET threads = {int(s.duckdb_threads)};")
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
    return _require_postgres_uri().replace("postgresql://", "postgresql+asyncpg://", 1)


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
