"""Dagster resources for OmicIDX storage configuration."""

import asyncio
import re
from contextlib import contextmanager
from urllib.parse import urlparse

import duckdb
import sqlglot
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from upath import UPath

import dagster as dg


class OmicidxStorage(dg.ConfigurableResource):
    """Configurable resource for R2/S3 storage.

    Env var names align with the compose convention in the monode
    infrastructure repo (S3_ENDPOINT, S3_ACCESS_KEY_ID, etc.).
    """

    publish_root: str = dg.EnvVar("PUBLISH_ROOT")
    s3_access_key_id: str = dg.EnvVar("S3_ACCESS_KEY_ID")
    s3_secret_access_key: str = dg.EnvVar("S3_SECRET_ACCESS_KEY")
    s3_endpoint: str = dg.EnvVar("S3_ENDPOINT")
    s3_url_style: str = dg.EnvVar("S3_URL_STYLE")
    s3_region: str = dg.EnvVar("S3_REGION")

    @property
    def storage_options(self) -> dict:
        """S3/R2 credentials for fsspec/UPath."""
        return {
            "key": self.s3_access_key_id,
            "secret": self.s3_secret_access_key,
            "endpoint_url": self.s3_endpoint,
            "client_kwargs": {
                "region_name": self.s3_region,
            },
        }

    def get_upath(self, *parts: str) -> UPath:
        """Build a UPath under the publish root (for fsspec/UPath operations)."""
        return UPath(self.publish_root, *parts, **self.storage_options)

    def get_duckdb_path(self, *parts: str) -> str:
        """Build an r2:// path for use in DuckDB SQL with an R2-type secret."""
        upath = UPath(self.publish_root, *parts)
        return str(upath).replace("s3://", "r2://", 1)


class DuckDBResource(dg.ConfigurableResource):
    """Resource for DuckDB with R2/S3 connectivity."""

    s3_access_key_id: str = dg.EnvVar("S3_ACCESS_KEY_ID")
    s3_secret_access_key: str = dg.EnvVar("S3_SECRET_ACCESS_KEY")
    s3_endpoint: str = dg.EnvVar("S3_ENDPOINT")
    s3_region: str = dg.EnvVar("S3_REGION")

    def get_connection(self, database: str = ":memory:") -> duckdb.DuckDBPyConnection:
        """Create a DuckDB connection with R2 credentials."""
        con = duckdb.connect(database)
        con.execute("INSTALL httpfs; LOAD httpfs;")

        # Cloudflare R2 specific ACCOUNT_ID extraction from endpoint
        account_id = self.s3_endpoint.replace("https://", "").split(".")[0]

        def _q(value: str) -> str:
            return value.replace("'", "''")

        sql = f"""
        CREATE OR REPLACE SECRET r2 (
            TYPE r2,
            KEY_ID '{_q(self.s3_access_key_id)}',
            SECRET '{_q(self.s3_secret_access_key)}',
            ACCOUNT_ID '{_q(account_id)}'
        );"""
        con.execute(sql)
        return con


class PostgresResource(dg.ConfigurableResource):
    """Resource for PostgreSQL connectivity.

    Set POSTGRES_URI in the environment, e.g.:
        postgresql://omicidx:secret@pg_duckdb_18:5432/omicidx
    """

    uri: str = dg.EnvVar("POSTGRES_URI")

    @property
    def async_uri(self) -> str:
        """Convert standard postgresql:// to asyncpg driver URI."""
        return self.uri.replace("postgresql://", "postgresql+asyncpg://", 1)

    def execute_sql(self, *statements: str) -> None:
        """Run SQL statements against Postgres via SQLAlchemy async + asyncpg.

        Each input string is parsed with sqlglot to split multi-statement
        SQL into individual statements, since asyncpg cannot execute
        multiple statements in a single prepared statement call.
        """

        async def _run():
            engine = create_async_engine(self.async_uri)
            async with engine.begin() as conn:
                for sql in statements:
                    for parsed in sqlglot.transpile(sql, read="postgres"):
                        await conn.execute(text(parsed))
            await engine.dispose()

        asyncio.run(_run())

    @contextmanager
    def attach(self, con: duckdb.DuckDBPyConnection, schema: str = "public"):
        """Attach this Postgres database to a DuckDB connection."""
        parsed = urlparse(self.uri)
        if parsed.scheme != "postgresql" or not parsed.hostname:
            raise ValueError("POSTGRES_URI must be a valid postgresql:// URI")
        if any(ch in self.uri for ch in ("'", ";", "\n", "\r", "\x00")):
            raise ValueError("POSTGRES_URI contains unsupported characters")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
            raise ValueError("Schema name must be a valid SQL identifier")

        def _q(value: str) -> str:
            return value.replace("'", "''")

        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(
            f"ATTACH '{_q(self.uri)}' AS pg (TYPE POSTGRES, SCHEMA '{_q(schema)}')"
        )
        try:
            yield
        finally:
            con.execute("DETACH pg")
