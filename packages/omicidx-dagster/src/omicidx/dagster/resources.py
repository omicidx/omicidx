"""Dagster resources for OmicIDX storage configuration."""

import dagster as dg
import duckdb
from upath import UPath


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
        """Build a UPath under the publish root."""
        return UPath(self.publish_root, *parts, **self.storage_options)


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
