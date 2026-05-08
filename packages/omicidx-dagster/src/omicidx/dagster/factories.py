"""Reusable asset/sensor factories for remote-file ingestion.

Harvested from monode/projects/dags/src/dags/defs/sra/factories.py and
biosample/sensors.py, adapted to omicidx-dagster's explicit Definitions
pattern (no IOManagers, no dg components).

The two main building blocks:

- ``etag_change_sensor`` — a standalone sensor that HEADs a URL and emits
  an ``AssetMaterialization`` only when the ETag changes. Replaces blind
  cron polling with change-driven sensing.
- ``remote_tsv_to_parquet`` — bundles (external asset, ETag sensor,
  DuckDB-backed ingestion asset) for a remote TSV/CSV URL.
"""

from __future__ import annotations

from typing import Callable

import httpx
from dateutil import parser as date_parser

import dagster as dg

from omicidx.dagster.resources import DuckDBResource, OmicidxStorage


def etag_change_sensor(
    *,
    url: str,
    asset_key: str | dg.AssetKey,
    sensor_name: str | None = None,
    minimum_interval_seconds: int = 60 * 60,
    default_status: dg.DefaultSensorStatus = dg.DefaultSensorStatus.RUNNING,
    request_timeout: float = 10.0,
) -> dg.SensorDefinition:
    """Build a sensor that materializes ``asset_key`` when the URL's ETag changes.

    The sensor stores the most recently seen ETag in its cursor. On each tick
    it issues a HEAD request; if the server returns a different ETag it emits
    an ``AssetMaterialization`` with file metadata (etag, size, last-modified)
    and updates the cursor. Tick is a no-op if the server omits an ETag.

    Pair with an asset whose ``automation_condition`` includes
    ``dg.AutomationCondition.any_deps_updated()`` to trigger ingestion only on
    actual change rather than blind cron.
    """
    key = asset_key if isinstance(asset_key, dg.AssetKey) else dg.AssetKey(asset_key)
    name = sensor_name or f"{'_'.join(key.path)}_etag_sensor"

    @dg.sensor(
        name=name,
        asset_selection=dg.AssetSelection.assets(key),
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=default_status,
    )
    def _sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
        try:
            response = httpx.head(url, timeout=request_timeout, follow_redirects=True)
            response.raise_for_status()
        except httpx.RequestError as exc:
            context.log.error(f"HEAD {url} failed: {exc}")
            return dg.SensorResult()

        etag = response.headers.get("ETag")
        if not etag:
            context.log.warning(f"No ETag header for {url}; sensor cannot detect changes")
            return dg.SensorResult()

        etag = etag.strip('"')
        if etag == context.cursor:
            return dg.SensorResult()

        metadata: dict[str, dg.MetadataValue] = {
            "etag": dg.MetadataValue.text(etag),
            "url": dg.MetadataValue.url(url),
        }
        size = response.headers.get("Content-Length")
        if size and size.isdigit():
            metadata["file_size_bytes"] = dg.MetadataValue.int(int(size))
        last_modified = response.headers.get("Last-Modified")
        if last_modified:
            try:
                metadata["last_modified"] = dg.MetadataValue.timestamp(
                    date_parser.parse(last_modified)
                )
            except (ValueError, TypeError):
                metadata["last_modified_raw"] = dg.MetadataValue.text(last_modified)

        context.log.info(f"ETag changed for {url}: {context.cursor} -> {etag}")
        return dg.SensorResult(
            asset_events=[dg.AssetMaterialization(asset_key=key, metadata=metadata)],
            cursor=etag,
        )

    return _sensor


def remote_tsv_to_parquet(
    *,
    url: str,
    external_key: str,
    asset_name: str,
    output_parts: tuple[str, ...],
    group_name: str = "consolidate",
    key_prefix: list[str] | None = None,
    description: str = "",
    delim: str = "\t",
    nullstr: str = "-",
    sensor_interval_seconds: int = 60 * 60,
    automation_condition: dg.AutomationCondition | None = None,
    tags: dict[str, str] | None = None,
) -> tuple[dg.AssetSpec, dg.SensorDefinition, dg.AssetsDefinition]:
    """Bundle (external asset, ETag sensor, DuckDB ingestion asset) for a remote TSV.

    The ingestion asset reads the URL via DuckDB's ``read_csv`` and writes a
    zstd-compressed Parquet file under ``storage.get_duckdb_path(*output_parts)``.

    The default ``automation_condition`` triggers when the ETag sensor reports
    an upstream change. Override for cron-only or hybrid policies.

    Returns the three definitions; the caller is responsible for wiring them
    into ``Definitions(assets=..., sensors=...)``.
    """
    external = dg.AssetSpec(
        key=external_key,
        group_name=group_name,
        description=description or f"External file at {url}",
        metadata={"url": dg.MetadataValue.url(url)},
    )

    sensor = etag_change_sensor(
        url=url,
        asset_key=external_key,
        minimum_interval_seconds=sensor_interval_seconds,
    )

    asset_tags = {"source": "remote", "storage": "parquet", **(tags or {})}
    automation = automation_condition or dg.AutomationCondition.any_deps_updated()

    @dg.asset(
        name=asset_name,
        key_prefix=key_prefix,
        group_name=group_name,
        deps=[external],
        kinds={"duckdb", "parquet", "s3"},
        tags=asset_tags,
        automation_condition=automation,
    )
    def _ingest(
        context: dg.AssetExecutionContext,
        storage: OmicidxStorage,
        duckdb_res: DuckDBResource,
    ) -> dg.MaterializeResult:
        output_path = storage.get_duckdb_path(*output_parts)
        sql = f"""
            COPY (
                SELECT * FROM read_csv(
                    '{url}',
                    delim='{delim}',
                    header=true,
                    nullstr='{nullstr}'
                )
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
        with duckdb_res.get_connection() as con:
            context.log.info(f"Ingesting {url} -> {output_path}")
            con.execute(sql)
            row_count = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
            ).fetchone()[0]

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(row_count),
                "output_path": dg.MetadataValue.text(output_path),
                "source_url": dg.MetadataValue.url(url),
            }
        )

    return external, sensor, _ingest


__all__: list[Callable | str] = [
    "etag_change_sensor",
    "remote_tsv_to_parquet",
]
