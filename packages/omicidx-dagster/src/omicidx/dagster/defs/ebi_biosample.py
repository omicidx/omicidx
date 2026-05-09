"""EBI Biosample ingestion + consolidation.

Daily-partitioned fetch of biosample updates from the EBI Biosamples API,
written as gzipped NDJSON per partition, then consolidated into a single
Parquet via DuckDB.

Many partition days legitimately have no samples; the raw asset still
materializes successfully with a row_count of 0.

Ported from monode/projects/dags/src/dags/defs/ebi_biosample/ebi_biosample.py
and adapted to use OmicidxStorage and DuckDBResource.
"""

import asyncio
import gzip
import shutil
import tempfile
from datetime import date, datetime
from pathlib import Path

import httpx
import orjson
import tenacity
from omicidx.dagster.resources import DuckDBResource, OmicidxStorage
from upath import UPath

import dagster as dg

EBI_BIOSAMPLES_BASE_URL = "https://www.ebi.ac.uk/biosamples/samples"
EBI_PAGE_SIZE = 200
EBI_REQUEST_TIMEOUT = 40.0

ebi_biosample_daily_partitions = dg.DailyPartitionsDefinition(
    start_date="2021-01-01",
)


def _partition_filename(partition_date: date) -> str:
    return f"biosamples-{partition_date.isoformat()}.ndjson.gz"


class _SampleFetcher:
    """Cursor-paginated fetch of EBI biosamples for a single date.

    The EBI API uses a custom `dt:update:from=<d>until=<d>` filter syntax.
    Records are written to a local gzipped NDJSON file; the caller is
    responsible for uploading the result if any samples were found.
    """

    def __init__(self, *, partition_date: date, local_path: Path) -> None:
        self.partition_date = partition_date
        self.local_path = local_path
        self.cursor = "*"
        self.next_url: str | None = None

    def _filter(self) -> str:
        d = self.partition_date.isoformat()
        return f"dt:update:from={d}until={d}"

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(10),
        wait=tenacity.wait_random_exponential(multiplier=1, max=40),
        retry=tenacity.retry_if_exception_type(httpx.HTTPError),
    )
    async def _request(self, client: httpx.AsyncClient) -> dict:
        if self.next_url is not None:
            response = await client.get(self.next_url, timeout=EBI_REQUEST_TIMEOUT)
        else:
            params = {
                "cursor": self.cursor,
                "size": EBI_PAGE_SIZE,
                "filter": self._filter(),
            }
            response = await client.get(
                EBI_BIOSAMPLES_BASE_URL,
                params=params,
                timeout=EBI_REQUEST_TIMEOUT,
            )
        response.raise_for_status()
        return response.json()

    async def _iter_samples(self, client: httpx.AsyncClient):
        while True:
            payload = await self._request(client)
            samples = payload.get("_embedded", {}).get("samples")
            if not samples:
                return
            for sample in samples:
                # Flatten characteristics: {"name": [{"text": "x"}, ...]}
                # → [{"text": "x", "characteristic": "name"}, ...]
                flattened = []
                for k, values in sample.get("characteristics", {}).items():
                    for v in values:
                        v["characteristic"] = k
                        flattened.append(v)
                sample["characteristics"] = flattened
                yield sample

            next_link = payload.get("_links", {}).get("next")
            if not next_link:
                return
            self.next_url = next_link["href"]

    async def run(self) -> int:
        count = 0
        async with httpx.AsyncClient() as client:
            with gzip.open(self.local_path, "wb") as fh:
                async for sample in self._iter_samples(client):
                    fh.write(orjson.dumps(sample))
                    fh.write(b"\n")
                    count += 1
        return count


@dg.asset(
    partitions_def=ebi_biosample_daily_partitions,
    group_name="ebi_biosample",
    kinds={"python", "json", "s3"},
    tags={
        "layer": "raw",
        "cost": "low",
        "sla": "daily",
        "source": "ebi_api",
        "storage": "jsonl",
    },
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
    automation_condition=dg.AutomationCondition.on_cron("0 2 * * *"),
)
def ebi_biosample_raw(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    """Fetch EBI Biosample updates for a single day partition."""
    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    output_dir: UPath = storage.get_upath("ebi_biosample", "raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    final_path = output_dir / _partition_filename(partition_date)

    context.log.info(f"Fetching EBI biosamples for {partition_date.isoformat()}")

    with tempfile.NamedTemporaryFile(suffix=".ndjson.gz", delete=False) as tmp:
        local_path = Path(tmp.name)

    try:
        fetcher = _SampleFetcher(partition_date=partition_date, local_path=local_path)
        record_count = asyncio.run(fetcher.run())

        if record_count == 0:
            context.log.info(
                f"No samples updated on {partition_date.isoformat()} "
                "(this is expected for many days)"
            )
            return dg.MaterializeResult(
                metadata={
                    "row_count": dg.MetadataValue.int(0),
                    "partition_date": dg.MetadataValue.text(partition_date.isoformat()),
                    "note": dg.MetadataValue.text("No samples updated on this date"),
                }
            )

        with open(local_path, "rb") as src, final_path.open("wb") as dst:
            shutil.copyfileobj(src, dst)

        size_bytes = local_path.stat().st_size
        context.log.info(
            f"Wrote {record_count:,} records to {final_path} "
            f"({size_bytes / (1024 * 1024):.2f} MB)"
        )

        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(record_count),
                "partition_date": dg.MetadataValue.text(partition_date.isoformat()),
                "output_path": dg.MetadataValue.text(str(final_path)),
                "file_size_mb": dg.MetadataValue.float(
                    round(size_bytes / (1024 * 1024), 2)
                ),
            }
        )
    finally:
        local_path.unlink(missing_ok=True)


@dg.asset(
    group_name="ebi_biosample",
    kinds={"duckdb", "parquet", "s3"},
    tags={
        "layer": "consolidated",
        "cost": "low",
        "sla": "daily",
        "source": "derived",
        "storage": "parquet",
    },
    deps=[ebi_biosample_raw],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    # ebi_biosample_raw is daily-partitioned with a long historical tail;
    # eager() requires every partition to be materialized (any_deps_missing)
    # before firing, which deadlocks the cascade. Run once daily if any
    # partition updated in the last 24h. Same fix as GEO consolidates (#95).
    automation_condition=(
        dg.AutomationCondition.on_cron("0 6 * * *")
        & dg.AutomationCondition.any_deps_updated()
    ),
)
def ebi_biosample_parquet(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
) -> dg.MaterializeResult:
    """Consolidate per-day NDJSON partitions into a single Parquet."""
    input_glob = storage.get_duckdb_path(
        "ebi_biosample", "raw", "biosamples-*.ndjson.gz"
    )
    output_path = storage.get_duckdb_path(
        "ebi_biosample", "parquet", "ebi_biosamples.parquet"
    )

    sql = f"""
        COPY (
            SELECT *
            FROM read_ndjson_auto(
                '{input_glob}',
                maximum_object_size = 1000000000,
                ignore_errors = false
            )
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """

    with duckdb_res.get_connection() as con:
        context.log.info(f"Consolidating {input_glob} → {output_path}")
        con.execute(sql)
        row_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{output_path}')"
        ).fetchone()[0]

    context.log.info(f"Wrote {row_count:,} rows to {output_path}")
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "output_path": dg.MetadataValue.text(output_path),
            "input_glob": dg.MetadataValue.text(input_glob),
        }
    )
