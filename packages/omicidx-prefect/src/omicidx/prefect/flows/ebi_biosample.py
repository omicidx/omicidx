"""EBI Biosample extract flow.

Partitions are calendar days (YYYY-MM-DD), starting at 2021-01-01. Each
day gets a semaphore at `_semaphores/ebi_biosample/{YYYY-MM-DD}.json`,
including empty days (legitimately many days have zero updates). The
flow defaults to enumerating from the start date to today, processing
only missing-semaphore days. The current day is always re-run.
"""

import asyncio
import gzip
import shutil
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
import orjson
import tenacity
from omicidx.prefect.config import get_duckdb_connection, get_duckdb_path, get_upath
from omicidx.prefect.semaphore import SemaphoreStore

from prefect import flow, get_run_logger, task
from prefect.task_runners import ThreadPoolTaskRunner

EBI_BIOSAMPLES_BASE_URL = "https://www.ebi.ac.uk/biosamples/samples"
EBI_PAGE_SIZE = 200
EBI_REQUEST_TIMEOUT = 40.0


def _partition_filename(partition_date: date) -> str:
    return f"biosamples-{partition_date.isoformat()}.ndjson.gz"


class _SampleFetcher:
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


@task(
    retries=2,
    retry_delay_seconds=30,
    task_run_name="ebi-biosample-extract-{key}",
)
def extract_ebi_biosample(key: str, force: bool = False) -> dict:
    log = get_run_logger()
    sem = SemaphoreStore("ebi_biosample")
    if not force and sem.exists(key):
        log.info(f"ebi_biosample/{key}: semaphore exists, skipping")
        return {"key": key, "skipped": True}

    partition_date = datetime.strptime(key, "%Y-%m-%d").date()
    output_dir = get_upath("ebi_biosample", "raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    final_path = output_dir / _partition_filename(partition_date)

    log.info(f"Fetching EBI biosamples for {partition_date.isoformat()}")

    with tempfile.NamedTemporaryFile(suffix=".ndjson.gz", delete=False) as tmp:
        local_path = Path(tmp.name)

    try:
        fetcher = _SampleFetcher(partition_date=partition_date, local_path=local_path)
        record_count = asyncio.run(fetcher.run())

        if record_count == 0:
            log.info(
                f"No samples updated on {partition_date.isoformat()} "
                "(this is expected for many days)"
            )
            sem.mark_done(
                key,
                metadata={"row_count": 0, "note": "no samples updated"},
            )
            return {"key": key, "skipped": False, "row_count": 0}

        with open(local_path, "rb") as src, final_path.open("wb") as dst:
            shutil.copyfileobj(src, dst)

        size_bytes = local_path.stat().st_size
        log.info(
            f"Wrote {record_count:,} records to {final_path} "
            f"({size_bytes / (1024 * 1024):.2f} MB)"
        )
        sem.mark_done(
            key,
            metadata={
                "row_count": record_count,
                "output_path": str(final_path),
                "file_size_bytes": size_bytes,
            },
        )
        return {"key": key, "skipped": False, "row_count": record_count}
    finally:
        local_path.unlink(missing_ok=True)


def _enumerate_days(start: str = "2021-01-01", end: str | None = None) -> list[str]:
    start_d = datetime.strptime(start, "%Y-%m-%d").date()
    end_d = datetime.strptime(end, "%Y-%m-%d").date() if end else date.today()
    keys: list[str] = []
    cur = start_d
    while cur <= end_d:
        keys.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return keys


@task(retries=1, retry_delay_seconds=60)
def consolidate_ebi_biosample_parquet() -> dict:
    """Consolidate per-day NDJSON into a single parquet via DuckDB."""
    log = get_run_logger()
    input_glob = get_duckdb_path("ebi_biosample", "raw", "biosamples-*.ndjson.gz")
    output_path = get_duckdb_path("ebi_biosample", "parquet", "ebi_biosamples.parquet")
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
    with get_duckdb_connection() as con:
        log.info(f"Consolidating {input_glob} → {output_path}")
        con.execute(sql)
        row_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{output_path}')"
        ).fetchone()[0]
    log.info(f"Wrote {row_count:,} rows to {output_path}")
    return {"row_count": row_count, "output_path": output_path}


@flow(
    name="ebi-biosample-extract",
    task_runner=ThreadPoolTaskRunner(max_workers=4),
)
def ebi_biosample_extract_flow(
    start_day: str = "2021-01-01",
    end_day: str | None = None,
    rerun_current_day: bool = True,
    force: bool = False,
    consolidate: bool = True,
) -> None:
    days = _enumerate_days(start=start_day, end=end_day)
    current_key = date.today().isoformat()
    futures = []
    for key in days:
        force_this = force or (rerun_current_day and key == current_key)
        futures.append(extract_ebi_biosample.submit(key=key, force=force_this))
    for fut in futures:
        fut.result()

    if consolidate:
        consolidate_ebi_biosample_parquet()


if __name__ == "__main__":
    ebi_biosample_extract_flow()
