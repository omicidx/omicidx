"""GEO extract flow.

Partitions are calendar months (YYYY-MM). Each month gets a semaphore
under `_semaphores/geo/{YYYY-MM}.json`. By default the flow processes
the current month every run (`rerun_current_month=True`) and skips any
historical month that already has a semaphore.

A second non-partitioned step (`geo_rna_seq_counts_flow`) refreshes the
list of GSEs with RNA-seq counts; it has no partitioning.
"""

import asyncio
import gzip
import re
import shutil
import tempfile
import time
from datetime import date, datetime, timedelta

import httpx
import orjson
import polars as pl
import tenacity
from dateutil.relativedelta import relativedelta
from omicidx.parsers.geo import parser as gp
from omicidx.prefect.config import get_upath
from omicidx.prefect.semaphore import SemaphoreStore
from upath import UPath

from prefect import flow, get_run_logger, task
from prefect.task_runners import ProcessPoolTaskRunner

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _month_key(partition_date: date) -> str:
    return partition_date.strftime("%Y-%m")


def _month_range(partition_date: date) -> tuple[date, date]:
    start = partition_date.replace(day=1)
    end = (start + relativedelta(months=1)) - timedelta(days=1)
    return start, end


def _entrezid_to_geo(entrezid: str) -> str:
    if entrezid.startswith("2"):
        return re.sub("^20*", "GSE", entrezid)
    if entrezid.startswith("1"):
        return re.sub("^10*", "GPL", entrezid)
    if entrezid.startswith("3"):
        return re.sub("^30*", "GSM", entrezid)
    raise ValueError(f"Expected entrezid to start with 1, 2, or 3: {entrezid}")


@tenacity.retry(
    wait=tenacity.wait_exponential_jitter(2, 60),
    stop=tenacity.stop_after_attempt(8),
    retry=tenacity.retry_if_exception(
        lambda e: (
            (
                isinstance(e, httpx.HTTPStatusError)
                and (
                    e.response.status_code == 429 or 500 <= e.response.status_code < 600
                )
            )
            or isinstance(
                e,
                httpx.RemoteProtocolError | httpx.ConnectError | httpx.TimeoutException,
            )
        )
    ),
)
async def _fetch_accessions(start_date: date, end_date: date) -> list[str]:
    accessions: list[str] = []
    offset = 0
    retmax = 5000

    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            response = await client.get(
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                params={
                    "db": "gds",
                    "term": (
                        f"(GSM[etyp] OR GSE[etyp] OR GPL[etyp]) AND "
                        f'("{start_date.strftime("%Y/%m/%d")}"[Update Date] : '
                        f'"{end_date.strftime("%Y/%m/%d")}"[Update Date])'
                    ),
                    "retmode": "json",
                    "retmax": retmax,
                    "retstart": offset,
                },
            )
            response.raise_for_status()
            result = response.json()
            ids = result["esearchresult"]["idlist"]
            for eid in ids:
                accessions.append(_entrezid_to_geo(eid))
            if len(ids) < retmax:
                break
            offset += retmax
            await asyncio.sleep(0.4)

    return accessions


@tenacity.retry(
    wait=tenacity.wait_exponential_jitter(2, 30),
    stop=tenacity.stop_after_attempt(5),
)
async def _fetch_soft(accession: str, client: httpx.AsyncClient) -> str:
    params = {
        "acc": accession,
        "targ": "self",
        "form": "text",
        "view": "quick" if accession.startswith("GSM") else "brief",
    }
    response = await client.get(
        "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi", params=params
    )
    response.raise_for_status()
    return response.text


async def _fetch_and_parse(
    accessions: list[str], concurrency: int = 30
) -> dict[str, list[dict]]:
    results: dict[str, list[dict]] = {"GSE": [], "GSM": [], "GPL": []}
    semaphore = asyncio.Semaphore(concurrency)

    async def _one(acc: str, client: httpx.AsyncClient) -> None:
        async with semaphore:
            text = await _fetch_soft(acc, client)
            lines = [x.strip() for x in text.split("\n")]
            entity = gp._parse_single_entity_soft(lines)
            if entity is not None:
                prefix = entity.accession[:3]
                if prefix in results:
                    results[prefix].append(entity.model_dump())

    async with httpx.AsyncClient(timeout=30) as client:
        await asyncio.gather(
            *(_one(acc, client) for acc in accessions), return_exceptions=True
        )

    return results


def _write_ndjson_gz(records: list[dict], path: UPath) -> int:
    with tempfile.NamedTemporaryFile(suffix=".ndjson.gz", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        with gzip.open(tmp_path, "wb") as f:
            for rec in records:
                f.write(orjson.dumps(rec))
                f.write(b"\n")

        path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp_path, "rb") as src, path.open("wb") as dst:
            shutil.copyfileobj(src, dst)
    finally:
        UPath(tmp_path).unlink(missing_ok=True)

    return len(records)


# ---------------------------------------------------------------------------
# Per-month extract task
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=60, task_run_name="geo-extract-{key}")
def extract_month(key: str, force: bool = False) -> dict:
    """Extract one calendar-month partition of GEO metadata."""
    log = get_run_logger()
    sem = SemaphoreStore("geo")

    if not force and sem.exists(key):
        log.info(f"geo/{key}: semaphore exists, skipping")
        return {"key": key, "skipped": True}

    partition_date = datetime.strptime(key, "%Y-%m").date()
    start_date, end_date = _month_range(partition_date)
    output_base = get_upath("geo", "raw")

    log.info(f"Processing GEO {start_date} to {end_date}")

    accessions = asyncio.run(_fetch_accessions(start_date, end_date))
    log.info(f"Found {len(accessions)} accessions for {key}")

    counts = {"GSE": 0, "GSM": 0, "GPL": 0}

    if accessions:
        parsed = asyncio.run(_fetch_and_parse(accessions))
    else:
        parsed = {"GSE": [], "GSM": [], "GPL": []}

    for prefix, entity in [("GSE", "gse"), ("GSM", "gsm"), ("GPL", "gpl")]:
        path = (
            output_base
            / entity
            / f"year={start_date.strftime('%Y')}"
            / f"month={start_date.strftime('%m')}"
            / "data_0.ndjson.gz"
        )
        n = _write_ndjson_gz(parsed[prefix], path)
        counts[prefix] = n
        log.info(f"Wrote {n} {prefix} records to {path}")

    sem.mark_done(
        key,
        metadata={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            **counts,
        },
    )
    return {"key": key, "skipped": False, **counts}


# ---------------------------------------------------------------------------
# RNA-seq counts (non-partitioned)
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=30)
def fetch_rna_seq_counts() -> dict:
    log = get_run_logger()
    offset = 0
    retmax = 5000
    accessions: list[dict] = []

    with httpx.Client(timeout=60) as client:
        while True:
            response = client.get(
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                params={
                    "db": "gds",
                    "term": '"rnaseq+counts"[filter]',
                    "retmode": "json",
                    "retmax": retmax,
                    "retstart": offset,
                },
            )
            response.raise_for_status()
            ids = response.json()["esearchresult"]["idlist"]
            for eid in ids:
                accessions.append({"accession": _entrezid_to_geo(eid)})
            if len(ids) < retmax:
                break
            offset += retmax
            time.sleep(0.5)

    df = pl.DataFrame(accessions)
    outpath = get_upath("geo", "raw", "gse_with_rna_seq_counts.parquet")
    outpath.parent.mkdir(parents=True, exist_ok=True)

    with outpath.open("wb") as f:
        df.write_parquet(f, use_pyarrow=True, compression="zstd")

    log.info(f"Wrote {len(accessions)} GSEs with RNA-seq counts to {outpath}")
    return {"row_count": len(accessions), "output_path": str(outpath)}


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------


def _enumerate_months(start: str = "2005-01", end: str | None = None) -> list[str]:
    start_d = datetime.strptime(start, "%Y-%m").date().replace(day=1)
    end_d = (
        datetime.strptime(end, "%Y-%m").date().replace(day=1)
        if end
        else date.today().replace(day=1)
    )
    keys: list[str] = []
    cur = start_d
    while cur <= end_d:
        keys.append(_month_key(cur))
        cur = cur + relativedelta(months=1)
    return keys


@flow(
    name="geo-extract",
    task_runner=ProcessPoolTaskRunner(max_workers=2),
)
def geo_extract_flow(
    start_month: str = "2005-01",
    end_month: str | None = None,
    rerun_current_month: bool = True,
    force: bool = False,
) -> None:
    """Extract GEO metadata, one monthly partition at a time.

    By default iterates from ``start_month`` to the current month, skipping
    any month whose semaphore exists. Set ``force=True`` to re-extract
    everything in the range. Set ``rerun_current_month=False`` to also
    skip the current month if its semaphore exists.
    """
    months = _enumerate_months(start=start_month, end=end_month)
    current_key = _month_key(date.today())
    futures = []
    for key in months:
        force_this = force or (rerun_current_month and key == current_key)
        futures.append(extract_month.submit(key=key, force=force_this))
    for fut in futures:
        fut.result()


@flow(name="geo-rna-seq-counts")
def geo_rna_seq_counts_flow() -> None:
    """Refresh the (small) GSE-with-RNA-seq-counts file. Not partitioned."""
    fetch_rna_seq_counts()


if __name__ == "__main__":
    geo_extract_flow()
