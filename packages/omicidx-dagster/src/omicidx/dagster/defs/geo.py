"""GEO extract assets with monthly partitions.

Historical months are materialized once. The current month is
re-materialized daily (data accumulates as NCBI indexes new entries).
"""

import asyncio
import gzip
import shutil
import tempfile
from datetime import date, datetime, timedelta

import httpx
import polars as pl
import tenacity
from dateutil.relativedelta import relativedelta
from omicidx.dagster.resources import OmicidxStorage
from omicidx.parsers.geo import parser as gp
from upath import UPath

import dagster as dg

geo_monthly_partitions = dg.MonthlyPartitionsDefinition(
    start_date="2005-01-01",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _month_range_from_partition(partition_key: str) -> tuple[date, date]:
    """Convert a partition key like '2024-03-01' to (start_date, end_date)."""
    start = datetime.strptime(partition_key, "%Y-%m-%d").date()
    end = (start + relativedelta(months=1)) - timedelta(days=1)
    return start, end


def _entrezid_to_geo(entrezid: str) -> str:
    """Convert an Entrez GDS id to a GEO accession."""
    import re

    if entrezid.startswith("2"):
        return re.sub("^20*", "GSE", entrezid)
    elif entrezid.startswith("1"):
        return re.sub("^10*", "GPL", entrezid)
    elif entrezid.startswith("3"):
        return re.sub("^30*", "GSM", entrezid)
    raise ValueError(f"Expected entrezid to start with 1, 2, or 3: {entrezid}")


@tenacity.retry(
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception(
        lambda e: (
            (isinstance(e, httpx.HTTPStatusError) and e.response.status_code in {429})
            or isinstance(
                e,
                httpx.RemoteProtocolError | httpx.ConnectError | httpx.TimeoutException,
            )
        )
    ),
)
async def _fetch_accessions(start_date: date, end_date: date) -> list[str]:
    """Query Entrez eutils for GEO accessions updated in a date range."""
    accessions = []
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

    return accessions


@tenacity.retry(
    wait=tenacity.wait_exponential_jitter(2, 30),
    stop=tenacity.stop_after_attempt(5),
)
async def _fetch_soft(accession: str, client: httpx.AsyncClient) -> str:
    """Fetch the GEO SOFT text for a single accession."""
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


async def _fetch_and_parse_accessions(
    accessions: list[str],
    concurrency: int = 30,
) -> dict[str, list[dict]]:
    """Fetch SOFT text and parse all accessions, returning dicts by prefix."""
    results: dict[str, list[dict]] = {"GSE": [], "GSM": [], "GPL": []}
    semaphore = asyncio.Semaphore(concurrency)

    async def _process_one(acc: str, client: httpx.AsyncClient):
        async with semaphore:
            text = await _fetch_soft(acc, client)
            lines = [x.strip() for x in text.split("\n")]
            entity = gp._parse_single_entity_soft(lines)
            if entity is not None:
                prefix = entity.accession[:3]
                if prefix in results:
                    results[prefix].append(entity.model_dump())

    async with httpx.AsyncClient(timeout=30) as client:
        tasks = [_process_one(acc, client) for acc in accessions]
        await asyncio.gather(*tasks, return_exceptions=True)

    return results


def _write_ndjson_gz(records: list[dict], path: UPath) -> int:
    """Write records as gzipped NDJSON to a UPath, returning count."""
    import orjson

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
# Assets
# ---------------------------------------------------------------------------


@dg.asset(
    group_name="geo",
    kinds={"python", "parquet", "s3"},
    tags={
        "layer": "raw",
        "cost": "low",
        "sla": "daily",
        "source": "ncbi_api",
        "storage": "parquet",
    },
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
)
def geo_rna_seq_counts(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    """Fetch GSE accessions with RNA-seq counts (eutils filter)."""
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
            import time

            time.sleep(0.5)

    df = pl.DataFrame(accessions)
    outpath = storage.get_upath("geo", "raw", "gse_with_rna_seq_counts.parquet")
    outpath.parent.mkdir(parents=True, exist_ok=True)

    with outpath.open("wb") as f:
        df.write_parquet(f, use_pyarrow=True, compression="zstd")

    context.log.info(f"Wrote {len(accessions)} GSEs with RNA-seq counts to {outpath}")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(accessions)),
            "output_path": dg.MetadataValue.text(str(outpath)),
        }
    )


@dg.multi_asset(
    outs={
        "geo_gse_raw": dg.AssetOut(
            group_name="geo",
            kinds={"python", "json", "s3"},
            tags={
                "layer": "raw",
                "cost": "high",
                "sla": "monthly",
                "source": "ncbi_api",
                "storage": "ndjson",
            },
        ),
        "geo_gsm_raw": dg.AssetOut(
            group_name="geo",
            kinds={"python", "json", "s3"},
            tags={
                "layer": "raw",
                "cost": "high",
                "sla": "monthly",
                "source": "ncbi_api",
                "storage": "ndjson",
            },
        ),
        "geo_gpl_raw": dg.AssetOut(
            group_name="geo",
            kinds={"python", "json", "s3"},
            tags={
                "layer": "raw",
                "cost": "high",
                "sla": "monthly",
                "source": "ncbi_api",
                "storage": "ndjson",
            },
        ),
    },
    partitions_def=geo_monthly_partitions,
    retry_policy=dg.RetryPolicy(max_retries=2, delay=60),
)
def geo_raw(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
):
    """Extract GEO metadata for a single month partition.

    Produces three outputs (GSE, GSM, GPL) as gzipped NDJSON files
    in hive-partitioned paths: geo/raw/{entity}/year=YYYY/month=MM/
    """
    partition_key = context.partition_key
    start_date, end_date = _month_range_from_partition(partition_key)
    output_base = storage.get_upath("geo", "raw")

    context.log.info(f"Processing GEO {start_date} to {end_date}")

    # 1. Fetch accessions updated in this month
    accessions = asyncio.run(_fetch_accessions(start_date, end_date))
    context.log.info(f"Found {len(accessions)} accessions for {partition_key}")

    if not accessions:
        # Write empty files so downstream knows this partition was processed
        counts = {}
        for prefix, entity in [("GSE", "gse"), ("GSM", "gsm"), ("GPL", "gpl")]:
            path = (
                output_base
                / entity
                / f"year={start_date.strftime('%Y')}"
                / f"month={start_date.strftime('%m')}"
                / "data_0.ndjson.gz"
            )
            _write_ndjson_gz([], path)
            counts[prefix] = 0

        yield dg.Output(None, output_name="geo_gse_raw", metadata={"row_count": 0})
        yield dg.Output(None, output_name="geo_gsm_raw", metadata={"row_count": 0})
        yield dg.Output(None, output_name="geo_gpl_raw", metadata={"row_count": 0})
        return

    # 2. Fetch SOFT and parse
    parsed = asyncio.run(_fetch_and_parse_accessions(accessions))

    # 3. Write each entity type
    for prefix, entity, output_name in [
        ("GSE", "gse", "geo_gse_raw"),
        ("GSM", "gsm", "geo_gsm_raw"),
        ("GPL", "gpl", "geo_gpl_raw"),
    ]:
        path = (
            output_base
            / entity
            / f"year={start_date.strftime('%Y')}"
            / f"month={start_date.strftime('%m')}"
            / "data_0.ndjson.gz"
        )
        n = _write_ndjson_gz(parsed[prefix], path)
        context.log.info(f"Wrote {n} {prefix} records to {path}")

        yield dg.Output(
            None,
            output_name=output_name,
            metadata={
                "row_count": dg.MetadataValue.int(n),
                "output_path": dg.MetadataValue.text(str(path)),
            },
        )
