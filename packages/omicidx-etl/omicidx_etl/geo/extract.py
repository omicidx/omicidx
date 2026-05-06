import anyio
import re
import faulthandler
from upath import UPath
from datetime import timedelta, datetime, date
from dateutil.relativedelta import relativedelta
import click
import polars as pl


import gzip

import httpx
from anyio import create_memory_object_stream
from anyio.streams.memory import MemoryObjectSendStream, MemoryObjectReceiveStream
from omicidx.geo import parser as gp
from tenacity import retry
import tenacity

from omicidx_etl.log import get_logger

logger = get_logger(__name__)


import tempfile
import shutil


faulthandler.enable()


@retry(
    wait=tenacity.wait_exponential_jitter(2, 30),
    stop=tenacity.stop_after_attempt(5),
# retry=tenacity.retry_if_exception(
#         lambda e: (
#             # turns out that the GEO API is not very reliable
#             # and returns 429s and 404s for valid accessions
#             (isinstance(e, httpx.HTTPStatusError) and (e.response.status_code in {429, 404}))
#             or isinstance(
#                 e,
#                 (httpx.RemoteProtocolError, httpx.ConnectError, httpx.TimeoutException),
#             )
#         )
#     ),
    before_sleep=lambda retry_state: logger.warning(
        f"GEO SOFT request failed, retrying in 2 seconds (attempt {retry_state.attempt_number}/5)"
    ),
)
async def get_geo_soft(accession, client) -> str:
    """Fetches the GEO SOFT file for the given accession."""
    params = {}
    params['acc'] = accession
    params['targ'] = 'self'
    params['form'] = 'text'
    params['view'] = 'quick' if accession.startswith("GSM") else 'brief'
    url = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi"
    response = await client.get(url, params=params)
    response.raise_for_status()
    return response.text


async def fetch_geo_soft_worker(
    accessions_to_fetch_receive: MemoryObjectReceiveStream,  # from entrez search
    entity_text_to_process_send: MemoryObjectSendStream,  # to process_entitity_worker
):
    """Fetches the GEO SOFT files for the accessions.

    We read from receive stream and send the text to the send stream.
    The send stream is then processed by the write_geo_ids function.
    """
    async with httpx.AsyncClient(timeout=30) as client:
        async with accessions_to_fetch_receive, entity_text_to_process_send:
            async for accession in accessions_to_fetch_receive:
                geo_text = await get_geo_soft(accession, client)
                await entity_text_to_process_send.send(geo_text)


def get_result_paths(start_date, end_date, output_path: UPath):
    """Get the output upaths for the given month.

    Assumes hive-like partitioning by year and month.
    Confirmed that duckdb can read from this structure.

    Args:
        start_date: Start date of the month
        end_date: End date of the month
        output_path: Base output path for GEO data

    Returns:
        Tuple of UPaths for GSE, GSM, and GPL
    """
    gse_path = output_path / "gse" / f"year={start_date.strftime('%Y')}" / f"month={start_date.strftime('%m')}" / "data_0.ndjson.gz"
    gsm_path = output_path / "gsm" / f"year={start_date.strftime('%Y')}" / f"month={start_date.strftime('%m')}" / "data_0.ndjson.gz"
    gpl_path = output_path / "gpl" / f"year={start_date.strftime('%Y')}" / f"month={start_date.strftime('%m')}" / "data_0.ndjson.gz"
    return gse_path, gsm_path, gpl_path


async def write_geo_entity_worker(
    entity_text_to_process_receive: MemoryObjectReceiveStream,  # from process_entitity_worker
    start_date: date,
    end_date: date,
    output_path: UPath,
):
    """Writes the entity to a file."""
    gse_path, gsm_path, gpl_path = get_result_paths(start_date, end_date, output_path)

    record_counts = {
        "GSE": 0,
        "GSM": 0,
        "GPL": 0,
    }

    with (
        tempfile.NamedTemporaryFile() as gse_temp,
        tempfile.NamedTemporaryFile() as gsm_temp,
        tempfile.NamedTemporaryFile() as gpl_temp,
    ):
        # Note that we don't use parquet because the schema
        # vary by file. We just write ndjson files.
        # The files can be converted in bulk to parquet by duckdb later
        with (
            gzip.open(gse_temp.name, "wb") as gse_tmp_write,
            gzip.open(gsm_temp.name, "wb") as gsm_tmp_write,
            gzip.open(gpl_temp.name, "wb") as gpl_tmp_write
        ):
            async with entity_text_to_process_receive:
                async for text in entity_text_to_process_receive:
                    lines = [x.strip() for x in text.split("\n")]
                    entity = gp._parse_single_entity_soft(lines)
                    if entity is None:
                        continue
                    if entity.accession.startswith("GSE"):  # type: ignore
                        gse_tmp_write.write(
                            entity.model_dump_json().encode("utf-8") + b"\n"
                        )  # type: ignore
                    elif entity.accession.startswith("GSM"):  # type: ignore
                        gsm_tmp_write.write(
                            entity.model_dump_json().encode("utf-8") + b"\n"
                        )  # type: ignore
                    elif entity.accession.startswith("GPL"):  # type: ignore
                        gpl_tmp_write.write(
                            entity.model_dump_json().encode("utf-8") + b"\n"
                        )  # type: ignore
                    record_counts[entity.accession[:3]] += 1  # type: ignore


        # Always write all three files, even if empty.
        # This ensures the skip guard in geo_metadata_by_date (which checks
        # that all three paths exist) correctly skips months on subsequent runs,
        # even for months where no GPL (platform) records were updated.
        for temp_file, path, entity_type in [
            (gse_temp, gse_path, "GSE"),
            (gsm_temp, gsm_path, "GSM"),
            (gpl_temp, gpl_path, "GPL"),
        ]:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(temp_file.name, "rb") as src:
                with path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
            n = record_counts[entity_type]
            if n > 0:
                logger.info(f"Wrote {n} {entity_type} records to {path}")
            else:
                logger.debug(f"No {entity_type} records for period; wrote empty {path}")

    logger.info(f"Record counts: {record_counts}")


def entrezid_to_geo(entrezid: str):
    if entrezid.startswith("2"):
        return re.sub("^20*", "GSE", entrezid)
    elif entrezid.startswith("1"):
        return re.sub("^10*", "GPL", entrezid)
    elif entrezid.startswith("3"):
        return re.sub("^30*", "GSM", entrezid)

    raise ValueError("Expected entrezid to start with 1, 2, or 3")


@retry(
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception(
        lambda e: (
            (isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 429)
            or isinstance(
                e,
                (httpx.RemoteProtocolError, httpx.ConnectError, httpx.TimeoutException),
            )
        )
    ),
    before_sleep=lambda retry_state: logger.warning(
        f"Entrez API request failed, retrying in 2 seconds (attempt {retry_state.attempt_number}/5)"
    ),
)
async def prod1(accessions_to_fetch_send: MemoryObjectSendStream, start_date, end_date):
    offset = 0
    RETMAX = 5000
    async with accessions_to_fetch_send:
        while True:
            async with httpx.AsyncClient(timeout=60) as client:
                logger.debug(f"Fetching {start_date} to {end_date} offset {offset}")
                response = await client.get(
                    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                    params={
                        "db": "gds",
                        "term": f"""(GSM[etyp] OR GSE[etyp] OR GPL[etyp]) AND ("{start_date.strftime("%Y/%m/%d")}"[Update Date] : "{end_date.strftime("%Y/%m/%d")}"[Update Date])""",
                        "retmode": "json",
                        "retmax": RETMAX,
                        "retstart": offset,
                    },
                )
                response.raise_for_status()
                json_results = response.json()
                for id in json_results["esearchresult"]["idlist"]:
                    await accessions_to_fetch_send.send(entrezid_to_geo(id))
                if len(json_results["esearchresult"]["idlist"]) < RETMAX:
                    break
                offset += 5000

@tenacity.retry(
    wait=tenacity.wait_fixed(2),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception(
        lambda e: (
            (isinstance(e, httpx.HTTPStatusError) and e.response.status_code == 429)
            or isinstance(
                e,
                (httpx.RemoteProtocolError, httpx.ConnectError, httpx.TimeoutException),
            )
        )
    ),
    before_sleep=lambda retry_state: logger.warning(
        f"Entrez API request failed, retrying in 2 seconds (attempt {retry_state.attempt_number}/5)"
    ),
)
def gse_with_rna_seq_counts() -> pl.DataFrame:
    """GEO supplies a hidden filter for getting GSEs with RNA-seq counts
    
    The filter is at the level of GSEs, not GSMs. This function just 
    applies the filter and returns a list of GSEs that have GEO/SRA-supplied
    RNA-seq counts.
    
    It is very fast to run since it runs against eutils and only returns
    ids. 
    """
    offset = 0
    RETMAX = 5000
    gses_with_rna_seq_counts = []
    while True:
        with httpx.Client(timeout=60) as client:
            response = client.get(
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                params={
                    "db": "gds",
                    "term": '"rnaseq+counts"[filter]',
                    "retmode": "json",
                    "retmax": RETMAX,
                    "retstart": offset,
                },
            )
            response.raise_for_status()
            json_results = response.json()
            for id in json_results["esearchresult"]["idlist"]:
                gses_with_rna_seq_counts.append(
                    {"accession": entrezid_to_geo(id)}
                )
            if len(json_results["esearchresult"]["idlist"]) < RETMAX:
                break
            offset += 5000
            import time
            time.sleep(0.5)  # to avoid hitting the rate limit
    # list of string GSE_ACCESSIONs
    return pl.DataFrame({"accession": gses_with_rna_seq_counts})



async def geo_metadata_by_date(
    start_date: date,
    end_date: date,
    output_path: UPath,
):
    gse_path, gsm_path, gpl_path = get_result_paths(start_date, end_date, output_path)
    if (
        gse_path.exists() or gsm_path.exists() or gpl_path.exists()
    ) and end_date < date.today():
        # Any existing output file is sufficient evidence that this month was
        # already processed: some months have no GSE records, others no GPL.
        # The companion fix (write_geo_entity_worker always writes all three
        # files) ensures every future run leaves all three files in place,
        # making this check unambiguous going forward.
        logger.debug(f"Skipping {start_date} to {end_date} since it already exists")
        return
    (
        accessions_to_fetch_send,
        accessions_to_fetch_receive,
    ) = create_memory_object_stream(100)
    (
        entity_text_to_process_send,
        entity_text_to_process_receive,
    ) = create_memory_object_stream(100)

    async with anyio.create_task_group() as tg:
        # start 30 workers to fetch the GEO SOFT files
        async with accessions_to_fetch_receive, entity_text_to_process_send:
            for i in range(30):
                tg.start_soon(
                    fetch_geo_soft_worker,
                    accessions_to_fetch_receive.clone(),
                    entity_text_to_process_send.clone(),
                )
        # start a worker to write the entity to a file
        # this worker will write the entity to a file
        # one for each of GSE, GSM, and GPL
        async with entity_text_to_process_receive:
            tg.start_soon(
                write_geo_entity_worker,
                entity_text_to_process_receive.clone(),
                start_date,
                end_date,
                output_path,
            )
        async with accessions_to_fetch_send:
            tg.start_soon(prod1, accessions_to_fetch_send.clone(), start_date, end_date)


def get_monthly_ranges(start_date_str: str, end_date_str: str) -> list[tuple]:
    """
    Given a start and end date, returns a list of tuples representing the start and end dates of each month in the range.

    :param start_date_str: The start date in 'YYYY-MM-DD' format
    :param end_date_str: The end date in 'YYYY-MM-DD' format
    :return: List of tuples, each containing the start and end date of a month in the range
    """
    # Convert strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    monthly_ranges = []
    current_start = start_date.replace(day=1)

    while current_start <= end_date:
        # Calculate the end of the current month
        current_end = (current_start + relativedelta(months=1)) - timedelta(days=1)
        # Adjust the end date if it's beyond the given end_date
        # if current_end > end_date:
        #    current_end = end_date
        monthly_ranges.append((current_start.date(), current_end.date()))
        # Move to the first day of the next month
        current_start = current_start + relativedelta(months=1)

    return monthly_ranges


async def main(output_path: UPath):
    # Get the GSEs with RNA-seq counts
    # updated each run since it is very fast

    gses_with_rna_seq = gse_with_rna_seq_counts()
    outfile = output_path / "gse_with_rna_seq_counts.parquet"

    # Write to a parquet file
    with outfile.open("wb") as f:
        gses_with_rna_seq.write_parquet(f, use_pyarrow=True, compression="zstd")
    logger.info(f"Wrote {len(gses_with_rna_seq)} GSEs with RNA-seq counts to {outfile}")

    start = "2005-01-01"
    end = date.today().strftime("%Y-%m-%d")
    ranges = get_monthly_ranges(start, end)
    for start_date, end_date in ranges:
        logger.info(f"Processing GEO metadata from {start_date} to {end_date}")
        await geo_metadata_by_date(start_date, end_date, output_path)


@click.group()
def geo():
    """OmicIDX ETL Pipeline - GEO data extraction tools."""
    pass


@geo.command()
@click.argument("output_base", required=False, default=None)
def extract(output_base: str | None):
    """Extract GEO metadata."""
    from omicidx_etl.config import settings
    base = UPath(output_base) if output_base else settings.publish_directory
    output_path = base / "geo" / "raw"
    logger.info(f"Starting GEO extraction to {output_path}")
    anyio.run(main, output_path)

if __name__ == "__main__":
    geo()
