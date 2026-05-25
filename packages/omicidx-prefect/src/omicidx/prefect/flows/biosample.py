"""BioSample and BioProject extract flows.

Neither source is partitioned — each run overwrites the single output
file. We still emit a semaphore per run (keyed by today's date) so
operators can see when the last successful run finished.
"""

import gzip
import shutil
import tempfile
import time
from datetime import date

import httpx
import orjson
import tenacity
from omicidx.parsers.biosample import BioProjectParser, BioSampleParser
from omicidx.prefect.config import get_duckdb_connection, get_duckdb_path, get_upath
from omicidx.prefect.semaphore import SemaphoreStore
from upath import UPath

from prefect import flow, get_run_logger, task

BIOSAMPLE_URL = "https://ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz"
BIOPROJECT_URL = "https://ftp.ncbi.nlm.nih.gov/bioproject/bioproject.xml"


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=30),
    retry=tenacity.retry_if_exception_type(httpx.RequestError),
    stop=tenacity.stop_after_attempt(5),
)
def _download(url: str, dest: str) -> None:
    log = get_run_logger()
    log.info(f"Downloading {url}")
    with (
        open(dest, "wb") as f,
        httpx.stream("GET", url, timeout=120, follow_redirects=True) as response,
    ):
        response.raise_for_status()
        for chunk in response.iter_bytes():
            f.write(chunk)
    log.info(f"Download complete: {url}")


def _extract_entity(
    *,
    url: str,
    entity: str,
    parser_class: type,
    use_gzip_input: bool,
    output_dir: UPath,
) -> dict:
    log = get_run_logger()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "data.jsonl.gz"

    start = time.time()
    count = 0

    with tempfile.NamedTemporaryFile(suffix=".download") as dl_tmp:
        _download(url, dl_tmp.name)

        open_fn = gzip.open if use_gzip_input else open

        with tempfile.NamedTemporaryFile(suffix=".jsonl.gz", delete=False) as out_tmp:
            out_tmp_path = out_tmp.name

        try:
            with (
                open_fn(dl_tmp.name, "rb") as infile,
                gzip.open(out_tmp_path, "wb") as outfile,
            ):
                for obj in parser_class(infile, validate_with_schema=False):
                    outfile.write(orjson.dumps(obj))
                    outfile.write(b"\n")
                    count += 1
                    if count % 100_000 == 0:
                        log.info(f"{entity}: parsed {count:,} records")

            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_tmp_path, "rb") as src, output_path.open("wb") as dst:
                shutil.copyfileobj(src, dst)
        finally:
            UPath(out_tmp_path).unlink(missing_ok=True)

    duration = time.time() - start
    log.info(
        f"{entity}: wrote {count:,} records to {output_path} "
        f"in {duration:.1f}s ({count / max(duration, 1e-3):.0f} rec/s)"
    )
    return {
        "row_count": count,
        "output_path": str(output_path),
        "duration_seconds": duration,
        "source_url": url,
    }


@task(retries=2, retry_delay_seconds=30)
def extract_biosample(force: bool = False) -> dict:
    log = get_run_logger()
    sem = SemaphoreStore("biosample")
    key = date.today().isoformat()
    if not force and sem.exists(key):
        log.info(f"biosample/{key}: semaphore exists, skipping")
        return {"key": key, "skipped": True}

    output_dir = get_upath("biosample", "raw")
    meta = _extract_entity(
        url=BIOSAMPLE_URL,
        entity="biosample",
        parser_class=BioSampleParser,
        use_gzip_input=True,
        output_dir=output_dir,
    )
    sem.mark_done(key, metadata=meta)
    return {"key": key, "skipped": False, **meta}


@task(retries=2, retry_delay_seconds=30)
def extract_bioproject(force: bool = False) -> dict:
    log = get_run_logger()
    sem = SemaphoreStore("bioproject")
    key = date.today().isoformat()
    if not force and sem.exists(key):
        log.info(f"bioproject/{key}: semaphore exists, skipping")
        return {"key": key, "skipped": True}

    output_dir = get_upath("bioproject", "raw")
    meta = _extract_entity(
        url=BIOPROJECT_URL,
        entity="bioproject",
        parser_class=BioProjectParser,
        use_gzip_input=False,
        output_dir=output_dir,
    )
    sem.mark_done(key, metadata=meta)
    return {"key": key, "skipped": False, **meta}


@task(retries=1, retry_delay_seconds=60)
def bioproject_to_parquet() -> dict:
    """Convert BioProject JSONL → parquet via DuckDB. Always runs (cheap)."""
    log = get_run_logger()
    input_path = get_duckdb_path("bioproject", "raw", "data.jsonl.gz")
    output_path = get_duckdb_path("bioproject", "parquet", "bioprojects.parquet")
    sql = f"""
        COPY (
            SELECT
                trim(title) as title,
                trim(description) as description,
                trim(name) as name,
                trim(accession) as accession,
                publications,
                locus_tags,
                release_date,
                data_types,
                external_links
            FROM read_ndjson_auto(
                '{input_path}',
                maximum_object_size = 1000000000
            )
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    with get_duckdb_connection() as con:
        log.info(f"Converting {input_path} to {output_path}")
        con.execute(sql)
        row_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{output_path}')"
        ).fetchone()[0]
    log.info(f"Wrote {row_count:,} rows to {output_path}")
    return {"row_count": row_count, "output_path": output_path}


@flow(name="biosample-extract")
def biosample_extract_flow(force: bool = False) -> None:
    extract_biosample(force=force)


@flow(name="bioproject-extract")
def bioproject_extract_flow(force: bool = False) -> None:
    extract_bioproject(force=force)
    bioproject_to_parquet()


if __name__ == "__main__":
    biosample_extract_flow()
    bioproject_extract_flow()
