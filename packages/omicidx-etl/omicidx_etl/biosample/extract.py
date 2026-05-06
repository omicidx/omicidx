"""
Simplified biosample/bioproject extraction without Prefect dependencies.
"""
import threading
import time
import httpx
import tempfile
import gzip
import orjson
import shutil
from upath import UPath
from pathlib import Path
from omicidx.biosample import BioSampleParser, BioProjectParser
import click
from omicidx_etl.log import get_logger
import tenacity
from datetime import datetime

from .asset_metadata import AssetMetadata

logger = get_logger(__name__)

# Configuration
BIO_SAMPLE_URL = "https://ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz"
BIO_PROJECT_URL = "https://ftp.ncbi.nlm.nih.gov/bioproject/bioproject.xml"
OUTPUT_SUFFIX = ".jsonl.gz"

# Heartbeat interval (seconds)
HEARTBEAT_INTERVAL = 60

@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=30),
    retry=tenacity.retry_if_exception_type(httpx.RequestError),
    stop=tenacity.stop_after_attempt(5),
)
def url_download(url: str, download_filename: str) -> None:
    """Download a file from a URL to a local destination."""

    try:
        logger.info(f"Downloading {url} to {download_filename}")
        with open(download_filename, "wb") as download_file:
            with httpx.stream("GET", url, timeout=60) as response:
                response.raise_for_status()

                for chunk in response.iter_bytes():
                    download_file.write(chunk)
        logger.info(f"Completed download of {url}")

    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        raise


def cleanup_old_files(output_dir: Path, entity: str) -> None:
    """Remove old output files for an entity."""
    for file_path in output_dir.glob(f"*{OUTPUT_SUFFIX}"):
        file_path.unlink()
        logger.info(f"Removed old file: {file_path}")


def extract_biosample(output_dir: UPath) -> AssetMetadata:
    """Extract biosample data to NDJSON files."""
    return _extract_entity(
        url=BIO_SAMPLE_URL,
        entity="biosample",
        output_dir=output_dir / "biosample" / "raw",
        parser_class=BioSampleParser,
        use_gzip_input=True,
    )


def extract_bioproject(output_dir: UPath) -> AssetMetadata:
    """Extract bioproject data to NDJSON files."""
    return _extract_entity(
        url=BIO_PROJECT_URL,
        entity="bioproject",
        output_dir=output_dir / "bioproject" / "raw",
        parser_class=BioProjectParser,
        use_gzip_input=False,
    )


def _extract_entity(
    url: str,
    entity: str,
    output_dir: UPath,
    parser_class,
    use_gzip_input: bool,
) -> AssetMetadata:
    """Extract a single entity type to gzipped JSONL (streaming)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    cleanup_old_files(output_dir, entity)

    logger.info(f"Downloading {url}")

    output_files: list[UPath] = []

    with tempfile.NamedTemporaryFile() as downloaded_file:
        url_download(url, downloaded_file.name)

        obj_counter = 0
        start_time = time.time()

        stop_event = threading.Event()

        def _log_heartbeat():
            while not stop_event.wait(HEARTBEAT_INTERVAL):
                elapsed = time.time() - start_time
                rps = obj_counter / elapsed if elapsed > 0 else 0.0
                logger.info(
                    f"Heartbeat: {entity} parsed {obj_counter} records in {elapsed:.1f}s ({rps:.1f} rec/s)"
                )

        # Open input file
        open_func = gzip.open if use_gzip_input else open
        mode = "rb"

        heartbeat_thread = threading.Thread(target=_log_heartbeat, daemon=True)
        heartbeat_thread.start()

        output_path = output_dir / f"data{OUTPUT_SUFFIX}"

        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", delete=False, suffix=OUTPUT_SUFFIX
            ) as tmp_out:
                tmp_out_path = Path(tmp_out.name)

            try:
                with open_func(downloaded_file.name, mode) as input_file, gzip.open(
                    tmp_out_path, "wb"
                ) as out_f:
                    # the parser yields dicts unless validate_with_schema=True
                    # we skip validation for performance
                    for obj in parser_class(input_file, validate_with_schema=False):
                        line = orjson.dumps(obj)

                        out_f.write(line)
                        out_f.write(b"\n")

                        obj_counter += 1

                output_path.parent.mkdir(parents=True, exist_ok=True)
                with tmp_out_path.open("rb") as src, output_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)

                output_files.append(output_path)
                logger.info(f"Wrote {obj_counter} records to {output_path}")
            finally:
                tmp_out_path.unlink(missing_ok=True)
        finally:
            stop_event.set()
            heartbeat_thread.join(timeout=1)

    logger.info(
        f"Completed {entity} extraction: {obj_counter} records, {len(output_files)} files"
    )
    asset_metadata = AssetMetadata(
        asset_key = f"src_{entity}",
        storage_path = str(output_path),
        upstream_assets = [BIO_SAMPLE_URL if entity == "biosample" else BIO_PROJECT_URL],
        row_count = obj_counter,
        format = "jsonl",
        compression = "gzip",
        created_at = datetime.now(),
        size_bytes = output_path.stat().st_size,
        runtime_seconds = time.time() - start_time,
    )
    return asset_metadata
        



def extract_all(output_dir: UPath) -> dict[str, list[UPath]]:
    """Extract both biosample and bioproject."""
    results = {}
    
    for entity_func, entity_name in [
        (extract_bioproject, "bioproject"),
        (extract_biosample, "biosample"),
    ]:
        results[entity_name] = entity_func(output_dir)

    return results


@click.group()
def biosample():
    pass


@biosample.command()
@click.argument("output_base", required=False, default=None)
def extract(output_base: str | None):
    """Command-line interface for extraction and optional upload."""
    from omicidx_etl.config import settings
    output_path = UPath(output_base) if output_base else settings.publish_directory
    logger.info(f"Starting extraction to {output_path}")
    extract_all(output_path)

if __name__ == "__main__":
    extract()