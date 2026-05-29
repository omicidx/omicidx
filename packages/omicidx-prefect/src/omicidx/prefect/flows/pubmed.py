"""PubMed extract flow.

Partitions are individual PubMed XML files (e.g., `pubmed25n0001`).
Each file gets a semaphore under `_semaphores/pubmed/{file_id}.json`.
The flow lists the NCBI FTP, maps an extract task across files whose
semaphores are missing, and writes one parquet per file.
"""

import re
import shutil
import tempfile
from datetime import datetime
from urllib.request import urlretrieve

import pubmed_parser as pp
import pyarrow as pa
import pyarrow.parquet as pq
from omicidx.prefect.config import get_upath
from omicidx.prefect.semaphore import SemaphoreStore
from upath import UPath

from prefect import flow, get_run_logger, task
from prefect.task_runners import ProcessPoolTaskRunner

PUBMED_BASE = UPath("https://ftp.ncbi.nlm.nih.gov/pubmed")
_XML_GZ_RE = re.compile(r"^(pubmed\d+n\d+)\.xml\.gz$")


def _list_pubmed_files() -> dict[str, str]:
    """List PubMed XML files via HTTPS. Returns {partition_key: url_string}."""
    result: dict[str, str] = {}
    for subdir in ["baseline", "updatefiles"]:
        for entry in (PUBMED_BASE / subdir).iterdir():
            m = _XML_GZ_RE.match(entry.name)
            if m:
                result[m.group(1)] = str(entry)
    return result


def _sanitize_utf8(obj):
    """Recursively replace invalid UTF-8 bytes in any string values."""
    if isinstance(obj, str):
        return obj.encode("utf-8", errors="surrogateescape").decode(
            "utf-8", errors="replace"
        )
    if isinstance(obj, dict):
        return {k: _sanitize_utf8(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_utf8(v) for v in obj]
    return obj


@task(retries=2, retry_delay_seconds=30, task_run_name="pubmed-extract-{key}")
def extract_pubmed_file(key: str, url: str, force: bool = False) -> dict:
    log = get_run_logger()
    sem = SemaphoreStore("pubmed")
    if not force and sem.exists(key):
        log.info(f"pubmed/{key}: semaphore exists, skipping")
        return {"key": key, "skipped": True}

    output_dir = get_upath("pubmed", "raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{key}.parquet"

    with (
        tempfile.NamedTemporaryFile(suffix=".xml.gz") as tmp_xml,
        tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_parquet,
    ):
        log.info(f"Downloading {url}")
        urlretrieve(str(url), filename=tmp_xml.name)

        log.info(f"Parsing {key}")
        articles = list(
            pp.parse_medline_xml(
                tmp_xml.name,
                year_info_only=False,
                nlm_category=True,
                author_list=True,
                reference_list=True,
                parse_downto_mesh_subterms=True,
            )
        )

        for obj in articles:
            obj["_inserted_at"] = datetime.now()
            obj["_read_from"] = str(url)

        articles = [_sanitize_utf8(a) for a in articles]
        table = pa.Table.from_pylist(articles)
        pq.write_table(table, tmp_parquet.name, compression="zstd")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp_parquet.name, "rb") as src, output_path.open("wb") as dst:
            shutil.copyfileobj(src, dst)

    log.info(f"Wrote {len(articles)} articles to {output_path}")
    sem.mark_done(
        key,
        metadata={
            "row_count": len(articles),
            "output_path": str(output_path),
            "source_url": str(url),
        },
    )
    return {"key": key, "skipped": False, "row_count": len(articles)}


@flow(
    name="pubmed-extract",
    task_runner=ProcessPoolTaskRunner(max_workers=12),
)
def pubmed_extract_flow(force: bool = False) -> None:
    """Extract every PubMed file whose semaphore is missing.

    Equivalent to the Dagster pubmed_sensor + pubmed_raw pair: the sensor's
    job (poll FTP, identify new files) is folded into the flow body.
    """
    log = get_run_logger()
    available = _list_pubmed_files()
    sem = SemaphoreStore("pubmed")
    done = set() if force else set(sem.list_keys())
    todo = sorted(set(available) - done)
    log.info(
        f"PubMed listing: {len(available)} files total, "
        f"{len(done)} done, {len(todo)} to extract"
    )

    futures = []
    for key in todo:
        futures.append(
            extract_pubmed_file.submit(key=key, url=available[key], force=force)
        )
    for fut in futures:
        fut.result()


if __name__ == "__main__":
    pubmed_extract_flow()
