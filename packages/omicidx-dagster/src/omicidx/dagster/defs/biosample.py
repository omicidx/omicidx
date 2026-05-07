"""BioSample and BioProject extract assets.

Downloads XML from NCBI FTP, parses via omicidx-parsers, writes JSONL.gz
to the configured publish directory.
"""

import gzip
import shutil
import tempfile
import time

import dagster as dg
import httpx
import orjson
import tenacity
from omicidx.parsers.biosample import BioProjectParser, BioSampleParser
from omicidx.dagster.resources import DuckDBResource, OmicidxStorage
from upath import UPath

BIOSAMPLE_URL = "https://ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz"
BIOPROJECT_URL = "https://ftp.ncbi.nlm.nih.gov/bioproject/bioproject.xml"


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=30),
    retry=tenacity.retry_if_exception_type(httpx.RequestError),
    stop=tenacity.stop_after_attempt(5),
)
def _download(url: str, dest: str, context: dg.AssetExecutionContext) -> None:
    """Stream-download a URL to a local file with retries."""
    context.log.info(f"Downloading {url}")
    with (
        open(dest, "wb") as f,
        httpx.stream("GET", url, timeout=120, follow_redirects=True) as response,
    ):
        response.raise_for_status()
        for chunk in response.iter_bytes():
            f.write(chunk)
    context.log.info(f"Download complete: {url}")


def _extract_entity(
    *,
    url: str,
    entity: str,
    parser_class: type,
    use_gzip_input: bool,
    output_dir: UPath,
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Download, parse, and write a single entity to JSONL.gz."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "data.jsonl.gz"

    start = time.time()
    count = 0

    with tempfile.NamedTemporaryFile(suffix=".download") as dl_tmp:
        _download(url, dl_tmp.name, context)

        open_fn = gzip.open if use_gzip_input else open

        with tempfile.NamedTemporaryFile(
            suffix=".jsonl.gz", delete=False
        ) as out_tmp:
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
                        context.log.info(f"{entity}: parsed {count:,} records")

            # Upload to final location
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_tmp_path, "rb") as src, output_path.open("wb") as dst:
                shutil.copyfileobj(src, dst)
        finally:
            UPath(out_tmp_path).unlink(missing_ok=True)

    duration = time.time() - start
    context.log.info(
        f"{entity}: wrote {count:,} records to {output_path} "
        f"in {duration:.1f}s ({count / duration:.0f} rec/s)"
    )

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "output_path": dg.MetadataValue.text(str(output_path)),
            "duration_seconds": dg.MetadataValue.float(duration),
            "source_url": dg.MetadataValue.url(url),
        }
    )


@dg.asset(
    group_name="biosample",
    kinds={"python", "json", "s3"},
    tags={
        "layer": "raw",
        "cost": "medium",
        "sla": "daily",
        "source": "ncbi_ftp",
        "storage": "jsonl",
    },
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
)
def biosample_raw(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    """Extract BioSample XML from NCBI FTP to JSONL.gz."""
    output_dir = storage.get_upath("biosample", "raw")
    return _extract_entity(
        url=BIOSAMPLE_URL,
        entity="biosample",
        parser_class=BioSampleParser,
        use_gzip_input=True,
        output_dir=output_dir,
        context=context,
    )


@dg.asset(
    group_name="biosample",
    kinds={"python", "json", "s3"},
    tags={
        "layer": "raw",
        "cost": "medium",
        "sla": "daily",
        "source": "ncbi_ftp",
        "storage": "jsonl",
    },
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
)
def bioproject_raw(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    """Extract BioProject XML from NCBI FTP to JSONL.gz."""
    output_dir = storage.get_upath("bioproject", "raw")
    return _extract_entity(
        url=BIOPROJECT_URL,
        entity="bioproject",
        parser_class=BioProjectParser,
        use_gzip_input=False,
        output_dir=output_dir,
        context=context,
    )


@dg.asset(
    group_name="biosample",
    kinds={"duckdb", "parquet", "s3"},
    tags={
        "layer": "transformed",
        "cost": "low",
        "sla": "daily",
        "source": "derived",
        "storage": "parquet",
    },
    deps=[bioproject_raw],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
)
def bioproject_parquet(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
) -> dg.MaterializeResult:
    """Convert BioProject JSONL to Parquet using DuckDB."""
    input_path = storage.get_duckdb_path("bioproject", "raw", "data.jsonl.gz")
    output_path = storage.get_duckdb_path("bioproject", "parquet", "bioprojects.parquet")

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

    with duckdb_res.get_connection() as con:
        context.log.info(f"Converting {input_path} to {output_path}")
        con.execute(sql)
        row_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{output_path}')"
        ).fetchone()[0]

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "output_path": dg.MetadataValue.text(output_path),
            "input_path": dg.MetadataValue.text(input_path),
        }
    )
