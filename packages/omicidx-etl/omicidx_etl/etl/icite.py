import zipfile
import tarfile
import click
import httpx
from pathlib import Path
from upath import UPath
import tempfile

from omicidx_etl.log import get_logger

from omicidx_etl.db import duckdb_connection

logger = get_logger(__name__)

ICITE_COLLECTION_ID = 4586573


def get_icite_collection_articles() -> list[dict[str, str]]:
    with httpx.Client(timeout=60) as client:
        response = client.get(
            f"https://api.figshare.com/v2/collections/{ICITE_COLLECTION_ID}/articles"
        )
        response.raise_for_status()
        logger.info("Getting latest ICITE articles from figshare")
        return response.json()


def get_icite_article_files(article_id: str):
    with httpx.Client(timeout=60) as client:
        response = client.get(
            f"https://api.figshare.com/v2/articles/{article_id}/files"
        )
        response.raise_for_status()
        logger.info("Getting latest ICITE article files from figshare")
        return response.json()


def clean_icite_output_directory(output_directory: UPath) -> None:
    if not output_directory.exists():
        return

    try:
        output_directory.fs.rm(output_directory.path, recursive=True)
    except TypeError:
        output_directory.fs.rm(output_directory.path, True)


def _find_file(file_json: list[dict], prefix: str) -> dict:
    """Find a file in the Figshare file list by name prefix."""
    for f in file_json:
        if f["name"].startswith(prefix):
            return f
    available = [f["name"] for f in file_json]
    raise ValueError(f"No file starting with '{prefix}' found. Available: {available}")


def _download_figshare_file(url: str, dest: str) -> None:
    """Stream-download a file from Figshare."""
    logger.info(f"Downloading {url}")
    with httpx.Client(timeout=None, follow_redirects=True) as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)
    logger.info(f"Downloaded to {dest}")


def _resolve_csv_source(file_info: dict, workdir: str) -> str:
    """Get a local CSV path from a Figshare file entry.

    If the file is a bare CSV, returns the download URL (DuckDB streams it).
    If it's a zip or tar.gz, downloads and extracts to workdir.
    """
    name = file_info["name"]
    url = file_info["download_url"]

    if name.endswith(".csv"):
        return url

    local_path = f"{workdir}/{name}"
    _download_figshare_file(url, local_path)

    if name.endswith(".zip"):
        with zipfile.ZipFile(local_path) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                raise ValueError(f"No CSV found inside {name}")
            zf.extract(csv_names[0], workdir)
            Path(local_path).unlink()
            return f"{workdir}/{csv_names[0]}"

    if name.endswith(".tar.gz"):
        with tarfile.open(local_path, "r:gz") as tar:
            csv_members = [m for m in tar.getmembers() if m.name.endswith(".csv")]
            if not csv_members:
                raise ValueError(f"No CSV found inside {name}")
            tar.extract(csv_members[0], workdir, filter="data")
            Path(local_path).unlink()
            return f"{workdir}/{csv_members[0].name}"

    raise ValueError(f"Unsupported file format: {name}")


def _csv_to_parquet(csv_source: str, output_dir: UPath, name: str) -> list[UPath]:
    """Convert a CSV source to partitioned Parquet files via DuckDB."""
    dest = output_dir / name
    sql = f"""
        COPY (SELECT * FROM read_csv_auto('{csv_source}', null_padding=true))
        TO '{dest}' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES '500MB')
    """
    logger.info(f"Converting {name} to parquet")
    with duckdb_connection() as conn:
        conn.execute(sql)
    return list(dest.glob("*.parquet"))


def icite_flow(output_directory: UPath) -> list[UPath]:
    """Ingest iCite data from Figshare to Parquet.

    The NIH iCite data is stored in a Figshare collection containing
    two datasets: icite_metadata and open_citation_collection.
    Files may be bare CSVs or zip/tar.gz archives depending on the
    Figshare release. This flow handles both formats.
    """
    articles: list[dict] = get_icite_collection_articles()  # type: ignore
    files: list[dict] = get_icite_article_files(articles[0]["id"])  # type: ignore

    logger.info(f"Found {len(files)} files in the latest iCite article")
    for f in files:
        logger.info(f"  {f['name']} ({f['size'] / 1e9:.1f} GB)")

    with tempfile.TemporaryDirectory() as workdir:
        clean_icite_output_directory(output_directory)

        metadata_info = _find_file(files, "icite_metadata")
        metadata_csv = _resolve_csv_source(metadata_info, workdir)
        metadata_files = _csv_to_parquet(metadata_csv, output_directory, "icite_metadata")

        citation_info = _find_file(files, "open_citation_collection")
        citation_csv = _resolve_csv_source(citation_info, workdir)
        citation_files = _csv_to_parquet(citation_csv, output_directory, "icite_opencitation")

    return metadata_files + citation_files


@click.group()
def icite():
    """ICITE extraction commands."""
    pass

@icite.command()
@click.argument('output_base', required=False, default=None)
def extract(output_base: str | None):
    """Extract iCite data from Figshare."""
    from omicidx_etl.config import settings
    base = UPath(output_base) if output_base else settings.publish_directory
    output_dir = base / "icite" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    icite_flow(output_dir)
    
if __name__ == "__main__":
    extract()