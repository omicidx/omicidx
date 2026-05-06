import re
import datetime
import click
import pubmed_parser as pp
from urllib.request import urlretrieve
import tempfile
import shutil
from upath import UPath
import pyarrow as pa
import pyarrow.parquet as pq

from omicidx_etl.log import get_logger

logger = get_logger(__name__)


# Module-level constants
PUBMED_BASE = UPath("https://ftp.ncbi.nlm.nih.gov/pubmed")
OUTPUT_EXTENSION = ".parquet"


def _url_to_pubmed_id(url: UPath) -> str:
    """Get the pubmed id from the url.
    
    For example, for a URL like `https://ftp.ncbi.nlm.nih.gov/pubmed/pubmed25n0023.xml.gz`
    it will return `pubmed25n0023`.
    """
    return re.sub(r"\..*", "", url.name)


def load_available_urls():
    """Load the available urls from the base directory.
    
    Note that this function covers both the base and update URLs."""
    available_urls = list(PUBMED_BASE.glob("baseline/pubmed*.xml.gz"))
    available_urls += list(PUBMED_BASE.glob("updatefiles/pubmed*.xml.gz"))
    id_to_available_url_map = {
        _url_to_pubmed_id(url): url for url in available_urls
    }
    return id_to_available_url_map


def load_existing_urls(output_path: UPath):
    """Load the existing urls from the output directory."""
    existing_urls = list(output_path.glob(f"**/*{OUTPUT_EXTENSION}"))
    id_to_existing_url_map = {
        _url_to_pubmed_id(url): url for url in existing_urls
    }
    return id_to_existing_url_map


def get_needed_ids(output_path: UPath, replace=False):
    """Return the ids that are needed to be processed."""
    available_urls = load_available_urls()
    existing_urls = load_existing_urls(output_path)
    
    in_ids = set(available_urls.keys())
    out_ids = set(existing_urls.keys())

    if replace:
        return in_ids

    return in_ids - out_ids


def get_needed_urls(output_path: UPath, replace=False) -> list[UPath]:
    """Return the urls that are needed to be processed."""
    available_urls = load_available_urls()
    needed_ids = get_needed_ids(output_path, replace=replace)
    return [available_urls[id] for id in needed_ids]


def parquet_file_for_url(url: UPath, output_path: UPath) -> UPath:
    """Get the parquet output file path for a given URL."""
    fname_out = url.name.replace(".xml.gz", ".parquet")
    return output_path / fname_out


def pubmed_url_to_parquet_file(url: UPath, output_path: UPath) -> None:
    """Pubmed files as parquet asset

    This asset covers the entire pubmed corpus. It is partitioned by
    pubmed file. Each partition is a line iterator that yields json
    objects for each article in the pubmed file after conversion from
    xml to json. The json objects are serialized to bytes using orjson.
    """
    with (
        tempfile.NamedTemporaryFile(suffix=".xml.gz") as temp_xml_file,
        tempfile.NamedTemporaryFile(suffix=".parquet") as local_parquet_file
    ):
        localfname = temp_xml_file.name
        urlretrieve(str(url), filename=localfname)
        pubmed_article_generator = pp.parse_medline_xml(
            localfname,
            year_info_only=False,
            nlm_category=True,
            author_list=True,
            reference_list=True,
            parse_downto_mesh_subterms=True,
        )
        objects = []
        for obj in pubmed_article_generator:
            obj["_inserted_at"] = datetime.datetime.now()
            obj["_read_from"] = str(url)
            objects.append(obj)
        pubmed_table = pa.Table.from_pylist(objects)
        # write the table to parquet locally first
        # then upload it to the final destination
        pq.write_table(pubmed_table, local_parquet_file.name, compression='zstd')
        with parquet_file_for_url(url, output_path).open("wb") as outfile:
            logger.info(f"Writing {url} to {str(outfile)}")
            with open(local_parquet_file.name, 'rb') as infile:
                shutil.copyfileobj(infile, outfile)
            logger.info(f"Finished writing {url} to {str(outfile)}")


def etl_pubmeds(output_path: UPath, replace: bool = False):
    needed_urls = get_needed_urls(output_path, replace=replace)
    logger.info(f"Processing {len(needed_urls)} urls")
    output_path.mkdir(parents=True, exist_ok=True)
    for index, url in enumerate(needed_urls):
        logger.info("Processing url: " + str(url))
        logger.info(f"Processing {index + 1} of {len(needed_urls)}")
        pubmed_url_to_parquet_file(url, output_path)  # type: ignore

@click.group()
def pubmed():
    pass


@pubmed.command()
@click.argument("output_base", required=False)
@click.option("--replace", is_flag=True, help="Reprocess all PubMed files.")
def extract(output_base: str | None, replace: bool):
    """Extract PubMed data to Parquet files."""
    output_path = resolve_output_path(output_base)
    logger.info(f"Starting extraction to {output_path}")
    etl_pubmeds(output_path, replace=replace)


def resolve_output_path(output_base: str | None) -> UPath:
    """Resolve PubMed output path using base-path conventions."""
    if output_base is None:
        from omicidx_etl.config import settings
        return settings.publish_directory / "pubmed" / "raw"
    return UPath(output_base) / "pubmed" / "raw"


if __name__ == "__main__":
    pubmed()
