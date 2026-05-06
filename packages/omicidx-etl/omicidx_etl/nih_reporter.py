import shutil
import zipfile
import httpx
import pathlib
import tempfile
import polars as pl
import gzip
from typing import Optional
from datetime import datetime
import click
from upath import UPath

from omicidx_etl.log import get_logger

logger = get_logger(__name__)

# Configuration constants
DEFAULT_OUTPUT_DIR = UPath("/tmp/omicidx/nih_reporter")
DEFAULT_TIMEOUT = 300  # 5 minutes
CHUNK_SIZE = 8192
TEMP_FILE_NAME = "nih_reporter_download.tmp"

# Entity configuration
ENTITIES = [
    "clinicalstudies",
    "patents", 
    "publications",
    "abstracts",
    "projects",
    "linktables",
]

# Entities that don't have yearly data (download full dataset)
FULL_DATASET_ENTITIES = {"clinicalstudies", "patents"}

# Start years for entities that have yearly data (end year is current year)
ENTITY_START_YEARS = {
    "projects": 1985,
    "publications": 1980,
    "linktables": 1980,
    "abstracts": 1985,
}


def get_current_year() -> int:
    """Get the current year."""
    return datetime.now().year


def is_full_dataset_entity(entity: str) -> bool:
    """Check if entity downloads full dataset (no yearly breakdown)."""
    return entity in FULL_DATASET_ENTITIES


def get_entity_years(entity: str) -> Optional[range]:
    """Get the year range for an entity, or None if it's a full dataset entity.
    
    Uses current year as the end year to automatically include new data.
    """
    if entity in FULL_DATASET_ENTITIES:
        return None
    
    start_year = ENTITY_START_YEARS.get(entity)
    if start_year is None:
        logger.warning(f"No start year defined for entity: {entity}")
        return None
    
    current_year = get_current_year()
    # Include current year + 1 to account for potential data availability
    end_year = current_year + 1
    
    return range(start_year, end_year)


def show_entity_configuration():
    """Display the current entity configuration and year ranges."""
    current_year = get_current_year()
    logger.info(f"NIH Reporter Entity Configuration (Current Year: {current_year})")
    logger.info("=" * 60)
    
    for entity in ENTITIES:
        if is_full_dataset_entity(entity):
            logger.info(f"{entity}: Full dataset (no yearly breakdown)")
        else:
            years = get_entity_years(entity)
            if years:
                start_year = years.start
                end_year = years.stop - 1  # range.stop is exclusive
                total_years = end_year - start_year + 1
                logger.info(f"{entity}: {start_year}-{end_year} ({total_years} years)")
            else:
                logger.info(f"{entity}: No year range configured")
    logger.info("=" * 60)


def exporter_url_by_entity_and_year(entity: str, year: Optional[int] = None) -> str:
    """Get the URL for a given entity and year.

    If year is None, get the URL for the full dataset.
    """
    if year is None:
        return f"https://reporter.nih.gov/exporter/{entity}/download"
    else:
        return f"https://reporter.nih.gov/exporter/{entity}/download/{year}"


def get_basename_for_entity(entity: str, year: Optional[int] = None) -> str:
    """Get the base for a filename for a given entity and year."""
    if year is None:
        return f"nih_reporter_{entity}"
    else:
        return f"nih_reporter_{entity}_{year}"


# Sometimes the reporter csv files include characters that
# are not utf-8 encoded. This function will:
# 1. Read the file as utf-8 in chunks
# 2. Decode the chunk as utf-8
# 3. Encode the chunk as utf-8
# 4. Write the chunk to a new gzip file
# 5. delete the original file
# 6. return the new gzip file name
def fix_encoding(pathlib_path: pathlib.Path) -> pathlib.Path:
    """Fix encoding of a given csv file."""
    logger.info(f"Fixing encoding for {pathlib_path}")
    tmppath = pathlib.Path(pathlib_path.parent / f"{pathlib_path.name}.tmp")
    with open(tmppath, "wb") as tmp:
        with open(pathlib_path, "rb") as f:
            for line in f:
                tmp.write(line.decode("utf-8", errors="ignore").encode("utf-8"))
    shutil.copyfile(tmppath, pathlib_path)
    logger.info(f"Done fixing encoding for {pathlib_path}")
    # Remove the temporary file
    tmppath.unlink(missing_ok=True)
    return pathlib_path


def extract_zipfile(zfile: pathlib.Path) -> pathlib.Path:
    """Extract a zipfile and return the path to the extracted CSV file."""
    logger.info(f"Extracting {zfile} to {zfile.parent}")
    try:
        with zipfile.ZipFile(zfile, "r") as zip_ref:
            zip_ref.extractall(zfile.parent)
        
        # Find the extracted CSV file
        csv_files = list(zfile.parent.glob("*.csv"))
        if len(csv_files) != 1:
            raise ValueError(f"Expected exactly 1 CSV file, found {len(csv_files)}: {csv_files}")
        
        csv_file = csv_files[0]
        logger.info(f"Extracted CSV file: {csv_file}")
        return csv_file
        
    except Exception as e:
        logger.error(f"Failed to extract {zfile}: {e}")
        raise


def csv_to_jsonl(csv_file: pathlib.Path, output_file: pathlib.Path) -> None:
    """Convert CSV file to compressed JSONL format."""
    logger.info(f"Converting {csv_file} to {output_file}")
    try:
        df = pl.read_csv(csv_file, infer_schema_length=100000)
        
        # Write to a temporary file first, then compress
        temp_jsonl = output_file.with_suffix('.jsonl')
        df.write_ndjson(temp_jsonl)
        
        # Compress the JSONL file
        with open(temp_jsonl, 'rb') as f_in:
            with gzip.open(output_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Remove temporary file
        temp_jsonl.unlink()
        logger.info(f"Successfully converted to {output_file}")
        
    except Exception as e:
        logger.error(f"Failed to convert {csv_file} to JSONL: {e}")
        raise


def download_file(url: str, output_path: pathlib.Path) -> None:
    """Download a file from URL to output path.
    
    Raises:
        httpx.HTTPStatusError: If the server returns an error status
        Exception: For other download failures
    """
    logger.info(f"Downloading {url}")
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=DEFAULT_TIMEOUT) as response:
            response.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=CHUNK_SIZE):
                    f.write(chunk)
        logger.info(f"Successfully downloaded to {output_path}")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            # Data for this year might not exist - this is acceptable
            logger.warning(f"Data not found (404) for URL: {url}")
            raise DataNotAvailableError(f"No data available for URL: {url}") from e
        else:
            logger.error(f"HTTP error {e.response.status_code} downloading {url}: {e}")
            raise
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        raise


class DataNotAvailableError(Exception):
    """Raised when data for a specific year/entity is not available."""
    pass


def download_and_extract(
    tempdir: pathlib.Path, entity: str, year: Optional[int] = None
) -> pathlib.Path:
    """Download and extract a given entity and year."""
    url = exporter_url_by_entity_and_year(entity, year)
    file_basename = get_basename_for_entity(entity, year)
    
    # Download to temporary file
    tmp_path = tempdir / TEMP_FILE_NAME
    download_file(url, tmp_path)
    
    try:
        # Process based on entity type
        if is_full_dataset_entity(entity):
            # These entities provide CSV directly
            csv_file = (tempdir / file_basename).with_suffix(".csv")
            shutil.move(tmp_path, csv_file)
        else:
            # These entities provide ZIP files
            csv_file = extract_zipfile(tmp_path)
            tmp_path.unlink(missing_ok=True)
        
        # Fix encoding issues
        csv_file = fix_encoding(csv_file)
        
        # Convert to compressed JSONL (create in temp directory)
        output_file = tempdir / (file_basename + ".jsonl.gz")
        csv_to_jsonl(csv_file, output_file)
        
        return output_file
        
    except Exception as e:
        logger.error(f"Failed to process {entity} {year}: {e}")
        tmp_path.unlink(missing_ok=True)
        raise


def process_entity(entity: str, output_dir: pathlib.Path = DEFAULT_OUTPUT_DIR):
    """Process a single entity - download and convert to JSONL."""
    logger.info(f"Processing {entity}")
    output_dir.mkdir(exist_ok=True, parents=True)
    
    if is_full_dataset_entity(entity):
        # Download full dataset
        with tempfile.TemporaryDirectory() as tempdir:
            tempdir = pathlib.Path(tempdir)
            jsonl_file = download_and_extract(tempdir, entity)
            
            # Move to final location
            final_path = output_dir / jsonl_file.name
            shutil.move(jsonl_file, final_path)
            logger.info(f"Completed {entity} -> {final_path}")
    else:
        # Download yearly data
        entity_years = get_entity_years(entity)
        if entity_years is None:
            logger.warning(f"No year range defined for {entity}")
            return
            
        for year in entity_years:
            try:
                # Use a separate temp directory for each year to avoid conflicts
                with tempfile.TemporaryDirectory() as year_tempdir:
                    year_tempdir = pathlib.Path(year_tempdir)
                    logger.info(f"Processing {entity} {year}")
                    jsonl_file = download_and_extract(year_tempdir, entity, year)
                    
                    # Move to final location
                    final_path = output_dir / jsonl_file.name
                    shutil.move(jsonl_file, final_path)
                    logger.info(f"Completed {entity} {year} -> {final_path}")
                    
            except DataNotAvailableError:
                logger.info(f"Data not available for {entity} {year} - skipping (this is normal for recent years)")
                continue
            except Exception as e:
                logger.error(f"Failed to process {entity} {year}: {e}")
                continue


def process_all_entities(output_dir: pathlib.Path = DEFAULT_OUTPUT_DIR):
    """Process all NIH Reporter entities."""
    logger.info("Starting NIH Reporter data processing")
    show_entity_configuration()
    
    for entity in ENTITIES:
        try:
            process_entity(entity, output_dir)
        except Exception as e:
            logger.error(f"Failed to process entity {entity}: {e}")
            continue
    
    logger.info("Completed NIH Reporter data processing")


@click.group()
def nih_reporter():
    """NIH Reporter ETL commands."""
    pass

@nih_reporter.command()
@click.argument('output_base', required=False, default=None)
def extract(output_base: str | None):
    """Extract data from NIH Reporter."""
    from omicidx_etl.config import settings
    base = UPath(output_base) if output_base else settings.publish_directory
    output_dir = base / "nih_reporter" / "raw"
    process_all_entities(output_dir)