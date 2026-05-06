"""
Parquet processing for SRA mirror entries.

This module provides functions to download, parse, and write SRA mirror entries
as parquet files in bounded-memory chunks.
"""
from __future__ import annotations

import gzip
import shutil
import tempfile
from typing import Callable, Iterable, Optional
from xml.etree.ElementTree import ParseError

from loguru import logger
from upath import UPath

from omicidx.sra.parser import sra_object_generator
from .schema import get_pyarrow_schema


NormalizeFn = Callable[[dict, object], dict]


def iter_sra_record_dicts_from_mirror_url(url: str) -> Iterable[dict]:
    """
    Stream remote .xml.gz -> yield dict records without staging the .gz locally.
    
    Args:
        url: URL to the SRA mirror .xml.gz file
        
    Yields:
        Parsed SRA record dictionaries
    """
    up = UPath(url)
    with up.open("rb") as f_in:
        with gzip.GzipFile(fileobj=f_in, mode="rb") as gz:
            for obj in sra_object_generator(gz):
                yield obj.data


def process_mirror_entry_to_parquet_parts(
    *,
    url: str,
    out_dir: UPath,
    entity: str,
    normalize_fn: Optional[NormalizeFn] = None,
    basename: str = "data",
) -> list[UPath]:
    """
    Stream parse + write parquet in bounded-memory chunks.
    
    Writes parquet files to {out_dir}/{basename}_{part:05d}.parquet
    
    Args:
        url: URL to the SRA mirror .xml.gz file
        out_dir: Output directory (can be local or remote)
        entity: SRA entity type (run, study, sample, experiment)
        schema: PyArrow schema (defaults to PYARROW_SCHEMAS[entity])
        normalize_fn: Optional function to normalize records
        basename: Base filename for parquet parts
        
    Returns:
        List of written parquet file paths
    """
    CHUNK_SIZE = 500_000
    compression = "zstd"
    
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = get_pyarrow_schema(entity)

    out_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Pushing to output directory: {out_dir}")

    buf: list[dict] = []
    part = 0
    written: list[UPath] = []

    def flush() -> None:
        nonlocal part
        if not buf:
            return

        table = pa.Table.from_pylist(buf, schema=schema)
        out_path = out_dir / f"{basename}_{part:05d}.parquet"

        # Stage locally (seekable) then upload to final location, then delete.
        with tempfile.NamedTemporaryFile("wb", delete=False, suffix=".parquet") as tmp:
            tmp_path = tmp.name

        try:
            pq.write_table(table, tmp_path, compression=compression)

            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp_path, "rb") as f_in, out_path.open("wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
                logger.debug(f"Uploaded parquet part to: {out_path}")

            written.append(out_path)
            logger.debug(f"Wrote parquet part: {out_path}")
        finally:
            UPath(tmp_path).unlink(missing_ok=True)

        part += 1
        buf.clear()

    logger.info(f"Processing {url}", entity=entity, chunk_size=CHUNK_SIZE)

    try:
        for rec in iter_sra_record_dicts_from_mirror_url(url):
            if normalize_fn is not None:
                rec = normalize_fn(rec, schema)
            buf.append(rec)

            if len(buf) >= CHUNK_SIZE:
                flush()
    except ParseError as e:
        logger.error(
            f"XML parse error in {url}: {e}. "
            f"Flushing {len(buf)} buffered records. "
            f"Already wrote {len(written)} parts."
        )
        flush()
        raise

    flush()
    return written