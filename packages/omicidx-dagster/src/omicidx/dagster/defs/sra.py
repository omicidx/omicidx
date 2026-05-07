"""SRA extract assets.

The NCBI SRA mirror publishes Full + Incremental XML dumps. This module
models the extraction as:
  - sra_mirror_listing: fetches the mirror file list, determines the current batch
  - sra_raw: partitioned by entity (study/sample/experiment/run), writes parquet
"""

import datetime
import gzip
import re
import shutil
import tempfile

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
from omicidx.parsers.sra.parser import sra_object_generator
from omicidx.dagster.resources import OmicidxStorage
from upath import UPath

ENTITIES = ["study", "sample", "experiment", "run"]

sra_entity_partitions = dg.StaticPartitionsDefinition(ENTITIES)


# ---------------------------------------------------------------------------
# PyArrow schemas (copied from omicidx-etl to avoid coupling)
# ---------------------------------------------------------------------------

# We import the schema helper inline to keep it simple.
# If we want full independence from omicidx-etl, we can copy the schema
# definitions here. For now, parsers + schema knowledge live together.


def _get_pyarrow_schema(entity: str) -> pa.Schema:
    """Get PyArrow schema for an SRA entity type.

    Inlined from omicidx.etl.sra.schema to avoid depending on omicidx-etl.
    """
    identifier_type = pa.struct(
        [("namespace", pa.string()), ("id", pa.string()), ("uuid", pa.string())]
    )
    attribute_type = pa.struct([("tag", pa.string()), ("value", pa.string())])
    xref_type = pa.struct([("db", pa.string()), ("id", pa.string())])
    file_alternative_type = pa.struct(
        [
            ("url", pa.string()),
            ("free_egress", pa.string()),
            ("access_type", pa.string()),
            ("org", pa.string()),
        ]
    )
    file_type = pa.struct(
        [
            ("cluster", pa.string()),
            ("filename", pa.string()),
            ("url", pa.string()),
            ("size", pa.int64()),
            ("date", pa.string()),
            ("md5", pa.string()),
            ("sratoolkit", pa.string()),
            ("alternatives", pa.list_(file_alternative_type)),
        ]
    )
    run_read_type = pa.struct(
        [
            ("index", pa.int64()),
            ("count", pa.int64()),
            ("mean_length", pa.float64()),
            ("sd_length", pa.float64()),
        ]
    )
    base_count_type = pa.struct([("base", pa.string()), ("count", pa.int64())])
    quality_type = pa.struct([("quality", pa.int32()), ("count", pa.int64())])
    tax_count_entry_type = pa.struct(
        [
            ("rank", pa.string()),
            ("name", pa.string()),
            ("parent", pa.int32()),
            ("total_count", pa.int64()),
            ("self_count", pa.int64()),
            ("tax_id", pa.int32()),
        ]
    )
    tax_analysis_type = pa.struct(
        [
            ("nspot_analyze", pa.int64()),
            ("total_spots", pa.int64()),
            ("mapped_spots", pa.int64()),
            ("tax_counts", pa.list_(tax_count_entry_type)),
        ]
    )
    experiment_read_type = pa.struct(
        [
            ("base_coord", pa.int64()),
            ("read_class", pa.string()),
            ("read_index", pa.int64()),
            ("read_type", pa.string()),
        ]
    )

    schemas = {
        "run": pa.schema(
            [
                ("accession", pa.string()),
                ("alias", pa.string()),
                ("experiment_accession", pa.string()),
                ("title", pa.string()),
                ("total_spots", pa.int64()),
                ("total_bases", pa.int64()),
                ("size", pa.int64()),
                ("avg_length", pa.float64()),
                ("identifiers", pa.list_(identifier_type)),
                ("attributes", pa.list_(attribute_type)),
                ("files", pa.list_(file_type)),
                ("reads", pa.list_(run_read_type)),
                ("base_counts", pa.list_(base_count_type)),
                ("qualities", pa.list_(quality_type)),
                ("tax_analysis", tax_analysis_type),
            ]
        ),
        "study": pa.schema(
            [
                ("accession", pa.string()),
                ("study_accession", pa.string()),
                ("alias", pa.string()),
                ("title", pa.string()),
                ("description", pa.string()),
                ("abstract", pa.string()),
                ("study_type", pa.string()),
                ("center_name", pa.string()),
                ("broker_name", pa.string()),
                ("BioProject", pa.string()),
                ("GEO", pa.string()),
                ("identifiers", pa.list_(identifier_type)),
                ("attributes", pa.list_(attribute_type)),
                ("xrefs", pa.list_(xref_type)),
                ("pubmed_ids", pa.list_(pa.string())),
            ]
        ),
        "sample": pa.schema(
            [
                ("accession", pa.string()),
                ("alias", pa.string()),
                ("title", pa.string()),
                ("organism", pa.string()),
                ("description", pa.string()),
                ("taxon_id", pa.int32()),
                ("geo", pa.string()),
                ("BioSample", pa.string()),
                ("identifiers", pa.list_(identifier_type)),
                ("attributes", pa.list_(attribute_type)),
                ("xrefs", pa.list_(xref_type)),
            ]
        ),
        "experiment": pa.schema(
            [
                ("accession", pa.string()),
                ("experiment_accession", pa.string()),
                ("alias", pa.string()),
                ("title", pa.string()),
                ("description", pa.string()),
                ("design", pa.string()),
                ("center_name", pa.string()),
                ("study_accession", pa.string()),
                ("sample_accession", pa.string()),
                ("platform", pa.string()),
                ("instrument_model", pa.string()),
                ("library_name", pa.string()),
                ("library_construction_protocol", pa.string()),
                ("library_layout", pa.string()),
                ("library_layout_orientation", pa.string()),
                ("library_layout_length", pa.string()),
                ("library_layout_sdev", pa.string()),
                ("library_strategy", pa.string()),
                ("library_source", pa.string()),
                ("library_selection", pa.string()),
                ("spot_length", pa.int64()),
                ("nreads", pa.int64()),
                ("identifiers", pa.list_(identifier_type)),
                ("attributes", pa.list_(attribute_type)),
                ("xrefs", pa.list_(xref_type)),
                ("reads", pa.list_(experiment_read_type)),
            ]
        ),
    }
    return schemas[entity]


# ---------------------------------------------------------------------------
# Mirror listing helpers
# ---------------------------------------------------------------------------


def _parse_mirror_entry(url: str) -> dict | None:
    """Parse a mirror URL into a metadata dict, or None if unparseable."""
    try:
        # Determine entity
        entity = None
        for e in ENTITIES:
            if e in url:
                entity = e
                break
        if entity is None:
            return None

        is_full = "Full" in url
        match = re.search(r"NCBI_SRA_Mirroring_(\d{8})", url)
        if not match:
            return None
        entry_date = datetime.datetime.strptime(match.group(1), "%Y%m%d").date()

        return {
            "url": url,
            "entity": entity,
            "is_full": is_full,
            "date": entry_date,
        }
    except (ValueError, AttributeError):
        return None


def _get_mirror_entries() -> list[dict]:
    """Fetch SRA mirror file list and determine current batch."""
    up = UPath("https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/")
    all_files = list(reversed([str(f) for f in up.glob("**/*set.xml.gz")]))

    entries = []
    found_full = False
    out_of_full = False

    for url in all_files:
        entry = _parse_mirror_entry(url)
        if entry is None:
            continue

        if entry["is_full"] and not found_full:
            found_full = True

        if found_full and not entry["is_full"]:
            out_of_full = True

        entry["in_current_batch"] = not out_of_full
        entries.append(entry)

    return entries


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------


@dg.asset(
    group_name="sra",
    kinds={"python"},
    tags={
        "layer": "raw",
        "cost": "low",
        "sla": "daily",
        "source": "ncbi_ftp",
    },
)
def sra_mirror_listing(
    context: dg.AssetExecutionContext,
) -> dict:
    """Fetch the SRA mirror file listing and determine current batch.

    Returns a dict mapping entity names to lists of URLs in the current batch.
    """
    entries = _get_mirror_entries()
    current = [e for e in entries if e["in_current_batch"]]
    context.log.info(
        f"Found {len(entries)} total entries, {len(current)} in current batch"
    )

    # Group current-batch URLs by entity
    urls_by_entity: dict[str, list[str]] = {e: [] for e in ENTITIES}
    for entry in current:
        urls_by_entity[entry["entity"]].append(entry["url"])

    for entity, urls in urls_by_entity.items():
        context.log.info(f"  {entity}: {len(urls)} files")

    return urls_by_entity


def _iter_records_from_url(url: str):
    """Stream a remote .xml.gz and yield parsed SRA record dicts."""
    up = UPath(url)
    with up.open("rb") as f_in, gzip.GzipFile(fileobj=f_in, mode="rb") as gz:
        for obj in sra_object_generator(gz):
            yield obj.data


def _write_parquet_chunks(
    url: str,
    entity: str,
    out_dir: UPath,
    context: dg.AssetExecutionContext,
    chunk_size: int = 500_000,
) -> tuple[int, int]:
    """Stream-parse an SRA XML file and write chunked parquet. Returns (records, parts)."""
    schema = _get_pyarrow_schema(entity)
    out_dir.mkdir(parents=True, exist_ok=True)

    buf: list[dict] = []
    part = 0
    total = 0

    def flush():
        nonlocal part
        if not buf:
            return
        table = pa.Table.from_pylist(buf, schema=schema)
        out_path = out_dir / f"data_{part:05d}.parquet"

        with tempfile.NamedTemporaryFile("wb", delete=False, suffix=".parquet") as tmp:
            tmp_path = tmp.name

        try:
            pq.write_table(table, tmp_path, compression="zstd")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp_path, "rb") as src, out_path.open("wb") as dst:
                shutil.copyfileobj(src, dst)
        finally:
            UPath(tmp_path).unlink(missing_ok=True)

        part += 1
        buf.clear()

    context.log.info(f"Processing {url}")
    for rec in _iter_records_from_url(url):
        buf.append(rec)
        total += 1
        if len(buf) >= chunk_size:
            flush()
            context.log.info(f"  {entity}: {total:,} records processed so far")

    flush()
    return total, part


@dg.asset(
    group_name="sra",
    kinds={"python", "parquet", "s3"},
    tags={
        "layer": "raw",
        "cost": "high",
        "sla": "daily",
        "source": "ncbi_ftp",
        "storage": "parquet",
    },
    partitions_def=sra_entity_partitions,
    deps=[sra_mirror_listing],
    retry_policy=dg.RetryPolicy(max_retries=2, delay=60),
)
def sra_raw(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    """Extract SRA data for a single entity type to parquet.

    Reads the current batch URLs from sra_mirror_listing and processes
    all Full + Incremental files for this entity.
    """
    entity = context.partition_key

    # Load the mirror listing to get URLs for this entity
    # Since sra_mirror_listing is unpartitioned and returns a dict,
    # we re-fetch it here (it's cheap — just an FTP listing).
    entries = _get_mirror_entries()
    current = [e for e in entries if e["in_current_batch"] and e["entity"] == entity]

    if not current:
        context.log.warning(f"No current-batch files for {entity}")
        return dg.MaterializeResult(
            metadata={"row_count": dg.MetadataValue.int(0)}
        )

    out_dir = storage.get_upath("sra", "raw", entity)

    total_records = 0
    total_parts = 0

    for entry in current:
        date_str = entry["date"].strftime("%Y-%m-%d")
        stage = "Full" if entry["is_full"] else "Incremental"
        partition_dir = out_dir / f"date={date_str}" / f"stage={stage}"

        context.log.info(f"Processing {entity} {stage} {date_str}")
        records, parts = _write_parquet_chunks(
            url=entry["url"],
            entity=entity,
            out_dir=partition_dir,
            context=context,
        )
        total_records += records
        total_parts += parts

    context.log.info(
        f"{entity}: wrote {total_records:,} records in {total_parts} parquet parts"
    )

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(total_records),
            "parquet_parts": dg.MetadataValue.int(total_parts),
            "files_processed": dg.MetadataValue.int(len(current)),
            "output_dir": dg.MetadataValue.text(str(out_dir)),
        }
    )
