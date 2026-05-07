"""PubMed extract assets with dynamic partitions.

Each PubMed baseline/update XML file becomes a partition. A sensor polls
the FTP listing and adds new partition keys as files appear.
"""

import re
import shutil
import tempfile
from datetime import datetime
from urllib.request import urlretrieve

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
import pubmed_parser as pp
from omicidx.dagster.resources import OmicidxStorage
from upath import UPath

PUBMED_BASE = UPath("https://ftp.ncbi.nlm.nih.gov/pubmed")
_XML_GZ_RE = re.compile(r"^(pubmed\d+n\d+)\.xml\.gz$")

pubmed_partitions = dg.DynamicPartitionsDefinition(name="pubmed_files")


def _list_pubmed_files() -> dict[str, str]:
    """List PubMed XML files via HTTPS. Returns {partition_key: url_string}."""
    result: dict[str, str] = {}
    for subdir in ["baseline", "updatefiles"]:
        for entry in (PUBMED_BASE / subdir).iterdir():
            m = _XML_GZ_RE.match(entry.name)
            if m:
                result[m.group(1)] = str(entry)
    return result


@dg.asset(
    group_name="pubmed",
    kinds={"python", "parquet", "s3"},
    tags={
        "layer": "raw",
        "cost": "medium",
        "sla": "daily",
        "source": "pubmed_ftp",
        "storage": "parquet",
    },
    partitions_def=pubmed_partitions,
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
)
def pubmed_raw(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    """Extract a single PubMed XML file to Parquet."""
    partition_key = context.partition_key
    available = _list_pubmed_files()

    if partition_key not in available:
        raise dg.Failure(f"PubMed file {partition_key} not found in FTP listing")

    url = available[partition_key]
    output_dir = storage.get_upath("pubmed", "raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{partition_key}.parquet"

    with (
        tempfile.NamedTemporaryFile(suffix=".xml.gz") as tmp_xml,
        tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_parquet,
    ):
        context.log.info(f"Downloading {url}")
        urlretrieve(str(url), filename=tmp_xml.name)

        context.log.info(f"Parsing {partition_key}")
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

        table = pa.Table.from_pylist(articles)
        pq.write_table(table, tmp_parquet.name, compression="zstd")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp_parquet.name, "rb") as src, output_path.open("wb") as dst:
            shutil.copyfileobj(src, dst)

    context.log.info(f"Wrote {len(articles)} articles to {output_path}")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(articles)),
            "output_path": dg.MetadataValue.text(str(output_path)),
            "source_url": dg.MetadataValue.url(str(url)),
        }
    )


@dg.sensor(
    asset_selection=dg.AssetSelection.assets(pubmed_raw),
    minimum_interval_seconds=3600,
)
def pubmed_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
    """Poll NCBI FTP for new PubMed files and add them as dynamic partitions."""
    available = _list_pubmed_files()
    available_ids = set(available.keys())

    existing = set(
        context.instance.get_dynamic_partitions("pubmed_files")
    )

    new_ids = sorted(available_ids - existing)

    if not new_ids:
        context.log.info("No new PubMed files found")
        return dg.SensorResult(run_requests=[], dynamic_partitions_requests=[])

    context.log.info(f"Found {len(new_ids)} new PubMed files")

    return dg.SensorResult(
        dynamic_partitions_requests=[
            pubmed_partitions.build_add_request(new_ids)
        ],
        run_requests=[
            dg.RunRequest(
                partition_key=pid,
            )
            for pid in new_ids
        ],
    )
