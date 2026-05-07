"""Dagster code location for OmicIDX ETL pipelines."""

from pathlib import Path

from dotenv import load_dotenv

# Load .env from the omicidx-dagster package directory
load_dotenv(Path(__file__).resolve().parents[3] / ".env", override=False)

import dagster as dg  # noqa: E402

from omicidx.dagster.defs.biosample import bioproject_parquet, bioproject_raw, biosample_raw
from omicidx.dagster.defs.geo import geo_monthly_partitions, geo_raw, geo_rna_seq_counts
from omicidx.dagster.defs.pubmed import pubmed_raw, pubmed_sensor
from omicidx.dagster.defs.sql import consolidated_parquet, omicidx_duckdb
from omicidx.dagster.defs.sra import sra_mirror_listing, sra_raw
from omicidx.dagster.resources import DuckDBResource, OmicidxStorage

# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------

daily_extract_schedule = dg.ScheduleDefinition(
    name="daily_extract",
    cron_schedule="0 2 * * *",  # 2 AM daily
    target=dg.AssetSelection.assets(
        biosample_raw,
        bioproject_raw,
        sra_mirror_listing,
        sra_raw,
        geo_rna_seq_counts,
    ),
)

geo_extract_job = dg.define_asset_job(
    name="geo_extract_job",
    selection=dg.AssetSelection.assets(geo_raw),
    partitions_def=geo_monthly_partitions,
)

daily_geo_schedule = dg.build_schedule_from_partitioned_job(
    name="daily_geo",
    job=geo_extract_job,
)


# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------

defs = dg.Definitions(
    assets=[
        # Biosample
        biosample_raw,
        bioproject_raw,
        bioproject_parquet,
        # GEO
        geo_rna_seq_counts,
        geo_raw,
        # PubMed
        pubmed_raw,
        # SRA
        sra_mirror_listing,
        sra_raw,
        # SQL
        consolidated_parquet,
        omicidx_duckdb,
    ],
    schedules=[daily_extract_schedule, daily_geo_schedule],
    sensors=[pubmed_sensor],
    resources={
        "storage": OmicidxStorage(),
        "duckdb_res": DuckDBResource(),
    },
)
