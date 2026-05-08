"""Dagster code location for OmicIDX ETL pipelines."""

from pathlib import Path

from dotenv import load_dotenv

# Load .env from the omicidx-dagster package directory
load_dotenv(Path(__file__).resolve().parents[3] / ".env", override=False)

from omicidx.dagster.defs.biosample import (  # noqa: E402
    bioproject_parquet,
    bioproject_raw,
    biosample_raw,
)
from omicidx.dagster.defs.consolidate import (  # noqa: E402
    biosample_parquet,
    geo_platforms_parquet,
    geo_rnaseq_counts_parquet,
    geo_samples_parquet,
    geo_series_parquet,
    pubmed_parquet,
    sra_accessions_etag_sensor,
    sra_accessions_external,
    sra_accessions_parquet,
    sra_experiments_parquet,
    sra_runs_parquet,
    sra_samples_parquet,
    sra_studies_parquet,
)
from omicidx.dagster.defs.ebi_biosample import (  # noqa: E402
    ebi_biosample_parquet,
    ebi_biosample_raw,
)
from omicidx.dagster.defs.geo import (  # noqa: E402
    geo_monthly_partitions,
    geo_raw,
    geo_rna_seq_counts,
)
from omicidx.dagster.defs.postgres import (  # noqa: E402
    bioproject_postgres,
    biosample_postgres,
    geo_platform_postgres,
    geo_sample_postgres,
    geo_series_postgres,
    pubmed_postgres,
    sra_experiment_postgres,
    sra_run_postgres,
    sra_sample_postgres,
    sra_study_postgres,
)
from omicidx.dagster.defs.pubmed import pubmed_raw, pubmed_sensor  # noqa: E402
from omicidx.dagster.defs.sql import omicidx_duckdb  # noqa: E402
from omicidx.dagster.defs.sra import sra_mirror_listing, sra_raw  # noqa: E402
from omicidx.dagster.resources import (  # noqa: E402
    DuckDBResource,
    OmicidxStorage,
    PostgresResource,
)

import dagster as dg  # noqa: E402

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
# Automation
# ---------------------------------------------------------------------------

automation_sensor = dg.AutomationConditionSensorDefinition(
    name="automation_sensor",
    target=dg.AssetSelection.all(),
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
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
        # EBI Biosample (daily-partitioned, separate from NCBI biosample)
        ebi_biosample_raw,
        ebi_biosample_parquet,
        # GEO
        geo_rna_seq_counts,
        geo_raw,
        # PubMed
        pubmed_raw,
        # SRA
        sra_mirror_listing,
        sra_raw,
        # Consolidation (raw → parquet)
        biosample_parquet,
        geo_platforms_parquet,
        geo_samples_parquet,
        geo_series_parquet,
        geo_rnaseq_counts_parquet,
        sra_studies_parquet,
        sra_samples_parquet,
        sra_experiments_parquet,
        sra_runs_parquet,
        sra_accessions_external,
        sra_accessions_parquet,
        pubmed_parquet,
        # DuckDB build
        omicidx_duckdb,
        # Postgres (API serving)
        bioproject_postgres,
        biosample_postgres,
        sra_study_postgres,
        sra_sample_postgres,
        sra_experiment_postgres,
        sra_run_postgres,
        geo_series_postgres,
        geo_sample_postgres,
        geo_platform_postgres,
        pubmed_postgres,
    ],
    schedules=[daily_extract_schedule, daily_geo_schedule],
    sensors=[pubmed_sensor, automation_sensor, sra_accessions_etag_sensor],
    resources={
        "storage": OmicidxStorage(),
        "duckdb_res": DuckDBResource(),
        "postgres": PostgresResource(),
    },
)
