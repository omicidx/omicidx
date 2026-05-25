"""Top-level pipeline flows.

`daily_pipeline_flow` mirrors the Dagster `daily_extract_schedule` plus
the downstream cascade — it runs the raw extracts, then consolidation,
then the postgres loads, then the duckdb build. Each step is a
subflow, so failure of one stage halts the rest with full visibility.
"""

from omicidx.prefect.flows.biosample import (
    bioproject_extract_flow,
    biosample_extract_flow,
)
from omicidx.prefect.flows.consolidate import consolidate_flow
from omicidx.prefect.flows.ebi_biosample import ebi_biosample_extract_flow
from omicidx.prefect.flows.geo import geo_extract_flow, geo_rna_seq_counts_flow
from omicidx.prefect.flows.postgres import postgres_load_flow
from omicidx.prefect.flows.pubmed import pubmed_extract_flow
from omicidx.prefect.flows.sql import omicidx_duckdb_flow
from omicidx.prefect.flows.sra import sra_extract_flow

from prefect import flow


@flow(name="raw-extract")
def raw_extract_flow(force: bool = False) -> None:
    """Run every raw extractor. Mirrors `daily_extract_schedule`."""
    biosample_extract_flow(force=force)
    bioproject_extract_flow(force=force)
    sra_extract_flow(force=force)
    geo_extract_flow(force=force)
    geo_rna_seq_counts_flow()
    ebi_biosample_extract_flow(force=force)
    pubmed_extract_flow(force=force)


@flow(name="daily-pipeline")
def daily_pipeline_flow(force: bool = False) -> None:
    """Daily end-to-end pipeline: extract → consolidate → load → build."""
    raw_extract_flow(force=force)
    consolidate_flow()
    postgres_load_flow()
    omicidx_duckdb_flow()


if __name__ == "__main__":
    daily_pipeline_flow()
