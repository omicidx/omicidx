"""Top-level pipeline flows.

`daily_pipeline_flow` runs the raw extracts, then the DuckLake load
(MERGE raw → lake.omicidx.*), then the postgres loads (which read from
the lake), then the duckdb build. Each step is a subflow, so failure of
one stage halts the rest with full visibility.

The old `consolidate` step (raw → consolidated parquet) is replaced by
`ducklake-load`. `consolidate.py` is retained for now only because the
public duckdb-build SQL views still read published parquet; that build
will be repointed to the lake separately.
"""

from omicidx.prefect.flows.biosample import (
    bioproject_extract_flow,
    biosample_extract_flow,
)
from omicidx.prefect.flows.ducklake_load import ducklake_load_flow
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
    """Daily end-to-end pipeline: extract → ducklake-load → postgres → build."""
    raw_extract_flow(force=force)
    ducklake_load_flow()
    postgres_load_flow()
    omicidx_duckdb_flow()


if __name__ == "__main__":
    daily_pipeline_flow()
