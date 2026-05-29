"""Top-level DuckLake load flow.

Assembles every per-entity loader into one flow that MERGEs raw data into
`lake.<schema>.*`. Sits between `raw-extract` and `postgres-load` in the
daily pipeline (wired in P3). Shared helpers live in `ducklake.py`; each
entity's source projection + task lives in its own `ducklake_<entity>.py`
module so they can evolve independently.

Targets `LAKE_SCHEMA` (`omicidx_dev` during the transition; promote to
`omicidx` at cutover).
"""

from omicidx.prefect.flows.ducklake import (
    LAKE_SCHEMA,
    bioproject_to_ducklake,
    ducklake_maintenance,
)
from omicidx.prefect.flows.ducklake_biosample import biosample_to_ducklake
from omicidx.prefect.flows.ducklake_geo import (
    geo_platform_to_ducklake,
    geo_sample_to_ducklake,
    geo_series_to_ducklake,
)
from omicidx.prefect.flows.ducklake_pubmed import pubmed_to_ducklake
from omicidx.prefect.flows.ducklake_sra import (
    sra_experiment_to_ducklake,
    sra_run_to_ducklake,
    sra_sample_to_ducklake,
    sra_study_to_ducklake,
)

from prefect import flow


@flow(name="ducklake-load")
def ducklake_load_flow(lake_schema: str = LAKE_SCHEMA) -> None:
    """MERGE every entity's raw data into the DuckLake catalog.

    Tasks are independent (distinct lake tables); order is unconstrained.
    SRA loaders are high-water-mark incremental; the rest are
    full-snapshot with the `_row_hash` gate. PubMed also applies deletes.
    """
    bioproject_to_ducklake(lake_schema=lake_schema)
    biosample_to_ducklake(lake_schema=lake_schema)
    geo_series_to_ducklake(lake_schema=lake_schema)
    geo_sample_to_ducklake(lake_schema=lake_schema)
    geo_platform_to_ducklake(lake_schema=lake_schema)
    sra_study_to_ducklake(lake_schema=lake_schema)
    sra_sample_to_ducklake(lake_schema=lake_schema)
    sra_experiment_to_ducklake(lake_schema=lake_schema)
    sra_run_to_ducklake(lake_schema=lake_schema)
    pubmed_to_ducklake(lake_schema=lake_schema)


@flow(name="ducklake-maintenance")
def ducklake_maintenance_flow(
    expire_older_than: str = "now() - INTERVAL 30 DAY",
    compact: bool = True,
) -> None:
    """Scheduled (weekly) catalog maintenance: retention + compaction.

    Runs independently of ducklake-load. Reclaims R2 space pinned by
    expired snapshots and coalesces the small parquet files that daily
    incremental MERGEs accumulate.
    """
    ducklake_maintenance(expire_older_than=expire_older_than, compact=compact)


if __name__ == "__main__":
    ducklake_load_flow()
