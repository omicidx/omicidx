"""DuckLake FULL-REPLACE derived-table loaders.

Unlike the incremental MERGE tasks in ducklake.py, these two entities are
derived / pre-filtered views of external sources that are always recomputed
from scratch. `replace_to_ducklake` does a CREATE OR REPLACE TABLE inside a
single stamped transaction.

Tables produced
---------------
lake.<schema>.sra_accessions
    Full projection of NCBI SRA_Accessions.tab (tens of millions of rows).
    Null sentinel '-' is handled by read_csv_auto(nullstr='-').

lake.<schema>.geo_series_with_rnaseq_counts
    Single-column accession list from the GEO RNA-seq counts parquet.
"""

from omicidx.prefect.config import get_duckdb_path, get_ducklake_connection
from omicidx.prefect.flows.ducklake import (
    LAKE_SCHEMA,
    _commit_extra,
    replace_to_ducklake,
)

from prefect import get_run_logger, task

_SRA_ACCESSIONS_URL = (
    "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab"
)


# ---------------------------------------------------------------------------
# sra_accessions
# ---------------------------------------------------------------------------

_SRA_ACCESSIONS_SQL = """
SELECT
    trim("Accession")   AS accession,
    trim("Submission")  AS submission,
    trim("Status")      AS status,
    "Updated"           AS updated,
    "Published"         AS published,
    "Received"          AS received,
    trim("Type")        AS type,
    trim("Center")      AS center,
    trim("Visibility")  AS visibility,
    trim("Alias")       AS alias,
    trim("Experiment")  AS experiment,
    trim("Sample")      AS sample,
    trim("Study")       AS study,
    "Loaded"            AS loaded,
    "Spots"             AS spots,
    "Bases"             AS bases,
    trim("Md5sum")      AS md5sum,
    trim("BioSample")   AS biosample,
    trim("BioProject")  AS bioproject,
    trim("ReplacedBy")  AS replacedby
FROM read_csv_auto(
    '{url}',
    nullstr = '-'
)
"""


@task(retries=1, retry_delay_seconds=60)
def sra_accessions_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """Full-replace lake.<lake_schema>.sra_accessions from SRA_Accessions.tab."""
    log = get_run_logger()
    source_sql = _SRA_ACCESSIONS_SQL.format(url=_SRA_ACCESSIONS_URL)
    log.info(
        f"Full-replace lake.{lake_schema}.sra_accessions from {_SRA_ACCESSIONS_URL}"
    )
    with get_ducklake_connection() as con:
        rows = replace_to_ducklake(
            con,
            schema=lake_schema,
            table="sra_accessions",
            source_sql=source_sql,
            commit_message=f"ducklake-load: sra_accessions -> {lake_schema}",
            commit_extra_info=_commit_extra(
                entity="sra_accessions",
                source=_SRA_ACCESSIONS_URL,
            ),
        )
    log.info(f"lake.{lake_schema}.sra_accessions now holds {rows:,} rows")
    return {"table": f"{lake_schema}.sra_accessions", "row_count": rows}


# ---------------------------------------------------------------------------
# geo_series_with_rnaseq_counts
# ---------------------------------------------------------------------------


@task(retries=1, retry_delay_seconds=60)
def geo_rnaseq_counts_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """Full-replace lake.<lake_schema>.geo_series_with_rnaseq_counts."""
    log = get_run_logger()
    raw = get_duckdb_path("geo", "raw", "gse_with_rna_seq_counts.parquet")
    source_sql = f"SELECT accession FROM read_parquet('{raw}') ORDER BY accession"
    log.info(
        f"Full-replace lake.{lake_schema}.geo_series_with_rnaseq_counts from {raw}"
    )
    with get_ducklake_connection() as con:
        rows = replace_to_ducklake(
            con,
            schema=lake_schema,
            table="geo_series_with_rnaseq_counts",
            source_sql=source_sql,
            commit_message=(
                f"ducklake-load: geo_series_with_rnaseq_counts -> {lake_schema}"
            ),
            commit_extra_info=_commit_extra(
                entity="geo_series_with_rnaseq_counts",
                source=raw,
            ),
        )
    log.info(
        f"lake.{lake_schema}.geo_series_with_rnaseq_counts now holds {rows:,} rows"
    )
    return {
        "table": f"{lake_schema}.geo_series_with_rnaseq_counts",
        "row_count": rows,
    }
