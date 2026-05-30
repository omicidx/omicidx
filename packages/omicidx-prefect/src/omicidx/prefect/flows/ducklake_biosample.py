"""DuckLake load task: MERGE raw biosample → lake.<schema>.biosample.

Follows the same pattern as bioproject_to_ducklake in ducklake.py:
  - Typed projection from read_ndjson_auto on the raw JSONL.GZ
  - QUALIFY dedup on accession (one row / accession already, but defensive)
  - md5(to_json({...payload...})) as _row_hash gates no-op UPDATEs
  - merge_to_ducklake() wraps CREATE TABLE IF NOT EXISTS + MERGE + commit stamp
"""

from omicidx.prefect.config import get_duckdb_path, get_ducklake_connection
from omicidx.prefect.flows.ducklake import (
    LAKE_SCHEMA,
    _commit_extra,
    merge_to_ducklake,
)

from prefect import get_run_logger, task

# ---------------------------------------------------------------------------
# Source SQL template — {path} is the only single-brace token; all struct
# braces are doubled so str.format(path=...) leaves them literal.
# ---------------------------------------------------------------------------

_BIOSAMPLE_SOURCE = """
SELECT * EXCLUDE (rn) FROM (
    SELECT
        trim(submission_date) AS submission_date,
        trim(last_update)     AS last_update,
        trim(publication_date) AS publication_date,
        trim(access)          AS access,
        trim(id)              AS id,
        trim(accession)       AS accession,
        id_recs,
        ids,
        trim(sra_sample)      AS sra_sample,
        trim(dbgap)           AS dbgap,
        trim(gsm)             AS gsm,
        trim(title)           AS title,
        trim(description)     AS description,
        trim(taxonomy_name)   AS taxonomy_name,
        taxon_id,
        attribute_recs,
        attributes,
        trim(model)           AS model,
        md5(to_json({{
            submission_date:  trim(submission_date),
            last_update:      trim(last_update),
            publication_date: trim(publication_date),
            access:           trim(access),
            id:               trim(id),
            id_recs:          id_recs,
            ids:              ids,
            sra_sample:       trim(sra_sample),
            dbgap:            trim(dbgap),
            gsm:              trim(gsm),
            title:            trim(title),
            description:      trim(description),
            taxonomy_name:    trim(taxonomy_name),
            taxon_id:         taxon_id,
            attribute_recs:   attribute_recs,
            attributes:       attributes,
            model:            trim(model)
        }})) AS _row_hash,
        row_number() OVER (
            PARTITION BY trim(accession) ORDER BY last_update DESC NULLS LAST
        ) AS rn
    FROM read_ndjson_auto('{path}', maximum_object_size = 1000000000)
    WHERE accession IS NOT NULL AND trim(accession) <> ''
) WHERE rn = 1
"""


@task(retries=1, retry_delay_seconds=60)
def biosample_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """MERGE raw biosample JSONL → lake.<lake_schema>.biosample."""
    log = get_run_logger()
    raw = get_duckdb_path("biosample", "raw", "data.jsonl.gz")
    source_sql = _BIOSAMPLE_SOURCE.format(path=raw)
    with get_ducklake_connection() as con:
        log.info(f"Merging {raw} → lake.{lake_schema}.biosample")
        rows = merge_to_ducklake(
            con,
            schema=lake_schema,
            table="biosample",
            source_sql=source_sql,
            key="accession",
            commit_message=f"ducklake-load: biosample → {lake_schema}",
            commit_extra_info=_commit_extra(entity="biosample", source=raw),
        )
    log.info(f"lake.{lake_schema}.biosample now holds {rows:,} rows")
    return {"table": f"{lake_schema}.biosample", "row_count": rows}
