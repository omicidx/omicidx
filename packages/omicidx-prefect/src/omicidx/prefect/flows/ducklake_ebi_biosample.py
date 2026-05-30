"""DuckLake load task: MERGE raw ebi_biosample → lake.<schema>.ebi_biosample.

Follows the same pattern as bioproject_to_ducklake in ducklake.py:
  - Typed projection from read_ndjson_auto on the raw per-day NDJSON.GZ glob
  - union_by_name=true is REQUIRED — many partition days are empty files whose
    schema may differ from days with data
  - QUALIFY dedup on accession (the EBI BioSample API returns one record per
    accession per day; dedup is defensive for the rare same-accession update
    that crosses a day boundary)
  - md5(to_json({...payload...})) as _row_hash gates no-op UPDATEs
  - merge_to_ducklake() wraps CREATE TABLE IF NOT EXISTS + MERGE + commit stamp

Raw schema (from DESCRIBE read_ndjson_auto, union_by_name=true):
  accession VARCHAR            — natural key (EBI BioSample accession)
  name VARCHAR
  domain VARCHAR
  status VARCHAR
  release VARCHAR              — ISO 8601 timestamp string
  update VARCHAR               — ISO 8601 timestamp string (recency field)
  submitted VARCHAR            — ISO 8601 timestamp string
  create VARCHAR               — ISO 8601 timestamp string
  taxId BIGINT
  sraAccession VARCHAR
  submittedVia VARCHAR
  webinSubmissionAccountId VARCHAR
  characteristics STRUCT(...)[]
  externalReferences STRUCT(url VARCHAR, duo VARCHAR[])[]
  relationships STRUCT(source VARCHAR, type VARCHAR, target VARCHAR)[]
  publications STRUCT(pubmed_id VARCHAR, doi VARCHAR)[]
  organization STRUCT(...)[]
  contact STRUCT(...)[]
  certificates STRUCT(name VARCHAR, version VARCHAR, fileName VARCHAR)[]

Excluded from projection:
  _links — internal HAL navigation, not useful for analytics
  structuredData — deeply nested AMR/assay content, very sparse
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
# literal braces are doubled so str.format(path=...) leaves them as-is.
# union_by_name=true is REQUIRED because many days have zero records and their
# empty NDJSON.GZ files may infer a narrower schema than the full dataset.
# ---------------------------------------------------------------------------

_EBI_BIOSAMPLE_SOURCE = """
SELECT * EXCLUDE (rn) FROM (
    SELECT
        trim(accession)                 AS accession,
        trim(name)                      AS name,
        trim(domain)                    AS domain,
        trim(status)                    AS status,
        trim(release)                   AS release,
        trim("update")                  AS update,
        trim(submitted)                 AS submitted,
        trim("create")                  AS create,
        taxId                           AS taxId,
        trim(sraAccession)              AS sraAccession,
        trim(submittedVia)              AS submittedVia,
        trim(webinSubmissionAccountId)  AS webinSubmissionAccountId,
        characteristics,
        externalReferences,
        relationships,
        publications,
        organization,
        contact,
        certificates,
        md5(to_json({{
            'name':                     trim(name),
            'domain':                   trim(domain),
            'status':                   trim(status),
            'release':                  trim(release),
            'update':                   trim("update"),
            'submitted':                trim(submitted),
            'create':                   trim("create"),
            'taxId':                    taxId,
            'sraAccession':             trim(sraAccession),
            'submittedVia':             trim(submittedVia),
            'webinSubmissionAccountId': trim(webinSubmissionAccountId),
            'characteristics':          characteristics,
            'externalReferences':       externalReferences,
            'relationships':            relationships,
            'publications':             publications,
            'organization':             organization,
            'contact':                  contact,
            'certificates':             certificates
        }})) AS _row_hash,
        row_number() OVER (
            PARTITION BY trim(accession)
            ORDER BY trim("update") DESC NULLS LAST
        ) AS rn
    FROM read_ndjson_auto(
        '{path}',
        maximum_object_size = 1000000000,
        union_by_name = true
    )
    WHERE accession IS NOT NULL AND trim(accession) <> ''
) WHERE rn = 1
"""

@task(retries=1, retry_delay_seconds=60)
def ebi_biosample_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """MERGE raw ebi_biosample NDJSON → lake.<lake_schema>.ebi_biosample."""
    log = get_run_logger()
    raw = get_duckdb_path("ebi_biosample", "raw", "biosamples-*.ndjson.gz")
    source_sql = _EBI_BIOSAMPLE_SOURCE.format(path=raw)
    with get_ducklake_connection() as con:
        log.info(f"Merging {raw} → lake.{lake_schema}.ebi_biosample")
        rows = merge_to_ducklake(
            con,
            schema=lake_schema,
            table="ebi_biosample",
            source_sql=source_sql,
            key="accession",
            commit_message=f"ducklake-load: ebi_biosample → {lake_schema}",
            commit_extra_info=_commit_extra(entity="ebi_biosample", source=raw),
        )
    log.info(f"lake.{lake_schema}.ebi_biosample now holds {rows:,} rows")
    return {"table": f"{lake_schema}.ebi_biosample", "row_count": rows}
