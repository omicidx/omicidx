"""DuckLake load task: MERGE + DELETE raw pubmed → lake.<schema>.pubmed_article.

Design: full-snapshot MERGE (no high-water-mark)
-------------------------------------------------
PubMed raw Parquet is NOT hive-partitioned by date — it lives at a flat
path (``pubmed/raw/*.parquet``) that mixes baseline and daily-update
files. Because PubMed update packages can revise *any* historical PMID
(date_revised is not monotone across files), scoping the MERGE source by
a high-water mark would silently miss back-dated revisions. A full-
snapshot read is therefore correct.

The `_row_hash` gate makes this efficient: unchanged rows that match the
hash are not updated, so DuckLake (copy-on-write) writes no new data
files for them and only a trivial catalog snapshot is produced. The
merge is therefore incremental at the storage level even though the SQL
source is a full scan.

DELETE handling
---------------
Raw parquet rows with ``delete IS TRUE`` are PubMed retraction/deletion
records. After the MERGE (which excludes deleted PMIDs from the upsert
via ``WHERE delete IS NOT TRUE``) we run a DELETE inside its own labeled
transaction to purge any previously loaded PMIDs that subsequently
appeared as deletions.

This module intentionally does NOT modify ``ducklake.py`` or any shared
helper — the DELETE is entity-specific and stays here.
"""

import orjson
from omicidx.prefect.config import get_duckdb_path, get_ducklake_connection
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA, _commit_extra, merge_to_ducklake

from prefect import get_run_logger, task
from prefect.runtime import flow_run

# ---------------------------------------------------------------------------
# Source projection
# ---------------------------------------------------------------------------

# Mirrors the SELECT in consolidate.pubmed_parquet (authoritative column list)
# with the following additions:
#   - WHERE delete IS NOT TRUE  (exclude retraction records from the live table)
#   - QUALIFY dedup by (pmid, date_revised DESC, date_completed DESC)
#   - _row_hash over all non-key payload columns
#
# Double braces {{ }} are literal DuckDB struct syntax; only {path} is
# a Python format placeholder.
_PUBMED_SOURCE = """
SELECT * EXCLUDE (rn) FROM (
    SELECT
        trim(pmid)                     AS pmid,
        trim(title)                    AS title,
        trim(issue)                    AS issue,
        trim(pages)                    AS pages,
        trim(abstract)                 AS abstract,
        trim(journal)                  AS journal,
        authors,
        trim(pubdate)                  AS pubdate,
        trim(mesh_terms)               AS mesh_terms,
        trim(publication_types)        AS publication_types,
        trim(chemical_list)            AS chemical_list,
        trim(keywords)                 AS keywords,
        trim(doi)                      AS doi,
        "references",
        trim(languages)                AS languages,
        trim(vernacular_title)         AS vernacular_title,
        trim(date_completed)           AS date_completed,
        trim(date_revised)             AS date_revised,
        trim(pmc)                      AS pmc,
        trim(other_id)                 AS other_id,
        trim(medline_ta)               AS medline_ta,
        trim(nlm_unique_id)            AS nlm_unique_id,
        trim(issn_linking)             AS issn_linking,
        trim(country)                  AS country,
        grant_ids,
        md5(to_json({{
            title:              trim(title),
            issue:              trim(issue),
            pages:              trim(pages),
            abstract:           trim(abstract),
            journal:            trim(journal),
            authors:            authors,
            pubdate:            trim(pubdate),
            mesh_terms:         trim(mesh_terms),
            publication_types:  trim(publication_types),
            chemical_list:      trim(chemical_list),
            keywords:           trim(keywords),
            doi:                trim(doi),
            'references':       "references",
            languages:          trim(languages),
            vernacular_title:   trim(vernacular_title),
            date_completed:     trim(date_completed),
            date_revised:       trim(date_revised),
            pmc:                trim(pmc),
            other_id:           trim(other_id),
            medline_ta:         trim(medline_ta),
            nlm_unique_id:      trim(nlm_unique_id),
            issn_linking:       trim(issn_linking),
            country:            trim(country),
            grant_ids:          grant_ids
        }})) AS _row_hash,
        row_number() OVER (
            PARTITION BY pmid
            ORDER BY
                TRY_CAST(date_revised  AS DATE) DESC NULLS LAST,
                TRY_CAST(date_completed AS DATE) DESC NULLS LAST
        ) AS rn
    FROM read_parquet('{path}')
    WHERE delete IS NOT TRUE
) WHERE rn = 1
"""

# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------


@task(retries=1, retry_delay_seconds=60)
def pubmed_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """MERGE raw pubmed → lake.<lake_schema>.pubmed_article; DELETE retracted PMIDs.

    Full-snapshot strategy: all raw parquet files are scanned on every run.
    Unchanged rows (matching ``_row_hash``) generate no data writes in DuckLake.
    Rows with ``delete IS TRUE`` are excluded from the MERGE and then
    explicitly deleted from the lake table in a separate labeled transaction.

    Returns a dict with ``table``, ``row_count`` (post-merge), and
    ``deleted_count`` (PMIDs removed by the delete pass).
    """
    log = get_run_logger()
    raw = get_duckdb_path("pubmed", "raw", "*.parquet")
    source_sql = _PUBMED_SOURCE.format(path=raw)
    table = "pubmed_article"
    fqn = f"lake.{lake_schema}.{table}"

    with get_ducklake_connection() as con:
        log.info(f"Merging {raw} → {fqn}")
        rows = merge_to_ducklake(
            con,
            schema=lake_schema,
            table=table,
            source_sql=source_sql,
            key="pmid",
            commit_message=f"ducklake-load: {table} → {lake_schema}",
            commit_extra_info=_commit_extra(entity=table, source=raw),
        )
        log.info(f"{fqn} holds {rows:,} rows after merge")

        # -- delete retracted PMIDs ------------------------------------------
        # PubMed signals article deletions via rows with delete=TRUE in raw.
        # These rows were excluded from the MERGE above; now remove any
        # previously loaded PMIDs that appear in the delete set.
        delete_extra = orjson.dumps(
            {
                "prefect_run_id": flow_run.get_id(),
                "entity": table,
                "source": raw,
                "operation": "delete_retracted",
            }
        ).decode()

        delete_set = (
            f"SELECT DISTINCT trim(pmid) FROM read_parquet('{raw}') "
            "WHERE delete IS TRUE"
        )
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(
                "CALL ducklake_set_commit_message('lake', ?, ?, extra_info := ?)",
                [
                    "prefect:ducklake-load",
                    f"ducklake-load: {table} deletes → {lake_schema}",
                    delete_extra,
                ],
            )
            # DuckDB has no changes(); count rows that will go before deleting.
            deleted_count = con.execute(
                f"SELECT count(*) FROM {fqn} WHERE pmid IN ({delete_set})"
            ).fetchone()[0]
            con.execute(f"DELETE FROM {fqn} WHERE pmid IN ({delete_set})")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        log.info(f"Deleted {deleted_count:,} retracted PMIDs from {fqn}")

    return {
        "table": f"{lake_schema}.{table}",
        "row_count": rows,
        "deleted_count": deleted_count,
    }
