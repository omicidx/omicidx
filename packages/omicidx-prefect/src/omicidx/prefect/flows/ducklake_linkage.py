"""DuckLake derived table: publication ↔ accession linkage (ADR-0001).

`publication_accession_linkage` inverts the publication cross-references
that already live on the lake's dataset tables into a single, queryable
(pmid, accession) edge list. It is *derived* — no raw ingestion — built
purely from `lake.<schema>.{sra_study, geo_series, bioproject}`, so it
must run after those entities are merged.

Each source carries PubMed references differently:

- `sra_study.pubmed_ids` — VARCHAR[] of PMIDs (TRY_CAST to BIGINT).
- `geo_series.pubmed_id`  — BIGINT[] of PMIDs (already numeric; no trim).
- `bioproject.publications` — list of struct(pubdate, id, db). The `db`
  discriminator distinguishes the id namespace. Verified against the
  live catalog, the distinct values are `Pubmed` (324,829 rows, numeric
  PMID e.g. `37452013`), `DOI` (14,016, e.g. `10.1016/...`), `PMC`
  (226, e.g. `PMC6408609`), and `NotAvailable` (13). Only `Pubmed` rows
  are PMIDs, so we filter `db = 'Pubmed'` and emit `id` as the pmid.

The build is a full replace (`CREATE OR REPLACE TABLE ... AS SELECT`):
the table is small and cheaply rederived, and a clean rebuild avoids any
incremental-merge bookkeeping. PMIDs are `TRY_CAST` to BIGINT and rows
where the cast fails (or the source value is NULL) are dropped, so the
output is exactly the distinct, well-typed (pmid, accession,
accession_type) triples ADR-0001 specifies.

Like the merge path, the replace runs inside a single transaction that
also stamps the DuckLake commit — DuckLake clears the commit message on
commit, so an auto-committed statement would lose the stamp.
"""

import duckdb
from omicidx.prefect.config import get_ducklake_connection
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA, _commit_extra

from prefect import get_run_logger, task

# Value of bioproject.publications[].db that marks a real PMID. The other
# observed values (DOI, PMC, NotAvailable) carry non-PMID identifiers.
_BIOPROJECT_PUBMED_DB = "Pubmed"


def _linkage_select(schema: str) -> str:
    """The UNION-of-unnests SELECT producing the ADR-0001 triples.

    Wrapped in an outer DISTINCT projection that drops rows whose PMID
    failed to parse (TRY_CAST → NULL) or was NULL in the source.
    """
    return f"""
    SELECT DISTINCT pmid, accession, accession_type, source
    FROM (
        SELECT
            TRY_CAST(trim(u.pmid) AS BIGINT) AS pmid,
            accession,
            'sra_study' AS accession_type,
            'sra_study' AS source
        FROM lake.{schema}.sra_study, UNNEST(pubmed_ids) AS u(pmid)
        WHERE pubmed_ids IS NOT NULL

        UNION ALL

        SELECT
            TRY_CAST(u.pmid AS BIGINT) AS pmid,
            accession,
            'geo_series' AS accession_type,
            'geo_series' AS source
        FROM lake.{schema}.geo_series, UNNEST(pubmed_id) AS u(pmid)
        WHERE pubmed_id IS NOT NULL

        UNION ALL

        SELECT
            TRY_CAST(trim(u.pub.id) AS BIGINT) AS pmid,
            accession,
            'bioproject' AS accession_type,
            'bioproject' AS source
        FROM lake.{schema}.bioproject, UNNEST(publications) AS u(pub)
        WHERE publications IS NOT NULL AND u.pub.db = '{_BIOPROJECT_PUBMED_DB}'
    )
    WHERE pmid IS NOT NULL
    """


def build_publication_accession_linkage(
    con: duckdb.DuckDBPyConnection,
    *,
    schema: str,
    author: str = "prefect:ducklake-load",
    commit_message: str | None = None,
    commit_extra_info: str | None = None,
) -> None:
    """Full-replace `lake.<schema>.publication_accession_linkage`.

    The CREATE OR REPLACE and the commit stamp share one transaction so
    the snapshot is self-documenting in `lake.snapshots()`.
    """
    con.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")
    message = (
        commit_message or f"ducklake-load: publication_accession_linkage → {schema}"
    )
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            "CALL ducklake_set_commit_message('lake', ?, ?, extra_info := ?)",
            [author, message, commit_extra_info],
        )
        con.execute(
            f'CREATE OR REPLACE TABLE lake.{schema}."publication_accession_linkage" '
            f"AS {_linkage_select(schema)}"
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


@task(retries=1, retry_delay_seconds=60)
def publication_accession_linkage_to_ducklake(
    lake_schema: str = LAKE_SCHEMA,
) -> dict:
    """Build lake.<lake_schema>.publication_accession_linkage (ADR-0001).

    Inverts the PubMed cross-references already on sra_study, geo_series,
    and bioproject into distinct (pmid, accession, accession_type, source)
    rows. Returns the table name, total row count, and per-type counts.
    """
    log = get_run_logger()
    table = f"{lake_schema}.publication_accession_linkage"
    with get_ducklake_connection() as con:
        log.info(f"Building lake.{table} from lake.{lake_schema}.*")
        build_publication_accession_linkage(
            con,
            schema=lake_schema,
            commit_extra_info=_commit_extra(entity="publication_accession_linkage"),
        )
        rows = con.execute(
            f'SELECT count(*) FROM lake.{lake_schema}."publication_accession_linkage"'
        ).fetchone()[0]
        by_type = dict(
            con.execute(
                f"SELECT accession_type, count(*) "
                f'FROM lake.{lake_schema}."publication_accession_linkage" '
                f"GROUP BY accession_type ORDER BY accession_type"
            ).fetchall()
        )
    log.info(f"lake.{table} now holds {rows:,} rows ({by_type})")
    return {"table": table, "row_count": rows, "by_type": by_type}
