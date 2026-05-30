"""DuckLake load flow: MERGE raw → lake.<schema>.* (incremental).

Each entity is merged into the DuckLake catalog by its natural key. The
MERGE source is a deduped, typed projection of the raw data; a per-row
content hash (`_row_hash`) gates UPDATEs so unchanged rows never rewrite
a data file — DuckLake is copy-on-write, so an idempotent re-run writes
no new files and only a trivial snapshot.

This sits between `raw-extract` and `postgres-load`. Loaders write to
`LAKE_SCHEMA` (production `omicidx`); pass an explicit `lake_schema` to
target a development schema (e.g. `omicidx_dev`) for validation.

`cdsci-lake` (the catalog's data bucket) is ducklake-controlled
exclusively. Raw inputs are read from PUBLISH_ROOT (a different bucket)
via `get_duckdb_path`; nothing else is written into the lake bucket.
"""

from contextlib import contextmanager

import duckdb
import orjson
from omicidx.prefect.config import get_duckdb_path, get_ducklake_connection
from omicidx.prefect.semaphore import SemaphoreStore

from prefect import get_run_logger, task
from prefect.runtime import flow_run

# Production lake schema. (Was omicidx_dev during the transition.)
LAKE_SCHEMA = "omicidx"


def _commit_extra(**fields: object) -> str:
    """JSON blob for a snapshot's commit_extra_info, tagged with run id."""
    return orjson.dumps({"prefect_run_id": flow_run.get_id(), **fields}).decode()


class HighWaterMark:
    """Track the highest raw-partition watermark merged into the lake.

    Source-incremental entities (SRA is hive-partitioned by `date`/`stage`)
    scope their MERGE source to partitions at or beyond the stored
    watermark, then advance it after a successful merge. Reads are made
    inclusive (`>=`) so same-day later stages are never skipped; the
    `_row_hash` gate makes re-reading the boundary partition a no-op.

    Backed by a semaphore file under namespace `ducklake/<entity>`
    (key `latest`), so it lists/clears alongside the raw semaphores and
    a backfill is just "clear the watermark, re-run".
    """

    def __init__(self, entity: str, lake_schema: str = LAKE_SCHEMA) -> None:
        # Scope by schema so a dev-schema run never advances the prod
        # watermark (which would silently skip un-loaded prod partitions).
        self._sem = SemaphoreStore(f"ducklake/{lake_schema}/{entity}")
        self._key = "latest"

    def get(self) -> str | None:
        rec = self._sem.read(self._key)
        return (rec or {}).get("metadata", {}).get("high_water")

    def set(self, value: str, **extra: object) -> None:
        self._sem.mark_done(self._key, metadata={"high_water": value, **extra})


@contextmanager
def _stamped_txn(
    con: duckdb.DuckDBPyConnection,
    author: str,
    message: str,
    extra_info: str | None,
):
    """Wrap DML in a transaction stamped with snapshot commit metadata.

    The stamp MUST share a transaction with the DML — DuckLake clears it
    on commit, so an auto-committed statement would lose it. A no-op DML
    writes no snapshot, so the stamp simply doesn't land.
    """
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            "CALL ducklake_set_commit_message('lake', ?, ?, extra_info := ?)",
            [author, message, extra_info],
        )
        yield
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def _src_columns(con: duckdb.DuckDBPyConnection, view: str = "_merge_src") -> list[str]:
    return [row[0] for row in con.execute(f"DESCRIBE {view}").fetchall()]


def merge_to_ducklake(
    con: duckdb.DuckDBPyConnection,
    *,
    schema: str,
    table: str,
    source_sql: str,
    key: str,
    hash_col: str = "_row_hash",
    author: str = "prefect:ducklake-load",
    commit_message: str | None = None,
    commit_extra_info: str | None = None,
) -> int:
    """MERGE a deduped source projection into lake.<schema>.<table>.

    The target table is created (empty) from the source projection on
    first run so its schema is locked to exactly what the MERGE writes.
    Subsequent runs hash-gate UPDATEs and INSERT unmatched rows.

    The caller's `source_sql` MUST yield at most one row per `key`
    (MERGE rejects multiple source matches) and include `key` and
    `hash_col`. The columns to UPDATE are derived from the source view
    (every column except the join key), so the projection is the single
    source of truth for the column set.
    """
    con.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")
    con.execute(f"CREATE OR REPLACE TEMP VIEW _merge_src AS {source_sql}")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS lake.{schema}.{table} AS "
        "SELECT * FROM _merge_src WHERE false"
    )
    # Update every non-key column. Quote identifiers so reserved-word
    # columns (e.g. pubmed's "references") are valid in the SET list.
    update_cols = [c for c in _src_columns(con) if c != key]
    set_clause = ", ".join(f'"{c}" = src."{c}"' for c in update_cols)
    message = commit_message or f"ducklake-load: merge {schema}.{table}"
    with _stamped_txn(con, author, message, commit_extra_info):
        con.execute(f"""
            MERGE INTO lake.{schema}.{table} tgt
            USING _merge_src src ON tgt."{key}" = src."{key}"
            WHEN MATCHED AND tgt."{hash_col}" <> src."{hash_col}"
                THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT *
        """)
    return con.execute(f"SELECT count(*) FROM lake.{schema}.{table}").fetchone()[0]


def replace_to_ducklake(
    con: duckdb.DuckDBPyConnection,
    *,
    schema: str,
    table: str,
    source_sql: str,
    author: str = "prefect:ducklake-load",
    commit_message: str | None = None,
    commit_extra_info: str | None = None,
) -> int:
    """Full-replace lake.<schema>.<table> with a derived query result.

    For derived tables that are cheaper to rebuild than to merge
    (sra_accessions, geo_series_with_rnaseq_counts, the linkage table).
    Stamped like `merge_to_ducklake` so snapshots stay self-documenting.
    """
    con.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")
    message = commit_message or f"ducklake-load: replace {schema}.{table}"
    with _stamped_txn(con, author, message, commit_extra_info):
        con.execute(f'CREATE OR REPLACE TABLE lake.{schema}."{table}" AS {source_sql}')
    return con.execute(f'SELECT count(*) FROM lake.{schema}."{table}"').fetchone()[0]


# -- bioproject (POC) ----------------------------------------------------------

# Full-dump source: one record per accession already, but we dedup
# defensively and hash the payload to gate no-op rewrites.
_BIOPROJECT_SOURCE = """
SELECT * EXCLUDE (rn) FROM (
    SELECT
        trim(accession) AS accession,
        trim(title) AS title,
        trim(description) AS description,
        trim(name) AS name,
        publications,
        locus_tags,
        release_date,
        data_types,
        external_links,
        md5(to_json({{
            title: trim(title), description: trim(description),
            name: trim(name), publications: publications,
            locus_tags: locus_tags, release_date: release_date,
            data_types: data_types, external_links: external_links
        }})) AS _row_hash,
        row_number() OVER (
            PARTITION BY trim(accession) ORDER BY release_date DESC NULLS LAST
        ) AS rn
    FROM read_ndjson_auto('{path}', maximum_object_size = 1000000000)
    WHERE accession IS NOT NULL AND trim(accession) <> ''
) WHERE rn = 1
"""


@task(retries=1, retry_delay_seconds=60)
def bioproject_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """MERGE raw bioproject JSONL → lake.<lake_schema>.bioproject."""
    log = get_run_logger()
    raw = get_duckdb_path("bioproject", "raw", "data.jsonl.gz")
    source_sql = _BIOPROJECT_SOURCE.format(path=raw)
    with get_ducklake_connection() as con:
        log.info(f"Merging {raw} → lake.{lake_schema}.bioproject")
        rows = merge_to_ducklake(
            con,
            schema=lake_schema,
            table="bioproject",
            source_sql=source_sql,
            key="accession",
            commit_message=f"ducklake-load: bioproject → {lake_schema}",
            commit_extra_info=_commit_extra(entity="bioproject", source=raw),
        )
    log.info(f"lake.{lake_schema}.bioproject now holds {rows:,} rows")
    return {"table": f"{lake_schema}.bioproject", "row_count": rows}


# -- maintenance ---------------------------------------------------------------


@task(retries=1, retry_delay_seconds=60)
def ducklake_maintenance(
    expire_older_than: str = "now() - INTERVAL 30 DAY",
    compact: bool = True,
) -> dict:
    """Expire old snapshots, delete their data files, and compact.

    DROP/rewrite in DuckLake only unlinks in the catalog; reclaiming R2
    space needs expire_snapshots + cleanup_old_files. Compaction
    (merge_adjacent_files) coalesces the many small parquet files that
    incremental MERGEs accumulate. Default retention is 30 days of
    snapshots (appropriate for incremental tables; full-snapshot tables
    keep little useful history, so a tighter window can be passed).
    """
    log = get_run_logger()
    with get_ducklake_connection() as con:
        con.execute(
            f"CALL ducklake_expire_snapshots('lake', older_than => {expire_older_than})"
        )
        deleted = con.execute(
            "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)"
        ).fetchall()
        if compact:
            con.execute("CALL ducklake_merge_adjacent_files('lake')")
        remaining = con.execute("SELECT count(*) FROM lake.snapshots()").fetchone()[0]
    log.info(
        f"Cleaned {len(deleted)} orphaned files; compact={compact}; "
        f"{remaining} snapshots remain"
    )
    return {
        "files_deleted": len(deleted),
        "compacted": compact,
        "snapshots_remaining": remaining,
    }
