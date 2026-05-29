"""DuckLake load flow for the four SRA entities (incremental by date).

SRA raw lands as hive-partitioned parquet under `sra/raw/<entity>/date=.../
stage=.../*.parquet`. Each entity here is MERGEd into the DuckLake catalog
by `accession`, reusing the deduped/typed projection from `consolidate.py`
(those SELECTs are authoritative).

Because SRA is large and grows daily, these tasks are *source-incremental*:
a `HighWaterMark` per entity stores the highest raw `date` partition already
merged. On a normal run the MERGE source only scans partitions with
`date >= <high_water>` (INCLUSIVE — re-reading the boundary day is a safe
no-op because the `_row_hash` gate suppresses unchanged-row UPDATEs and
DuckLake is copy-on-write). A `force=True` run drops the filter and scans
all partitions (full backfill / reconciliation).

After a successful merge the watermark is advanced to `max(date)` present in
raw. `cdsci-lake` (the catalog bucket) is ducklake-controlled exclusively;
raw is read from PUBLISH_ROOT via `get_duckdb_path`.
"""

import duckdb
from omicidx.prefect.config import get_duckdb_path, get_ducklake_connection
from omicidx.prefect.flows.ducklake import (
    LAKE_SCHEMA,
    HighWaterMark,
    _commit_extra,
    merge_to_ducklake,
)

from prefect import flow, get_run_logger, task

# ---------------------------------------------------------------------------
# Per-entity source projections.
#
# Each is a python str.format(path=..., filt=...) template:
#   {path} — the r2:// glob for the entity's raw partitions
#   {filt} — "" or "date >= '<high_water>'" (the incremental scope)
# Struct literals for md5(to_json(...)) are written with DOUBLED braces so
# str.format leaves them intact. The column lists mirror consolidate.py's
# sra_*_parquet projections exactly (those are authoritative); we add a
# payload `_row_hash` and the standard date/stage dedup QUALIFY.
# ---------------------------------------------------------------------------

_STUDY_SOURCE = """
SELECT
    trim(accession) AS accession,
    trim(study_accession) AS study_accession,
    trim(alias) AS alias, trim(title) AS title,
    trim(description) AS description,
    trim(abstract) AS abstract,
    trim(study_type) AS study_type,
    trim(center_name) AS center_name,
    trim(broker_name) AS broker_name,
    trim("BioProject") AS bioproject,
    trim("GEO") AS geo,
    identifiers, attributes, xrefs, pubmed_ids,
    md5(to_json({{
        study_accession: trim(study_accession), alias: trim(alias),
        title: trim(title), description: trim(description),
        abstract: trim(abstract), study_type: trim(study_type),
        center_name: trim(center_name), broker_name: trim(broker_name),
        bioproject: trim("BioProject"), geo: trim("GEO"),
        identifiers: identifiers, attributes: attributes,
        xrefs: xrefs, pubmed_ids: pubmed_ids
    }})) AS _row_hash
FROM read_parquet('{path}', hive_partitioning=true)
{filt}
QUALIFY row_number() OVER (
    PARTITION BY accession ORDER BY date DESC, stage DESC
) = 1
"""

_STUDY_UPDATE_COLS = [
    "study_accession", "alias", "title", "description", "abstract",
    "study_type", "center_name", "broker_name", "bioproject", "geo",
    "identifiers", "attributes", "xrefs", "pubmed_ids", "_row_hash",
]

_SAMPLE_SOURCE = """
SELECT
    trim(accession) AS accession,
    trim(alias) AS alias, trim(title) AS title,
    trim(organism) AS organism,
    trim(description) AS description,
    taxon_id,
    trim("BioSample") AS biosample,
    identifiers, attributes, xrefs,
    md5(to_json({{
        alias: trim(alias), title: trim(title),
        organism: trim(organism), description: trim(description),
        taxon_id: taxon_id, biosample: trim("BioSample"),
        identifiers: identifiers, attributes: attributes, xrefs: xrefs
    }})) AS _row_hash
FROM read_parquet('{path}', hive_partitioning=true)
{filt}
QUALIFY row_number() OVER (
    PARTITION BY accession ORDER BY date DESC, stage DESC
) = 1
"""

_SAMPLE_UPDATE_COLS = [
    "alias", "title", "organism", "description", "taxon_id", "biosample",
    "identifiers", "attributes", "xrefs", "_row_hash",
]

_EXPERIMENT_SOURCE = """
SELECT
    trim(accession) AS accession,
    trim(experiment_accession) AS experiment_accession,
    trim(alias) AS alias, trim(title) AS title,
    trim(design) AS design,
    trim(center_name) AS center_name,
    trim(study_accession) AS study_accession,
    trim(sample_accession) AS sample_accession,
    trim(platform) AS platform,
    trim(instrument_model) AS instrument_model,
    trim(library_name) AS library_name,
    trim(library_construction_protocol) AS library_construction_protocol,
    trim(library_layout) AS library_layout,
    trim(library_layout_length) AS library_layout_length,
    trim(library_layout_sdev) AS library_layout_sdev,
    trim(library_strategy) AS library_strategy,
    trim(library_source) AS library_source,
    trim(library_selection) AS library_selection,
    spot_length, nreads,
    identifiers, attributes, xrefs, reads,
    md5(to_json({{
        experiment_accession: trim(experiment_accession),
        alias: trim(alias), title: trim(title), design: trim(design),
        center_name: trim(center_name),
        study_accession: trim(study_accession),
        sample_accession: trim(sample_accession),
        platform: trim(platform),
        instrument_model: trim(instrument_model),
        library_name: trim(library_name),
        library_construction_protocol: trim(library_construction_protocol),
        library_layout: trim(library_layout),
        library_layout_length: trim(library_layout_length),
        library_layout_sdev: trim(library_layout_sdev),
        library_strategy: trim(library_strategy),
        library_source: trim(library_source),
        library_selection: trim(library_selection),
        spot_length: spot_length, nreads: nreads,
        identifiers: identifiers, attributes: attributes,
        xrefs: xrefs, reads: reads
    }})) AS _row_hash
FROM read_parquet('{path}', hive_partitioning=true)
{filt}
QUALIFY row_number() OVER (
    PARTITION BY accession ORDER BY date DESC, stage DESC
) = 1
"""

_EXPERIMENT_UPDATE_COLS = [
    "experiment_accession", "alias", "title", "design", "center_name",
    "study_accession", "sample_accession", "platform", "instrument_model",
    "library_name", "library_construction_protocol", "library_layout",
    "library_layout_length", "library_layout_sdev", "library_strategy",
    "library_source", "library_selection", "spot_length", "nreads",
    "identifiers", "attributes", "xrefs", "reads", "_row_hash",
]

_RUN_SOURCE = """
SELECT
    trim(accession) AS accession,
    trim(alias) AS alias,
    trim(experiment_accession) AS experiment_accession,
    trim(title) AS title,
    identifiers, attributes, qualities,
    md5(to_json({{
        alias: trim(alias),
        experiment_accession: trim(experiment_accession),
        title: trim(title), identifiers: identifiers,
        attributes: attributes, qualities: qualities
    }})) AS _row_hash
FROM read_parquet('{path}', hive_partitioning=true)
{filt}
QUALIFY row_number() OVER (
    PARTITION BY accession ORDER BY date DESC, stage DESC
) = 1
"""

_RUN_UPDATE_COLS = [
    "alias", "experiment_accession", "title", "identifiers", "attributes",
    "qualities", "_row_hash",
]


def _merge_sra(
    entity: str,
    table: str,
    raw_subdir: str,
    source_template: str,
    update_cols: list[str],
    lake_schema: str,
    force: bool,
) -> dict:
    """Incrementally MERGE one SRA entity's raw partitions into the lake.

    Shared body for the four public tasks. Wires the per-entity
    `HighWaterMark`: read the stored watermark, scope the MERGE source to
    `date >= <watermark>` (unless first run or force), merge, then advance
    the watermark to the max raw `date` actually present.
    """
    log = get_run_logger()
    raw = get_duckdb_path("sra", "raw", raw_subdir, "**", "*parquet")
    hwm = HighWaterMark(entity)
    last = hwm.get()

    if last is not None and not force:
        filt = f"WHERE date >= '{last}'"
        log.info(f"{entity}: incremental scope date >= {last}")
    else:
        filt = ""
        reason = "force=True" if force else "no prior watermark"
        log.info(f"{entity}: full scan ({reason})")

    source_sql = source_template.format(path=raw, filt=filt)

    with get_ducklake_connection() as con:
        log.info(f"Merging {raw} → lake.{lake_schema}.{table}")
        rows = merge_to_ducklake(
            con,
            schema=lake_schema,
            table=table,
            source_sql=source_sql,
            key="accession",
            update_cols=update_cols,
            commit_message=f"ducklake-load: {entity} → {lake_schema}",
            commit_extra_info=_commit_extra(
                entity=entity, source=raw, high_water_from=last
            ),
        )
        new_max = _max_raw_date(con, raw)

    if new_max is not None:
        hwm.set(str(new_max), row_count=rows)
        log.info(f"{entity}: watermark advanced to {new_max}")
    else:
        log.warning(f"{entity}: no raw partitions found; watermark unchanged")

    log.info(f"lake.{lake_schema}.{table} now holds {rows:,} rows")
    return {
        "table": f"{lake_schema}.{table}",
        "row_count": rows,
        "high_water_from": last,
        "high_water_to": str(new_max) if new_max is not None else None,
        "forced": force,
    }


def _max_raw_date(con: duckdb.DuckDBPyConnection, raw: str) -> object | None:
    """Max `date` hive partition present in raw (the new watermark).

    Reads partition metadata only — cheap relative to the merge scan.
    Returns None when no partitions exist so the watermark is left as-is.
    """
    row = con.execute(
        f"SELECT max(date) FROM read_parquet('{raw}', hive_partitioning=true)"
    ).fetchone()
    return row[0] if row else None


@task(retries=1, retry_delay_seconds=60)
def sra_study_to_ducklake(
    lake_schema: str = LAKE_SCHEMA, force: bool = False
) -> dict:
    """MERGE raw SRA study partitions → lake.<lake_schema>.sra_study."""
    return _merge_sra(
        entity="sra_study",
        table="sra_study",
        raw_subdir="study",
        source_template=_STUDY_SOURCE,
        update_cols=_STUDY_UPDATE_COLS,
        lake_schema=lake_schema,
        force=force,
    )


@task(retries=1, retry_delay_seconds=60)
def sra_sample_to_ducklake(
    lake_schema: str = LAKE_SCHEMA, force: bool = False
) -> dict:
    """MERGE raw SRA sample partitions → lake.<lake_schema>.sra_sample."""
    return _merge_sra(
        entity="sra_sample",
        table="sra_sample",
        raw_subdir="sample",
        source_template=_SAMPLE_SOURCE,
        update_cols=_SAMPLE_UPDATE_COLS,
        lake_schema=lake_schema,
        force=force,
    )


@task(retries=1, retry_delay_seconds=60)
def sra_experiment_to_ducklake(
    lake_schema: str = LAKE_SCHEMA, force: bool = False
) -> dict:
    """MERGE raw SRA experiment partitions → lake.<lake_schema>.sra_experiment."""
    return _merge_sra(
        entity="sra_experiment",
        table="sra_experiment",
        raw_subdir="experiment",
        source_template=_EXPERIMENT_SOURCE,
        update_cols=_EXPERIMENT_UPDATE_COLS,
        lake_schema=lake_schema,
        force=force,
    )


@task(retries=1, retry_delay_seconds=60)
def sra_run_to_ducklake(
    lake_schema: str = LAKE_SCHEMA, force: bool = False
) -> dict:
    """MERGE raw SRA run partitions → lake.<lake_schema>.sra_run."""
    return _merge_sra(
        entity="sra_run",
        table="sra_run",
        raw_subdir="run",
        source_template=_RUN_SOURCE,
        update_cols=_RUN_UPDATE_COLS,
        lake_schema=lake_schema,
        force=force,
    )


@flow(name="ducklake-load-sra")
def ducklake_load_sra_flow(
    lake_schema: str = LAKE_SCHEMA, force: bool = False
) -> None:
    """Merge all four SRA entities into the lake (order unconstrained)."""
    sra_study_to_ducklake(lake_schema=lake_schema, force=force)
    sra_sample_to_ducklake(lake_schema=lake_schema, force=force)
    sra_experiment_to_ducklake(lake_schema=lake_schema, force=force)
    sra_run_to_ducklake(lake_schema=lake_schema, force=force)


if __name__ == "__main__":
    ducklake_load_sra_flow()
