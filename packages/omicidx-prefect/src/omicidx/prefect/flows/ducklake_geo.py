"""DuckLake load flow: MERGE GEO entities → lake.<schema>.*.

Three tasks cover the three GEO entity types:

- geo_series_to_ducklake   → lake.<schema>.geo_series   (key: accession)
- geo_sample_to_ducklake   → lake.<schema>.geo_sample   (key: accession)
- geo_platform_to_ducklake → lake.<schema>.geo_platform (key: accession)

Source paths (raw-direct)
=========================
All three entities read raw hive-partitioned NDJSON directly:

- geo_series   — gse/**/*.ndjson.gz
- geo_sample   — gsm/**/*.ndjson.gz
- geo_platform — gpl/**/*.ndjson.gz

``read_ndjson_auto`` is called with ``union_by_name=true`` because many
early year/month partition files are empty; without it DuckDB samples only
empty files and infers a single ``json`` column, breaking the typed
projection. Each projection applies ``trim`` to text fields and a QUALIFY
dedup (one row per accession, latest ``last_update_date`` wins) — exactly
what the raw stream needs.

The consolidate flow reads these same raw globs in production, so the field
names line up with these projections. There is intentionally no dependency
on ``geo/parquet/*.parquet``: the consolidate step is slated for removal in
P3, so DuckLake loads source raw directly.

Column lists are authoritative from consolidate.py (geo_series_parquet,
geo_samples_parquet, geo_platforms_parquet). Only the ``cdsci-lake`` bucket
is written; raw inputs are read from PUBLISH_ROOT via ``get_duckdb_path``.
"""

from omicidx.prefect.config import get_duckdb_path, get_ducklake_connection
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA, _commit_extra, merge_to_ducklake

from prefect import get_run_logger, task

# ---------------------------------------------------------------------------
# geo_series  (source: raw hive-partitioned NDJSON via glob)
#
# union_by_name=true is mandatory: early year/month partitions contain
# empty gzip files that yield no schema; without union_by_name DuckDB
# returns a single `json` column and the typed projection fails.
# ---------------------------------------------------------------------------

_GEO_SERIES_SOURCE = """
SELECT * EXCLUDE (rn) FROM (
    SELECT
        trim(title) AS title,
        trim(status) AS status,
        submission_date,
        last_update_date,
        trim(accession) AS accession,
        subseries,
        bioprojects,
        sra_studies,
        contact,
        type,
        trim(summary) AS summary,
        relation,
        pubmed_id,
        sample_id,
        sample_taxid,
        sample_organism,
        platform_id,
        platform_taxid,
        platform_organism,
        supplemental_files,
        trim(overall_design) AS overall_design,
        contributor,
        md5(to_json({{
            title: trim(title),
            status: trim(status),
            submission_date: submission_date,
            last_update_date: last_update_date,
            subseries: subseries,
            bioprojects: bioprojects,
            sra_studies: sra_studies,
            contact: contact,
            type: type,
            summary: trim(summary),
            relation: relation,
            pubmed_id: pubmed_id,
            sample_id: sample_id,
            sample_taxid: sample_taxid,
            sample_organism: sample_organism,
            platform_id: platform_id,
            platform_taxid: platform_taxid,
            platform_organism: platform_organism,
            supplemental_files: supplemental_files,
            overall_design: trim(overall_design),
            contributor: contributor
        }})) AS _row_hash,
        row_number() OVER (
            PARTITION BY trim(accession) ORDER BY last_update_date DESC NULLS LAST
        ) AS rn
    FROM read_ndjson_auto(
        '{path}',
        maximum_object_size = 1000000000,
        union_by_name = true
    )
    WHERE accession IS NOT NULL AND trim(accession) <> ''
) WHERE rn = 1
"""

# ---------------------------------------------------------------------------
# geo_sample  (source: raw hive-partitioned NDJSON via glob)
# union_by_name=true for the same empty-partition reason as geo_series.
# ---------------------------------------------------------------------------

_GEO_SAMPLE_SOURCE = """
SELECT * EXCLUDE (rn) FROM (
    SELECT
        trim(title) AS title,
        trim(status) AS status,
        submission_date,
        last_update_date,
        trim(type) AS type,
        trim(anchor) AS anchor,
        contact,
        trim(description) AS description,
        trim(accession) AS accession,
        biosample,
        tag_count,
        tag_length,
        trim(platform_id) AS platform_id,
        trim(hyb_protocol) AS hyb_protocol,
        channel_count,
        trim(scan_protocol) AS scan_protocol,
        data_row_count,
        library_source,
        sra_experiment,
        trim(data_processing) AS data_processing,
        supplemental_files,
        channels,
        contributor,
        md5(to_json({{
            title: trim(title),
            status: trim(status),
            submission_date: submission_date,
            last_update_date: last_update_date,
            type: trim(type),
            anchor: trim(anchor),
            contact: contact,
            description: trim(description),
            biosample: biosample,
            tag_count: tag_count,
            tag_length: tag_length,
            platform_id: trim(platform_id),
            hyb_protocol: trim(hyb_protocol),
            channel_count: channel_count,
            scan_protocol: trim(scan_protocol),
            data_row_count: data_row_count,
            library_source: library_source,
            sra_experiment: sra_experiment,
            data_processing: trim(data_processing),
            supplemental_files: supplemental_files,
            channels: channels,
            contributor: contributor
        }})) AS _row_hash,
        row_number() OVER (
            PARTITION BY trim(accession) ORDER BY last_update_date DESC NULLS LAST
        ) AS rn
    FROM read_ndjson_auto(
        '{path}',
        maximum_object_size = 1000000000,
        union_by_name = true
    )
    WHERE accession IS NOT NULL AND trim(accession) <> ''
) WHERE rn = 1
"""

# ---------------------------------------------------------------------------
# geo_platform  (source: raw hive-partitioned NDJSON via glob)
# union_by_name=true for the same empty-partition reason as geo_series.
# ---------------------------------------------------------------------------

_GEO_PLATFORM_SOURCE = """
SELECT * EXCLUDE (rn) FROM (
    SELECT
        trim(title) AS title,
        trim(status) AS status,
        submission_date,
        last_update_date,
        trim(accession) AS accession,
        contact,
        trim(organism) AS organism,
        sample_id,
        series_id,
        trim(technology) AS technology,
        trim(description) AS description,
        trim(distribution) AS distribution,
        manufacturer,
        data_row_count,
        contributor,
        relation,
        trim(manufacture_protocol) AS manufacture_protocol,
        md5(to_json({{
            title: trim(title),
            status: trim(status),
            submission_date: submission_date,
            last_update_date: last_update_date,
            contact: contact,
            organism: trim(organism),
            sample_id: sample_id,
            series_id: series_id,
            technology: trim(technology),
            description: trim(description),
            distribution: trim(distribution),
            manufacturer: manufacturer,
            data_row_count: data_row_count,
            contributor: contributor,
            relation: relation,
            manufacture_protocol: trim(manufacture_protocol)
        }})) AS _row_hash,
        row_number() OVER (
            PARTITION BY trim(accession) ORDER BY last_update_date DESC NULLS LAST
        ) AS rn
    FROM read_ndjson_auto(
        '{path}',
        maximum_object_size = 1000000000,
        union_by_name = true
    )
    WHERE accession IS NOT NULL AND trim(accession) <> ''
) WHERE rn = 1
"""

# ---------------------------------------------------------------------------
# Shared merge helper (avoids repeating the log/return boilerplate)
# ---------------------------------------------------------------------------


def _merge_geo(
    entity: str,
    table: str,
    raw_path: str,
    source_template: str,
    lake_schema: str,
) -> dict:
    """Run one GEO entity MERGE and return a summary dict."""
    log = get_run_logger()
    source_sql = source_template.format(path=raw_path)
    with get_ducklake_connection() as con:
        log.info(f"Merging {raw_path} → lake.{lake_schema}.{table}")
        rows = merge_to_ducklake(
            con,
            schema=lake_schema,
            table=table,
            source_sql=source_sql,
            key="accession",
            commit_message=f"ducklake-load: {entity} → {lake_schema}",
            commit_extra_info=_commit_extra(entity=entity, source=raw_path),
        )
    log.info(f"lake.{lake_schema}.{table} now holds {rows:,} rows")
    return {"table": f"{lake_schema}.{table}", "row_count": rows}


# ---------------------------------------------------------------------------
# Public tasks
# ---------------------------------------------------------------------------


@task(retries=1, retry_delay_seconds=60)
def geo_series_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """MERGE raw GEO series NDJSON → lake.<lake_schema>.geo_series.

    Reads from raw hive-partitioned NDJSON (gse/**/*.ndjson.gz). The
    ``union_by_name=true`` option is required because many early partition
    files are empty; without it DuckDB returns only a ``json`` column.
    """
    raw = get_duckdb_path("geo", "raw", "gse", "**", "*.ndjson.gz")
    return _merge_geo(
        entity="geo_series",
        table="geo_series",
        raw_path=raw,
        source_template=_GEO_SERIES_SOURCE,
        lake_schema=lake_schema,
    )


@task(retries=1, retry_delay_seconds=60)
def geo_sample_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """MERGE raw GEO sample NDJSON → lake.<lake_schema>.geo_sample.

    Reads from raw hive-partitioned NDJSON (gsm/**/*.ndjson.gz). The
    ``union_by_name=true`` option is required because many early partition
    files are empty; without it DuckDB returns only a ``json`` column.
    """
    raw = get_duckdb_path("geo", "raw", "gsm", "**", "*.ndjson.gz")
    return _merge_geo(
        entity="geo_sample",
        table="geo_sample",
        raw_path=raw,
        source_template=_GEO_SAMPLE_SOURCE,
        lake_schema=lake_schema,
    )


@task(retries=1, retry_delay_seconds=60)
def geo_platform_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """MERGE raw GEO platform NDJSON → lake.<lake_schema>.geo_platform.

    Reads from raw hive-partitioned NDJSON (gpl/**/*.ndjson.gz). The
    ``union_by_name=true`` option is required because many early partition
    files are empty; without it DuckDB returns only a ``json`` column.
    """
    raw = get_duckdb_path("geo", "raw", "gpl", "**", "*.ndjson.gz")
    return _merge_geo(
        entity="geo_platform",
        table="geo_platform",
        raw_path=raw,
        source_template=_GEO_PLATFORM_SOURCE,
        lake_schema=lake_schema,
    )
