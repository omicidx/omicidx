"""Postgres-load flow.

One task per entity. Each task loads a DuckLake table
(lake.<LAKE_SCHEMA>.<entity>) into a Postgres backing table (A/B-slot
pattern) and atomically swaps a view to point at the new one. The API
reads through the view, so reads never block during reload.

Sources track the ducklake-load schema (LAKE_SCHEMA = omicidx). Postgres
reads the lake tables directly, independent of the public parquet export.
"""

import asyncio
import os
import re

from omicidx.prefect.config import (
    attach_postgres,
    execute_postgres_sql,
    get_ducklake_connection,
    postgres_async_uri,
)
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from prefect import flow, get_run_logger, task
from prefect.task_runners import ThreadPoolTaskRunner

# Read at import (avoids triggering full Settings validation here); mirrors
# Settings.postgres_load_concurrency default. Bounds how many independent
# per-entity loads run at once — pair with duckdb_memory_limit/threads so
# concurrent DuckDB connections don't oversubscribe RAM/cores.
_PG_CONCURRENCY = int(os.getenv("POSTGRES_LOAD_CONCURRENCY", "4"))


def _validate_sql_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def _get_live_backing_table(view_name: str) -> str | None:
    """Return the live `{view}_a|{view}_b` backing table, or None."""
    view_name = _validate_sql_identifier(view_name)

    async def _check() -> str | None:
        engine = create_async_engine(postgres_async_uri())
        async with engine.begin() as conn:
            view_exists_result = await conn.execute(
                text("""
                    SELECT 1
                    FROM pg_class view_cls
                    JOIN pg_namespace view_ns ON view_ns.oid = view_cls.relnamespace
                    WHERE view_cls.relkind = 'v'
                      AND view_ns.nspname = 'public'
                      AND view_cls.relname = :view_name
                """),
                {"view_name": view_name},
            )
            view_exists = view_exists_result.first() is not None
            referenced_result = await conn.execute(
                text("""
                    SELECT DISTINCT cls.relname
                    FROM pg_class view_cls
                    JOIN pg_namespace view_ns ON view_ns.oid = view_cls.relnamespace
                    JOIN pg_rewrite rw ON rw.ev_class = view_cls.oid
                    JOIN pg_depend dep ON dep.objid = rw.oid
                    JOIN pg_class cls ON cls.oid = dep.refobjid
                    WHERE view_cls.relkind = 'v'
                      AND view_ns.nspname = 'public'
                      AND view_cls.relname = :view_name
                      AND cls.relkind IN ('r', 'p')
                """),
                {"view_name": view_name},
            )
            referenced_tables = [row[0] for row in referenced_result.fetchall()]
            slot_a = f"{view_name}_a"
            slot_b = f"{view_name}_b"
            result = await conn.execute(
                text("""
                    SELECT DISTINCT cls.relname
                    FROM pg_class view_cls
                    JOIN pg_namespace view_ns ON view_ns.oid = view_cls.relnamespace
                    JOIN pg_rewrite rw ON rw.ev_class = view_cls.oid
                    JOIN pg_depend dep ON dep.objid = rw.oid
                    JOIN pg_class cls ON cls.oid = dep.refobjid
                    WHERE view_cls.relkind = 'v'
                      AND view_ns.nspname = 'public'
                      AND view_cls.relname = :view_name
                      AND cls.relkind IN ('r', 'p')
                      AND (cls.relname = :slot_a OR cls.relname = :slot_b)
                """),
                {"view_name": view_name, "slot_a": slot_a, "slot_b": slot_b},
            )
            rows = result.fetchall()
        await engine.dispose()
        if not rows:
            if view_exists:
                referenced = ", ".join(referenced_tables) or "none"
                raise ValueError(
                    f"View {view_name!r} does not reference expected A/B tables "
                    f"({slot_a}, {slot_b}); found: {referenced}. "
                    "Check for manual schema/view changes."
                )
            return None
        if len(rows) > 1:
            table_names = ", ".join(row[0] for row in rows)
            raise ValueError(
                f"View {view_name!r} references multiple backing tables: {table_names}. "
                "Check for manual schema/view modifications or orphaned A/B tables."
            )
        return rows[0][0]

    return asyncio.run(_check())


def _load(
    *,
    table: str,
    ddl: str,
    lake_table: str,
    insert_sql_template: str,
    indexes: str | None = None,
    lake_schema: str = LAKE_SCHEMA,
) -> int:
    """Load a DuckLake table into Postgres with zero-downtime view swap.

    Reads `lake.<lake_schema>.<lake_table>` (DuckLake attached on the same
    connection that attaches the serving Postgres as `pg`) and inserts
    into the inactive A/B backing table, then swaps the public view.
    """
    log = get_run_logger()
    table = _validate_sql_identifier(table)
    lake_ref = (
        f"lake.{_validate_sql_identifier(lake_schema)}"
        f".{_validate_sql_identifier(lake_table)}"
    )
    tbl_a = f"{table}_a"
    tbl_b = f"{table}_b"

    live = _get_live_backing_table(table)
    target = tbl_b if live == tbl_a else tbl_a

    log.info(f"Loading into {target} (live={live or 'none'})")
    target_ddl = ddl.replace(table, target)
    execute_postgres_sql(f"DROP TABLE IF EXISTS {target} CASCADE", target_ddl)

    source_table_pattern = rf"\bpg\.{re.escape(table)}\b"
    if not re.search(source_table_pattern, insert_sql_template):
        raise ValueError(
            f"Insert template must contain pg.{table} so loads can be redirected "
            "to the inactive A/B backing table."
        )
    if "{lake_ref}" not in insert_sql_template:
        raise ValueError(
            "Insert template must contain {lake_ref} so the source can be bound "
            "to the DuckLake table."
        )
    target_insert = re.sub(source_table_pattern, f"pg.{target}", insert_sql_template)
    with get_ducklake_connection() as con, attach_postgres(con):
        log.info(f"Loading {target} from {lake_ref} (no secondary indexes)")
        con.execute(target_insert.format(lake_ref=lake_ref))
        row_count = con.execute(f"SELECT count(*) FROM pg.{target}").fetchone()[0]

    if indexes:
        target_indexes = indexes.replace(table, target)
        log.info(f"Building secondary indexes on {target}")
        execute_postgres_sql(target_indexes)

    log.info(f"Swapping view {table} → {target} ({row_count:,} rows)")
    execute_postgres_sql(
        f"DROP VIEW IF EXISTS public.{table}",
        f"DROP TABLE IF EXISTS public.{table} CASCADE",
        f"CREATE VIEW {table} AS SELECT * FROM {target}",
    )
    if live:
        execute_postgres_sql(f"DROP TABLE IF EXISTS {live} CASCADE")

    log.info(f"Loaded {row_count:,} rows into {table}")
    return row_count


# -- DDL / inserts -------------------------------------------------------------

_BIOPROJECT_DDL = """
CREATE TABLE IF NOT EXISTS bioproject (
    accession TEXT PRIMARY KEY,
    name TEXT,
    title TEXT,
    release_date TEXT,
    data JSONB NOT NULL,
    _loaded_at TIMESTAMPTZ DEFAULT now()
)
"""

_BIOPROJECT_INSERT = """
INSERT INTO pg.bioproject (accession, name, title, release_date, data)
SELECT accession, name, title, release_date, to_json({{
    accession: accession, name: name, title: title,
    description: description, release_date: release_date,
    data_types: data_types, publications: publications,
    external_links: external_links, locus_tags: locus_tags
}})::TEXT AS data
FROM {lake_ref}
"""

_BIOSAMPLE_DDL = """
CREATE TABLE IF NOT EXISTS biosample (
    accession TEXT PRIMARY KEY,
    sra_sample_id TEXT,
    organism TEXT,
    tax_id INTEGER,
    submission_date DATE,
    last_update DATE,
    is_reference BOOLEAN,
    data JSONB NOT NULL,
    _loaded_at TIMESTAMPTZ DEFAULT now()
);
"""

_BIOSAMPLE_INDEXES = """
CREATE INDEX IF NOT EXISTS ix_biosample_organism ON biosample (organism);
CREATE INDEX IF NOT EXISTS ix_biosample_tax_id ON biosample (tax_id);
CREATE INDEX IF NOT EXISTS ix_biosample_submission_date ON biosample (submission_date);
CREATE INDEX IF NOT EXISTS ix_biosample_sra_sample_id ON biosample (sra_sample_id);
"""

_BIOSAMPLE_INSERT = """
INSERT INTO pg.biosample (accession, sra_sample_id, organism, tax_id, submission_date, last_update, data)
SELECT
    accession, sra_sample, taxonomy_name, taxon_id,
    TRY_CAST(submission_date AS DATE),
    TRY_CAST(last_update AS DATE),
    to_json({{
        accession: accession, id: id, title: title,
        description: description, taxonomy_name: taxonomy_name,
        taxon_id: taxon_id, sra_sample: sra_sample,
        dbgap: dbgap, gsm: gsm, model: model,
        submission_date: submission_date, last_update: last_update,
        publication_date: publication_date, access: access,
        attribute_recs: attribute_recs, attributes: attributes,
        id_recs: id_recs, ids: ids
    }})::TEXT AS data
FROM {lake_ref}
"""

_SRA_STUDY_DDL = """
CREATE TABLE IF NOT EXISTS sra_study (
    accession TEXT PRIMARY KEY,
    organism TEXT,
    study_type TEXT,
    title TEXT,
    bioproject TEXT,
    submission_date DATE,
    last_update DATE,
    data JSONB NOT NULL,
    _loaded_at TIMESTAMPTZ DEFAULT now()
);
"""

_SRA_STUDY_INDEXES = """
CREATE INDEX IF NOT EXISTS ix_sra_study_organism ON sra_study (organism);
CREATE INDEX IF NOT EXISTS ix_sra_study_study_type ON sra_study (study_type);
CREATE INDEX IF NOT EXISTS ix_sra_study_bioproject ON sra_study (bioproject);
"""

_SRA_STUDY_INSERT = """
INSERT INTO pg.sra_study (accession, study_type, title, bioproject, data)
SELECT
    accession, study_type, title, bioproject,
    to_json({{
        accession: accession, alias: alias, title: title,
        description: description, abstract: abstract,
        study_type: study_type, center_name: center_name,
        broker_name: broker_name, bioproject: bioproject,
        geo: geo, identifiers: identifiers,
        attributes: attributes, xrefs: xrefs,
        pubmed_ids: pubmed_ids
    }})::TEXT AS data
FROM {lake_ref}
"""

_SRA_SAMPLE_DDL = """
CREATE TABLE IF NOT EXISTS sra_sample (
    accession TEXT PRIMARY KEY,
    organism TEXT,
    tax_id INTEGER,
    biosample TEXT,
    title TEXT,
    submission_date DATE,
    last_update DATE,
    data JSONB NOT NULL,
    _loaded_at TIMESTAMPTZ DEFAULT now()
);
"""

_SRA_SAMPLE_INDEXES = """
CREATE INDEX IF NOT EXISTS ix_sra_sample_organism ON sra_sample (organism);
CREATE INDEX IF NOT EXISTS ix_sra_sample_tax_id ON sra_sample (tax_id);
CREATE INDEX IF NOT EXISTS ix_sra_sample_biosample ON sra_sample (biosample);
"""

_SRA_SAMPLE_INSERT = """
INSERT INTO pg.sra_sample (accession, organism, tax_id, biosample, title, data)
SELECT
    accession, organism, taxon_id, biosample, title,
    to_json({{
        accession: accession, alias: alias, title: title,
        organism: organism, taxon_id: taxon_id,
        description: description, biosample: biosample,
        identifiers: identifiers, attributes: attributes,
        xrefs: xrefs
    }})::TEXT AS data
FROM {lake_ref}
"""

_SRA_EXPERIMENT_DDL = """
CREATE TABLE IF NOT EXISTS sra_experiment (
    accession TEXT PRIMARY KEY,
    library_strategy TEXT,
    library_source TEXT,
    platform TEXT,
    instrument_model TEXT,
    sample_accession TEXT,
    study_accession TEXT,
    submission_date DATE,
    last_update DATE,
    data JSONB NOT NULL,
    _loaded_at TIMESTAMPTZ DEFAULT now()
);
"""

_SRA_EXPERIMENT_INDEXES = """
CREATE INDEX IF NOT EXISTS ix_sra_experiment_library_strategy ON sra_experiment (library_strategy);
CREATE INDEX IF NOT EXISTS ix_sra_experiment_library_source ON sra_experiment (library_source);
CREATE INDEX IF NOT EXISTS ix_sra_experiment_platform ON sra_experiment (platform);
CREATE INDEX IF NOT EXISTS ix_sra_experiment_sample_accession ON sra_experiment (sample_accession);
CREATE INDEX IF NOT EXISTS ix_sra_experiment_study_accession ON sra_experiment (study_accession);
"""

_SRA_EXPERIMENT_INSERT = """
INSERT INTO pg.sra_experiment (accession, library_strategy, library_source, platform, instrument_model, sample_accession, study_accession, data)
SELECT
    accession, library_strategy, library_source, platform,
    instrument_model, sample_accession, study_accession,
    to_json({{
        accession: accession, alias: alias, title: title,
        design: design, center_name: center_name,
        study_accession: study_accession,
        sample_accession: sample_accession,
        platform: platform, instrument_model: instrument_model,
        library_name: library_name,
        library_construction_protocol: library_construction_protocol,
        library_layout: library_layout,
        library_strategy: library_strategy,
        library_source: library_source,
        library_selection: library_selection,
        identifiers: identifiers, attributes: attributes,
        xrefs: xrefs, reads: reads
    }})::TEXT AS data
FROM {lake_ref}
"""

_SRA_RUN_DDL = """
CREATE TABLE IF NOT EXISTS sra_run (
    accession TEXT PRIMARY KEY,
    experiment_accession TEXT,
    total_spots INTEGER,
    total_bases BIGINT,
    published DATE,
    last_update DATE,
    data JSONB NOT NULL,
    _loaded_at TIMESTAMPTZ DEFAULT now()
);
"""

_SRA_RUN_INDEXES = """
CREATE INDEX IF NOT EXISTS ix_sra_run_experiment_accession ON sra_run (experiment_accession);
CREATE INDEX IF NOT EXISTS ix_sra_run_published ON sra_run (published);
"""

_SRA_RUN_INSERT = """
INSERT INTO pg.sra_run (accession, experiment_accession, data)
SELECT
    accession, experiment_accession,
    to_json({{
        accession: accession, alias: alias,
        experiment_accession: experiment_accession,
        title: title, identifiers: identifiers,
        attributes: attributes, qualities: qualities
    }})::TEXT AS data
FROM {lake_ref}
"""

_GEO_SERIES_DDL = """
CREATE TABLE IF NOT EXISTS geo_series (
    accession TEXT PRIMARY KEY,
    title TEXT,
    organism TEXT,
    series_type TEXT,
    submission_date DATE,
    last_update DATE,
    data JSONB NOT NULL,
    _loaded_at TIMESTAMPTZ DEFAULT now()
);
"""

_GEO_SERIES_INDEXES = """
CREATE INDEX IF NOT EXISTS ix_geo_series_organism ON geo_series (organism);
CREATE INDEX IF NOT EXISTS ix_geo_series_series_type ON geo_series (series_type);
CREATE INDEX IF NOT EXISTS ix_geo_series_submission_date ON geo_series (submission_date);
"""

_GEO_SERIES_INSERT = """
INSERT INTO pg.geo_series (accession, title, organism, series_type, submission_date, last_update, data)
SELECT
    accession, title, sample_organism[1], type[1],
    TRY_CAST(submission_date AS DATE),
    TRY_CAST(last_update_date AS DATE),
    to_json({{
        accession: accession, title: title, status: status,
        submission_date: submission_date,
        last_update_date: last_update_date,
        summary: summary, overall_design: overall_design,
        type: type, contact: contact,
        pubmed_id: pubmed_id, relation: relation,
        sample_id: sample_id, platform_id: platform_id,
        sample_organism: sample_organism,
        platform_organism: platform_organism,
        supplemental_files: supplemental_files,
        subseries: subseries, bioprojects: bioprojects,
        sra_studies: sra_studies, contributor: contributor
    }})::TEXT AS data
FROM {lake_ref}
"""

_GEO_SAMPLE_DDL = """
CREATE TABLE IF NOT EXISTS geo_sample (
    accession TEXT PRIMARY KEY,
    organism TEXT,
    platform_id TEXT,
    series_id TEXT,
    title TEXT,
    submission_date DATE,
    last_update DATE,
    data JSONB NOT NULL,
    _loaded_at TIMESTAMPTZ DEFAULT now()
);
"""

_GEO_SAMPLE_INDEXES = """
CREATE INDEX IF NOT EXISTS ix_geo_sample_organism ON geo_sample (organism);
CREATE INDEX IF NOT EXISTS ix_geo_sample_platform_id ON geo_sample (platform_id);
CREATE INDEX IF NOT EXISTS ix_geo_sample_series_id ON geo_sample (series_id);
"""

_GEO_SAMPLE_INSERT = """
INSERT INTO pg.geo_sample (accession, platform_id, title, submission_date, last_update, data)
SELECT
    accession, platform_id, title,
    TRY_CAST(submission_date AS DATE),
    TRY_CAST(last_update_date AS DATE),
    to_json({{
        accession: accession, title: title, status: status,
        submission_date: submission_date,
        last_update_date: last_update_date,
        type: type, description: description,
        platform_id: platform_id, channel_count: channel_count,
        channels: channels, contact: contact,
        biosample: biosample, sra_experiment: sra_experiment,
        library_source: library_source,
        data_processing: data_processing,
        supplemental_files: supplemental_files,
        contributor: contributor
    }})::TEXT AS data
FROM {lake_ref}
"""

_GEO_PLATFORM_DDL = """
CREATE TABLE IF NOT EXISTS geo_platform (
    accession TEXT PRIMARY KEY,
    title TEXT,
    organism TEXT,
    technology TEXT,
    data JSONB NOT NULL,
    _loaded_at TIMESTAMPTZ DEFAULT now()
)
"""

_GEO_PLATFORM_INSERT = """
INSERT INTO pg.geo_platform (accession, title, organism, technology, data)
SELECT
    accession, title, organism, technology,
    to_json({{
        accession: accession, title: title, status: status,
        submission_date: submission_date,
        last_update_date: last_update_date,
        organism: organism, technology: technology,
        description: description, distribution: distribution,
        manufacturer: manufacturer, data_row_count: data_row_count,
        contact: contact, contributor: contributor,
        relation: relation
    }})::TEXT AS data
FROM {lake_ref}
"""

_PUBMED_DDL = """
CREATE TABLE IF NOT EXISTS pubmed_article (
    pmid INTEGER PRIMARY KEY,
    title TEXT,
    journal TEXT,
    pub_date DATE,
    data JSONB NOT NULL,
    _loaded_at TIMESTAMPTZ DEFAULT now()
);
"""

_PUBMED_INDEXES = """
CREATE INDEX IF NOT EXISTS ix_pubmed_article_journal ON pubmed_article (journal);
CREATE INDEX IF NOT EXISTS ix_pubmed_article_pub_date ON pubmed_article (pub_date);
"""

_PUBMED_INSERT = """
INSERT INTO pg.pubmed_article (pmid, title, journal, pub_date, data)
SELECT
    CAST(pmid AS INTEGER), title, journal,
    TRY_CAST(pubdate AS DATE),
    to_json({{
        pmid: pmid, title: title, journal: journal,
        pubdate: pubdate, abstract: abstract,
        authors: authors, mesh_terms: mesh_terms,
        publication_types: publication_types,
        keywords: keywords, doi: doi,
        pmc: pmc, issue: issue, pages: pages,
        "references": "references",
        languages: languages,
        chemical_list: chemical_list,
        grant_ids: grant_ids,
        country: country, medline_ta: medline_ta
    }})::TEXT AS data
FROM {lake_ref}
"""


# -- Tasks ---------------------------------------------------------------------


@task(retries=1, retry_delay_seconds=60)
def bioproject_postgres() -> int:
    return _load(
        table="bioproject",
        ddl=_BIOPROJECT_DDL,
        lake_table="bioproject",
        insert_sql_template=_BIOPROJECT_INSERT,
    )


@task(retries=1, retry_delay_seconds=60)
def biosample_postgres() -> int:
    return _load(
        table="biosample",
        ddl=_BIOSAMPLE_DDL,
        indexes=_BIOSAMPLE_INDEXES,
        lake_table="biosample",
        insert_sql_template=_BIOSAMPLE_INSERT,
    )


@task(retries=1, retry_delay_seconds=60)
def sra_study_postgres() -> int:
    return _load(
        table="sra_study",
        ddl=_SRA_STUDY_DDL,
        indexes=_SRA_STUDY_INDEXES,
        lake_table="sra_study",
        insert_sql_template=_SRA_STUDY_INSERT,
    )


@task(retries=1, retry_delay_seconds=60)
def sra_sample_postgres() -> int:
    return _load(
        table="sra_sample",
        ddl=_SRA_SAMPLE_DDL,
        indexes=_SRA_SAMPLE_INDEXES,
        lake_table="sra_sample",
        insert_sql_template=_SRA_SAMPLE_INSERT,
    )


@task(retries=1, retry_delay_seconds=60)
def sra_experiment_postgres() -> int:
    return _load(
        table="sra_experiment",
        ddl=_SRA_EXPERIMENT_DDL,
        indexes=_SRA_EXPERIMENT_INDEXES,
        lake_table="sra_experiment",
        insert_sql_template=_SRA_EXPERIMENT_INSERT,
    )


@task(retries=1, retry_delay_seconds=60)
def sra_run_postgres() -> int:
    return _load(
        table="sra_run",
        ddl=_SRA_RUN_DDL,
        indexes=_SRA_RUN_INDEXES,
        lake_table="sra_run",
        insert_sql_template=_SRA_RUN_INSERT,
    )


@task(retries=1, retry_delay_seconds=60)
def geo_series_postgres() -> int:
    return _load(
        table="geo_series",
        ddl=_GEO_SERIES_DDL,
        indexes=_GEO_SERIES_INDEXES,
        lake_table="geo_series",
        insert_sql_template=_GEO_SERIES_INSERT,
    )


@task(retries=1, retry_delay_seconds=60)
def geo_sample_postgres() -> int:
    return _load(
        table="geo_sample",
        ddl=_GEO_SAMPLE_DDL,
        indexes=_GEO_SAMPLE_INDEXES,
        lake_table="geo_sample",
        insert_sql_template=_GEO_SAMPLE_INSERT,
    )


@task(retries=1, retry_delay_seconds=60)
def geo_platform_postgres() -> int:
    return _load(
        table="geo_platform",
        ddl=_GEO_PLATFORM_DDL,
        lake_table="geo_platform",
        insert_sql_template=_GEO_PLATFORM_INSERT,
    )


@task(retries=1, retry_delay_seconds=60)
def pubmed_postgres() -> int:
    return _load(
        table="pubmed_article",
        ddl=_PUBMED_DDL,
        indexes=_PUBMED_INDEXES,
        lake_table="pubmed_article",
        insert_sql_template=_PUBMED_INSERT,
    )


@flow(name="postgres-load", task_runner=ThreadPoolTaskRunner(max_workers=_PG_CONCURRENCY))
def postgres_load_flow() -> None:
    """Reload every API-serving table from its DuckLake source table.

    The 10 per-entity loads are independent (separate tables, A/B slots and
    views), so they run concurrently bounded by POSTGRES_LOAD_CONCURRENCY.
    Tasks are submitted largest-table-first (LPT) to minimise makespan: the
    longest poles start immediately and smaller loads backfill freed slots.
    DuckDB memory_limit/threads (config) keep concurrent connections from
    oversubscribing the box.
    """
    # Largest → smallest by approximate row count (LPT scheduling).
    loads = [
        biosample_postgres,
        sra_run_postgres,
        sra_experiment_postgres,
        pubmed_postgres,
        sra_sample_postgres,
        geo_sample_postgres,
        bioproject_postgres,
        sra_study_postgres,
        geo_series_postgres,
        geo_platform_postgres,
    ]
    futures = [t.submit() for t in loads]
    for f in futures:
        f.result()  # re-raise any task failure


if __name__ == "__main__":
    postgres_load_flow()
