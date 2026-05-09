"""Assets that load consolidated Parquet data into PostgreSQL for the API."""

import re

from omicidx.dagster.defs.biosample import bioproject_parquet
from omicidx.dagster.defs.consolidate import (
    biosample_parquet,
    geo_platforms_parquet,
    geo_samples_parquet,
    geo_series_parquet,
    pubmed_parquet,
    sra_experiments_parquet,
    sra_runs_parquet,
    sra_samples_parquet,
    sra_studies_parquet,
)
from omicidx.dagster.resources import DuckDBResource, OmicidxStorage, PostgresResource

import dagster as dg

_PG_TAGS = {
    "layer": "serving",
    "cost": "low",
    "sla": "daily",
    "source": "derived",
    "storage": "postgres",
}


def _validate_sql_identifier(name: str) -> str:
    """Validate an unquoted PostgreSQL identifier and return it unchanged.

    Unquoted identifiers are case-insensitive (folded to lowercase) in Postgres.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def _escape_sql_single_quotes(value: str) -> str:
    """Escape single quotes for SQL string literal usage."""
    return value.replace("'", "''")


def _escape_format_braces(value: str) -> str:
    """Escape braces so `str.format()` treats them as literal characters."""
    return value.replace("{", "{{").replace("}", "}}")


def _get_live_backing_table(postgres: PostgresResource, view_name: str) -> str | None:
    """Return active `{view}_a|{view}_b` backing table, or None when not detected.

    Raises ValueError when the view unexpectedly points to multiple A/B tables.
    """
    import asyncio

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    view_name = _validate_sql_identifier(view_name)

    async def _check():
        engine = create_async_engine(postgres.async_uri)
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
                    -- Trace view dependencies through rewrite rules to find
                    -- the underlying physical table backing the view.
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


def _load_to_postgres(
    *,
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
    postgres: PostgresResource,
    table: str,
    ddl: str,
    parquet_parts: tuple[str, ...],
    insert_sql_template: str,
    indexes: str | None = None,
) -> dg.MaterializeResult:
    """Load parquet into Postgres with zero-downtime view swap.

    The API reads from a view named `{table}`. Two backing tables alternate:
    `{table}_a` and `{table}_b`. Each reload writes to whichever is NOT
    currently backing the view, swaps the view, then drops the old one.

    Indexes (if supplied) are created AFTER the bulk INSERT and BEFORE the
    view swap. Bulk-loading into an unindexed table is dramatically faster
    than maintaining secondary indexes per-row; PRIMARY KEY (an implicit
    btree) is already part of `ddl` and unavoidable. Index references to
    `{table}` get rewritten to the target slot.

    CREATE OR REPLACE VIEW only takes AccessShareLock — reads never block.
    """
    table = _validate_sql_identifier(table)
    parquet_path = storage.get_duckdb_path(*parquet_parts)
    parquet_path_literal = _escape_sql_single_quotes(
        _escape_format_braces(parquet_path)
    )
    tbl_a = f"{table}_a"
    tbl_b = f"{table}_b"

    live = _get_live_backing_table(postgres, table)
    target = tbl_b if live == tbl_a else tbl_a

    context.log.info(f"Loading into {target} (live={live or 'none'})")
    target_ddl = ddl.replace(table, target)
    postgres.execute_sql(
        f"DROP TABLE IF EXISTS {target} CASCADE",
        target_ddl,
    )

    source_table_pattern = rf"\bpg\.{re.escape(table)}\b"
    if not re.search(source_table_pattern, insert_sql_template):
        raise ValueError(
            f"Insert template must contain pg.{table} so loads can be redirected "
            "to the inactive A/B backing table."
        )
    target_insert = re.sub(source_table_pattern, f"pg.{target}", insert_sql_template)
    with duckdb_res.get_connection() as con, postgres.attach(con):
        context.log.info(f"Loading {target} from {parquet_path} (no secondary indexes)")
        con.execute(target_insert.format(path=parquet_path_literal))
        row_count = con.execute(f"SELECT count(*) FROM pg.{target}").fetchone()[0]

    if indexes:
        target_indexes = indexes.replace(table, target)
        context.log.info(f"Building secondary indexes on {target}")
        postgres.execute_sql(target_indexes)

    context.log.info(f"Swapping view {table} → {target} ({row_count:,} rows)")
    # Defensive: if {table} exists as a non-view (legacy direct-load
    # table from before the A/B-view pattern), CREATE OR REPLACE VIEW
    # will fail with WrongObjectTypeError. Drop the legacy object so
    # the migration to the view pattern can proceed.
    postgres.execute_sql(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_class
                WHERE relname = '{table}'
                  AND relnamespace = 'public'::regnamespace
                  AND relkind <> 'v'
            ) THEN
                EXECUTE 'DROP TABLE IF EXISTS public.{table} CASCADE';
            END IF;
        END $$;
        """,
        f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM {target}",
    )
    if live:
        postgres.execute_sql(f"DROP TABLE IF EXISTS {live} CASCADE")

    context.log.info(f"Loaded {row_count:,} rows into {table}")
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "source_path": dg.MetadataValue.text(parquet_path),
        }
    )


# -- BioProject ----------------------------------------------------------------

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
FROM read_parquet('{path}')
"""


@dg.asset(
    group_name="postgres",
    kinds={"postgres", "duckdb"},
    tags=_PG_TAGS,
    deps=[bioproject_parquet],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def bioproject_postgres(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    return _load_to_postgres(
        context=context,
        storage=storage,
        duckdb_res=duckdb_res,
        postgres=postgres,
        table="bioproject",
        ddl=_BIOPROJECT_DDL,
        parquet_parts=("bioproject", "parquet", "bioprojects.parquet"),
        insert_sql_template=_BIOPROJECT_INSERT,
    )


# -- BioSample -----------------------------------------------------------------

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
FROM read_parquet('{path}')
"""


@dg.asset(
    group_name="postgres",
    kinds={"postgres", "duckdb"},
    tags=_PG_TAGS,
    deps=[biosample_parquet],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def biosample_postgres(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    return _load_to_postgres(
        context=context,
        storage=storage,
        duckdb_res=duckdb_res,
        postgres=postgres,
        table="biosample",
        ddl=_BIOSAMPLE_DDL,
        indexes=_BIOSAMPLE_INDEXES,
        parquet_parts=("biosample", "parquet", "biosamples.parquet"),
        insert_sql_template=_BIOSAMPLE_INSERT,
    )


# -- SRA Study ------------------------------------------------------------------

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
FROM read_parquet('{path}')
"""


@dg.asset(
    group_name="postgres",
    kinds={"postgres", "duckdb"},
    tags=_PG_TAGS,
    deps=[sra_studies_parquet],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def sra_study_postgres(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    return _load_to_postgres(
        context=context,
        storage=storage,
        duckdb_res=duckdb_res,
        postgres=postgres,
        table="sra_study",
        ddl=_SRA_STUDY_DDL,
        indexes=_SRA_STUDY_INDEXES,
        parquet_parts=("sra", "parquet", "sra_studies.parquet"),
        insert_sql_template=_SRA_STUDY_INSERT,
    )


# -- SRA Sample -----------------------------------------------------------------

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
FROM read_parquet('{path}')
"""


@dg.asset(
    group_name="postgres",
    kinds={"postgres", "duckdb"},
    tags=_PG_TAGS,
    deps=[sra_samples_parquet],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def sra_sample_postgres(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    return _load_to_postgres(
        context=context,
        storage=storage,
        duckdb_res=duckdb_res,
        postgres=postgres,
        table="sra_sample",
        ddl=_SRA_SAMPLE_DDL,
        indexes=_SRA_SAMPLE_INDEXES,
        parquet_parts=("sra", "parquet", "sra_samples.parquet"),
        insert_sql_template=_SRA_SAMPLE_INSERT,
    )


# -- SRA Experiment -------------------------------------------------------------

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
FROM read_parquet('{path}')
"""


@dg.asset(
    group_name="postgres",
    kinds={"postgres", "duckdb"},
    tags=_PG_TAGS,
    deps=[sra_experiments_parquet],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def sra_experiment_postgres(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    return _load_to_postgres(
        context=context,
        storage=storage,
        duckdb_res=duckdb_res,
        postgres=postgres,
        table="sra_experiment",
        ddl=_SRA_EXPERIMENT_DDL,
        indexes=_SRA_EXPERIMENT_INDEXES,
        parquet_parts=("sra", "parquet", "sra_experiments.parquet"),
        insert_sql_template=_SRA_EXPERIMENT_INSERT,
    )


# -- SRA Run --------------------------------------------------------------------

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
FROM read_parquet('{path}')
"""


@dg.asset(
    group_name="postgres",
    kinds={"postgres", "duckdb"},
    tags=_PG_TAGS,
    deps=[sra_runs_parquet],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def sra_run_postgres(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    return _load_to_postgres(
        context=context,
        storage=storage,
        duckdb_res=duckdb_res,
        postgres=postgres,
        table="sra_run",
        ddl=_SRA_RUN_DDL,
        indexes=_SRA_RUN_INDEXES,
        parquet_parts=("sra", "parquet", "sra_runs.parquet"),
        insert_sql_template=_SRA_RUN_INSERT,
    )


# -- GEO Series -----------------------------------------------------------------

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
FROM read_parquet('{path}')
"""


@dg.asset(
    group_name="postgres",
    kinds={"postgres", "duckdb"},
    tags=_PG_TAGS,
    deps=[geo_series_parquet],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def geo_series_postgres(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    return _load_to_postgres(
        context=context,
        storage=storage,
        duckdb_res=duckdb_res,
        postgres=postgres,
        table="geo_series",
        ddl=_GEO_SERIES_DDL,
        indexes=_GEO_SERIES_INDEXES,
        parquet_parts=("geo", "parquet", "geo_series.parquet"),
        insert_sql_template=_GEO_SERIES_INSERT,
    )


# -- GEO Sample -----------------------------------------------------------------

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
FROM read_parquet('{path}')
"""


@dg.asset(
    group_name="postgres",
    kinds={"postgres", "duckdb"},
    tags=_PG_TAGS,
    deps=[geo_samples_parquet],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def geo_sample_postgres(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    return _load_to_postgres(
        context=context,
        storage=storage,
        duckdb_res=duckdb_res,
        postgres=postgres,
        table="geo_sample",
        ddl=_GEO_SAMPLE_DDL,
        indexes=_GEO_SAMPLE_INDEXES,
        parquet_parts=("geo", "parquet", "geo_samples.parquet"),
        insert_sql_template=_GEO_SAMPLE_INSERT,
    )


# -- GEO Platform ---------------------------------------------------------------

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
FROM read_parquet('{path}')
"""


@dg.asset(
    group_name="postgres",
    kinds={"postgres", "duckdb"},
    tags=_PG_TAGS,
    deps=[geo_platforms_parquet],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def geo_platform_postgres(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    return _load_to_postgres(
        context=context,
        storage=storage,
        duckdb_res=duckdb_res,
        postgres=postgres,
        table="geo_platform",
        ddl=_GEO_PLATFORM_DDL,
        parquet_parts=("geo", "parquet", "geo_platforms.parquet"),
        insert_sql_template=_GEO_PLATFORM_INSERT,
    )


# -- PubMed ---------------------------------------------------------------------

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
FROM read_parquet('{path}')
"""


@dg.asset(
    group_name="postgres",
    kinds={"postgres", "duckdb"},
    tags=_PG_TAGS,
    deps=[pubmed_parquet],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def pubmed_postgres(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    return _load_to_postgres(
        context=context,
        storage=storage,
        duckdb_res=duckdb_res,
        postgres=postgres,
        table="pubmed_article",
        ddl=_PUBMED_DDL,
        indexes=_PUBMED_INDEXES,
        parquet_parts=("pubmed", "parquet", "pubmed_articles.parquet"),
        insert_sql_template=_PUBMED_INSERT,
    )
