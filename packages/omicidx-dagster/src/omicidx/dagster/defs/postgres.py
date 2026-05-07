"""Assets that load consolidated Parquet data into PostgreSQL for the API."""

from omicidx.dagster.defs.biosample import bioproject_parquet
from omicidx.dagster.resources import DuckDBResource, OmicidxStorage, PostgresResource

import dagster as dg

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


@dg.asset(
    group_name="postgres",
    kinds={"postgres", "duckdb"},
    tags={
        "layer": "serving",
        "cost": "low",
        "sla": "daily",
        "source": "derived",
        "storage": "postgres",
    },
    deps=[bioproject_parquet],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
)
def bioproject_postgres(
    context: dg.AssetExecutionContext,
    storage: OmicidxStorage,
    duckdb_res: DuckDBResource,
    postgres: PostgresResource,
) -> dg.MaterializeResult:
    """Load BioProject parquet into PostgreSQL for API serving."""
    parquet_path = storage.get_duckdb_path("bioproject", "parquet", "bioprojects.parquet")

    # DDL via SQLAlchemy async + asyncpg (Postgres-native types like JSONB)
    context.log.info("Ensuring bioproject table exists")
    postgres.execute_sql(_BIOPROJECT_DDL, "TRUNCATE bioproject")

    # Bulk load via DuckDB postgres_scanner (fast parquet → Postgres)
    with duckdb_res.get_connection() as con, postgres.attach(con):
        context.log.info(f"Loading from {parquet_path}")
        con.execute(f"""
            INSERT INTO pg.bioproject (accession, name, title, release_date, data)
            SELECT
                accession,
                name,
                title,
                release_date,
                to_json({{
                    accession: accession,
                    name: name,
                    title: title,
                    description: description,
                    release_date: release_date,
                    data_types: data_types,
                    publications: publications,
                    external_links: external_links,
                    locus_tags: locus_tags
                }})::TEXT AS data
            FROM read_parquet('{parquet_path}')
        """)

        row_count = con.execute("SELECT count(*) FROM pg.bioproject").fetchone()[0]

    context.log.info(f"Loaded {row_count:,} bioprojects into PostgreSQL")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "source_path": dg.MetadataValue.text(parquet_path),
        }
    )
