"""Per-entity parquet consolidation assets.

Each asset reads raw data from R2 and writes a deduplicated, trimmed parquet
file. Replaces the monolithic consolidated_parquet asset that ran all entities
in a single 010_raw_to_parquet.sql execution.
"""

from omicidx.dagster.defs.biosample import biosample_raw
from omicidx.dagster.defs.geo import geo_raw
from omicidx.dagster.defs.pubmed import pubmed_raw
from omicidx.dagster.defs.sra import sra_raw
from omicidx.dagster.resources import DuckDBResource, OmicidxStorage

import dagster as dg

_CONSOLIDATE_TAGS = {
    "layer": "consolidated",
    "cost": "medium",
    "source": "derived",
    "storage": "parquet",
}


def _consolidate(
    *,
    context: dg.AssetExecutionContext,
    duckdb_res: DuckDBResource,
    storage: OmicidxStorage,
    sql: str,
    output_parts: tuple[str, ...],
) -> dg.MaterializeResult:
    """Run a COPY consolidation SQL and return row count metadata."""
    output_path = storage.get_duckdb_path(*output_parts)

    with duckdb_res.get_connection() as con:
        context.log.info(f"Consolidating to {output_path}")
        con.execute(sql)
        row_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{output_path}')"
        ).fetchone()[0]

    context.log.info(f"Wrote {row_count:,} rows to {output_path}")
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "output_path": dg.MetadataValue.text(output_path),
        }
    )


# -- GEO -----------------------------------------------------------------------


@dg.asset(
    group_name="consolidate",
    kinds={"duckdb", "parquet", "s3"},
    tags={**_CONSOLIDATE_TAGS, "sla": "monthly"},
    deps=[geo_raw],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def geo_platforms_parquet(
    context: dg.AssetExecutionContext,
    duckdb_res: DuckDBResource,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    output = storage.get_duckdb_path("geo", "parquet", "geo_platforms.parquet")
    input_path = storage.get_duckdb_path("geo", "raw", "gpl", "**", "*.ndjson.gz")
    sql = f"""
        COPY (
            SELECT
                trim(title) as title, trim(status) as status,
                submission_date, last_update_date,
                trim(accession) as accession, contact,
                trim(organism) as organism, sample_id, series_id,
                trim(technology) as technology,
                trim(description) as description,
                trim(distribution) as distribution,
                manufacturer, data_row_count, contributor, relation,
                trim(manufacture_protocol) as manufacture_protocol
            FROM read_ndjson_auto('{input_path}')
            QUALIFY row_number() OVER (PARTITION BY accession ORDER BY last_update_date DESC NULLS LAST) = 1
            ORDER BY accession
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _consolidate(
        context=context,
        duckdb_res=duckdb_res,
        storage=storage,
        sql=sql,
        output_parts=("geo", "parquet", "geo_platforms.parquet"),
    )


@dg.asset(
    group_name="consolidate",
    kinds={"duckdb", "parquet", "s3"},
    tags={**_CONSOLIDATE_TAGS, "sla": "monthly"},
    deps=[geo_raw],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def geo_series_parquet(
    context: dg.AssetExecutionContext,
    duckdb_res: DuckDBResource,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    output = storage.get_duckdb_path("geo", "parquet", "geo_series.parquet")
    input_path = storage.get_duckdb_path("geo", "raw", "gse", "**", "*.ndjson.gz")
    sql = f"""
        COPY (
            SELECT
                trim(title) as title, trim(status) as status,
                submission_date, last_update_date,
                trim(accession) as accession, subseries, bioprojects,
                sra_studies, contact, type, trim(summary) as summary,
                relation, pubmed_id, sample_id, sample_taxid,
                sample_organism, platform_id, platform_taxid,
                platform_organism, supplemental_files,
                trim(overall_design) as overall_design, contributor
            FROM read_ndjson_auto('{input_path}')
            QUALIFY row_number() OVER (PARTITION BY accession ORDER BY last_update_date DESC NULLS LAST) = 1
            ORDER BY accession
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _consolidate(
        context=context,
        duckdb_res=duckdb_res,
        storage=storage,
        sql=sql,
        output_parts=("geo", "parquet", "geo_series.parquet"),
    )


@dg.asset(
    group_name="consolidate",
    kinds={"duckdb", "parquet", "s3"},
    tags={**_CONSOLIDATE_TAGS, "sla": "monthly"},
    deps=[geo_raw],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def geo_samples_parquet(
    context: dg.AssetExecutionContext,
    duckdb_res: DuckDBResource,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    output = storage.get_duckdb_path("geo", "parquet", "geo_samples.parquet")
    input_path = storage.get_duckdb_path("geo", "raw", "gsm", "**", "*.ndjson.gz")
    sql = f"""
        COPY (
            SELECT
                trim(title) as title, trim(status) as status,
                submission_date, last_update_date,
                trim(type) as type, trim(anchor) as anchor,
                contact, trim(description) as description,
                trim(accession) as accession, biosample, tag_count,
                tag_length, trim(platform_id) as platform_id,
                trim(hyb_protocol) as hyb_protocol, channel_count,
                trim(scan_protocol) as scan_protocol, data_row_count,
                library_source, sra_experiment,
                trim(data_processing) as data_processing,
                supplemental_files, channels, contributor
            FROM read_ndjson_auto('{input_path}')
            QUALIFY row_number() OVER (PARTITION BY accession ORDER BY last_update_date DESC NULLS LAST) = 1
            ORDER BY accession
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _consolidate(
        context=context,
        duckdb_res=duckdb_res,
        storage=storage,
        sql=sql,
        output_parts=("geo", "parquet", "geo_samples.parquet"),
    )


@dg.asset(
    group_name="consolidate",
    kinds={"duckdb", "parquet", "s3"},
    tags={**_CONSOLIDATE_TAGS, "sla": "daily"},
    deps=["geo_rna_seq_counts"],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def geo_rnaseq_counts_parquet(
    context: dg.AssetExecutionContext,
    duckdb_res: DuckDBResource,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    output = storage.get_duckdb_path(
        "geo", "parquet", "geo_series_with_rnaseq_counts.parquet"
    )
    input_path = storage.get_duckdb_path(
        "geo", "raw", "gse_with_rna_seq_counts.parquet"
    )
    sql = f"""
        COPY (
            SELECT accession.accession as accession
            FROM read_parquet('{input_path}')
            ORDER BY accession
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _consolidate(
        context=context,
        duckdb_res=duckdb_res,
        storage=storage,
        sql=sql,
        output_parts=("geo", "parquet", "geo_series_with_rnaseq_counts.parquet"),
    )


# -- SRA -----------------------------------------------------------------------


@dg.asset(
    group_name="consolidate",
    kinds={"duckdb", "parquet", "s3"},
    tags={**_CONSOLIDATE_TAGS, "sla": "daily"},
    deps=[sra_raw],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def sra_studies_parquet(
    context: dg.AssetExecutionContext,
    duckdb_res: DuckDBResource,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    output = storage.get_duckdb_path("sra", "parquet", "sra_studies.parquet")
    input_path = storage.get_duckdb_path("sra", "raw", "study", "**", "*parquet")
    sql = f"""
        COPY (
            SELECT
                trim(accession) as accession,
                trim(study_accession) as study_accession,
                trim(alias) as alias, trim(title) as title,
                trim(description) as description,
                trim(abstract) as abstract,
                trim(study_type) as study_type,
                trim(center_name) as center_name,
                trim(broker_name) as broker_name,
                trim("BioProject") as bioproject,
                trim("GEO") as geo,
                identifiers, attributes, xrefs, pubmed_ids
            FROM read_parquet('{input_path}')
            ORDER BY accession
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _consolidate(
        context=context,
        duckdb_res=duckdb_res,
        storage=storage,
        sql=sql,
        output_parts=("sra", "parquet", "sra_studies.parquet"),
    )


@dg.asset(
    group_name="consolidate",
    kinds={"duckdb", "parquet", "s3"},
    tags={**_CONSOLIDATE_TAGS, "sla": "daily"},
    deps=[sra_raw],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def sra_samples_parquet(
    context: dg.AssetExecutionContext,
    duckdb_res: DuckDBResource,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    output = storage.get_duckdb_path("sra", "parquet", "sra_samples.parquet")
    input_path = storage.get_duckdb_path("sra", "raw", "sample", "**", "*parquet")
    sql = f"""
        COPY (
            SELECT
                trim(accession) as accession,
                trim(alias) as alias, trim(title) as title,
                trim(organism) as organism,
                trim(description) as description,
                taxon_id,
                trim("BioSample") as biosample,
                identifiers, attributes, xrefs
            FROM read_parquet('{input_path}')
            ORDER BY accession
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _consolidate(
        context=context,
        duckdb_res=duckdb_res,
        storage=storage,
        sql=sql,
        output_parts=("sra", "parquet", "sra_samples.parquet"),
    )


@dg.asset(
    group_name="consolidate",
    kinds={"duckdb", "parquet", "s3"},
    tags={**_CONSOLIDATE_TAGS, "sla": "daily"},
    deps=[sra_raw],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def sra_experiments_parquet(
    context: dg.AssetExecutionContext,
    duckdb_res: DuckDBResource,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    output = storage.get_duckdb_path("sra", "parquet", "sra_experiments.parquet")
    input_path = storage.get_duckdb_path("sra", "raw", "experiment", "**", "*parquet")
    sql = f"""
        COPY (
            SELECT
                trim(accession) as accession,
                trim(experiment_accession) as experiment_accession,
                trim(alias) as alias, trim(title) as title,
                trim(design) as design,
                trim(center_name) as center_name,
                trim(study_accession) as study_accession,
                trim(sample_accession) as sample_accession,
                trim(platform) as platform,
                trim(instrument_model) as instrument_model,
                trim(library_name) as library_name,
                trim(library_construction_protocol) as library_construction_protocol,
                trim(library_layout) as library_layout,
                trim(library_layout_length) as library_layout_length,
                trim(library_layout_sdev) as library_layout_sdev,
                trim(library_strategy) as library_strategy,
                trim(library_source) as library_source,
                trim(library_selection) as library_selection,
                spot_length, nreads,
                identifiers, attributes, xrefs, reads
            FROM read_parquet('{input_path}')
            ORDER BY accession
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _consolidate(
        context=context,
        duckdb_res=duckdb_res,
        storage=storage,
        sql=sql,
        output_parts=("sra", "parquet", "sra_experiments.parquet"),
    )


@dg.asset(
    group_name="consolidate",
    kinds={"duckdb", "parquet", "s3"},
    tags={**_CONSOLIDATE_TAGS, "sla": "daily"},
    deps=[sra_raw],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def sra_runs_parquet(
    context: dg.AssetExecutionContext,
    duckdb_res: DuckDBResource,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    output = storage.get_duckdb_path("sra", "parquet", "sra_runs.parquet")
    input_path = storage.get_duckdb_path("sra", "raw", "run", "**", "*parquet")
    sql = f"""
        COPY (
            SELECT
                trim(accession) as accession,
                trim(alias) as alias,
                trim(experiment_accession) as experiment_accession,
                trim(title) as title,
                identifiers, attributes, qualities
            FROM read_parquet('{input_path}')
            ORDER BY accession
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _consolidate(
        context=context,
        duckdb_res=duckdb_res,
        storage=storage,
        sql=sql,
        output_parts=("sra", "parquet", "sra_runs.parquet"),
    )


@dg.asset(
    group_name="consolidate",
    kinds={"duckdb", "parquet", "s3"},
    tags={**_CONSOLIDATE_TAGS, "sla": "daily"},
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.on_cron("0 3 * * *"),
)
def sra_accessions_parquet(
    context: dg.AssetExecutionContext,
    duckdb_res: DuckDBResource,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    """Consolidate SRA_Accessions.tab from NCBI FTP (no upstream asset dependency)."""
    output = storage.get_duckdb_path("sra", "parquet", "sra_accessions.parquet")
    sql = f"""
        COPY (
            SELECT
                trim("Accession") as accession,
                trim("Submission") as submission,
                trim("Status") as status,
                "Updated" as updated, "Published" as published,
                "Received" as received,
                trim("Type") as type, trim("Center") as center,
                trim("Visibility") as visibility,
                trim("Alias") as alias,
                trim("Experiment") as experiment,
                trim("Sample") as sample, trim("Study") as study,
                "Loaded" as loaded, "Spots" as spots, "Bases" as bases,
                trim("Md5sum") as md5sum,
                trim("BioSample") as biosample,
                trim("BioProject") as bioproject,
                trim("ReplacedBy") as replacedby
            FROM read_csv_auto(
                'https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab',
                nullstr = '-'
            )
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _consolidate(
        context=context,
        duckdb_res=duckdb_res,
        storage=storage,
        sql=sql,
        output_parts=("sra", "parquet", "sra_accessions.parquet"),
    )


# -- BioSample -----------------------------------------------------------------


@dg.asset(
    group_name="consolidate",
    kinds={"duckdb", "parquet", "s3"},
    tags={**_CONSOLIDATE_TAGS, "sla": "daily"},
    deps=[biosample_raw],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    automation_condition=dg.AutomationCondition.eager(),
)
def biosample_parquet(
    context: dg.AssetExecutionContext,
    duckdb_res: DuckDBResource,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    output = storage.get_duckdb_path("biosample", "parquet", "biosamples.parquet")
    input_path = storage.get_duckdb_path("biosample", "raw", "data.jsonl.gz")
    sql = f"""
        COPY (
            SELECT
                trim(submission_date) as submission_date,
                trim(last_update) as last_update,
                trim(publication_date) as publication_date,
                trim(access) as access,
                trim(id) as id,
                trim(accession) as accession,
                id_recs, ids,
                trim(sra_sample) as sra_sample,
                trim(dbgap) as dbgap,
                trim(gsm) as gsm,
                trim(title) as title,
                trim(description) as description,
                trim(taxonomy_name) as taxonomy_name,
                taxon_id, attribute_recs, attributes,
                trim(model) as model
            FROM read_ndjson_auto(
                '{input_path}',
                maximum_object_size = 1000000000
            )
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _consolidate(
        context=context,
        duckdb_res=duckdb_res,
        storage=storage,
        sql=sql,
        output_parts=("biosample", "parquet", "biosamples.parquet"),
    )


# -- PubMed --------------------------------------------------------------------


@dg.asset(
    group_name="consolidate",
    kinds={"duckdb", "parquet", "s3"},
    tags={**_CONSOLIDATE_TAGS, "sla": "daily"},
    deps=[pubmed_raw],
    retry_policy=dg.RetryPolicy(max_retries=1, delay=60),
    # PubMed raw lands hourly via the file sensor; eager() would cause
    # full historical reconsolidation each hour. Run once daily, only
    # if at least one new partition has arrived since last materialization.
    automation_condition=(
        dg.AutomationCondition.on_cron("0 3 * * *")
        & dg.AutomationCondition.any_deps_updated()
    ),
)
def pubmed_parquet(
    context: dg.AssetExecutionContext,
    duckdb_res: DuckDBResource,
    storage: OmicidxStorage,
) -> dg.MaterializeResult:
    output = storage.get_duckdb_path("pubmed", "parquet", "pubmed_articles.parquet")
    input_path = storage.get_duckdb_path("pubmed", "raw", "*.parquet")
    sql = f"""
        COPY (
            SELECT
                trim(title) as title, trim(issue) as issue,
                trim(pages) as pages, trim(abstract) as abstract,
                trim(journal) as journal, authors,
                trim(pubdate) as pubdate, trim(pmid) as pmid,
                trim(mesh_terms) as mesh_terms,
                trim(publication_types) as publication_types,
                trim(chemical_list) as chemical_list,
                trim(keywords) as keywords, trim(doi) as doi,
                "references",
                trim(languages) as languages,
                trim(vernacular_title) as vernacular_title,
                trim(date_completed) as date_completed,
                trim(date_revised) as date_revised,
                trim(pmc) as pmc, trim(other_id) as other_id,
                trim(medline_ta) as medline_ta,
                trim(nlm_unique_id) as nlm_unique_id,
                trim(issn_linking) as issn_linking,
                trim(country) as country, grant_ids
            FROM read_parquet('{input_path}')
            WHERE delete IS NOT TRUE
            ORDER BY pmid
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _consolidate(
        context=context,
        duckdb_res=duckdb_res,
        storage=storage,
        sql=sql,
        output_parts=("pubmed", "parquet", "pubmed_articles.parquet"),
    )
