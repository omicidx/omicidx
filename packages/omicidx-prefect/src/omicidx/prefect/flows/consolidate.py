"""Consolidation flow: raw partitions → per-entity parquet.

Each entity is a separate task running a DuckDB COPY. Tasks here are
not partitioned — they aggregate across all raw partitions for the
entity. The flow takes no semaphores; the cost is in re-aggregation
(seconds-to-minutes) not in the upstream raw extracts.
"""

import httpx
from omicidx.prefect.config import get_duckdb_connection, get_duckdb_path

from prefect import flow, get_run_logger, task

_SRA_ACCESSIONS_URL = (
    "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab"
)


def _run_copy(sql: str, output_path: str) -> int:
    log = get_run_logger()
    with get_duckdb_connection() as con:
        log.info(f"Consolidating to {output_path}")
        con.execute(sql)
        row_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{output_path}')"
        ).fetchone()[0]
    log.info(f"Wrote {row_count:,} rows to {output_path}")
    return row_count


# -- GEO -----------------------------------------------------------------------


@task(retries=1, retry_delay_seconds=60)
def geo_platforms_parquet() -> int:
    output = get_duckdb_path("geo", "parquet", "geo_platforms.parquet")
    input_path = get_duckdb_path("geo", "raw", "gpl", "**", "*.ndjson.gz")
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
    return _run_copy(sql, output)


@task(retries=1, retry_delay_seconds=60)
def geo_series_parquet() -> int:
    output = get_duckdb_path("geo", "parquet", "geo_series.parquet")
    input_path = get_duckdb_path("geo", "raw", "gse", "**", "*.ndjson.gz")
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
    return _run_copy(sql, output)


@task(retries=1, retry_delay_seconds=60)
def geo_samples_parquet() -> int:
    output = get_duckdb_path("geo", "parquet", "geo_samples.parquet")
    input_path = get_duckdb_path("geo", "raw", "gsm", "**", "*.ndjson.gz")
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
    return _run_copy(sql, output)


@task(retries=1, retry_delay_seconds=60)
def geo_rnaseq_counts_parquet() -> int:
    output = get_duckdb_path("geo", "parquet", "geo_series_with_rnaseq_counts.parquet")
    input_path = get_duckdb_path("geo", "raw", "gse_with_rna_seq_counts.parquet")
    sql = f"""
        COPY (
            SELECT accession
            FROM read_parquet('{input_path}')
            ORDER BY accession
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _run_copy(sql, output)


# -- SRA -----------------------------------------------------------------------


@task(retries=1, retry_delay_seconds=60)
def sra_studies_parquet() -> int:
    output = get_duckdb_path("sra", "parquet", "sra_studies.parquet")
    input_path = get_duckdb_path("sra", "raw", "study", "**", "*parquet")
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
            FROM read_parquet('{input_path}', hive_partitioning=true)
            QUALIFY row_number() OVER (
                PARTITION BY accession ORDER BY date DESC, stage DESC
            ) = 1
            ORDER BY accession
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _run_copy(sql, output)


@task(retries=1, retry_delay_seconds=60)
def sra_samples_parquet() -> int:
    output = get_duckdb_path("sra", "parquet", "sra_samples.parquet")
    input_path = get_duckdb_path("sra", "raw", "sample", "**", "*parquet")
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
            FROM read_parquet('{input_path}', hive_partitioning=true)
            QUALIFY row_number() OVER (
                PARTITION BY accession ORDER BY date DESC, stage DESC
            ) = 1
            ORDER BY accession
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _run_copy(sql, output)


@task(retries=1, retry_delay_seconds=60)
def sra_experiments_parquet() -> int:
    output = get_duckdb_path("sra", "parquet", "sra_experiments.parquet")
    input_path = get_duckdb_path("sra", "raw", "experiment", "**", "*parquet")
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
            FROM read_parquet('{input_path}', hive_partitioning=true)
            QUALIFY row_number() OVER (
                PARTITION BY accession ORDER BY date DESC, stage DESC
            ) = 1
            ORDER BY accession
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _run_copy(sql, output)


@task(retries=1, retry_delay_seconds=60)
def sra_runs_parquet() -> int:
    output = get_duckdb_path("sra", "parquet", "sra_runs.parquet")
    input_path = get_duckdb_path("sra", "raw", "run", "**", "*parquet")
    sql = f"""
        COPY (
            SELECT
                trim(accession) as accession,
                trim(alias) as alias,
                trim(experiment_accession) as experiment_accession,
                trim(title) as title,
                identifiers, attributes, qualities
            FROM read_parquet('{input_path}', hive_partitioning=true)
            QUALIFY row_number() OVER (
                PARTITION BY accession ORDER BY date DESC, stage DESC
            ) = 1
            ORDER BY accession
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _run_copy(sql, output)


@task(retries=1, retry_delay_seconds=60)
def sra_accessions_parquet() -> int:
    """Ingest SRA_Accessions.tab → parquet. Checked for ETag freshness upstream."""
    output = get_duckdb_path("sra", "parquet", "sra_accessions.parquet")
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
                '{_SRA_ACCESSIONS_URL}',
                nullstr = '-'
            )
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _run_copy(sql, output)


def _fetch_etag(url: str) -> str | None:
    try:
        response = httpx.head(url, timeout=10, follow_redirects=True)
        response.raise_for_status()
    except httpx.RequestError:
        return None
    etag = response.headers.get("ETag")
    if etag:
        etag = etag.strip('"')
    return etag


@task(retries=1, retry_delay_seconds=60)
def sra_accessions_if_changed(force: bool = False) -> dict:
    """Run sra_accessions_parquet only if the URL's ETag changed.

    The semaphore for namespace `sra_accessions_etag` stores the most
    recently seen ETag (one key: 'latest'). If the upstream ETag differs
    we re-ingest and update the semaphore.
    """
    from omicidx.prefect.semaphore import SemaphoreStore

    log = get_run_logger()
    sem = SemaphoreStore("sra_accessions_etag")
    etag = _fetch_etag(_SRA_ACCESSIONS_URL)
    if not etag:
        log.warning("No ETag returned for SRA_Accessions.tab; running ingest anyway")
    else:
        last = sem.read("latest")
        last_etag = (last or {}).get("metadata", {}).get("etag")
        if not force and etag == last_etag:
            log.info(f"SRA_Accessions ETag unchanged ({etag}); skipping ingest")
            return {"skipped": True, "etag": etag}

    rows = sra_accessions_parquet()
    sem.mark_done(
        "latest",
        metadata={"etag": etag, "row_count": rows, "url": _SRA_ACCESSIONS_URL},
    )
    return {"skipped": False, "etag": etag, "row_count": rows}


# -- BioSample -----------------------------------------------------------------


@task(retries=1, retry_delay_seconds=60)
def biosample_parquet() -> int:
    output = get_duckdb_path("biosample", "parquet", "biosamples.parquet")
    input_path = get_duckdb_path("biosample", "raw", "data.jsonl.gz")
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
    return _run_copy(sql, output)


# -- PubMed --------------------------------------------------------------------


@task(retries=1, retry_delay_seconds=60)
def pubmed_parquet() -> int:
    output = get_duckdb_path("pubmed", "parquet", "pubmed_articles.parquet")
    input_path = get_duckdb_path("pubmed", "raw", "*.parquet")
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
            QUALIFY row_number() OVER (
                PARTITION BY pmid
                ORDER BY TRY_CAST(date_revised AS DATE) DESC NULLS LAST,
                         TRY_CAST(date_completed AS DATE) DESC NULLS LAST
            ) = 1
            ORDER BY pmid
        ) TO '{output}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    return _run_copy(sql, output)


# -- Top-level consolidation flow ---------------------------------------------


@flow(name="consolidate")
def consolidate_flow() -> None:
    """Run every per-entity consolidation task. Order is unconstrained."""
    geo_platforms_parquet()
    geo_series_parquet()
    geo_samples_parquet()
    geo_rnaseq_counts_parquet()
    sra_studies_parquet()
    sra_samples_parquet()
    sra_experiments_parquet()
    sra_runs_parquet()
    sra_accessions_if_changed()
    biosample_parquet()
    pubmed_parquet()


if __name__ == "__main__":
    consolidate_flow()
