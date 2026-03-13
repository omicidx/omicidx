-- Staging views: cleaned, typed, and normalized
--
-- These stg_* views:
--   1. Apply type corrections and coercions
--   2. Normalize column names (lowercase, consistent naming)
--   3. Enrich with accession-level statistics where needed
--
-- Note: The parquet files are already deduplicated by the ETL pipeline.

-----
-- SRA Staging Views
-----

CREATE OR REPLACE VIEW stg_sra_studies AS
SELECT
    accession,
    alias,
    title,
    description,
    abstract,
    study_type,
    center_name,
    broker_name,
    bioproject AS bioproject_accession,
    geo AS geo_accession,
    identifiers,
    attributes,
    xrefs,
    pubmed_ids
FROM src_sra_studies;

CREATE OR REPLACE VIEW stg_sra_samples AS
SELECT
    accession,
    alias,
    title,
    organism,
    description,
    taxon_id,
    biosample AS biosample_accession,
    identifiers,
    attributes,
    xrefs
FROM src_sra_samples;

CREATE OR REPLACE VIEW stg_sra_experiments AS
SELECT
    accession,
    alias,
    title,
    design,
    center_name,
    study_accession,
    sample_accession,
    platform,
    instrument_model,
    library_name,
    library_construction_protocol,
    library_layout,
    TRY_CAST(library_layout_length AS INTEGER) AS library_layout_length,
    TRY_CAST(library_layout_sdev AS DOUBLE) AS library_layout_sdev,
    library_strategy,
    library_source,
    library_selection,
    spot_length,
    nreads,
    identifiers,
    attributes,
    xrefs,
    reads
FROM src_sra_experiments;

CREATE OR REPLACE VIEW stg_sra_runs AS
SELECT
    r.accession,
    r.alias,
    r.experiment_accession,
    r.title,
    a.spots AS total_spots,
    a.bases AS total_bases,
    r.identifiers,
    r.attributes,
    r.qualities
FROM src_sra_runs r
LEFT JOIN src_sra_accessions a ON r.accession = a.accession;

-----
-- GEO Staging Views
-----

CREATE OR REPLACE VIEW stg_geo_series AS
SELECT * FROM src_geo_series;

CREATE OR REPLACE VIEW stg_geo_samples AS
SELECT * FROM src_geo_samples;

CREATE OR REPLACE VIEW stg_geo_platforms AS
SELECT * FROM src_geo_platforms;

-----
-- Biosample/Bioproject Staging Views
-----

CREATE OR REPLACE VIEW stg_biosamples AS
SELECT * FROM src_biosamples;

CREATE OR REPLACE VIEW stg_bioprojects AS
SELECT * FROM src_bioprojects;

-----
-- PubMed Staging Views
-----

CREATE OR REPLACE VIEW stg_pubmed_articles AS
SELECT DISTINCT ON (pmid)
    pmid,
    title,
    abstract,
    journal,
    medline_ta,
    country,
    issn_linking,
    nlm_unique_id,
    pubdate,
    date_completed,
    date_revised,
    doi,
    pmc,
    issue,
    pages,
    languages,
    vernacular_title,
    other_id,
    authors,
    mesh_terms,
    publication_types,
    chemical_list,
    keywords,
    "references",
    grant_ids
FROM src_pubmed_articles;
