-- Staging views: deduplicated, cleaned, and typed
--
-- The src_* views contain all snapshots (Full + Incrementals).
-- These stg_* views:
--   1. Deduplicate to get the latest version of each record
--   2. Apply type corrections and coercions
--   3. Normalize column names (lowercase, consistent naming)
--   4. Drop internal columns (date, stage) not needed downstream
--
-- Deduplication logic:
--   - PARTITION BY accession (unique identifier)
--   - ORDER BY date DESC, stage DESC (Incremental beats Full on same date)
--   - Take row_number = 1

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
    BioProject AS bioproject_accession,
    GEO AS geo_accession,
    identifiers,
    attributes,
    xrefs,
    pubmed_ids
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY accession
            ORDER BY date DESC, stage DESC
        ) as rn
    FROM src_sra_studies
)
WHERE rn = 1;

CREATE OR REPLACE VIEW stg_sra_samples AS
SELECT
    accession,
    alias,
    title,
    organism,
    description,
    taxon_id,
    geo AS geo_accession,
    BioSample AS biosample_accession,
    identifiers,
    attributes,
    xrefs
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY accession
            ORDER BY date DESC, stage DESC
        ) as rn
    FROM src_sra_samples
)
WHERE rn = 1;

CREATE OR REPLACE VIEW stg_sra_experiments AS
SELECT
    accession,
    alias,
    title,
    description,
    design,
    center_name,
    study_accession,
    sample_accession,
    platform,
    instrument_model,
    library_name,
    library_construction_protocol,
    library_layout,
    library_layout_orientation,
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
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY accession
            ORDER BY date DESC, stage DESC
        ) as rn
    FROM src_sra_experiments
)
WHERE rn = 1;

CREATE OR REPLACE VIEW stg_sra_runs AS
SELECT
    accession,
    alias,
    experiment_accession,
    title,
    total_spots,
    total_bases,
    size AS size_bytes,
    avg_length,
    identifiers,
    attributes,
    files,
    reads,
    base_counts,
    qualities,
    tax_analysis
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY accession
            ORDER BY date DESC, stage DESC
        ) as rn
    FROM src_sra_runs
)
WHERE rn = 1;

-----
-- GEO Staging Views
-- Passthrough for now - add deduplication if needed
-----

CREATE OR REPLACE VIEW stg_geo_series AS
SELECT * FROM src_geo_series;

CREATE OR REPLACE VIEW stg_geo_samples AS
SELECT * FROM src_geo_samples;

CREATE OR REPLACE VIEW stg_geo_platforms AS
SELECT * FROM src_geo_platforms;

-----
-- Biosample/Bioproject Staging Views
-- Passthrough for consistency in naming convention
-----

CREATE OR REPLACE VIEW stg_biosamples AS
SELECT * FROM src_biosamples;

CREATE OR REPLACE VIEW stg_bioprojects AS
SELECT * FROM src_bioprojects;

-----
-- PubMed Staging Views
-----

CREATE OR REPLACE VIEW stg_pubmed_articles AS
SELECT
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
    authors,
    mesh_terms,
    publication_types,
    chemical_list,
    keywords,
    references,
    grant_ids
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY pmid
            ORDER BY _inserted_at DESC
        ) as rn
    FROM src_pubmed_articles
)
WHERE rn = 1;
