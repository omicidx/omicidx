-- SRAdb-compatible views over OmicIDX parquet files
-- These views approximate the functionality of the original SRAmetadb.sqlite
-- using DuckDB as the engine and parquet files as the backend storage.
--
-- Usage:
--   1. First run 020_base_parquet_views.sql to create the src_* views
--   2. Then run 030_staging_views.sql to create the stg_* views (deduplicated)
--   3. Then run this file to create the sradb schema and views
--
-- The views are designed to be compatible with existing SRAdb queries where possible.

CREATE SCHEMA IF NOT EXISTS sradb;

USE sradb;

-----
-- study table
-- Maps: stg_sra_studies -> sradb.study
-----
CREATE OR REPLACE VIEW study AS
SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS study_ID,
    alias AS study_alias,
    accession AS study_accession,
    title AS study_title,
    study_type,
    abstract AS study_abstract,
    broker_name,
    center_name,
    bioproject_accession AS center_project_name,
    description AS study_description,
    NULL AS related_studies,
    NULL AS primary_study,
    NULL AS sra_link,
    NULL AS study_url_link,
    NULL AS xref_link,
    NULL AS study_entrez_link,
    NULL AS ddbj_link,
    NULL AS ena_link,
    -- Convert attributes array to JSON string for compatibility
    CAST(attributes AS VARCHAR) AS study_attribute,
    NULL AS submission_accession,
    NULL AS sradb_updated
FROM main.stg_sra_studies;

-----
-- sample table
-- Maps: stg_sra_samples -> sradb.sample
-----
CREATE OR REPLACE VIEW sample AS
SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS sample_ID,
    alias AS sample_alias,
    accession AS sample_accession,
    NULL AS broker_name,
    NULL AS center_name,
    taxon_id,
    organism AS scientific_name,
    NULL AS common_name,
    NULL AS anonymized_name,
    NULL AS individual_name,
    description,
    NULL AS sra_link,
    NULL AS sample_url_link,
    NULL AS xref_link,
    NULL AS sample_entrez_link,
    NULL AS ddbj_link,
    NULL AS ena_link,
    CAST(attributes AS VARCHAR) AS sample_attribute,
    NULL AS submission_accession,
    NULL AS sradb_updated
FROM main.stg_sra_samples;

-----
-- experiment table
-- Maps: stg_sra_experiments -> sradb.experiment
-----
CREATE OR REPLACE VIEW experiment AS
SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS experiment_ID,
    NULL AS bamFile,
    NULL AS fastqFTP,
    alias AS experiment_alias,
    accession AS experiment_accession,
    NULL AS broker_name,
    center_name,
    title,
    NULL AS study_name,
    study_accession,
    design AS design_description,
    NULL AS sample_name,
    sample_accession,
    NULL AS sample_member,
    library_name,
    library_strategy,
    library_source,
    library_selection,
    library_layout,
    NULL AS targeted_loci,
    library_construction_protocol,
    spot_length,
    NULL AS adapter_spec,
    CAST(reads AS VARCHAR) AS read_spec,
    platform,
    instrument_model,
    NULL AS platform_parameters,
    NULL AS sequence_space,
    NULL AS base_caller,
    NULL AS quality_scorer,
    NULL AS number_of_levels,
    NULL AS multiplier,
    NULL AS qtype,
    NULL AS sra_link,
    NULL AS experiment_url_link,
    NULL AS xref_link,
    NULL AS experiment_entrez_link,
    NULL AS ddbj_link,
    NULL AS ena_link,
    CAST(attributes AS VARCHAR) AS experiment_attribute,
    NULL AS submission_accession,
    NULL AS sradb_updated
FROM main.stg_sra_experiments;

-----
-- run table
-- Maps: stg_sra_runs -> sradb.run
-----
CREATE OR REPLACE VIEW run AS
SELECT
    ROW_NUMBER() OVER (ORDER BY accession) AS run_ID,
    NULL AS bamFile,
    alias AS run_alias,
    accession AS run_accession,
    NULL AS broker_name,
    NULL AS instrument_name,
    NULL AS run_date,
    CAST(files AS VARCHAR) AS run_file,
    NULL AS run_center,
    NULL AS total_data_blocks,
    experiment_accession,
    NULL AS experiment_name,
    NULL AS sra_link,
    NULL AS run_url_link,
    NULL AS xref_link,
    NULL AS run_entrez_link,
    NULL AS ddbj_link,
    NULL AS ena_link,
    CAST(attributes AS VARCHAR) AS run_attribute,
    NULL AS submission_accession,
    NULL AS sradb_updated
FROM main.stg_sra_runs;

-----
-- sra table (denormalized join of all entities)
-- This is the main table that SRAdb users typically query
-- Maps: stg_sra_runs + experiments + samples + studies -> sradb.sra
-----
CREATE OR REPLACE VIEW sra AS
SELECT
    ROW_NUMBER() OVER (ORDER BY r.accession) AS sra_ID,
    NULL AS SRR_bamFile,
    NULL AS SRX_bamFile,
    NULL AS SRX_fastqFTP,
    -- Run fields
    ROW_NUMBER() OVER (ORDER BY r.accession) AS run_ID,
    r.alias AS run_alias,
    r.accession AS run_accession,
    NULL AS run_date,
    NULL AS updated_date,
    r.total_spots AS spots,
    r.total_bases AS bases,
    NULL AS run_center,
    NULL AS experiment_name,
    NULL AS run_url_link,
    NULL AS run_entrez_link,
    CAST(r.attributes AS VARCHAR) AS run_attribute,
    -- Experiment fields
    ROW_NUMBER() OVER (ORDER BY e.accession) AS experiment_ID,
    e.alias AS experiment_alias,
    e.accession AS experiment_accession,
    e.title AS experiment_title,
    NULL AS study_name,
    NULL AS sample_name,
    e.design AS design_description,
    e.library_name,
    e.library_strategy,
    e.library_source,
    e.library_selection,
    e.library_layout,
    e.library_construction_protocol,
    NULL AS adapter_spec,
    CAST(e.reads AS VARCHAR) AS read_spec,
    e.platform,
    e.instrument_model,
    NULL AS instrument_name,
    NULL AS platform_parameters,
    NULL AS sequence_space,
    NULL AS base_caller,
    NULL AS quality_scorer,
    NULL AS number_of_levels,
    NULL AS multiplier,
    NULL AS qtype,
    NULL AS experiment_url_link,
    NULL AS experiment_entrez_link,
    CAST(e.attributes AS VARCHAR) AS experiment_attribute,
    -- Sample fields
    ROW_NUMBER() OVER (ORDER BY sa.accession) AS sample_ID,
    sa.alias AS sample_alias,
    sa.accession AS sample_accession,
    sa.taxon_id,
    NULL AS common_name,
    NULL AS anonymized_name,
    NULL AS individual_name,
    sa.description,
    NULL AS sample_url_link,
    NULL AS sample_entrez_link,
    CAST(sa.attributes AS VARCHAR) AS sample_attribute,
    -- Study fields
    ROW_NUMBER() OVER (ORDER BY st.accession) AS study_ID,
    st.alias AS study_alias,
    st.accession AS study_accession,
    st.title AS study_title,
    st.study_type,
    st.abstract AS study_abstract,
    st.bioproject_accession AS center_project_name,
    st.description AS study_description,
    NULL AS study_url_link,
    NULL AS study_entrez_link,
    CAST(st.attributes AS VARCHAR) AS study_attribute,
    NULL AS related_studies,
    NULL AS primary_study,
    -- Submission fields (not available in current data)
    NULL AS submission_ID,
    NULL AS submission_accession,
    NULL AS submission_comment,
    NULL AS submission_center,
    NULL AS submission_lab,
    NULL AS submission_date,
    NULL AS sradb_updated
FROM main.stg_sra_runs r
LEFT JOIN main.stg_sra_experiments e ON r.experiment_accession = e.accession
LEFT JOIN main.stg_sra_samples sa ON e.sample_accession = sa.accession
LEFT JOIN main.stg_sra_studies st ON e.study_accession = st.accession;

-----
-- Convenience views for common queries
-----

-- View to get run info with study context (common use case)
CREATE OR REPLACE VIEW run_with_study AS
SELECT
    r.accession AS run_accession,
    r.total_spots,
    r.total_bases,
    e.accession AS experiment_accession,
    e.library_strategy,
    e.library_source,
    e.library_selection,
    e.library_layout,
    e.platform,
    e.instrument_model,
    sa.accession AS sample_accession,
    sa.organism,
    sa.taxon_id,
    st.accession AS study_accession,
    st.title AS study_title,
    st.study_type,
    st.bioproject_accession AS BioProject
FROM main.stg_sra_runs r
LEFT JOIN main.stg_sra_experiments e ON r.experiment_accession = e.accession
LEFT JOIN main.stg_sra_samples sa ON e.sample_accession = sa.accession
LEFT JOIN main.stg_sra_studies st ON e.study_accession = st.accession;

-- View for RNA-seq experiments (common filter)
CREATE OR REPLACE VIEW rnaseq_runs AS
SELECT *
FROM run_with_study
WHERE library_strategy = 'RNA-Seq';

-- View for WGS experiments
CREATE OR REPLACE VIEW wgs_runs AS
SELECT *
FROM run_with_study
WHERE library_strategy = 'WGS';

-- View for human samples
CREATE OR REPLACE VIEW human_runs AS
SELECT *
FROM run_with_study
WHERE taxon_id = 9606;

-- View for mouse samples
CREATE OR REPLACE VIEW mouse_runs AS
SELECT *
FROM run_with_study
WHERE taxon_id = 10090;

-- End of SRAdb-compatible views
use main;