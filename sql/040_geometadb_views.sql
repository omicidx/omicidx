create schema IF NOT EXISTS geometadb;

use geometadb;

create or replace view gsm as (
SELECT
    title,
    accession AS gsm,
    platform_id AS gpl,
    status,
    submission_date,
    last_update_date,
    type,
    channels[1].source_name AS source_name_ch1,
    channels[1].organism AS organism_ch1,
    channels[1].characteristics AS characteristics_ch1,
    channels[1].molecule AS molecule_ch1,
    channels[1].label AS label_ch1,
    channels[1].treatment_protocol AS treatment_protocol_ch1,
    channels[1].extract_protocol AS extract_protocol_ch1,
    channels[1].label_protocol AS label_protocol_ch1,
    channels[2].source_name AS source_name_ch2,
    channels[2].organism AS organism_ch2,
    channels[2].characteristics AS characteristics_ch2,
    channels[2].molecule AS molecule_ch2,
    channels[2].label AS label_ch2,
    channels[2].treatment_protocol AS treatment_protocol_ch2,
    channels[2].extract_protocol AS extract_protocol_ch2,
    channels[2].label_protocol AS label_protocol_ch2,
    channels AS channel_records,
    hyb_protocol,
    description,
    data_processing,
    contact."name"."first" || ' ' || contact."name"."last" AS contact,
    supplemental_files,
    data_row_count,
    channel_count
FROM src_geo_samples
);

create or replace view gse as (
WITH has_geo_computed_rnaseq AS (
    SELECT
        r.accession
    FROM
        src_geo_series_with_rnaseq_counts r
)
SELECT
    g.accession AS gse,
    title,
    status,
    submission_date,
    last_update_date,
    summary,
    pubmed_id,
    type,
    contributor,
    'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=' || g.accession AS web_link,
    overall_design,
    contact.country AS contact_country,
    contact.email AS contact_email,
    contact."name"."first" AS contact_first_name,
    contact.institute AS contact_institute,
    contact."name"."last" AS contact_last_name,
    contact."name"."first" || ' ' || contact."name"."last" AS contact,
    supplemental_files,
    data_processing,

    -- Indicates if the GEO Series has associated ncbi-supplied RNA-Seq data
    CASE WHEN h.accession IS NOT NULL THEN TRUE ELSE FALSE END AS has_geo_computed_rnaseq
FROM src_geo_series g
LEFT JOIN has_geo_computed_rnaseq h
    ON g.accession = h.accession
);

create or replace view gpl as (
    SELECT
    title,
    accession AS gpl,
    status,
    submission_date,
    last_update_date,
    technology,
    distribution,
    organism,
    manufacturer,
    manufacture_protocol,
    description,
    'https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=' || accession AS web_link,
    contact."name"."first" || ' ' || contact."name"."last" AS contact,
    data_row_count,
    summary
FROM src_geo_platforms
);

------
--
--  JOIN tables
--
------

create or replace view gse_gpl as (
SELECT DISTINCT
    accession AS gpl,
    UNNEST(series_id) AS gse
FROM src_geo_platforms
);


create or replace view gse_gsm as (
SELECT DISTINCT
    accession AS gse,
    UNNEST(sample_id) AS gsm
FROM src_geo_series
);

create or replace view geo_supplemental_files as (
WITH supp_file AS (
    SELECT
        accession,
        'gse' AS accession_type,
        UNNEST(supplemental_files) AS supplemental_file
    FROM src_geo_series

    UNION ALL

    SELECT
        accession,
        'gsm' AS accession_type,
        UNNEST(supplemental_files) AS supplemental_file
    FROM src_geo_samples
)
SELECT
    accession,
    accession_type,
    regexp_replace(supplemental_file, '^ftp://', 'https://') AS supplemental_file,
    regexp_extract(supplemental_file, '[^/]+$') AS filename
FROM supp_file
WHERE supplemental_file != 'NONE'
);

-- End of geometadb views
use main;