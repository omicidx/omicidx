-- Base views over the public Parquet snapshot (reverse-ETL output of
-- `parquet-export`). {{PUBLIC_PARQUET_BASE}} is substituted at build time
-- from PUBLIC_PARQUET_HTTPS_BASE (config.py); the published views.sql ships
-- with the concrete URL so external DuckDB users can `.read` it directly.

create or replace view src_geo_series as (
    select *
    from read_parquet('{{PUBLIC_PARQUET_BASE}}/latest/geo_series.parquet')
);

create or replace view src_geo_samples as (
    select *
    from read_parquet('{{PUBLIC_PARQUET_BASE}}/latest/geo_samples.parquet')
);

-- TODO(derived): geo_series_with_rnaseq_counts is not yet exported by
-- parquet-export (orphaned ducklake loader). Lives under the public base at
-- geo/parquet/ (not latest/) until the derived loaders are wired.
create or replace view src_geo_series_with_rnaseq_counts as (
    select accession
    from read_parquet('{{PUBLIC_PARQUET_BASE}}/geo/parquet/geo_series_with_rnaseq_counts.parquet')
);

create or replace view src_geo_platforms as (
    select *
    from read_parquet('{{PUBLIC_PARQUET_BASE}}/latest/geo_platforms.parquet')
);

-----
--
-- SRA Views
--
-----

create or replace view src_sra_studies as (
    select *
    from read_parquet('{{PUBLIC_PARQUET_BASE}}/latest/sra_studies.parquet')
);

create or replace view src_sra_samples as (
    select *
    from read_parquet('{{PUBLIC_PARQUET_BASE}}/latest/sra_samples.parquet')
);

create or replace view src_sra_experiments as (
    select *
    from read_parquet('{{PUBLIC_PARQUET_BASE}}/latest/sra_experiments.parquet')
);

create or replace view src_sra_runs as (
    select *
    from read_parquet('{{PUBLIC_PARQUET_BASE}}/latest/sra_runs.parquet')
);

-- TODO(derived): sra_accessions is not yet exported by parquet-export
-- (orphaned ducklake loader). Lives under the public base at sra/parquet/
-- (not latest/) until the derived loaders are wired.
create or replace view src_sra_accessions as (
    select *
    from read_parquet('{{PUBLIC_PARQUET_BASE}}/sra/parquet/sra_accessions.parquet')
);

-----
--
-- BioSample and BioProject Views
--
-----

create or replace view src_biosamples as (
    select *
    from read_parquet('{{PUBLIC_PARQUET_BASE}}/latest/biosamples.parquet')
);

create or replace view src_bioprojects as (
    select *
    from read_parquet('{{PUBLIC_PARQUET_BASE}}/latest/bioprojects.parquet')
);

-----
--
-- PubMed Views
--
-----

create or replace view src_pubmed_articles as (
    select *
    from read_parquet('{{PUBLIC_PARQUET_BASE}}/latest/pubmed_articles.parquet')
);

-- End of base parquet views
