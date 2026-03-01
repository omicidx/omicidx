create or replace view src_geo_series as (
    select *
    from read_parquet('https://data-omicidx.cancerdatasci.org/geo/parquet/geo_series.parquet')
);

create or replace view src_geo_samples as (
    select *
    from read_parquet('https://data-omicidx.cancerdatasci.org/geo/parquet/geo_samples.parquet')
);

create or replace view src_geo_series_with_rnaseq_counts as (
    select json_extract_string(accession,'accession') as accession
    from read_parquet('https://data-omicidx.cancerdatasci.org/geo/parquet/geo_series_with_rnaseq_counts.parquet')
);

create or replace view src_geo_platforms as (
    select *
    from read_parquet('https://data-omicidx.cancerdatasci.org/geo/parquet/geo_platforms.parquet')
);

create or replace view src_sra_accessions as (
    select *
    from read_parquet('https://data-omicidx.cancerdatasci.org/sra/parquet/sra_accessions.parquet')
);

-----
--
-- SRA Views
--
-----

create or replace view src_sra_studies as (
    select *
    from read_parquet('https://data-omicidx.cancerdatasci.org/sra/parquet/sra_studies.parquet')
);

create or replace view src_sra_samples as (
    select *
    from read_parquet('https://data-omicidx.cancerdatasci.org/sra/parquet/sra_samples.parquet')
);

create or replace view src_sra_experiments as (
    select *
    from read_parquet('https://data-omicidx.cancerdatasci.org/sra/parquet/sra_experiments.parquet')
);

create or replace view src_sra_runs as (
    select *
    from read_parquet('https://data-omicidx.cancerdatasci.org/sra/parquet/sra_runs.parquet')
);

create or replace view src_sra_accessions as (
    select *
    from read_parquet('https://data-omicidx.cancerdatasci.org/sra/parquet/sra_accessions.parquet')
);

-----
--
-- BioSample and BioProject Views
--
-----

create or replace view src_biosamples as (
    select *
    from read_parquet('https://data-omicidx.cancerdatasci.org/biosample/parquet/biosamples.parquet')
);

create or replace view src_bioprojects as (
    select *
    from read_parquet('https://data-omicidx.cancerdatasci.org/bioproject/parquet/bioprojects.parquet')
);

-- End of base parquet views

