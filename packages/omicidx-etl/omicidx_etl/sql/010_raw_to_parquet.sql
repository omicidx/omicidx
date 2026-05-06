--------
--
-- GEO Parquet Files
--
--------


copy (
    select accession.accession as accession
    from read_parquet('r2://omicidx/geo/raw/gse_with_rna_seq_counts.parquet')
    order by accession
) to 'r2://omicidx/geo/parquet/geo_series_with_rnaseq_counts.parquet' (format parquet, compression zstd);

select count(*) from read_parquet('r2://omicidx/geo/parquet/geo_series_with_rnaseq_counts.parquet');

copy (
    select
        trim(title) as title,
        trim(status) as status,
        submission_date,
        last_update_date,
        trim(accession) as accession,
        contact,
        trim(organism) as organism,
        sample_id,
        series_id,
        trim(technology) as technology,
        trim(description) as description,
        trim(distribution) as distribution,
        manufacturer,
        data_row_count,
        contributor,
        relation,
        trim(manufacture_protocol) as manufacture_protocol
    from read_ndjson_auto('r2://omicidx/geo/raw/gpl/**/*.ndjson.gz')
    order by accession
) to 'r2://omicidx/geo/parquet/geo_platforms.parquet' (format parquet, compression zstd);


select count(*) from read_parquet('r2://omicidx/geo/parquet/geo_platforms.parquet');

copy (
    select
        trim(title) as title,
        trim(status) as status,
        submission_date,
        last_update_date,
        trim(accession) as accession,
        subseries,
        bioprojects,
        sra_studies,
        contact,
        type,
        trim(summary) as summary,
        relation,
        pubmed_id,
        sample_id,
        sample_taxid,
        sample_organism,
        platform_id,
        platform_taxid,
        platform_organism,
        supplemental_files,
        trim(overall_design) as overall_design,
        contributor
    from read_ndjson_auto('r2://omicidx/geo/raw/gse/**/*.ndjson.gz')
    order by accession
) to 'r2://omicidx/geo/parquet/geo_series.parquet' (format parquet, compression zstd);


select count(*) from read_parquet('r2://omicidx/geo/parquet/geo_series.parquet');

copy (
    select
        trim(title) as title,
        trim(status) as status,
        submission_date,
        last_update_date,
        trim(type) as type,
        trim(anchor) as anchor,
        contact,
        trim(description) as description,
        trim(accession) as accession,
        biosample,
        tag_count,
        tag_length,
        trim(platform_id) as platform_id,
        trim(hyb_protocol) as hyb_protocol,
        channel_count,
        trim(scan_protocol) as scan_protocol,
        data_row_count,
        library_source,
        sra_experiment,
        trim(data_processing) as data_processing,
        supplemental_files,
        channels,
        contributor
    from read_ndjson_auto('r2://omicidx/geo/raw/gsm/**/*.ndjson.gz')
    order by accession
) to 'r2://omicidx/geo/parquet/geo_samples.parquet' (format parquet, compression zstd);


select count(*) from read_parquet('r2://omicidx/geo/parquet/geo_samples.parquet');

--------
--
-- SRA Parquet Files
--
--------

copy (
    select
        trim("Accession") as accession,
        trim("Submission") as submission,
        trim("Status") as status,
        "Updated" as updated,
        "Published" as published,
        "Received" as received,
        trim("Type") as type,
        trim("Center") as center,
        trim("Visibility") as visibility,
        trim("Alias") as alias,
        trim("Experiment") as experiment,
        trim("Sample") as sample,
        trim("Study") as study,
        "Loaded" as loaded,
        "Spots" as spots,
        "Bases" as bases,
        trim("Md5sum") as md5sum,
        trim("BioSample") as biosample,
        trim("BioProject") as bioproject,
        trim("ReplacedBy") as replacedby
    from read_csv_auto(
            'https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab',
            nullstr = '-'
        )
) to 'r2://omicidx/sra/parquet/sra_accessions.parquet' (format parquet, compression zstd);

select count(*) from read_parquet('r2://omicidx/sra/parquet/sra_accessions.parquet');

copy (
    select
        trim(accession) as accession,
        trim(alias) as alias,
        trim(experiment_accession) as experiment_accession,
        trim(title) as title,
        identifiers,
        attributes,
        qualities
    from read_parquet('r2://omicidx/sra/raw/run/**/*parquet')
    order by accession
) to 'r2://omicidx/sra/parquet/sra_runs.parquet' (format parquet, compression zstd);


select count(*) from read_parquet('r2://omicidx/sra/parquet/sra_runs.parquet');

copy (
    select
        trim(accession) as accession,
        trim(experiment_accession) as experiment_accession,
        trim(alias) as alias,
        trim(title) as title,
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
        spot_length,
        nreads,
        identifiers,
        attributes,
        xrefs,
        reads
    from read_parquet('r2://omicidx/sra/raw/experiment/**/*parquet')
    order by accession
) to 'r2://omicidx/sra/parquet/sra_experiments.parquet' (format parquet, compression zstd);


select count(*) from read_parquet('r2://omicidx/sra/parquet/sra_experiments.parquet');

copy (
    select
        trim(accession) as accession,
        trim(alias) as alias,
        trim(title) as title,
        trim(organism) as organism,
        trim(description) as description,
        taxon_id,
        trim("BioSample") as biosample,
        identifiers,
        attributes,
        xrefs
    from read_parquet('r2://omicidx/sra/raw/sample/**/*parquet')
    order by accession
) to 'r2://omicidx/sra/parquet/sra_samples.parquet' (format parquet, compression zstd);


select count(*) from read_parquet('r2://omicidx/sra/parquet/sra_samples.parquet');

copy (
    select
        trim(accession) as accession,
        trim(study_accession) as study_accession,
        trim(alias) as alias,
        trim(title) as title,
        trim(description) as description,
        trim(abstract) as abstract,
        trim(study_type) as study_type,
        trim(center_name) as center_name,
        trim(broker_name) as broker_name,
        trim("BioProject") as bioproject,
        trim("GEO") as geo,
        identifiers,
        attributes,
        xrefs,
        pubmed_ids
    from read_parquet('r2://omicidx/sra/raw/study/**/*parquet')
    order by accession
) to 'r2://omicidx/sra/parquet/sra_studies.parquet' (format parquet, compression zstd);


select count(*) from read_parquet('r2://omicidx/sra/parquet/sra_studies.parquet');

--------
--
-- BioProject and BioSample Parquet Files
--
--------


copy (
    select
        trim(title) as title,
        trim(description) as description,
        trim(name) as name,
        trim(accession) as accession,
        publications,
        locus_tags,
        release_date,
        data_types,
        external_links
    from read_ndjson_auto(
            'r2://omicidx/biosample/biosample/raw/bioproject/raw/bioproject.jsonl.gz',
            maximum_object_size = 1000000000
        )
) to 'r2://omicidx/bioproject/parquet/bioprojects.parquet' (format parquet, compression zstd);


select count(*) from read_parquet('r2://omicidx/bioproject/parquet/bioprojects.parquet');

copy (
    select
        trim(submission_date) as submission_date,
        trim(last_update) as last_update,
        trim(publication_date) as publication_date,
        trim(access) as access,
        trim(id) as id,
        trim(accession) as accession,
        id_recs,
        ids,
        trim(sra_sample) as sra_sample,
        trim(dbgap) as dbgap,
        trim(gsm) as gsm,
        trim(title) as title,
        trim(description) as description,
        trim(taxonomy_name) as taxonomy_name,
        taxon_id,
        attribute_recs,
        attributes,
        trim(model) as model
    from read_ndjson_auto(
            'r2://omicidx/biosample/biosample/raw/biosample/raw/biosample.jsonl.gz',
            maximum_object_size = 1000000000
        )
) to 'r2://omicidx/biosample/parquet/biosamples.parquet' (format parquet, compression zstd);

select count(*) from read_parquet('r2://omicidx/biosample/parquet/biosamples.parquet');

--------
--
-- PubMed Parquet Files
--
--------

copy (
    select
        trim(title) as title,
        trim(issue) as issue,
        trim(pages) as pages,
        trim(abstract) as abstract,
        trim(journal) as journal,
        authors,
        trim(pubdate) as pubdate,
        trim(pmid) as pmid,
        trim(mesh_terms) as mesh_terms,
        trim(publication_types) as publication_types,
        trim(chemical_list) as chemical_list,
        trim(keywords) as keywords,
        trim(doi) as doi,
        "references",
        trim(languages) as languages,
        trim(vernacular_title) as vernacular_title,
        trim(date_completed) as date_completed,
        trim(date_revised) as date_revised,
        trim(pmc) as pmc,
        trim(other_id) as other_id,
        trim(medline_ta) as medline_ta,
        trim(nlm_unique_id) as nlm_unique_id,
        trim(issn_linking) as issn_linking,
        trim(country) as country,
        grant_ids
    from read_parquet('r2://omicidx/pubmed/raw/*.parquet')
    where delete is not true
    order by pmid
) to 'r2://omicidx/pubmed/parquet/pubmed_articles.parquet' (format parquet, compression zstd);

select count(*) from read_parquet('r2://omicidx/pubmed/parquet/pubmed_articles.parquet');
