# omicidx-gh-etl

ETL pipelines for OmicIDX metadata resources.

Current production extraction components are:

- `sra`
- `geo`
- `biosample`

## ETL status badges

| Workflow | Description |
|-------|------| 
| [![Biosample](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/ncbi_biosample.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/ncbi_biosample.yaml) | All metadata from the NCBI Biosample and Bioproject databases |
| [![ETL for NCBI GEO](https://github.com/omicidx/omicidx-etl/actions/workflows/ncbi_geo_etl.yaml/badge.svg)](https://github.com/omicidx/omicidx-etl/actions/workflows/ncbi_geo_etl.yaml) | All metadata from NCBI Gene Expression Omnibus (GEO) |
| [![SRA](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/ncbi_sra_etl.yaml/badge.svg)](https://github.com/seandavi/omicidx-gh-etl/actions/workflows/ncbi_sra_etl.yaml) | All metadata from NCBI Sequence Read Archive (SRA) |
| [![PubMed](https://github.com/omicidx/omicidx-etl/actions/workflows/pubmed_etl.yaml/badge.svg)](https://github.com/omicidx/omicidx-etl/actions/workflows/pubmed_etl.yaml) | All metadata from NCBI PubMed (titles, abstracts, and metadata) |


## Components

Note that in the table descriptions below, details of last modified date, bytes, etc. are not routinely updated. 
The important information is the schema description for each table. 

## Usage

### Biosample/Bioproject

```bash
uv run oidx biosample extract s3://omicidx
```

```
 → Creates: s3://omicidx/biosample/raw/data.jsonl.gz
            s3://omicidx/bioproject/raw/data.jsonl.gz
```

### SRA (--dest is now required)

```bash
uv run oidx sra extract --dest s3://omicidx/sra/raw
```

```
 → Creates: s3://omicidx/sra/raw/{entity}/date=YYYY-MM-DD/stage=Full/data_*.parquet
```

### GEO

```bash
uv run oidx geo extract s3://omicidx
```

```
# → Creates: s3://omicidx/geo/raw/{gse,gsm,gpl}/year=YYYY/month=MM/data_0.ndjson.gz
```

### PubMed

```bash
uv run oidx pubmed extract s3://omicidx
```

```
# → Creates: s3://omicidx/pubmed/raw/pubmed*.parquet
```
