# OmicIDX

Cloud-native replacement for [SRAdb](https://bioconductor.org/packages/SRAdb/) and [GEOmetadb](https://bioconductor.org/packages/GEOmetadb/) — query 80M+ SRA runs, 8M GEO samples, and 50M biosamples via DuckDB.

Data is updated daily and served as Parquet files over HTTPS. No account, no API key, no download required.

## Quick Start

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Find human RNA-seq runs
df = con.sql("""
    SELECT accession, title, total_spots, total_bases
    FROM read_parquet('https://data-omicidx.cancerdatasci.org/sra/parquet/sra_runs.parquet') r
    JOIN read_parquet('https://data-omicidx.cancerdatasci.org/sra/parquet/sra_experiments.parquet') e
      ON r.experiment_accession = e.accession
    WHERE e.library_strategy = 'RNA-Seq'
      AND e.taxon_id = 9606
    LIMIT 10
""").df()
```

## Using the DuckDB Database

For the full experience with pre-built views (SRAdb and GEOmetadb compatible), download the database file:

```python
import duckdb

# Download once (~300 MB), then query locally
con = duckdb.connect("omicidx.duckdb")
con.execute("INSTALL httpfs; LOAD httpfs;")

# Load all views
for sql_file in ["020_base_parquet_views.sql", "030_staging_views.sql",
                 "040_geometadb_views.sql", "050_sradb_views.sql"]:
    # SQL files available in the sql/ directory of this repo
    pass

# Or build it yourself:
# uv run build_db.py
```

Once built, query with familiar SRAdb/GEOmetadb patterns:

```sql
-- SRAdb-style: find RNA-seq studies
SELECT * FROM sradb.study
WHERE study_type = 'Transcriptome Analysis'
LIMIT 10;

-- GEOmetadb-style: find GEO series with supplementary files
SELECT gse, title, supplementary_file
FROM geometadb.gse
JOIN geometadb.geo_supplemental_files ON gse = accession
WHERE supplementary_file LIKE '%counts%'
LIMIT 10;

-- Staging views: deduplicated, cleaned data
SELECT * FROM stg_sra_runs
WHERE total_bases > 1e9
LIMIT 10;
```

## Available Data

| Dataset | Table | Records | Source |
|---------|-------|---------|--------|
| SRA Runs | `src_sra_runs` | 83M+ | NCBI SRA |
| SRA Experiments | `src_sra_experiments` | 78M+ | NCBI SRA |
| SRA Samples | `src_sra_samples` | 81M+ | NCBI SRA |
| SRA Studies | `src_sra_studies` | 1.4M+ | NCBI SRA |
| SRA Accessions | `src_sra_accessions` | 143M+ | NCBI SRA |
| GEO Samples | `src_geo_samples` | 8.3M+ | NCBI GEO |
| GEO Series | `src_geo_series` | 280K+ | NCBI GEO |
| GEO Platforms | `src_geo_platforms` | 28K+ | NCBI GEO |
| BioSamples | `src_biosamples` | 51M+ | NCBI BioSample |
| BioProjects | `src_bioprojects` | 1M+ | NCBI BioProject |

## Parquet Endpoints

All data is available as Parquet files at `https://data-omicidx.cancerdatasci.org/`:

```
sra/parquet/sra_runs.parquet
sra/parquet/sra_experiments.parquet
sra/parquet/sra_samples.parquet
sra/parquet/sra_studies.parquet
sra/parquet/sra_accessions.parquet
geo/parquet/geo_series.parquet
geo/parquet/geo_samples.parquet
geo/parquet/geo_platforms.parquet
biosample/parquet/biosamples.parquet
bioproject/parquet/bioprojects.parquet
```

These work with any tool that reads Parquet over HTTP: DuckDB, Polars, PyArrow, R arrow, etc.

```r
# R example
library(arrow)
library(dplyr)

sra_runs <- read_parquet("https://data-omicidx.cancerdatasci.org/sra/parquet/sra_runs.parquet")
sra_runs |> filter(total_bases > 1e10) |> head()
```

## Building the Database

```bash
# Clone and build
git clone https://github.com/omicidx/omicidx.git
cd omicidx
uv run build_db.py

# This creates omicidx.duckdb with all views
```

## Schema

The SQL view definitions are in `sql/`:

- **`020_base_parquet_views.sql`** — `src_*` views: raw data access layer
- **`030_staging_views.sql`** — `stg_*` views: deduplicated and cleaned
- **`040_geometadb_views.sql`** — `geometadb.*` schema: GEO-compatible views
- **`050_sradb_views.sql`** — `sradb.*` schema: SRAdb-compatible views

## Related

- [omicidx-etl](https://github.com/omicidx/omicidx-etl) — ETL pipelines that extract and update the raw data

## License

MIT
