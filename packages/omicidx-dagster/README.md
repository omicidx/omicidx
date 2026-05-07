# omicidx-dagster

Dagster code location for OmicIDX ETL pipelines (SRA, GEO, BioSample, PubMed) + SQL consolidation layer.

## Quick Start

### Development Mode (with live reload)

For local development with automatic code reload:

```bash
# 1. Copy .env.example to .env and configure
cp .env.example .env

# 2. Build the image (only needed once or after dependency changes)
docker compose -f docker-compose.dev.yml build

# 3. Start the gRPC server with bind-mounted source code
docker compose -f docker-compose.dev.yml up
```

Changes to Python files in `src/` will be picked up automatically by Dagster's gRPC server without rebuilding.

### Production Mode (standalone deployment)

For deployment to existing Dagster infrastructure:

```bash
# 1. Configure environment
cp .env.example .env

# 2. Build and start
docker compose build
docker compose up -d
```

This creates a self-contained gRPC code location server on port 4000.

## Running Locally (without Docker)

From the workspace root (`omicidx/`):

```bash
# Install workspace
uv sync

# Start gRPC server
dagster dev -m omicidx.dagster.definitions
```

Or point an existing Dagster deployment to this code location via `workspace.yaml`:

```yaml
load_from:
  - python_module:
      module_name: omicidx.dagster.definitions
      location_name: omicidx
```

## Architecture

### Assets

**Raw extraction assets** (from NCBI/GEO APIs):
- `biosample_raw`, `bioproject_raw` - BioSample/BioProject XML → JSONL.gz
- `sra_mirror_listing`, `sra_raw` - SRA metadata → Parquet (date partitioned)
- `geo_raw` - GEO SOFT files → NDJSON (monthly partitioned)
- `geo_rna_seq_counts` - GEO RNA-seq accession counts
- `pubmed_raw` - PubMed baseline + daily updates → Parquet

**Transform/SQL assets**:
- `bioproject_parquet` - consolidates BioProject JSONL → Parquet
- `consolidated_parquet` - runs DuckDB SQL to create normalized Parquet tables
- `omicidx_duckdb` - builds final DuckDB database with views

### Resources

- `OmicidxStorage` - S3/R2 storage abstraction (configured via env vars)
- `DuckDBResource` - DuckDB connection for SQL consolidation

### Schedules

- `daily_extract` - runs daily at 2 AM (BioSample, BioProject, SRA, GEO counts)
- `daily_geo` - partitioned GEO SOFT extraction (one partition per day)

### Sensors

- `pubmed_sensor` - detects new PubMed baseline/update files on NCBI FTP

## Environment Variables

See `.env.example`. Key variables:

- `S3_ENDPOINT` - S3-compatible endpoint (e.g., Cloudflare R2)
- `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` - credentials
- `PUBLISH_ROOT` - root S3/R2 URI (e.g., `s3://omicidx`)
- `DAGSTER_SENSOR_GRPC_TIMEOUT` - timeout for PubMed sensor (default 300s)

## Integration with monode Infrastructure

For deployment to the monode Dagster infrastructure, this code location is registered in:

```
monode/infrastructure/compose/dagster/docker-compose.yml
```

As the `dagster-omicidx-code-location` service. The production compose file here provides a standalone version for testing or alternative deployments.
