# omicidx-dagster

Dagster code location for OmicIDX ETL pipelines (SRA, GEO, BioSample, PubMed) + SQL consolidation layer.

## Docker Configurations

This package provides two Docker Compose configurations for different use cases:

### 1. Development Mode (`docker-compose.dev.yml`)

**When to use:**
- Local development and testing
- Iterating on asset definitions, schedules, or sensors
- Testing changes before deployment

**Features:**
- **Live code reload**: Changes to Python files automatically reload without restarting
- **Bind mounts**: Source code mounted from host (`src/omicidx/dagster/` and `src/omicidx/parsers/`)
- **Auto-reload**: Uses `dagster code-server start` which watches for file changes
- **Network integration**: Connects to existing Dagster infrastructure networks

**Setup:**

```bash
# 1. Copy and configure environment variables
cp .env.example .env
# Edit .env with your PostgreSQL and S3/R2 credentials

# 2. Build the image (only needed once or after dependency changes)
docker compose -f docker-compose.dev.yml build

# 3. Start the gRPC server with bind-mounted source code
docker compose -f docker-compose.dev.yml up -d

# 4. View logs
docker compose -f docker-compose.dev.yml logs -f

# 5. When done
docker compose -f docker-compose.dev.yml down
```

**How it works:**
- Container runs `dagster code-server start` which supports hot reload
- Source directories are bind-mounted, so edits on host → immediate reload
- Connects to `dagster_dagster` and `pg_and_duckdb_default` networks to integrate with existing Dagster daemon/webserver

**Connecting to existing Dagster deployment:**

Update your Dagster workspace.yaml to point to the dev container:

```yaml
load_from:
  - grpc_server:
      host: omicidx-dagster-dev  # container name on shared network
      port: 4000
      location_name: omicidx
```

Then restart your Dagster daemon and webserver to pick up the changes.

### 2. Production Mode (`docker-compose.yml`)

**When to use:**
- Standalone testing (no existing Dagster infrastructure)
- Alternative deployment environments
- CI/CD builds

**Features:**
- **Self-contained**: All code baked into the image
- **No bind mounts**: Immutable once built
- **Portable**: Can be deployed anywhere with Docker
- **Standalone**: Runs independently on port 4000

**Setup:**

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env with your S3/R2 credentials

# 2. Build and start
docker compose build
docker compose up -d

# 3. Container exposes gRPC server on localhost:4000
```

**When NOT to use:**
- If you need to make code changes (use dev mode instead)
- If you have an existing Dagster deployment (use dev mode or monode integration)

## Running Locally (without Docker)

**When to use:**
- Fastest iteration cycle (no container restart needed)
- Debugging with IDE breakpoints
- Running in Dagster's built-in dev UI

**Setup:**

From the workspace root (`omicidx/`):

```bash
# Install workspace
uv sync

# Start Dagster dev server (includes UI at localhost:3000)
dagster dev -m omicidx.dagster.definitions
```

This starts a complete Dagster instance with UI, daemon, and code server in a single process.

**Connecting to existing Dagster deployment:**

If you have a separate Dagster instance, point it to this code location via `workspace.yaml`:

```yaml
load_from:
  - python_module:
      module_name: omicidx.dagster.definitions
      location_name: omicidx
```

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

Assets are organized by **group** (primary navigation), **kinds** (technology badges), and **tags** (operational metadata).

#### Groups

- `biosample` - BioSample and BioProject extraction
- `geo` - GEO (Gene Expression Omnibus) extraction
- `pubmed` - PubMed literature extraction
- `sra` - SRA (Sequence Read Archive) extraction
- `sql` - SQL consolidation and DuckDB build

#### Kinds (Visual Badges)

Assets display icon badges based on technologies used:
- `python` - Python processing
- `duckdb` - DuckDB transformation
- `sql` - SQL queries
- `parquet` - Parquet output
- `json` - JSON/JSONL output
- `s3` - S3/R2 storage

#### Tags (Filtering/Metadata)

Assets are tagged with operational characteristics:

- **layer**: `raw` | `transformed` | `consolidated` | `published`
  - `raw`: Direct from source APIs (NCBI, GEO)
  - `transformed`: Cleaned/restructured
  - `consolidated`: Multi-source joins via SQL
  - `published`: Final outputs (DuckDB database)

- **cost**: `low` | `medium` | `high`
  - `low`: < 1 min runtime, < 1GB storage
  - `medium`: < 1 hour runtime, < 100GB storage
  - `high`: > 1 hour runtime or > 100GB storage

- **sla**: `daily` | `weekly` | `monthly` | `on-demand`
  - Expected update frequency

- **source**: `ncbi_ftp` | `ncbi_api` | `geo_ftp` | `pubmed_ftp` | `derived`
  - Data source type

- **storage**: `parquet` | `jsonl` | `duckdb` | `ndjson`
  - Output format

**Example usage in UI:**
- Filter all raw assets: `AssetSelection.tag("layer", "raw")`
- Find high-cost assets: `AssetSelection.tag("cost", "high")`
- Get all parquet outputs: `AssetSelection.tag("storage", "parquet")`

#### Raw Extraction Assets

- `biosample_raw`, `bioproject_raw` - BioSample/BioProject XML → JSONL.gz
- `sra_mirror_listing` - SRA FTP listing (determines current batch)
- `sra_raw` - SRA metadata → Parquet (partitioned by entity type)
- `geo_raw` - GEO SOFT files → NDJSON (monthly partitioned)
- `geo_rna_seq_counts` - GEO RNA-seq accession counts → Parquet
- `pubmed_raw` - PubMed baseline + daily updates → Parquet (dynamically partitioned)

#### Transform/SQL Assets

- `bioproject_parquet` - consolidates BioProject JSONL → Parquet via DuckDB
- `consolidated_parquet` - runs `010_raw_to_parquet.sql` (multi-source consolidation)
- `omicidx_duckdb` - builds final DuckDB database from `020-050_*.sql` view definitions

### Resources

- `OmicidxStorage` - S3/R2 storage abstraction (configured via env vars)
- `DuckDBResource` - DuckDB connection for SQL consolidation

### Schedules

- `daily_extract` - runs daily at 2 AM (BioSample, BioProject, SRA, GEO counts)
- `daily_geo` - partitioned GEO SOFT extraction (one partition per day)

### Sensors

- `pubmed_sensor` - detects new PubMed baseline/update files on NCBI FTP

## Environment Variables

See `.env.example` for a complete template. Required variables:

### PostgreSQL (required for run execution in Docker)

```bash
DAGSTER_POSTGRES_USER=dagster
DAGSTER_POSTGRES_PASSWORD=your-password
DAGSTER_POSTGRES_DB=dagster
DAGSTER_POSTGRES_HOSTNAME=pg_duckdb_18  # or your postgres host
DAGSTER_POSTGRES_PORT=5432
```

**Note**: Only required when running in Docker and connecting to an existing Dagster deployment. The container needs these to execute runs (write to Dagster's event log). Not needed for local `dagster dev`.

### S3/R2 Storage (required)

```bash
S3_ENDPOINT=https://your-r2-endpoint.r2.cloudflarestorage.com
S3_ACCESS_KEY_ID=your-access-key
S3_SECRET_ACCESS_KEY=your-secret-key
S3_REGION=auto  # for Cloudflare R2
S3_URL_STYLE=path
```

### OmicIDX Configuration

```bash
PUBLISH_ROOT=s3://omicidx  # production
# or
PUBLISH_ROOT=/tmp/omicidx-dev  # development (local filesystem)
```

### Optional

```bash
DAGSTER_SENSOR_GRPC_TIMEOUT=300  # PubMed sensor timeout (default 300s)
DAGSTER_LOG_LEVEL=INFO  # DEBUG for verbose logging
```

## Integration with monode Infrastructure

For deployment to the monode Dagster infrastructure:

**Production deployment:**

The production service is defined in `monode/infrastructure/compose/dagster/docker-compose.yml` as `dagster-omicidx-code-location`. It builds from this package and connects to the shared Dagster infrastructure.

**Development workflow:**

1. Start the dev container in this repo:
   ```bash
   cd packages/omicidx-dagster
   docker compose -f docker-compose.dev.yml up -d
   ```

2. Stop the production container in monode:
   ```bash
   cd monode/infrastructure/compose/dagster
   docker compose stop dagster-omicidx-code-location
   ```

3. Update monode's `workspace.yaml` to point to dev container:
   ```yaml
   - grpc_server:
       host: omicidx-dagster-dev
       port: 4000
       location_name: omicidx
   ```

4. Restart Dagster daemon/webserver:
   ```bash
   docker compose restart dagster-daemon dagster-webserver
   ```

5. When done, revert workspace.yaml and restart the production container.

## Troubleshooting

### "Auto-reload not working in dev mode"

**Symptom**: Code changes don't appear in Dagster UI
**Cause**: Using `dagster api grpc` instead of `dagster code-server start`
**Fix**: Ensure `docker-compose.dev.yml` uses:
```yaml
command:
  - dagster
  - code-server
  - start
  - -h
  - "0.0.0.0"
  - -p
  - "4000"
  - -m
  - omicidx.dagster.definitions
```

### "Could not reach user code server" or "UNAVAILABLE"

**Symptom**: Dagster daemon can't connect to code location
**Cause**: Container not on the same Docker network
**Fix**: Ensure dev container is on `dagster_dagster` network:
```yaml
networks:
  - dagster_dagster
  - pg_and_duckdb_default
```

And that these networks exist:
```bash
docker network ls | grep dagster
```

### "You have attempted to fetch the environment variable 'DAGSTER_POSTGRES_*' which is not set"

**Symptom**: Run fails with missing PostgreSQL config
**Cause**: Missing environment variables in `.env`
**Fix**: Copy `.env.example` to `.env` and fill in PostgreSQL credentials. These must match your Dagster deployment's PostgreSQL instance.

### "TypeError: multi_asset got an unexpected keyword argument 'tags'"

**Symptom**: Container crashes on startup
**Cause**: Tags applied at `@multi_asset` decorator level
**Fix**: Move tags to individual `AssetOut` objects:
```python
@multi_asset(
    outs={
        "asset_name": AssetOut(
            group_name="group",
            kinds={"python"},
            tags={"layer": "raw"},  # tags here, not on decorator
        ),
    }
)
```

### "Container exits immediately" or "Exited (1)"

**Symptom**: `docker ps -a` shows container exited
**Cause**: Python error during module import
**Fix**: Check logs for the actual error:
```bash
docker logs omicidx-dagster-dev
```

Common causes:
- Syntax error in Python code
- Missing dependency (rebuild image)
- Invalid `.env` configuration

### "Changes to parsers not reloading"

**Symptom**: Edits to `omicidx-parsers` don't appear
**Fix**: Ensure both bind mounts are present in `docker-compose.dev.yml`:
```yaml
volumes:
  - ./src/omicidx/dagster:/opt/dagster/workspace/packages/omicidx-dagster/src/omicidx/dagster
  - ../omicidx-parsers/src/omicidx/parsers:/opt/dagster/workspace/packages/omicidx-parsers/src/omicidx/parsers
```

## Performance Notes

- **Dev container startup**: ~5-10s (no rebuild needed)
- **Production build**: ~2-3 min (installs all dependencies)
- **Auto-reload latency**: ~2-5s after file save
- **Memory usage**: ~500MB idle, ~2-4GB during large materializations (SRA, GEO)
