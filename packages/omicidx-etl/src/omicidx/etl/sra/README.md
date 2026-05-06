# SRA Module

Extract and transform NCBI SRA (Sequence Read Archive) metadata into parquet format.

## Problem

The SRA archive publishes XML metadata dumps on an FTP mirror with periodic full and incremental updates. This module downloads, parses, and normalizes these dumps into parquet format for efficient querying and analysis.

## Output Structure

```
s3://bucket/sra/
├── study/
│   ├── date=2025-12-06/stage=Full/
│   │   ├── data_00000.parquet
│   │   ├── data_00001.parquet
│   │   └── ...
│   └── date=2025-12-05/stage=Incremental/
│       └── data_00000.parquet
├── sample/
├── experiment/
└── run/
```

Each entity (study, sample, experiment, run) is organized by date and stage (Full/Incremental), with parquet files chunked for memory efficiency.

## Modules

### `mirror.py` - Mirror entry parsing
- **`SRAMirrorEntry`** - Parses SRA mirror URLs to extract entity type, date, and whether it's a full or incremental dump
- **`get_sra_mirror_entries()`** - Fetches the latest mirror files and determines the current batch (most recent full + subsequent incrementals)

### `mirror_parquet.py` - Streaming parquet writer
- **`process_mirror_entry_to_parquet_parts()`** - Downloads, parses, and writes SRA XML to parquet in bounded-memory chunks
- Stages locally then uploads to avoid non-seekable remote FS issues
- Uses explicit PyArrow schemas for consistent field types

### `catalog.py` - Orchestration
- **`SRACatalog`** - Manages processing and cleanup
- Path layout and organization
- Progress tracking with structured logging

### `schema.py` - PyArrow schemas
- **`get_pyarrow_schemas()`** - Defines schemas for run, study, sample, experiment
- **`PYARROW_SCHEMAS`** - Module-level mapping for schema lookups

## Usage

```python
from omicidx_etl.extract_config import get_path_provider
from omicidx_etl.sra.mirror import get_sra_mirror_entries
from omicidx_etl.sra.catalog import SRACatalog

# Get mirror entries
entries = get_sra_mirror_entries()

# Create catalog
pp = get_path_provider("s3://omicidx/sra")
catalog = SRACatalog(pp)

# Process current batch
catalog.process(entries)

# Clean up old entries
catalog.cleanup(entries)
```

## Logging

Uses centralized `omicidx_etl.log` module:
- **Local dev**: Colorized human-readable logs
- **CI/Production**: Structured JSON logs for aggregation

Configure via environment:
```bash
OMICIDX_JSON_LOGS=1  # Force JSON logging
# or auto-detects in CI (GitHub Actions, GitLab CI, etc.)
```

## Configuration

Set output destination via `PathProvider`:

```python
# Local filesystem
pp = get_path_provider("/local/sra")

# S3
pp = get_path_provider("s3://bucket/sra")
```

## CLI (TODO)

Future: integrate into `oidx sra sync` command for easy orchestration.
