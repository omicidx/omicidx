# Biosample ETL Module

Simplified extraction tools for NCBI Biosample and Bioproject data.

## Quick Start

### CLI Usage

```bash
# Extract both biosample and bioproject
python -m omicidx_etl.cli biosample extract

# Extract to custom directory
python -m omicidx_etl.cli biosample extract --output-dir /custom/path

# Extract only biosample data
python -m omicidx_etl.cli biosample extract --entity biosample

# Extract and upload to R2
python -m omicidx_etl.cli biosample extract --upload

# Show file statistics
python -m omicidx_etl.cli biosample stats

# Upload existing files to R2
python -m omicidx_etl.cli biosample upload biosample

# Clean up files
python -m omicidx_etl.cli biosample clean
```

### Python API

```python
from pathlib import Path
from omicidx_etl.biosample.extract import extract_all, get_file_stats

# Extract all data
output_dir = Path("/data/omicidx/biosample")
results = extract_all(output_dir)

# Get statistics
stats = get_file_stats(output_dir)
print(f"Biosample files: {stats['biosample']['file_count']}")
```

## Configuration

### R2 Storage (Optional)

Set environment variables for R2 upload:

```bash
export R2_ACCESS_KEY="your-access-key"
export R2_SECRET_KEY="your-secret-key" 
export R2_ENDPOINT="https://your-account.r2.cloudflarestorage.com"
```

## Architecture

- **extract.py**: Core extraction functions (Prefect-free)
- **cli.py**: Command-line interface
- **etl.py**: Legacy Prefect-based code (deprecated)

## Performance

Optimized for high-memory systems:
- Biosample batch size: 2M records per file
- Bioproject batch size: 500K records per file
- Uses gzip compression for output files

## Migration from Legacy

The new approach removes:
- Prefect task decorators and flows
- BigQuery loading functionality  
- Complex temporary file handling

While maintaining:
- Same output format (NDJSON.gz files)
- Same parsing logic
- Compatible file naming scheme
