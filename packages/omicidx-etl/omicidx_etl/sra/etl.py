"""
ETL entry point for SRA extraction pipeline.

This module provides the main entry point for GitHub Actions workflows.
It simply invokes the CLI's extract command with default settings.

Usage:
    python -m omicidx_etl.sra.etl

Environment Variables:
    OMICIDX_SRA_DEST: Output destination (required, e.g., s3://omicidx/sra/raw)

The extract command will:
1. Fetch the latest SRA mirror entries
2. Process only the current batch (latest Full + subsequent Incrementals)
3. Write to parquet format with proper partitioning
"""
import sys
from .cli import sra

if __name__ == "__main__":
    # Invoke the extract command with default arguments
    # All configuration comes from environment variables
    sys.exit(sra(["extract"]))
