from .config import settings
import duckdb
import contextlib
from pathlib import Path
from typing import Optional
import tempfile


def duckdb_setup_sql(temp_directory: Optional[str] = None):
    """
    Generate DuckDB setup SQL with optional custom temp directory.

    Args:
        temp_directory: Custom temp directory path. If None, uses /tmp/db_temp
    """
    if temp_directory is None:
        temp_directory = '/tmp/db_temp'

    endpoint = 's3.amazonaws.com'
    if settings.AWS_ENDPOINT_URL is not None:
        endpoint = settings.AWS_ENDPOINT_URL.replace('https://', '').replace('http://', '')

    return f"""
    INSTALL httpfs;
    LOAD httpfs;
    SET memory_limit='16GB';
    SET preserve_insertion_order=false;
    SET temp_directory='{temp_directory}';
    SET max_temp_directory_size='100GB';
    CREATE SECRET minio (
        TYPE S3,
        KEY_ID '{settings.AWS_ACCESS_KEY_ID}',
        SECRET '{settings.AWS_SECRET_ACCESS_KEY}',
        ENDPOINT '{endpoint}',
        URL_STYLE '{settings.AWS_URL_STYLE or "path"}',
        USE_SSL 'true'
    );
    """

@contextlib.contextmanager
def duckdb_connection(temp_directory: Optional[str] = None):
    """
    Create a DuckDB connection with optional custom temp directory.

    Args:
        temp_directory: Custom temp directory path. Defaults to /tmp/db_temp.
                       For large operations, use a directory on a filesystem
                       with sufficient space.

    Example:
        # Use custom temp directory
        with duckdb_connection(temp_directory='/data/tmp') as con:
            con.execute("SELECT * FROM large_table")
    """
    with duckdb.connect() as con, tempfile.TemporaryDirectory() as temp_dir:
        if temp_directory is None:
            temp_directory = temp_dir
        else:
            Path(temp_directory).mkdir(parents=True, exist_ok=True)
        sql = duckdb_setup_sql(temp_directory)
        con.execute(sql)
        yield con
        
        
if __name__ == "__main__":
    with duckdb_connection() as con:
        # Example usage of the connection
        result = con.execute("select * from read_parquet('s3://omicidx/sra/raw/study/**/*parquet') limit 10").df()
        print(result)
        
        con.execute("copy (select 1 as abc) to 's3://omicidx/test.parquet' (format parquet);")