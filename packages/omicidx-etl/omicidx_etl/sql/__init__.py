"""SQL files for DuckDB ETL transformations and view definitions.

Two groups of SQL files:
  - 010_*: Consolidation (raw -> parquet on R2, run via `oidx sql run`)
  - 020_*-050_*: View definitions (parquet -> DuckDB views, run via `oidx build-db`)

Usage:
    from omicidx_etl.sql import get_sql, list_sql_files, SQL_DIR

    # Get SQL content by filename
    sql = get_sql("010_raw_to_parquet.sql")

    # List available SQL files
    files = list_sql_files()
"""

from pathlib import Path

SQL_DIR = Path(__file__).parent


def get_sql(name: str) -> str:
    """Load SQL file content by name.

    Args:
        name: SQL filename (e.g., "010_raw_to_parquet.sql")

    Returns:
        SQL file contents as string

    Raises:
        FileNotFoundError: If SQL file doesn't exist
    """
    path = SQL_DIR / name
    if not path.exists():
        available = ", ".join(list_sql_files())
        raise FileNotFoundError(
            f"SQL file '{name}' not found. Available: {available}"
        )
    return path.read_text()


def list_sql_files() -> list[str]:
    """List available SQL files in order."""
    return sorted(p.name for p in SQL_DIR.glob("*.sql"))
