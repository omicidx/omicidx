import hashlib
from dataclasses import dataclass
from datetime import datetime

from upath import UPath


@dataclass
class AssetMetadata:
    """Richer metadata for lineage and cataloging"""

    asset_key: str
    storage_path: str
    upstream_assets: list[str]

    # Data profile
    row_count: int | None = None
    column_count: int | None = None
    columns: list[str] | None = None
    partition: str | None = None  # For date partitions

    # Storage metadata
    format: str = "parquet"
    compression: str | None = None
    size_bytes: int | None = None
    checksum: str | None = None

    # Execution metadata
    created_at: datetime | None = None
    runtime_seconds: float | None = None

    def compute_checksum(self) -> str:
        """Compute checksum for data validation"""
        with UPath(self.storage_path).open("rb") as f:
            return hashlib.md5(f.read()).hexdigest()

    def to_catalog_entry(self) -> dict:
        """For DuckDB catalog"""
        return {
            "asset_key": self.asset_key,
            "path": self.storage_path,
            "row_count": self.row_count,
            "checksum": self.checksum,
            "created_at": self.created_at,
            "upstream": self.upstream_assets,
        }
