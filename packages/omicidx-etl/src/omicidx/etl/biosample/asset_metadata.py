from dataclasses import dataclass
from typing import Dict, List, Any, Optional
import hashlib
from datetime import datetime
from upath import UPath


@dataclass
class AssetMetadata:
    """Richer metadata for lineage and cataloging"""
    asset_key: str
    storage_path: str
    upstream_assets: List[str]
    
    # Data profile
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    columns: Optional[List[str]] = None
    partition: Optional[str] = None  # For date partitions
    
    # Storage metadata
    format: str = "parquet"
    compression: Optional[str] = None
    size_bytes: Optional[int] = None
    checksum: Optional[str] = None
    
    # Execution metadata
    created_at: Optional[datetime] = None
    runtime_seconds: Optional[float] = None
    
    def compute_checksum(self) -> str:
        """Compute checksum for data validation"""
        with UPath(self.storage_path).open('rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    
    def to_catalog_entry(self) -> dict:
        """For DuckDB catalog"""
        return {
            'asset_key': self.asset_key,
            'path': self.storage_path,
            'row_count': self.row_count,
            'checksum': self.checksum,
            'created_at': self.created_at,
            'upstream': self.upstream_assets
        }