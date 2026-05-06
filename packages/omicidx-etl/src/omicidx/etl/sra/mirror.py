"""
SRA mirror entry management.

This module provides classes and functions for working with NCBI SRA mirror files,
including parsing mirror URLs and determining which files to process.
"""
import datetime
import re
from typing import List

from upath import UPath
from loguru import logger


class SRAMirrorEntry:
    """
    Represents an entry in the SRA mirror file list.
    
    An entry will have a URL like:
    https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/NCBI_SRA_Mirroring_20251206_Full/meta_study_set.xml.gz
    
    From the URL, we can extract:
    - SRA entity (study, sample, experiment, run)
    - Full or incremental file
    - Date of the file
    """
    
    def __init__(self, url: str):
        """
        Initialize a mirror entry from a URL.
        
        Args:
            url: The mirror file URL
            
        Raises:
            ValueError: If the URL doesn't match expected patterns
        """
        self.url = url
        self.setup()
        
    def setup(self) -> None:
        """Set up the mirror entry by extracting relevant information."""
        self._extract_sra_entity()
        self._is_full_file()
        self._extract_date()
        self._in_current_batch()
    
    def __repr__(self):
        return (
            f"SRAMirrorEntry(url={self.url}, entity={self.entity}, "
            f"is_full={self.is_full}, date={self.date}, "
            f"in_current_batch={self.in_current_batch})"
        )
    
    def _extract_sra_entity(self) -> None:
        """Extract the SRA entity type from the URL."""
        if 'study' in self.url:
            self.entity = 'study'
        elif 'sample' in self.url:
            self.entity = 'sample'
        elif 'experiment' in self.url:
            self.entity = 'experiment'
        elif 'run' in self.url:
            self.entity = 'run'
        else:
            raise ValueError(f"Unknown SRA entity in URL: {self.url}")
    
    def _is_full_file(self) -> None:
        """Determine if this is a full or incremental file."""
        self.is_full = 'Full' in self.url
    
    def _extract_date(self) -> None:
        """
        Extract the date from the URL.
        
        Expected format: NCBI_SRA_Mirroring_YYYYMMDD_Full
        
        Raises:
            ValueError: If date pattern is not found in URL
        """
        match = re.search(r'NCBI_SRA_Mirroring_(\d{8})', self.url)
        if not match:
            raise ValueError(f"Could not extract date from URL: {self.url}")
        
        date_str = match.group(1)  # e.g., '20251206'
        self.date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
    
    def _in_current_batch(self) -> None:
        """Initialize the current batch flag (set by get_sra_mirror_entries)."""
        self.in_current_batch = False


def get_sra_mirror_entries() -> List[SRAMirrorEntry]:
    """
    Fetch the SRA mirror entries from the SRA mirror file URLs.
    
    The strategy is to get the latest full file and then all incremental files
    that follow it. Files are processed in reverse chronological order, and once
    we find the first "Full" file, we include it and all subsequent files until
    we encounter another "Full" file (which marks the start of the previous batch).
    
    Returns:
        List of SRAMirrorEntry objects with in_current_batch flag set appropriately
    """
    logger.info("Fetching SRA mirror entries")
    
    up = UPath("https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/")
    all_files = list(reversed([str(f) for f in up.glob("**/*set.xml.gz")]))
    
    logger.info(f"Found {len(all_files)} total mirror files")
    
    found_full = False
    out_of_full = False
    entries = []
    
    for url in all_files:
        try:
            sra_mirror_entry = SRAMirrorEntry(url)
        except ValueError as e:
            logger.debug(f"Skipping URL due to parse error: {e}")
            continue
        
        # Mark the first Full file we encounter
        if "Full" in url and not found_full:
            found_full = True
        
        # After the first Full, if we hit another Full, we're out of the current batch
        if found_full and "Full" not in url:
            out_of_full = True
        
        if out_of_full:
            sra_mirror_entry.in_current_batch = False
            entries.append(sra_mirror_entry)
            continue
        
        sra_mirror_entry.in_current_batch = True
        entries.append(sra_mirror_entry)
    
    current_batch = [e for e in entries if e.in_current_batch]
    logger.info(
        f"Processed {len(entries)} valid entries, "
        f"{len(current_batch)} in current batch"
    )
    
    return entries
