"""
SRA catalog management.

This module provides the SRACatalog class for managing the processing and cleanup
of SRA mirror entries.
"""
from typing import List
from datetime import datetime
import json
import re

from upath import UPath

from ..log import get_logger, log_operation, LogProgress
from .mirror import SRAMirrorEntry
from .mirror_parquet import process_mirror_entry_to_parquet_parts

ENTITIES = ("study", "sample", "experiment", "run")


class SRACatalog:
    """
    Manages the SRA catalog: processing mirror entries and cleaning up old data.

    The catalog organizes data in a directory structure like:
        {base_dir}/{entity}/date={YYYY-MM-DD}/stage={Full|Incremental}/data_*.parquet
    """

    def __init__(self, base_dir: UPath | str):
        """
        Initialize the SRA catalog.

        Args:
            base_dir: Base directory for SRA data (e.g., 's3://omicidx/sra/raw')
        """
        self.base_dir = UPath(base_dir)
        self.log = get_logger(__name__)

    def _done_marker_path(self, mirror_entry: SRAMirrorEntry) -> UPath:
        """Return the marker path indicating this entry is processed."""
        return self.parquet_dir_for_mirror_entry(mirror_entry) / "data.done"
    
    def path_for_mirror_entry(self, mirror_entry: SRAMirrorEntry) -> UPath:
        """
        Return the legacy path where a single NDJSON file would be stored.

        This is kept for cleanup of old data but not used for new writes.

        Args:
            mirror_entry: The SRA mirror entry

        Returns:
            Path to the legacy NDJSON file
        """
        return (
            self.base_dir
            / mirror_entry.entity
            / f"date={mirror_entry.date.strftime('%Y-%m-%d')}"
            / f"stage={'Full' if mirror_entry.is_full else 'Incremental'}"
            / "data_0.ndjson.gz"
        )

    def parquet_dir_for_mirror_entry(self, mirror_entry: SRAMirrorEntry) -> UPath:
        """
        Return the directory path where parquet parts should be stored.

        Args:
            mirror_entry: The SRA mirror entry

        Returns:
            Path to the parquet directory for this entry
        """
        return (
            self.base_dir
            / mirror_entry.entity
            / f"date={mirror_entry.date.strftime('%Y-%m-%d')}"
            / f"stage={'Full' if mirror_entry.is_full else 'Incremental'}"
        )
    
    def _rm_tree(self, p: UPath) -> None:
        """
        Remove a directory/prefix recursively.
        
        Works for both local filesystems and fsspec-backed remotes like S3.
        
        Args:
            p: Path to remove recursively
        """
        if not p.exists():
            return
        
        try:
            p.fs.rm(p.path, recursive=True)
        except TypeError:
            # Some FS implementations don't accept recursive as kwarg
            p.fs.rm(p.path, True)
    
    def cleanup_one(self, mirror_entry: SRAMirrorEntry) -> None:
        """
        Remove all stored artifacts for a mirror entry (entire directory/prefix).
        
        Args:
            mirror_entry: The SRA mirror entry to clean up
        """
        log = self.log.bind(
            entity=mirror_entry.entity,
            date=str(mirror_entry.date),
            stage="Full" if mirror_entry.is_full else "Incremental",
        )
        
        out_dir = self.parquet_dir_for_mirror_entry(mirror_entry)
        
        with log_operation(log, "cleanup", url=mirror_entry.url):
            self._rm_tree(out_dir)
            
            # Optional: remove legacy single-file landing path
            legacy = self.path_for_mirror_entry(mirror_entry)
            try:
                legacy.unlink(missing_ok=True)
            except Exception:
                pass
    
    def get_completed_entities(self, mirror_entries: List[SRAMirrorEntry]) -> set[str]:
        """Return entity names whose current-batch Full entry has a done marker."""
        completed = set()
        for entry in mirror_entries:
            if entry.in_current_batch and entry.is_full:
                if self._done_marker_path(entry).exists():
                    completed.add(entry.entity)
        return completed

    def _current_batch_dirs(self, mirror_entries: List[SRAMirrorEntry], entity: str) -> set[str]:
        """Return the set of directory names (date=.../stage=...) that belong to the current batch for an entity."""
        dirs: set[str] = set()
        for e in mirror_entries:
            if e.in_current_batch and e.entity == entity:
                d = self.parquet_dir_for_mirror_entry(e)
                dirs.add(str(d))
        return dirs

    def cleanup_entity_by_filesystem(
        self,
        entity: str,
        keep_dirs: set[str],
    ) -> int:
        """
        Scan the filesystem/S3 prefix for an entity and remove any date
        partitions not in keep_dirs.

        This catches residual directories that the mirror-entry-based cleanup
        misses (e.g., old entries NCBI no longer lists).

        Args:
            entity: The SRA entity type (study, sample, experiment, run)
            keep_dirs: Set of full directory paths to keep

        Returns:
            Number of directories removed
        """
        entity_dir = self.base_dir / entity
        removed = 0

        if not entity_dir.exists():
            return removed

        # List date= partitions under the entity directory
        try:
            date_dirs = [
                p for p in entity_dir.iterdir()
                if re.match(r"date=\d{4}-\d{2}-\d{2}$", p.name)
            ]
        except (FileNotFoundError, OSError):
            return removed

        for date_dir in date_dirs:
            # List stage= partitions under each date
            try:
                stage_dirs = [
                    p for p in date_dir.iterdir()
                    if p.name.startswith("stage=")
                ]
            except (FileNotFoundError, OSError):
                continue

            for stage_dir in stage_dirs:
                if str(stage_dir) not in keep_dirs:
                    self.log.info(
                        "Removing residual directory",
                        entity=entity,
                        path=str(stage_dir),
                    )
                    try:
                        self._rm_tree(stage_dir)
                        removed += 1
                    except Exception as e:
                        self.log.error(
                            "Failed to remove residual directory",
                            path=str(stage_dir),
                            error=str(e),
                        )

            # Clean up empty date partition if no stage dirs remain
            try:
                remaining = list(date_dir.iterdir())
                if not remaining:
                    self._rm_tree(date_dir)
            except (FileNotFoundError, OSError):
                pass

        return removed

    def cleanup(self, mirror_entries: List[SRAMirrorEntry], completed_entities: set[str] | None = None) -> None:
        """
        Clean up the catalog by removing old files.

        Two-pass approach:
        1. Remove entries from the mirror listing that are not in the current batch.
        2. Scan the filesystem for any residual directories not in the current batch
           (catches files that NCBI no longer lists or that were left by earlier runs).

        If completed_entities is provided, only cleans up entities in that set.

        Args:
            mirror_entries: List of all mirror entries
            completed_entities: If provided, only clean up entities in this set
        """
        entities_to_clean = completed_entities or set(ENTITIES)

        # Pass 1: mirror-entry-based cleanup (existing behavior)
        to_cleanup = [
            e for e in mirror_entries
            if not e.in_current_batch and e.entity in entities_to_clean
        ]

        self.log.info(
            "Starting cleanup",
            total_entries=len(mirror_entries),
            mirror_based_cleanup=len(to_cleanup),
            entities=sorted(entities_to_clean),
        )

        progress = LogProgress(
            self.log,
            total=len(to_cleanup),
            operation="cleanup_entries",
            log_every=10,
        )

        for entry in to_cleanup:
            try:
                self.cleanup_one(entry)
                progress.update()
            except Exception as e:
                self.log.error(
                    "Failed to cleanup entry",
                    url=entry.url,
                    entity=entry.entity,
                    error=str(e),
                    exc_info=True,
                )

        progress.complete()

        # Pass 2: filesystem-based cleanup for residual directories
        total_residual = 0
        for entity in sorted(entities_to_clean):
            keep = self._current_batch_dirs(mirror_entries, entity)
            removed = self.cleanup_entity_by_filesystem(entity, keep)
            total_residual += removed

        if total_residual:
            self.log.info(
                "Removed residual directories not in current batch",
                residual_removed=total_residual,
            )
    
    def process_one(self, mirror_entry: SRAMirrorEntry) -> None:
        """
        Process a single mirror entry and write parquet parts.
        
        Args:
            mirror_entry: The SRA mirror entry to process
        """
        log = self.log.bind(
            entity=mirror_entry.entity,
            date=str(mirror_entry.date),
            stage="Full" if mirror_entry.is_full else "Incremental",
        )
        
        out_dir = self.parquet_dir_for_mirror_entry(mirror_entry)
        done_marker = self._done_marker_path(mirror_entry)

        if done_marker.exists():
            log.info(
                f"Skipping entry; done marker exists {done_marker}, {mirror_entry.url}, {out_dir}"
            )
            return
        
        with log_operation(log, "process_entry", url=mirror_entry.url):
            written_parts = process_mirror_entry_to_parquet_parts(
                url=mirror_entry.url,
                out_dir=out_dir,
                entity=mirror_entry.entity,
                # files will be named data_000000.parquet, data_000001.parquet, etc.
                basename = "data"
            )

            # Create completion marker with a small amount of metadata
            out_dir.mkdir(parents=True, exist_ok=True)
            marker_payload = {
                "completed_at": datetime.utcnow().isoformat() + "Z",
                "url": mirror_entry.url,
                "entity": mirror_entry.entity,
                "date": str(mirror_entry.date),
                "is_full": mirror_entry.is_full,
                "parts_written": len(written_parts),
            }
            with done_marker.open("w") as fp:
                json.dump(marker_payload, fp)
            log.info("Wrote done marker", marker=str(done_marker))
    
    def process(self, mirror_entries: List[SRAMirrorEntry]) -> None:
        """
        Process the SRA mirror entries and store them in the catalog.
        
        Only processes entries that are in the current batch.
        
        Args:
            mirror_entries: List of all mirror entries
        """
        current_batch = [e for e in mirror_entries if e.in_current_batch]
        
        self.log.info(
            "Starting batch processing",
            total_entries=len(mirror_entries),
            current_batch=len(current_batch),
        )
        
        progress = LogProgress(
            self.log,
            total=len(current_batch),
            operation="process_mirror_entries",
            log_every=1,  # Log every entry since there are typically few
        )
        
        failures = []
        for entry in current_batch:
            try:
                self.process_one(entry)
                progress.update()
            except Exception as e:
                failures.append(entry.entity)
                self.log.error(
                    "Failed to process entry — continuing with remaining entities",
                    url=entry.url,
                    entity=entry.entity,
                    error=str(e),
                    exc_info=True,
                )

        progress.complete()

        if failures:
            self.log.error(
                "Batch completed with failures",
                failed_entities=failures,
                total=len(current_batch),
            )
            raise RuntimeError(
                f"Failed to process {len(failures)} entries: {', '.join(failures)}"
            )
