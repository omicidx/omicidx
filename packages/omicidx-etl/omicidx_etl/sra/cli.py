"""
CLI commands for SRA module.

Provides commands to sync SRA mirror entries and manage the catalog.
"""
from typing import Optional
from datetime import date

import click

from omicidx_etl.log import get_logger

from .mirror import get_sra_mirror_entries, SRAMirrorEntry
from .catalog import SRACatalog


@click.group()
def sra():
    """SRA (Sequence Read Archive) metadata extraction and management."""
    pass


@sra.command()
@click.argument("output_base", required=False, default=None)
@click.option(
    "--since",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Only process entries on or after this date (YYYY-MM-DD)",
)
@click.option(
    "--until",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Only process entries on or before this date (YYYY-MM-DD)",
)
@click.option(
    "--entity",
    type=click.Choice(["study", "sample", "experiment", "run", "all"]),
    default="all",
    help="Which entity types to process (default: all)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be processed without writing",
)
@click.option(
    "--max-entries",
    type=int,
    default=None,
    help="Limit number of entries to process (useful for testing)",
)
@click.option(
    "--cleanup/--no-cleanup",
    default=True,
    help="Clean up old entries after processing (default: on)",
)
def extract(
    output_base: Optional[str],
    since: Optional[date],
    until: Optional[date],
    entity: str,
    dry_run: bool,
    max_entries: Optional[int],
    cleanup: bool,
):
    """
    Extract SRA metadata from NCBI mirror to parquet format.

    Downloads and processes the latest SRA mirror, filtering by date and entity type.
    """
    from omicidx_etl.config import settings
    from upath import UPath

    log = get_logger(__name__)
    base = UPath(output_base) if output_base else settings.publish_directory
    dest = str(base / "sra" / "raw")

    try:
        log.info(
            "Starting SRA sync",
            dest=dest,
            since=since,
            until=until,
            entity=entity,
            dry_run=dry_run,
        )
        
        # Fetch mirror entries
        log.info("Fetching SRA mirror entries")
        all_entries = get_sra_mirror_entries()
        
        # Filter by entity type
        if entity != "all":
            filtered_entries = [e for e in all_entries if e.entity == entity]
            log.info(f"Filtered to {len(filtered_entries)} {entity} entries")
        else:
            filtered_entries = all_entries
        
        # Filter by date range
        if since or until:
            since_date = since.date() if since else None
            until_date = until.date() if until else None
            before_filter = len(filtered_entries)

            if since_date and until_date:
                filtered_entries = [
                    e for e in filtered_entries
                    if since_date <= e.date <= until_date
                ]
            elif since_date:
                filtered_entries = [e for e in filtered_entries if e.date >= since_date]
            elif until_date:
                filtered_entries = [e for e in filtered_entries if e.date <= until_date]
            
            log.info(
                f"Filtered to {len(filtered_entries)} entries by date range",
                removed=before_filter - len(filtered_entries),
            )
        
        # Apply max entries limit
        if max_entries:
            filtered_entries = filtered_entries[:max_entries]
            log.info(f"Limited to {max_entries} entries")
        
        # Filter to current batch only
        current_batch = [e for e in filtered_entries if e.in_current_batch]
        log.info(f"Current batch has {len(current_batch)} entries to process")
        
        if not current_batch:
            log.warning("No entries to process")
            return
        
        if dry_run:
            log.info("DRY RUN: Would process the following entries:")
            for entry in current_batch:
                log.info(
                    f"  {entry.entity:10} {entry.date} {entry.url}",
                )
            return
        
        # Process entries
        log.info(f"Creating SRACatalog for destination {dest}")
        catalog = SRACatalog(dest)

        log.info("Processing entries")
        process_error = None
        try:
            catalog.process(filtered_entries)
        except RuntimeError as e:
            # Partial failure — some entities succeeded, some failed.
            # Continue to cleanup for the ones that succeeded.
            process_error = e
            log.warning("Processing had partial failures, continuing to cleanup", error=str(e))

        # Auto-cleanup: remove old data only for entities that completed successfully.
        # Pass all_entries (not filtered_entries) so the filesystem-based cleanup
        # knows which directories belong to the current batch across all entities.
        if cleanup:
            completed = catalog.get_completed_entities(all_entries)
            if completed:
                log.info("Running auto-cleanup for completed entities", entities=sorted(completed))
                catalog.cleanup(all_entries, completed_entities=completed)
            else:
                log.info("No entities completed successfully, skipping cleanup")
        else:
            log.info("Cleanup disabled via --no-cleanup")

        if process_error:
            raise process_error

        log.info("SRA sync completed successfully")

    except RuntimeError:
        # Already logged above — re-raise without double-logging
        raise
    except Exception as e:
        log.error("SRA sync failed", error=str(e), exc_info=True)
        raise


@sra.command()
@click.argument("output_base", required=False, default=None)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be cleaned up without deleting",
)
def cleanup(output_base: Optional[str], dry_run: bool):
    """
    Clean up old SRA mirror entries.

    Removes all entries that are no longer in the current batch.
    """
    from omicidx_etl.config import settings
    from upath import UPath

    log = get_logger(__name__)
    base = UPath(output_base) if output_base else settings.publish_directory
    dest = str(base / "sra" / "raw")

    try:
        log.info("Starting SRA cleanup", dest=dest, dry_run=dry_run)
        
        # Fetch mirror entries
        log.info("Fetching SRA mirror entries")
        all_entries = get_sra_mirror_entries()
        
        to_cleanup = [e for e in all_entries if not e.in_current_batch]
        log.info(f"Found {len(to_cleanup)} entries to clean up")
        
        if not to_cleanup:
            log.info("No entries to clean up")
            return
        
        if dry_run:
            log.info("DRY RUN: Would clean up the following entries:")
            for entry in to_cleanup:
                log.info(
                    f"  {entry.entity:10} {entry.date} {entry.url}",
                )
            return
        
        # Perform cleanup
        catalog = SRACatalog(dest)

        log.info("Cleaning up old entries")
        catalog.cleanup(all_entries)
        
        log.info("SRA cleanup completed successfully")
        
    except Exception as e:
        log.error("SRA cleanup failed", error=str(e), exc_info=True)
        raise


def list_entries_text(entries: list[SRAMirrorEntry]) -> str:
    """Helper function to format a list of entries as text."""
    lines = []
    for entry in entries:
        batch_raw = "CURRENT" if entry.in_current_batch else "old"
        stage_raw = "Full" if entry.is_full else "Incremental"
        entity = f"{entry.entity:<10}"
        date_str = f"{str(entry.date):<12}"
        stage = f"{stage_raw:<12}"
        batch = f"{batch_raw:<8}"
        line = f"{entity} {date_str} {stage} {batch}"
        lines.append(line)
    return "\n".join(lines)

def list_entries_json(entries: list[SRAMirrorEntry]) -> str:
    """Helper function to format a list of entries as JSON."""
    import json
    entries = [e.__dict__ for e in entries] # type: ignore
    for e in entries:
        e['date'] = str(e['date'])          # type: ignore
    return json.dumps(entries, indent=2)

@sra.command()
@click.option(
    "--json",
    is_flag=True,
    help="Output entries in JSON format",
)
def list_entries(json: bool):
    """
    List all SRA mirror entries.

    Displays the current and old entries in a tabular format.
    """
    entries = get_sra_mirror_entries()
    if json:
        click.echo(list_entries_json(entries))
    else:
        click.echo(list_entries_text(entries))


if __name__ == "__main__":
    sra()
