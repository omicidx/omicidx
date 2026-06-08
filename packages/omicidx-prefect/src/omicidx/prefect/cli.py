"""Command-line helpers for omicidx-prefect.

Most operations go through `prefect deployment run ...`. This CLI
covers semaphore inspection and ad-hoc local flow runs.
"""

import click
from omicidx.prefect.semaphore import SemaphoreStore


@click.group()
def cli() -> None:
    """omicidx-prefect operator CLI."""


@cli.group()
def semaphores() -> None:
    """Inspect and clear partition-completion semaphores."""


@semaphores.command("list")
@click.argument("namespace")
def list_semaphores(namespace: str) -> None:
    """List completed partition keys for NAMESPACE (e.g. sra/study, geo)."""
    store = SemaphoreStore(namespace)
    keys = store.list_keys()
    click.echo(f"{namespace}: {len(keys)} semaphores")
    for k in keys:
        click.echo(f"  {k}")


@semaphores.command("show")
@click.argument("namespace")
@click.argument("key")
def show_semaphore(namespace: str, key: str) -> None:
    """Print the semaphore JSON for NAMESPACE/KEY."""
    import json

    payload = SemaphoreStore(namespace).read(key)
    if payload is None:
        raise click.ClickException(f"No semaphore at {namespace}/{key}")
    click.echo(json.dumps(payload, indent=2))


@semaphores.command("clear")
@click.argument("namespace")
@click.argument("key", required=False)
@click.option("--all", "clear_all", is_flag=True, help="Clear the whole namespace")
def clear_semaphore(namespace: str, key: str | None, clear_all: bool) -> None:
    """Clear one semaphore or the whole namespace (with --all)."""
    store = SemaphoreStore(namespace)
    if clear_all:
        n = store.clear_all()
        click.echo(f"Cleared {n} semaphores in {namespace}")
        return
    if not key:
        raise click.UsageError("Pass KEY or --all")
    removed = store.clear(key)
    click.echo(f"{'Cleared' if removed else 'No semaphore at'} {namespace}/{key}")


@cli.group()
def run() -> None:
    """Run flows locally (no scheduler)."""


@run.command("sra")
@click.option("--force", is_flag=True)
def run_sra(force: bool) -> None:
    from omicidx.prefect.flows.sra import sra_extract_flow

    sra_extract_flow(force=force)


@run.command("geo")
@click.option("--start-month", default="2005-01")
@click.option("--end-month", default=None)
@click.option("--force", is_flag=True)
def run_geo(start_month: str, end_month: str | None, force: bool) -> None:
    from omicidx.prefect.flows.geo import geo_extract_flow

    geo_extract_flow(start_month=start_month, end_month=end_month, force=force)


@run.command("biosample")
@click.option("--force", is_flag=True)
def run_biosample(force: bool) -> None:
    from omicidx.prefect.flows.biosample import biosample_extract_flow

    biosample_extract_flow(force=force)


@run.command("bioproject")
@click.option("--force", is_flag=True)
def run_bioproject(force: bool) -> None:
    from omicidx.prefect.flows.biosample import bioproject_extract_flow

    bioproject_extract_flow(force=force)


@run.command("pubmed")
@click.option("--force", is_flag=True)
def run_pubmed(force: bool) -> None:
    from omicidx.prefect.flows.pubmed import pubmed_extract_flow

    pubmed_extract_flow(force=force)


@run.command("ebi-biosample")
@click.option("--start-day", default="2021-01-01")
@click.option("--end-day", default=None)
@click.option("--force", is_flag=True)
def run_ebi_biosample(start_day: str, end_day: str | None, force: bool) -> None:
    from omicidx.prefect.flows.ebi_biosample import ebi_biosample_extract_flow

    ebi_biosample_extract_flow(start_day=start_day, end_day=end_day, force=force)


@run.command("consolidate")
def run_consolidate() -> None:
    from omicidx.prefect.flows.consolidate import consolidate_flow

    consolidate_flow()


@run.command("ducklake-load")
@click.option("--lake-schema", default=None, help="Override target lake schema.")
def run_ducklake_load(lake_schema: str | None) -> None:
    from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
    from omicidx.prefect.flows.ducklake_load import ducklake_load_flow

    ducklake_load_flow(lake_schema=lake_schema or LAKE_SCHEMA)


@run.command("ducklake-maintenance")
def run_ducklake_maintenance() -> None:
    from omicidx.prefect.flows.ducklake_load import ducklake_maintenance_flow

    ducklake_maintenance_flow()


@run.command("parquet-export")
@click.option("--lake-schema", default=None, help="Override source lake schema.")
def run_parquet_export(lake_schema: str | None) -> None:
    from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
    from omicidx.prefect.flows.parquet_export import parquet_export_flow

    parquet_export_flow(lake_schema=lake_schema or LAKE_SCHEMA)


@run.command("postgres")
def run_postgres() -> None:
    from omicidx.prefect.flows.postgres import postgres_load_flow

    postgres_load_flow()


@run.command("duckdb")
def run_duckdb() -> None:
    from omicidx.prefect.flows.sql import omicidx_duckdb_flow

    omicidx_duckdb_flow()


@run.command("daily")
@click.option("--force", is_flag=True)
def run_daily(force: bool) -> None:
    from omicidx.prefect.flows.main import daily_pipeline_flow

    daily_pipeline_flow(force=force)


if __name__ == "__main__":
    cli()
