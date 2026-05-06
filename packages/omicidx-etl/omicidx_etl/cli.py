"""
Main CLI entry point for omicidx-etl.
"""

import click
from dotenv import load_dotenv

# Load .env file early to ensure AWS credentials are available
load_dotenv()

from omicidx_etl.biosample.extract import biosample
from omicidx_etl.etl.europepmc_textmined import europepmc
from omicidx_etl.etl.icite import icite
from omicidx_etl.etl.pubmed import pubmed
from omicidx_etl.geo.extract import geo
from omicidx_etl.nih_reporter import nih_reporter
from omicidx_etl.sra.cli import sra
from omicidx_etl.sql.runner import sql
from omicidx_etl.build_db import build_db


@click.group()
@click.version_option()
def cli():
    """OmicIDX ETL Pipeline - Simplified data extraction tools."""
    pass


# Add subcommands
cli.add_command(biosample)
cli.add_command(europepmc)
cli.add_command(icite)
cli.add_command(pubmed)
cli.add_command(geo)
cli.add_command(nih_reporter)
cli.add_command(sra)
cli.add_command(sql)
cli.add_command(build_db)

if __name__ == "__main__":
    cli()
