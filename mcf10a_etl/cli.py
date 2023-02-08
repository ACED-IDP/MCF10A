import logging

import click

from mcf10a_etl.extract import extract
from mcf10a_etl.transform import transform

logging.basicConfig(format='%(asctime)s %(message)s',  encoding='utf-8', level=logging.INFO)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """Extract, Transform MCF10A data."""
    pass


cli.add_command(extract)
cli.add_command(transform)


if __name__ == '__main__':
    cli()
