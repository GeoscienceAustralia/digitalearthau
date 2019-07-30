from pathlib import Path

import click

from datacube import Datacube
from datacube.ui import click as ui
from digitalearthau.vpmapper.worker import Dataset2Dataset, execute_with_dask, execute_task


@click.group()
def cli():
    pass


@cli.command()
@click.option('--environment',
              help='Name of the datacube environment to connect to.')
@click.option('--limit', type=int,
              help='For testing, specify a small number of tasks to run.')
@click.argument('config_file')
@ui.parsed_search_expressions
def run_many(config_file, expressions, environment=None, limit=None):
    # Load Configuration file
    d4 = Dataset2Dataset(config_file=config_file, dc_env=environment)

    tasks = d4.generate_tasks(expressions, limit=limit)

    execute_with_dask(tasks)


@cli.command()
@click.option('--environment')
@click.argument('config_file')
@click.argument('input_dataset')
def run_one(config_file, input_dataset, environment=None):
    d4 = Dataset2Dataset(config_file=config_file, dc_env=environment)

    input_uri = Path(input_dataset).as_uri()
    dc = Datacube(env=environment)
    ds = dc.index.datasets.get_datasets_for_location(input_uri)

    task = d4.generate_task(ds)
    execute_task(task)


if __name__ == '__main__':
    cli()
