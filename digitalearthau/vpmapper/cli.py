from pathlib import Path

import click

from datacube import Datacube
from digitalearthau.vpmapper.worker import D4


@click.group()
def cli():
    pass


@cli()
@click.argument('config_file')
def run_many(config_file):
    # Load Configuration file
    d4 = D4(config_file=config_file)

    tasks = d4.generate_tasks()

    d4.execute_with_dask(tasks)


@cli()
@click.argument('config_file')
@click.argument('input_dataset')
def run_one(config_file, input_dataset)
    d4 = D4(config_file=config_file)

    input_uri = Path(input_dataset).as_uri()
    dc = Datacube()
    ds = dc.index.datasets.get_datasets_for_location(input_uri)

    task = d4.generate_task(ds)
    d4.execute_task(task)


if __name__ == '__main__':
    cli()
