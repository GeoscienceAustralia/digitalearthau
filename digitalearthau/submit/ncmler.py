#!/usr/bin/env python

from __future__ import print_function

import re
import subprocess
from pathlib import Path
from typing import Iterable

import click
import yaml

import digitalearthau
from digitalearthau import INGEST_CONFIG_DIR

DISTRIBUTED_SCRIPT_PATH = digitalearthau.SCRIPT_DIR / 'run_distributed.sh'


def cell_list_to_file(filename, cell_list):
    with open('cell_index.txt', 'w') as cell_file:
        for cell in cell_list:
            cell_file.write('{0},{1}\n'.format(*cell))


@click.group()
def cli():
    pass


@cli.command('list')
def list_products():
    """List available products
    """
    for cfg in INGEST_CONFIG_DIR.glob('*.yaml'):
        print(cfg.name)


@cli.command(help='Submit a job to create the full stack of ncml')
@click.option('--queue', '-q', default='normal',
              type=click.Choice(['normal', 'express']))
@click.option('--project', '-P', default='v10')
@click.option('--walltime', '-t', default=10,
              help='Number of hours to request',
              type=click.IntRange(1, 10))
@click.option('--data-subfolder-count', default=1)
@click.option('--name', help='Job name to use')
@click.argument('product_name')
def full(product_name, queue, project, walltime, name, data_subfolder_count):
    """Submit a job to create the full stack of ncml

    ncmler full ls5_nabr_albers.yaml
    """
    qsub_ncml('full', product_name, queue, project, walltime, name, data_subfolder_count, years=[])


@cli.command(help='Submit a job to create a nested ncml stack')
@click.option('--queue', '-q', default='normal',
              type=click.Choice(['normal', 'express']))
@click.option('--project', '-P', default='v10')
@click.option('--walltime', '-t', default=10,
              help='Number of hours to request',
              type=click.IntRange(1, 10))
@click.option('--data-subfolder-count', default=1)
@click.option('--name', help='Job name to use')
@click.argument('product_name')
@click.argument('nested_years', nargs=-1, type=click.INT)
def nest(product_name, queue, project, walltime, name, data_subfolder_count, nested_years):
    """Submit a job to create a stack of ncml with nested years

    NCML file
     - ls8_nbar_albers_2003.nc
     - ls8_nbar_albers_2004.nc
     - ...
     - ls8_nbar_albers_2015.nc
     - ls8_nbar_albers_2016.ncml
     - ls8_nbar_albers_2017.ncml

    ncmler nest ls8_nbar_albers.yaml 2016 2017
    """
    qsub_ncml('nest', product_name, queue, project, walltime, name, data_subfolder_count, years=nested_years)


@cli.command(help='Submit a job to update a single nested year')
@click.option('--queue', '-q', default='normal',
              type=click.Choice(['normal', 'express']))
@click.option('--project', '-P', default='v10')
@click.option('--walltime', '-t', default=10,
              help='Number of hours to request',
              type=click.IntRange(1, 10))
@click.option('--data-subfolder-count', default=1)
@click.option('--name', help='Job name to use')
@click.argument('product_name')
@click.argument('year', type=click.INT)
def update(product_name, year, queue, project, walltime, name, data_subfolder_count):
    """Submit a job to update a single nested year

    ncmler update ls5_nbar_albers.yaml 2016
    """
    qsub_ncml('update', product_name, queue, project, walltime, name, data_subfolder_count, years=[year])


def qsub_ncml(command: str,
              product_name: str,
              queue: str,
              project: str,
              walltime: int,
              name: str,
              data_subfolder_count: int,
              years: Iterable[int]):
    config_path = INGEST_CONFIG_DIR / '{}.yaml'.format(product_name)

    if not config_path.exists():
        raise click.BadParameter("No config found for product_name {!r}".format(product_name))

    cell_index_file = Path(product_name + '_ncml_cells.txt').absolute()

    subprocess.check_call('datacube -v system check', shell=True)

    config_dict = yaml.load(config_path.open())
    sample_path = Path(config_dict['location']) / config_dict['file_path_template']
    cull_parts = 1 + data_subfolder_count
    cells_folder = Path().joinpath(*sample_path.parts[:-cull_parts])
    cell_list = cell_list_from_path(cells_folder)
    cell_list_to_file(cell_index_file, cell_list)

    name = name or 'ncml_full_' + product_name
    args = [
        'qsub',
        '-q', queue,
        '-N', name,
        '-P', project,
        '-l',
        'ncpus={ncpus},mem={mem}gb,walltime={walltime}:00:00'.format(
            ncpus=2,
            mem=4,
            walltime=walltime
        ),
        '--',
        '/bin/bash',
        str(DISTRIBUTED_SCRIPT_PATH),
        digitalearthau.MODULE_NAME,
        '--ppn', '1',
        'datacube-ncml', '-v', '--executor', 'distributed', 'DSCHEDULER',
        command,
        ' '.join([str(year) for year in years])
    ]
    if click.confirm('\n%s\n\nRUN?' % (args,), default=True):
        subprocess.check_call(args, shell=True)


def _cell_from_filename(name):
    """
    >>> _cell_from_filename('-17_-23')
    (-17, -23)
    >>> _cell_from_filename('2_-12')
    (2, -12)
    >>> _cell_from_filename('-13_3')
    (-13, 3)
    >>> _cell_from_filename('-3_12')
    (-3, 12)
    >>> _cell_from_filename('3 -4')
    (3, -4)
    >>> _cell_from_filename('requirements.txt')
    >>> _cell_from_filename('a_b')
    """
    cell_matcher = re.compile(r'(-?\d+)\s*[,_ ]\s*(-?\d+)')

    match = cell_matcher.match(name)
    if not match:
        return None

    return tuple(int(i) for i in match.groups())


def cell_list_from_path(path):
    """
    Example expected input path: /g/data/rs0/datacube/002//g/data/rs0/datacube/002
    """
    base_path = Path(path)
    for folder_path in base_path.iterdir():
        cell = _cell_from_filename(folder_path.name)
        if cell:
            yield cell


if __name__ == '__main__':
    cli()
