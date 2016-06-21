#!/usr/bin/env python

from __future__ import print_function

import os
import click
from pathlib import Path


ROOT_DIR = Path(__file__).parent.parent
CONFIG_DIR = ROOT_DIR/'config'
SCRIPT_DIR = ROOT_DIR/'script'


@click.group()
def cli():
  pass


@cli.command()
def list():
  for cfg in CONFIG_DIR.glob('*.yaml'):
    print(cfg.name)


@cli.command()
@click.argument('config')
def test(config):
  print('datacube -v ingest -c "%(config)s" --dry-run' % dict(config=CONFIG_DIR/config))


@cli.command()
@click.argument('config')
def qsub(config):
  qsub = 'qsub -q express -N ls5_ingest -P v10 -l ncpus=48,mem=90gb,walltime=2:00:00 -- /bin/bash "%(distr)s" --ppn 16 --env "%(env)s" datacube -v ingest -c "%(config)s" --executor distributed DSCHEDULER'
  print(qsub % dict(config=CONFIG_DIR/config,
                    env=SCRIPT_DIR/'environment.sh',
                    distr=SCRIPT_DIR/'distributed.sh'))


if __name__ == '__main__':
  cli()

