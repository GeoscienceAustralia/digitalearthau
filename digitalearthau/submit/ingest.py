#!/usr/bin/env python

from __future__ import print_function

import subprocess
from pathlib import Path

import click
from click import echo

import digitalearthau
from digitalearthau import INGEST_CONFIG_DIR

DISTRIBUTED_SCRIPT = digitalearthau.SCRIPT_DIR / 'run_distributed.sh'


@click.group()
def cli():
    pass


@cli.command('list')
def list_products():
    """List available products to ingest."""
    for cfg in INGEST_CONFIG_DIR.glob('*.yaml'):
        echo(cfg.stem)


@cli.command('qsub')
@click.option('--queue', '-q', default='normal',
              type=click.Choice(['normal', 'express']))
@click.option('--project', '-P', default='v10')
@click.option('--nodes', '-n', required=True,
              help='Number of nodes to request',
              type=click.IntRange(1, 100))
@click.option('--walltime', '-t', default=10,
              help='Number of hours (range: 1-48hrs) to request',
              type=click.IntRange(1, 48))
@click.option('--name', help='Job name to use')
@click.option('--allow-product-changes', help='allow changes to product definition', is_flag=True)
@click.option('--email_options', '-m', default='a',
              type=click.Choice(['a', 'b', 'e', 'n']),
              help='Send Email options when execution, \n'
              '[aborted | begins | ends | do not send email]')
@click.option('--email_id', '-M', default='nci.monitor@dea.ga.gov.au',
              help='Email Recipient List')
@click.option('--job_attributes', '-W', default='umask=33',
              help='Setting job attributes\n'
              '<attribute>=<value>')
@click.option('--config-file', '-c', default='',
              type=click.Path(exists=False, readable=True, writable=False, dir_okay=False),
              help='Ingest configuration file')
@click.argument('product_name')
@click.argument('year')
def do_qsub(product_name, year, queue, project, nodes, walltime, name, allow_product_changes, email_options, email_id,
            job_attributes, config_file):
    """Submits an ingest job to qsub."""
    if not config_file:
        config_path = INGEST_CONFIG_DIR / '{}.yaml'.format(product_name)
    else:
        config_path = Path(config_file).absolute()
    taskfile = Path(product_name + '_' + year.replace('-', '_') + '.bin').absolute()

    if not config_path.exists():
        raise click.BadParameter("No config found for product {!r}".format(product_name))

    subprocess.check_call('datacube -v system check', shell=True)

    product_changes_flag = '--allow-product-changes' if allow_product_changes else ''

    prep = 'datacube -v ingest -c "%(config)s" %(product_changes_flag)s --year %(year)s ' \
           '--save-tasks "%(taskfile)s"'
    cmd = prep % dict(config=config_path, taskfile=taskfile, year=year,
                      product_changes_flag=product_changes_flag)
    if click.confirm('\n' + cmd + '\nRUN?', default=True):
        subprocess.check_call(cmd, shell=True)

    test = 'datacube -v ingest %(product_changes_flag)s --load-tasks "%(taskfile)s" --dry-run'
    cmd = test % dict(taskfile=taskfile, product_changes_flag=product_changes_flag)
    if click.confirm('\n' + cmd + '\nRUN?', default=False):
        subprocess.check_call(cmd, shell=True)

    name = name or taskfile.stem
    qsub = 'qsub -V -q %(queue)s -N %(name)s -P %(project)s ' \
           '-m %(email_options)s -M %(email_id)s -W %(job_attributes)s ' \
           '-l ncpus=%(ncpus)d,mem=%(mem)dgb,walltime=%(walltime)d:00:00 ' \
           '-- /bin/bash "%(distr)s" "%(dea_module)s" --ppn 16 ' \
           'datacube -v ingest %(product_changes_flag)s --load-tasks "%(taskfile)s" ' \
           '--executor distributed DSCHEDULER'
    cmd = qsub % dict(taskfile=taskfile,
                      distr=DISTRIBUTED_SCRIPT,
                      dea_module=digitalearthau.MODULE_NAME,
                      queue=queue,
                      name=name,
                      project=project,
                      ncpus=nodes * 16,
                      mem=nodes * 20,
                      walltime=walltime,
                      email_options=email_options,
                      job_attributes=job_attributes,
                      email_id=email_id,
                      product_changes_flag=product_changes_flag)
    if click.confirm('\n' + cmd + '\nRUN?', default=True):
        subprocess.check_call(cmd, shell=True)


@cli.command()
@click.option('--queue', '-q', default='normal',
              type=click.Choice(['normal', 'express']))
@click.option('--project', '-P', default='v10')
@click.option('--nodes', '-n', required=True,
              help='Number of nodes to request',
              type=click.IntRange(1, 100))
@click.option('--walltime', '-t', default=10,
              help='Number of hours to request',
              type=click.IntRange(1, 48))
@click.option('--name', help='Job name to use')
@click.argument('product_name')
@click.argument('year')
def stack(product_name, year, queue, project, nodes, walltime, name):
    """Stacks a year of tiles into a single NetCDF."""
    config_path = INGEST_CONFIG_DIR / '{}.yaml'.format(product_name)
    if not config_path.exists():
        raise click.BadParameter("No config found for product {!r}".format(product_name))

    taskfile = Path(product_name + '_' + year.replace('-', '_') + '.bin').absolute()

    subprocess.check_call('datacube -v system check', shell=True)

    prep = 'datacube-stacker -v --app-config "%(config)s" --year %(year)s --save-tasks "%(taskfile)s"'
    cmd = prep % dict(config=config_path, taskfile=taskfile, year=year)
    if click.confirm('\n' + cmd + '\nRUN?', default=True):
        try:
            subprocess.check_call(cmd, shell=True)
        except subprocess.CalledProcessError as e:
            if e.returncode == 2:
                click.echo('No tasks found - nothing to do!')
                click.get_current_context().exit(e.returncode)
            else:
                raise e

    name = name or taskfile.stem
    qsub = 'qsub -q %(queue)s -N %(name)s -P %(project)s ' \
           '-l ncpus=%(ncpus)d,mem=%(mem)dgb,walltime=%(walltime)d:00:00 ' \
           '-- /bin/bash "%(distr)s" "%(dea_module)s" --ppn 16 ' \
           'datacube-stacker -v --load-tasks "%(taskfile)s" --executor distributed DSCHEDULER'
    cmd = qsub % dict(taskfile=taskfile,
                      distr=DISTRIBUTED_SCRIPT,
                      dea_module=digitalearthau.MODULE_NAME,
                      queue=queue,
                      name=name,
                      project=project,
                      ncpus=nodes * 16,
                      mem=nodes * 31,
                      walltime=walltime)
    if click.confirm('\n' + cmd + '\nRUN?', default=True):
        subprocess.check_call(cmd, shell=True)


@cli.command()
@click.option('--queue', '-q', default='normal',
              type=click.Choice(['normal', 'express']))
@click.option('--project', '-P', default='v10')
@click.option('--nodes', '-n', required=True,
              help='Number of nodes to request',
              type=click.IntRange(1, 100))
@click.option('--walltime', '-t', default=10,
              help='Number of hours to request',
              type=click.IntRange(1, 48))
@click.option('--name', help='Job name to use')
@click.argument('product_name')
@click.argument('year')
def fix(product_name, year, queue, project, nodes, walltime, name):
    """Rewrites files with metadata from the config."""
    taskfile = Path(product_name + '_' + year.replace('-', '_') + '.bin').absolute()
    config_path = INGEST_CONFIG_DIR / '{}.yaml'.format(product_name)
    if not config_path.exists():
        raise click.BadParameter("No config found for product {!r}".format(product_name))

    subprocess.check_call('datacube -v system check', shell=True)

    prep = 'datacube-fixer -v --app-config "%(config)s" --year %(year)s --save-tasks "%(taskfile)s"'
    cmd = prep % dict(config=config_path, taskfile=taskfile, year=year)
    if click.confirm('\n' + cmd + '\nRUN?', default=True):
        subprocess.check_call(cmd, shell=True)

    name = name or taskfile.stem
    qsub = 'qsub -q %(queue)s -N %(name)s -P %(project)s ' \
           '-l ncpus=%(ncpus)d,mem=%(mem)dgb,walltime=%(walltime)d:00:00 ' \
           '-- /bin/bash "%(distr)s" "%(dea_module)s" --ppn 16 ' \
           'datacube-fixer -v --load-tasks "%(taskfile)s" --executor distributed DSCHEDULER'
    cmd = qsub % dict(taskfile=taskfile,
                      distr=DISTRIBUTED_SCRIPT,
                      dea_module=digitalearthau.MODULE_NAME,
                      queue=queue,
                      name=name,
                      project=project,
                      ncpus=nodes * 16,
                      mem=nodes * 31,
                      walltime=walltime)
    if click.confirm('\n' + cmd + '\nRUN?', default=True):
        subprocess.check_call(cmd, shell=True)


if __name__ == '__main__':
    cli()
