#!/usr/bin/env python

from __future__ import print_function

import os
import subprocess
from pathlib import Path

import click
from click import echo

import digitalearthau
from digitalearthau import INGEST_CONFIG_DIR

from datacube.ui import click as ui

DISTRIBUTED_SCRIPT = digitalearthau.SCRIPT_DIR / 'run_distributed.sh'
APP_NAME = 'dea-submit-ingest'

# pylint: disable=invalid-name
queue_options = click.option('--queue', '-q', default='normal',
                             type=click.Choice(['normal', 'express']))

# pylint: disable=invalid-name
project_options = click.option('--project', '-P', default='v10')

# pylint: disable=invalid-name
node_options = click.option('--nodes', '-n', required=True,
                            help='Number of nodes to request',
                            type=click.IntRange(1, 100))

# pylint: disable=invalid-name
walltime_options = click.option('--walltime', '-t', default=10,
                                help='Number of hours (range: 1-48hrs) to request',
                                type=click.IntRange(1, 48))

# pylint: disable=invalid-name
name_option = click.option('--name', help='Job name to use')

# pylint: disable=invalid-name
mail_options = click.option('--email-options', '-m', default='abe',
                            type=click.Choice(['a', 'b', 'e', 'n', 'ae', 'ab', 'be', 'abe']),
                            help='Send Email when execution is, \n'
                            '[a = aborted | b = begins | e = ends | n = do not send email]')

# pylint: disable=invalid-name
email_id_options = click.option('--email-id', '-M', default='nci.monitor@dea.ga.gov.au',
                                help='Email Recipient List')


@click.group()
def cli():
    pass


@cli.command('list')
def list_products():
    """List available products to ingest."""
    for cfg in INGEST_CONFIG_DIR.glob('*.yaml'):
        echo(cfg.stem)


@cli.command('qsub')
@ui.config_option
@ui.verbose_option
@queue_options
@project_options
@node_options
@walltime_options
@name_option
@click.option('--allow-product-changes', help='allow changes to product definition', is_flag=True)
@mail_options
@email_id_options
@click.option('--job-attributes', '-W', default='umask=33',
              help='Setting job attributes\n'
              '<attribute>=<value>')
@click.option('--app-config', '-c', default='',
              type=click.Path(exists=False, readable=True, writable=False, dir_okay=False),
              help='Ingest configuration file')
@click.option('--queue-size', type=click.IntRange(1, 100000), default=40000, help='Ingest task queue size')
@click.argument('product_name')
@click.argument('year')
def do_qsub(queue, project, nodes, walltime, name, allow_product_changes, email_options, email_id,
            job_attributes, app_config, queue_size, product_name, year):
    """Submits an ingest job, using a two stage PBS job submission."""
    if not app_config:
        config_path = INGEST_CONFIG_DIR / '{}.yaml'.format(product_name)
    else:
        config_path = Path(app_config).absolute()
    taskfile = Path(product_name + '_' + year.replace('-', '_') + '.bin').absolute()

    if not config_path.exists():
        raise click.BadParameter("No config found for product {!r}".format(product_name))

    subprocess.check_call('datacube -v system check', shell=True)

    product_changes_flag = '--allow-product-changes' if allow_product_changes else ''

    prep = 'qsub -V -q %(queue)s -N ingest_save_tasks -P %(project)s ' \
           '-m %(email_options)s -M %(email_id)s -l walltime=05:00:00,mem=31GB -W umask=33 ' \
           '-- datacube -v ingest -c "%(config)s" %(product_changes_flag)s --year %(year)s ' \
           '--save-tasks "%(taskfile)s"'
    cmd = prep % dict(queue=queue, project=project, email_options=email_options, email_id=email_id,
                      config=config_path, product_changes_flag=product_changes_flag,
                      year=year, taskfile=taskfile)
    if click.confirm('\n' + cmd + '\nRUN?', default=True):
        savetask_job = subprocess.check_output(cmd, shell=True).decode("utf-8").split('\n')[0]
    else:
        click.echo('Two stage ingest PBS job not requested, hence exiting!')
        click.get_current_context().exit(0)

    test = 'qsub -V -q %(queue)s -N ingest_dry_run -P %(project)s -W depend=afterok:%(savetask_job)s ' \
           '-m %(email_options)s -M %(email_id)s -l walltime=05:00:00,mem=31GB -W umask=33 ' \
           '-- datacube -v ingest %(product_changes_flag)s --load-tasks "%(taskfile)s" --dry-run'
    cmd = test % dict(queue=queue, project=project, savetask_job=savetask_job,
                      email_options=email_options, email_id=email_id,
                      product_changes_flag=product_changes_flag, taskfile=taskfile)
    if click.confirm('\n' + cmd + '\nRUN?', default=False):
        dryrun_job = subprocess.check_output(cmd, shell=True).decode("utf-8").split('\n')[0]
    else:
        click.echo('Dry run not requested!')
        dryrun_job = savetask_job

    datacube_config = os.environ.get('DATACUBE_CONFIG_PATH')
    name = name or taskfile.stem
    qsub = 'qsub -V -q %(queue)s -N %(name)s -P %(project)s -W depend=afterok:%(savetask_job)s:%(dryrun_job)s: ' \
           '-m %(email_options)s -M %(email_id)s -W %(job_attributes)s ' \
           '-l ncpus=%(ncpus)d,mem=%(mem)dgb,walltime=%(walltime)d:00:00 ' \
           '-- "%(distr)s" "%(dea_module)s" --ppn 16 ' \
           'datacube -v -C %(datacube_config)s ingest %(product_changes_flag)s --load-tasks "%(taskfile)s" ' \
           '--queue-size %(queue_size)s --executor distributed DSCHEDULER'
    cmd = qsub % dict(queue=queue,
                      name=name,
                      project=project,
                      savetask_job=savetask_job,
                      dryrun_job=dryrun_job,
                      email_options=email_options,
                      email_id=email_id,
                      job_attributes=job_attributes,
                      ncpus=nodes * 16,
                      mem=nodes * 31,
                      walltime=walltime,
                      distr=DISTRIBUTED_SCRIPT,
                      dea_module=digitalearthau.MODULE_NAME,
                      datacube_config=datacube_config,
                      product_changes_flag=product_changes_flag,
                      taskfile=taskfile,
                      queue_size=queue_size)
    if click.confirm('\n' + cmd + '\nRUN?', default=True):
        subprocess.check_call(cmd, shell=True)
    else:
        click.echo('Load task for datacube ingest not requested, hence exiting.')
        click.get_current_context().exit(0)


@cli.command()
@ui.config_option
@ui.verbose_option
@queue_options
@project_options
@node_options
@walltime_options
@name_option
@mail_options
@email_id_options
@click.argument('product_name')
@click.argument('year')
def stack(queue, project, nodes, walltime, name, email_options, email_id, product_name, year):
    """Stacks a year of tiles into a single NetCDF, using a two stage PBS job submission."""
    config_path = INGEST_CONFIG_DIR / '{}.yaml'.format(product_name)
    if not config_path.exists():
        raise click.BadParameter("No config found for product {!r}".format(product_name))

    taskfile = Path(product_name + '_' + year.replace('-', '_') + '.bin').absolute()

    subprocess.check_call('datacube -v system check', shell=True)

    prep = 'qsub -V -q %(queue)s -N stack_save_tasks -P %(project)s ' \
           '-m %(email_options)s -M %(email_id)s -l walltime=05:00:00,mem=31GB -W umask=33 ' \
           '-- datacube-stacker -v --app-config "%(config)s" --year %(year)s ' \
           '--save-tasks "%(taskfile)s"'
    cmd = prep % dict(queue=queue, project=project, email_options=email_options, email_id=email_id,
                      config=config_path, year=year, taskfile=taskfile)
    if click.confirm('\n' + cmd + '\nRUN?', default=True):
        savetask_job = subprocess.check_output(cmd, shell=True).decode("utf-8").split('\n')[0]
    else:
        click.echo('Two stage datacube-stacker PBS job not requested, hence exiting!')
        click.get_current_context().exit(0)

    datacube_config = os.environ.get('DATACUBE_CONFIG_PATH')
    name = name or taskfile.stem
    qsub = 'qsub -V -q %(queue)s -N %(name)s -P %(project)s ' \
           '-m %(email_options)s -M %(email_id)s -l ncpus=%(ncpus)d,mem=%(mem)dgb,walltime=%(walltime)d:00:00 ' \
           '-W depend=afterok:%(savetask_job)s,umask=33 -- "%(distr)s" "%(dea_module)s" --ppn 16 ' \
           'datacube-stacker -v -C %(datacube_config)s --load-tasks "%(taskfile)s" --executor distributed DSCHEDULER'
    cmd = qsub % dict(queue=queue,
                      name=name,
                      project=project,
                      email_options=email_options,
                      email_id=email_id,
                      ncpus=nodes * 16,
                      mem=nodes * 31,
                      walltime=walltime,
                      savetask_job=savetask_job,
                      distr=DISTRIBUTED_SCRIPT,
                      dea_module=digitalearthau.MODULE_NAME,
                      datacube_config=datacube_config,
                      taskfile=taskfile)
    if click.confirm('\n' + cmd + '\nRUN?', default=True):
        subprocess.check_call(cmd, shell=True)
    else:
        click.echo('Load task for datacube-stacker not requested, hence exiting.')
        click.get_current_context().exit(0)


@cli.command()
@ui.config_option
@ui.verbose_option
@queue_options
@project_options
@node_options
@walltime_options
@name_option
@mail_options
@email_id_options
@click.argument('product_name')
@click.argument('year')
def fix(queue, project, nodes, walltime, name, email_options, email_id, product_name, year):
    """Rewrites files with metadata from the config, using a two stage PBS job submission."""
    taskfile = Path(product_name + '_' + year.replace('-', '_') + '.bin').absolute()
    config_path = INGEST_CONFIG_DIR / '{}.yaml'.format(product_name)
    if not config_path.exists():
        raise click.BadParameter("No config found for product {!r}".format(product_name))

    subprocess.check_call('datacube -v system check', shell=True)

    prep = 'qsub -V -q %(queue)s -N fix_save_tasks -P %(project)s ' \
           '-m %(email_options)s -M %(email_id)s -l walltime=05:00:00,mem=31GB -W umask=33 ' \
           '-- datacube-fixer -v --app-config "%(config)s" --year %(year)s --save-tasks "%(taskfile)s"'
    cmd = prep % dict(queue=queue, project=project, email_options=email_options, email_id=email_id,
                      config=config_path, year=year, taskfile=taskfile)
    if click.confirm('\n' + cmd + '\nRUN?', default=True):
        savetask_job = subprocess.check_output(cmd, shell=True).decode("utf-8").split('\n')[0]
    else:
        click.echo('Two stage datacube-fixer PBS job not requested, hence exiting!')
        click.get_current_context().exit(0)

    datacube_config = os.environ.get('DATACUBE_CONFIG_PATH')
    name = name or taskfile.stem
    qsub = 'qsub -V -q %(queue)s -N %(name)s -P %(project)s ' \
           '-m %(email_options)s -M %(email_id)s -l ncpus=%(ncpus)d,mem=%(mem)dgb,walltime=%(walltime)d:00:00 ' \
           '-W depend=afterok:%(savetask_job)s,umask=33 -- "%(distr)s" "%(dea_module)s" --ppn 16 ' \
           'datacube-fixer -v -C %(datacube_config)s --load-tasks "%(taskfile)s" --executor distributed DSCHEDULER'
    cmd = qsub % dict(queue=queue,
                      name=name,
                      project=project,
                      email_options=email_options,
                      email_id=email_id,
                      ncpus=nodes * 16,
                      mem=nodes * 31,
                      walltime=walltime,
                      savetask_job=savetask_job,
                      distr=DISTRIBUTED_SCRIPT,
                      dea_module=digitalearthau.MODULE_NAME,
                      datacube_config=datacube_config,
                      taskfile=taskfile)
    if click.confirm('\n' + cmd + '\nRUN?', default=True):
        subprocess.check_call(cmd, shell=True)
    else:
        click.echo('Load task for datacube-fixer not requested, hence exiting.')
        click.get_current_context().exit(0)


if __name__ == '__main__':
    cli()
