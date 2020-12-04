"""
DEA Stacker

A DEA version of `stacker` from ODC

Uses the exact same implementation, but wraps it in QSUB Running/Event logging goodness.

As such, it is a three step process, with the second two happening automatically after the first:

1. Submit

 Submit a single threaded PBS job to calculate how big the requested job is.
 This step executes very quickly, creates the 'job' directory which groups log files
 and metadata for the full job, then finally schedules a 'generate' job to run using
 PBS qsub.

2. Generate

 Run as a single threaded process inside PBS to:
 calculate how much work is required,
 write the tasks out to a task file and then
 schedule another PBS job of an appropriate size to 'Run' the stacking

3. Run

 Do all of the stacking. Spin up a Redis queue and use celery to farm the tasks
 out across a large PBS job.
"""
import logging
from datetime import datetime
from functools import partial
from math import ceil
from pathlib import Path
from typing import Tuple

import click

import datacube
import digitalearthau
from datacube.api.query import Query
from datacube.index import Index
from datacube.ui import click as ui
from datacube.ui import task_app
from datacube_apps.stacker import stacker
from digitalearthau import __version__
from digitalearthau import paths, serialise
# pylint: disable=invalid-name
from digitalearthau.qsub import with_qsub_runner, TaskRunner
from digitalearthau.runners.model import TaskDescription
from digitalearthau.runners.util import init_task_app, submit_subjob

_LOG = logging.getLogger(__file__)
APP_NAME = 'dea-stacker'


@click.group('dea-stacker', help=__doc__)
@click.version_option(version=__version__)
def cli():
    pass


@cli.command(help='Kick off two stage PBS job')
@click.option('--project', '-P', default='u46')
@click.option('--queue', '-q', default='normal',
              type=click.Choice(['normal', 'express']))
@click.option('--year', 'time_range',
              callback=task_app.validate_year,
              help='Limit the process to a particular year')
@click.option('--no-qsub', is_flag=True, default=False,
              help="Skip submitting job")
@task_app.app_config_option
@ui.config_option
@ui.verbose_option
@ui.pass_index(app_name=APP_NAME)
def submit(index: Index,
           app_config: str,
           project: str,
           queue: str,
           no_qsub: bool,
           time_range: Tuple[datetime, datetime]):
    app_config_path = Path(app_config).resolve()
    app_config = paths.read_document(app_config_path)

    task_desc, task_path = init_task_app(
        job_type="stack",
        source_products=[app_config['output_type']],  # With stacker, source=output
        output_products=[app_config['output_type']],  # With stacker, source=output
        # TODO: Use @datacube.ui.click.parsed_search_expressions to allow params other than time from the cli?
        datacube_query_args=Query(index=index, time=time_range).search_terms,
        app_config_path=app_config_path,
        pbs_project=project,
        pbs_queue=queue
    )
    _LOG.info("Created task description: %s", task_path)

    if no_qsub:
        _LOG.info('Skipping submission due to --no-qsub')
        return

    submit_subjob(
        name='generate',
        task_desc=task_desc,
        command=[
            'generate', '-v', '-v',
            '--task-desc', str(task_path),
        ],
        qsub_params=dict(
            mem='20G',
            wd=True,
            ncpus=1,
            walltime='1h',
            name='stack-generate-{}'.format(make_tag(task_desc))
        )
    )


def make_tag(task_desc: TaskDescription):
    return "{:%Y%m%d%H%M%S}".format(task_desc.task_dt)


@cli.command(help='Generate Tasks into file and Queue PBS job to process them')
@click.option('--no-qsub', is_flag=True, default=False, help="Skip submitting qsub for next step")
@click.option(
    '--task-desc', 'task_desc_file', help='Task environment description file',
    required=True,
    type=click.Path(exists=True, readable=True, writable=False, dir_okay=False)
)
@ui.verbose_option
@ui.log_queries_option
@ui.pass_index(app_name=APP_NAME)
def generate(index: Index,
             task_desc_file: str,
             no_qsub: bool):
    config, task_desc = _make_config_and_description(index, Path(task_desc_file))

    num_tasks_saved = task_app.save_tasks(
        config,
        stacker.make_stacker_tasks(index, config, **task_desc.parameters.query),
        task_desc.runtime_state.task_serialisation_path
    )
    _LOG.info('Found and saved %d tasks', num_tasks_saved)

    if not num_tasks_saved:
        _LOG.info("No tasks. Finishing.")
        return

    nodes, walltime = estimate_job_size(num_tasks_saved)
    _LOG.info('Will request %d nodes and %s time', nodes, walltime)

    if no_qsub:
        _LOG.info('Skipping submission due to --no-qsub')
        return

    submit_subjob(
        name='run',
        task_desc=task_desc,

        command=[
            'run',
            '-vv',
            '--task-desc', str(task_desc_file),
            '--celery', 'pbs-launch',
        ],
        qsub_params=dict(
            name='stack-run-{}'.format(make_tag(task_desc)),
            mem='small',
            wd=True,
            nodes=nodes,
            walltime=walltime
        ),
    )


def _make_config_and_description(index: Index, task_desc_path: Path) -> Tuple[dict, TaskDescription]:
    task_desc = serialise.load_structure(task_desc_path, TaskDescription)

    app_config = task_desc.runtime_state.config_path

    config = paths.read_document(app_config)

    config['output_type'] = config['output_type']  # TODO: Temporary until ODC code is updated
    config['app_config_file'] = str(app_config)
    config = stacker.make_stacker_config(index, config)
    config['taskfile_utctime'] = make_tag(task_desc)
    config['version'] = digitalearthau.__version__ + ' ' + datacube.__version__

    return config, task_desc


def estimate_job_size(num_tasks):
    """ Translate num_tasks to number of nodes and walltime
    """
    max_nodes = 5
    cores_per_node = 16
    task_time_mins = 10

    if num_tasks < max_nodes * cores_per_node:
        nodes = ceil(num_tasks / cores_per_node / 4)  # If fewer tasks than max cores, try to get 4 tasks to a core
    else:
        nodes = max_nodes

    tasks_per_cpu = ceil(num_tasks / (nodes * cores_per_node))
    wall_time_mins = '{mins}m'.format(mins=(task_time_mins * tasks_per_cpu))

    return nodes, wall_time_mins


@cli.command(help='Process all tasks in a task file')
@click.option('--dry-run', is_flag=True, default=False, help='Check if output files already exist')
@click.option(
    '--task-desc', 'task_desc_file', help='Task environment description file',
    required=True,
    type=click.Path(exists=True, readable=True, writable=False, dir_okay=False)
)
@with_qsub_runner()
@ui.config_option
@ui.verbose_option
@ui.pass_index(app_name=APP_NAME)
def run(index,
        dry_run: bool,
        task_desc_file: str,
        runner: TaskRunner,
        qsub):
    _LOG.info('Starting DEA Stacker processing...')

    task_desc = serialise.load_structure(Path(task_desc_file), TaskDescription)
    config, tasks = task_app.load_tasks(task_desc.runtime_state.task_serialisation_path)

    if dry_run:
        task_app.check_existing_files((task['filename'] for task in tasks))
        return

    task_func = partial(stacker.do_stack_task, config)
    process_func = partial(stacker.process_result, index)

    try:
        runner(task_desc, tasks, task_func, process_func)
        _LOG.info("Runner finished normally, triggering shutdown.")
    finally:
        runner.stop()


if __name__ == "__main__":
    cli()
