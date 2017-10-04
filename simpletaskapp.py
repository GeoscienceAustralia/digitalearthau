import logging
from typing import List, Iterable

import click
import time

from celery.utils.log import get_task_logger

from datacube._celery_runner import CeleryExecutor
from datacube.ui.click import pass_index
from datacube.ui.task_app import task_app, task_app_options, run_tasks


def make_simple_config(index, config, dry_run=False, **query):
    return dict(config)


def make_simple_tasks(index, config, year=None, **kwargs):
    for i in range(100):
        yield dict(i=i, year=year)


from digitalearthau.system import MyThing


def run_simple(task):
    _LOG = get_task_logger(__name__)
    # _LOG.info("Starting task")

    i = task['i']
    # time.sleep(1)

    big_num = task['year'] or 30
    res = i + big_num
    print("Printing")
    # _LOG.warning("Logging %r", i)

    MyThing.try_logging(i)
    # _LOG.info("Got result %r", res)
    # time.sleep(1)
    return res


@click.command()
@pass_index(app_name='simpletask')
@task_app_options
@task_app(make_config=make_simple_config, make_tasks=make_simple_tasks)
def cli(index, config, tasks: Iterable[dict], executor: CeleryExecutor, **kwargs):
    run_tasks(tasks, executor, run_simple, None)


if __name__ == '__main__':
    cli()
