#!/usr/bin/env python
""" test app for qsub submission
"""
from __future__ import print_function

from collections import namedtuple
import pathlib

import click

import datacube
from datacube.ui.task_app import wrap_task
from datacube.ui.click import verbose_option

from digitalearthau.qsub import with_qsub_runner
from digitalearthau.pbs import hostname

Task = namedtuple('Task', ['val'])
Result = namedtuple('Result', ['result', 'val', 'op', 'worker'])


def random_sleep(amount_secs=0.1, prop=0.5):
    """emulate processing time variance"""
    from time import sleep
    from random import uniform

    if uniform(0, 1) < prop:
        sleep(amount_secs)


def task_generator(num_tasks):
    """ generates sample tasks
    """
    for i in range(num_tasks):
        click.echo('Generating task: {}'.format(i))
        yield Task(i)._asdict()


# pylint: disable=invalid-name
def run_task(task, op):
    """ Runs across multiple cpus/nodes
    """
    from math import sqrt

    if not isinstance(task, Task):
        task = Task(**task)

    host = hostname()

    ops = {'sqrt': sqrt,
           'pow2': lambda x: x * x}

    random_sleep(1, 0.1)  # Sleep for 1 second 10% of the time

    val = task.val

    if val == 666:
        click.echo('Injecting failure')
        raise IOError('Fake IO Error')

    result = ops[op](val)
    click.echo('{} => {}'.format(val, result))

    return Result(result=result,
                  val=val,
                  op=op,
                  worker=host)


def log_completed_task(result):
    """ dump result to stdout
    """
    click.echo('From [{worker}]: {val} => {op} => {result}'.format(**result._asdict()))


@click.command(help='TODO')
@verbose_option
@click.argument('num_tasks', nargs=1, type=int)
@click.option('--op', help='Configure dummy task: sqrt|pow2', default='sqrt')
@with_qsub_runner()
def main(num_tasks, op, qsub=None, runner=None):
    """ test app for qsub/runner
    """
    if qsub is not None:
        click.echo(repr(qsub))
        return qsub('--op', op, num_tasks)

    click.echo(datacube.__file__)
    click.echo('PWD:' + str(pathlib.Path('.').absolute()))
    click.echo('num_tasks: ' + str(num_tasks))

    if runner.start() is False:
        click.echo('Failed to launch worker pool')
        return 1

    runner(task_generator(num_tasks),
           wrap_task(run_task, op),
           log_completed_task)

    click.echo('Shutting down worker pool')
    runner.stop()
    click.echo('All done!')
    return 0


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
