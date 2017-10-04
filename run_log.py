import json
import logging

import click
from celery.result import AsyncResult

from digitalearthau import simpletask

from datacube._celery_runner import app
from celery.utils.log import get_task_logger

_LOG = logging.getLogger("runtask")

from celery import Celery

import celery.events.state as celery_state

import sys

def my_monitor(app):
    state: celery_state.State = app.events.State()

    def dump(event):
        # click.secho(f"{event['type']} ", bold=True, nl=False)
        print(json.dumps(event))
        sys.stdout.flush()

    def announce_failed_tasks(event):
        state.event(event)
        # task name is sent only with -received event, and state
        # will keep track of this for us.
        task: celery_state.Task = state.tasks.get(event['uuid'])

        print('TASK FAILED: %s[%s] %s' % (
            task.name, task.uuid, task.info(),))

    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
            # 'task-failed': announce_failed_tasks,
            '*': dump
        })
        recv.capture(limit=None, timeout=None, wakeup=True)


if __name__ == '__main__':
    my_monitor(app)
