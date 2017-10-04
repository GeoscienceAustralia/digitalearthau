import logging

import celery
import time
from celery import Celery
from celery.result import AsyncResult
from cloudpickle import cloudpickle
import kombu.serialization

from digitalearthau.system import MyThing

kombu.serialization.registry.register('cloudpickle', cloudpickle.dumps, cloudpickle.loads,
                                      content_type='application/x-python-cloudpickle',
                                      content_encoding='binary')

app = Celery(__name__, broker='redis://localhost', backend='rpc://')
app.conf.accept_content = ['cloudpickle', 'json']
app.conf.task_serializer = app.conf.result_serializer = app.conf.event_serializer = 'cloudpickle'
app.conf.task_compression = app.conf.result_compression = 'gzip'

from celery.utils.log import get_task_logger

# _LOG = get_task_logger(__name__)
_LOG = logging.getLogger(__name__)


def do_something4(x, y):
    time.sleep(5)
    # _LOG.info(f'doing {x}, {y}')
    MyThing.try_logging(x)
    time.sleep(5)
    return x + y


@app.task(track_started=True, compression=None)
def do_something(x, y):
    time.sleep(5)
    # _LOG.info(f'doing {x}, {y}')
    # MyThing.try_logging(x)
    time.sleep(5)
    return x + y


class MyTask(celery.Task):
    pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    print(cloudpickle.dumps(do_something4))
