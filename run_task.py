import logging

from celery.result import AsyncResult

from digitalearthau import simpletask
from celery.utils.log import get_task_logger

_LOG = logging.getLogger("runtask")

from celery import Celery

import celery.events.state as celery_state

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    res: AsyncResult = simpletask.do_something.delay(3, 8)
    _LOG.info("Submitted")
    _LOG.info(f"Result {res.get()}")
    _LOG.info("submitting failure")
    res: AsyncResult = simpletask.do_something.delay(3, 'sht')
    _LOG.info(f"Result {repr(res.get(propagate=False))}")
    _LOG.info("Done")
